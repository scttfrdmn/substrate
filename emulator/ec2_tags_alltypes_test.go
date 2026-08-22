package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The five resource types CreateTags could not reach before #708 — an AMI, a launch
// template, a fleet, a placement group and a key pair.
//
// Each was well-formed, named a real resource substrate stores, and was dropped:
// ec2TaggableResource's switch had no arm for its prefix, so the request fell to the
// default and answered <return>true</return> having written nothing. Two of the five were
// worse than that — a placement group and a key pair were absent from DescribeTags' scan
// as well, so tags applied by any route were unreportable, and a launch template's own
// tags could not be set by *any* path, since the TagSpecification CreateLaunchTemplate
// read lives inside LaunchTemplateData and is scoped to instance/volume.
//
// Every assertion here goes through HTTP for the reason #688's do: a hand-built plugin
// call would not exercise the dispatcher, and silent success is exactly the failure a
// caller cannot detect.

// ec2TagAllTypesCase is one resource type CreateTags has to reach: how to create an
// instance of it, and the resourceType DescribeTags reports it under.
type ec2TagAllTypesCase struct {
	name         string
	resourceType string
	// create returns the ID a caller passes as ResourceId.N, which for a placement group
	// and a key pair is *not* the name its record is keyed by.
	create func(t *testing.T, ts *httptest.Server) string
}

// ec2TagAllTypesCases is the five types #708 adds, and only those: the other ten already
// had an arm, and [TestEC2_TagAuthz_ScopedDenyOnAnyNamedResourceBlocks] covers all
// fifteen at the authorization layer.
func ec2TagAllTypesCases() []ec2TagAllTypesCase {
	return []ec2TagAllTypesCase{
		{
			name:         "an AMI",
			resourceType: "image",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				return ec2CreateImageID(t, ts, map[string]string{
					"Action":     "CreateImage",
					"InstanceId": ec2TagTestInstance(t, ts),
					"Name":       "tagme-ami",
				})
			},
		},
		{
			name:         "a launch template",
			resourceType: "launch-template",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				return newFleetLaunchTemplate(t, ts, "tagme-lt")
			},
		},
		{
			name:         "a fleet",
			resourceType: "fleet",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				return ec2TagTestFleet(t, ts)
			},
		},
		{
			name:         "a placement group",
			resourceType: "placement-group",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				id, _ := ec2TagTestPlacementGroup(t, ts, "tagme-pg", nil)
				return id
			},
		},
		{
			name:         "a key pair",
			resourceType: "key-pair",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				id, _ := ec2TagTestKeyPair(t, ts, "tagme-key", nil)
				return id
			},
		},
	}
}

// ec2TagTestFleet creates an instant fleet and returns its ID, which carries four
// internal hyphens of its own — the shape nothing in the tagging path may split on "-".
func ec2TagTestFleet(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	ltID := newFleetLaunchTemplate(t, ts, "tagme-fleet-lt")
	var fleet struct {
		FleetID string `xml:"fleetId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &fleet)
	require.NotEmpty(t, fleet.FleetID)
	require.Greater(t, strings.Count(fleet.FleetID, "-"), 1,
		"a fleet ID carries internal hyphens; a one-hyphen fixture would not test the prefix match")
	return fleet.FleetID
}

// ec2TagTestPlacementGroup creates a placement group and returns its ID and the tagSet
// CreatePlacementGroup reported, which is how the tag-on-create half is read back.
func ec2TagTestPlacementGroup(t *testing.T, ts *httptest.Server, name string,
	extra map[string]string) (string, []describedTag) {
	t.Helper()
	params := map[string]string{"Action": "CreatePlacementGroup", "GroupName": name, "Strategy": "spread"}
	for k, v := range extra {
		params[k] = v
	}
	var pg struct {
		GroupID string         `xml:"placementGroup>groupId"`
		Tags    []describedTag `xml:"placementGroup>tagSet>item"`
	}
	ec2FleetXML(t, ts, params, &pg)
	require.True(t, strings.HasPrefix(pg.GroupID, "pg-"), "got %q", pg.GroupID)
	return pg.GroupID, pg.Tags
}

// ec2TagTestKeyPair creates a key pair and returns its ID and the tagSet CreateKeyPair
// reported.
func ec2TagTestKeyPair(t *testing.T, ts *httptest.Server, name string,
	extra map[string]string) (string, []describedTag) {
	t.Helper()
	params := map[string]string{"Action": "CreateKeyPair", "KeyName": name}
	for k, v := range extra {
		params[k] = v
	}
	var kp struct {
		KeyPairID string         `xml:"keyPairId"`
		Tags      []describedTag `xml:"tagSet>item"`
	}
	ec2FleetXML(t, ts, params, &kp)
	require.True(t, strings.HasPrefix(kp.KeyPairID, "key-"), "got %q", kp.KeyPairID)
	return kp.KeyPairID, kp.Tags
}

// TestEC2_CreateTags_ReachesEveryTaggableType is the decisive test for #708: a tag
// applied to each of the five new types is stored and reported.
//
// DescribeTags is the reader, for two reasons. It is type-agnostic — one scan over every
// namespace — so a single assertion covers storage and reporting; and its resourceId is
// where the two name-keyed types could go wrong silently. A placement group and a key pair
// are keyed by *name* in state, so reporting the state key's tail would name the resource
// by something a caller cannot pass back to CreateTags. The ID member each record carries
// is what ec2TagScanTarget.idMember exists to read.
func TestEC2_CreateTags_ReachesEveryTaggableType(t *testing.T) {
	for _, tc := range ec2TagAllTypesCases() {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			id := tc.create(t, ts)

			status, code, message := ec2CreateTags(t, ts, id, map[string]string{
				"Tag.1.Key":   "Env",
				"Tag.1.Value": "prod",
			})
			require.Equal(t, http.StatusOK, status, "CreateTags on %s: %s %s", tc.name, code, message)

			tags, _ := ec2DescribeTags(t, ts, map[string]string{
				"Filter.1.Name":    "resource-id",
				"Filter.1.Value.1": id,
			})
			require.Len(t, tags, 1, "DescribeTags must report the tag CreateTags wrote on %s", tc.name)
			assert.Equal(t, id, tags[0].ResourceID,
				"the tag is reported against the ID the caller tagged, not the name its record is keyed by")
			assert.Equal(t, tc.resourceType, tags[0].ResourceType)
			assert.Equal(t, "Env", tags[0].Key)
			assert.Equal(t, "prod", tags[0].Value)
		})
	}
}

// TestEC2_DeleteTags_ReachesEveryTaggableType pins the symmetric path: the same five
// types can have a tag removed again.
//
// Worth its own test rather than an assertion appended to the one above, because the two
// operations resolve their IDs through the same function and could still diverge — until
// #708 both fell to the same silent default, so a fix to one would look complete.
func TestEC2_DeleteTags_ReachesEveryTaggableType(t *testing.T) {
	for _, tc := range ec2TagAllTypesCases() {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			id := tc.create(t, ts)
			status, _, _ := ec2CreateTags(t, ts, id, map[string]string{
				"Tag.1.Key":   "Env",
				"Tag.1.Value": "prod",
			})
			require.Equal(t, http.StatusOK, status)

			// DeleteTags names keys and treats the value as optional.
			ec2FleetXML(t, ts, map[string]string{
				"Action":       "DeleteTags",
				"ResourceId.1": id,
				"Tag.1.Key":    "Env",
			}, nil)

			tags, _ := ec2DescribeTags(t, ts, map[string]string{
				"Filter.1.Name":    "resource-id",
				"Filter.1.Value.1": id,
			})
			assert.Empty(t, tags, "DeleteTags left the tag on %s", tc.name)
		})
	}
}

// ec2InvalidTagIDMessage is the wording substrate emits for a ResourceId whose prefix
// names no taggable type, pinned so it cannot drift silently.
//
// Provenance is split. The *code* InvalidID is AWS's, from EC2's client-error table —
// CreateTags' own Errors section publishes nothing operation-specific, pointing only at
// the common error types — and so is the sentence of guidance: "Ensure that you provide
// the full resource ID; for example, ami-2bb65342 for an AMI." Naming the offending ID
// where AWS writes "The specified ID" is substrate's, and is the half a caller who sent a
// truncated ID actually needs.
func ec2InvalidTagIDMessage(id string) string {
	return "The ID '" + id + "' for the resource you are trying to tag is not valid. " +
		"Ensure that you provide the full resource ID; for example, ami-2bb65342 for an AMI."
}

// TestEC2_Tags_UnknownResourceIDRefused covers the other half of #708: an ID whose prefix
// names no taggable type is refused with InvalidID rather than reported as a success.
//
// Silent success is the one answer a caller cannot detect. It was substrate's answer for
// every unrecognized prefix, under a comment claiming it matched AWS — AWS publishes
// InvalidID for exactly this condition.
func TestEC2_Tags_UnknownResourceIDRefused(t *testing.T) {
	for _, id := range []string{
		"tgw-0abc11112222333d", // a transit gateway: real EC2 tags it, substrate does not model it
		"not-an-id",
		"i", // a truncated prefix, which is the mistake AWS's guidance is written for
	} {
		for _, action := range []string{"CreateTags", "DeleteTags"} {
			t.Run(action+" on "+id, func(t *testing.T) {
				ts := newEC2TestServer(t)
				status, code, message := ec2ErrorDetail(t, ts, map[string]string{
					"Action":       action,
					"ResourceId.1": id,
					"Tag.1.Key":    "Env",
					"Tag.1.Value":  "prod",
				})
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidID", code)
				assert.Equal(t, ec2InvalidTagIDMessage(id), message)
			})
		}
	}
}

// TestEC2_CreateTags_RefusesBeforeTaggingAnything is why the ID check is a pre-pass over
// the whole request rather than a refusal inside the apply loop.
//
// A request naming a good ID *before* a bad one would otherwise be refused partway
// through, leaving the resources ahead of the bad ID tagged — the partially-applied state
// CreateTags' three other whole-request checks (reserved keys, lengths, the 50-tag limit)
// exist to prevent, and that real EC2 never produces. The instance is named first
// deliberately: the test fails against an implementation that refuses in the loop.
func TestEC2_CreateTags_RefusesBeforeTaggingAnything(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": instID,
		"ResourceId.2": "tgw-0abc11112222333d",
		"Tag.1.Key":    "Env",
		"Tag.1.Value":  "prod",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidID", code)

	assert.Equal(t, 0, ec2InstanceTagCount(t, ts, instID),
		"the instance named before the invalid ID was tagged by a refused request")
}

// TestEC2_Tags_WellFormedIDNamingNothingIsANoOp pins the third state an ID can be in,
// which is neither of the two above.
//
// A pg- or key- ID is resolved by scanning the namespace for the ID inside each record,
// so an absent placement group or key pair yields no state key at all — where every other
// type's key is computed from the ID and simply reads back nothing. That difference must
// not become a difference in behavior: substrate does not verify a taggable resource's
// existence for any type, so an ID naming nothing is a no-op, and answering InvalidID
// here would refuse a request real EC2 accepts.
func TestEC2_Tags_WellFormedIDNamingNothingIsANoOp(t *testing.T) {
	for _, id := range []string{
		"pg-0abc11112222333d4",
		"key-0abc11112222333d4",
		"ami-0abc11112222333d4",
		"lt-0abc11112222333d4",
		"fleet-12a34b56-7cd8-90ef-1a2b-3c4d5e6f7a8b",
	} {
		t.Run(id, func(t *testing.T) {
			ts := newEC2TestServer(t)
			status, code, message := ec2CreateTags(t, ts, id, map[string]string{
				"Tag.1.Key":   "Env",
				"Tag.1.Value": "prod",
			})
			assert.Equal(t, http.StatusOK, status, "%s %s", code, message)

			tags, _ := ec2DescribeTags(t, ts, nil)
			assert.Empty(t, tags, "a no-op wrote a tag somewhere")
		})
	}
}

// ec2RawXML returns the response body for one EC2 request, which is what an assertion
// about an *absent* element has to read: an unmarshalled struct cannot tell an omitted
// tagSet from an empty one.
func ec2RawXML(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body = %s", body)
	return string(body)
}

// TestEC2_TagOnCreate_KeyPairAndPlacementGroup covers the tag-on-create half of #708 for
// the two types whose records gained tag storage.
//
// Both operations document TagSpecification.N and a tagSet in their responses, and
// neither read it: EC2KeyPair had no Tags field at all, and nothing ever wrote
// EC2PlacementGroup.Tags. ImportKeyPair is covered beside CreateKeyPair because the two
// build the record independently — a fix to one leaves the other silent.
func TestEC2_TagOnCreate_KeyPairAndPlacementGroup(t *testing.T) {
	spec := map[string]string{
		"TagSpecification.1.ResourceType": "", // filled per row
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	}

	t.Run("CreateKeyPair", func(t *testing.T) {
		ts := newEC2TestServer(t)
		params := map[string]string{}
		for k, v := range spec {
			params[k] = v
		}
		params["TagSpecification.1.ResourceType"] = "key-pair"
		id, tags := ec2TagTestKeyPair(t, ts, "tagged-key", params)
		require.Len(t, tags, 1, "CreateKeyPair reported no tagSet")
		assert.Equal(t, "Env", tags[0].Key)
		assert.Equal(t, "prod", tags[0].Value)

		// DescribeKeyPairs reads the stored record rather than the response it just
		// built, so this is what proves the tag was persisted.
		var desc struct {
			KeyPairs []struct {
				KeyPairID string         `xml:"keyPairId"`
				Tags      []describedTag `xml:"tagSet>item"`
			} `xml:"keySet>item"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribeKeyPairs"}, &desc)
		require.Len(t, desc.KeyPairs, 1)
		assert.Equal(t, id, desc.KeyPairs[0].KeyPairID)
		require.Len(t, desc.KeyPairs[0].Tags, 1, "DescribeKeyPairs reported no tagSet")
		assert.Equal(t, "Env", desc.KeyPairs[0].Tags[0].Key)
	})

	t.Run("ImportKeyPair", func(t *testing.T) {
		ts := newEC2TestServer(t)
		var kp struct {
			KeyPairID string         `xml:"keyPairId"`
			Tags      []describedTag `xml:"tagSet>item"`
		}
		ec2FleetXML(t, ts, map[string]string{
			"Action":                          "ImportKeyPair",
			"KeyName":                         "imported-key",
			"PublicKeyMaterial":               "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIexample",
			"TagSpecification.1.ResourceType": "key-pair",
			"TagSpecification.1.Tag.1.Key":    "Env",
			"TagSpecification.1.Tag.1.Value":  "prod",
		}, &kp)
		require.NotEmpty(t, kp.KeyPairID)
		require.Len(t, kp.Tags, 1, "ImportKeyPair reported no tagSet")
		assert.Equal(t, "Env", kp.Tags[0].Key)
	})

	t.Run("CreatePlacementGroup", func(t *testing.T) {
		ts := newEC2TestServer(t)
		params := map[string]string{}
		for k, v := range spec {
			params[k] = v
		}
		params["TagSpecification.1.ResourceType"] = "placement-group"
		id, tags := ec2TagTestPlacementGroup(t, ts, "tagged-pg", params)
		require.Len(t, tags, 1, "CreatePlacementGroup reported no tagSet")
		assert.Equal(t, "Env", tags[0].Key)

		var desc struct {
			Groups []struct {
				GroupID string         `xml:"groupId"`
				Tags    []describedTag `xml:"tagSet>item"`
			} `xml:"placementGroupSet>item"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribePlacementGroups"}, &desc)
		require.Len(t, desc.Groups, 1)
		assert.Equal(t, id, desc.Groups[0].GroupID)
		require.Len(t, desc.Groups[0].Tags, 1, "DescribePlacementGroups reported no tagSet")
		assert.Equal(t, "prod", desc.Groups[0].Tags[0].Value)
	})
}

// TestEC2_TagOnCreate_LaunchTemplateOwnTags covers the type whose own tags were settable
// by no path at all.
//
// CreateLaunchTemplate reads a TagSpecification inside LaunchTemplateData, which AWS
// scopes to the instances and volumes a *launch* from the template creates — a different
// question from tagging the template. The template's own tags come from a top-level
// TagSpecification with ResourceType=launch-template, which substrate did not read, while
// ec2LaunchTemplateSummary had always rendered a tagSet from a field nothing wrote.
func TestEC2_TagOnCreate_LaunchTemplateOwnTags(t *testing.T) {
	ts := newEC2TestServer(t)

	var created struct {
		ID   string         `xml:"launchTemplate>launchTemplateId"`
		Tags []describedTag `xml:"launchTemplate>tagSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                          "CreateLaunchTemplate",
		"LaunchTemplateName":              "tagged-lt",
		"LaunchTemplateData.ImageId":      "ami-0lt00000000000001",
		"LaunchTemplateData.InstanceType": "t3.micro",
		"TagSpecification.1.ResourceType": "launch-template",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	}, &created)
	require.NotEmpty(t, created.ID)
	require.Len(t, created.Tags, 1, "CreateLaunchTemplate reported no tagSet for its own tags")
	assert.Equal(t, "Env", created.Tags[0].Key)

	// The tags scoped to a launch stay where they were: a specification naming instance
	// belongs to LaunchTemplateData and must not become the template's own.
	var scoped struct {
		Tags []describedTag `xml:"launchTemplate>tagSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "instance-scoped-lt",
		"LaunchTemplateData.ImageId": "ami-0lt00000000000002",
		"LaunchTemplateData.TagSpecification.1.ResourceType": "instance",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "Env",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":  "prod",
	}, &scoped)
	assert.Empty(t, scoped.Tags,
		"a TagSpecification scoped to instance tagged the template itself")
}

// TestEC2_TagOnCreate_NoTagSpecificationOmitsTheElement pins the shape of the answer when
// no tags were asked for, which is not the same as an empty tagSet.
//
// AWS's CreatePlacementGroup Example 2 creates a group with no TagSpecification and its
// response carries no tagSet element at all, where Example 1 renders one — so the member
// is omitempty rather than always-present. An SDK tells an absent list from an empty one,
// and a consumer asserting "this resource has no tags" reads the difference.
func TestEC2_TagOnCreate_NoTagSpecificationOmitsTheElement(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{
			name: "CreatePlacementGroup",
			params: map[string]string{
				"Action": "CreatePlacementGroup", "GroupName": "bare-pg", "Strategy": "cluster",
			},
		},
		{
			name:   "CreateKeyPair",
			params: map[string]string{"Action": "CreateKeyPair", "KeyName": "bare-key"},
		},
		{
			name: "ImportKeyPair",
			params: map[string]string{
				"Action": "ImportKeyPair", "KeyName": "bare-import",
				"PublicKeyMaterial": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIexample",
			},
		},
		{
			name: "CreateLaunchTemplate",
			params: map[string]string{
				"Action": "CreateLaunchTemplate", "LaunchTemplateName": "bare-lt",
				"LaunchTemplateData.ImageId": "ami-0lt00000000000003",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			body := ec2RawXML(t, ts, tc.params)
			assert.NotContains(t, body, "<tagSet>",
				"a create carrying no TagSpecification rendered a tagSet element")
			// And the document still parses, so the omission is an absent element rather
			// than a malformed one.
			var doc struct{ XMLName xml.Name }
			require.NoError(t, xml.Unmarshal([]byte(body), &doc))
			assert.NotEmpty(t, doc.XMLName.Local)
		})
	}
}
