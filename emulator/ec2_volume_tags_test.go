package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #670's surface: a volume can be tagged at creation, reports its tags, and is
// findable by them.
//
// Through v0.104.0 an EBS volume was the one taggable EC2 resource with no tagging
// path at all. CreateVolume accepted TagSpecification.N and stored nothing;
// CreateTags on a vol- ID answered <return>true</return> and wrote nothing, because
// ec2TaggableStateKey had no vol- arm; DescribeVolumes rendered no tagSet and
// supported no tag filter. EC2Volume.Tags existed the whole time and nothing ever
// wrote to it — the store-and-hide shape, except that even the store was missing.
//
// Every assertion here goes through HTTP, because the defect was entirely on the
// wire: state carried a field the API could neither fill nor read.

// volTag is one tagSet>item entry.
type volTag struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// volTagMap collapses a tagSet to a map for order-independent comparison. AWS states
// outright that response element order may vary, so a test must not pin it.
func volTagMap(tags []volTag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		out[t.Key] = t.Value
	}
	return out
}

// volCreateResult is a CreateVolume response, parsed.
//
// TagSet is a pointer so the test can tell an absent element from a present-but-empty
// one: AWS's Example 2 emits <tagSet/> for a volume created with no tags, so absent is
// the wrong answer and only a pointer distinguishes the two.
type volCreateResult struct {
	VolumeID   string    `xml:"volumeId"`
	Size       int       `xml:"size"`
	VolumeType string    `xml:"volumeType"`
	IOPS       int       `xml:"iops"`
	Throughput int       `xml:"throughput"`
	TagSet     *struct { //nolint:govet // Shape mirrors the wire, not field alignment.
		Items []volTag `xml:"item"`
	} `xml:"tagSet"`
}

// volCreate calls CreateVolume and returns the parsed response.
func volCreate(t *testing.T, ts *httptest.Server, params map[string]string) volCreateResult {
	t.Helper()
	all := map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "20",
	}
	for k, v := range params {
		all[k] = v
	}
	resp := ec2Request(t, ts, all)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed volCreateResult
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	require.NotEmpty(t, parsed.VolumeID)
	return parsed
}

// volDescribe calls DescribeVolumes and returns each volume's ID with its tags,
// sorted by volume ID so an assertion does not depend on state-listing order.
func volDescribe(t *testing.T, ts *httptest.Server, params map[string]string) []struct {
	ID   string
	Tags map[string]string
} {
	t.Helper()
	all := map[string]string{"Action": "DescribeVolumes"}
	for k, v := range params {
		all[k] = v
	}
	resp := ec2Request(t, ts, all)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Items []struct {
			VolumeID string   `xml:"volumeId"`
			Tags     []volTag `xml:"tagSet>item"`
		} `xml:"volumeSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	out := make([]struct {
		ID   string
		Tags map[string]string
	}, 0, len(parsed.Items))
	for _, item := range parsed.Items {
		out = append(out, struct {
			ID   string
			Tags map[string]string
		}{ID: item.VolumeID, Tags: volTagMap(item.Tags)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// volIDs returns just the volume IDs volDescribe reported.
func volIDs(described []struct {
	ID   string
	Tags map[string]string
}) []string {
	ids := make([]string, 0, len(described))
	for _, v := range described {
		ids = append(ids, v.ID)
	}
	return ids
}

// ec2InstanceTagMap returns the tags DescribeInstances reports for one instance, so a
// volume-scoped assertion can also prove the instance scope was not affected.
func ec2InstanceTagMap(t *testing.T, ts *httptest.Server, instanceID string) map[string]string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instanceID,
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Instances []struct {
			Tags []volTag `xml:"tagSet>item"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	require.Len(t, parsed.Instances, 1)
	return volTagMap(parsed.Instances[0].Tags)
}

// ec2InstanceIDs returns every instance DescribeInstances reports, which is how a
// refusal is shown to have written nothing.
func ec2InstanceIDs(t *testing.T, ts *httptest.Server) []string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		IDs []string `xml:"reservationSet>item>instancesSet>item>instanceId"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	return parsed.IDs
}

// TestEC2_CreateVolume_TagOnCreate is the wire shape AWS's own Example 3 documents:
// TagSpecification.1.ResourceType=volume with Tag.1.Key/Value, answered with a
// populated tagSet.
//
// The parser is the one ec2LaunchTagsForResource already uses for RunInstances,
// CreateFleet, CreateImage and CreateNatGateway, called with one different argument —
// the wire shape is byte-identical, so a second walk would only be a second thing to
// drift.
func TestEC2_CreateVolume_TagOnCreate(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := volCreate(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "stack",
		"TagSpecification.1.Tag.1.Value":  "production",
		"TagSpecification.1.Tag.2.Key":    "Name",
		"TagSpecification.1.Tag.2.Value":  "data",
	})
	require.NotNil(t, created.TagSet, "CreateVolume rendered no tagSet element at all")
	assert.Equal(t, map[string]string{"stack": "production", "Name": "data"},
		volTagMap(created.TagSet.Items))

	// And they are stored, not merely echoed.
	described := volDescribe(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, described, 1)
	assert.Equal(t, map[string]string{"stack": "production", "Name": "data"}, described[0].Tags)
}

// TestEC2_CreateVolume_ScopeIsHonored pins that the resource-type scope is read
// rather than ignored. A specification scoped to something else contributes nothing,
// and one scoped to volume that follows it still applies — which is the skip-don't-stop
// rule ec2TagSpecificationTags already implements and this is the volume-side proof of.
func TestEC2_CreateVolume_ScopeIsHonored(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := volCreate(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "wrong",
		"TagSpecification.1.Tag.1.Value":  "scope",
		"TagSpecification.2.ResourceType": "volume",
		"TagSpecification.2.Tag.1.Key":    "right",
		"TagSpecification.2.Tag.1.Value":  "scope",
	})
	require.NotNil(t, created.TagSet)
	assert.Equal(t, map[string]string{"right": "scope"}, volTagMap(created.TagSet.Items))
}

// TestEC2_CreateVolume_EmptyTagSetIsPresent pins the present-but-empty element, which
// AWS's Example 2 shows verbatim for a volume created with no tags: <tagSet/>. An SDK
// maps a present-but-empty element to an empty slice and an omitted one to nil, so
// omitempty here would report "unknown" where AWS reports "none".
func TestEC2_CreateVolume_EmptyTagSetIsPresent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := volCreate(t, ts, nil)
	require.NotNil(t, created.TagSet, "an untagged volume must still render <tagSet/>")
	assert.Empty(t, created.TagSet.Items)

	described := volDescribe(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, described, 1)
	assert.Empty(t, described[0].Tags)
}

// TestEC2_CreateVolume_IopsAndThroughput closes a store-and-hide gap found in the same
// parser: EC2Volume carries IOPS and Throughput and DescribeVolumes renders both, but
// createVolume never read either parameter — so CreateVolume(Iops=…) and
// RunInstances(…Ebs.Iops=…) disagreed about the same field.
func TestEC2_CreateVolume_IopsAndThroughput(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := volCreate(t, ts, map[string]string{
		"VolumeType": "gp3",
		"Iops":       "4000",
		"Throughput": "250",
	})
	assert.Equal(t, 4000, created.IOPS)
	assert.Equal(t, 250, created.Throughput)

	vols := bdmDescribeVolumes(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, vols, 1)
	assert.Equal(t, 4000, vols[0].IOPS)
	assert.Equal(t, 250, vols[0].Throughput)
}

// TestEC2_CreateVolume_TagRulesRefused pins that tag-on-create here goes through the
// same restrictions every other tag-on-create path does, rather than being a second
// unrestricted door. AWS documents no volume-specific tag constraint, so the rules are
// exactly the resource-independent ones.
//
// The volume must not exist afterwards. A refusal that had already written the volume
// would leave an untagged volume behind for the next request in the same test to see,
// which is the rollback rule every other tag-on-create path follows.
func TestEC2_CreateVolume_TagRulesRefused(t *testing.T) {
	t.Parallel()
	manyTags := func(n int) map[string]string {
		params := map[string]string{"TagSpecification.1.ResourceType": "volume"}
		for i := 1; i <= n; i++ {
			params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
			params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Value"] = "v"
		}
		return params
	}
	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name: "a reserved key",
			params: map[string]string{
				"TagSpecification.1.ResourceType": "volume",
				"TagSpecification.1.Tag.1.Key":    "aws:cloudformation:stack-name",
				"TagSpecification.1.Tag.1.Value":  "mine",
			},
			code: "InvalidParameterValue",
		},
		{
			name: "an over-long key",
			params: map[string]string{
				"TagSpecification.1.ResourceType": "volume",
				"TagSpecification.1.Tag.1.Key":    strings.Repeat("k", 129),
				"TagSpecification.1.Tag.1.Value":  "v",
			},
			code: "InvalidParameterValue",
		},
		{
			name:   "more tags than the per-resource limit",
			params: manyTags(51),
			code:   "TagLimitExceeded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			all := map[string]string{
				"Action":           "CreateVolume",
				"AvailabilityZone": "us-east-1a",
				"Size":             "20",
			}
			for k, v := range tc.params {
				all[k] = v
			}
			status, code, msg := ec2ErrorDetail(t, ts, all)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code, "message: %s", msg)

			assert.Empty(t, volIDs(volDescribe(t, ts, nil)),
				"a refused CreateVolume left a volume behind")
		})
	}
}

// TestEC2_CreateVolume_TagLimitAllowsExactly50 is the boundary the limit check must not
// be off by one on: 50 user tags is legal.
func TestEC2_CreateVolume_TagLimitAllowsExactly50(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	params := map[string]string{"TagSpecification.1.ResourceType": "volume"}
	for i := 1; i <= 50; i++ {
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Value"] = "v"
	}
	created := volCreate(t, ts, params)
	require.NotNil(t, created.TagSet)
	assert.Len(t, created.TagSet.Items, 50)
}

// TestEC2_CreateTags_OnVolume is the plainest form of #670's report, and the one that
// answered 200 while writing nothing: ec2TaggableStateKey had arms for instances, VPCs,
// subnets, security groups, internet gateways, route tables, elastic IPs and NAT
// gateways, and none for a volume — so the ID fell through to the default and both
// applyTagsToResource and resourceTags treated it as an untaggable type.
//
// Nothing else needed changing for this: applyTagsToResource is type-agnostic, reading
// and writing the record's "tags" JSON member, and EC2Volume.Tags already serialized
// there. This test is what verifies that rather than asserting it.
func TestEC2_CreateTags_OnVolume(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := volCreate(t, ts, nil)

	resp := ec2Request(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": created.VolumeID,
		"Tag.1.Key":    "Env",
		"Tag.1.Value":  "prod",
		"Tag.2.Key":    "Owner",
		"Tag.2.Value":  "team-a",
	})
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	described := volDescribe(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, described, 1)
	assert.Equal(t, map[string]string{"Env": "prod", "Owner": "team-a"}, described[0].Tags)

	// DeleteTags reaches it through the same resolver, so it must too.
	del := ec2Request(t, ts, map[string]string{
		"Action":       "DeleteTags",
		"ResourceId.1": created.VolumeID,
		"Tag.1.Key":    "Env",
	})
	require.NoError(t, del.Body.Close())
	require.Equal(t, http.StatusOK, del.StatusCode)

	after := volDescribe(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, after, 1)
	assert.Equal(t, map[string]string{"Owner": "team-a"}, after[0].Tags)
}

// TestEC2_CreateTags_OnVolumeChecksTheLimit pins that a volume joined the *checked* set
// and not merely the written one: the tag limit is counted against the resource's
// existing tags, which requires resourceTags to resolve the same key
// applyTagsToResource writes to.
func TestEC2_CreateTags_OnVolumeChecksTheLimit(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	params := map[string]string{"TagSpecification.1.ResourceType": "volume"}
	for i := 1; i <= 50; i++ {
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Value"] = "v"
	}
	created := volCreate(t, ts, params)

	// A 51st distinct key does not fit.
	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": created.VolumeID,
		"Tag.1.Key":    "one-too-many",
		"Tag.1.Value":  "v",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "TagLimitExceeded", code)

	// Overwriting a key already on the volume adds nothing to the count, so it fits —
	// the post-merge count ec2CheckTagLimit documents, now reachable for a volume.
	ok := ec2Request(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": created.VolumeID,
		"Tag.1.Key":    "k1",
		"Tag.1.Value":  "overwritten",
	})
	require.NoError(t, ok.Body.Close())
	require.Equal(t, http.StatusOK, ok.StatusCode)

	described := volDescribe(t, ts, map[string]string{"VolumeId.1": created.VolumeID})
	require.Len(t, described, 1)
	assert.Equal(t, "overwritten", described[0].Tags["k1"])
}

// TestEC2_DescribeVolumes_TagFilters pins the two tag filters AWS's DescribeVolumes
// reference documents, and only those two: tag:<key> and tag-key. There is no
// tag-value filter on this operation — of its twenty documented filters those are the
// only two in the tag family — so an unknown name keeps falling through to the drop
// this operation has always done.
func TestEC2_DescribeVolumes_TagFilters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	prod := volCreate(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	dev := volCreate(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "dev",
	})
	untagged := volCreate(t, ts, nil)

	for _, tc := range []struct {
		name    string
		filters map[string]string
		want    []string
	}{
		{
			name: "tag:Env=prod matches the one volume",
			filters: map[string]string{
				"Filter.1.Name":    "tag:Env",
				"Filter.1.Value.1": "prod",
			},
			want: []string{prod.VolumeID},
		},
		{
			name: "a tag: filter's values are OR-combined",
			filters: map[string]string{
				"Filter.1.Name":    "tag:Env",
				"Filter.1.Value.1": "prod",
				"Filter.1.Value.2": "dev",
			},
			want: []string{dev.VolumeID, prod.VolumeID},
		},
		{
			name: "tag-key matches any value",
			filters: map[string]string{
				"Filter.1.Name":    "tag-key",
				"Filter.1.Value.1": "Env",
			},
			want: []string{dev.VolumeID, prod.VolumeID},
		},
		{
			name: "tag-key on a key nothing carries matches nothing",
			filters: map[string]string{
				"Filter.1.Name":    "tag-key",
				"Filter.1.Value.1": "Absent",
			},
			want: []string{},
		},
		{
			// Two filters are AND-combined, which is the rule that makes a tag filter
			// usable beside the volume-scoped ones this operation already had.
			name: "a tag filter combines with a volume-scoped one",
			filters: map[string]string{
				"Filter.1.Name":    "tag-key",
				"Filter.1.Value.1": "Env",
				"Filter.2.Name":    "volume-id",
				"Filter.2.Value.1": prod.VolumeID,
			},
			want: []string{prod.VolumeID},
		},
		{
			// Substrate's answer where AWS documents none: "You can't specify a filter
			// value of null" is all Using_Filtering says, and tag-key exists for the
			// any-value question. Matching nothing follows DescribeInstances.
			name: "a tag: filter with no value matches nothing",
			filters: map[string]string{
				"Filter.1.Name": "tag:Env",
			},
			want: []string{},
		},
		{
			// Unchanged: this operation drops a name it does not know rather than
			// refusing or matching nothing, and replacing the filter loop must not
			// change that.
			name: "an unknown filter name is still dropped",
			filters: map[string]string{
				"Filter.1.Name":    "not-a-filter",
				"Filter.1.Value.1": "x",
			},
			want: []string{dev.VolumeID, prod.VolumeID, untagged.VolumeID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := volIDs(volDescribe(t, ts, tc.filters))
			sort.Strings(tc.want)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestEC2_DescribeVolumes_ExistingFiltersStillWork guards the replacement of
// describeVolumes' hand-rolled filter loop with the shared extractEC2Filters walk. All
// nine names it already supported must keep matching exactly what they did, since a
// silent regression here would be invisible to the tag tests above.
func TestEC2_DescribeVolumes_ExistingFiltersStillWork(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":              "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize":          "30",
		"BlockDeviceMapping.2.DeviceName":              "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize":          "40",
		"BlockDeviceMapping.2.Ebs.VolumeType":          "gp3",
		"BlockDeviceMapping.2.Ebs.DeleteOnTermination": "true",
	})
	require.Len(t, ids, 1)

	for _, tc := range []struct {
		name    string
		filters map[string]string
		want    int
	}{
		{"attachment.instance-id", map[string]string{
			"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0]}, 2},
		{"attachment.device", map[string]string{
			"Filter.1.Name": "attachment.device", "Filter.1.Value.1": "/dev/sdf"}, 1},
		{"attachment.delete-on-termination", map[string]string{
			"Filter.1.Name": "attachment.delete-on-termination", "Filter.1.Value.1": "true"}, 2},
		{"status", map[string]string{
			"Filter.1.Name": "status", "Filter.1.Value.1": "in-use"}, 2},
		{"size", map[string]string{
			"Filter.1.Name": "size", "Filter.1.Value.1": "40"}, 1},
		{"volume-type", map[string]string{
			"Filter.1.Name": "volume-type", "Filter.1.Value.1": "gp3"}, 1},
		{"availability-zone", map[string]string{
			"Filter.1.Name": "availability-zone", "Filter.1.Value.1": "us-east-1a"}, 2},
		{"snapshot-id matches the empty snapshot nothing came from", map[string]string{
			"Filter.1.Name": "snapshot-id", "Filter.1.Value.1": "snap-absent"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Len(t, volDescribe(t, ts, tc.filters), tc.want)
		})
	}
}

// TestEC2_RunInstances_VolumeTagSpecifications is the launch-time half: a
// TagSpecification scoped to volume tags every volume the launch materializes, and
// nothing else.
//
// The two scopes are independent, which the assertion pins from both directions: the
// instance does not pick up the volume's tags and the volumes do not pick up the
// instance's. A launch's implicit root volume is tagged too — it is a volume the launch
// created, and AWS's own wording is "the tags to apply to the resources that are
// created during instance launch".
func TestEC2_RunInstances_VolumeTagSpecifications(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "40",
		"TagSpecification.1.ResourceType":     "instance",
		"TagSpecification.1.Tag.1.Key":        "Name",
		"TagSpecification.1.Tag.1.Value":      "web",
		"TagSpecification.2.ResourceType":     "volume",
		"TagSpecification.2.Tag.1.Key":        "Backup",
		"TagSpecification.2.Tag.1.Value":      "daily",
	})
	require.Len(t, ids, 1)

	// Both volumes — the declared /dev/sdf and the synthesized root — carry the
	// volume-scoped tag and only it.
	described := volDescribe(t, ts, map[string]string{
		"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0],
	})
	require.Len(t, described, 2)
	for _, vol := range described {
		assert.Equal(t, map[string]string{"Backup": "daily"}, vol.Tags,
			"volume %s", vol.ID)
	}

	// And the volume tag did not land on the instance.
	instTags := ec2InstanceTagMap(t, ts, ids[0])
	assert.Equal(t, map[string]string{"Name": "web"}, instTags)
}

// TestEC2_RunInstances_VolumeTagsAreFilterable is the round trip the issue is really
// about: tag at launch, then find the volume by that tag without knowing its ID.
func TestEC2_RunInstances_VolumeTagsAreFilterable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	bdmRunInstances(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "Role",
		"TagSpecification.1.Tag.1.Value":  "root",
	})
	found := volDescribe(t, ts, map[string]string{
		"Filter.1.Name": "tag:Role", "Filter.1.Value.1": "root",
	})
	require.Len(t, found, 1)
	assert.Equal(t, map[string]string{"Role": "root"}, found[0].Tags)
}

// TestEC2_RunInstances_VolumeTagRulesRefused pins that the volume scope is checked
// before the launch writes anything. The tags reach state through
// ec2VolumeFromMapping, inside the launch loop and after the instance's own Put, so a
// check made there would leave the instance — and on a multi-count launch, some of its
// siblings — behind.
func TestEC2_RunInstances_VolumeTagRulesRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-0abcdef1234567890",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "aws:reserved",
		"TagSpecification.1.Tag.1.Value":  "no",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code, "message: %s", msg)

	assert.Empty(t, volIDs(volDescribe(t, ts, nil)),
		"a refused launch materialized a volume")
	assert.Empty(t, ec2InstanceIDs(t, ts), "a refused launch left an instance behind")
}

// TestEC2_LaunchTemplate_VolumeTagSpecifications pins the template route, which is the
// only route by which a fleet launch can tag its volumes.
//
// The template's volume scope is a separately-named field rather than a widening of the
// existing flat instance-scoped one. Widening the persisted field would unmarshal every
// stored template into an element with an empty resource type and no tags, silently
// untagging the instances they already tag — the replay guarantee the emulator exists
// for. It is the same mitigation EC2LaunchTemplateData.NetworkInterfaces uses.
func TestEC2_LaunchTemplate_VolumeTagSpecifications(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "tagged-volumes",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.TagSpecification.1.ResourceType": "instance",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "Name",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":  "from-template",
		"LaunchTemplateData.TagSpecification.2.ResourceType": "volume",
		"LaunchTemplateData.TagSpecification.2.Tag.1.Key":    "Backup",
		"LaunchTemplateData.TagSpecification.2.Tag.1.Value":  "weekly",
	})
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ids := bdmRunInstances(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "tagged-volumes",
	})
	require.Len(t, ids, 1)

	described := volDescribe(t, ts, map[string]string{
		"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0],
	})
	require.Len(t, described, 1)
	assert.Equal(t, map[string]string{"Backup": "weekly"}, described[0].Tags)
	assert.Equal(t, map[string]string{"Name": "from-template"},
		ec2InstanceTagMap(t, ts, ids[0]))
}

// TestEC2_LaunchTemplate_VolumeTagsAreReplacedNotMerged pins the same all-or-nothing
// merge rule every other launch-template field follows: the request's own volume tags
// win outright rather than merging with the template's. AWS states only the general
// rule — "any additional parameters that you specify for the new instance overwrite the
// corresponding parameters included in the launch template" — and this is that rule
// applied to the volume scope, exactly as the instance scope already applies it.
func TestEC2_LaunchTemplate_VolumeTagsAreReplacedNotMerged(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "merge-check",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.TagSpecification.1.ResourceType": "volume",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "Backup",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":  "weekly",
		"LaunchTemplateData.TagSpecification.1.Tag.2.Key":    "Team",
		"LaunchTemplateData.TagSpecification.1.Tag.2.Value":  "infra",
	})
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ids := bdmRunInstances(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "merge-check",
		"TagSpecification.1.ResourceType":   "volume",
		"TagSpecification.1.Tag.1.Key":      "Backup",
		"TagSpecification.1.Tag.1.Value":    "hourly",
	})
	require.Len(t, ids, 1)

	described := volDescribe(t, ts, map[string]string{
		"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0],
	})
	require.Len(t, described, 1)
	assert.Equal(t, map[string]string{"Backup": "hourly"}, described[0].Tags,
		"the request's volume tags replace the template's rather than merging")
}
