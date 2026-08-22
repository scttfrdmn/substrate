package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The twelve describes that parsed no Filter.N at all before #695.
//
// Each accepted a Filter.N on the wire, read none of it, and answered with every resource
// in the region — the failure #685 fixed for subnets, twelve times over. These tests are
// written per operation rather than as one table because what each filter *means* is
// per operation: the same name reads a different member, and the split between the names
// substrate can evaluate and the documented ones it cannot is different on every page.
//
// Every assertion goes through HTTP, so the spec check, the Filter.N walk, the matcher and
// the renderer are all in the path. A matcher tested directly would pass while the handler
// still ignored the parameter, which is precisely the bug being fixed.

// ec2DescribeXML sends a describe and decodes its body into v, failing on any non-200.
func ec2DescribeXML(t *testing.T, ts *httptest.Server, params map[string]string, v any) {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(v))
}

// ec2DescribeBody sends a describe and returns its raw body, which is the only way to tell
// an absent element from an empty one — a decoder reports both as a zero value.
func ec2DescribeBody(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(raw)
}

// ec2OneFilter is the params for a describe carrying exactly one filter, which is the shape
// almost every case below needs.
func ec2OneFilter(action, name, value string) map[string]string {
	return map[string]string{
		"Action":           action,
		"Filter.1.Name":    name,
		"Filter.1.Value.1": value,
	}
}

// --- DescribeVpcs ---

// ec2TestVPC is a vpcSet item as a caller decodes it.
type ec2TestVPC struct {
	VpcID     string         `xml:"vpcId"`
	State     string         `xml:"state"`
	CIDRBlock string         `xml:"cidrBlock"`
	OwnerID   string         `xml:"ownerId"`
	IsDefault bool           `xml:"isDefault"`
	Tags      []describedTag `xml:"tagSet>item"`
}

func ec2DescribeVPCs(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestVPC {
	t.Helper()
	var doc struct {
		XMLName xml.Name     `xml:"DescribeVpcsResponse"`
		Items   []ec2TestVPC `xml:"vpcSet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

func ec2CreateVPC(t *testing.T, ts *httptest.Server, cidr string) string {
	t.Helper()
	var doc struct {
		XMLName xml.Name `xml:"CreateVpcResponse"`
		VpcID   string   `xml:"vpc>vpcId"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": cidr}, &doc)
	require.NotEmpty(t, doc.VpcID)
	return doc.VpcID
}

// ec2TagResource applies one tag through CreateTags, which is how every fixture here that
// needs a tagged resource gets one.
func ec2TagResource(t *testing.T, ts *httptest.Server, id, key, value string) {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": id,
		"Tag.1.Key":    key,
		"Tag.1.Value":  value,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestEC2_VPCFilters_EvaluatedNamesSelect pins the six filters DescribeVpcs can answer.
//
// Before #695 every row here returned both VPCs: a caller asking for "the VPC with CIDR
// 10.9.0.0/16" was handed its neighbors too, with nothing in the response to say the filter
// had been dropped.
func TestEC2_VPCFilters_EvaluatedNamesSelect(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	wanted := ec2CreateVPC(t, ts, "10.9.0.0/16")
	ec2CreateVPC(t, ts, "10.8.0.0/16")
	ec2TagResource(t, ts, wanted, "Env", "prod")

	owner := ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": wanted})[0].OwnerID
	require.NotEmpty(t, owner, "ownerId must be rendered for the owner-id filter to be readable")

	// state and owner-id are the same on both VPCs, so they pin that a *matching* filter
	// does not exclude — the complement of the single-selection rows below.
	assert.Len(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "state", "available")), 2)
	assert.Len(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "owner-id", owner)), 2)

	for _, tc := range []struct{ name, filter, value string }{
		{"cidr", "cidr", "10.9.0.0/16"},
		{"cidr wildcards", "cidr", "10.9.*"},
		{"vpc-id", "vpc-id", wanted},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", tc.filter, tc.value))
			ids := make([]string, 0, len(items))
			for _, v := range items {
				ids = append(ids, v.VpcID)
			}
			assert.Equal(t, []string{wanted}, ids,
				"%s=%s must select only the VPC it names", tc.filter, tc.value)
		})
	}
}

// TestEC2_VPCFilters_IsDefaultAndNonMatchingValues pins the two answers a caller most needs
// to be able to trust: a filter that matches nothing returns nothing, and is-default reads
// the stored flag rather than defaulting to true.
func TestEC2_VPCFilters_IsDefaultAndNonMatchingValues(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2CreateVPC(t, ts, "10.9.0.0/16")

	assert.Empty(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "cidr", "192.168.0.0/16")),
		"a filter no VPC matches selects nothing, not everything")
	assert.Empty(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "owner-id", "000000000001")),
		"another account owns no VPC here")
	assert.Empty(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "true")),
		"a created VPC is not the default one")
	assert.Len(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "false")), 1)
}

// TestEC2_VPCFilters_InertNameSelectsEverything pins that a documented name substrate cannot
// answer constrains nothing — the same rule [TestEC2_FilterNames_InertMeansEveryResource]
// pins for instances, on the operation whose inert list is longest.
func TestEC2_VPCFilters_InertNameSelectsEverything(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2CreateVPC(t, ts, "10.9.0.0/16")

	for _, filter := range []string{"dhcp-options-id", "cidr-block-association.cidr-block", "ipv6-cidr-block-association.state"} {
		t.Run(filter, func(t *testing.T) {
			items := ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", filter, "nothing-matches-this"))
			assert.Len(t, items, 1, "an inert filter is accepted and constrains nothing")
		})
	}
}

// TestEC2_DescribeVpcs_StateElementIsState pins the wire fix.
//
// Both CreateVpc and DescribeVpcs rendered the state as <vpcState>, where AWS's own samples
// for both operations render <state>. Nothing in the suite or the docs pinned it, so an SDK
// caller decoded State: "" from a VPC that was in fact available — and could not tell that
// from a VPC whose state substrate had failed to set.
func TestEC2_DescribeVpcs_StateElementIsState(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.9.0.0/16")

	items := ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID})
	require.Len(t, items, 1)
	assert.Equal(t, "available", items[0].State,
		"AWS's DescribeVpcs sample renders <state>available</state>")

	body := ec2DescribeBody(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID})
	assert.NotContains(t, body, "vpcState", "the old spelling is gone, not rendered alongside")
}

// TestEC2_CreateVpc_AgreesWithDescribe pins that the two operations render one VPC.
//
// They rendered different member subsets before #695 — neither carried an owner or a tagSet,
// and both misspelled the state element — which is the drift one shared renderer prevents,
// the reason #685 gave for [ec2SubnetItem].
func TestEC2_CreateVpc_AgreesWithDescribe(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var created struct {
		XMLName xml.Name   `xml:"CreateVpcResponse"`
		Vpc     ec2TestVPC `xml:"vpc"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.7.0.0/16"}, &created)

	// Asserted before the comparison, because equality alone is satisfied by both
	// responses being equally blank — which is what the two renderers did before #695.
	require.NotEmpty(t, created.Vpc.VpcID)
	require.Equal(t, "available", created.Vpc.State)
	require.Equal(t, "10.7.0.0/16", created.Vpc.CIDRBlock)
	require.NotEmpty(t, created.Vpc.OwnerID)

	described := ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": created.Vpc.VpcID})
	require.Len(t, described, 1)
	assert.Equal(t, created.Vpc, described[0],
		"the create response and the describe must describe the same VPC")
}

// TestEC2_DescribeVpcs_TagSetIsAbsentWhenUntagged pins this page's own convention.
//
// AWS's DescribeVpcs sample omits tagSet on an untagged VPC, so substrate omits it —
// unlike DescribeInternetGateways, whose sample publishes a present-but-empty <tagSet/> and
// which [TestEC2_DescribeInternetGateways_EmptySetsArePresent] pins the other way. Each
// response answers to the page that documents it (#708).
func TestEC2_DescribeVpcs_TagSetIsAbsentWhenUntagged(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	untagged := ec2CreateVPC(t, ts, "10.9.0.0/16")
	tagged := ec2CreateVPC(t, ts, "10.10.0.0/16")
	ec2TagResource(t, ts, tagged, "Env", "prod")

	assert.NotContains(t, ec2DescribeBody(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": untagged}),
		"tagSet", "an untagged VPC omits the element, as AWS's sample does")
	assert.Contains(t, ec2DescribeBody(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": tagged}),
		"<tagSet>", "a tagged VPC renders it")
}

// --- DescribeInternetGateways ---

// ec2TestIGW is an internetGatewaySet item as a caller decodes it.
type ec2TestIGW struct {
	InternetGatewayID string `xml:"internetGatewayId"`
	OwnerID           string `xml:"ownerId"`
	Attachments       []struct {
		State string `xml:"state"`
		VpcID string `xml:"vpcId"`
	} `xml:"attachmentSet>item"`
	Tags []describedTag `xml:"tagSet>item"`
}

func ec2DescribeIGWs(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestIGW {
	t.Helper()
	var doc struct {
		XMLName xml.Name     `xml:"DescribeInternetGatewaysResponse"`
		Items   []ec2TestIGW `xml:"internetGatewaySet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

func ec2CreateIGW(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	var doc struct {
		XMLName xml.Name `xml:"CreateInternetGatewayResponse"`
		ID      string   `xml:"internetGateway>internetGatewayId"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "CreateInternetGateway"}, &doc)
	require.NotEmpty(t, doc.ID)
	return doc.ID
}

// TestEC2_InternetGatewayFilters_AllSixEvaluated pins the one operation in this batch whose
// documented filter set substrate can answer completely.
//
// The response carried nothing but internetGatewayId before #695, so both halves of every
// row here are new: the filter, and the member it selects on being readable at all.
func TestEC2_InternetGatewayFilters_AllSixEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.9.0.0/16")
	attached := ec2CreateIGW(t, ts)
	detached := ec2CreateIGW(t, ts)
	ec2TagResource(t, ts, attached, "Env", "prod")

	resp := ec2Request(t, ts, map[string]string{
		"Action": "AttachInternetGateway", "InternetGatewayId": attached, "VpcId": vpcID,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	owner := ec2DescribeIGWs(t, ts, map[string]string{"Action": "DescribeInternetGateways"})[0].OwnerID
	require.NotEmpty(t, owner)

	for _, tc := range []struct{ name, filter, value string }{
		{"attachment.state", "attachment.state", "available"},
		{"attachment.vpc-id", "attachment.vpc-id", vpcID},
		{"internet-gateway-id", "internet-gateway-id", attached},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribeIGWs(t, ts, ec2OneFilter("DescribeInternetGateways", tc.filter, tc.value))
			require.Len(t, items, 1, "%s=%s selected %d gateways", tc.filter, tc.value, len(items))
			assert.Equal(t, attached, items[0].InternetGatewayID)
		})
	}

	t.Run("owner-id selects both", func(t *testing.T) {
		items := ec2DescribeIGWs(t, ts, ec2OneFilter("DescribeInternetGateways", "owner-id", owner))
		assert.Len(t, items, 2, "both gateways belong to the requesting account")
	})

	t.Run("an unattached gateway matches no attachment filter", func(t *testing.T) {
		// AWS: attachment.state is "Present only if a VPC is attached", so a gateway
		// with no attachment is not a match for any value of it — including the state
		// its future attachment would have.
		items := ec2DescribeIGWs(t, ts, ec2OneFilter("DescribeInternetGateways", "attachment.state", "available"))
		for _, igw := range items {
			assert.NotEqual(t, detached, igw.InternetGatewayID)
		}
	})
}

// TestEC2_DescribeInternetGateways_EmptySetsArePresent pins this page's convention, which is
// the opposite of DescribeVpcs'.
//
// AWS's DescribeInternetGateways sample renders <attachmentSet/> and <tagSet/> on an
// unattached, untagged gateway — present and empty, not absent. Substrate follows the page in
// front of it rather than a house rule, which is why the two operations differ here on
// purpose; a future sweep "for consistency" would break one of them against its own
// reference.
func TestEC2_DescribeInternetGateways_EmptySetsArePresent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	igwID := ec2CreateIGW(t, ts)

	body := ec2DescribeBody(t, ts, map[string]string{
		"Action": "DescribeInternetGateways", "InternetGatewayId.1": igwID,
	})
	assert.Contains(t, body, "attachmentSet", "AWS's sample carries the element even when empty")
	assert.Contains(t, body, "tagSet", "as it does for tagSet")
}

// TestEC2_CreateInternetGateway_AgreesWithDescribe pins that the two render one gateway.
func TestEC2_CreateInternetGateway_AgreesWithDescribe(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var created struct {
		XMLName xml.Name   `xml:"CreateInternetGatewayResponse"`
		IGW     ec2TestIGW `xml:"internetGateway"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "CreateInternetGateway"}, &created)

	// As in [TestEC2_CreateVpc_AgreesWithDescribe], the members are asserted present
	// first: two responses that both render nothing but the ID are trivially equal.
	require.NotEmpty(t, created.IGW.InternetGatewayID)
	require.NotEmpty(t, created.IGW.OwnerID)

	described := ec2DescribeIGWs(t, ts, map[string]string{
		"Action": "DescribeInternetGateways", "InternetGatewayId.1": created.IGW.InternetGatewayID,
	})
	require.Len(t, described, 1)
	assert.Equal(t, created.IGW, described[0])
}

// --- DescribeKeyPairs ---

// ec2TestKeyPair is a keySet item as a caller decodes it.
type ec2TestKeyPair struct {
	KeyPairID      string         `xml:"keyPairId"`
	KeyName        string         `xml:"keyName"`
	KeyFingerprint string         `xml:"keyFingerprint"`
	Tags           []describedTag `xml:"tagSet>item"`
}

func ec2DescribeKeyPairs(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestKeyPair {
	t.Helper()
	var doc struct {
		XMLName xml.Name         `xml:"DescribeKeyPairsResponse"`
		Items   []ec2TestKeyPair `xml:"keySet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

// TestEC2_KeyPairFilters_AllFiveEvaluated pins the five filters this page documents, all of
// which substrate can answer since #708 gave EC2KeyPair a Tags field.
func TestEC2_KeyPairFilters_AllFiveEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	wantedID, _ := ec2TagTestKeyPair(t, ts, "filter-wanted", map[string]string{
		"TagSpecification.1.ResourceType": "key-pair",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	ec2TagTestKeyPair(t, ts, "filter-other", nil)

	fingerprint := ec2DescribeKeyPairs(t, ts, map[string]string{
		"Action": "DescribeKeyPairs", "KeyPairId.1": wantedID,
	})[0].KeyFingerprint
	require.NotEmpty(t, fingerprint)

	for _, tc := range []struct{ name, filter, value string }{
		{"key-name", "key-name", "filter-wanted"},
		{"key-name wildcards", "key-name", "*wanted"},
		{"key-pair-id", "key-pair-id", wantedID},
		{"fingerprint", "fingerprint", fingerprint},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribeKeyPairs(t, ts, ec2OneFilter("DescribeKeyPairs", tc.filter, tc.value))
			require.Len(t, items, 1, "%s=%s selected %d key pairs", tc.filter, tc.value, len(items))
			assert.Equal(t, "filter-wanted", items[0].KeyName)
		})
	}
}

// TestEC2_KeyPairs_IdentitySelectorsUnion pins KeyPairId.N, which was read by nothing before
// #695, and the reading of a request that carries both identity lists.
//
// AWS documents no rule for the combination. Substrate unions them: a request naming a key
// pair by name and another by ID is answered with both, because AWS answers NotFound for a
// name or ID it cannot resolve — which only makes sense if each is expected in the response.
func TestEC2_KeyPairs_IdentitySelectorsUnion(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	byIDTarget, _ := ec2TagTestKeyPair(t, ts, "union-by-id", nil)
	ec2TagTestKeyPair(t, ts, "union-by-name", nil)
	ec2TagTestKeyPair(t, ts, "union-neither", nil)

	t.Run("KeyPairId.N alone selects", func(t *testing.T) {
		items := ec2DescribeKeyPairs(t, ts, map[string]string{
			"Action": "DescribeKeyPairs", "KeyPairId.1": byIDTarget,
		})
		require.Len(t, items, 1, "KeyPairId.N was ignored before #695, answering with every key pair")
		assert.Equal(t, "union-by-id", items[0].KeyName)
	})

	t.Run("both lists union", func(t *testing.T) {
		items := ec2DescribeKeyPairs(t, ts, map[string]string{
			"Action": "DescribeKeyPairs", "KeyPairId.1": byIDTarget, "KeyName.1": "union-by-name",
		})
		names := make([]string, 0, len(items))
		for _, kp := range items {
			names = append(names, kp.KeyName)
		}
		assert.ElementsMatch(t, []string{"union-by-id", "union-by-name"}, names)
	})
}

// --- DescribeAvailabilityZones ---

// ec2TestAZ is an availabilityZoneInfo item as a caller decodes it.
type ec2TestAZ struct {
	ZoneName   string `xml:"zoneName"`
	State      string `xml:"zoneState"`
	RegionName string `xml:"regionName"`
	ZoneID     string `xml:"zoneId"`
}

func ec2DescribeAZs(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestAZ {
	t.Helper()
	var doc struct {
		XMLName xml.Name    `xml:"DescribeAvailabilityZonesResponse"`
		Items   []ec2TestAZ `xml:"availabilityZoneInfo>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

// TestEC2_AvailabilityZoneSelectors pins every selector this operation documents and
// substrate can answer. The handler discarded the request entirely before #695 — its
// signature took `_ *AWSRequest` — so ZoneName.N, ZoneId.N and Filter.N were all ignored.
func TestEC2_AvailabilityZoneSelectors(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	all := ec2DescribeAZs(t, ts, map[string]string{"Action": "DescribeAvailabilityZones"})
	require.Greater(t, len(all), 1, "the fixture needs more than one zone for a filter to be visible")
	first := all[0]

	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{"ZoneName.N", map[string]string{"Action": "DescribeAvailabilityZones", "ZoneName.1": first.ZoneName}},
		{"ZoneId.N", map[string]string{"Action": "DescribeAvailabilityZones", "ZoneId.1": first.ZoneID}},
		{"zone-name filter", ec2OneFilter("DescribeAvailabilityZones", "zone-name", first.ZoneName)},
		{"zone-id filter", ec2OneFilter("DescribeAvailabilityZones", "zone-id", first.ZoneID)},
		{"zone-name wildcards", ec2OneFilter("DescribeAvailabilityZones", "zone-name", "*"+first.ZoneName[len(first.ZoneName)-2:])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribeAZs(t, ts, tc.params)
			require.Len(t, items, 1, "%s selected %d zones of %d", tc.name, len(items), len(all))
			assert.Equal(t, first, items[0])
		})
	}

	t.Run("region-name and state select every zone", func(t *testing.T) {
		byRegion := ec2DescribeAZs(t, ts, ec2OneFilter("DescribeAvailabilityZones", "region-name", first.RegionName))
		assert.Len(t, byRegion, len(all))
		byState := ec2DescribeAZs(t, ts, ec2OneFilter("DescribeAvailabilityZones", "state", "available"))
		assert.Len(t, byState, len(all), "substrate reports every seeded zone available")
	})

	t.Run("a non-matching value selects nothing", func(t *testing.T) {
		assert.Empty(t, ec2DescribeAZs(t, ts, ec2OneFilter("DescribeAvailabilityZones", "zone-name", "eu-west-9z")))
		assert.Empty(t, ec2DescribeAZs(t, ts, map[string]string{
			"Action": "DescribeAvailabilityZones", "ZoneId.1": "usw2-az9",
		}))
	})

	t.Run("an inert name selects everything", func(t *testing.T) {
		// substrate models no zone grouping or opt-in, so these are accepted and inert.
		for _, filter := range []string{"zone-type", "opt-in-status", "parent-zone-id", "group-name"} {
			items := ec2DescribeAZs(t, ts, ec2OneFilter("DescribeAvailabilityZones", filter, "no-zone-matches"))
			assert.Len(t, items, len(all), "%s is documented but inert", filter)
		}
	})
}

// --- DescribePlacementGroups ---

// ec2TestPlacementGroup is a placementGroupSet item as a caller decodes it.
type ec2TestPlacementGroup struct {
	GroupName string         `xml:"groupName"`
	GroupID   string         `xml:"groupId"`
	GroupARN  string         `xml:"groupArn"`
	Strategy  string         `xml:"strategy"`
	State     string         `xml:"state"`
	Tags      []describedTag `xml:"tagSet>item"`
}

func ec2DescribePlacementGroups(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestPlacementGroup {
	t.Helper()
	var doc struct {
		XMLName xml.Name                `xml:"DescribePlacementGroupsResponse"`
		Items   []ec2TestPlacementGroup `xml:"placementGroupSet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

// TestEC2_PlacementGroupFilters_SixOfSevenEvaluated pins the filters this page documents,
// including group-arn — which needs the ARN to be rendered before a caller can use it.
func TestEC2_PlacementGroupFilters_SixOfSevenEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	wantedID, _ := ec2TagTestPlacementGroup(t, ts, "filter-wanted-pg", map[string]string{
		"TagSpecification.1.ResourceType": "placement-group",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	ec2TagTestPlacementGroup(t, ts, "filter-other-pg", nil)

	wanted := ec2DescribePlacementGroups(t, ts, map[string]string{
		"Action": "DescribePlacementGroups", "GroupName.1": "filter-wanted-pg",
	})
	require.Len(t, wanted, 1)
	require.NotEmpty(t, wanted[0].GroupARN, "group-arn cannot be filtered on if it is never rendered")

	for _, tc := range []struct{ name, filter, value string }{
		{"group-name", "group-name", "filter-wanted-pg"},
		{"group-name wildcards", "group-name", "filter-wanted*"},
		{"group-arn", "group-arn", wanted[0].GroupARN},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribePlacementGroups(t, ts, ec2OneFilter("DescribePlacementGroups", tc.filter, tc.value))
			require.Len(t, items, 1, "%s=%s selected %d groups", tc.filter, tc.value, len(items))
			assert.Equal(t, "filter-wanted-pg", items[0].GroupName)
		})
	}

	t.Run("GroupId.N selects", func(t *testing.T) {
		items := ec2DescribePlacementGroups(t, ts, map[string]string{
			"Action": "DescribePlacementGroups", "GroupId.1": wantedID,
		})
		require.Len(t, items, 1, "GroupId.N was documented and read by nothing before #695")
		assert.Equal(t, "filter-wanted-pg", items[0].GroupName)
	})

	t.Run("state and strategy select both", func(t *testing.T) {
		assert.Len(t, ec2DescribePlacementGroups(t, ts,
			ec2OneFilter("DescribePlacementGroups", "state", "available")), 2)
		assert.Len(t, ec2DescribePlacementGroups(t, ts,
			ec2OneFilter("DescribePlacementGroups", "strategy", "spread")), 2)
	})

	t.Run("spread-level is inert", func(t *testing.T) {
		items := ec2DescribePlacementGroups(t, ts,
			ec2OneFilter("DescribePlacementGroups", "spread-level", "host"))
		assert.Len(t, items, 2, "substrate records no spread level, so the filter constrains nothing")
	})
}

// TestEC2_PlacementGroupARN_MatchesTheTaggableResolver pins that the group-arn filter and an
// IAM ArnEquals condition compare the same string.
//
// A placement group's ARN is by *name*, not by ID —
// arn:${Partition}:ec2:${Region}:${Account}:placement-group/${PlacementGroupName} — and two
// readers compose it: the filter, through ec2PlacementGroupARN, and the authorization
// resolver, through ec2Taggable.arn. #674 was exactly the defect of a filter and an
// authorization decision disagreeing about a resource's identity, so the spellings are pinned
// against each other here. ec2TagARN is the formula the authz suite asserts against, in
// [TestEC2_TagAuthz_ScopedDenyOnAnyNamedResourceBlocks].
func TestEC2_PlacementGroupARN_MatchesTheTaggableResolver(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2TagTestPlacementGroup(t, ts, "arn-shape-pg", nil)

	groups := ec2DescribePlacementGroups(t, ts, map[string]string{
		"Action": "DescribePlacementGroups", "GroupName.1": "arn-shape-pg",
	})
	require.Len(t, groups, 1)

	// The account is read back from the response rather than hardcoded, because the test
	// server takes it from the request's credentials.
	vpcOwner := ec2DescribeVPCs(t, ts, map[string]string{
		"Action": "DescribeVpcs", "VpcId.1": ec2CreateVPC(t, ts, "10.31.0.0/16"),
	})[0].OwnerID
	require.NotEmpty(t, vpcOwner)

	assert.Equal(t, "arn:aws:ec2:us-east-1:"+vpcOwner+":placement-group/arn-shape-pg", groups[0].GroupARN,
		"the ARN is by name and matches the template ec2Taggable.arn composes")
}

// --- DescribeAddresses ---

// ec2TestAddress is an addressesSet item as a caller decodes it.
type ec2TestAddress struct {
	AllocationID string         `xml:"allocationId"`
	PublicIP     string         `xml:"publicIp"`
	InstanceID   string         `xml:"instanceId"`
	Domain       string         `xml:"domain"`
	Tags         []describedTag `xml:"tagSet>item"`
}

func ec2DescribeAddresses(t *testing.T, ts *httptest.Server, params map[string]string) []ec2TestAddress {
	t.Helper()
	var doc struct {
		XMLName xml.Name         `xml:"DescribeAddressesResponse"`
		Items   []ec2TestAddress `xml:"addressesSet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

func ec2AllocateAddress(t *testing.T, ts *httptest.Server) (allocationID, publicIP string) {
	t.Helper()
	var doc struct {
		XMLName      xml.Name `xml:"AllocateAddressResponse"`
		AllocationID string   `xml:"allocationId"`
		PublicIP     string   `xml:"publicIp"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"}, &doc)
	require.NotEmpty(t, doc.AllocationID)
	require.NotEmpty(t, doc.PublicIP)
	return doc.AllocationID, doc.PublicIP
}

// TestEC2_AddressFilters_EvaluatedNamesSelect pins the filters DescribeAddresses can answer,
// and PublicIp.N, which was documented and read by nothing.
func TestEC2_AddressFilters_EvaluatedNamesSelect(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID := ec2TagTestInstance(t, ts)
	wantedAlloc, wantedIP := ec2AllocateAddress(t, ts)
	otherAlloc, otherIP := ec2AllocateAddress(t, ts)
	ec2TagResource(t, ts, wantedAlloc, "Env", "prod")

	resp := ec2Request(t, ts, map[string]string{
		"Action": "AssociateAddress", "AllocationId": wantedAlloc, "InstanceId": instanceID,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	for _, tc := range []struct{ name, filter, value string }{
		{"public-ip", "public-ip", wantedIP},
		{"allocation-id", "allocation-id", wantedAlloc},
		{"instance-id", "instance-id", instanceID},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := ec2DescribeAddresses(t, ts, ec2OneFilter("DescribeAddresses", tc.filter, tc.value))
			require.Len(t, items, 1, "%s=%s selected %d addresses", tc.filter, tc.value, len(items))
			assert.Equal(t, wantedAlloc, items[0].AllocationID)
		})
	}

	t.Run("an unassociated address matches no association filter", func(t *testing.T) {
		items := ec2DescribeAddresses(t, ts, ec2OneFilter("DescribeAddresses", "instance-id", instanceID))
		for _, a := range items {
			assert.NotEqual(t, otherAlloc, a.AllocationID,
				"instance-id compares the recorded instance, which is empty on an unassociated address")
		}
	})

	t.Run("PublicIp.N selects", func(t *testing.T) {
		items := ec2DescribeAddresses(t, ts, map[string]string{
			"Action": "DescribeAddresses", "PublicIp.1": otherIP,
		})
		require.Len(t, items, 1, "PublicIp.N answered with every address before #695")
		assert.Equal(t, otherAlloc, items[0].AllocationID)
	})

	t.Run("AllocationId.N and PublicIp.N union", func(t *testing.T) {
		items := ec2DescribeAddresses(t, ts, map[string]string{
			"Action": "DescribeAddresses", "AllocationId.1": wantedAlloc, "PublicIp.1": otherIP,
		})
		ids := make([]string, 0, len(items))
		for _, a := range items {
			ids = append(ids, a.AllocationID)
		}
		assert.ElementsMatch(t, []string{wantedAlloc, otherAlloc}, ids,
			"a request naming one address by allocation and another by IP is answered with both")
	})

	t.Run("an address named by IP does not make its own allocation ID unresolved", func(t *testing.T) {
		// The regression this guards: ec2IDFilter.match records that a requested
		// allocation ID resolved. If the union short-circuited past it, an address
		// selected by PublicIp.N would leave its allocation ID looking unseen and the
		// response would be InvalidAllocationID.NotFound instead of the address.
		items := ec2DescribeAddresses(t, ts, map[string]string{
			"Action": "DescribeAddresses", "AllocationId.1": wantedAlloc, "PublicIp.1": wantedIP,
		})
		require.Len(t, items, 1, "both selectors name the same address")
		assert.Equal(t, wantedAlloc, items[0].AllocationID)
	})

	t.Run("tagSet is rendered when the address is tagged", func(t *testing.T) {
		items := ec2DescribeAddresses(t, ts, map[string]string{
			"Action": "DescribeAddresses", "AllocationId.1": wantedAlloc,
		})
		require.Len(t, items, 1)
		require.Len(t, items[0].Tags, 1, "a tag:<key> filter is unreadable if tagSet is never rendered")
		assert.Equal(t, "Env", items[0].Tags[0].Key)

		untagged := ec2DescribeBody(t, ts, map[string]string{
			"Action": "DescribeAddresses", "AllocationId.1": otherAlloc,
		})
		assert.NotContains(t, untagged, "tagSet",
			"no sample on this page shows a tagSet on an untagged address")
	})
}

// --- DescribeRegions ---

// TestEC2_RegionFilters_AllThreeEvaluated pins the three filters this page documents.
func TestEC2_RegionFilters_AllThreeEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	type region struct {
		RegionName     string `xml:"regionName"`
		RegionEndpoint string `xml:"regionEndpoint"`
		OptInStatus    string `xml:"optInStatus"`
	}
	describe := func(params map[string]string) []region {
		var doc struct {
			XMLName xml.Name `xml:"DescribeRegionsResponse"`
			Items   []region `xml:"regionInfo>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeRegions"})
	require.Greater(t, len(all), 1)
	first := all[0]

	t.Run("region-name", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeRegions", "region-name", first.RegionName))
		require.Len(t, items, 1)
		assert.Equal(t, first, items[0])
	})

	t.Run("endpoint", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeRegions", "endpoint", first.RegionEndpoint))
		require.Len(t, items, 1)
		assert.Equal(t, first, items[0])
	})

	t.Run("opt-in-status", func(t *testing.T) {
		assert.Len(t, describe(ec2OneFilter("DescribeRegions", "opt-in-status", "opt-in-not-required")), len(all))
		assert.Empty(t, describe(ec2OneFilter("DescribeRegions", "opt-in-status", "opted-in")),
			"every seeded region is opt-in-not-required, so asking for an opted-in one selects nothing")
	})

	t.Run("wildcards", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeRegions", "region-name", "us-*"))
		assert.NotEmpty(t, items)
		for _, r := range items {
			assert.Contains(t, r.RegionName, "us-")
		}
	})
}

// --- DescribeInstanceTypes ---

// TestEC2_InstanceTypeFilters_FiveOfFiftySeven pins the filters the catalog can answer, which
// retires the filter half of TODO(#495).
func TestEC2_InstanceTypeFilters_FiveOfFiftySeven(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	type itype struct {
		InstanceType string   `xml:"instanceType"`
		VCpus        int      `xml:"vCpuInfo>defaultVCpus"`
		MemoryMiB    int      `xml:"memoryInfo>sizeInMiB"`
		Archs        []string `xml:"processorInfo>supportedArchitectures>item"`
		UsageClasses []string `xml:"supportedUsageClasses>item"`
	}
	describe := func(params map[string]string) []itype {
		var doc struct {
			XMLName xml.Name `xml:"DescribeInstanceTypesResponse"`
			Items   []itype  `xml:"instanceTypeSet>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeInstanceTypes"})
	require.Greater(t, len(all), 5)
	one := all[0]

	t.Run("instance-type", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeInstanceTypes", "instance-type", one.InstanceType))
		require.Len(t, items, 1)
		assert.Equal(t, one.InstanceType, items[0].InstanceType)
	})

	t.Run("instance-type wildcards", func(t *testing.T) {
		// AWS documents the wildcard on this filter itself: "for example c5.2xlarge or
		// c5*", the one place a wildcard appears beside a specific filter rather than in
		// the general rule.
		items := describe(ec2OneFilter("DescribeInstanceTypes", "instance-type", "c5*"))
		require.NotEmpty(t, items)
		for _, it := range items {
			assert.Contains(t, it.InstanceType, "c5")
		}
	})

	t.Run("vcpu-info.default-vcpus", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeInstanceTypes", "vcpu-info.default-vcpus", "2"))
		require.NotEmpty(t, items)
		for _, it := range items {
			assert.Equal(t, 2, it.VCpus)
		}
	})

	t.Run("memory-info.size-in-mib", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeInstanceTypes", "memory-info.size-in-mib", "4096"))
		require.NotEmpty(t, items)
		for _, it := range items {
			assert.Equal(t, 4096, it.MemoryMiB)
		}
	})

	t.Run("processor-info.supported-architecture", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeInstanceTypes", "processor-info.supported-architecture", "x86_64"))
		require.NotEmpty(t, items)
		for _, it := range items {
			assert.Contains(t, it.Archs, "x86_64")
		}
	})

	t.Run("supported-usage-class", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeInstanceTypes", "supported-usage-class", "spot"))
		require.NotEmpty(t, items)
		for _, it := range items {
			assert.Contains(t, it.UsageClasses, "spot")
		}
	})

	t.Run("no comparison operators", func(t *testing.T) {
		// AWS supports no greater-than/less-than on filters, so a numeric filter is a
		// string match: "4097" selects nothing rather than "more than 4096".
		assert.Empty(t, describe(ec2OneFilter("DescribeInstanceTypes", "memory-info.size-in-mib", "4097")))
	})

	t.Run("an inert name selects everything", func(t *testing.T) {
		for _, filter := range []string{"bare-metal", "hypervisor", "network-info.ipv6-supported"} {
			items := describe(ec2OneFilter("DescribeInstanceTypes", filter, "no-type-matches"))
			assert.Len(t, items, len(all), "%s is documented but inert", filter)
		}
	})
}

// --- DescribeSpotPriceHistory ---

// TestEC2_SpotPriceFilters_FiveOfSix pins the filters this page documents, and the
// ProductDescription.N index fix that rode along.
//
// The clock is frozen because the timestamp subtest below filters on a value read out of
// an earlier response, and the rendered value changes at every second boundary — see
// [newEC2TestServerFrozenClock].
func TestEC2_SpotPriceFilters_FiveOfSix(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	type price struct {
		InstanceType       string `xml:"instanceType"`
		ProductDescription string `xml:"productDescription"`
		SpotPrice          string `xml:"spotPrice"`
		Timestamp          string `xml:"timestamp"`
		AvailabilityZone   string `xml:"availabilityZone"`
	}
	describe := func(params map[string]string) []price {
		var doc struct {
			XMLName xml.Name `xml:"DescribeSpotPriceHistoryResponse"`
			Items   []price  `xml:"spotPriceHistorySet>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeSpotPriceHistory"})
	require.Greater(t, len(all), 1)
	one := all[0]

	t.Run("availability-zone", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeSpotPriceHistory", "availability-zone", one.AvailabilityZone))
		require.NotEmpty(t, items)
		for _, p := range items {
			assert.Equal(t, one.AvailabilityZone, p.AvailabilityZone)
		}
		assert.Less(t, len(items), len(all), "one zone's prices are a subset of every zone's")
	})

	t.Run("instance-type", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeSpotPriceHistory", "instance-type", one.InstanceType))
		require.NotEmpty(t, items)
		for _, p := range items {
			assert.Equal(t, one.InstanceType, p.InstanceType)
		}
	})

	t.Run("product-description", func(t *testing.T) {
		assert.Len(t, describe(ec2OneFilter("DescribeSpotPriceHistory", "product-description", "Linux/UNIX")), len(all))
		assert.Empty(t, describe(ec2OneFilter("DescribeSpotPriceHistory", "product-description", "Windows")),
			"substrate's catalog prices Linux/UNIX only")
	})

	t.Run("spot-price", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeSpotPriceHistory", "spot-price", one.SpotPrice))
		require.NotEmpty(t, items)
		for _, p := range items {
			assert.Equal(t, one.SpotPrice, p.SpotPrice)
		}
	})

	t.Run("timestamp matches the rendered value", func(t *testing.T) {
		// The filter compares what the response renders, which is RFC3339. AWS's own
		// description gives its example in a third format; comparing against the
		// rendered value is the only reading that lets a caller filter on something it
		// can read back.
		assert.Len(t, describe(ec2OneFilter("DescribeSpotPriceHistory", "timestamp", one.Timestamp)), len(all))
	})

	t.Run("availability-zone-id is inert", func(t *testing.T) {
		items := describe(ec2OneFilter("DescribeSpotPriceHistory", "availability-zone-id", "use1-az1"))
		assert.Len(t, items, len(all), "substrate records no zone ID against a price")
	})

	t.Run("ProductDescription.N is read at every index", func(t *testing.T) {
		// Only ProductDescription.1 was consulted before #695, so a caller asking for
		// two platforms was answered as if it had asked for the first.
		items := describe(map[string]string{
			"Action":                 "DescribeSpotPriceHistory",
			"ProductDescription.1":   "Windows",
			"ProductDescription.2":   "Linux/UNIX",
			"MaxResults":             "0",
			"AvailabilityZone":       one.AvailabilityZone,
			"InstanceType.1":         one.InstanceType,
			"ProductDescription.foo": "ignored",
		})
		assert.NotEmpty(t, items,
			"Linux/UNIX at index 2 must be read; before #695 only index 1 was")
	})
}

// --- DescribeLaunchTemplates ---

// TestEC2_LaunchTemplateFilters_AllFourEvaluated pins the four filters this page documents
// and the multi-index selector fix.
func TestEC2_LaunchTemplateFilters_AllFourEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	wantedID := newFleetLaunchTemplate(t, ts, "filter-wanted-lt")
	otherID := newFleetLaunchTemplate(t, ts, "filter-other-lt")
	thirdID := newFleetLaunchTemplate(t, ts, "filter-third-lt")
	ec2TagResource(t, ts, wantedID, "Env", "prod")

	type template struct {
		LaunchTemplateID   string         `xml:"launchTemplateId"`
		LaunchTemplateName string         `xml:"launchTemplateName"`
		CreateTime         string         `xml:"createTime"`
		Tags               []describedTag `xml:"tagSet>item"`
	}
	describe := func(params map[string]string) []template {
		var doc struct {
			XMLName xml.Name   `xml:"DescribeLaunchTemplatesResponse"`
			Items   []template `xml:"launchTemplates>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeLaunchTemplates"})
	require.Len(t, all, 3)
	createTime := all[0].CreateTime
	require.NotEmpty(t, createTime)

	for _, tc := range []struct{ name, filter, value string }{
		{"launch-template-name", "launch-template-name", "filter-wanted-lt"},
		{"launch-template-name wildcards", "launch-template-name", "*wanted*"},
		{"tag by key and value", "tag:Env", "prod"},
		{"tag-key", "tag-key", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := describe(ec2OneFilter("DescribeLaunchTemplates", tc.filter, tc.value))
			require.Len(t, items, 1, "%s=%s selected %d templates", tc.filter, tc.value, len(items))
			assert.Equal(t, "filter-wanted-lt", items[0].LaunchTemplateName)
		})
	}

	t.Run("create-time selects every template", func(t *testing.T) {
		assert.Len(t, describe(ec2OneFilter("DescribeLaunchTemplates", "create-time", createTime)), 3,
			"all three were created at the same simulated instant")
	})

	t.Run("every index of both identity lists is read", func(t *testing.T) {
		// Only LaunchTemplateId.1 and LaunchTemplateName.1 were consulted before #695,
		// so a caller naming three templates was answered about one — indistinguishable
		// from the other two not existing.
		items := describe(map[string]string{
			"Action":                 "DescribeLaunchTemplates",
			"LaunchTemplateId.1":     wantedID,
			"LaunchTemplateId.2":     otherID,
			"LaunchTemplateName.1":   "filter-third-lt",
			"LaunchTemplateName.foo": "ignored",
		})
		ids := make([]string, 0, len(items))
		for _, lt := range items {
			ids = append(ids, lt.LaunchTemplateID)
		}
		assert.ElementsMatch(t, []string{wantedID, otherID, thirdID}, ids)
	})

	t.Run("naming a template twice returns it once", func(t *testing.T) {
		items := describe(map[string]string{
			"Action":               "DescribeLaunchTemplates",
			"LaunchTemplateId.1":   wantedID,
			"LaunchTemplateName.1": "filter-wanted-lt",
		})
		assert.Len(t, items, 1, "the two spellings name one template, and AWS never doubles it")
	})

	t.Run("a filter still applies to a named template", func(t *testing.T) {
		items := describe(map[string]string{
			"Action":             "DescribeLaunchTemplates",
			"LaunchTemplateId.1": wantedID,
			"Filter.1.Name":      "tag-key",
			"Filter.1.Value.1":   "NotATagOnThis",
		})
		assert.Empty(t, items, "an ID list and a non-matching filter intersect, as on every other describe")
	})
}

// --- DescribeLaunchTemplateVersions ---

// TestEC2_LaunchTemplateVersionFilters_FourOfFourteen pins the filters this page documents
// that a version's stored data can answer, and that filtering happens before pagination.
func TestEC2_LaunchTemplateVersionFilters_FourOfFourteen(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "version-filters-lt")

	// A second version differing in both filterable data members, so each filter has
	// something to exclude.
	resp := ec2Request(t, ts, map[string]string{
		"Action":                            "CreateLaunchTemplateVersion",
		"LaunchTemplateId":                  ltID,
		"LaunchTemplateData.ImageId":        "ami-0000000000000beef",
		"LaunchTemplateData.InstanceType":   "m5.4xlarge",
		"LaunchTemplateData.KeyName":        "irrelevant",
		"LaunchTemplateData.EbsOptimized":   "false",
		"LaunchTemplateData.DisableApiStop": "false",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	type version struct {
		VersionNumber  int64  `xml:"versionNumber"`
		DefaultVersion bool   `xml:"defaultVersion"`
		CreateTime     string `xml:"createTime"`
		ImageID        string `xml:"launchTemplateData>imageId"`
		InstanceType   string `xml:"launchTemplateData>instanceType"`
	}
	describe := func(params map[string]string) []version {
		var doc struct {
			XMLName xml.Name  `xml:"DescribeLaunchTemplateVersionsResponse"`
			Items   []version `xml:"launchTemplateVersionSet>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID})
	require.Len(t, all, 2)
	first := all[0]

	t.Run("image-id", func(t *testing.T) {
		items := describe(map[string]string{
			"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
			"Filter.1.Name": "image-id", "Filter.1.Value.1": "ami-0000000000000beef",
		})
		require.Len(t, items, 1)
		assert.Equal(t, "ami-0000000000000beef", items[0].ImageID)
	})

	t.Run("instance-type", func(t *testing.T) {
		items := describe(map[string]string{
			"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
			"Filter.1.Name": "instance-type", "Filter.1.Value.1": "m5.4xlarge",
		})
		require.Len(t, items, 1)
		assert.Equal(t, "m5.4xlarge", items[0].InstanceType)
	})

	t.Run("is-default-version", func(t *testing.T) {
		items := describe(map[string]string{
			"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
			"Filter.1.Name": "is-default-version", "Filter.1.Value.1": "true",
		})
		require.Len(t, items, 1, "exactly one version is the default")
		assert.True(t, items[0].DefaultVersion)
	})

	t.Run("create-time", func(t *testing.T) {
		items := describe(map[string]string{
			"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
			"Filter.1.Name": "create-time", "Filter.1.Value.1": first.CreateTime,
		})
		assert.NotEmpty(t, items)
	})

	t.Run("an inert name selects every version", func(t *testing.T) {
		for _, filter := range []string{"ram-disk-id", "kernel-id", "iam-instance-profile"} {
			items := describe(map[string]string{
				"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
				"Filter.1.Name": filter, "Filter.1.Value.1": "no-version-matches",
			})
			assert.Len(t, items, 2, "%s is documented but inert", filter)
		}
	})

	t.Run("filters apply before pagination", func(t *testing.T) {
		// A page must hold MaxResults *matching* versions. Filtering after paging would
		// answer a MaxResults=1 request with an empty page and a next token, which reads
		// as "no version matches" to a caller that does not follow the token.
		var doc struct {
			XMLName   xml.Name  `xml:"DescribeLaunchTemplateVersionsResponse"`
			Items     []version `xml:"launchTemplateVersionSet>item"`
			NextToken string    `xml:"nextToken"`
		}
		ec2DescribeXML(t, ts, map[string]string{
			"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": ltID,
			"MaxResults":    "1",
			"Filter.1.Name": "instance-type", "Filter.1.Value.1": "m5.4xlarge",
		}, &doc)
		require.Len(t, doc.Items, 1)
		assert.Equal(t, "m5.4xlarge", doc.Items[0].InstanceType)
		assert.Empty(t, doc.NextToken, "one version matches, so the page is the whole answer")
	})

	t.Run("an undocumented name is refused before the template is resolved", func(t *testing.T) {
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":           "DescribeLaunchTemplateVersions",
			"LaunchTemplateId": "lt-0000000000000dead",
			"Filter.1.Name":    "not-a-filter",
			"Filter.1.Value.1": "x",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code,
			"the filter name is refused rather than the missing template being reported first")
	})
}

// --- DescribeInstanceStatus ---

// TestEC2_InstanceStatusFilters_ThreeOfEighteen pins the three filters this operation can
// answer, which it reads through DescribeInstances' matcher.
func TestEC2_InstanceStatusFilters_ThreeOfEighteen(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instanceID := ec2TagTestInstance(t, ts)

	type status struct {
		InstanceID       string `xml:"instanceId"`
		AvailabilityZone string `xml:"availabilityZone"`
		StateName        string `xml:"instanceState>name"`
		StateCode        int    `xml:"instanceState>code"`
	}
	describe := func(params map[string]string) []status {
		var doc struct {
			XMLName xml.Name `xml:"DescribeInstanceStatusResponse"`
			Items   []status `xml:"instanceStatusSet>item"`
		}
		ec2DescribeXML(t, ts, params, &doc)
		return doc.Items
	}

	all := describe(map[string]string{"Action": "DescribeInstanceStatus"})
	require.Len(t, all, 1)
	require.NotEmpty(t, all[0].AvailabilityZone,
		"the availability-zone filter is unreadable if the zone is never rendered")

	for _, tc := range []struct{ name, filter, value string }{
		{"instance-state-name", "instance-state-name", all[0].StateName},
		{"instance-state-code", "instance-state-code", "16"},
		{"availability-zone", "availability-zone", all[0].AvailabilityZone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := describe(ec2OneFilter("DescribeInstanceStatus", tc.filter, tc.value))
			require.Len(t, items, 1)
			assert.Equal(t, instanceID, items[0].InstanceID)
		})
	}

	t.Run("a non-matching value selects nothing", func(t *testing.T) {
		assert.Empty(t, describe(ec2OneFilter("DescribeInstanceStatus", "instance-state-name", "terminated")))
		assert.Empty(t, describe(ec2OneFilter("DescribeInstanceStatus", "availability-zone", "eu-west-9z")))
	})

	t.Run("the health and event families are inert", func(t *testing.T) {
		// Substrate models no status check, scheduled event or operator, so each of
		// these is accepted and constrains nothing. Promoting any of them to an
		// evaluated case on DescribeInstances would silently change this operation too,
		// which is the hazard describeInstanceStatus' doc comment records.
		for _, filter := range []string{
			"system-status.status", "instance-status.status", "event.code", "operator.managed",
			"availability-zone-id",
		} {
			items := describe(ec2OneFilter("DescribeInstanceStatus", filter, "nothing-matches"))
			assert.Len(t, items, 1, "%s is documented but inert", filter)
		}
	})
}
