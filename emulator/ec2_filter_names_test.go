package emulator_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One rule for a filter name EC2 does not document: refuse it (#687).
//
// Before this change the answer depended on which operation received the name — dropped
// on volumes, snapshots, security groups, route tables, NAT gateways, fleets and images;
// matched nothing on instances; refused on instance-type offerings. Real EC2 refuses. The
// table below covers one operation per former behavior class, which is what the issue
// asks for, plus the two cases the split into evaluated/accepted names creates: a
// documented name substrate cannot answer must *not* be refused, and an operation that
// documents no tag filter must refuse one.

// ec2FilterRefusal sends a one-filter describe and returns its status, code and message.
// No fixture is needed: the refusal is decided before the state scan, so it does not
// depend on any resource existing.
func ec2FilterRefusal(t *testing.T, action, name string) (int, string, string) {
	t.Helper()
	ts := newEC2TestServer(t)
	return ec2ErrorDetail(t, ts, map[string]string{
		"Action":           action,
		"Filter.1.Name":    name,
		"Filter.1.Value.1": "whatever",
	})
}

// TestEC2_FilterNames_UndocumentedNameRefused is the behavior change, one operation per
// former class.
func TestEC2_FilterNames_UndocumentedNameRefused(t *testing.T) {
	for _, tc := range []struct {
		action string
		filter string
		was    string
	}{
		{"DescribeInstances", "some-unknown-filter", "matched nothing"},
		{"DescribeVolumes", "not-a-filter", "dropped"},
		{"DescribeSnapshots", "vloume-id", "dropped"},
		{"DescribeImages", "ami-id", "dropped"},
		{"DescribeSecurityGroups", "groupname", "dropped"},
		{"DescribeRouteTables", "route.destination", "dropped"},
		{"DescribeNatGateways", "natgateway-id", "dropped"},
		// FleetId is a top-level parameter on this operation, never a filter name, so
		// the plausible-looking spelling is exactly the mistake worth refusing.
		{"DescribeFleets", "fleet-id", "dropped"},
		{"DescribeInstanceTypeOfferings", "location-type", "refused"},
	} {
		t.Run(tc.action+"/"+tc.filter, func(t *testing.T) {
			status, code, message := ec2FilterRefusal(t, tc.action, tc.filter)
			assert.Equal(t, http.StatusBadRequest, status,
				"%s previously %s this name", tc.action, tc.was)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, tc.filter,
				"the message must name the offending filter, which is the one thing a caller needs")
		})
	}
}

// TestEC2_FilterNames_CaseIsSignificant pins that a case variant of a real filter name is
// refused.
//
// AWS's Filter type states "Filter names are case-sensitive", so `VPC-Id` is not `vpc-id`
// and accepting it would be leniency past the model. This is also the most likely typo to
// reach substrate from hand-written code, so it is worth its own case.
func TestEC2_FilterNames_CaseIsSignificant(t *testing.T) {
	for _, name := range []string{"VPC-Id", "Tag-Key", "Instance-Id"} {
		t.Run(name, func(t *testing.T) {
			status, code, _ := ec2FilterRefusal(t, "DescribeInstances", name)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	}
}

// TestEC2_FilterNames_DocumentedButUnevaluatedIsAccepted is the other half of the rule,
// and the reason the spec carries two lists.
//
// Every name below is on its operation's reference page and is one substrate keeps no
// state to answer. Refusing them would be a false deny for filters real EC2 accepts, so
// they are accepted and inert — the honest gap, listed per operation in docs/services.md
// rather than left for a caller to discover.
func TestEC2_FilterNames_DocumentedButUnevaluatedIsAccepted(t *testing.T) {
	for _, tc := range []struct{ action, filter string }{
		{"DescribeInstances", "platform"},
		{"DescribeInstances", "network-interface.ipv6-addresses.is-primary-ipv6"},
		{"DescribeImages", "creation-date"},
		{"DescribeVolumes", "encrypted"},
		{"DescribeSnapshots", "volume-size"},
		{"DescribeSecurityGroups", "ip-permission.from-port"},
		{"DescribeRouteTables", "route.state"},
		{"DescribeNatGateways", "nat-gateway-id"},
		{"DescribeFleets", "replace-unhealthy-instances"},
	} {
		t.Run(tc.action+"/"+tc.filter, func(t *testing.T) {
			status, code, _ := ec2FilterRefusal(t, tc.action, tc.filter)
			assert.Equal(t, http.StatusOK, status,
				"%s documents %s, so refusing it would deny a filter real EC2 accepts",
				tc.action, tc.filter)
			assert.Empty(t, code)
		})
	}
}

// TestEC2_FilterNames_InertMeansEveryResource pins what "inert" answers, which the status
// code alone cannot show: an empty result set is also HTTP 200.
//
// DescribeInstances used to answer a documented-but-unevaluated name by matching *nothing*
// while DescribeVolumes matched *everything*, so the same filter emptied one operation and
// constrained neither on the other. Both now ignore it. Neither answer is real EC2's — it
// would apply the filter — but only one of them can be a single rule, and match-nothing is
// the worse half of the choice: it is indistinguishable from "the resource does not exist",
// so a consumer's wait loop polls forever instead of over-matching once.
func TestEC2_FilterNames_InertMeansEveryResource(t *testing.T) {
	ts := newEC2TestServer(t)
	want := ec2TagTestInstance(t, ts)

	for _, filter := range []string{"platform", "architecture", "root-device-type"} {
		t.Run(filter, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{
				"Action":           "DescribeInstances",
				"Filter.1.Name":    filter,
				"Filter.1.Value.1": "no-instance-could-match-this",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var doc struct {
				XMLName xml.Name `xml:"DescribeInstancesResponse"`
				IDs     []string `xml:"reservationSet>item>instancesSet>item>instanceId"`
			}
			require.NoError(t, xml.NewDecoder(resp.Body).Decode(&doc))
			assert.Equal(t, []string{want}, doc.IDs,
				"an inert filter constrains nothing, so every instance is still returned")
		})
	}
}

// TestEC2_FilterNames_TagFilterFollowsTheOperation pins that whether tag:<key> is accepted
// is per operation, not a package-wide assumption.
//
// DescribeFleets and DescribeInstanceTypeOfferings document no tag filter at all — not
// tag:<key> and not tag-key — even though a fleet carries tags and DescribeFleets renders
// them. So the same filter name is a bad request on one operation and valid on its
// neighbor, which is AWS's set rather than an omission in the transcription.
func TestEC2_FilterNames_TagFilterFollowsTheOperation(t *testing.T) {
	t.Run("refused where the operation documents no tag filter", func(t *testing.T) {
		for _, tc := range []struct{ action, filter string }{
			{"DescribeFleets", "tag:Env"},
			{"DescribeFleets", "tag-key"},
			{"DescribeInstanceTypeOfferings", "tag:Env"},
			{"DescribeInstanceTypeOfferings", "tag-key"},
		} {
			t.Run(tc.action+"/"+tc.filter, func(t *testing.T) {
				status, code, _ := ec2FilterRefusal(t, tc.action, tc.filter)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
			})
		}
	})

	t.Run("accepted where it is documented", func(t *testing.T) {
		for _, tc := range []struct{ action, filter string }{
			{"DescribeInstances", "tag:Env"},
			{"DescribeVolumes", "tag:Env"},
			{"DescribeImages", "tag-key"},
			{"DescribeSnapshots", "tag:Env"},
			{"DescribeRouteTables", "tag:Env"},
			{"DescribeNatGateways", "tag-key"},
		} {
			t.Run(tc.action+"/"+tc.filter, func(t *testing.T) {
				status, _, _ := ec2FilterRefusal(t, tc.action, tc.filter)
				assert.Equal(t, http.StatusOK, status)
			})
		}
	})
}

// TestEC2_FilterNames_RefusalIsDeterministic pins which of two undocumented names the
// refusal reports.
//
// The check this replaced iterated the filter *map*, so it named whichever bad filter Go's
// map order surfaced first — two replays of one recorded request could report different
// refusals, which the event log must never allow. Walking Filter.N in request order names
// the first offender instead. Repeating the request is what makes the test meaningful: a
// map-order implementation passes it only by luck, and not twenty times running.
func TestEC2_FilterNames_RefusalIsDeterministic(t *testing.T) {
	ts := newEC2TestServer(t)
	for i := 0; i < 20; i++ {
		_, code, message := ec2ErrorDetail(t, ts, map[string]string{
			"Action":           "DescribeInstances",
			"Filter.1.Name":    "aaa-not-a-filter",
			"Filter.1.Value.1": "x",
			"Filter.2.Name":    "zzz-not-a-filter",
			"Filter.2.Value.1": "y",
		})
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Contains(t, message, "aaa-not-a-filter",
			"the first filter in request order is the one reported")
		assert.NotContains(t, message, "zzz-not-a-filter")
	}
}

// TestEC2_FilterNames_UndocumentedNameRefusedBeforeTheScan pins the ordering.
//
// A refusal must not depend on whether any resource matched, which is the rule
// ec2IDFilter.unresolved states from the other side: a resource a filter excluded still
// counts as resolved. An empty account and a populated one must answer the same way, or a
// caller's first run would 400 and a later one would not.
func TestEC2_FilterNames_UndocumentedNameRefusedBeforeTheScan(t *testing.T) {
	ts := newEC2TestServer(t)
	ec2TagTestInstance(t, ts) // one running instance, so the scan has something to find

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "not-a-filter",
		"Filter.1.Value.1": "x",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
}
