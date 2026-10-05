package emulator_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Redshift's share of the v0.122.0 batch audits: pagination and filters (#1195), required members
// (#1197), and the cluster shape (#1199). Every assertion is on the raw XML or on the refusal's code
// and status, never through a typed decode, which cannot see a member that should not be there.

// redshiftAuditPlugin returns a Redshift plugin over state, on a frozen clock, and the request
// context every call below uses.
func redshiftAuditPlugin(t *testing.T, state emulator.StateManager) (*emulator.RedshiftPlugin, *emulator.RequestContext) {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.RedshiftPlugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "RedshiftPlugin.Initialize")
	return p, &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-redshift-audit"}
}

// redshiftAuditOK issues action and requires a 200, returning the raw body.
func redshiftAuditOK(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, action string, params map[string]string) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, action, params))
	require.NoErrorf(t, err, "%s %v", action, params)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return string(resp.Body)
}

// redshiftAuditRefusal issues action and requires an AWS refusal, returning its code and status.
func redshiftAuditRefusal(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, action string, params map[string]string) (code string, status int, message string) {
	t.Helper()
	_, err := p.HandleRequest(ctx, redshiftRequest(t, action, params))
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "%s %v must be refused, got %v", action, params, err)
	return awsErr.Code, awsErr.HTTPStatus, awsErr.Message
}

// redshiftAuditCluster is a CreateCluster request carrying every member API_CreateCluster marks
// Required, for the cluster named id.
func redshiftAuditCluster(id string) map[string]string {
	return map[string]string{"ClusterIdentifier": id, "NodeType": "ra3.xlplus", "MasterUsername": "admin"}
}

var redshiftMarkerRE = regexp.MustCompile(`<Marker>([^<]*)</Marker>`)

// TestRedshiftAudit_EveryDescribePagesThroughAListingLargerThanOnePage is #1195's paging test, one
// subtest per describe. Twenty-five records at MaxRecords=20 must answer twenty and a Marker, then the
// remaining five and no Marker. Before #1195 every describe answered all twenty-five in one page with
// no Marker, so the first assertion fails against that tree.
func TestRedshiftAudit_EveryDescribePagesThroughAListingLargerThanOnePage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		describe, element string
		create            func(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, i int)
	}{
		{"DescribeClusters", "<ClusterIdentifier>", func(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, i int) {
			redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster(fmt.Sprintf("page-%02d", i)))
		}},
		{"DescribeClusterParameterGroups", "<ParameterGroupName>", func(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, i int) {
			redshiftAuditOK(t, p, ctx, "CreateClusterParameterGroup", map[string]string{
				"ParameterGroupName": fmt.Sprintf("page-%02d", i), "ParameterGroupFamily": "redshift-1.0", "Description": "d",
			})
		}},
		{"DescribeClusterSubnetGroups", "<ClusterSubnetGroupName>", func(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, i int) {
			redshiftAuditOK(t, p, ctx, "CreateClusterSubnetGroup", map[string]string{
				"ClusterSubnetGroupName": fmt.Sprintf("page-%02d", i), "Description": "d", "SubnetIds.SubnetIdentifier.1": "subnet-1",
			})
		}},
		{"DescribeClusterSnapshots", "<SnapshotIdentifier>", func(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, i int) {
			if i == 0 {
				redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster("snap-src"))
			}
			redshiftAuditOK(t, p, ctx, "CreateClusterSnapshot", map[string]string{
				"ClusterIdentifier": "snap-src", "SnapshotIdentifier": fmt.Sprintf("page-%02d", i),
			})
		}},
	} {
		t.Run(tc.describe, func(t *testing.T) {
			t.Parallel()
			p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
			for i := range 25 {
				tc.create(t, p, ctx, i)
			}

			first := redshiftAuditOK(t, p, ctx, tc.describe, map[string]string{"MaxRecords": "20"})
			require.Equalf(t, 20, strings.Count(first, tc.element), "page one of %s holds MaxRecords records: %s", tc.describe, first)
			m := redshiftMarkerRE.FindStringSubmatch(first)
			require.NotNilf(t, m, "page one of %s must carry a Marker: %s", tc.describe, first)
			assert.Contains(t, first, "page-19")
			assert.NotContains(t, first, "page-20")

			second := redshiftAuditOK(t, p, ctx, tc.describe, map[string]string{"MaxRecords": "20", "Marker": m[1]})
			assert.Equalf(t, 5, strings.Count(second, tc.element), "page two of %s holds the rest: %s", tc.describe, second)
			assert.Contains(t, second, "page-20")
			assert.NotContainsf(t, second, "<Marker>", "the last page of %s omits Marker rather than answering it empty", tc.describe)

			// The default page size is the published 100, so the whole listing fits in one page.
			all := redshiftAuditOK(t, p, ctx, tc.describe, nil)
			assert.Equal(t, 25, strings.Count(all, tc.element))
			assert.NotContains(t, all, "<Marker>")
		})
	}
}

// TestRedshiftAudit_PaginationRefusesWhatThePagesDoNotAccept covers #1195's range and token criteria.
// None of the four pages publishes a code for either, so both answer Redshift's Common Errors
// InvalidParameterValue/400. DescribeClusters' "either ClusterIdentifier or Marker, but not both" is
// InvalidParameterCombination/400 from the same page.
func TestRedshiftAudit_PaginationRefusesWhatThePagesDoNotAccept(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	for _, describe := range []string{"DescribeClusters", "DescribeClusterParameterGroups", "DescribeClusterSubnetGroups", "DescribeClusterSnapshots"} {
		for _, tc := range []struct {
			name   string
			params map[string]string
		}{
			{"MaxRecords below 20", map[string]string{"MaxRecords": "19"}},
			{"MaxRecords above 100", map[string]string{"MaxRecords": "101"}},
			{"MaxRecords not an integer", map[string]string{"MaxRecords": "twenty"}},
			{"a Marker substrate never issued", map[string]string{"Marker": "not base64!"}},
		} {
			t.Run(describe+"/"+tc.name, func(t *testing.T) {
				code, status, _ := redshiftAuditRefusal(t, p, ctx, describe, tc.params)
				assert.Equal(t, "InvalidParameterValue", code)
				assert.Equal(t, http.StatusBadRequest, status)
			})
		}
		for _, edge := range []string{"20", "100"} {
			t.Run(describe+"/MaxRecords "+edge+" is accepted", func(t *testing.T) {
				redshiftAuditOK(t, p, ctx, describe, map[string]string{"MaxRecords": edge})
			})
		}
	}
	t.Run("DescribeClusters/ClusterIdentifier and Marker together", func(t *testing.T) {
		code, status, _ := redshiftAuditRefusal(t, p, ctx, "DescribeClusters", map[string]string{
			"ClusterIdentifier": "x", "Marker": "eA==",
		})
		assert.Equal(t, "InvalidParameterCombination", code)
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

// TestRedshiftAudit_FiltersNarrow is #1195's filter test: each filter the describes publish and
// substrate holds a value for narrows the listing, and a single-resource filter naming nothing answers
// its page's fault at its page's status. Before #1195 the three group and snapshot describes read no
// parameter at all, so every narrowing assertion below fails against that tree.
func TestRedshiftAudit_FiltersNarrow(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	for _, id := range []string{"alpha", "beta"} {
		redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster(id))
		redshiftAuditOK(t, p, ctx, "CreateClusterParameterGroup", map[string]string{
			"ParameterGroupName": id + "-pg", "ParameterGroupFamily": "redshift-1.0", "Description": "d",
		})
		redshiftAuditOK(t, p, ctx, "CreateClusterSubnetGroup", map[string]string{
			"ClusterSubnetGroupName": id + "-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "subnet-" + id,
		})
		redshiftAuditOK(t, p, ctx, "CreateClusterSnapshot", map[string]string{"ClusterIdentifier": id, "SnapshotIdentifier": id + "-snap"})
	}
	// An orphan: a snapshot whose cluster has since been deleted.
	redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster("gone"))
	redshiftAuditOK(t, p, ctx, "CreateClusterSnapshot", map[string]string{"ClusterIdentifier": "gone", "SnapshotIdentifier": "gone-snap"})
	redshiftAuditOK(t, p, ctx, "DeleteCluster", map[string]string{"ClusterIdentifier": "gone", "SkipFinalClusterSnapshot": "true"})

	for _, tc := range []struct {
		name, describe string
		params         map[string]string
		want, notWant  []string
	}{
		{"cluster by identifier", "DescribeClusters", map[string]string{"ClusterIdentifier": "beta"}, []string{">beta<"}, []string{">alpha<"}},
		{"parameter group by name", "DescribeClusterParameterGroups", map[string]string{"ParameterGroupName": "alpha-pg"}, []string{"alpha-pg"}, []string{"beta-pg"}},
		{"parameter group by name, any case", "DescribeClusterParameterGroups", map[string]string{"ParameterGroupName": "ALPHA-PG"}, []string{"alpha-pg"}, []string{"beta-pg"}},
		{"subnet group by name", "DescribeClusterSubnetGroups", map[string]string{"ClusterSubnetGroupName": "beta-sg"}, []string{"beta-sg"}, []string{"alpha-sg"}},
		{"snapshots by cluster", "DescribeClusterSnapshots", map[string]string{"ClusterIdentifier": "alpha"}, []string{"alpha-snap"}, []string{"beta-snap", "gone-snap"}},
		{"snapshot by identifier", "DescribeClusterSnapshots", map[string]string{"SnapshotIdentifier": "beta-snap"}, []string{"beta-snap"}, []string{"alpha-snap"}},
		{"snapshot by ARN", "DescribeClusterSnapshots", map[string]string{"SnapshotArn": "arn:aws:redshift:us-east-1:123456789012:snapshot:alpha/alpha-snap"}, []string{"alpha-snap"}, []string{"beta-snap"}},
		{"snapshots by type", "DescribeClusterSnapshots", map[string]string{"SnapshotType": "automated"}, nil, []string{"alpha-snap", "beta-snap", "gone-snap"}},
		{"snapshots after their creation", "DescribeClusterSnapshots", map[string]string{"StartTime": "2023-11-14T22:13:21Z"}, nil, []string{"alpha-snap"}},
		{"snapshots at their creation", "DescribeClusterSnapshots", map[string]string{"StartTime": "2023-11-14T22:13:20Z", "EndTime": "2023-11-14T22:13:20Z"}, []string{"alpha-snap", "beta-snap"}, nil},
		{"snapshots before their creation", "DescribeClusterSnapshots", map[string]string{"EndTime": "2023-11-14T22:13:19Z"}, nil, []string{"alpha-snap"}},
		{"snapshots of another owner", "DescribeClusterSnapshots", map[string]string{"OwnerAccount": "999999999999"}, nil, []string{"alpha-snap"}},
		{"snapshots of the caller as owner", "DescribeClusterSnapshots", map[string]string{"OwnerAccount": "123456789012"}, []string{"alpha-snap", "gone-snap"}, nil},
		{"ClusterExists true, existing cluster", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "true", "ClusterIdentifier": "alpha"}, []string{"alpha-snap"}, []string{"beta-snap"}},
		{"ClusterExists true, deleted cluster", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "true", "ClusterIdentifier": "gone"}, nil, []string{"gone-snap"}},
		{"ClusterExists false, no cluster named: the orphans", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "false"}, []string{"gone-snap"}, []string{"alpha-snap", "beta-snap"}},
		{"ClusterExists false, deleted cluster named", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "false", "ClusterIdentifier": "gone"}, []string{"gone-snap"}, nil},
		{"ClusterExists false, existing cluster named", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "false", "ClusterIdentifier": "alpha"}, nil, []string{"alpha-snap"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := redshiftAuditOK(t, p, ctx, tc.describe, tc.params)
			for _, w := range tc.want {
				assert.Containsf(t, body, w, "%s %v", tc.describe, tc.params)
			}
			for _, nw := range tc.notWant {
				assert.NotContainsf(t, body, nw, "%s %v narrows", tc.describe, tc.params)
			}
		})
	}

	for _, tc := range []struct {
		name, describe string
		params         map[string]string
		code           string
		status         int
	}{
		{"an unknown parameter group", "DescribeClusterParameterGroups", map[string]string{"ParameterGroupName": "nope"}, "ClusterParameterGroupNotFound", http.StatusNotFound},
		{"an unknown subnet group", "DescribeClusterSubnetGroups", map[string]string{"ClusterSubnetGroupName": "nope"}, "ClusterSubnetGroupNotFoundFault", http.StatusBadRequest},
		{"an unknown snapshot", "DescribeClusterSnapshots", map[string]string{"SnapshotIdentifier": "nope"}, "ClusterSnapshotNotFound", http.StatusNotFound},
		{"an unknown cluster", "DescribeClusters", map[string]string{"ClusterIdentifier": "nope"}, "ClusterNotFound", http.StatusNotFound},
		{"SnapshotType outside its Valid Values", "DescribeClusterSnapshots", map[string]string{"SnapshotType": "weekly"}, "InvalidParameterValue", http.StatusBadRequest},
		{"a StartTime that is not ISO 8601", "DescribeClusterSnapshots", map[string]string{"StartTime": "yesterday"}, "InvalidParameterValue", http.StatusBadRequest},
		{"ClusterExists that is not a boolean", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "maybe"}, "InvalidParameterValue", http.StatusBadRequest},
		{"ClusterExists true with no ClusterIdentifier", "DescribeClusterSnapshots", map[string]string{"ClusterExists": "true"}, "MissingParameter", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, status, _ := redshiftAuditRefusal(t, p, ctx, tc.describe, tc.params)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.status, status)
		})
	}
}

// TestRedshiftAudit_ARequiredMemberIsRefusedRatherThanDefaulted is #1197's test. Each member the
// three create pages mark `Required: Yes` is omitted in turn, and each must answer MissingParameter/400
// naming the member. Before #1197 NodeType was defaulted to dc2.large, MasterUsername stored empty, and
// the groups recorded with no family, description or subnets, so every row here was a 200.
func TestRedshiftAudit_ARequiredMemberIsRefusedRatherThanDefaulted(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	complete := map[string]map[string]string{
		"CreateCluster":               redshiftAuditCluster("req"),
		"CreateClusterParameterGroup": {"ParameterGroupName": "req-pg", "ParameterGroupFamily": "redshift-1.0", "Description": "d"},
		"CreateClusterSubnetGroup":    {"ClusterSubnetGroupName": "req-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "subnet-1"},
	}
	for action, members := range map[string][]string{
		"CreateCluster":               {"ClusterIdentifier", "MasterUsername", "NodeType"},
		"CreateClusterParameterGroup": {"ParameterGroupName", "ParameterGroupFamily", "Description"},
		"CreateClusterSubnetGroup":    {"ClusterSubnetGroupName", "Description", "SubnetIds.SubnetIdentifier.1"},
	} {
		for _, member := range members {
			t.Run(action+"/without "+member, func(t *testing.T) {
				params := map[string]string{}
				for k, v := range complete[action] {
					if k != member {
						params[k] = v
					}
				}
				code, status, message := redshiftAuditRefusal(t, p, ctx, action, params)
				assert.Equal(t, "MissingParameter", code)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Contains(t, message, strings.TrimSuffix(member, ".SubnetIdentifier.1"), "the refusal names the member")
			})
		}
		t.Run(action+"/complete", func(t *testing.T) {
			redshiftAuditOK(t, p, ctx, action, complete[action])
		})
	}
}

// TestRedshiftAudit_ACheckedMemberHonorsItsPublishedConstraints covers #1197's fourth criterion:
// every member #1197 now checks has its published Valid Values or constraints enforced, or is declined
// in its handler's doc comment.
func TestRedshiftAudit_ACheckedMemberHonorsItsPublishedConstraints(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	with := func(base map[string]string, k, v string) map[string]string {
		out := map[string]string{}
		for bk, bv := range base {
			out[bk] = bv
		}
		out[k] = v
		return out
	}
	cluster := redshiftAuditCluster("constrained")
	pg := map[string]string{"ParameterGroupName": "ok-pg", "ParameterGroupFamily": "redshift-1.0", "Description": "d"}
	sg := map[string]string{"ClusterSubnetGroupName": "ok-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "subnet-1"}
	manySubnets := map[string]string{"ClusterSubnetGroupName": "many", "Description": "d"}
	for i := 1; i <= 21; i++ {
		manySubnets[fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i)] = fmt.Sprintf("subnet-%d", i)
	}
	for _, tc := range []struct {
		name, action string
		params       map[string]string
		code         string
	}{
		{"a NodeType outside its Valid Values", "CreateCluster", with(cluster, "NodeType", "dc1.large"), "InvalidParameterValue"},
		{"MasterUsername PUBLIC", "CreateCluster", with(cluster, "MasterUsername", "PUBLIC"), "InvalidParameterValue"},
		{"MasterUsername starting with a digit", "CreateCluster", with(cluster, "MasterUsername", "1admin"), "InvalidParameterValue"},
		{"MasterUsername with an uppercase letter", "CreateCluster", with(cluster, "MasterUsername", "Admin"), "InvalidParameterValue"},
		{"MasterUsername with a colon", "CreateCluster", with(cluster, "MasterUsername", "ad:min"), "InvalidParameterValue"},
		{"MasterUsername over 128 characters", "CreateCluster", with(cluster, "MasterUsername", "a"+strings.Repeat("b", 128)), "InvalidParameterValue"},
		{"a parameter group name starting with a digit", "CreateClusterParameterGroup", with(pg, "ParameterGroupName", "1pg"), "InvalidParameterValue"},
		{"a parameter group name ending in a hyphen", "CreateClusterParameterGroup", with(pg, "ParameterGroupName", "pg-"), "InvalidParameterValue"},
		{"a parameter group name with two hyphens", "CreateClusterParameterGroup", with(pg, "ParameterGroupName", "p--g"), "InvalidParameterValue"},
		{"a parameter group name with an underscore", "CreateClusterParameterGroup", with(pg, "ParameterGroupName", "p_g"), "InvalidParameterValue"},
		{"a subnet group named Default", "CreateClusterSubnetGroup", with(sg, "ClusterSubnetGroupName", "Default"), "InvalidParameterValue"},
		{"a subnet group name with an underscore", "CreateClusterSubnetGroup", with(sg, "ClusterSubnetGroupName", "s_g"), "InvalidParameterValue"},
		{"more than twenty subnets", "CreateClusterSubnetGroup", manySubnets, "ClusterSubnetQuotaExceededFault"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, status, _ := redshiftAuditRefusal(t, p, ctx, tc.action, tc.params)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}

	// The accepted edges: every published NodeType, and a MasterUsername using every allowed character.
	for _, nodeType := range []string{"dc2.large", "dc2.8xlarge", "rg.xlarge", "rg.4xlarge", "ra3.large", "ra3.xlplus", "ra3.4xlarge", "ra3.16xlarge"} {
		redshiftAuditOK(t, p, ctx, "CreateCluster", map[string]string{
			"ClusterIdentifier": "node-" + strings.ReplaceAll(nodeType, ".", "-"), "NodeType": nodeType, "MasterUsername": "a1_+.@-z",
		})
	}
}

// TestRedshiftAudit_AGroupNameInUseIsRefusedAndStoredLowerCase covers the two published codes that had
// no site, ClusterParameterGroupAlreadyExists and ClusterSubnetGroupAlreadyExists, and the pages'
// "stored as a lower-case string". Before this, a second create silently overwrote the first.
func TestRedshiftAudit_AGroupNameInUseIsRefusedAndStoredLowerCase(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	pg := map[string]string{"ParameterGroupName": "MixedPG", "ParameterGroupFamily": "redshift-1.0", "Description": "first"}
	sg := map[string]string{"ClusterSubnetGroupName": "MixedSG", "Description": "first", "SubnetIds.SubnetIdentifier.1": "subnet-1"}

	assert.Contains(t, redshiftAuditOK(t, p, ctx, "CreateClusterParameterGroup", pg), "<ParameterGroupName>mixedpg</ParameterGroupName>")
	assert.Contains(t, redshiftAuditOK(t, p, ctx, "CreateClusterSubnetGroup", sg), "<ClusterSubnetGroupName>mixedsg</ClusterSubnetGroupName>")

	for _, tc := range []struct{ action, code string }{
		{"CreateClusterParameterGroup", "ClusterParameterGroupAlreadyExists"},
		{"CreateClusterSubnetGroup", "ClusterSubnetGroupAlreadyExists"},
	} {
		params := pg
		if tc.action == "CreateClusterSubnetGroup" {
			params = sg
		}
		code, status, _ := redshiftAuditRefusal(t, p, ctx, tc.action, params)
		assert.Equal(t, tc.code, code)
		assert.Equal(t, http.StatusBadRequest, status)
	}
	assert.Contains(t, redshiftAuditOK(t, p, ctx, "DescribeClusterParameterGroups", nil), "<Description>first</Description>",
		"the refused second create left the first group as it was")
}

// TestRedshiftAudit_ASubnetGroupAnswersItsSubnetsAndNoRequestVpcID covers #1197's VpcId criterion:
// the handler read VpcId, a response member, off the request and never read the subnets the page
// requires. It now records the subnets and answers them, and a VpcId sent anyway is not echoed.
func TestRedshiftAudit_ASubnetGroupAnswersItsSubnetsAndNoRequestVpcID(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	params := map[string]string{
		"ClusterSubnetGroupName": "sg", "Description": "d", "VpcId": "vpc-from-the-request",
		"SubnetIds.SubnetIdentifier.1": "subnet-aaa", "SubnetIds.SubnetIdentifier.2": "subnet-bbb",
	}
	for _, body := range []string{
		redshiftAuditOK(t, p, ctx, "CreateClusterSubnetGroup", params),
		redshiftAuditOK(t, p, ctx, "DescribeClusterSubnetGroups", nil),
	} {
		assert.Contains(t, body, "<SubnetGroupStatus>Complete</SubnetGroupStatus>")
		assert.Contains(t, body, "<Subnets><Subnet><SubnetIdentifier>subnet-aaa</SubnetIdentifier><SubnetStatus>Active</SubnetStatus></Subnet>"+
			"<Subnet><SubnetIdentifier>subnet-bbb</SubnetIdentifier><SubnetStatus>Active</SubnetStatus></Subnet></Subnets>")
		assert.NotContains(t, body, "vpc-from-the-request", "VpcId is not a request parameter")
	}
}

// TestRedshiftAudit_AClusterAnswersNoNamespaceARNOfTheWrongResourceType covers #1199's
// ClusterNamespaceArn criterion. API_Cluster glosses it as the cluster's *namespace* ARN, and
// substrate filled it with the `:cluster:` ARN. It is now omitted, which the criterion allows.
func TestRedshiftAudit_AClusterAnswersNoNamespaceARNOfTheWrongResourceType(t *testing.T) {
	t.Parallel()
	p, ctx := redshiftAuditPlugin(t, emulator.NewMemoryStateManager())
	for _, body := range []string{
		redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster("ns")),
		redshiftAuditOK(t, p, ctx, "DescribeClusters", nil),
	} {
		assert.NotContains(t, body, "ClusterNamespaceArn")
		assert.NotContains(t, body, ":cluster:ns")
		assert.Contains(t, body, "<ClusterIdentifier>ns</ClusterIdentifier>", "presence anchor")
	}
}

// TestRedshiftAudit_AStoreFaultIsAnError asserts every store read and write the new paths make is
// returned as an error, never answered as a refusal, an absence or a success. A describe that skipped
// an unreadable record would report a not-found for a record that exists.
func TestRedshiftAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, action string
		params       map[string]string
		arm          func(*cfFaultStateManager)
	}{
		{"CreateCluster, existing-cluster read", "CreateCluster", redshiftAuditCluster("new"), func(m *cfFaultStateManager) { m.failGet = "cluster:" }},
		{"DescribeClusters, record read", "DescribeClusters", nil, func(m *cfFaultStateManager) { m.failGet = "cluster:" }},
		{"DescribeClusters, corrupt record", "DescribeClusters", nil, func(m *cfFaultStateManager) { m.corruptGet = "cluster:" }},
		{"CreateClusterParameterGroup, existing-group read", "CreateClusterParameterGroup",
			map[string]string{"ParameterGroupName": "new-pg", "ParameterGroupFamily": "f", "Description": "d"}, func(m *cfFaultStateManager) { m.failGet = "paramgroup:" }},
		{"CreateClusterParameterGroup, write", "CreateClusterParameterGroup",
			map[string]string{"ParameterGroupName": "new-pg", "ParameterGroupFamily": "f", "Description": "d"}, func(m *cfFaultStateManager) { m.failPut = "paramgroup:" }},
		{"DescribeClusterParameterGroups, record read", "DescribeClusterParameterGroups", nil, func(m *cfFaultStateManager) { m.failGet = "paramgroup:" }},
		{"CreateClusterSubnetGroup, existing-group read", "CreateClusterSubnetGroup",
			map[string]string{"ClusterSubnetGroupName": "new-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "s"}, func(m *cfFaultStateManager) { m.failGet = "subnetgroup:" }},
		{"CreateClusterSubnetGroup, write", "CreateClusterSubnetGroup",
			map[string]string{"ClusterSubnetGroupName": "new-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "s"}, func(m *cfFaultStateManager) { m.failPut = "subnetgroup:" }},
		{"DescribeClusterSubnetGroups, corrupt record", "DescribeClusterSubnetGroups", nil, func(m *cfFaultStateManager) { m.corruptGet = "subnetgroup:" }},
		{"DescribeClusterSnapshots, record read", "DescribeClusterSnapshots", nil, func(m *cfFaultStateManager) { m.failGet = "snapshot:" }},
		{"DescribeClusterSnapshots, the ClusterExists cluster read", "DescribeClusterSnapshots",
			map[string]string{"ClusterExists": "false"}, func(m *cfFaultStateManager) { m.failGet = "cluster:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p, ctx := redshiftAuditPlugin(t, fault)
			redshiftAuditOK(t, p, ctx, "CreateCluster", redshiftAuditCluster("seed"))
			redshiftAuditOK(t, p, ctx, "CreateClusterParameterGroup", map[string]string{"ParameterGroupName": "seed-pg", "ParameterGroupFamily": "f", "Description": "d"})
			redshiftAuditOK(t, p, ctx, "CreateClusterSubnetGroup", map[string]string{"ClusterSubnetGroupName": "seed-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "s"})
			redshiftAuditOK(t, p, ctx, "CreateClusterSnapshot", map[string]string{"ClusterIdentifier": "seed", "SnapshotIdentifier": "seed-snap"})

			tc.arm(fault)
			_, err := p.HandleRequest(ctx, redshiftRequest(t, tc.action, tc.params))
			require.Errorf(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
