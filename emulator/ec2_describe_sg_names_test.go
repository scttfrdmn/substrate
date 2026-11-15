package emulator_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DescribeSecurityGroups narrows on GroupName.N (#749).
//
// AWS documents the parameter as "[Default VPC] The names of the security groups" and substrate
// read it nowhere, so a caller naming one group by name was answered about every group in the
// account — the superset failure #731 fixed for DescribeImages' ImageId.N, and the worse of the
// two failure directions, because an error is visible where a superset reads as a successful
// narrowing.
//
// Two decisions here are substrate's reading, and each has a test below that pins it rather than
// leaving it to be rediscovered:
//
//   - A name is matched **in the default VPC only**, as AWS documents it (#1491). Through
//     v0.127.0 it was matched account-wide.
//   - A name matching nothing answers an **empty set**, not InvalidGroup.NotFound. The
//     operation's Errors section is empty and EC2's client-error table describes that code as a
//     missing security group *ID*, so refusing would mean inventing wording AWS does not publish
//     for this operation.

// ec2CreateSG creates a security group and returns its ID.
func ec2CreateSG(t *testing.T, ts *httptest.Server, name, description, vpcID string) string {
	t.Helper()
	params := map[string]string{
		"Action":           "CreateSecurityGroup",
		"GroupName":        name,
		"GroupDescription": description,
	}
	if vpcID != "" {
		params["VpcId"] = vpcID
	}
	var doc struct {
		GroupID string `xml:"groupId"`
	}
	ec2FleetXML(t, ts, params, &doc)
	require.NotEmpty(t, doc.GroupID, "CreateSecurityGroup %s", name)
	return doc.GroupID
}

// ec2DescribeSGNames returns the group names DescribeSecurityGroups answers with, so a table
// row can assert on the selection rather than on the rendering.
func ec2DescribeSGNames(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	req := map[string]string{"Action": "DescribeSecurityGroups"}
	for k, v := range params {
		req[k] = v
	}
	var doc struct {
		Groups []struct {
			GroupName string `xml:"groupName"`
		} `xml:"securityGroupInfo>item"`
	}
	ec2FleetXML(t, ts, req, &doc)
	var names []string
	for _, g := range doc.Groups {
		names = append(names, g.GroupName)
	}
	return names
}

// TestEC2_DescribeSecurityGroups_GroupNameNarrows is the table the issue asks for: the name
// list selects, in both spellings, and unions with GroupId.N rather than intersecting it.
//
// The union is the same reading [ec2SelectedByEither] records for the five other paired identity
// selectors: the two lists are two spellings of one question, so intersecting them would answer
// with the empty set while both named groups exist.
func TestEC2_DescribeSecurityGroups_GroupNameNarrows(t *testing.T) {
	ts := newEC2TestServer(t)
	web := ec2CreateSG(t, ts, "web", "web tier", "")
	db := ec2CreateSG(t, ts, "db", "db tier", "")
	ec2CreateSG(t, ts, "cache", "cache tier", "")

	tests := []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{
			name:   "no selector answers every group, the default VPC's included",
			params: nil,
			want:   []string{"default", "web", "db", "cache"},
		},
		{
			name:   "one indexed name",
			params: map[string]string{"GroupName.1": "web"},
			want:   []string{"web"},
		},
		{
			name:   "the un-indexed spelling",
			params: map[string]string{"GroupName": "db"},
			want:   []string{"db"},
		},
		{
			name:   "two names",
			params: map[string]string{"GroupName.1": "web", "GroupName.2": "cache"},
			want:   []string{"web", "cache"},
		},
		{
			name:   "a name that matches nothing answers empty, not NotFound",
			params: map[string]string{"GroupName.1": "no-such-group"},
			want:   nil,
		},
		{
			name:   "one name of two matching answers only the match",
			params: map[string]string{"GroupName.1": "web", "GroupName.2": "no-such-group"},
			want:   []string{"web"},
		},
		{
			name:   "an ID alone still selects",
			params: map[string]string{"GroupId.1": db},
			want:   []string{"db"},
		},
		{
			name:   "the two lists union",
			params: map[string]string{"GroupId.1": web, "GroupName.1": "cache"},
			want:   []string{"web", "cache"},
		},
		{
			name:   "naming one group in both lists returns it once",
			params: map[string]string{"GroupId.1": web, "GroupName.1": "web"},
			want:   []string{"web"},
		},
		{
			name:   "a name AND a non-matching filter answers empty",
			params: map[string]string{"GroupName.1": "web", "Filter.1.Name": "group-name", "Filter.1.Value.1": "db"},
			want:   nil,
		},
		{
			name:   "a name AND a matching filter answers the group",
			params: map[string]string{"GroupName.1": "web", "Filter.1.Name": "group-name", "Filter.1.Value.1": "web"},
			want:   []string{"web"},
		},
		{
			name:   "the name is exact, never a glob",
			params: map[string]string{"GroupName.1": "we*"},
			want:   nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ec2DescribeSGNames(t, ts, tc.params))
		})
	}
}

// TestEC2_DescribeSecurityGroups_NameDoesNotWeakenTheIDContract is the half of the union that
// could have gone wrong silently: the name list must not launder an ID the walk never resolved.
//
// The ID half keeps its full contract — malformed before the walk, NotFound after it — and the
// name half has neither, so a request carrying both has to apply each rule to its own list. The
// ordering rows matter because AWS validates ID syntax before it looks anything up, which is why
// [ec2IDFilter.validate] runs ahead of the loop.
func TestEC2_DescribeSecurityGroups_NameDoesNotWeakenTheIDContract(t *testing.T) {
	ts := newEC2TestServer(t)
	ec2CreateSG(t, ts, "web", "web tier", "")

	tests := []struct {
		name   string
		params map[string]string
		status int
		code   string
	}{
		{
			name:   "an unknown ID beside a matching name is still NotFound",
			params: map[string]string{"GroupId.1": "sg-0000000000000dead", "GroupName.1": "web"},
			status: 400,
			code:   "InvalidGroup.NotFound",
		},
		{
			name:   "a malformed ID beside a matching name is still Malformed",
			params: map[string]string{"GroupId.1": "not-an-id", "GroupName.1": "web"},
			status: 400,
			code:   "InvalidGroupId.Malformed",
		},
		{
			name:   "malformed beats NotFound when both are named",
			params: map[string]string{"GroupId.1": "not-an-id", "GroupId.2": "sg-0000000000000dead"},
			status: 400,
			code:   "InvalidGroupId.Malformed",
		},
		{
			name:   "a malformed ID is refused even when the name selects nothing",
			params: map[string]string{"GroupId.1": "not-an-id", "GroupName.1": "no-such-group"},
			status: 400,
			code:   "InvalidGroupId.Malformed",
		},
		{
			name:   "a name matching nothing is not a refusal",
			params: map[string]string{"GroupName.1": "no-such-group"},
			status: 200,
			code:   "",
		},
		{
			name:   "a resolvable ID beside a name matching nothing succeeds",
			params: map[string]string{"GroupName.1": "no-such-group", "Filter.1.Name": "vpc-id", "Filter.1.Value.1": "vpc-x"},
			status: 200,
			code:   "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := map[string]string{"Action": "DescribeSecurityGroups"}
			for k, v := range tc.params {
				req[k] = v
			}
			status, code := ec2ErrorCode(t, ts, req)
			assert.Equal(t, tc.status, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

// TestEC2_DescribeSecurityGroups_NameIsScopedToTheDefaultVPC pins the scope decision (#1491).
//
// AWS scopes GroupName.N to the default VPC, "[Default VPC] The names of the security groups",
// and through v0.127.0 substrate matched account-wide, so one name could select groups in several
// VPCs. A group of the same name in a caller-made VPC is now selected by its ID, not its name.
func TestEC2_DescribeSecurityGroups_NameIsScopedToTheDefaultVPC(t *testing.T) {
	ts := newEC2TestServer(t)

	var vpcA struct {
		VpcID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"}, &vpcA)
	require.NotEmpty(t, vpcA.VpcID)

	inDefault := ec2CreateSG(t, ts, "shared", "in the default VPC", "")
	inA := ec2CreateSG(t, ts, "shared", "in vpc A", vpcA.VpcID)
	require.NotEqual(t, inDefault, inA, "one name in two VPCs is two groups")

	ids := func(params map[string]string) []string {
		var doc struct {
			Groups []struct {
				GroupID string `xml:"groupId"`
			} `xml:"securityGroupInfo>item"`
		}
		req := map[string]string{"Action": "DescribeSecurityGroups"}
		for k, v := range params {
			req[k] = v
		}
		ec2FleetXML(t, ts, req, &doc)
		out := []string{}
		for _, g := range doc.Groups {
			out = append(out, g.GroupID)
		}
		return out
	}
	assert.Equal(t, []string{inDefault}, ids(map[string]string{"GroupName.1": "shared"}),
		"a name selects in the default VPC only")
	assert.Equal(t, []string{inDefault}, ids(map[string]string{"GroupName.1": "SHARED"}),
		`"Security group names are not case-sensitive" (VPC User Guide)`)
	assert.Empty(t, ids(map[string]string{"Filter.1.Name": "group-name", "Filter.1.Value.1": "SHARED"}),
		`the group-name filter stays exact: "Filter values are case-sensitive"`)
	assert.ElementsMatch(t, []string{inDefault, inA}, ids(map[string]string{"GroupId.1": inA, "GroupName.1": "shared"}),
		"the other VPC's group is reachable by ID, and the two lists still union")

	ec2CreateSG(t, ts, "only-in-a", "in vpc A", vpcA.VpcID)
	assert.Empty(t, ec2DescribeSGNames(t, ts, map[string]string{"GroupName.1": "only-in-a"}),
		"a name held only outside the default VPC selects nothing")
}
