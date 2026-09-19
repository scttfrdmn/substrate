package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DescribeInstanceTypes reported currentGeneration as a hardcoded true for every type in the
// seeded catalog, and the current-generation filter was parsed and dropped (#1028).
//
// Both halves had to move together, which is why they are asserted together here and in
// TestEC2_InstanceTypeFilters_SixOfFiftySix. A constant true is invisible behind an inert
// filter: a caller asking for Values=false got the whole catalog, every row of it claiming to be
// current, and nothing in the response contradicted anything else in it. Fixing only the value
// would have made the filter answer Values=true with the previous-generation types included — a
// self-contradiction inside one document — and fixing only the filter would have had it narrow on
// a constant.
//
// Provenance. AWS publishes the family list on the EC2 Instance Types guide's previous-generation
// page (https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html), whose Instance family
// table carries A1, C1, C3, C4, G3, I2, M1, M2, M3, M4, P3, P3dn, R3, R4 and T1 — with P3 spelled
// exactly p3.2xlarge | p3.8xlarge | p3.16xlarge. API_DescribeInstanceTypes itself says only
// "Indicates whether the instance type is current generation" and names no family, so the guide is
// the enumeration and the API model is the member.
//
// The other AWS list is not this one, which is worth knowing when re-checking these assertions:
// https://aws.amazon.com/ec2/previous-generation/ omits P3 and P3dn, names G2 where the guide
// names G3, adds C2, CR1 and HS1, and lists M4, R4 and D2 as upgrade targets. Read as authority it
// would classify every catalog family as current and leave the hardcoded true looking correct. It
// is a marketing page about hardware AWS is steering customers off, not a statement about what the
// API reports.

// ec2GenerationType is a DescribeInstanceTypes item reduced to the two members these cases read.
type ec2GenerationType struct {
	InstanceType      string `xml:"instanceType"`
	CurrentGeneration bool   `xml:"currentGeneration"`
}

// TestEC2_CurrentGenerationIsPerFamily asserts the value is derived from AWS's family table rather
// than hardcoded: p3's three sizes report false and every other catalog type reports true.
//
// No MaxResults is sent, deliberately — ec2MaxResults treats an absent MaxResults as no limit, so
// one request carries every type and the count below is the catalog's own rather than a page's.
//
// The total is asserted as well as the exceptions, because a per-type loop alone would pass
// against a constant false as readily as against the fix. Ninety-five is hard-coded rather than
// derived so that widening the catalog has to come past this test: a family added without a
// generation decision changes the total and fails here, which is the point of classifying by
// family name instead of by a p3 special case.
func TestEC2_CurrentGenerationIsPerFamily(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var doc struct {
		Items []ec2GenerationType `xml:"instanceTypeSet>item"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstanceTypes"}, &doc)
	require.Len(t, doc.Items, 95, "the seeded catalog is ninety-five types across eighteen families")

	var previous []string
	for _, it := range doc.Items {
		if !it.CurrentGeneration {
			previous = append(previous, it.InstanceType)
		}
	}
	assert.Equal(t, []string{"p3.2xlarge", "p3.8xlarge", "p3.16xlarge"}, previous,
		"p3 is the one cataloged family on AWS's previous-generation page, and it has three sizes")
}

// TestEC2_CurrentGenerationByFamily states the answer for each of the eighteen cataloged
// families, which is the granularity AWS publishes the property at.
//
// Only these eighteen are asserted, and the fourteen further names in AWS's table are not: a1, c1,
// c3, c4, g3, i2, m1, m2, m3, m4, p3dn, r3, r4 and t1 are families the catalog does not carry, so
// DescribeInstanceTypes has nothing to report for them and an assertion would be reading
// substrate's own copy of the table back to itself. They are transcribed in the source for the
// reason recorded there — a family added later is classified by AWS's answer rather than by
// whoever adds it — and this test covers the part that is observable through an API call.
//
// c5/c5a and m5/m5a are the cases worth stating explicitly. AWS lists C1, C3 and C4 as previous
// generation and c5 as neither, and M1 through M4 as previous and m5 as neither, so a prefix match
// on "c" or "m" — or a rule like "the low-numbered generations are previous" — would classify them
// wrongly. The family name is matched whole.
//
// The `instance-type` wildcard selects exactly one family here because the pattern carries the dot:
// `c5.*` cannot match `c5a.large`, and `p4d.*` cannot match `p4de.24xlarge`.
func TestEC2_CurrentGenerationByFamily(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for family, wantCurrent := range map[string]bool{
		"c5": true, "c5a": true, "g4dn": true, "g5": true, "g6": true,
		"inf1": true, "inf2": true, "m5": true, "m5a": true,
		"p3":  false,
		"p4d": true, "p4de": true, "p5": true, "r5": true,
		"t3": true, "t3a": true, "trn1": true, "trn2": true,
	} {
		var doc struct {
			Items []ec2GenerationType `xml:"instanceTypeSet>item"`
		}
		ec2DescribeXML(t, ts, ec2OneFilter("DescribeInstanceTypes", "instance-type", family+".*"), &doc)
		require.NotEmpty(t, doc.Items, "%s is in the catalog", family)
		for _, it := range doc.Items {
			assert.Equal(t, wantCurrent, it.CurrentGeneration, "%s", it.InstanceType)
		}
	}
}

// TestEC2_CurrentGenerationFalseIsRendered reads the raw body, which is the only way to tell a
// false currentGeneration from an absent one.
//
// A decoder reports both as false, so every decoded assertion above would pass against a member
// carrying omitempty — and an omitted element is a different answer: a caller could not
// distinguish "AWS reports this type as previous generation" from "substrate declined to say".
// AWS's response syntax carries the member for every type, so it is rendered for every type.
func TestEC2_CurrentGenerationFalseIsRendered(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	body := ec2DescribeBody(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "p3.8xlarge",
		"InstanceType.2": "p5.4xlarge",
	})
	assert.Contains(t, body, "<currentGeneration>false</currentGeneration>",
		"a previous-generation type renders the element, it does not omit it")
	assert.Contains(t, body, "<currentGeneration>true</currentGeneration>",
		"and the same response carries a current-generation type beside it")
}
