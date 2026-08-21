package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #688's surface: DescribeTags, the operation whose whole job is finding resources by tag.
//
// Before this it reached the dispatcher's default arm and answered InvalidAction / HTTP 400
// while four bundled managed policies granted ec2:DescribeTags — a policy permitted an
// operation nothing served. Every assertion here goes through HTTP, because that is where the whole
// defect lived.

// describedTag is one DescribeTags tagSet>item.
type describedTag struct {
	ResourceID   string `xml:"resourceId"`
	ResourceType string `xml:"resourceType"`
	Key          string `xml:"key"`
	Value        string `xml:"value"`
}

// ec2DescribeTags sends DescribeTags with params and returns the tags and the next token.
func ec2DescribeTags(t *testing.T, ts *httptest.Server, params map[string]string) ([]describedTag, string) {
	t.Helper()
	full := map[string]string{"Action": "DescribeTags"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Tags      []describedTag `xml:"tagSet>item"`
		NextToken string         `xml:"nextToken"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Tags, doc.NextToken
}

// ec2TagKeysFor reduces the answer to "<resourceId>/<key>=<value>" strings, which is what a
// filter test asserts on: the identity of each tag, not the element order AWS says may vary.
func ec2TagKeysFor(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	tags, _ := ec2DescribeTags(t, ts, params)
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		out = append(out, tag.ResourceID+"/"+tag.Key+"="+tag.Value)
	}
	return out
}

// ec2SeedTaggedResources tags one instance and one VPC through CreateTags and returns their
// IDs. Two resource types, so a resource-type filter has something to exclude.
func ec2SeedTaggedResources(t *testing.T, ts *httptest.Server) (instID, vpcID string) {
	t.Helper()
	instID = ec2TagTestInstance(t, ts)

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.7.0.0/16"}, &vpc)
	vpcID = vpc.VPCID
	require.NotEmpty(t, vpcID)

	ec2FleetXML(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": instID,
		"Tag.1.Key":    "Name",
		"Tag.1.Value":  "webserver",
		"Tag.2.Key":    "Env",
		"Tag.2.Value":  "prod",
	}, nil)
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": vpcID,
		"Tag.1.Key":    "Env",
		"Tag.1.Value":  "staging",
		"Tag.2.Key":    "Empty",
		"Tag.2.Value":  "",
	}, nil)
	return instID, vpcID
}

// TestEC2_DescribeTags_ReportsEveryStoredTag is the fail-before: this request answered
// InvalidAction / 400 before #688, so nothing below could be asserted at all.
//
// It also pins the four members of AWS's TagDescription together, since resourceId and
// resourceType are the whole difference between this operation and reading a tagSet off each
// describe — a caller asks DescribeTags precisely because it does not know which resource
// carries the tag.
func TestEC2_DescribeTags_ReportsEveryStoredTag(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instID, vpcID := ec2SeedTaggedResources(t, ts)

	tags, next := ec2DescribeTags(t, ts, nil)
	assert.Empty(t, next, "four tags is one page, so the last page carries no token")

	byID := map[string][]describedTag{}
	for _, tag := range tags {
		byID[tag.ResourceID] = append(byID[tag.ResourceID], tag)
	}
	require.Len(t, byID[instID], 2, "both instance tags are reported")
	require.Len(t, byID[vpcID], 2, "both VPC tags are reported")

	for _, tag := range byID[instID] {
		assert.Equal(t, "instance", tag.ResourceType,
			"resourceType is AWS's TagSpecification spelling of the type")
	}
	for _, tag := range byID[vpcID] {
		assert.Equal(t, "vpc", tag.ResourceType)
	}
	assert.Contains(t, ec2TagKeysFor(t, ts, nil), vpcID+"/Empty=",
		"a tag whose value is the empty string is reported as one, not dropped")
}

// TestEC2_DescribeTags_FiltersNarrowTheAnswer walks each of the five filter names AWS
// documents, in both the exact and the wildcard form its Example 4 shows.
func TestEC2_DescribeTags_FiltersNarrowTheAnswer(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instID, vpcID := ec2SeedTaggedResources(t, ts)

	tests := []struct {
		name   string
		filter map[string]string
		want   []string
	}{
		{"key selects a key whatever its value", ec2Filter("key", "Env"),
			[]string{instID + "/Env=prod", vpcID + "/Env=staging"}},
		{"key values OR", ec2Filter("key", "Name", "Empty"),
			[]string{instID + "/Name=webserver", vpcID + "/Empty="}},
		{"resource-id selects one resource", ec2Filter("resource-id", vpcID),
			[]string{vpcID + "/Empty=", vpcID + "/Env=staging"}},
		{"resource-type excludes the other type", ec2Filter("resource-type", "vpc"),
			[]string{vpcID + "/Empty=", vpcID + "/Env=staging"}},
		{"resource-type matches nothing for an unstored type",
			ec2Filter("resource-type", "volume"), []string{}},
		{"value selects by value alone", ec2Filter("value", "staging"),
			[]string{vpcID + "/Env=staging"}},
		{"value matches an explicitly empty value", ec2Filter("value", ""),
			[]string{vpcID + "/Empty="}},
		{"tag:<key> needs the key and the value", ec2Filter("tag:Env", "prod"),
			[]string{instID + "/Env=prod"}},
		{"tag:<key> with a non-matching value selects nothing",
			ec2Filter("tag:Env", "absent"), []string{}},
		// AWS's Example 4, verbatim: "You can use wildcards with filters, so you could
		// specify the value as ?ebserver to find tags with the key webserver or Webserver."
		{"a ? wildcard matches one character", ec2Filter("value", "?ebserver"),
			[]string{instID + "/Name=webserver"}},
		{"a * wildcard matches a run", ec2Filter("value", "*ing"),
			[]string{vpcID + "/Env=staging"}},
		{"a wildcard applies to key too", ec2Filter("key", "E*"),
			[]string{instID + "/Env=prod", vpcID + "/Empty=", vpcID + "/Env=staging"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// ElementsMatch, not Equal: this table asserts which tags the filter
			// selects, and the deterministic order is pinned by the pagination test
			// instead — the one place the order is load-bearing.
			assert.ElementsMatch(t, tc.want, ec2TagKeysFor(t, ts, tc.filter))
		})
	}
}

// TestEC2_DescribeTags_FiltersAnd pins the AND-across-names, OR-within-values rule on the
// operation whose reference states it, using AWS's Example 5 shape: several filters, one of
// them multi-valued.
func TestEC2_DescribeTags_FiltersAnd(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instID, vpcID := ec2SeedTaggedResources(t, ts)

	assert.Equal(t, []string{instID + "/Env=prod"}, ec2TagKeysFor(t, ts, map[string]string{
		"Filter.1.Name":    "key",
		"Filter.1.Value.1": "Env",
		"Filter.2.Name":    "value",
		"Filter.2.Value.1": "prod",
		"Filter.2.Value.2": "dev",
	}), "key=Env AND value in (prod, dev) selects the instance tag alone")

	assert.Empty(t, ec2TagKeysFor(t, ts, map[string]string{
		"Filter.1.Name":    "resource-id",
		"Filter.1.Value.1": vpcID,
		"Filter.2.Name":    "value",
		"Filter.2.Value.1": "prod",
	}), "two filters that no single tag satisfies select nothing, rather than the union")
}

// TestEC2_DescribeTags_RefusesUndocumentedFilterNames pins #687's rule on this operation,
// and with it the distinction worth stating out loud: DescribeTags documents **no tag-key
// filter**, alone in the describe family, because its `key` filter already asks that
// question. So a caller reaching for the neighboring spelling is refused rather than
// silently given everything.
func TestEC2_DescribeTags_RefusesUndocumentedFilterNames(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedTaggedResources(t, ts)

	for _, name := range []string{"tag-key", "resource-typ", "tag", "instance-id"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			params := ec2Filter(name, "Env")
			params["Action"] = "DescribeTags"
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	}
}

// TestEC2_DescribeTags_Pagination covers the MaxResults range and the offset token.
//
// The ordering assertion is the load-bearing one: StateManager.List documents no ordering,
// so an offset token over an unordered list could skip or repeat a tag between pages, and
// two replays of one recorded request could answer differently.
func TestEC2_DescribeTags_Pagination(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	// Twelve tags on one instance, created in an order that is not the sorted order.
	params := map[string]string{"Action": "CreateTags", "ResourceId.1": instID}
	for i := 12; i >= 1; i-- {
		n := strconv.Itoa(i)
		params["Tag."+n+".Key"] = "k" + string(rune('a'+i-1))
		params["Tag."+n+".Value"] = "v" + n
	}
	ec2FleetXML(t, ts, params, nil)

	all, next := ec2DescribeTags(t, ts, nil)
	require.Len(t, all, 12)
	assert.Empty(t, next)
	keys := make([]string, 0, len(all))
	for _, tag := range all {
		keys = append(keys, tag.Key)
	}
	assert.Equal(t, []string{"ka", "kb", "kc", "kd", "ke", "kf", "kg", "kh", "ki", "kj", "kk", "kl"},
		keys, "one resource's tags come back sorted by key, whatever order they were written in")

	// Paged at AWS's minimum of five, the three pages concatenate to the same list.
	var paged []describedTag
	token := ""
	for page := 0; page < 3; page++ {
		req := map[string]string{"MaxResults": "5"}
		if token != "" {
			req["NextToken"] = token
		}
		var items []describedTag
		items, token = ec2DescribeTags(t, ts, req)
		paged = append(paged, items...)
		if page < 2 {
			assert.Len(t, items, 5)
			assert.NotEmpty(t, token, "a full page must carry a token")
		}
	}
	assert.Empty(t, token, "the last page carries no token, since AWS documents it as null")
	assert.Equal(t, all, paged, "the pages concatenate to the unpaged answer, in order")
}

// TestEC2_DescribeTags_RefusesBadPagination pins the two refusals, both of which are
// InvalidParameterValue: a MaxResults outside 5–1000, and a token that is not an offset.
//
// A value outside the range is refused rather than clamped — a caller who asked for 2000
// items asked for something the operation cannot do, and silently giving 1000 hides that.
func TestEC2_DescribeTags_RefusesBadPagination(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedTaggedResources(t, ts)

	tests := []struct {
		name   string
		params map[string]string
		want   bool
	}{
		{"MaxResults below AWS's minimum of 5", map[string]string{"MaxResults": "4"}, true},
		{"MaxResults above AWS's maximum of 1000", map[string]string{"MaxResults": "1001"}, true},
		{"MaxResults not a number", map[string]string{"MaxResults": "many"}, true},
		{"MaxResults at the minimum", map[string]string{"MaxResults": "5"}, false},
		{"MaxResults at the maximum", map[string]string{"MaxResults": "1000"}, false},
		{"a token that is not an offset", map[string]string{"NextToken": "abc"}, true},
		{"a negative offset", map[string]string{"NextToken": "-1"}, true},
		// Clamped, not refused: a caller resuming after a tag was deleted gets an empty
		// last page rather than an error.
		{"an offset past the end", map[string]string{"NextToken": "9999"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := map[string]string{"Action": "DescribeTags"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, code, _ := ec2ErrorDetail(t, ts, params)
			if tc.want {
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
				return
			}
			assert.Equal(t, http.StatusOK, status)
			assert.Empty(t, code)
		})
	}
}

// TestEC2_DescribeTags_ScanIsWiderThanCreateTags pins the asymmetry deliberately: a
// snapshot's tags are reported even though CreateTags cannot write them.
//
// ec2TaggableResource covers nine prefixes, and a snap- ID is not one of them (#689 adds the
// arm) — but CreateImage's TagSpecification writes a snapshot's tags anyway, so a
// DescribeTags that reported only the CreateTags-writable types would hide tags a caller had
// successfully applied. Reporting every tag substrate stores, however it was applied, is the
// operation's job.
func TestEC2_DescribeTags_ScanIsWiderThanCreateTags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snaps := ec2SeedTaggedSnapshots(t, ts, "alpha")

	assert.Equal(t, []string{snaps["alpha"] + "/Scope=alpha"},
		ec2TagKeysFor(t, ts, ec2Filter("resource-type", "snapshot")),
		"a snapshot tagged through CreateImage's TagSpecification is still reported")

	// And CreateTags on that same snapshot still writes nothing, so the two halves of the
	// asymmetry are pinned together and #689 has a test to change.
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": snaps["alpha"],
		"Tag.1.Key":    "Added",
		"Tag.1.Value":  "later",
	}, nil)
	assert.NotContains(t, ec2TagKeysFor(t, ts, ec2Filter("resource-type", "snapshot")),
		snaps["alpha"]+"/Added=later",
		"CreateTags has no snap- arm yet, so it silently writes nothing — #689")
}

// TestEC2_DescribeTags_EmptyWhenNothingIsTagged pins that an account with no tags answers an
// empty tagSet and a 200, not an error and not the untagged resources themselves.
func TestEC2_DescribeTags_EmptyWhenNothingIsTagged(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2TagTestInstance(t, ts)

	tags, next := ec2DescribeTags(t, ts, nil)
	assert.Empty(t, tags, "an untagged instance contributes no tags")
	assert.Empty(t, next)
}
