package emulator_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DescribeImages' tag filters (#686).
//
// describeImages parsed Filter.N itself rather than through extractEC2Filters, and the
// two disagreed in three ways that compounded:
//
//   - a tag: filter with no values matched any value, which is tag-key's job — and
//     tag-key itself was unsupported, so the operation had the wrong spelling of the
//     any-value question and not the right one;
//   - the hand-rolled walk stopped at the first empty Filter.N.Value.M, so an
//     explicitly empty filter value arrived as *no* values and silently became the
//     any-value form; and
//   - extractEC2Filters, which every other describe uses, overwrote a repeated filter
//     name instead of accumulating its values, so a caller who sent the same name twice
//     got only the last one applied and no indication the first was dropped.
//
// These tests pin all three, because the fix for the first is to adopt extractEC2Filters
// and the other two are what adopting it would otherwise change or carry over.

// ec2ImageFilterFixture registers two AMIs — one tagged Env=prod, one untagged — and
// returns their IDs. Both are needed for every case: a filter that wrongly matches
// everything and one that wrongly matches nothing are only distinguishable when the
// fixture holds an image that should be excluded.
func ec2ImageFilterFixture(t *testing.T, ts *httptest.Server) (tagged, untagged string) {
	t.Helper()
	instID := ec2TagTestInstance(t, ts)

	tagged = ec2CreateImageID(t, ts, map[string]string{
		"Action":                          "CreateImage",
		"InstanceId":                      instID,
		"Name":                            "tagged-ami",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	untagged = ec2CreateImageID(t, ts, map[string]string{
		"Action":     "CreateImage",
		"InstanceId": instID,
		"Name":       "untagged-ami",
	})
	return tagged, untagged
}

// ec2CreateImageID sends a CreateImage request and returns the AMI ID it reports.
func ec2CreateImageID(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	var res struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, params, &res)
	require.NotEmpty(t, res.ImageID)
	return res.ImageID
}

// ec2FilteredImageIDs returns the image IDs DescribeImages reports for params.
func ec2FilteredImageIDs(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	params["Action"] = "DescribeImages"
	var res struct {
		Images []struct {
			ImageID string `xml:"imageId"`
		} `xml:"imagesSet>item"`
	}
	ec2FleetXML(t, ts, params, &res)
	ids := make([]string, 0, len(res.Images))
	for _, im := range res.Images {
		ids = append(ids, im.ImageID)
	}
	return ids
}

// TestEC2_DescribeImages_ValuelessTagFilterMatchesNothing is the defect #686 reports.
//
// AWS documents no rule for a filter with no values — Using_Filtering says only that
// "You can't specify a filter value of null" — so matching nothing is substrate's
// reading, taken from ec2InstanceMatchesFilter and describeVolumes, which both already
// answer that way. What made this a defect is not the answer but the disagreement: the
// same filter meant two different things depending on which operation received it.
func TestEC2_DescribeImages_ValuelessTagFilterMatchesNothing(t *testing.T) {
	ts := newEC2TestServer(t)
	tagged, untagged := ec2ImageFilterFixture(t, ts)

	got := ec2FilteredImageIDs(t, ts, map[string]string{"Filter.1.Name": "tag:Env"})
	assert.Empty(t, got,
		"a tag: filter with no values matched any value, which is tag-key's job")
	assert.NotContains(t, got, tagged)
	assert.NotContains(t, got, untagged)
}

// TestEC2_DescribeImages_ExplicitEmptyTagValue pins the difference the hand-rolled walk
// erased: an empty filter value is a value, and it is not the same request as no value.
//
// tag:Env with Value.1 set to the empty string asks for an image whose Env tag is
// empty. The old walk broke out of its value loop on the first empty string, so the
// request arrived indistinguishable from the valueless form above and matched Env=prod.
func TestEC2_DescribeImages_ExplicitEmptyTagValue(t *testing.T) {
	ts := newEC2TestServer(t)
	ec2ImageFilterFixture(t, ts)

	got := ec2FilteredImageIDs(t, ts, map[string]string{
		"Filter.1.Name": "tag:Env", "Filter.1.Value.1": "",
	})
	assert.Empty(t, got, "tag:Env= asks for an empty Env value, and no image has one")
}

// TestEC2_DescribeImages_TagKeyFilter pins tag-key, which DescribeImages documents and
// substrate did not support.
//
// This is the filter the valueless-tag: form was standing in for, so it has to work
// before that form can stop working — otherwise the fix takes away the only way to ask
// the any-value question. An unsupported name was silently dropped, which made the
// filter a no-op and returned the untagged AMI too.
func TestEC2_DescribeImages_TagKeyFilter(t *testing.T) {
	ts := newEC2TestServer(t)
	tagged, untagged := ec2ImageFilterFixture(t, ts)

	got := ec2FilteredImageIDs(t, ts, map[string]string{
		"Filter.1.Name": "tag-key", "Filter.1.Value.1": "Env",
	})
	assert.Equal(t, []string{tagged}, got,
		"tag-key=Env selects the images carrying an Env tag whatever its value")
	assert.NotContains(t, got, untagged)
}

// ec2TwoTagImages registers one AMI tagged Env=prod,Team=core and one tagged Env=prod
// alone, which is the pair every multi-filter case below needs: one image satisfying both
// requirements and one satisfying only the first.
func ec2TwoTagImages(t *testing.T, ts *httptest.Server) (both, envOnly string) {
	t.Helper()
	instID := ec2TagTestInstance(t, ts)

	both = ec2CreateImageID(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": instID, "Name": "both-tags",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
		"TagSpecification.1.Tag.2.Key":    "Team",
		"TagSpecification.1.Tag.2.Value":  "core",
	})
	envOnly = ec2CreateImageID(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": instID, "Name": "env-only",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	return both, envOnly
}

// TestEC2_DescribeImages_DistinctFilterNamesAreANDed guards the property adopting
// extractEC2Filters could have taken away.
//
// Using_Filtering is explicit that filters are ANDed and the values within one ORed, so
// two differently-named tag: filters state two requirements. describeImages' own walk got
// this right by appending each Filter.N to a slice; extractEC2Filters keys a map by name,
// which preserves it only because these two names differ. This test is the regression
// guard on that, and it passes both before and after the switch to the shared extractor —
// which is the point.
func TestEC2_DescribeImages_DistinctFilterNamesAreANDed(t *testing.T) {
	ts := newEC2TestServer(t)
	both, envOnly := ec2TwoTagImages(t, ts)

	got := ec2FilteredImageIDs(t, ts, map[string]string{
		"Filter.1.Name": "tag:Env", "Filter.1.Value.1": "prod",
		"Filter.2.Name": "tag:Team", "Filter.2.Value.1": "core",
	})
	assert.Equal(t, []string{both}, got, "both tag filters must apply, not only the last")
	assert.NotContains(t, got, envOnly,
		"an image satisfying one of two ANDed filters must not match")
}

// TestEC2_DescribeImages_RepeatedFilterNameORsItsValues pins the one case where the map
// keyed by name actually collides: the same name sent twice.
//
// AWS documents that filters are ANDed and values ORed, and says nothing about a name
// appearing twice, so merging the two value lists into one OR is substrate's reading.
// What it replaces was not a reading of anything — extractEC2Filters overwrote the
// earlier entry, so a caller who sent two got the last one applied and no indication the
// first had been dropped. Every operation using the shared extractor inherits this fix,
// which is why it is pinned here rather than left implicit in the ANDing case above.
func TestEC2_DescribeImages_RepeatedFilterNameORsItsValues(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	prod := ec2CreateImageID(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": instID, "Name": "prod-ami",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	staging := ec2CreateImageID(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": instID, "Name": "staging-ami",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "staging",
	})
	dev := ec2CreateImageID(t, ts, map[string]string{
		"Action": "CreateImage", "InstanceId": instID, "Name": "dev-ami",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "dev",
	})

	got := ec2FilteredImageIDs(t, ts, map[string]string{
		"Filter.1.Name": "tag:Env", "Filter.1.Value.1": "prod",
		"Filter.2.Name": "tag:Env", "Filter.2.Value.1": "staging",
	})
	assert.ElementsMatch(t, []string{prod, staging}, got,
		"a repeated filter name keeps both values instead of discarding the first")
	assert.NotContains(t, got, dev)
}
