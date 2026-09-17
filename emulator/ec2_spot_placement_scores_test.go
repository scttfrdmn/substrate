package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #892's surface: GetSpotPlacementScores answers, and the score it answers is seedable.
//
// Every assertion is at HTTP level, because the whole of the defect was observable only through
// a response: the action reached the dispatcher's default arm and answered InvalidAction, so a
// consumer that samples the free placement score before paying for a fulfillment probe could not
// exercise that pairing at all — neither the promising branch nor the skip-it branch.

// spsItem is a spotPlacementScoreSet item as a caller decodes it.
type spsItem struct {
	Region             string `xml:"region"`
	AvailabilityZoneID string `xml:"availabilityZoneId"`
	Score              int    `xml:"score"`
}

// spsResponse is GetSpotPlacementScoresResponse as a caller decodes it.
type spsResponse struct {
	XMLName   xml.Name  `xml:"GetSpotPlacementScoresResponse"`
	Scores    []spsItem `xml:"spotPlacementScoreSet>item"`
	NextToken string    `xml:"nextToken"`
}

// ec2GetSpotPlacementScores sends a GetSpotPlacementScores request, filling in the one required
// parameter so a case only has to name what it is testing.
func ec2GetSpotPlacementScores(t *testing.T, ts *httptest.Server, params map[string]string) spsResponse {
	t.Helper()
	full := map[string]string{"Action": "GetSpotPlacementScores", "TargetCapacity": "1"}
	for k, v := range params {
		full[k] = v
	}
	var got spsResponse
	ec2DescribeXML(t, ts, full, &got)
	return got
}

// ec2SPSParams is the params for a GetSpotPlacementScores refusal case, which needs the raw map
// rather than the defaults ec2GetSpotPlacementScores supplies.
func ec2SPSParams(extra map[string]string) map[string]string {
	params := map[string]string{"Action": "GetSpotPlacementScores"}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// ec2SeedSpotPlacementScore POSTs a placement-score seed to the control plane.
func ec2SeedSpotPlacementScore(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/ec2/spot-placement-scores", "application/json", strings.NewReader(body))
	require.NoError(t, err, "seed placement score")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed placement score = %d: %s", resp.StatusCode, got)
	}
}

// ec2ClearSpotPlacementScores DELETEs placement-score seeds; an empty query clears every seed.
func ec2ClearSpotPlacementScores(t *testing.T, ts *httptest.Server, query string) {
	t.Helper()
	u := ts.URL + "/v1/ec2/spot-placement-scores"
	if query != "" {
		u += "?" + query
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	require.NoError(t, err, "build clear request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "clear placement scores")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear placement scores = %d: %s", resp.StatusCode, got)
	}
}

// TestEC2_GetSpotPlacementScores_Regions covers the nominal Region-scored answer: one entry per
// seeded Region, no availabilityZoneId, ordered by score then Region.
func TestEC2_GetSpotPlacementScores_Regions(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{
		"InstanceType.1": "m5.large",
		"InstanceType.2": "m5.xlarge",
		"InstanceType.3": "m5.2xlarge",
	})

	require.Len(t, got.Scores, 3, "one entry per seeded Region")
	assert.Equal(t, []string{"eu-west-1", "us-east-1", "us-west-2"},
		[]string{got.Scores[0].Region, got.Scores[1].Region, got.Scores[2].Region},
		"equal scores tie-break on Region ascending")
	for _, item := range got.Scores {
		assert.Empty(t, item.AvailabilityZoneID,
			"a Region-scored answer names no zone (%s)", item.Region)
		assert.Equal(t, 7, item.Score, "nominal score for %s", item.Region)
	}
	assert.Empty(t, got.NextToken)
}

// TestEC2_GetSpotPlacementScores_FewInstanceTypesScoreLow covers the one relationship between a
// request and its score that AWS publishes: "If you specify one or two instance types ... the
// returned placement score will always be low."
//
// The repro on #892 names exactly one instance type, so this is the answer a faithful emulator
// owes that exact command — and it is the reason a test must not assume a mid-range default.
func TestEC2_GetSpotPlacementScores_FewInstanceTypesScoreLow(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	tests := []struct {
		name  string
		types map[string]string
		want  int
	}{
		{"none named is not the documented condition", map[string]string{}, 7},
		{"one is low", map[string]string{"InstanceType.1": "m5.large"}, 3},
		{"two is low", map[string]string{
			"InstanceType.1": "m5.large", "InstanceType.2": "m5.xlarge"}, 3},
		{"three is nominal", map[string]string{
			"InstanceType.1": "m5.large", "InstanceType.2": "m5.xlarge",
			"InstanceType.3": "m5.2xlarge"}, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ec2GetSpotPlacementScores(t, ts, tt.types)
			require.NotEmpty(t, got.Scores)
			for _, item := range got.Scores {
				assert.Equal(t, tt.want, item.Score, item.Region)
			}
		})
	}
}

// TestEC2_GetSpotPlacementScores_SingleAvailabilityZone covers SingleAvailabilityZone=true: the
// answer becomes one entry per zone, each naming its AZ **ID**.
//
// The IDs are cross-checked against DescribeAvailabilityZones, because the correlation #892 asks
// for is only possible if the two operations agree on the identifier — a placement score for a
// zone a caller cannot then look up is not a usable signal.
func TestEC2_GetSpotPlacementScores_SingleAvailabilityZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"SingleAvailabilityZone": "true"})

	require.Len(t, got.Scores, 9, "three zones in each of three seeded Regions")
	var ids []string
	for _, item := range got.Scores {
		assert.NotEmpty(t, item.AvailabilityZoneID, "a zone-scored entry names its zone")
		assert.NotEmpty(t, item.Region, "a zone-scored entry still names its Region")
		ids = append(ids, item.AvailabilityZoneID)
	}
	assert.Equal(t, []string{
		"euw1-az1", "euw1-az2", "euw1-az3",
		"use1-az1", "use1-az2", "use1-az3",
		"usw2-az1", "usw2-az2", "usw2-az3",
	}, ids, "equal scores tie-break on Region then zone ID, both ascending")

	// The same IDs must be resolvable through DescribeAvailabilityZones.
	zones := ec2DescribeBody(t, ts, map[string]string{
		"Action": "DescribeAvailabilityZones", "Region": "us-east-1"})
	for _, id := range []string{"use1-az1", "use1-az2", "use1-az3"} {
		assert.Contains(t, zones, id, "DescribeAvailabilityZones reports the same AZ ID")
	}
}

// TestEC2_GetSpotPlacementScores_RegionNameNarrows covers RegionName.N, whose own text is "The
// Regions used to narrow down the list of Regions to be scored" — so it filters, and a Region
// substrate does not seed contributes nothing rather than answering an error.
func TestEC2_GetSpotPlacementScores_RegionNameNarrows(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"RegionName.1": "us-east-1"})
	require.Len(t, got.Scores, 1)
	assert.Equal(t, "us-east-1", got.Scores[0].Region)

	got = ec2GetSpotPlacementScores(t, ts, map[string]string{
		"RegionName.1": "us-west-2", "RegionName.2": "eu-west-1"})
	require.Len(t, got.Scores, 2)
	assert.Equal(t, []string{"eu-west-1", "us-west-2"},
		[]string{got.Scores[0].Region, got.Scores[1].Region})

	// A Region substrate does not seed narrows the answer to nothing, and the set element is
	// still rendered — an empty list, not an absent one.
	body := ec2DescribeBody(t, ts, map[string]string{
		"Action":         "GetSpotPlacementScores",
		"TargetCapacity": "1",
		"RegionName.1":   "ap-south-1",
	})
	assert.Contains(t, body, "spotPlacementScoreSet")
	var empty spsResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &empty))
	assert.Empty(t, empty.Scores)
}

// TestEC2_GetSpotPlacementScores_PluralWireNamesAreNotAccepted pins the correction recorded on
// #892: the list parameters are InstanceType.N and RegionName.N, **singular**, even though the CLI
// flags are --instance-types and --region-names.
//
// Tolerating the plural would be the one divergence direction that matters — a consumer's wrong
// request would pass here and fail against AWS. So the plural is simply not a list: the request is
// answered as if it named nothing, which for RegionNames.N means every Region is still scored.
func TestEC2_GetSpotPlacementScores_PluralWireNamesAreNotAccepted(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"RegionNames.1": "us-east-1"})
	assert.Len(t, got.Scores, 3, "RegionNames.N narrows nothing because it is not the wire name")

	// InstanceTypes.1 does not name one instance type, so the documented low score for a
	// one-or-two-type request is not triggered.
	got = ec2GetSpotPlacementScores(t, ts, map[string]string{"InstanceTypes.1": "m5.large"})
	require.NotEmpty(t, got.Scores)
	assert.Equal(t, 7, got.Scores[0].Score)
}

// TestEC2_GetSpotPlacementScores_Deterministic covers the guarantee the tie-break exists for: two
// identical requests answer byte-identically, so an assertion on the first element is not a coin
// toss. AWS's own page warns that scopes may share a score, which is when an unstable order shows.
func TestEC2_GetSpotPlacementScores_Deterministic(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	params := map[string]string{
		"Action":                 "GetSpotPlacementScores",
		"TargetCapacity":         "1",
		"SingleAvailabilityZone": "true",
	}
	first := ec2DescribeBody(t, ts, params)
	second := ec2DescribeBody(t, ts, params)
	assert.Equal(t, first, second)
}

// TestEC2_GetSpotPlacementScores_SeededRegion covers the seed a consumer's skip-this-Region branch
// needs: one Region scores low while the others stay nominal, and the low one sorts last.
func TestEC2_GetSpotPlacementScores_SeededRegion(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedSpotPlacementScore(t, ts, `{"region":"us-east-1","score":2}`)

	got := ec2GetSpotPlacementScores(t, ts, nil)
	require.Len(t, got.Scores, 3)
	assert.Equal(t, "us-east-1", got.Scores[2].Region, "the low score sorts last")
	assert.Equal(t, 2, got.Scores[2].Score)
	assert.Equal(t, 7, got.Scores[0].Score, "an unseeded Region is unaffected")
	assert.Equal(t, 7, got.Scores[1].Score)
}

// TestEC2_GetSpotPlacementScores_SeededZone covers the zone scope, including that a zone-scoped
// seed is observable only in the mode that names zones.
func TestEC2_GetSpotPlacementScores_SeededZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedSpotPlacementScore(t, ts, `{"availabilityZoneId":"use1-az2","score":1}`)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"SingleAvailabilityZone": "true"})
	require.Len(t, got.Scores, 9)
	scores := map[string]int{}
	for _, item := range got.Scores {
		scores[item.AvailabilityZoneID] = item.Score
	}
	assert.Equal(t, 1, scores["use1-az2"], "the seeded zone")
	assert.Equal(t, 7, scores["use1-az1"], "a sibling zone is unaffected")
	assert.Equal(t, 7, scores["use1-az3"])
	assert.Equal(t, "use1-az2", got.Scores[8].AvailabilityZoneID,
		"the answer is ordered highest score first, so the seeded low zone sorts last")

	// Region-scored mode names no zone, so a zone-scoped seed cannot apply to it.
	got = ec2GetSpotPlacementScores(t, ts, nil)
	require.Len(t, got.Scores, 3)
	for _, item := range got.Scores {
		assert.Equal(t, 7, item.Score, item.Region)
	}
}

// TestEC2_GetSpotPlacementScores_ZoneSeedBeatsRegionSeed covers the specificity rule: the most
// specific scope carrying a seed decides, so seeding one bad zone in an otherwise-seeded Region
// works rather than being overwritten by the coarser seed.
func TestEC2_GetSpotPlacementScores_ZoneSeedBeatsRegionSeed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedSpotPlacementScore(t, ts, `{"region":"us-east-1","score":9}`)
	ec2SeedSpotPlacementScore(t, ts, `{"availabilityZoneId":"use1-az1","score":2}`)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"SingleAvailabilityZone": "true"})
	scores := map[string]int{}
	for _, item := range got.Scores {
		scores[item.AvailabilityZoneID] = item.Score
	}
	assert.Equal(t, 2, scores["use1-az1"], "the zone seed wins over the Region seed")
	assert.Equal(t, 9, scores["use1-az2"], "a sibling falls back to the Region seed")
	assert.Equal(t, 7, scores["euw1-az1"], "another Region is unaffected")
}

// TestEC2_GetSpotPlacementScores_SeededInstanceType covers the per-instance-type scope and the
// rule that the lowest seeded score among the named types wins — a request is no easier to place
// than its scarcest instance type, and a nominal sibling must not mask the seeded one.
func TestEC2_GetSpotPlacementScores_SeededInstanceType(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedSpotPlacementScore(t, ts, `{"instanceType":"inf2.48xlarge","score":1}`)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{
		"InstanceType.1": "m5.large",
		"InstanceType.2": "m5.xlarge",
		"InstanceType.3": "inf2.48xlarge",
	})
	require.Len(t, got.Scores, 3)
	for _, item := range got.Scores {
		assert.Equal(t, 1, item.Score, "%s: the scarcest named type decides", item.Region)
	}

	// A request that does not name the seeded type is unaffected.
	got = ec2GetSpotPlacementScores(t, ts, map[string]string{
		"InstanceType.1": "m5.large",
		"InstanceType.2": "m5.xlarge",
		"InstanceType.3": "m5.2xlarge",
	})
	require.Len(t, got.Scores, 3)
	for _, item := range got.Scores {
		assert.Equal(t, 7, item.Score, item.Region)
	}
}

// TestEC2_GetSpotPlacementScores_SeededWildcard covers the seed scoped to every Region and every
// instance type, which is how a test makes the whole answer low in one call.
func TestEC2_GetSpotPlacementScores_SeededWildcard(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedSpotPlacementScore(t, ts, `{"score":4}`)

	got := ec2GetSpotPlacementScores(t, ts, map[string]string{"SingleAvailabilityZone": "true"})
	require.Len(t, got.Scores, 9)
	for _, item := range got.Scores {
		assert.Equal(t, 4, item.Score, item.AvailabilityZoneID)
	}
}

// TestEC2_GetSpotPlacementScores_ClearSeed covers both clear forms: one scope by query parameter,
// and every seed at once.
func TestEC2_GetSpotPlacementScores_ClearSeed(t *testing.T) {
	t.Parallel()

	t.Run("one scope", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedSpotPlacementScore(t, ts, `{"region":"us-east-1","score":2}`)
		ec2SeedSpotPlacementScore(t, ts, `{"region":"us-west-2","score":3}`)

		ec2ClearSpotPlacementScores(t, ts, "region=us-east-1")

		got := ec2GetSpotPlacementScores(t, ts, nil)
		scores := map[string]int{}
		for _, item := range got.Scores {
			scores[item.Region] = item.Score
		}
		assert.Equal(t, 7, scores["us-east-1"], "the cleared seed is gone")
		assert.Equal(t, 3, scores["us-west-2"], "the other seed survives")
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedSpotPlacementScore(t, ts, `{"region":"us-east-1","score":2}`)
		ec2SeedSpotPlacementScore(t, ts, `{"availabilityZoneId":"usw2-az1","score":1}`)
		ec2SeedSpotPlacementScore(t, ts, `{"instanceType":"m5.large","score":1}`)

		ec2ClearSpotPlacementScores(t, ts, "")

		got := ec2GetSpotPlacementScores(t, ts, map[string]string{
			"SingleAvailabilityZone": "true", "InstanceType.1": "m5.large"})
		require.Len(t, got.Scores, 9)
		for _, item := range got.Scores {
			// One instance type, so the documented low score applies — but no seed does.
			assert.Equal(t, 3, item.Score, item.AvailabilityZoneID)
		}
	})
}

// TestEC2_GetSpotPlacementScores_SeedRefusals covers what the seeding endpoint refuses. The 1-to-10
// scale is published as prose rather than as a Valid Range line on the member, so this endpoint is
// where substrate owns the refusal — a response outside the published scale would be substrate's
// own invention rather than a seeded observation.
func TestEC2_GetSpotPlacementScores_SeedRefusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"score below the scale", `{"region":"us-east-1","score":0}`, http.StatusBadRequest},
		{"score above the scale", `{"region":"us-east-1","score":11}`, http.StatusBadRequest},
		{"negative score", `{"region":"us-east-1","score":-1}`, http.StatusBadRequest},
		{"Region and zone together", `{"region":"us-east-1","availabilityZoneId":"use1-az1","score":5}`,
			http.StatusBadRequest},
		{"malformed body", `{`, http.StatusBadRequest},
		{"lowest legal score", `{"region":"us-east-1","score":1}`, http.StatusOK},
		{"highest legal score", `{"region":"us-east-1","score":10}`, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := http.Post(ts.URL+"/v1/ec2/spot-placement-scores",
				"application/json", strings.NewReader(tt.body))
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

// TestEC2_GetSpotPlacementScores_Refusals covers the request-level refusals, every one of which is
// either a published Valid Range/Valid Values line or a documented prose rule.
func TestEC2_GetSpotPlacementScores_Refusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	tests := []struct {
		name     string
		params   map[string]string
		wantCode string
	}{
		{"TargetCapacity is required", map[string]string{}, "MissingParameter"},
		{"TargetCapacity below its range",
			map[string]string{"TargetCapacity": "0"}, "InvalidParameterValue"},
		{"TargetCapacity above its range",
			map[string]string{"TargetCapacity": "2000000001"}, "InvalidParameterValue"},
		{"TargetCapacity is not an integer",
			map[string]string{"TargetCapacity": "lots"}, "InvalidParameterValue"},
		{"TargetCapacity at its floor",
			map[string]string{"TargetCapacity": "1"}, ""},
		{"TargetCapacity at its ceiling",
			map[string]string{"TargetCapacity": "2000000000"}, ""},
		{"MaxResults below this operation's floor of ten",
			map[string]string{"TargetCapacity": "1", "MaxResults": "9"}, "InvalidParameterValue"},
		{"MaxResults at the floor of ten",
			map[string]string{"TargetCapacity": "1", "MaxResults": "10"}, ""},
		{"MaxResults above its ceiling",
			map[string]string{"TargetCapacity": "1", "MaxResults": "1001"}, "InvalidParameterValue"},
		{"NextToken is not an offset",
			map[string]string{"TargetCapacity": "1", "NextToken": "abc"}, "InvalidParameterValue"},
		{"TargetCapacityUnitType outside its Valid Values",
			map[string]string{"TargetCapacity": "1", "TargetCapacityUnitType": "vcpus"},
			"InvalidParameterValue"},
		{"TargetCapacityUnitType vcpu",
			map[string]string{"TargetCapacity": "1", "TargetCapacityUnitType": "vcpu"}, ""},
		{"TargetCapacityUnitType memory-mib",
			map[string]string{"TargetCapacity": "1", "TargetCapacityUnitType": "memory-mib"}, ""},
		{"TargetCapacityUnitType units",
			map[string]string{"TargetCapacity": "1", "TargetCapacityUnitType": "units"}, ""},
		{"InstanceRequirementsWithMetadata with InstanceType", map[string]string{
			"TargetCapacity": "1",
			"InstanceType.1": "m5.large",
			"InstanceRequirementsWithMetadata.ArchitectureTypes.1": "x86_64",
		}, "InvalidParameterCombination"},
		{"InstanceRequirementsWithMetadata alone", map[string]string{
			"TargetCapacity": "1",
			"InstanceRequirementsWithMetadata.ArchitectureTypes.1": "x86_64",
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, code := ec2ErrorCode(t, ts, ec2SPSParams(tt.params))
			if tt.wantCode == "" {
				assert.Equal(t, http.StatusOK, status)
				assert.Empty(t, code)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

// TestEC2_GetSpotPlacementScores_TooManyRegionNames covers RegionName.N's published item count,
// "Maximum number of 10 items".
func TestEC2_GetSpotPlacementScores_TooManyRegionNames(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	params := ec2SPSParams(map[string]string{"TargetCapacity": "1"})
	for i, name := range []string{
		"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1",
		"eu-west-2", "eu-west-3", "eu-central-1", "ap-south-1", "ap-northeast-1",
	} {
		params["RegionName."+strconv.Itoa(i+1)] = name
	}
	status, code := ec2ErrorCode(t, ts, params)
	require.Equal(t, http.StatusOK, status, "ten names is the documented maximum")
	require.Empty(t, code)

	params["RegionName.11"] = "sa-east-1"
	status, code = ec2ErrorCode(t, ts, params)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
}

// TestEC2_GetSpotPlacementScores_NoTokenIsReachable records a consequence of two AWS facts
// meeting: MaxResults floors at ten, and substrate seeds three Regions of three zones each — so
// the largest answer it can build is nine items and no legal MaxResults can truncate it.
//
// Asserted rather than left implicit, because a caller looking for a pagination loop to test here
// will not find one, and a test that expected a nextToken would be asserting something unreachable
// rather than something broken.
func TestEC2_GetSpotPlacementScores_NoTokenIsReachable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	body := ec2DescribeBody(t, ts, map[string]string{
		"Action":                 "GetSpotPlacementScores",
		"TargetCapacity":         "1",
		"SingleAvailabilityZone": "true",
		"MaxResults":             "10",
	})
	assert.NotContains(t, body, "nextToken", "nine items cannot fill the smallest legal page")

	// An offset a caller could not have been handed is still accepted, matching every other
	// paginated EC2 describe: ec2NextTokenOffset refuses a malformed token, not an out-of-range
	// one, and an offset past the end pages to nothing.
	var got spsResponse
	ec2DescribeXML(t, ts, map[string]string{
		"Action":         "GetSpotPlacementScores",
		"TargetCapacity": "1",
		"NextToken":      "99",
	}, &got)
	assert.Empty(t, got.Scores)
}
