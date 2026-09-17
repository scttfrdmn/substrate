package emulator

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// GetSpotPlacementScores (#892).
//
// Through v0.117.0 the operation reached the dispatcher's default arm and answered
// InvalidAction, so a consumer that samples a placement score alongside each on-demand probe —
// the free signal it correlates against paid fulfillment — could not run that pairing against
// substrate at all.
//
// Three wire facts the operation's own reference settles, each of which a plausible
// implementation gets wrong:
//
//   - the list parameters are InstanceType.N and RegionName.N, **singular**. The CLI flags are
//     --instance-types and --region-names, so the plural is what a reader writes; a parser keyed
//     on it reads an empty list from a well-formed request, and because RegionName.N publishes
//     "Minimum number of 0 items" the failure is silent rather than a refusal. Only the singular
//     form is accepted here: tolerating the plural would let a consumer's wrong request pass
//     against substrate and fail against AWS, which is the one divergence direction that matters.
//   - MaxResults publishes "Valid Range: Minimum value of 10. Maximum value of 1000." The floor
//     of **ten** is this operation's alone — every other paginated EC2 describe in the tree floors
//     at one or five — so [ec2MinUnpublishedMaxResults] is deliberately not used.
//   - SpotPlacementScore carries exactly three members: availabilityZoneId, region and score.
//     There is no capacityAvailable, no ARN and no metadata, so there is nothing further to model.
//
// Provenance. Neither API_GetSpotPlacementScores.html nor API_SpotPlacementScore.html publishes
// an Examples section or any sample XML, so the element names below come from the two member
// lists and the item-wrapper nesting rests on substrate's existing EC2 query-protocol rendering
// rather than on a citation. Every refusal is either a published Valid Range/Valid Values line or
// a documented prose rule, and each says which; the error codes are from EC2's client-error table
// rather than from this page, whose Errors section points only at the common types. The score
// *values* substrate reports absent a seed are its own reading, argued at [ec2SPSNominalScore].
// The 1-to-10 scale is published as prose on the operation ("scored on a scale from 1 to 10") and
// **not** as a Valid Range line on the member, which is why it bounds what a seed may set rather
// than being treated as a response invariant AWS guarantees.
//
// DryRun is inert, as it is at every other EC2 operation in the tree.

// ec2SPSMinMaxResults and ec2SPSMaxMaxResults are GetSpotPlacementScores' published MaxResults
// range, "Valid Range: Minimum value of 10. Maximum value of 1000."
//
// The floor is ten rather than one, which no other EC2 operation substrate models publishes. It
// is stated here as its own pair rather than borrowed from [ec2MinPublishedMaxResults] because
// the published fact is this operation's, not a range that happens to agree with a sibling's.
const (
	ec2SPSMinMaxResults = 10
	ec2SPSMaxMaxResults = 1000
)

// ec2SPSMinTargetCapacity and ec2SPSMaxTargetCapacity are TargetCapacity's published range:
// "Minimum value of 1. Maximum value of 2000000000." Both bounds are refusals rather than clamps,
// following every other published range in the package.
const (
	ec2SPSMinTargetCapacity = 1
	ec2SPSMaxTargetCapacity = 2000000000
)

// ec2SPSMaxInstanceTypes and ec2SPSMaxRegionNames are the published item counts for
// InstanceType.N ("Maximum number of 1000 items") and RegionName.N ("Maximum number of 10
// items"). Both publish a minimum of 0 items, so naming neither is a legal request.
const (
	ec2SPSMaxInstanceTypes = 1000
	ec2SPSMaxRegionNames   = 10
)

// ec2SPSRecommendedInstanceTypes is the number of instance types below which AWS states the
// score is always low: "We recommend that you specify at least three instance types. If you
// specify one or two instance types ... the returned placement score will always be low." That
// makes the count a documented input to the answer rather than only advice.
const ec2SPSRecommendedInstanceTypes = 3

// ec2SPSTopResults is the number of scored Regions or Availability Zones one response carries,
// from the operation's own description of the answer: "The Spot placement score for the top 10
// Regions or Availability Zones".
//
// It bounds the answer independently of MaxResults, whose floor happens to be the same ten.
// Substrate seeds three Regions ([ec2SeededRegions]) with three zones each
// ([ec2SeededAZSuffixes]), so nine scored zones is the largest answer it can build today and the
// cap is not reachable — it is written down anyway, because the bound is AWS's and a later change
// to the seeded Region set should not silently start returning more than AWS does.
const ec2SPSTopResults = 10

// ec2SPSMinScore and ec2SPSMaxScore are the published score scale, "scored on a scale from 1 to
// 10".
//
// This is prose on the operation page; SpotPlacementScore.score itself publishes no Valid Range
// line. The pair therefore bounds what a *seed* may set — where substrate owns the refusal —
// rather than asserting a range AWS guarantees in a response.
const (
	ec2SPSMinScore = 1
	ec2SPSMaxScore = 10
)

// ec2SPSLowScore and ec2SPSNominalScore are the scores substrate reports when no seed applies.
//
// AWS publishes exactly one relationship between a request and its score: "If you specify one or
// two instance types ... the returned placement score will always be low." That relationship is
// AWS's and is honored — a request naming one or two instance types is scored low. The two
// numbers are **substrate's reading**, chosen inside the published 1-to-10 scale, far enough
// apart that a test can tell them apart and neither at an endpoint: a 1 would claim there is no
// capacity anywhere and a 10 would claim fulfillment is certain, and substrate models no capacity
// broker that could know either.
//
// A request naming no instance types at all is scored nominally rather than low. Naming none is
// legal ("Minimum number of 0 items") and has not met the documented condition, which is about
// specifying one or two rather than about specifying few.
//
// Everything else a caller might want observed here — a scarce Region, a zone that scores lower
// than its siblings, a score that differs per instance type — is seedable, per the rule that a
// deterministic emulator produces different results by being seeded rather than by behaving
// nondeterministically. See [ec2SpotPlacementScoreSeed].
const (
	ec2SPSLowScore     = 3
	ec2SPSNominalScore = 7
)

// ec2SPSTargetCapacityUnitTypes are TargetCapacityUnitType's published Valid Values.
var ec2SPSTargetCapacityUnitTypes = []string{"vcpu", "memory-mib", "units"}

// ec2SpotPlacementScoreItem is AWS's spotPlacementScore element.
//
// availabilityZoneId carries omitempty because the two response modes are alternatives rather
// than one shape with a hole in it: SingleAvailabilityZone's own text is "Specify true so that
// the response returns a list of scored Availability Zones. Otherwise, the response returns a
// list of scored Regions." A Region-scored answer has no zone to name, and rendering an empty
// element would report one.
type ec2SpotPlacementScoreItem struct {
	Region             string `xml:"region"`
	AvailabilityZoneID string `xml:"availabilityZoneId,omitempty"`
	Score              int    `xml:"score"`
}

// getSpotPlacementScores scores the seeded Regions, or their zones when SingleAvailabilityZone
// is true.
//
// Validation runs before any state is read, which is the ordering #887 established: whether a
// request is well formed must not depend on what happens to be seeded. Within that, the order is
// the parameter list's own — TargetCapacity first because it is the one required member, then the
// two lists, then the documented mutual exclusion, then the unit type, then pagination.
//
// RegionName.N narrows rather than asserts. Its text is "The Regions used to narrow down the list
// of Regions to be scored", so a Region substrate does not seed contributes nothing instead of
// answering an error — the same reading [EC2Plugin.describeSpotPriceHistory] applies to its own
// InstanceType.N. That keeps the answer consistent with DescribeRegions, which reports exactly
// the three seeded Regions.
func (p *EC2Plugin) getSpotPlacementScores(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if awsErr := ec2SPSValidateTargetCapacity(req.Params); awsErr != nil {
		return nil, awsErr
	}
	instanceTypes := indexedParams(req.Params, "InstanceType.%d")
	if len(instanceTypes) > ec2SPSMaxInstanceTypes {
		return nil, ec2SPSCountError("InstanceType", ec2SPSMaxInstanceTypes)
	}
	regionNames := indexedParams(req.Params, "RegionName.%d")
	if len(regionNames) > ec2SPSMaxRegionNames {
		return nil, ec2SPSCountError("RegionName", ec2SPSMaxRegionNames)
	}
	if awsErr := ec2SPSRefuseTypesWithRequirements(req.Params, instanceTypes); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2SPSValidateUnitType(req.Params); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2SPSMinMaxResults, ec2SPSMaxMaxResults)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	singleAZ := req.Params["SingleAvailabilityZone"] == "true"
	scores, err := p.ec2SPSScoreScopes(instanceTypes, regionNames, singleAZ)
	if err != nil {
		return nil, err
	}
	if len(scores) > ec2SPSTopResults {
		scores = scores[:ec2SPSTopResults]
	}

	type response struct {
		XMLName   xml.Name                    `xml:"GetSpotPlacementScoresResponse"`
		XMLNS     string                      `xml:"xmlns,attr"`
		Scores    []ec2SpotPlacementScoreItem `xml:"spotPlacementScoreSet>item"`
		NextToken string                      `xml:"nextToken,omitempty"`
	}
	page, next := ec2Page(scores, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:     "http://ec2.amazonaws.com/doc/2016-11-15/",
		Scores:    page,
		NextToken: next,
	})
}

// ec2SPSValidateTargetCapacity checks TargetCapacity, the operation's one required parameter,
// against its published range.
//
// The value is validated and then not used, because substrate models no capacity broker: how much
// capacity a request asks for cannot move a score substrate does not compute from capacity.
// Refusing an out-of-range value is still the caller-visible half of the parameter, and it is the
// half a consumer's own request builder is tested against.
//
// An absent TargetCapacity is MissingParameter rather than InvalidParameterValue, because the
// two say different things to a caller and the parameter is published "Required: Yes" rather than
// as a value constraint. Neither code is quoted by the page, so both are substrate's reading of
// EC2's client-error table.
func ec2SPSValidateTargetCapacity(params map[string]string) *AWSError {
	raw := params["TargetCapacity"]
	if raw == "" {
		return &AWSError{
			Code:       "MissingParameter",
			Message:    "TargetCapacity is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return ec2SPSValueError(raw, "targetCapacity", "It must be an integer.")
	}
	if n < ec2SPSMinTargetCapacity || n > ec2SPSMaxTargetCapacity {
		return ec2SPSValueError(raw, "targetCapacity", fmt.Sprintf(
			"It must be between %d and %d.", ec2SPSMinTargetCapacity, ec2SPSMaxTargetCapacity))
	}
	return nil
}

// ec2SPSValidateUnitType checks TargetCapacityUnitType against its published Valid Values.
//
// The parameter is otherwise inert: it says whether TargetCapacity counts instances, vCPUs or
// mebibytes, and substrate does not compute a score from the target capacity at all.
func ec2SPSValidateUnitType(params map[string]string) *AWSError {
	unitType := params["TargetCapacityUnitType"]
	if unitType == "" || containsStr(ec2SPSTargetCapacityUnitTypes, unitType) {
		return nil
	}
	return ec2SPSValueError(unitType, "targetCapacityUnitType", "It must be one of "+
		strings.Join(ec2SPSTargetCapacityUnitTypes, ", ")+".")
}

// ec2SPSCountError refuses a list parameter naming more items than its page publishes.
func ec2SPSCountError(param string, maxItems int) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf("a request may specify at most %d %s values",
			maxItems, param),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2SPSValueError builds the InvalidParameterValue error for a bad parameter value, in the
// lowercase-parameter form EC2's value-error messages use — see [ec2InvalidInstanceCount], which
// carries the reasoning for the shape.
func ec2SPSValueError(value, wireName, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("Invalid value '%s' for parameter %s. %s", value, wireName, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2SPSRefuseTypesWithRequirements enforces the mutual exclusion the page states on
// InstanceRequirementsWithMetadata: "If you specify InstanceRequirementsWithMetadata, you can't
// specify InstanceTypes."
//
// Presence of the requirements member is detected from any parameter beneath it rather than from
// a member substrate reads, because substrate reads none: attribute-based instance selection is
// not modeled, so the requirements are recorded intent that cannot move an answer. Refusing the
// *combination* is still worth having, since it is a documented refusal a consumer's request
// builder is tested against, and answering 200 to it would let code AWS rejects pass here.
//
// The scan is over the parameter map rather than an indexed walk because the member is a nested
// structure whose spelling below the prefix substrate does not model, and a map scan is order-
// independent: the refusal does not depend on which nested parameter is found first.
func ec2SPSRefuseTypesWithRequirements(params map[string]string, instanceTypes []string) *AWSError {
	if len(instanceTypes) == 0 {
		return nil
	}
	for key := range params {
		if strings.HasPrefix(key, "InstanceRequirementsWithMetadata.") {
			return &AWSError{
				Code: "InvalidParameterCombination",
				Message: "The parameter InstanceRequirementsWithMetadata cannot be used with " +
					"the parameter InstanceType",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	return nil
}

// ec2SPSScoreScopes builds the scored answer, one entry per Region or per zone.
//
// The order is by score descending, then by Region and zone ID ascending. AWS describes the
// answer as "the top 10 Regions or Availability Zones", which fixes the primary key; the
// tie-break is substrate's, and it is what makes the answer reproducible when several scopes
// score alike — which the page says they may: "Different Regions or Availability Zones might
// return the same score." Without it two runs of one request could order the same scores
// differently and an assertion on the first element would be a coin toss.
func (p *EC2Plugin) ec2SPSScoreScopes(instanceTypes, regionNames []string, singleAZ bool) ([]ec2SpotPlacementScoreItem, error) {
	wanted := map[string]bool{}
	for _, name := range regionNames {
		wanted[name] = true
	}
	defaultScore := ec2SPSDefaultScore(instanceTypes)

	var items []ec2SpotPlacementScoreItem
	for _, region := range ec2SeededRegions {
		if len(wanted) > 0 && !wanted[region.Name] {
			continue
		}
		if !singleAZ {
			score, err := p.resolveSpotPlacementScore(region.Name, "", instanceTypes, defaultScore)
			if err != nil {
				return nil, err
			}
			items = append(items, ec2SpotPlacementScoreItem{Region: region.Name, Score: score})
			continue
		}
		for _, zone := range ec2SeededZones(region.Name) {
			score, err := p.resolveSpotPlacementScore(region.Name, zone.ZoneID, instanceTypes, defaultScore)
			if err != nil {
				return nil, err
			}
			items = append(items, ec2SpotPlacementScoreItem{
				Region:             region.Name,
				AvailabilityZoneID: zone.ZoneID,
				Score:              score,
			})
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		if items[i].Region != items[j].Region {
			return items[i].Region < items[j].Region
		}
		return items[i].AvailabilityZoneID < items[j].AvailabilityZoneID
	})
	return items, nil
}

// ec2SPSDefaultScore reports the score for a scope no seed matches, from the one relationship
// between a request and its score that AWS publishes. See [ec2SPSNominalScore].
func ec2SPSDefaultScore(instanceTypes []string) int {
	if len(instanceTypes) > 0 && len(instanceTypes) < ec2SPSRecommendedInstanceTypes {
		return ec2SPSLowScore
	}
	return ec2SPSNominalScore
}
