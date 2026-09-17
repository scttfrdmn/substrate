package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #891's surface: CreateCapacityReservation, DescribeCapacityReservations and
// CancelCapacityReservation answer, and the outcome of a create is seedable.
//
// Every assertion is at HTTP level, because the whole of the defect was observable only through
// a response: all three actions reached the dispatcher's default arm and answered InvalidAction,
// so a consumer whose probe primitive is reserve-read-cancel could not run at all. Where the
// question is whether an element is *absent* rather than empty the raw body is asserted, because
// a decoder reports both as a zero value — which is how the missing-member class of defect keeps
// getting through a struct assertion.

// crItem is a capacityReservation element as a caller decodes it.
type crItem struct {
	AvailabilityZone       string `xml:"availabilityZone"`
	AvailabilityZoneID     string `xml:"availabilityZoneId"`
	AvailableInstanceCount int    `xml:"availableInstanceCount"`
	CapacityReservationARN string `xml:"capacityReservationArn"`
	CapacityReservationID  string `xml:"capacityReservationId"`
	CreateDate             string `xml:"createDate"`
	DeliveryPreference     string `xml:"deliveryPreference"`
	EBSOptimized           bool   `xml:"ebsOptimized"`
	EndDate                string `xml:"endDate"`
	EndDateType            string `xml:"endDateType"`
	EphemeralStorage       bool   `xml:"ephemeralStorage"`
	InstanceMatchCriteria  string `xml:"instanceMatchCriteria"`
	InstancePlatform       string `xml:"instancePlatform"`
	InstanceType           string `xml:"instanceType"`
	OutpostARN             string `xml:"outpostArn"`
	OwnerID                string `xml:"ownerId"`
	PlacementGroupARN      string `xml:"placementGroupArn"`
	ReservationType        string `xml:"reservationType"`
	StartDate              string `xml:"startDate"`
	State                  string `xml:"state"`
	Tags                   []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
	Tenancy            string `xml:"tenancy"`
	TotalInstanceCount int    `xml:"totalInstanceCount"`
}

// crCreateResponse is CreateCapacityReservationResponse as a caller decodes it.
type crCreateResponse struct {
	XMLName     xml.Name `xml:"CreateCapacityReservationResponse"`
	Reservation crItem   `xml:"capacityReservation"`
}

// crDescribeResponse is DescribeCapacityReservationsResponse as a caller decodes it.
type crDescribeResponse struct {
	XMLName      xml.Name `xml:"DescribeCapacityReservationsResponse"`
	Reservations []crItem `xml:"capacityReservationSet>item"`
	NextToken    string   `xml:"nextToken"`
}

// crCancelResponse is CancelCapacityReservationResponse as a caller decodes it.
type crCancelResponse struct {
	XMLName xml.Name `xml:"CancelCapacityReservationResponse"`
	Return  bool     `xml:"return"`
}

// crCreateParams fills in the three required parameters so a case only has to name what it is
// testing. A case testing one of the three absent builds its own map instead.
func crCreateParams(extra map[string]string) map[string]string {
	params := map[string]string{
		"Action":           "CreateCapacityReservation",
		"InstanceType":     "m5.large",
		"InstancePlatform": "Linux/UNIX",
		"InstanceCount":    "2",
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// ec2CreateCapacityReservation creates a reservation and returns it, failing on any non-200.
func ec2CreateCapacityReservation(t *testing.T, ts *httptest.Server, extra map[string]string) crItem {
	t.Helper()
	var got crCreateResponse
	ec2DescribeXML(t, ts, crCreateParams(extra), &got)
	return got.Reservation
}

// ec2DescribeCapacityReservations sends a describe and returns the decoded response.
func ec2DescribeCapacityReservations(t *testing.T, ts *httptest.Server, extra map[string]string) crDescribeResponse {
	t.Helper()
	params := map[string]string{"Action": "DescribeCapacityReservations"}
	for k, v := range extra {
		params[k] = v
	}
	var got crDescribeResponse
	ec2DescribeXML(t, ts, params, &got)
	return got
}

// ec2SeedCapacityReservationOutcome POSTs a create-outcome seed to the control plane.
func ec2SeedCapacityReservationOutcome(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/ec2/capacity-reservation-outcomes",
		"application/json", strings.NewReader(body))
	require.NoError(t, err, "seed capacity reservation outcome")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed capacity reservation outcome = %d: %s", resp.StatusCode, got)
	}
}

// ec2ClearCapacityReservationOutcomes DELETEs outcome seeds; an empty query clears every seed.
func ec2ClearCapacityReservationOutcomes(t *testing.T, ts *httptest.Server, query string) {
	t.Helper()
	u := ts.URL + "/v1/ec2/capacity-reservation-outcomes"
	if query != "" {
		u += "?" + query
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	require.NoError(t, err, "build clear request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "clear capacity reservation outcomes")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear capacity reservation outcomes = %d: %s", resp.StatusCode, got)
	}
}

// TestEC2_CreateCapacityReservation_Nominal covers the whole nominal answer: every member the
// CapacityReservation shape publishes that substrate can fill, and the defaults for the four
// parameters a minimal request names none of.
func TestEC2_CreateCapacityReservation_Nominal(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1a"})

	assert.True(t, strings.HasPrefix(got.CapacityReservationID, "cr-"),
		"a reservation ID starts with cr-: %q", got.CapacityReservationID)
	assert.Len(t, got.CapacityReservationID, len("cr-")+17,
		"AWS publishes the form cr-xxxxxxxxxxxxxxxxx")
	assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:capacity-reservation/"+
		got.CapacityReservationID, got.CapacityReservationARN)
	assert.Equal(t, "123456789012", got.OwnerID)
	assert.Equal(t, "m5.large", got.InstanceType)
	assert.Equal(t, "Linux/UNIX", got.InstancePlatform)
	assert.Equal(t, "us-east-1a", got.AvailabilityZone)
	assert.Equal(t, "use1-az1", got.AvailabilityZoneID,
		"the zone ID is derived from the name through the same seeded zones DescribeAvailabilityZones reports")
	assert.Equal(t, 2, got.TotalInstanceCount)
	assert.Equal(t, 2, got.AvailableInstanceCount,
		"nothing consumes a reservation, so the whole reservation stays available")
	assert.Equal(t, "active", got.State)
	assert.Equal(t, "default", got.Tenancy, "AWS's default tenancy is spelled default, not shared")
	assert.Equal(t, "open", got.InstanceMatchCriteria, "InstanceMatchCriteria publishes Default: open")
	assert.Equal(t, "unlimited", got.EndDateType, "no EndDate means the reservation does not end")
	assert.Equal(t, "default", got.ReservationType, "a Capacity Block cannot be created here")
	assert.False(t, got.EBSOptimized)
	assert.False(t, got.EphemeralStorage)
	assert.NotEmpty(t, got.CreateDate)
	assert.Equal(t, got.CreateDate, got.StartDate,
		"an immediate reservation starts when it is created")
}

// TestEC2_CreateCapacityReservation_OmitsUnsetMembers pins the seven members that are absent
// rather than empty when a request names nothing for them.
//
// Asserted on the raw body, because a decoder cannot tell `<endDate/>` from no element at all —
// and an SDK can: an empty element says "the value is the empty string", which for a timestamp
// or an ARN is a value no caller can use. #827 established that substrate does not report a
// value nothing wrote.
func TestEC2_CreateCapacityReservation_OmitsUnsetMembers(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	body := ec2DescribeBody(t, ts, crCreateParams(nil))

	for _, member := range []string{
		"availabilityZone", "availabilityZoneId", "endDate", "outpostArn",
		"placementGroupArn", "deliveryPreference", "tagSet",
	} {
		assert.NotContains(t, body, "<"+member+">", "%s must be absent, not empty", member)
		assert.NotContains(t, body, "<"+member+"/>", "%s must be absent, not empty", member)
	}
	// The counts and the booleans are the other half of the rule: zero and false are real
	// answers there, so they are rendered even though they are the zero value.
	assert.Contains(t, body, "<ebsOptimized>false</ebsOptimized>")
	assert.Contains(t, body, "<ephemeralStorage>false</ephemeralStorage>")
}

// TestEC2_CreateCapacityReservation_NoZone covers a request naming neither AvailabilityZone nor
// AvailabilityZoneId, which AWS accepts — both are `Required: No` and the page states no rule on
// the pair, unlike CreateVolume's.
//
// The reservation is created and reports no zone, rather than substrate picking one: AWS's zone
// selection is undocumented, and answering a zone the caller never named would report a
// reservation somewhere it may not be.
func TestEC2_CreateCapacityReservation_NoZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, nil)

	assert.Empty(t, got.AvailabilityZone)
	assert.Empty(t, got.AvailabilityZoneID)
	assert.Equal(t, "active", got.State, "a zoneless request is accepted, not refused")
}

// TestEC2_CreateCapacityReservation_ZoneID covers the zone-ID half of the pair: an ID is
// translated into the name the record and every response carry, and both spellings are reported.
func TestEC2_CreateCapacityReservation_ZoneID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZoneId": "use1-az2"})

	assert.Equal(t, "us-east-1b", got.AvailabilityZone)
	assert.Equal(t, "use1-az2", got.AvailabilityZoneID)
}

// TestEC2_CreateCapacityReservation_UnseededZoneName covers the asymmetry [ec2VolumeZone]
// documents and every zone-taking operation follows: a zone *name* is recorded as given and not
// validated, while a zone *ID* must resolve because it has to be translated.
//
// The reservation is created with the name as sent and reports no zone ID, rather than an
// invented one — substrate seeds three zones per Region and a name outside them is reachable.
func TestEC2_CreateCapacityReservation_UnseededZoneName(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1f"})

	assert.Equal(t, "us-east-1f", got.AvailabilityZone)
	assert.Empty(t, got.AvailabilityZoneID, "no zone ID may be invented for an unseeded name")
}

// TestEC2_CreateCapacityReservation_ZonePairAgrees covers the pair AWS does not forbid: naming
// both spellings of one zone is accepted, because the page states no mutual exclusion.
func TestEC2_CreateCapacityReservation_ZonePairAgrees(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, map[string]string{
		"AvailabilityZone":   "us-east-1c",
		"AvailabilityZoneId": "use1-az3",
	})

	assert.Equal(t, "us-east-1c", got.AvailabilityZone)
	assert.Equal(t, "use1-az3", got.AvailabilityZoneID)
}

// TestEC2_CreateCapacityReservation_Refusals pins every documented refusal on the create path,
// each row naming the published constraint it comes from.
func TestEC2_CreateCapacityReservation_Refusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
		status int
		code   string
	}{
		{
			// `Required: Yes`, so an absent parameter is a missing one rather than a bad value.
			name:   "no InstanceType",
			params: map[string]string{"InstancePlatform": "Linux/UNIX", "InstanceCount": "1"},
			status: http.StatusBadRequest, code: "MissingParameter",
		},
		{
			name:   "no InstancePlatform",
			params: map[string]string{"InstanceType": "m5.large", "InstanceCount": "1"},
			status: http.StatusBadRequest, code: "MissingParameter",
		},
		{
			name:   "no InstanceCount",
			params: map[string]string{"InstanceType": "m5.large", "InstancePlatform": "Linux/UNIX"},
			status: http.StatusBadRequest, code: "MissingParameter",
		},
		{
			// InstancePlatform's Valid Values, and the value that looks like one and is not:
			// the RHEL-with-HA family stops at Standard and Enterprise.
			name:   "platform outside the eighteen",
			params: crCreateParams(map[string]string{"InstancePlatform": "RHEL with HA and SQL Server Web"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			// Tenancy is `default | dedicated`. `shared` is the spelling the Spot and fleet
			// APIs use for the same concept, so it is the one a reader supplies from memory.
			name:   "tenancy shared",
			params: crCreateParams(map[string]string{"Tenancy": "shared"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "match criteria outside the two",
			params: crCreateParams(map[string]string{"InstanceMatchCriteria": "any"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "delivery preference outside the two",
			params: crCreateParams(map[string]string{"DeliveryPreference": "immediate"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "end date type outside the two",
			params: crCreateParams(map[string]string{"EndDateType": "forever"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			// "Valid range: 1 - 1000", published as prose in the parameter's description.
			name:   "instance count zero",
			params: crCreateParams(map[string]string{"InstanceCount": "0"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "instance count above 1000",
			params: crCreateParams(map[string]string{"InstanceCount": "1001"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "instance count not an integer",
			params: crCreateParams(map[string]string{"InstanceCount": "two"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			// EndDateType's own text: "You must provide an EndDate value if the EndDateType
			// value is limited".
			name:   "limited without an end date",
			params: crCreateParams(map[string]string{"EndDateType": "limited"}),
			status: http.StatusBadRequest, code: "InvalidParameterCombination",
		},
		{
			// And the other half: "Do not provide an EndDate if the EndDateType is unlimited."
			name: "unlimited with an end date",
			params: crCreateParams(map[string]string{
				"EndDateType": "unlimited", "EndDate": "2030-01-01T00:00:00Z",
			}),
			status: http.StatusBadRequest, code: "InvalidParameterCombination",
		},
		{
			name:   "end date not a timestamp",
			params: crCreateParams(map[string]string{"EndDate": "next tuesday"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			name:   "zone ID naming no zone",
			params: crCreateParams(map[string]string{"AvailabilityZoneId": "use1-az9"}),
			status: http.StatusBadRequest, code: "InvalidParameterValue",
		},
		{
			// Two names for one zone cannot both be answered, and AWS's model has no way to
			// say which wins — so the pair is refused rather than resolved by precedence.
			name: "zone pair disagrees",
			params: crCreateParams(map[string]string{
				"AvailabilityZone": "us-east-1a", "AvailabilityZoneId": "use1-az3",
			}),
			status: http.StatusBadRequest, code: "InvalidParameterCombination",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			params := tt.params
			if params["Action"] == "" {
				params["Action"] = "CreateCapacityReservation"
			}
			status, code := ec2ErrorCode(t, ts, params)
			assert.Equal(t, tt.status, status)
			assert.Equal(t, tt.code, code)
		})
	}
}

// TestEC2_CreateCapacityReservation_RefusesFutureDated covers the one place substrate refuses a
// request AWS accepts, which is a divergence worth a test of its own.
//
// StartDate and CommitmentDuration ask for a future-dated reservation, whose nine extra states
// and delivery model substrate does not have. Answering `active` to a request for capacity two
// days out would be a false observation with no signal in it, so the request is refused with
// `Unsupported` — "The specified request is unsupported" — and the divergence is loud.
func TestEC2_CreateCapacityReservation_RefusesFutureDated(t *testing.T) {
	t.Parallel()
	for _, param := range []string{"StartDate", "CommitmentDuration"} {
		t.Run(param, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			value := "2030-01-01T00:00:00Z"
			if param == "CommitmentDuration" {
				value = "86400"
			}
			status, code, message := ec2ErrorDetail(t, ts, crCreateParams(map[string]string{param: value}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "Unsupported", code)
			assert.Contains(t, message, param, "the message must name the parameter that was refused")
		})
	}
}

// TestEC2_CreateCapacityReservation_EndDateInference covers EndDateType's inference, which is the
// difference between accepting AWS's request set and refusing part of it.
//
// AWS publishes no `Default:` line for EndDateType, and its prose forbids exactly two
// combinations. Defaulting an absent EndDateType to `unlimited` would turn a request naming only
// an EndDate — which nothing forbids — into the second of those refusals.
func TestEC2_CreateCapacityReservation_EndDateInference(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		params      map[string]string
		endDateType string
		endDate     string
	}{
		{
			name:        "no end date is unlimited",
			params:      nil,
			endDateType: "unlimited",
		},
		{
			name:        "an end date alone infers limited",
			params:      map[string]string{"EndDate": "2030-06-01T12:00:00Z"},
			endDateType: "limited",
			endDate:     "2030-06-01T12:00:00Z",
		},
		{
			name: "limited with an end date is accepted as written",
			params: map[string]string{
				"EndDateType": "limited", "EndDate": "2030-06-01T12:00:00Z",
			},
			endDateType: "limited",
			endDate:     "2030-06-01T12:00:00Z",
		},
		{
			name:        "unlimited alone is accepted as written",
			params:      map[string]string{"EndDateType": "unlimited"},
			endDateType: "unlimited",
		},
		{
			// AWS's type is Timestamp and publishes no format, so a bare date is accepted and
			// normalized — which is what lets the stored value, the rendered element and the
			// end-date filter all compare one string.
			name:        "a bare date is normalized to RFC 3339 UTC",
			params:      map[string]string{"EndDate": "2030-06-01"},
			endDateType: "limited",
			endDate:     "2030-06-01T00:00:00Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			got := ec2CreateCapacityReservation(t, ts, tt.params)
			assert.Equal(t, tt.endDateType, got.EndDateType)
			assert.Equal(t, tt.endDate, got.EndDate)
		})
	}
}

// TestEC2_CreateCapacityReservation_RecordedIntent covers the four parameters substrate stores
// and echoes without modeling what they mean.
//
// Each is a real observation a caller can read back, which is the point: a consumer's request
// builder is tested against the fact that the value survives the round trip, even though
// substrate places nothing, models no Outpost, and delivers no capacity.
func TestEC2_CreateCapacityReservation_RecordedIntent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	got := ec2CreateCapacityReservation(t, ts, map[string]string{
		"OutpostArn":         "arn:aws:outposts:us-east-1:123456789012:outpost/op-0123456789abcdef0",
		"PlacementGroupArn":  "arn:aws:ec2:us-east-1:123456789012:placement-group/cluster-1",
		"DeliveryPreference": "incremental",
		"EbsOptimized":       "true",
		"EphemeralStorage":   "true",
		"ClientToken":        "token-1",
	})

	assert.Equal(t, "arn:aws:outposts:us-east-1:123456789012:outpost/op-0123456789abcdef0", got.OutpostARN)
	assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:placement-group/cluster-1", got.PlacementGroupARN)
	assert.Equal(t, "incremental", got.DeliveryPreference)
	assert.True(t, got.EBSOptimized)
	assert.True(t, got.EphemeralStorage)
	assert.Equal(t, "active", got.State, "an accepted-and-ignored ClientToken does not change the answer")
}

// TestEC2_CreateCapacityReservation_Tags covers the tag-on-create path and the three routes a
// caller can then read those tags back through.
//
// The create response carries the whole CapacityReservation structure rather than an ID, so the
// tags are readable from the create itself — and DescribeCapacityReservations and DescribeTags
// must agree with it. DescribeTags matters more than usual here: DescribeCapacityReservations
// documents no tag filter, so it is the documented route to finding a reservation by tag.
func TestEC2_CreateCapacityReservation_Tags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := ec2CreateCapacityReservation(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "capacity-reservation",
		"TagSpecification.1.Tag.1.Key":    "Team",
		"TagSpecification.1.Tag.1.Value":  "research",
		"TagSpecification.1.Tag.2.Key":    "Cost",
		"TagSpecification.1.Tag.2.Value":  "grant-7",
	})
	require.Len(t, created.Tags, 2, "the create response carries the tags it applied")

	got := ec2DescribeCapacityReservations(t, ts, nil)
	require.Len(t, got.Reservations, 1)
	require.Len(t, got.Reservations[0].Tags, 2)
	assert.Equal(t, "Team", got.Reservations[0].Tags[0].Key)
	assert.Equal(t, "research", got.Reservations[0].Tags[0].Value)

	// DescribeTags reaches a reservation because "cr" is registered in ec2TagScanTargets, and it
	// reports AWS's TagSpecification spelling of the type.
	body := ec2DescribeBody(t, ts, map[string]string{
		"Action":           "DescribeTags",
		"Filter.1.Name":    "resource-id",
		"Filter.1.Value.1": created.CapacityReservationID,
	})
	assert.Contains(t, body, "<resourceType>capacity-reservation</resourceType>")
	assert.Contains(t, body, "<key>Team</key>")
}

// TestEC2_CapacityReservation_CreateTags covers tagging a reservation after the fact, which is
// the half of the pipeline a TagSpecification does not exercise.
//
// A cr- ID resolves through ec2TaggableResource, so CreateTags writes to the record's "tags"
// member and the reservation's own describe reports it — the alignment #708 established, and the
// one whose absence made CreateTags answer <return>true</return> having written nothing.
func TestEC2_CapacityReservation_CreateTags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := ec2CreateCapacityReservation(t, ts, nil)

	resp := ec2Request(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": created.CapacityReservationID,
		"Tag.1.Key":    "Owner",
		"Tag.1.Value":  "scheduler",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	got := ec2DescribeCapacityReservations(t, ts, nil)
	require.Len(t, got.Reservations, 1)
	require.Len(t, got.Reservations[0].Tags, 1, "CreateTags on a cr- ID must not be a silent no-op")
	assert.Equal(t, "Owner", got.Reservations[0].Tags[0].Key)
}

// TestEC2_DescribeCapacityReservations_ByID covers the narrowing rule: an ID selects, and a
// well-formed ID naming no reservation contributes nothing rather than refusing.
//
// Narrowing rather than asserting is what keeps a reaper's sweep over a list of IDs from
// failing because one of them had already been released and swept — the reading
// DescribeFleets already applies to FleetId.N. AWS's own behavior here is undocumented.
func TestEC2_DescribeCapacityReservations_ByID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	first := ec2CreateCapacityReservation(t, ts, map[string]string{"InstanceType": "m5.large"})
	second := ec2CreateCapacityReservation(t, ts, map[string]string{"InstanceType": "c5.xlarge"})

	got := ec2DescribeCapacityReservations(t, ts, map[string]string{
		"CapacityReservationId.1": second.CapacityReservationID,
	})
	require.Len(t, got.Reservations, 1)
	assert.Equal(t, second.CapacityReservationID, got.Reservations[0].CapacityReservationID)

	// One known ID and one well-formed unknown one: the known reservation is still reported.
	got = ec2DescribeCapacityReservations(t, ts, map[string]string{
		"CapacityReservationId.1": first.CapacityReservationID,
		"CapacityReservationId.2": "cr-12345678901200000",
	})
	require.Len(t, got.Reservations, 1)
	assert.Equal(t, first.CapacityReservationID, got.Reservations[0].CapacityReservationID)
}

// TestEC2_CapacityReservation_MalformedID covers the ID error that *is* published, and the one
// place the two documented ID errors are treated differently.
//
// AWS publishes InvalidCapacityReservationId.Malformed with the form
// cr-xxxxxxxxxxxxxxxxx, so a caller mistake in the request itself is refused on both operations
// — where a well-formed ID naming nothing narrows on a describe and answers
// InvalidCapacityReservationId.NotFound on a cancel.
func TestEC2_CapacityReservation_MalformedID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
	}{
		{
			name: "describe",
			params: map[string]string{
				"Action":                  "DescribeCapacityReservations",
				"CapacityReservationId.1": "cr-typo",
			},
		},
		{
			name: "cancel",
			params: map[string]string{
				"Action":                "CancelCapacityReservation",
				"CapacityReservationId": "cr-typo",
			},
		},
		{
			name: "cancel with the wrong prefix",
			params: map[string]string{
				"Action":                "CancelCapacityReservation",
				"CapacityReservationId": "fleet-0123456789abcdef",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, tt.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidCapacityReservationId.Malformed", code)
		})
	}
}

// TestEC2_DescribeCapacityReservations_Filters covers all twelve documented filter names, each
// with a matching and a non-matching value, and the fact that an undocumented name is refused.
//
// The tag filters are the point of the last two rows: the page documents twelve names and
// neither tag:<key> nor tag-key, so both are refused here while they are accepted on most
// neighboring describes. That is AWS's set, and #671's rule is to model only what the API model
// states.
func TestEC2_DescribeCapacityReservations_Filters(t *testing.T) {
	t.Parallel()

	// One reservation carrying a value for every filterable member, so each row can ask for a
	// hit and a miss against the same resource.
	create := map[string]string{
		"InstanceType":                    "r5.4xlarge",
		"InstancePlatform":                "Windows",
		"AvailabilityZone":                "us-east-1b",
		"Tenancy":                         "dedicated",
		"InstanceMatchCriteria":           "targeted",
		"EndDate":                         "2031-03-04T05:06:07Z",
		"OutpostArn":                      "arn:aws:outposts:us-east-1:123456789012:outpost/op-1",
		"PlacementGroupArn":               "arn:aws:ec2:us-east-1:123456789012:placement-group/pg-1",
		"TagSpecification.1.ResourceType": "capacity-reservation",
		"TagSpecification.1.Tag.1.Key":    "Team",
		"TagSpecification.1.Tag.1.Value":  "research",
	}

	tests := []struct {
		name  string
		hit   string
		miss  string
		value func(crItem) string
	}{
		{name: "instance-type", hit: "r5.4xlarge", miss: "m5.large"},
		{name: "instance-platform", hit: "Windows", miss: "Linux/UNIX"},
		{name: "availability-zone", hit: "us-east-1b", miss: "us-east-1a"},
		{name: "tenancy", hit: "dedicated", miss: "default"},
		{name: "instance-match-criteria", hit: "targeted", miss: "open"},
		{name: "state", hit: "active", miss: "failed"},
		{name: "end-date-type", hit: "limited", miss: "unlimited"},
		{name: "end-date", hit: "2031-03-04T05:06:07Z", miss: "2031-03-04T05:06:08Z"},
		{name: "owner-id", hit: "123456789012", miss: "111111111111"},
		{
			name: "outpost-arn",
			hit:  "arn:aws:outposts:us-east-1:123456789012:outpost/op-1",
			miss: "arn:aws:outposts:us-east-1:123456789012:outpost/op-2",
		},
		{
			name: "placement-group-arn",
			hit:  "arn:aws:ec2:us-east-1:123456789012:placement-group/pg-1",
			miss: "arn:aws:ec2:us-east-1:123456789012:placement-group/pg-2",
		},
		{
			// A date filter compares the rendered string, which is why a wildcard is how a
			// caller asks for a range — the reading start-time already records on snapshots.
			name: "start-date", hit: "*", miss: "1999-*",
			value: func(item crItem) string { return item.StartDate },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			created := ec2CreateCapacityReservation(t, ts, create)
			if tt.value != nil {
				require.NotEmpty(t, tt.value(created))
			}

			got := ec2DescribeCapacityReservations(t, ts, map[string]string{
				"Filter.1.Name":    tt.name,
				"Filter.1.Value.1": tt.hit,
			})
			require.Len(t, got.Reservations, 1, "%s=%s must match", tt.name, tt.hit)
			assert.Equal(t, created.CapacityReservationID, got.Reservations[0].CapacityReservationID)

			got = ec2DescribeCapacityReservations(t, ts, map[string]string{
				"Filter.1.Name":    tt.name,
				"Filter.1.Value.1": tt.miss,
			})
			assert.Empty(t, got.Reservations, "%s=%s must not match", tt.name, tt.miss)
		})
	}
}

// TestEC2_DescribeCapacityReservations_UndocumentedFilters covers the names the page does not
// document, including both tag forms.
func TestEC2_DescribeCapacityReservations_UndocumentedFilters(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"tag:Team", "tag-key", "capacity-reservation-id", "instance-count"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, map[string]string{
				"Action":           "DescribeCapacityReservations",
				"Filter.1.Name":    name,
				"Filter.1.Value.1": "anything",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	}
}

// TestEC2_DescribeCapacityReservations_Pagination covers MaxResults, its published range, and a
// NextToken round trip that visits every reservation exactly once.
func TestEC2_DescribeCapacityReservations_Pagination(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	const total = 5
	ids := map[string]bool{}
	for i := range total {
		created := ec2CreateCapacityReservation(t, ts, map[string]string{
			"InstanceCount": strconv.Itoa(i + 1),
		})
		ids[created.CapacityReservationID] = true
	}
	require.Len(t, ids, total, "five creates must mint five distinct IDs")

	seen := map[string]bool{}
	token := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, total+1, "pagination must terminate")
		params := map[string]string{"MaxResults": "2"}
		if token != "" {
			params["NextToken"] = token
		}
		got := ec2DescribeCapacityReservations(t, ts, params)
		require.LessOrEqual(t, len(got.Reservations), 2)
		for _, item := range got.Reservations {
			require.False(t, seen[item.CapacityReservationID], "a page must not repeat a reservation")
			seen[item.CapacityReservationID] = true
		}
		token = got.NextToken
		if token == "" {
			break
		}
	}
	assert.Equal(t, ids, seen, "every reservation must be visited exactly once")

	// The published range is 1 to 1000, and substrate refuses rather than clamping — the
	// convention every other published EC2 range follows.
	for _, bad := range []string{"0", "1001"} {
		status, code := ec2ErrorCode(t, ts, map[string]string{
			"Action":     "DescribeCapacityReservations",
			"MaxResults": bad,
		})
		assert.Equal(t, http.StatusBadRequest, status, "MaxResults=%s", bad)
		assert.Equal(t, "InvalidParameterValue", code, "MaxResults=%s", bad)
	}
}

// TestEC2_DescribeCapacityReservations_IDsWithMaxResults covers the rule EC2 publishes once for
// the whole service rather than on this page: an ID list and MaxResults in one request is
// InvalidParameterCombination.
//
// It is the refusal that matters most in the divergence direction — without it the combination
// answers 200 here and fails in production — and it is checked before the ID's own shape,
// because whether two parameters may appear together does not depend on either being well
// formed.
func TestEC2_DescribeCapacityReservations_IDsWithMaxResults(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := ec2CreateCapacityReservation(t, ts, nil)

	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action":                  "DescribeCapacityReservations",
		"CapacityReservationId.1": created.CapacityReservationID,
		"MaxResults":              "10",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterCombination", code)

	// Before the ID's own shape: a malformed ID plus MaxResults answers the combination error,
	// not InvalidCapacityReservationId.Malformed.
	_, code = ec2ErrorCode(t, ts, map[string]string{
		"Action":                  "DescribeCapacityReservations",
		"CapacityReservationId.1": "cr-typo",
		"MaxResults":              "10",
	})
	assert.Equal(t, "InvalidParameterCombination", code)
}

// TestEC2_DescribeCapacityReservations_Empty covers the answer before anything is reserved: a 200
// with no items rather than a refusal, and no nextToken.
func TestEC2_DescribeCapacityReservations_Empty(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	body := ec2DescribeBody(t, ts, map[string]string{"Action": "DescribeCapacityReservations"})

	assert.NotContains(t, body, "<capacityReservationId>")
	assert.NotContains(t, body, "<nextToken>", "no token when there is no next page")
}

// TestEC2_CancelCapacityReservation covers the nominal cancel and what it changes.
//
// AWS: "Cancels the specified Capacity Reservation, releases the reserved capacity, and changes
// the Capacity Reservation's state to cancelled." The released capacity is availableInstanceCount,
// which becomes zero, while totalInstanceCount keeps reporting what was reserved — of the two
// readings, the one that does not report remaining capacity on a reservation that has none.
func TestEC2_CancelCapacityReservation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := ec2CreateCapacityReservation(t, ts, map[string]string{"InstanceCount": "4"})

	var cancelled crCancelResponse
	ec2DescribeXML(t, ts, map[string]string{
		"Action":                "CancelCapacityReservation",
		"CapacityReservationId": created.CapacityReservationID,
	}, &cancelled)
	assert.True(t, cancelled.Return, "the response is return, not a reservation structure")

	got := ec2DescribeCapacityReservations(t, ts, nil)
	require.Len(t, got.Reservations, 1, "a cancelled reservation is still described")
	assert.Equal(t, "cancelled", got.Reservations[0].State)
	assert.Equal(t, 0, got.Reservations[0].AvailableInstanceCount, "the capacity was released")
	assert.Equal(t, 4, got.Reservations[0].TotalInstanceCount, "what was reserved is still reported")
}

// TestEC2_CancelCapacityReservation_Refusals pins the cancel path's refusals.
func TestEC2_CancelCapacityReservation_Refusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "no ID",
			params: map[string]string{"Action": "CancelCapacityReservation"},
			code:   "MissingParameter",
		},
		{
			// The code string is published in EC2's client error table; the message wording
			// follows a real AWS response rather than the table's own description.
			name: "well-formed ID naming nothing",
			params: map[string]string{
				"Action":                "CancelCapacityReservation",
				"CapacityReservationId": "cr-0123456789abcdef0",
			},
			code: "InvalidCapacityReservationId.NotFound",
		},
		{
			// ApplyCancellationCharges publishes one Valid Value, commitment-wind-down.
			name: "cancellation charges outside the one value",
			params: map[string]string{
				"Action":                   "CancelCapacityReservation",
				"CapacityReservationId":    "cr-0123456789abcdef0",
				"ApplyCancellationCharges": "yes",
			},
			code: "InvalidParameterValue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, tt.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.code, code)
		})
	}
}

// TestEC2_CancelCapacityReservation_NotCancellable covers cancelling a reservation twice.
//
// AWS publishes no code for this: CancelCapacityReservation's Errors section is the common-types
// boilerplate, no state-shaped code in EC2's error reference covers a non-cancellable ODCR state,
// and AWS's own sample tooling pre-checks the state through a describe rather than catching an
// error. `IncorrectState` is substrate's reading, and it is the code substrate already answers
// for this shape on a volume and a snapshot.
func TestEC2_CancelCapacityReservation_NotCancellable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	created := ec2CreateCapacityReservation(t, ts, nil)
	cancel := map[string]string{
		"Action":                "CancelCapacityReservation",
		"CapacityReservationId": created.CapacityReservationID,
	}

	resp := ec2Request(t, ts, cancel)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	status, code, message := ec2ErrorDetail(t, ts, cancel)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IncorrectState", code)
	assert.Contains(t, message, "cancelled", "the message names the state the code does not carry")
}

// TestEC2_CapacityReservation_ExpiresFromTheClock covers the one state transition substrate
// models: AWS states on endDate that "the Capacity Reservation's state changes to expired when
// it reaches its end date and time".
//
// It is derived at observation time from the simulated clock rather than stored, which is what
// makes it assertable without a wall-clock dependence — a reservation created with an end date
// already in the past is expired on its first observation. The clock is frozen so the create
// itself cannot straddle a second boundary.
func TestEC2_CapacityReservation_ExpiresFromTheClock(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	past := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	created := ec2CreateCapacityReservation(t, ts, map[string]string{"EndDate": past})
	// The create response is an observation like any other, so it reports the derived state
	// rather than the one just stored — the alternative would answer `active` for a
	// reservation a describe one microsecond later calls expired.
	assert.Equal(t, "expired", created.State)

	got := ec2DescribeCapacityReservations(t, ts, nil)
	require.Len(t, got.Reservations, 1)
	assert.Equal(t, "expired", got.Reservations[0].State,
		"an active reservation past its end date is observed as expired")
	assert.Equal(t, "limited", got.Reservations[0].EndDateType)

	// And it cannot be cancelled, because the state a describe reports is the state the cancel
	// checks — reading the stored `active` there would let an expired reservation be cancelled.
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action":                "CancelCapacityReservation",
		"CapacityReservationId": created.CapacityReservationID,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IncorrectState", code)
}

// TestEC2_CapacityReservation_ExpiredStateFilter covers the filter agreeing with the element
// beside it: a derived `expired` state is what the state filter compares.
//
// A filter that disagreed with the rendered value would be worse than no filter at all, which
// is why the matcher takes the rendered item rather than the stored record.
func TestEC2_CapacityReservation_ExpiredStateFilter(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	ec2CreateCapacityReservation(t, ts, map[string]string{"EndDate": past})

	got := ec2DescribeCapacityReservations(t, ts, map[string]string{
		"Filter.1.Name":    "state",
		"Filter.1.Value.1": "expired",
	})
	assert.Len(t, got.Reservations, 1)

	got = ec2DescribeCapacityReservations(t, ts, map[string]string{
		"Filter.1.Name":    "state",
		"Filter.1.Value.1": "active",
	})
	assert.Empty(t, got.Reservations, "the stored state must not be what the filter compares")
}

// TestEC2_CapacityReservation_SeededError covers the five failure codes a seed can make a create
// answer, and the HTTP class each carries.
//
// Two of the five are **server** errors in errors-overview.html's own table —
// InsufficientInstanceCapacity and RequestLimitExceeded — whose preamble says such errors are
// "accompanied by a 500-series HTTP response code". A consumer reads
// InsufficientInstanceCapacity as a capacity signal and would expect a 400; answering one would
// let retry logic pass here and fail against AWS.
func TestEC2_CapacityReservation_SeededError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code   string
		status int
	}{
		{"InsufficientInstanceCapacity", http.StatusInternalServerError},
		{"RequestLimitExceeded", http.StatusInternalServerError},
		{"InstanceLimitExceeded", http.StatusBadRequest},
		{"VcpuLimitExceeded", http.StatusBadRequest},
		{"Unsupported", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			ec2SeedCapacityReservationOutcome(t, ts, `{"errorCode":"`+tt.code+`"}`)

			status, code, message := ec2ErrorDetail(t, ts, crCreateParams(nil))
			assert.Equal(t, tt.status, status)
			assert.Equal(t, tt.code, code)
			assert.NotEmpty(t, message, "a seeded code answers AWS's own words for it")

			// Nothing was written: a refused create must not leave a reservation behind.
			got := ec2DescribeCapacityReservations(t, ts, nil)
			assert.Empty(t, got.Reservations)
		})
	}
}

// TestEC2_CapacityReservation_SeededErrorMessage covers a seed supplying its own message, which
// is how a test pins the wording its own error handling matches on.
func TestEC2_CapacityReservation_SeededErrorMessage(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedCapacityReservationOutcome(t, ts,
		`{"errorCode":"InsufficientInstanceCapacity","errorMessage":"no p5.48xlarge in use1-az1"}`)

	_, _, message := ec2ErrorDetail(t, ts, crCreateParams(nil))
	assert.Equal(t, "no p5.48xlarge in use1-az1", message)
}

// TestEC2_CapacityReservation_SeededState covers the other documented shape of a capacity
// failure: the call succeeds and the reservation reports a non-nominal state.
//
// AWS's own prose for `failed` is "A request can fail due to request parameters that are not
// valid, capacity constraints, or instance limit constraints" — so which of the two shapes AWS
// produces for a given cell is undocumented, and a seed selects one rather than substrate
// choosing on the caller's behalf.
func TestEC2_CapacityReservation_SeededState(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"pending", "failed", "expired", "cancelled", "active"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			ec2SeedCapacityReservationOutcome(t, ts, `{"state":"`+state+`"}`)

			created := ec2CreateCapacityReservation(t, ts, nil)
			assert.Equal(t, state, created.State)

			got := ec2DescribeCapacityReservations(t, ts, nil)
			require.Len(t, got.Reservations, 1)
			assert.Equal(t, state, got.Reservations[0].State,
				"a seeded state survives the round trip")
		})
	}
}

// TestEC2_CapacityReservation_SeededStateSurvivesExpiry covers the precedence between a seeded
// terminal state and the clock-driven expiry.
//
// Only an `active` reservation expires: a `failed` one has already reached a terminal state, and
// reporting `expired` for it would lose the outcome the test seeded.
func TestEC2_CapacityReservation_SeededStateSurvivesExpiry(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	ec2SeedCapacityReservationOutcome(t, ts, `{"state":"failed"}`)
	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	ec2CreateCapacityReservation(t, ts, map[string]string{"EndDate": past})

	got := ec2DescribeCapacityReservations(t, ts, nil)
	require.Len(t, got.Reservations, 1)
	assert.Equal(t, "failed", got.Reservations[0].State)
}

// TestEC2_CapacityReservation_SeedScoping covers the four-key resolution: the exact cell, the
// type in any zone, any type in the zone, and the wildcard, most specific first.
//
// Type-before-zone is substrate's ordering, and it is the one a caller expects — a seed naming an
// instance type is a statement about that type's scarcity, the narrower claim of the two.
func TestEC2_CapacityReservation_SeedScoping(t *testing.T) {
	t.Parallel()

	t.Run("a type-scoped seed applies in every zone", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedCapacityReservationOutcome(t, ts,
			`{"instanceType":"p5.48xlarge","errorCode":"InsufficientInstanceCapacity"}`)

		_, code := ec2ErrorCode(t, ts, crCreateParams(map[string]string{
			"InstanceType": "p5.48xlarge", "AvailabilityZone": "us-east-1c",
		}))
		assert.Equal(t, "InsufficientInstanceCapacity", code)

		// Another type in the same zone is unaffected.
		got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1c"})
		assert.Equal(t, "active", got.State)
	})

	t.Run("a zone-scoped seed applies to every type", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedCapacityReservationOutcome(t, ts,
			`{"availabilityZone":"us-east-1a","errorCode":"Unsupported"}`)

		_, code := ec2ErrorCode(t, ts, crCreateParams(map[string]string{
			"AvailabilityZone": "us-east-1a",
		}))
		assert.Equal(t, "Unsupported", code)

		got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1b"})
		assert.Equal(t, "active", got.State)
	})

	t.Run("the exact cell beats a wildcard", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedCapacityReservationOutcome(t, ts, `{"state":"pending"}`)
		ec2SeedCapacityReservationOutcome(t, ts,
			`{"instanceType":"m5.large","availabilityZone":"us-east-1a","errorCode":"VcpuLimitExceeded"}`)

		_, code := ec2ErrorCode(t, ts, crCreateParams(map[string]string{
			"AvailabilityZone": "us-east-1a",
		}))
		assert.Equal(t, "VcpuLimitExceeded", code, "the narrower seed wins")

		// A different zone falls through to the wildcard.
		got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1b"})
		assert.Equal(t, "pending", got.State)
	})

	t.Run("a wildcard seed reaches a request naming no zone", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedCapacityReservationOutcome(t, ts, `{"state":"failed"}`)

		got := ec2CreateCapacityReservation(t, ts, nil)
		assert.Equal(t, "failed", got.State)
	})
}

// TestEC2_CapacityReservation_ClearSeed covers both DELETE forms: one cell, and every seed.
func TestEC2_CapacityReservation_ClearSeed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedCapacityReservationOutcome(t, ts,
		`{"instanceType":"m5.large","availabilityZone":"us-east-1a","errorCode":"Unsupported"}`)
	ec2SeedCapacityReservationOutcome(t, ts, `{"state":"pending"}`)

	ec2ClearCapacityReservationOutcomes(t, ts, "instanceType=m5.large&availabilityZone=us-east-1a")
	got := ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1a"})
	assert.Equal(t, "pending", got.State, "clearing one cell leaves the wildcard seed")

	ec2ClearCapacityReservationOutcomes(t, ts, "")
	got = ec2CreateCapacityReservation(t, ts, map[string]string{"AvailabilityZone": "us-east-1a"})
	assert.Equal(t, "active", got.State, "clearing everything restores the nominal answer")
}

// TestEC2_CapacityReservation_SeedRefusals covers what the seeding endpoint refuses.
//
// A seed naming both an errorCode and a state is refused because the two are alternative shapes
// of one answer rather than independent knobs; a code outside the table is refused because the
// HTTP class is the half substrate cannot derive.
func TestEC2_CapacityReservation_SeedRefusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
	}{
		{name: "both", body: `{"errorCode":"Unsupported","state":"failed"}`},
		{name: "neither", body: `{"instanceType":"m5.large"}`},
		{name: "code outside the table", body: `{"errorCode":"InsufficientCapacityOnHost"}`},
		{name: "state outside the five", body: `{"state":"assessing"}`},
		{name: "not JSON", body: `nope`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			resp, err := http.Post(ts.URL+"/v1/ec2/capacity-reservation-outcomes",
				"application/json", strings.NewReader(tt.body))
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
