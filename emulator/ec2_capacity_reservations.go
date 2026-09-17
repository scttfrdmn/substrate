package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// On-Demand Capacity Reservations: CreateCapacityReservation,
// DescribeCapacityReservations and CancelCapacityReservation (#891).
//
// Through v0.117.0 all three reached the dispatcher's default arm and answered InvalidAction,
// so a consumer whose probe primitive *is* an immediate Capacity Reservation — reserve, read
// the outcome, cancel — could not run against substrate at all.
//
// Four wire facts the three pages settle, each of which a plausible implementation gets
// wrong:
//
//   - DescribeCapacityReservations takes `CapacityReservationId.N`, **singular**. The CLI flag
//     is --capacity-reservation-ids, so the plural is what a reader writes; a parser keyed on
//     it reads an empty list from a well-formed request and answers with every reservation in
//     the Region instead of the one asked for. Only the singular form is accepted here.
//   - CreateCapacityReservation requires exactly **three** parameters: InstanceCount,
//     InstancePlatform and InstanceType. **AvailabilityZone is `Required: No`**, as is
//     AvailabilityZoneId, and the page states no rule making one of the pair mandatory — so a
//     validation that required a zone would refuse a request AWS accepts.
//   - `Tenancy` is `default | dedicated`. Not `shared`, which is the spelling every other
//     tenancy-taking EC2 operation uses and the one a reader supplies from memory.
//   - `InstancePlatform` publishes **eighteen** values, not the handful an implementation
//     tends to allow. The comment on [ec2CRInstancePlatforms] carries the list and the one
//     value that is *not* in it.
//
// Create returns the whole `capacityReservation` structure rather than an ID, so the tags a
// create applies are readable from the create response itself and need no second call.
//
// Provenance. None of the four pages — CreateCapacityReservation,
// DescribeCapacityReservations, CancelCapacityReservation, CapacityReservation — publishes an
// Examples section or any sample XML, so the element names below come from the member lists,
// their **order** is the CapacityReservation type page's own (alphabetical) order for want of
// a sample to follow, and the item-wrapper nesting rests on substrate's existing EC2
// query-protocol rendering rather than on a citation. All three Errors sections are empty
// ("For information about the errors that are common to all actions, see Common Error
// Types"), so every refusal code here is substrate's reading of EC2's error tables and says
// so at the site. Each refusal is either a published Valid Values line, a published Valid
// Range line, or a documented prose rule, and each names which.
//
// DryRun is inert, as it is at every other EC2 operation in the tree.
//
// Deliberately not modeled, and refused rather than answered falsely: a **future-dated**
// reservation. StartDate and CommitmentDuration are the two parameters that request one, and
// eight of the state member's thirteen values (assessing, scheduled, delayed, unsupported,
// cancelling, payment-pending, payment-failed, and by omission unavailable) exist only for a
// future-dated reservation or a Capacity Block. Substrate models neither, and answering
// `active` to a request for capacity two days out would be a false observation with no signal
// in it — see [ec2CRRefuseFutureDated].
//
// Also not modeled, and recorded rather than refused: a reservation is never *consumed*.
// RunInstances has no CapacityReservationTarget arm, so availableInstanceCount equals
// totalInstanceCount for as long as a reservation holds capacity, and no instance ever
// occupies one. Only a cancel releases it — see [EC2Plugin.cancelCapacityReservation].

// ec2CRMinInstanceCount and ec2CRMaxInstanceCount are InstanceCount's range.
//
// Published as **prose** — "Valid range: 1 - 1000" inside the parameter's description — and
// not as the `Valid Range: Minimum value of …` constraint line every other bounded EC2
// parameter carries. It is enforced anyway, which is substrate's convention for a published
// range, but the provenance is worth stating: this is the one bound on the page a reader
// could take for advice. (CommitmentDuration, by contrast, publishes a real constraint line —
// and is refused outright here, per [ec2CRRefuseFutureDated].)
const (
	ec2CRMinInstanceCount = 1
	ec2CRMaxInstanceCount = 1000
)

// ec2CRMinMaxResults and ec2CRMaxMaxResults are DescribeCapacityReservations' published
// MaxResults range: "Valid Range: Minimum value of 1. Maximum value of 1000".
const (
	ec2CRMinMaxResults = 1
	ec2CRMaxMaxResults = 1000
)

// ec2CRNominalState is the state a new immediate Capacity Reservation reports.
//
// AWS's three CLI examples all show "State": "active" on the create response, and the User
// Guide states that "the reserved capacity becomes available for use immediately after you
// create it, and billing starts as soon as the Capacity Reservation enters the active state".
// `pending` exists in the enum — "The Capacity Reservation request was successful but the
// capacity provisioning is still pending" — but it is not the documented result of creating an
// immediate reservation, so it is a seedable alternative rather than a stage every create
// passes through. See [ec2CRSeedableStates].
const ec2CRNominalState = "active"

// ec2CRExpiredState is the state a reservation reports once its end date has passed.
//
// AWS states the transition on the endDate member: "The Capacity Reservation's state changes
// to expired when it reaches its end date and time." It is derived at observation time from
// the simulated clock rather than stored, which is what makes it assertable without a
// wall-clock dependence — see [ec2CRObservedState].
const ec2CRExpiredState = "expired"

// ec2CRCancelledState is the state CancelCapacityReservation puts a reservation into, from
// the operation's own first sentence: "Cancels the specified Capacity Reservation, releases
// the reserved capacity, and changes the Capacity Reservation's state to cancelled".
const ec2CRCancelledState = "cancelled"

// ec2CRInstancePlatforms are InstancePlatform's published Valid Values, all eighteen.
//
// Transcribed from the page in its own order rather than sorted, so it can be diffed against
// the Valid Values line by eye. The family names are not symmetrical and cannot be generated:
// there is a "RHEL with SQL Server Web" but **no "RHEL with HA and SQL Server Web"** — the
// HA family stops at Standard and Enterprise — and "Linux/UNIX" carries a slash where "SUSE
// Linux" does not. A list built by combining a base with a SQL Server edition would invent
// the missing one and accept a value AWS refuses.
var ec2CRInstancePlatforms = []string{
	"Linux/UNIX",
	"Red Hat Enterprise Linux",
	"SUSE Linux",
	"Windows",
	"Windows with SQL Server",
	"Windows with SQL Server Enterprise",
	"Windows with SQL Server Standard",
	"Windows with SQL Server Web",
	"Linux with SQL Server Standard",
	"Linux with SQL Server Web",
	"Linux with SQL Server Enterprise",
	"RHEL with SQL Server Standard",
	"RHEL with SQL Server Enterprise",
	"RHEL with SQL Server Web",
	"RHEL with HA",
	"RHEL with HA and SQL Server Standard",
	"RHEL with HA and SQL Server Enterprise",
	"Ubuntu Pro",
}

// ec2CRTenancies are Tenancy's published Valid Values, and ec2CRDefaultTenancy the value a
// request naming none reports.
//
// **`default`, not `shared`.** RunInstances spells the same concept "default | dedicated |
// host" and the Spot and fleet APIs use "shared" for it, so this is the spelling most likely
// to be guessed wrong. AWS publishes no `Default:` line for the parameter; `default` is
// substrate's reading, and it is the only one of the two values whose own name says it.
var ec2CRTenancies = []string{"default", "dedicated"}

// ec2CRDefaultTenancy is the tenancy a request naming none reports. See [ec2CRTenancies].
const ec2CRDefaultTenancy = "default"

// ec2CREndDateTypes are EndDateType's published Valid Values.
var ec2CREndDateTypes = []string{"unlimited", "limited"}

// ec2CREndDateTypeUnlimited and ec2CREndDateTypeLimited are the two EndDateType values,
// named because both halves of the mutual constraint on EndDate are stated in terms of them.
const (
	ec2CREndDateTypeUnlimited = "unlimited"
	ec2CREndDateTypeLimited   = "limited"
)

// ec2CRInstanceMatchCriteria are InstanceMatchCriteria's published Valid Values, and
// ec2CRDefaultMatchCriteria the published `Default: open`.
var ec2CRInstanceMatchCriteria = []string{"open", "targeted"}

// ec2CRDefaultMatchCriteria is InstanceMatchCriteria's published default.
const ec2CRDefaultMatchCriteria = "open"

// ec2CRDeliveryPreferences are DeliveryPreference's published Valid Values.
//
// The page adds "The only supported value is incremental", which is prose about AWS's
// delivery of future-dated capacity rather than a narrowing of the parameter's Valid Values
// line. Substrate delivers no capacity at all, so both values are accepted and echoed and
// neither changes an observation.
var ec2CRDeliveryPreferences = []string{"fixed", "incremental"}

// ec2CRCancellationCharges is ApplyCancellationCharges' published Valid Values — one value.
var ec2CRCancellationCharges = []string{"commitment-wind-down"}

// ec2CRReservationType is the value substrate's reservations report for reservationType,
// whose published Valid Values are "default | capacity-block".
//
// Always `default`: a Capacity Block is bought through
// PurchaseCapacityBlock, which substrate does not implement, and the page states that a
// Capacity Block cannot be modified or cancelled — so nothing here can produce one.
const ec2CRReservationType = "default"

// ec2CRCancellableStates are the states CancelCapacityReservation accepts, from the
// operation's own enumeration of them.
//
// Four states: assessing, scheduled, active and delayed. Three of the four belong to a
// future-dated reservation, which substrate does not model, so `active` is the only one
// reachable here today — the list is written in full anyway, because it is AWS's and a later
// change that made another state reachable should not have to rediscover it. AWS's own
// wording adds two conditions substrate cannot check: `scheduled` and a committed `active`
// reservation need a cancellation quote from
// CreateCapacityReservationCancellationQuote, which is a separate operation.
var ec2CRCancellableStates = []string{"assessing", "scheduled", "active", "delayed"}

// EC2CapacityReservation is a stored On-Demand Capacity Reservation.
//
// Every member of AWS's CapacityReservation shape is `Required: No`, so the record carries
// only what substrate can answer: what the request named, what substrate minted, and the
// state. Twelve of AWS's members are absent because nothing substrate models could set them —
// capacityBlockId, capacityReservationFleetId, commitmentInfo, interruptible,
// interruptibleCapacityAllocation, interruptionInfo, unusedReservationBillingOwnerId,
// zeroSizePreference and CapacityAllocationSet among them — and they are omitted rather than
// rendered empty, which is the honest-empty behavior #827 established.
type EC2CapacityReservation struct {
	// CapacityReservationID is the minted "cr-" identifier.
	CapacityReservationID string `json:"capacityReservationId"`

	// AccountID is the account that owns the reservation, reported as ownerId.
	AccountID string `json:"accountId"`

	// Region is the Region the reservation lives in. AWS: "The results describe only the
	// Capacity Reservations in the AWS Region that you're currently using."
	Region string `json:"region"`

	// AvailabilityZone is the zone name the reservation was created in, empty when the
	// request named neither AvailabilityZone nor AvailabilityZoneId.
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// InstanceType is the reserved instance type.
	InstanceType string `json:"instanceType"`

	// InstancePlatform is the reserved platform, one of [ec2CRInstancePlatforms].
	InstancePlatform string `json:"instancePlatform"`

	// Tenancy is the reserved tenancy, one of [ec2CRTenancies].
	Tenancy string `json:"tenancy"`

	// InstanceCount is the number of instances reserved, reported as totalInstanceCount and
	// — because substrate never consumes a reservation — as availableInstanceCount too until
	// the reservation is cancelled.
	InstanceCount int `json:"instanceCount"`

	// AvailableInstanceCount is the remaining capacity. Equal to InstanceCount until
	// CancelCapacityReservation releases it.
	AvailableInstanceCount int `json:"availableInstanceCount"`

	// EBSOptimized records the EbsOptimized request parameter.
	EBSOptimized bool `json:"ebsOptimized"`

	// EphemeralStorage records the EphemeralStorage request parameter, which AWS publishes
	// as `Deprecated.` — accepted, stored and echoed, and it means nothing.
	EphemeralStorage bool `json:"ephemeralStorage"`

	// State is the stored state. What a describe *reports* may differ: an active
	// reservation whose end date has passed reports expired. See [ec2CRObservedState].
	State string `json:"state"`

	// StartDate is when the reservation started, which for an immediate reservation is when
	// it was created.
	StartDate string `json:"startDate"`

	// EndDate is when the reservation expires, empty for an unlimited one.
	EndDate string `json:"endDate,omitempty"`

	// EndDateType is how the reservation ends, one of [ec2CREndDateTypes].
	EndDateType string `json:"endDateType"`

	// InstanceMatchCriteria is which instances may occupy the reservation, one of
	// [ec2CRInstanceMatchCriteria]. Recorded intent: nothing occupies one.
	InstanceMatchCriteria string `json:"instanceMatchCriteria"`

	// CreateDate is when the reservation was created.
	CreateDate string `json:"createDate"`

	// OutpostArn records the OutpostArn request parameter. Recorded intent: substrate models
	// no Outpost, and an Outpost reservation is observed exactly as any other.
	OutpostArn string `json:"outpostArn,omitempty"`

	// PlacementGroupArn records the PlacementGroupArn request parameter. Recorded intent,
	// and not checked against the account's placement groups: AWS documents no error for an
	// ARN naming none, and substrate places nothing.
	PlacementGroupArn string `json:"placementGroupArn,omitempty"`

	// DeliveryPreference records the DeliveryPreference request parameter. See
	// [ec2CRDeliveryPreferences].
	DeliveryPreference string `json:"deliveryPreference,omitempty"`

	// Tags are the reservation's tags. The member name is "tags" because
	// [ec2ApplyTagsToResource] reads and writes that member on every EC2 record, which is
	// what makes CreateTags and DeleteTags work on a cr- ID without a type switch.
	Tags []EC2Tag `json:"tags,omitempty"`
}

// arn returns the reservation's ARN, in AWS's published format
// arn:${Partition}:ec2:${Region}:${Account}:capacity-reservation/${CapacityReservationId}.
func (r EC2CapacityReservation) arn() string {
	return "arn:aws:ec2:" + r.Region + ":" + r.AccountID + ":capacity-reservation/" + r.CapacityReservationID
}

// generateCapacityReservationID mints a Capacity Reservation ID, "cr-" followed by
// seventeen hex characters in AWS's long-ID form.
//
// [randomHex] takes a **byte** count, so eight bytes is sixteen characters; the seventeenth
// comes from AWS's own shape, whose IDs are "cr-" plus seventeen. One character is dropped
// from a nine-byte draw rather than eight bytes being padded, so every character is minted.
func generateCapacityReservationID() string {
	return "cr-" + randomHex(9)[:17]
}

// ec2CapacityReservationStateKey returns the state key for a Capacity Reservation.
//
// The "cr" namespace is registered in [ec2TagScanTargets] and in [ec2TaggableResource], which
// is what makes a reservation's tags readable through DescribeTags and writable through
// CreateTags — and, since DescribeCapacityReservations documents no tag filter, DescribeTags
// is the documented route to finding a reservation by tag.
func ec2CapacityReservationStateKey(accountID, region, id string) string {
	return "cr:" + accountID + "/" + region + "/" + id
}

// createCapacityReservation handles CreateCapacityReservation.
//
// Validation runs before anything is written or read, in the parameter list's own order: the
// three required members, then the enums, then the pair rule on the zone, then the mutual
// constraint on EndDate, then the two parameters that request a future-dated reservation.
// Whether a request is well formed must not depend on what happens to be seeded, which is the
// ordering #887 established.
//
// The nominal outcome is an `active` reservation. A seeded outcome
// (POST /v1/ec2/capacity-reservation-outcomes) either fails the call with one of the five
// documented codes or creates the reservation in a non-nominal state, which is how a test
// exercises the capacity and quota paths that are rare, slow and expensive to trigger against
// real AWS (#891). See [ec2CapacityReservationOutcome].
func (p *EC2Plugin) createCapacityReservation(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instanceType := req.Params["InstanceType"]
	if instanceType == "" {
		return nil, ec2MissingParameter("InstanceType")
	}
	platform := req.Params["InstancePlatform"]
	if platform == "" {
		return nil, ec2MissingParameter("InstancePlatform")
	}
	if !containsStr(ec2CRInstancePlatforms, platform) {
		return nil, ec2CRValueError(platform, "instancePlatform", ec2CROneOf(ec2CRInstancePlatforms))
	}
	count, awsErr := ec2CRInstanceCount(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	tenancy := req.Params["Tenancy"]
	if tenancy == "" {
		tenancy = ec2CRDefaultTenancy
	} else if !containsStr(ec2CRTenancies, tenancy) {
		return nil, ec2CRValueError(tenancy, "tenancy", ec2CROneOf(ec2CRTenancies))
	}
	matchCriteria := req.Params["InstanceMatchCriteria"]
	if matchCriteria == "" {
		matchCriteria = ec2CRDefaultMatchCriteria
	} else if !containsStr(ec2CRInstanceMatchCriteria, matchCriteria) {
		return nil, ec2CRValueError(matchCriteria, "instanceMatchCriteria",
			ec2CROneOf(ec2CRInstanceMatchCriteria))
	}
	delivery := req.Params["DeliveryPreference"]
	if delivery != "" && !containsStr(ec2CRDeliveryPreferences, delivery) {
		return nil, ec2CRValueError(delivery, "deliveryPreference", ec2CROneOf(ec2CRDeliveryPreferences))
	}
	zone, awsErr := ec2CRZone(reqCtx.Region, req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	endDate, endDateType, awsErr := ec2CREndDate(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CRRefuseFutureDated(req.Params); awsErr != nil {
		return nil, awsErr
	}

	tags := ec2TagSpecificationTags(req.Params, "", "capacity-reservation")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}

	seed, err := p.resolveCapacityReservationOutcome(instanceType, zone)
	if err != nil {
		return nil, err
	}
	if seed != nil && seed.ErrorCode != "" {
		return nil, ec2CRSeededError(seed)
	}
	state := ec2CRNominalState
	if seed != nil && seed.State != "" {
		state = seed.State
	}

	now := p.tc.Now().UTC().Format(time.RFC3339)
	reservation := EC2CapacityReservation{
		CapacityReservationID:  generateCapacityReservationID(),
		AccountID:              reqCtx.AccountID,
		Region:                 reqCtx.Region,
		AvailabilityZone:       zone,
		InstanceType:           instanceType,
		InstancePlatform:       platform,
		Tenancy:                tenancy,
		InstanceCount:          count,
		AvailableInstanceCount: count,
		EBSOptimized:           req.Params["EbsOptimized"] == "true",
		EphemeralStorage:       req.Params["EphemeralStorage"] == "true",
		State:                  state,
		StartDate:              now,
		EndDate:                endDate,
		EndDateType:            endDateType,
		InstanceMatchCriteria:  matchCriteria,
		CreateDate:             now,
		OutpostArn:             req.Params["OutpostArn"],
		PlacementGroupArn:      req.Params["PlacementGroupArn"],
		DeliveryPreference:     delivery,
		Tags:                   tags,
	}

	data, err := json.Marshal(reservation)
	if err != nil {
		return nil, fmt.Errorf("ec2 createCapacityReservation marshal: %w", err)
	}
	key := ec2CapacityReservationStateKey(reqCtx.AccountID, reqCtx.Region, reservation.CapacityReservationID)
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createCapacityReservation state.Put: %w", err)
	}

	type response struct {
		XMLName     xml.Name                      `xml:"CreateCapacityReservationResponse"`
		XMLNS       string                        `xml:"xmlns,attr"`
		Reservation ec2CapacityReservationItemXML `xml:"capacityReservation"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:       "http://ec2.amazonaws.com/doc/2016-11-15/",
		Reservation: ec2CapacityReservationItem(reservation, p.tc.Now()),
	})
}

// describeCapacityReservations handles DescribeCapacityReservations.
//
// The answer is assembled in the order [EC2Plugin.describeTags] established: refuse an
// undocumented filter name, validate MaxResults and NextToken, then scan, filter and page.
// Refusal comes first so that whether a request is well formed never depends on how many
// reservations happen to exist.
//
// CapacityReservationId.N **narrows** and does not assert, and this is the one place the two
// documented ID errors are treated differently. A **malformed** ID is refused
// ([ec2CRValidateID]), because the request itself is wrong. An ID that is well formed but names
// no reservation contributes nothing rather than answering
// InvalidCapacityReservationId.NotFound: the operation's Errors section is the common-types
// boilerplate, so nothing documents which of the two a describe does, and narrowing is the
// reading [EC2Plugin.describeFleets] already applies to FleetId.N — the one that cannot turn a
// reaper's sweep over a list of IDs into a refusal because one of them had already been
// released and swept. AWS's own behavior here is UNVERIFIED, and a caller that needs the
// distinction gets it from the count of items it asked for versus the count it received.
//
// Naming an ID list *and* MaxResults is refused by [ec2RefuseIDsWithMaxResults], the rule
// EC2 publishes once for the whole service rather than per operation — so it reaches an
// operation whose own page repeats none of it, and this operation is not the exception.
func (p *EC2Plugin) describeCapacityReservations(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if awsErr := ec2CapacityReservationFilterSpec().check(req.Params); awsErr != nil {
		return nil, awsErr
	}
	ids := extractIndexedParams(req.Params, "CapacityReservationId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "CapacityReservationId", ids); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2CRMinMaxResults, ec2CRMaxMaxResults)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	for _, id := range ids {
		if awsErr := ec2CRValidateID(id); awsErr != nil {
			return nil, awsErr
		}
	}
	filters := extractEC2Filters(req.Params)

	goCtx := context.Background()
	prefix := ec2StatePrefix("cr", reqCtx.AccountID, reqCtx.Region)
	keys, err := p.state.List(goCtx, ec2Namespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("ec2 describeCapacityReservations: %w", err)
	}

	now := p.tc.Now()
	var items []ec2CapacityReservationItemXML
	for _, key := range keys {
		data, getErr := p.state.Get(goCtx, ec2Namespace, key)
		if getErr != nil || data == nil {
			continue
		}
		var reservation EC2CapacityReservation
		if json.Unmarshal(data, &reservation) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, reservation.CapacityReservationID) {
			continue
		}
		item := ec2CapacityReservationItem(reservation, now)
		if !ec2CapacityReservationMatchesFilters(item, filters) {
			continue
		}
		items = append(items, item)
	}

	type response struct {
		XMLName      xml.Name                        `xml:"DescribeCapacityReservationsResponse"`
		XMLNS        string                          `xml:"xmlns,attr"`
		Reservations []ec2CapacityReservationItemXML `xml:"capacityReservationSet>item"`
		NextToken    string                          `xml:"nextToken,omitempty"`
	}
	page, next := ec2Page(items, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:        "http://ec2.amazonaws.com/doc/2016-11-15/",
		Reservations: page,
		NextToken:    next,
	})
}

// cancelCapacityReservation handles CancelCapacityReservation.
//
// AWS: "Cancels the specified Capacity Reservation, releases the reserved capacity, and
// changes the Capacity Reservation's state to cancelled." The released capacity is what
// availableInstanceCount reports, so it becomes zero while totalInstanceCount keeps reporting
// what was reserved — nothing published says what either becomes, and of the two readings
// this is the one that does not report remaining capacity on a reservation that has none.
//
// The response is `requestId` and `return`, not a reservation structure, so a caller that
// wants the cancelled state reads it back through DescribeCapacityReservations.
func (p *EC2Plugin) cancelCapacityReservation(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CapacityReservationId"]
	if id == "" {
		return nil, ec2MissingParameter("CapacityReservationId")
	}
	if awsErr := ec2CRValidateID(id); awsErr != nil {
		return nil, awsErr
	}
	if charges := req.Params["ApplyCancellationCharges"]; charges != "" &&
		!containsStr(ec2CRCancellationCharges, charges) {
		return nil, ec2CRValueError(charges, "applyCancellationCharges",
			ec2CROneOf(ec2CRCancellationCharges))
	}

	goCtx := context.Background()
	key := ec2CapacityReservationStateKey(reqCtx.AccountID, reqCtx.Region, id)
	data, err := p.state.Get(goCtx, ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 cancelCapacityReservation get: %w", err)
	}
	if data == nil {
		return nil, ec2CRNotFound(id)
	}
	var reservation EC2CapacityReservation
	if err := json.Unmarshal(data, &reservation); err != nil {
		return nil, fmt.Errorf("ec2 cancelCapacityReservation unmarshal: %w", err)
	}

	// The state a *describe* would report, not the stored one: a reservation whose end date
	// has passed is expired and cannot be cancelled, and reading the stored `active` here
	// would let it be.
	state := ec2CRObservedState(reservation, p.tc.Now())
	if !containsStr(ec2CRCancellableStates, state) {
		return nil, ec2CRNotCancellable(id, state)
	}

	reservation.State = ec2CRCancelledState
	reservation.AvailableInstanceCount = 0
	updated, err := json.Marshal(reservation)
	if err != nil {
		return nil, fmt.Errorf("ec2 cancelCapacityReservation marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ec2Namespace, key, updated); err != nil {
		return nil, fmt.Errorf("ec2 cancelCapacityReservation state.Put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"CancelCapacityReservationResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:  "http://ec2.amazonaws.com/doc/2016-11-15/",
		Return: true,
	})
}

// ec2CapacityReservationItemXML is AWS's capacityReservation element.
//
// Member order is the CapacityReservation type page's own, which is alphabetical: no page
// publishes a sample response for any of the three operations, so there is no rendered order
// to copy and the page's order is the only one with a citation behind it.
//
// omitempty on the six members substrate can leave unset, so an element is absent rather
// than present-and-empty when the request named nothing for it — an SDK tells those two
// apart, and #827 established that substrate does not report a value nothing wrote. The
// counts, the booleans and the six members every reservation has carry no omitempty: zero and
// false are real answers there.
//
// tagSet is the seventh, and it needs [ec2TagSetXML]'s pointer-to-wrapper shape rather than
// `xml:"tagSet>item,omitempty"` for the reason that type's doc comment gives: omitempty
// suppresses the items and encoding/xml still writes the parent, so an untagged reservation
// would answer <tagSet></tagSet>. Which of the two an operation should answer is normally
// settled by its own samples, and this operation has none — so it follows the shape the
// members beside it follow, an absent element for a value nothing wrote.
type ec2CapacityReservationItemXML struct {
	AvailabilityZone       string        `xml:"availabilityZone,omitempty"`
	AvailabilityZoneID     string        `xml:"availabilityZoneId,omitempty"`
	AvailableInstanceCount int           `xml:"availableInstanceCount"`
	CapacityReservationARN string        `xml:"capacityReservationArn"`
	CapacityReservationID  string        `xml:"capacityReservationId"`
	CreateDate             string        `xml:"createDate"`
	DeliveryPreference     string        `xml:"deliveryPreference,omitempty"`
	EBSOptimized           bool          `xml:"ebsOptimized"`
	EndDate                string        `xml:"endDate,omitempty"`
	EndDateType            string        `xml:"endDateType"`
	EphemeralStorage       bool          `xml:"ephemeralStorage"`
	InstanceMatchCriteria  string        `xml:"instanceMatchCriteria"`
	InstancePlatform       string        `xml:"instancePlatform"`
	InstanceType           string        `xml:"instanceType"`
	OutpostARN             string        `xml:"outpostArn,omitempty"`
	OwnerID                string        `xml:"ownerId"`
	PlacementGroupARN      string        `xml:"placementGroupArn,omitempty"`
	ReservationType        string        `xml:"reservationType"`
	StartDate              string        `xml:"startDate"`
	State                  string        `xml:"state"`
	Tags                   *ec2TagSetXML `xml:"tagSet,omitempty"`
	Tenancy                string        `xml:"tenancy"`
	TotalInstanceCount     int           `xml:"totalInstanceCount"`
}

// ec2CapacityReservationItem renders a stored reservation as observed at now.
//
// The zone **ID** is derived from the zone name through [ec2SeededZones] rather than stored,
// which is the same single derivation DescribeAvailabilityZones and CreateVolume share: a
// zone ID a caller reads out of one operation is a zone ID the others accept. A zone name
// substrate does not seed yields no ID rather than a computed one — a name is recorded as
// given and never validated, per the asymmetry [ec2VolumeZone] documents, so an unseeded name
// is reachable and inventing an ID for it would report a zone that does not exist.
func ec2CapacityReservationItem(r EC2CapacityReservation, now time.Time) ec2CapacityReservationItemXML {
	var zoneID string
	for _, zone := range ec2SeededZones(r.Region) {
		if zone.ZoneName == r.AvailabilityZone {
			zoneID = zone.ZoneID
			break
		}
	}
	return ec2CapacityReservationItemXML{
		AvailabilityZone:       r.AvailabilityZone,
		AvailabilityZoneID:     zoneID,
		AvailableInstanceCount: r.AvailableInstanceCount,
		CapacityReservationARN: r.arn(),
		CapacityReservationID:  r.CapacityReservationID,
		CreateDate:             r.CreateDate,
		DeliveryPreference:     r.DeliveryPreference,
		EBSOptimized:           r.EBSOptimized,
		EndDate:                r.EndDate,
		EndDateType:            r.EndDateType,
		EphemeralStorage:       r.EphemeralStorage,
		InstanceMatchCriteria:  r.InstanceMatchCriteria,
		InstancePlatform:       r.InstancePlatform,
		InstanceType:           r.InstanceType,
		OutpostARN:             r.OutpostArn,
		OwnerID:                r.AccountID,
		PlacementGroupARN:      r.PlacementGroupArn,
		ReservationType:        ec2CRReservationType,
		StartDate:              r.StartDate,
		State:                  ec2CRObservedState(r, now),
		Tags:                   ec2TagSet(r.Tags),
		Tenancy:                r.Tenancy,
		TotalInstanceCount:     r.InstanceCount,
	}
}

// ec2CRObservedState reports the state a reservation is observed in at now, which is the
// stored state except that an active limited reservation whose end date has passed is expired.
//
// AWS states the transition on the endDate member — "The Capacity Reservation's state changes
// to expired when it reaches its end date and time" — so it is a time-ordered progression
// driven by the simulated clock rather than a stored value, which is what lets a test assert
// it without depending on wall-clock time: a reservation created with an end date already in
// the past is expired on its first observation.
//
// Only an `active` reservation expires. A cancelled or failed one has already reached a
// terminal state, and reporting `expired` for it would lose the outcome a test seeded. An
// unparseable stored end date leaves the state alone: nothing here should turn a bad
// timestamp into a state transition, and [ec2CREndDate] refuses one at the door.
func ec2CRObservedState(r EC2CapacityReservation, now time.Time) string {
	if r.State != ec2CRNominalState || r.EndDate == "" {
		return r.State
	}
	end, err := time.Parse(time.RFC3339, r.EndDate)
	if err != nil {
		return r.State
	}
	if now.After(end) {
		return ec2CRExpiredState
	}
	return r.State
}

// ec2CRInstanceCount reads InstanceCount and checks it against its range.
//
// The parameter is `Required: Yes`, so an absent one is MissingParameter rather than
// InvalidParameterValue: the two say different things to a caller, and the page publishes the
// requirement as a requirement rather than as a value constraint. The range is prose — see
// [ec2CRMinInstanceCount] — and is enforced as a refusal rather than a clamp, following every
// other published range in the package.
func ec2CRInstanceCount(params map[string]string) (int, *AWSError) {
	raw := params["InstanceCount"]
	if raw == "" {
		return 0, &AWSError{
			Code:       "MissingParameter",
			Message:    "The request must contain the parameter InstanceCount",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, ec2CRValueError(raw, "instanceCount", "It must be an integer.")
	}
	if n < ec2CRMinInstanceCount || n > ec2CRMaxInstanceCount {
		return 0, ec2CRValueError(raw, "instanceCount", fmt.Sprintf("It must be between %d and %d.",
			ec2CRMinInstanceCount, ec2CRMaxInstanceCount))
	}
	return n, nil
}

// ec2CRZone resolves the reservation's zone from AvailabilityZone or AvailabilityZoneId.
//
// Both are `Required: No` and the page states no rule on the pair, unlike CreateVolume's
// "Either AvailabilityZone or AvailabilityZoneId must be specified, but not both" — so
// neither an absent zone nor both spellings at once is refused here for being absent or
// duplicated. What *is* refused is a pair that disagrees: two names for one zone cannot both
// be answered, and AWS's model has no way to say which wins.
//
// A zone **ID** must resolve, because it has to be translated into the name the record and
// every response carry; a zone **name** is recorded as given and not checked against
// [ec2SeededZones], which is the asymmetry [ec2VolumeZone] documents and the one every
// zone-taking operation in the tree follows.
//
// A request naming neither yields the empty string, and the two zone members are then omitted
// from the response rather than filled in. AWS picks a zone in that case and publishes no rule
// for which; inventing one would report a reservation in a zone the caller never named, which
// is the failure #712 recorded for CreateVolume.
func ec2CRZone(region string, params map[string]string) (string, *AWSError) {
	name, id := params["AvailabilityZone"], params["AvailabilityZoneId"]
	if id == "" {
		return name, nil
	}
	for _, zone := range ec2SeededZones(region) {
		if zone.ZoneID != id {
			continue
		}
		if name != "" && name != zone.ZoneName {
			return "", &AWSError{
				Code: "InvalidParameterCombination",
				Message: fmt.Sprintf(
					"AvailabilityZone '%s' and AvailabilityZoneId '%s' name different zones",
					name, id),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		return zone.ZoneName, nil
	}
	return "", ec2CRValueError(id, "availabilityZoneId",
		fmt.Sprintf("No such Availability Zone in %s.", region))
}

// ec2CREndDate resolves EndDate and EndDateType, enforcing the mutual constraint the page
// states in both directions.
//
// AWS, on EndDateType: `limited` — "You must provide an EndDate value if the EndDateType
// value is limited"; `unlimited` — "Do not provide an EndDate if the EndDateType is
// unlimited." Exactly those two combinations are refused.
//
// An absent EndDateType is **inferred** from the presence of an EndDate rather than defaulted
// to unlimited: the page publishes no `Default:` line for the parameter, so defaulting to
// unlimited would turn a request naming only an EndDate — which AWS's prose does not
// forbid — into a refusal, and refusing a request AWS accepts is the divergence that costs a
// caller a working request. Inference enforces the documented rule and nothing beyond it.
//
// The timestamp is parsed and re-rendered in RFC 3339 UTC so that the stored value, the
// rendered element and the end-date filter all compare the same string, and so that
// [ec2CRObservedState] has a timestamp it can read. AWS's type is Timestamp and no page
// publishes a format, so the normalization is substrate's; a value that parses as neither
// RFC 3339 nor a bare date is refused rather than stored, since a stored value nothing can
// parse would silently disable the expiry transition.
func ec2CREndDate(params map[string]string) (endDate, endDateType string, awsErr *AWSError) {
	raw := params["EndDate"]
	endDateType = params["EndDateType"]
	if endDateType != "" && !containsStr(ec2CREndDateTypes, endDateType) {
		return "", "", ec2CRValueError(endDateType, "endDateType", ec2CROneOf(ec2CREndDateTypes))
	}
	switch {
	case endDateType == "":
		endDateType = ec2CREndDateTypeUnlimited
		if raw != "" {
			endDateType = ec2CREndDateTypeLimited
		}
	case endDateType == ec2CREndDateTypeLimited && raw == "":
		return "", "", &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "You must provide an EndDate value if the EndDateType value is limited",
			HTTPStatus: http.StatusBadRequest,
		}
	case endDateType == ec2CREndDateTypeUnlimited && raw != "":
		return "", "", &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "Do not provide an EndDate if the EndDateType is unlimited",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if raw == "" {
		return "", endDateType, nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC().Format(time.RFC3339), endDateType, nil
		}
	}
	return "", "", ec2CRValueError(raw, "endDate", "It must be a timestamp.")
}

// ec2CRRefuseFutureDated refuses the two parameters that request a future-dated Capacity
// Reservation.
//
// StartDate ("The date and time at which the future-dated Capacity Reservation should start")
// and CommitmentDuration are the two, and a future-dated reservation is a different observable
// thing from an immediate one: it is assessed, then scheduled, then delivered or delayed or
// unsupported, which is eight of the state member's thirteen values and a delivery model
// substrate has none of. Accepting either parameter and answering `active` would report that
// capacity two days out is available now — a false observation with nothing in it to tell a
// caller substrate did not model the request.
//
// The code is substrate's reading. CreateCapacityReservation's Errors section is empty, and
// `Unsupported` is the code in EC2's client-error table whose gloss covers this shape: "The
// specified request is unsupported." docs/services.md records the refusal so it is discovered
// there rather than at a call.
func ec2CRRefuseFutureDated(params map[string]string) *AWSError {
	for _, param := range []string{"StartDate", "CommitmentDuration"} {
		if params[param] == "" {
			continue
		}
		return &AWSError{
			Code: "Unsupported",
			Message: "A future-dated Capacity Reservation is not supported; " +
				param + " must not be specified",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// ec2CRValueError builds the InvalidParameterValue error for a bad parameter value, in the
// lowercase-parameter form EC2's value-error messages use — see [ec2InvalidInstanceCount],
// which carries the reasoning for the shape.
func ec2CRValueError(value, wireName, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("Invalid value '%s' for parameter %s. %s", value, wireName, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CROneOf renders an enum's published Valid Values as the reason half of a value error.
func ec2CROneOf(values []string) string {
	return "It must be one of " + strings.Join(values, ", ") + "."
}

// ec2CRIDBodyLen is the number of characters a Capacity Reservation ID carries after "cr-".
//
// Seventeen, from the form errors-overview.html publishes in
// InvalidCapacityReservationId.Malformed's own description: "Ensure that you specify the
// Capacity Reservation ID in the form cr-xxxxxxxxxxxxxxxxx." That is the only place in AWS's
// documentation the ID's shape appears at all — no page publishes a `Pattern:` line for
// CapacityReservationId — which is why the length is checked and the **character class is
// not**: `x` is AWS's placeholder and nothing states it means a hex digit, so refusing a
// non-hex character would be substrate inventing a rule.
const ec2CRIDBodyLen = 17

// ec2CRValidateID refuses an ID that is not of AWS's published form.
//
// AWS: InvalidCapacityReservationId.Malformed — "The ID for the Capacity Reservation is
// malformed. Ensure that you specify the Capacity Reservation ID in the form
// cr-xxxxxxxxxxxxxxxxx." The code is in errors-overview.html's client table, so it carries a
// 400.
//
// This is checked wherever an ID is accepted, including DescribeCapacityReservations — where
// an ID that names no reservation narrows silently rather than refusing. The two are not in
// tension: a *malformed* ID is a caller mistake in the request itself, where a well-formed one
// naming nothing is a question with an honest empty answer, and an ID substrate minted is
// always well formed however stale it has become. So a describe sweeping a list of cancelled
// IDs still succeeds, while `cr-typo` is refused rather than silently matching nothing.
func ec2CRValidateID(id string) *AWSError {
	if strings.HasPrefix(id, "cr-") && len(id) == len("cr-")+ec2CRIDBodyLen {
		return nil
	}
	return &AWSError{
		Code: "InvalidCapacityReservationId.Malformed",
		Message: "The ID for the Capacity Reservation is malformed. Ensure that you specify " +
			"the Capacity Reservation ID in the form cr-xxxxxxxxxxxxxxxxx",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CRNotFound refuses an ID no reservation is stored under.
//
// AWS: InvalidCapacityReservationId.NotFound — "The specified Capacity Reservation ID does not
// exist." The code is published in errors-overview.html's client table, so the code string
// itself comes from the API model rather than from observation; note the casing is
// `...ReservationId.NotFound` with a lowercase `d`, unlike the sibling
// InvalidInstanceID.NotFound and InvalidAllocationID.NotFound, which spell it `ID`.
//
// **The message wording is observed behavior, not published.** None of the three operation
// pages enumerates errors, so the phrasing follows a real AWS response pasted into
// hashicorp/terraform-provider-aws#36716 — "The capacity reservation ID 'cr-…' was not found"
// — rather than the error table's own description, which is written for a reader of the table.
// Consumers that special-case the code (terraform-provider-aws, karpenter-provider-aws,
// steampipe, dstack) corroborate the code string but not the wording.
//
// Only CancelCapacityReservation answers this. See
// [EC2Plugin.describeCapacityReservations] for why a describe does not.
func ec2CRNotFound(id string) *AWSError {
	return &AWSError{
		Code:       "InvalidCapacityReservationId.NotFound",
		Message:    fmt.Sprintf("The capacity reservation ID '%s' was not found", id),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CRNotCancellable refuses a cancel of a reservation whose state does not allow one.
//
// **The code is substrate's reading, and AWS publishes nothing here.**
// CancelCapacityReservation's Errors section is the common-types boilerplate; a search of
// EC2's whole error-code reference turns up no code for this case — the only state-shaped one
// is InvalidCapacityReservationState.PendingActivation, "Your Capacity Block is not active
// yet", which is about a Capacity Block rather than a non-cancellable ODCR state, and neither
// a bare `InvalidCapacityReservationState` nor an `IncorrectCapacityReservationState` appears
// anywhere. AWS's own sample tooling avoids the question: aws-samples/on-demand-capacity-
// reservations' cancelODCR.py pre-checks the state through a describe and prints a message
// rather than catching an error, which is itself evidence that no dedicated code exists.
//
// `IncorrectState` is the code chosen, from EC2's client-error table — "The resource is in an
// incorrect state for the request" — and it is the one substrate already answers for this
// shape elsewhere: a volume that is not `available` (see [EC2Plugin.attachVolume]) and a
// snapshot that is not usable yet ([ec2CheckMappingSnapshot]). One reading applied
// consistently is worth more here than a second invented code, since a consumer cannot match
// on a code AWS does not publish either way.
//
// The message names the state, so a caller that must branch has the fact the code does not
// carry — and the states that *are* cancellable are AWS's own list, [ec2CRCancellableStates].
func ec2CRNotCancellable(id, state string) *AWSError {
	return &AWSError{
		Code: "IncorrectState",
		Message: fmt.Sprintf(
			"The capacity reservation %s is in the %s state and cannot be cancelled. "+
				"To cancel a Capacity Reservation, its state must be one of %s",
			id, state, strings.Join(ec2CRCancellableStates, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CapacityReservationMatchesFilters reports whether a reservation satisfies every supplied
// DescribeCapacityReservations filter.
//
// It takes the rendered item rather than the record, so a filter compares the value the
// response reports: the state filter matches `expired` for a reservation whose end date has
// passed, and the availability-zone filter has a zone to compare even though the record and
// the response spell it the same. A filter that disagreed with the element beside it would be
// worse than no filter at all.
func ec2CapacityReservationMatchesFilters(item ec2CapacityReservationItemXML, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2CapacityReservationMatchesFilter(item, name, values) {
			return false
		}
	}
	return true
}

// ec2CapacityReservationMatchesFilter evaluates a single DescribeCapacityReservations filter.
//
// All twelve of AWS's documented names are evaluated; see
// [ec2CapacityReservationFilterSpec] for why there is no tag filter to evaluate and no inert
// name to skip. The two date filters compare the string substrate renders, which is the
// reading [ec2SnapshotMatchesFilter] records for start-time: AWS documents no matching
// semantics of its own for a date filter, so a caller wanting a range asks with a wildcard.
func ec2CapacityReservationMatchesFilter(item ec2CapacityReservationItemXML, name string, values []string) bool {
	switch name {
	case "availability-zone":
		return ec2FilterAccepts(values, item.AvailabilityZone)
	case "end-date":
		return ec2FilterAccepts(values, item.EndDate)
	case "end-date-type":
		return ec2FilterAccepts(values, item.EndDateType)
	case "instance-match-criteria":
		return ec2FilterAccepts(values, item.InstanceMatchCriteria)
	case "instance-platform":
		return ec2FilterAccepts(values, item.InstancePlatform)
	case "instance-type":
		return ec2FilterAccepts(values, item.InstanceType)
	case "outpost-arn":
		return ec2FilterAccepts(values, item.OutpostARN)
	case "owner-id":
		// The account that owns the reservation, which is always the requesting account:
		// substrate is single-account, so this filter either matches everything or nothing.
		// It is still evaluated rather than inert, for the reason
		// [ec2SnapshotMatchesFilter] records — a caller naming another account is asking a
		// question whose honest answer is "none".
		return ec2FilterAccepts(values, item.OwnerID)
	case "placement-group-arn":
		return ec2FilterAccepts(values, item.PlacementGroupARN)
	case "start-date":
		return ec2FilterAccepts(values, item.StartDate)
	case "state":
		// The state the response reports, which for an active reservation past its end date
		// is expired. Note AWS's own filter documentation lists five values here where the
		// state member publishes thirteen; nothing is validated either way, since a filter
		// comparing strings simply matches nothing for a value no reservation carries.
		return ec2FilterAccepts(values, item.State)
	case "tenancy":
		return ec2FilterAccepts(values, item.Tenancy)
	default:
		return true
	}
}
