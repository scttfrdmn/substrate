package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Spot Instance requests (#1436).
//
// RequestSpotInstances, DescribeSpotInstanceRequests and CancelSpotInstanceRequests were
// unrouted through v0.125.0 — InvalidAction — and RunInstances ignored
// InstanceMarketOptions, so a launch asking for a Spot Instance got an On-Demand one with no
// request behind it. A consumer whose terminate path cancels the instance's Spot request first
// (spawn does, because a persistent request otherwise launches a replacement) could not drive
// that path at all.
//
// What is modeled is what the API reports: a sir- request per instance, linked both ways
// (SpotInstanceRequestId on the instance, InstanceId on the request), with the published
// state and status codes, tags, filters and pagination; cancellation, which "does not
// terminate running Spot Instances associated with the request"; and the one behavior a fake
// cannot stand in for — terminating the instance of an active persistent request launches a
// replacement from the request, with no tags, and moves the request's InstanceId to it.
// Spot pricing, interruption and rebalancing are not: an instance runs until something
// terminates it. A capacity shortfall is seedable instead — see
// [EC2Plugin.spotRequestOutcomeSeed].

// ec2InstanceLifecycleSpot is Instance.instanceLifecycle for a Spot Instance.
const ec2InstanceLifecycleSpot = "spot"

// The Spot Instance request states SpotInstanceRequest publishes: "open | active | closed |
// cancelled | failed".
const (
	ec2SpotStateOpen      = "open"
	ec2SpotStateActive    = "active"
	ec2SpotStateClosed    = "closed"
	ec2SpotStateCancelled = "cancelled"
)

// The Spot request status codes substrate reports, from the EC2 User Guide's "Spot request
// status codes" list, each with the message substrate renders for it.
const (
	ec2SpotStatusFulfilled            = "fulfilled"
	ec2SpotStatusCanceledRunning      = "request-canceled-and-instance-running"
	ec2SpotStatusCanceledBeforeFulfil = "canceled-before-fulfillment"
	ec2SpotStatusTerminatedByUser     = "instance-terminated-by-user"
	ec2SpotStatusCapacityNotAvailable = "capacity-not-available"
	ec2SpotStatusPriceTooLow          = "price-too-low"
	ec2SpotStatusBadParameters        = "bad-parameters"
	ec2SpotStatusScheduleExpired      = "schedule-expired"
)

// ec2SpotStatusMessages are the status messages substrate renders, AWS's own explanations
// from the status-code list reworded as a request's message.
var ec2SpotStatusMessages = map[string]string{
	ec2SpotStatusFulfilled:            "Your Spot request is fulfilled.",
	ec2SpotStatusCanceledRunning:      "Spot Instance request canceled, but the instance remains running.",
	ec2SpotStatusCanceledBeforeFulfil: "Your Spot request was canceled before it was fulfilled.",
	ec2SpotStatusTerminatedByUser:     "Spot Instance terminated due to user-initiated termination.",
	ec2SpotStatusCapacityNotAvailable: "There is not enough capacity available for the instances that you requested.",
	ec2SpotStatusPriceTooLow:          "Your maximum price is below the current Spot price.",
	ec2SpotStatusBadParameters:        "One or more parameters for your Spot request are not valid.",
	ec2SpotStatusScheduleExpired:      "Your Spot request expired because it was not fulfilled before the specified date.",
	// The two an experiment's interruption records (#1619); see ec2_internal_actions.go.
	ec2SpotStatusTerminatedByExperiment: "Spot Instance terminated by an AWS FIS experiment.",
	ec2SpotStatusStoppedByExperiment:    "Spot Instance stopped by an AWS FIS experiment.",
}

// ec2SpotSeedableStatuses are the outcomes the control plane may seed: the two status codes
// that leave a request open with no instance, which is the state a consumer's wait loop
// exists to handle.
var ec2SpotSeedableStatuses = []string{ec2SpotStatusCapacityNotAvailable, ec2SpotStatusPriceTooLow}

// ec2SpotInstanceRequestIDKind is the sir- ID family, with the codes the client-error table
// publishes: InvalidSpotInstanceRequestID.NotFound ("The specified Spot Instance request ID
// does not exist") and InvalidSpotInstanceRequestID.Malformed ("Ensure that you specify the
// Spot Instance request ID in the form sir-xxxxxxxx").
var ec2SpotInstanceRequestIDKind = ec2IDKind{
	Prefix: "sir-", NotFound: "InvalidSpotInstanceRequestID.NotFound",
	Malformed: "InvalidSpotInstanceRequestID.Malformed", Noun: "Spot Instance request",
}

// generateSpotInstanceRequestID mints a sir- ID: eight hex characters, the form the
// Malformed code's own message gives.
func generateSpotInstanceRequestID(m *IDMint) string {
	return "sir-" + m.Hex(4)
}

// ec2SpotRequestStateKey is the state key of one Spot Instance request.
func ec2SpotRequestStateKey(accountID, region, id string) string {
	return "spot_request:" + accountID + "/" + region + "/" + id
}

// EC2SpotLaunchSpecification is the launch a Spot Instance request records, the subset of
// AWS's LaunchSpecification substrate reports.
type EC2SpotLaunchSpecification struct {
	// ImageID is the AMI.
	ImageID string `json:"image_id"`

	// InstanceType is the instance type.
	InstanceType string `json:"instance_type"`

	// KeyName is the key pair, if any.
	KeyName string `json:"key_name,omitempty"`

	// SubnetID is the subnet the instance launches into.
	SubnetID string `json:"subnet_id,omitempty"`

	// SecurityGroupIDs are the instance's security groups.
	SecurityGroupIDs []string `json:"security_group_ids,omitempty"`

	// AvailabilityZone is the launch's placement zone.
	AvailabilityZone string `json:"availability_zone,omitempty"`
}

// EC2SpotInstanceRequest is one Spot Instance request (#1436).
type EC2SpotInstanceRequest struct {
	// SpotInstanceRequestID is the request's sir- ID.
	SpotInstanceRequestID string `json:"spot_instance_request_id"`

	// Type is one-time or persistent.
	Type string `json:"type"`

	// State is open, active, closed, cancelled or failed.
	State string `json:"state"`

	// StatusCode and StatusMessage are the request's status; StatusUpdateTime is when it
	// last changed, in RFC 3339.
	StatusCode       string `json:"status_code"`
	StatusMessage    string `json:"status_message"`
	StatusUpdateTime string `json:"status_update_time"`

	// InstanceID is the instance fulfilling the request, empty while it has none.
	InstanceID string `json:"instance_id,omitempty"`

	// InstanceInterruptionBehavior is hibernate, stop or terminate.
	InstanceInterruptionBehavior string `json:"instance_interruption_behavior"`

	// SpotPrice is the maximum price the request named, empty when it named none.
	SpotPrice string `json:"spot_price,omitempty"`

	// ValidUntil is the request's end date in RFC 3339, empty for none.
	ValidUntil string `json:"valid_until,omitempty"`

	// LaunchGroup and AvailabilityZoneGroup are recorded as given and not enforced.
	LaunchGroup           string `json:"launch_group,omitempty"`
	AvailabilityZoneGroup string `json:"availability_zone_group,omitempty"`

	// CreateTime is when the request was created, in RFC 3339.
	CreateTime string `json:"create_time"`

	// ProductDescription is Linux/UNIX or Windows, from the AMI's platform.
	ProductDescription string `json:"product_description"`

	// LaunchedAvailabilityZone is the zone the current instance launched in.
	LaunchedAvailabilityZone string `json:"launched_availability_zone,omitempty"`

	// LaunchSpecification is the launch the request records.
	LaunchSpecification EC2SpotLaunchSpecification `json:"launch_specification"`

	// LaunchParams are the RunInstances parameters a replacement is launched from: the
	// request's own, less its tags, counts and market options.
	LaunchParams map[string]string `json:"launch_params,omitempty"`

	// Tags holds the request's tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the request.
	AccountID string `json:"account_id"`

	// Region is the AWS region the request was made in.
	Region string `json:"region"`
}

// ec2SpotOptions are a Spot launch's market options.
type ec2SpotOptions struct {
	requestType           string
	interruptionBehavior  string
	maxPrice              string
	validUntil            string
	launchGroup           string
	availabilityZoneGroup string
}

// ec2SpotLaunch carries a Spot launch through [EC2Plugin.launchInstances].
type ec2SpotLaunch struct {
	opts ec2SpotOptions

	// tags are the request-scoped tags (ResourceType spot-instances-request).
	tags []EC2Tag

	// launchParams are what a replacement is launched from.
	launchParams map[string]string

	// replacing names the persistent request a replacement launch fulfills, nil for a
	// launch that creates its own requests.
	replacing *EC2SpotInstanceRequest

	// fleetOnly marks a CreateFleet Spot pool: its instances are Spot Instances, but no
	// request is written for them and the request outcome seed does not apply — see
	// [EC2Plugin.launchFleetPool].
	fleetOnly bool
}

// ec2SpotUnfulfilledError reports a Spot launch that a seeded outcome left unfulfilled.
type ec2SpotUnfulfilledError struct {
	statusCode string
	// imageID and image are the AMI the launch resolved before the shortfall, so a request
	// left open reports the AMI the launch would have used — a resolve:ssm: reference already
	// resolved (#1506) — without resolving it a second time.
	imageID string
	image   EC2Image
}

// Error implements error.
func (e *ec2SpotUnfulfilledError) Error() string {
	return "spot request unfulfilled: " + e.statusCode
}

// runInstancesError is the RunInstances answer to an unfulfilled Spot launch:
// InsufficientInstanceCapacity, which the server-error table publishes for "There is not
// enough capacity to fulfill your request" and which a 500-series status accompanies — the
// same reading [ec2CRSeedableErrors] records. A RunInstances Spot launch is synchronous, so it
// fails where RequestSpotInstances leaves an open request behind.
func (e *ec2SpotUnfulfilledError) runInstancesError() *AWSError {
	return &AWSError{
		Code:       "InsufficientInstanceCapacity",
		Message:    "There is no Spot capacity available that matches your request.",
		HTTPStatus: http.StatusInternalServerError,
	}
}

// ec2SpotReplacementParams returns the RunInstances parameters a persistent request's
// replacement is launched from: the request's own, less its tags — "LaunchSpecification has
// no tag field", so a replacement carries none — its counts and its market options.
func ec2SpotReplacementParams(params map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range params {
		switch {
		case strings.HasPrefix(k, "TagSpecification."), strings.HasPrefix(k, "InstanceMarketOptions."),
			k == "MinCount", k == "MaxCount", k == "ClientToken", k == "DryRun", k == "Action", k == "Version":
			continue
		}
		out[k] = v
	}
	return out
}

// ec2ParseSpotOptions validates a Spot launch's options against SpotMarketOptions'
// published values.
//
// fromRunInstances applies the two rules that page states for RunInstances alone: "persistent
// Spot Instance requests are only supported when the instance interruption behavior is either
// hibernate or stop", and ValidUntil is "Supported only for persistent requests".
func ec2ParseSpotOptions(opts ec2SpotOptions, fromRunInstances bool) (ec2SpotOptions, *AWSError) {
	if opts.requestType == "" {
		opts.requestType = "one-time"
	}
	if opts.requestType != "one-time" && opts.requestType != "persistent" {
		return opts, ec2SpotInvalidValue("SpotInstanceType", opts.requestType, "one-time | persistent")
	}
	if opts.interruptionBehavior == "" {
		opts.interruptionBehavior = "terminate"
	}
	switch opts.interruptionBehavior {
	case "hibernate", "stop", "terminate":
	default:
		return opts, ec2SpotInvalidValue("InstanceInterruptionBehavior", opts.interruptionBehavior,
			"hibernate | stop | terminate")
	}
	if opts.maxPrice != "" {
		price, err := strconv.ParseFloat(opts.maxPrice, 64)
		if err != nil || price < 0.001 {
			return opts, &AWSError{
				Code:       "InvalidParameterValue",
				Message:    "The maximum price '" + opts.maxPrice + "' is not valid; it must be more than USD $0.001.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if opts.validUntil != "" {
		parsed, err := time.Parse(time.RFC3339, opts.validUntil)
		if err != nil {
			return opts, ec2SpotInvalidValue("ValidUntil", opts.validUntil, "YYYY-MM-DDTHH:MM:SSZ")
		}
		opts.validUntil = parsed.UTC().Format(time.RFC3339)
	}
	if fromRunInstances && opts.requestType == "persistent" && opts.interruptionBehavior == "terminate" {
		return opts, &AWSError{
			Code: "InvalidParameterCombination",
			Message: "A persistent Spot Instance request launched by RunInstances requires an instance " +
				"interruption behavior of stop or hibernate.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if fromRunInstances && opts.requestType == "one-time" && opts.validUntil != "" {
		return opts, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "ValidUntil is supported only for persistent Spot Instance requests.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return opts, nil
}

// ec2SpotInvalidValue is InvalidParameterValue for a Spot option outside its published set.
func ec2SpotInvalidValue(param, value, valid string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value (" + value + ") for parameter " + param + " is invalid. Valid values: " + valid + ".",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2SpotLaunchFromRunInstances reads a RunInstances request's InstanceMarketOptions,
// returning nil for an On-Demand launch.
//
// MarketType spot is the one value read. capacity-block, the other value the reference
// publishes, names a reservation substrate does not model, so a launch naming it runs
// On-Demand as it did before.
func ec2SpotLaunchFromRunInstances(params map[string]string) (*ec2SpotLaunch, *AWSError) {
	if params["InstanceMarketOptions.MarketType"] != "spot" {
		return nil, nil
	}
	const p = "InstanceMarketOptions.SpotOptions."
	opts, awsErr := ec2ParseSpotOptions(ec2SpotOptions{
		requestType:          params[p+"SpotInstanceType"],
		interruptionBehavior: params[p+"InstanceInterruptionBehavior"],
		maxPrice:             params[p+"MaxPrice"],
		validUntil:           params[p+"ValidUntil"],
	}, true)
	if awsErr != nil {
		return nil, awsErr
	}
	return &ec2SpotLaunch{
		opts:         opts,
		tags:         ec2LaunchTagsForResource(params, "spot-instances-request"),
		launchParams: ec2SpotReplacementParams(params),
	}, nil
}

// spotRequestOutcomeSeed returns the status code a seed makes a Spot launch of instanceType
// answer, or "" when none applies. A seed for the type wins over the "*" wildcard.
func (p *EC2Plugin) spotRequestOutcomeSeed(instanceType string) (string, error) {
	for _, key := range []string{ec2SpotCtrlKey(instanceType), ec2SpotCtrlKey("*")} {
		data, err := p.state.Get(context.Background(), ec2SpotCtrlNamespace, key)
		if err != nil {
			return "", fmt.Errorf("ec2 spot request outcome get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ec2SpotRequestOutcomeSeed
		if err := json.Unmarshal(data, &seed); err != nil {
			return "", fmt.Errorf("ec2 spot request outcome unmarshal: %w", err)
		}
		return seed.StatusCode, nil
	}
	return "", nil
}

// ec2SpotProductDescription is the request's productDescription, from the AMI's platform.
func ec2SpotProductDescription(img EC2Image) string {
	if img.Platform == "windows" {
		return "Windows"
	}
	return "Linux/UNIX"
}

// putSpotRequest writes one request record.
func (p *EC2Plugin) putSpotRequest(r EC2SpotInstanceRequest) error {
	return p.putEC2Record(context.Background(), ec2SpotRequestStateKey(r.AccountID, r.Region, r.SpotInstanceRequestID), r)
}

// getSpotRequest reads one request record, nil when absent.
func (p *EC2Plugin) getSpotRequest(reqCtx *RequestContext, id string) (*EC2SpotInstanceRequest, error) {
	data, err := p.state.Get(context.Background(), ec2Namespace, ec2SpotRequestStateKey(reqCtx.AccountID, reqCtx.Region, id))
	if err != nil {
		return nil, fmt.Errorf("ec2 spot request get: %w", err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) is "no such request", which callers handle.
	}
	var r EC2SpotInstanceRequest
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ec2 spot request unmarshal: %w", err)
	}
	return &r, nil
}

// setStatus records a status change, with the message substrate renders for the code.
func (r *EC2SpotInstanceRequest) setStatus(code, now string) {
	r.StatusCode = code
	r.StatusMessage = ec2SpotStatusMessages[code]
	r.StatusUpdateTime = now
}

// fulfillSpotRequest writes the request a just-launched Spot Instance fulfills: a new
// active/fulfilled request, or the persistent request a replacement was launched for, now
// pointing at the replacement.
func (p *EC2Plugin) fulfillSpotRequest(
	reqCtx *RequestContext, spot *ec2SpotLaunch, inst EC2Instance, img EC2Image, now string,
) (EC2SpotInstanceRequest, error) {
	var r EC2SpotInstanceRequest
	if spot.replacing != nil {
		r = *spot.replacing
	} else {
		r = EC2SpotInstanceRequest{
			SpotInstanceRequestID:        inst.SpotInstanceRequestID,
			Type:                         spot.opts.requestType,
			InstanceInterruptionBehavior: spot.opts.interruptionBehavior,
			SpotPrice:                    spot.opts.maxPrice,
			ValidUntil:                   spot.opts.validUntil,
			LaunchGroup:                  spot.opts.launchGroup,
			AvailabilityZoneGroup:        spot.opts.availabilityZoneGroup,
			CreateTime:                   now,
			ProductDescription:           ec2SpotProductDescription(img),
			LaunchParams:                 spot.launchParams,
			Tags:                         spot.tags,
			AccountID:                    reqCtx.AccountID,
			Region:                       reqCtx.Region,
		}
	}
	r.State = ec2SpotStateActive
	r.setStatus(ec2SpotStatusFulfilled, now)
	r.InstanceID = inst.InstanceID
	r.LaunchedAvailabilityZone = inst.AvailabilityZone
	r.LaunchSpecification = EC2SpotLaunchSpecification{
		ImageID:          inst.ImageID,
		InstanceType:     inst.InstanceType,
		KeyName:          inst.KeyName,
		SubnetID:         inst.SubnetID,
		SecurityGroupIDs: inst.SecurityGroupIDs,
		AvailabilityZone: inst.AvailabilityZone,
	}
	if err := p.putSpotRequest(r); err != nil {
		return EC2SpotInstanceRequest{}, err
	}
	return r, nil
}

// onSpotInstanceTerminated settles the request a terminated Spot Instance fulfilled.
//
// "You terminated a Spot Instance that had been fulfilled, so the request state is closed
// (unless it's a persistent request) and the instance state is terminated" — the User Guide's
// gloss on instance-terminated-by-user. A persistent request that is still active, and whose
// ValidUntil has not passed, launches a replacement from its recorded launch: the request
// "is opened again" and is fulfilled again at once, since substrate models no capacity wait.
// A seeded shortfall leaves it open with that status instead, and a replacement the launch
// refuses — its AMI deregistered since, say — leaves it open with bad-parameters. A cancelled
// request launches nothing, which is the reason a consumer cancels before terminating. status is
// the status code the request records — instance-terminated-by-user for a caller's termination —
// and at the time of the termination, which that status carries.
func (p *EC2Plugin) onSpotInstanceTerminated(reqCtx *RequestContext, inst EC2Instance, status string, at time.Time) error {
	r, err := p.getSpotRequest(reqCtx, inst.SpotInstanceRequestID)
	if err != nil || r == nil || r.InstanceID != inst.InstanceID {
		return err
	}
	// The termination's status carries the termination's time; whether a replacement launches,
	// and the launch with its fulfilled status, are the clock's reading now.
	r.setStatus(status, at.UTC().Format(time.RFC3339))
	nowTime := p.tc.Now().UTC()
	now := nowTime.Format(time.RFC3339)
	expired := false
	if r.ValidUntil != "" {
		if until, parseErr := time.Parse(time.RFC3339, r.ValidUntil); parseErr == nil && !nowTime.Before(until) {
			expired = true
		}
	}
	if r.State == ec2SpotStateCancelled {
		return p.putSpotRequest(*r)
	}
	if r.Type != "persistent" || r.State != ec2SpotStateActive || expired {
		r.State = ec2SpotStateClosed
		return p.putSpotRequest(*r)
	}

	return p.launchForSpotRequest(reqCtx, r, now)
}

// requestSpotInstances handles RequestSpotInstances.
//
// The LaunchSpecification members are RunInstances' own under a "LaunchSpecification."
// prefix, so the request is launched as a RunInstances of InstanceCount instances — one
// request per instance, as AWS creates them — through the same path, with every refusal that
// path makes. TagSpecification.N tags the requests: "The value for ResourceType must be
// spot-instances-request, otherwise the Spot Instance request fails". The instances carry no
// tags.
//
// A request is fulfilled at once, active/fulfilled with its instance, where AWS passes
// through pending-evaluation first; substrate models no evaluation delay. A seeded outcome
// (see [EC2Plugin.spotRequestOutcomeSeed]) instead leaves InstanceCount open requests with
// the seeded status and no instance. A one-time request's ValidUntil defaults to seven days
// from creation, the default the reference states. ValidFrom, BlockDurationMinutes and
// ClientToken are not read.
func (p *EC2Plugin) requestSpotInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if awsErr := ec2CheckTagSpecificationTypes(req.Params, "spot-instances-request"); awsErr != nil {
		return nil, awsErr
	}
	count := 1
	if raw, ok := req.Params["InstanceCount"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return nil, ec2SpotInvalidValue("InstanceCount", raw, "an integer of at least 1")
		}
		count = n
	}
	opts, awsErr := ec2ParseSpotOptions(ec2SpotOptions{
		requestType:           req.Params["Type"],
		interruptionBehavior:  req.Params["InstanceInterruptionBehavior"],
		maxPrice:              req.Params["SpotPrice"],
		validUntil:            req.Params["ValidUntil"],
		launchGroup:           req.Params["LaunchGroup"],
		availabilityZoneGroup: req.Params["AvailabilityZoneGroup"],
	}, false)
	if awsErr != nil {
		return nil, awsErr
	}
	nowTime := p.tc.Now().UTC()
	if opts.requestType == "one-time" && opts.validUntil == "" {
		opts.validUntil = nowTime.Add(7 * 24 * time.Hour).Format(time.RFC3339)
	}

	run := map[string]string{"MinCount": "1", "MaxCount": strconv.Itoa(count)}
	for k, v := range req.Params {
		if rest, ok := strings.CutPrefix(k, "LaunchSpecification."); ok {
			run[rest] = v
		}
	}
	spot := &ec2SpotLaunch{
		opts:         opts,
		tags:         ec2LaunchTagsForResource(req.Params, "spot-instances-request"),
		launchParams: ec2SpotReplacementParams(run),
	}
	launch := &AWSRequest{Service: "ec2", Operation: "RunInstances", Params: run, Headers: map[string]string{}}
	_, _, requests, err := p.launchInstances(reqCtx, launch, nil, spot)
	if err != nil {
		var unfulfilled *ec2SpotUnfulfilledError
		if !errors.As(err, &unfulfilled) {
			return nil, err
		}
		requests, err = p.openSpotRequests(reqCtx, spot, run, count, unfulfilled, nowTime.Format(time.RFC3339))
		if err != nil {
			return nil, err
		}
	}
	type response struct {
		XMLName  xml.Name                    `xml:"RequestSpotInstancesResponse"`
		XMLNS    string                      `xml:"xmlns,attr"`
		Requests []ec2SpotInstanceRequestXML `xml:"spotInstanceRequestSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, r := range requests {
		resp.Requests = append(resp.Requests, ec2SpotRequestXML(r))
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// openSpotRequests writes count open requests a seeded outcome left unfulfilled.
//
// The AMI is the one the launch resolved before the shortfall ([ec2SpotUnfulfilledError]), so a
// resolve:ssm: ImageId is reported as the AMI ID it named, and its product description is that
// AMI's — the same values a fulfilled request records from its instance (#1506).
func (p *EC2Plugin) openSpotRequests(
	reqCtx *RequestContext, spot *ec2SpotLaunch, run map[string]string, count int,
	unfulfilled *ec2SpotUnfulfilledError, now string,
) ([]EC2SpotInstanceRequest, error) {
	code, img := unfulfilled.statusCode, unfulfilled.image
	instanceType := run["InstanceType"]
	if instanceType == "" {
		instanceType = "t3.micro"
	}
	out := make([]EC2SpotInstanceRequest, 0, count)
	for i := 0; i < count; i++ {
		r := EC2SpotInstanceRequest{
			SpotInstanceRequestID:        generateSpotInstanceRequestID(reqCtx.IDs),
			Type:                         spot.opts.requestType,
			State:                        ec2SpotStateOpen,
			InstanceInterruptionBehavior: spot.opts.interruptionBehavior,
			SpotPrice:                    spot.opts.maxPrice,
			ValidUntil:                   spot.opts.validUntil,
			LaunchGroup:                  spot.opts.launchGroup,
			AvailabilityZoneGroup:        spot.opts.availabilityZoneGroup,
			CreateTime:                   now,
			ProductDescription:           ec2SpotProductDescription(img),
			LaunchSpecification: EC2SpotLaunchSpecification{
				ImageID:          unfulfilled.imageID,
				InstanceType:     instanceType,
				KeyName:          run["KeyName"],
				SubnetID:         run["SubnetId"],
				SecurityGroupIDs: indexedParams(run, "SecurityGroupId.%d"),
				AvailabilityZone: run["Placement.AvailabilityZone"],
			},
			LaunchParams: spot.launchParams,
			Tags:         spot.tags,
			AccountID:    reqCtx.AccountID,
			Region:       reqCtx.Region,
		}
		r.setStatus(code, now)
		if err := p.putSpotRequest(r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// ec2SpotStatusXML is SpotInstanceStatus.
type ec2SpotStatusXML struct {
	Code       string `xml:"code"`
	UpdateTime string `xml:"updateTime"`
	Message    string `xml:"message"`
}

// ec2SpotLaunchSpecXML is the LaunchSpecification a request renders.
type ec2SpotLaunchSpecXML struct {
	ImageID      string           `xml:"imageId"`
	KeyName      string           `xml:"keyName,omitempty"`
	Groups       []ec2GroupItem   `xml:"groupSet>item,omitempty"`
	InstanceType string           `xml:"instanceType"`
	Placement    *ec2PlacementXML `xml:"placement,omitempty"`
	SubnetID     string           `xml:"subnetId,omitempty"`
}

// ec2PlacementXML is SpotPlacement's availabilityZone.
type ec2PlacementXML struct {
	AvailabilityZone string `xml:"availabilityZone"`
}

// ec2SpotInstanceRequestXML is SpotInstanceRequest as DescribeSpotInstanceRequests' sample
// renders it, member for member, plus the published members substrate records.
type ec2SpotInstanceRequestXML struct {
	SpotInstanceRequestID        string               `xml:"spotInstanceRequestId"`
	SpotPrice                    string               `xml:"spotPrice,omitempty"`
	Type                         string               `xml:"type"`
	State                        string               `xml:"state"`
	Status                       ec2SpotStatusXML     `xml:"status"`
	ValidUntil                   string               `xml:"validUntil,omitempty"`
	LaunchGroup                  string               `xml:"launchGroup,omitempty"`
	AvailabilityZoneGroup        string               `xml:"availabilityZoneGroup,omitempty"`
	LaunchSpecification          ec2SpotLaunchSpecXML `xml:"launchSpecification"`
	InstanceID                   string               `xml:"instanceId,omitempty"`
	CreateTime                   string               `xml:"createTime"`
	ProductDescription           string               `xml:"productDescription"`
	LaunchedAvailabilityZone     string               `xml:"launchedAvailabilityZone,omitempty"`
	InstanceInterruptionBehavior string               `xml:"instanceInterruptionBehavior"`
	Tags                         *ec2TagSetXML        `xml:"tagSet,omitempty"`
}

// ec2SpotRequestXML renders one request.
func ec2SpotRequestXML(r EC2SpotInstanceRequest) ec2SpotInstanceRequestXML {
	spec := ec2SpotLaunchSpecXML{
		ImageID:      r.LaunchSpecification.ImageID,
		KeyName:      r.LaunchSpecification.KeyName,
		InstanceType: r.LaunchSpecification.InstanceType,
		SubnetID:     r.LaunchSpecification.SubnetID,
	}
	for _, id := range r.LaunchSpecification.SecurityGroupIDs {
		spec.Groups = append(spec.Groups, ec2GroupItem{GroupID: id})
	}
	if r.LaunchSpecification.AvailabilityZone != "" {
		spec.Placement = &ec2PlacementXML{AvailabilityZone: r.LaunchSpecification.AvailabilityZone}
	}
	return ec2SpotInstanceRequestXML{
		SpotInstanceRequestID: r.SpotInstanceRequestID,
		SpotPrice:             r.SpotPrice,
		Type:                  r.Type,
		State:                 r.State,
		Status: ec2SpotStatusXML{
			Code: r.StatusCode, UpdateTime: r.StatusUpdateTime, Message: r.StatusMessage,
		},
		ValidUntil:                   r.ValidUntil,
		LaunchGroup:                  r.LaunchGroup,
		AvailabilityZoneGroup:        r.AvailabilityZoneGroup,
		LaunchSpecification:          spec,
		InstanceID:                   r.InstanceID,
		CreateTime:                   r.CreateTime,
		ProductDescription:           r.ProductDescription,
		LaunchedAvailabilityZone:     r.LaunchedAvailabilityZone,
		InstanceInterruptionBehavior: r.InstanceInterruptionBehavior,
		Tags:                         ec2TagSet(r.Tags),
	}
}

// ec2SpotRequestFilterSpec is DescribeSpotInstanceRequests' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotInstanceRequests.html.
//
// The accepted-but-inert names select on members substrate does not record — block device
// mappings, kernel and RAM disk, monitoring, network-interface detail — so they constrain
// nothing, the rule [ec2FilterSpec] states for every operation. fault-code and fault-message
// are evaluated against the empty string, because substrate records no fault.
func ec2SpotRequestFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"availability-zone-group", "create-time", "fault-code", "fault-message",
			"instance-id", "launch-group", "launch.group-id", "launch.image-id",
			"launch.instance-type", "launch.key-name", "launched-availability-zone",
			"network-interface.subnet-id", "product-description", "spot-instance-request-id",
			"spot-price", "state", "status-code", "status-message", "tag-key", "type",
			"valid-until",
		},
		accepted: []string{
			"launch.block-device-mapping.delete-on-termination",
			"launch.block-device-mapping.device-name", "launch.block-device-mapping.snapshot-id",
			"launch.block-device-mapping.volume-size", "launch.block-device-mapping.volume-type",
			"launch.group-name", "launch.kernel-id", "launch.monitoring-enabled",
			"launch.ramdisk-id", "launched-availability-zone-id",
			"network-interface.addresses.primary", "network-interface.delete-on-termination",
			"network-interface.description", "network-interface.device-index",
			"network-interface.group-id", "network-interface.network-interface-id",
			"network-interface.private-ip-address", "valid-from",
		},
	}
}

// ec2SpotRequestMatchesFilters reports whether a request satisfies every filter.
func ec2SpotRequestMatchesFilters(r EC2SpotInstanceRequest, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2SpotRequestMatchesFilter(r, name, values) {
			return false
		}
	}
	return true
}

// ec2SpotRequestMatchesFilter evaluates one DescribeSpotInstanceRequests filter.
func ec2SpotRequestMatchesFilter(r EC2SpotInstanceRequest, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(r.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "availability-zone-group":
		return ec2FilterAccepts(values, r.AvailabilityZoneGroup)
	case "create-time":
		return ec2FilterAccepts(values, r.CreateTime)
	case "fault-code", "fault-message":
		return ec2FilterAccepts(values, "")
	case "instance-id":
		return r.InstanceID != "" && ec2FilterAccepts(values, r.InstanceID)
	case "launch-group":
		return ec2FilterAccepts(values, r.LaunchGroup)
	case "launch.group-id":
		for _, id := range r.LaunchSpecification.SecurityGroupIDs {
			if ec2FilterAccepts(values, id) {
				return true
			}
		}
		return false
	case "launch.image-id":
		return ec2FilterAccepts(values, r.LaunchSpecification.ImageID)
	case "launch.instance-type":
		return ec2FilterAccepts(values, r.LaunchSpecification.InstanceType)
	case "launch.key-name":
		return ec2FilterAccepts(values, r.LaunchSpecification.KeyName)
	case "launched-availability-zone":
		return ec2FilterAccepts(values, r.LaunchedAvailabilityZone)
	case "network-interface.subnet-id":
		return ec2FilterAccepts(values, r.LaunchSpecification.SubnetID)
	case "product-description":
		return ec2FilterAccepts(values, r.ProductDescription)
	case "spot-instance-request-id":
		return ec2FilterAccepts(values, r.SpotInstanceRequestID)
	case "spot-price":
		return ec2FilterAccepts(values, r.SpotPrice)
	case "state":
		return ec2FilterAccepts(values, r.State)
	case "status-code":
		return ec2FilterAccepts(values, r.StatusCode)
	case "status-message":
		return ec2FilterAccepts(values, r.StatusMessage)
	case "type":
		return ec2FilterAccepts(values, r.Type)
	case "valid-until":
		return ec2FilterAccepts(values, r.ValidUntil)
	default:
		return true
	}
}

// describeSpotInstanceRequests handles DescribeSpotInstanceRequests: SpotInstanceRequestId.N
// asserts existence, Filter.N narrows through [ec2SpotRequestFilterSpec], and MaxResults/
// NextToken page the answer.
//
// The page publishes MaxResults with no Valid Range — it only recommends "a value between 5
// and 1000" — so, per #671, the floor is [ec2MinUnpublishedMaxResults] and there is no
// ceiling, as for DescribeImages. "Spot Instance requests are deleted four hours after they
// are canceled and their instances are terminated" is not modeled: a request stays listed.
func (p *EC2Plugin) describeSpotInstanceRequests(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	requestIDs := extractIndexedParams(req.Params, "SpotInstanceRequestId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "SpotInstanceRequestId", requestIDs); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinUnpublishedMaxResults, ec2NoMaxResultsCeiling)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	ids := newEC2IDFilter(requestIDs, ec2SpotInstanceRequestIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2SpotRequestFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	if err := p.reconcileOpenSpotRequests(reqCtx); err != nil {
		return nil, err
	}
	keys, err := p.state.List(context.Background(), ec2Namespace, "spot_request:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSpotInstanceRequests: %w", err)
	}
	type response struct {
		XMLName   xml.Name                    `xml:"DescribeSpotInstanceRequestsResponse"`
		XMLNS     string                      `xml:"xmlns,attr"`
		Requests  []ec2SpotInstanceRequestXML `xml:"spotInstanceRequestSet>item"`
		NextToken string                      `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var r EC2SpotInstanceRequest
		if json.Unmarshal(data, &r) != nil {
			continue
		}
		if !ids.match(r.SpotInstanceRequestID) || !ec2SpotRequestMatchesFilters(r, filters) {
			continue
		}
		resp.Requests = append(resp.Requests, ec2SpotRequestXML(r))
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	resp.Requests, resp.NextToken = ec2Page(resp.Requests, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}

// reconcileOpenSpotRequests re-evaluates every open request a seeded outcome left
// unfulfilled, so a caller's wait-for-fulfillment loop terminates once the seed is cleared.
//
// It runs on observation — each DescribeSpotInstanceRequests — rather than on a clock, the
// same choice [progression] makes: an open request is fulfilled by the first describe that
// finds no seed blocking its instance type, launching its instance from the request's own
// launch and minting from that describe's request ID, so a replay of the same describe mints
// the same instance. A request whose ValidUntil has passed is closed with schedule-expired
// instead ("The Spot request expired because it was not fulfilled before the specified
// date"). An open request whose launch is refused outright stays open with bad-parameters.
func (p *EC2Plugin) reconcileOpenSpotRequests(reqCtx *RequestContext) error {
	keys, err := p.state.List(context.Background(), ec2Namespace, "spot_request:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return fmt.Errorf("ec2 reconcile spot requests: %w", err)
	}
	nowTime := p.tc.Now().UTC()
	now := nowTime.Format(time.RFC3339)
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var r EC2SpotInstanceRequest
		if json.Unmarshal(data, &r) != nil || r.State != ec2SpotStateOpen || r.InstanceID != "" ||
			!containsStr(ec2SpotSeedableStatuses, r.StatusCode) {
			continue
		}
		if r.ValidUntil != "" {
			if until, parseErr := time.Parse(time.RFC3339, r.ValidUntil); parseErr == nil && !nowTime.Before(until) {
				r.State = ec2SpotStateClosed
				r.setStatus(ec2SpotStatusScheduleExpired, now)
				if err := p.putSpotRequest(r); err != nil {
					return err
				}
				continue
			}
		}
		code, seedErr := p.spotRequestOutcomeSeed(r.LaunchSpecification.InstanceType)
		if seedErr != nil {
			return seedErr
		}
		if code != "" {
			continue
		}
		if err := p.launchForSpotRequest(reqCtx, &r, now); err != nil {
			return err
		}
	}
	return nil
}

// launchForSpotRequest launches one instance from a request's recorded launch and points the
// request at it. A launch the seed or the launch path refuses leaves the request open, with
// the seeded status or bad-parameters, and no instance.
func (p *EC2Plugin) launchForSpotRequest(reqCtx *RequestContext, r *EC2SpotInstanceRequest, now string) error {
	params := maps.Clone(r.LaunchParams)
	if params == nil {
		params = map[string]string{}
	}
	params["MinCount"], params["MaxCount"] = "1", "1"
	launch := &AWSRequest{Service: "ec2", Operation: "RunInstances", Params: params, Headers: map[string]string{}}
	spot := &ec2SpotLaunch{
		opts: ec2SpotOptions{
			requestType: r.Type, interruptionBehavior: r.InstanceInterruptionBehavior,
			maxPrice: r.SpotPrice, validUntil: r.ValidUntil,
		},
		launchParams: r.LaunchParams,
		replacing:    r,
	}
	if _, _, _, launchErr := p.launchInstances(reqCtx, launch, nil, spot); launchErr != nil {
		r.State = ec2SpotStateOpen
		r.InstanceID = ""
		r.LaunchedAvailabilityZone = ""
		var unfulfilled *ec2SpotUnfulfilledError
		var refused *AWSError
		switch {
		case errors.As(launchErr, &unfulfilled):
			r.setStatus(unfulfilled.statusCode, now)
		case errors.As(launchErr, &refused):
			r.setStatus(ec2SpotStatusBadParameters, now)
		default:
			return launchErr
		}
		return p.putSpotRequest(*r)
	}
	return nil
}

// cancelSpotInstanceRequests handles CancelSpotInstanceRequests.
//
// "Canceling a Spot Instance request does not terminate running Spot Instances associated
// with the request." Every named request moves to cancelled; its status says what happened
// to its instance — request-canceled-and-instance-running while the instance is up,
// canceled-before-fulfillment when there never was one, and instance-terminated-by-user is
// kept for a request whose instance is already gone. Every ID is resolved before any is
// cancelled, so an unknown one — InvalidSpotInstanceRequestID.NotFound — cancels nothing.
func (p *EC2Plugin) cancelSpotInstanceRequests(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "SpotInstanceRequestId")
	if len(ids) == 0 {
		return nil, ec2MissingParameter("SpotInstanceRequestId")
	}
	requests := make([]*EC2SpotInstanceRequest, 0, len(ids))
	for _, id := range ids {
		r, err := p.getSpotRequest(reqCtx, id)
		if err != nil {
			return nil, err
		}
		if awsErr := ec2SpotInstanceRequestIDKind.requireResource(id, r != nil); awsErr != nil {
			return nil, awsErr
		}
		requests = append(requests, r)
	}
	now := p.tc.Now().UTC().Format(time.RFC3339)
	type item struct {
		SpotInstanceRequestID string `xml:"spotInstanceRequestId"`
		State                 string `xml:"state"`
	}
	type response struct {
		XMLName xml.Name `xml:"CancelSpotInstanceRequestsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Items   []item   `xml:"spotInstanceRequestSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, r := range requests {
		if r.State != ec2SpotStateCancelled {
			running, err := p.spotInstanceRunning(reqCtx, r.InstanceID)
			if err != nil {
				return nil, err
			}
			switch {
			case running:
				r.setStatus(ec2SpotStatusCanceledRunning, now)
			case r.InstanceID == "":
				r.setStatus(ec2SpotStatusCanceledBeforeFulfil, now)
			}
			r.State = ec2SpotStateCancelled
			if err := p.putSpotRequest(*r); err != nil {
				return nil, err
			}
		}
		resp.Items = append(resp.Items, item{SpotInstanceRequestID: r.SpotInstanceRequestID, State: r.State})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// spotInstanceRunning reports whether instanceID names an instance that is not terminated.
func (p *EC2Plugin) spotInstanceRunning(reqCtx *RequestContext, instanceID string) (bool, error) {
	if instanceID == "" {
		return false, nil
	}
	data, err := p.state.Get(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+instanceID)
	if err != nil {
		return false, fmt.Errorf("ec2 spot instance get: %w", err)
	}
	if data == nil {
		return false, nil
	}
	var inst EC2Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return false, fmt.Errorf("ec2 spot instance unmarshal: %w", err)
	}
	return inst.State.Name != ec2StateTerminated, nil
}

// --- Control plane: seeding a Spot request outcome ---

// ec2SpotCtrlNamespace holds the Spot request outcome seeds.
const ec2SpotCtrlNamespace = "ec2-spot-ctrl"

// ec2SpotCtrlKey is the seed key for an instance type, or "*" for every type.
func ec2SpotCtrlKey(instanceType string) string {
	if instanceType == "" {
		instanceType = "*"
	}
	return "outcome:" + instanceType
}

// ec2SpotRequestOutcomeSeed is the body of POST /v1/ec2/spot-request-outcomes.
type ec2SpotRequestOutcomeSeed struct {
	// InstanceType scopes the seed; empty or "*" applies it to every type.
	InstanceType string `json:"instanceType,omitempty"`

	// StatusCode is the status an unfulfilled request reports: capacity-not-available or
	// price-too-low.
	StatusCode string `json:"statusCode"`
}

// handleEC2SeedSpotRequestOutcome serves POST /v1/ec2/spot-request-outcomes: while a seed
// applies, a Spot launch of the type is not fulfilled — RunInstances answers
// InsufficientInstanceCapacity, RequestSpotInstances leaves open requests with the seeded
// status, and a persistent request's replacement leaves it open.
func (s *Server) handleEC2SeedSpotRequestOutcome(w http.ResponseWriter, r *http.Request) {
	var seed ec2SpotRequestOutcomeSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if !containsStr(ec2SpotSeedableStatuses, seed.StatusCode) {
		http.Error(w, fmt.Sprintf(`{"error":"statusCode must be one of %s"}`,
			strings.Join(ec2SpotSeedableStatuses, ", ")), http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := ec2SpotCtrlKey(seed.InstanceType)
	if err := s.state.Put(r.Context(), ec2SpotCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "scope": key, "statusCode": seed.StatusCode})
}

// handleEC2ClearSpotRequestOutcome serves DELETE /v1/ec2/spot-request-outcomes, clearing the
// seed for ?instanceType= or, with no query, every seed.
func (s *Server) handleEC2ClearSpotRequestOutcome(w http.ResponseWriter, r *http.Request) {
	if instanceType := r.URL.Query().Get("instanceType"); instanceType != "" {
		if err := s.state.Delete(r.Context(), ec2SpotCtrlNamespace, ec2SpotCtrlKey(instanceType)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), ec2SpotCtrlNamespace, "outcome:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, key := range keys {
		if err := s.state.Delete(r.Context(), ec2SpotCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "cleared": strconv.Itoa(len(keys))})
}
