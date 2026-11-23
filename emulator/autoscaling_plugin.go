package emulator

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Amazon EC2 Auto Scaling, and where its boundary with EC2 sits (#1460).
//
// An Auto Scaling group is a desired-capacity contract over EC2 instances, and the only
// part of that contract a caller can observe is through two APIs: this one's describes and
// EC2's DescribeInstances. So this plugin keeps the group — its sizes, launch template or
// mixed-instances policy, tags, suspended processes, member instances and scaling
// activities — and reaches EC2 for every instance it launches or terminates, by building
// the RunInstances or TerminateInstances request a real group's service-linked role would
// send. The instances it launches are therefore EC2's own records: DescribeInstances
// reports them, with the reserved `aws:autoscaling:groupName` tag and the group's
// PropagateAtLaunch tags, and EC2's seeded pending → running progression governs what a
// describe reports about them. Nothing here adds a clock of its own.
//
// # Reconciliation is synchronous and observation-driven
//
// A real group closes the gap between desired and actual capacity asynchronously. Substrate
// closes it at two kinds of moment, both of which are requests and so both of which are in
// the event log:
//
//   - **Inside the request that changes capacity.** CreateAutoScalingGroup,
//     UpdateAutoScalingGroup, SetDesiredCapacity and TerminateInstanceInAutoScalingGroup
//     launch or terminate before they answer, so the next DescribeInstances already sees
//     the result. This is EC2's own instantaneous-transition model applied to a group.
//   - **On the next observation of the group.** DescribeAutoScalingGroups,
//     DescribeAutoScalingInstances and DescribeScalingActivities first check every member
//     instance against EC2's record. An instance terminated directly through EC2 — which is
//     how aws-elastic-globus-compute's workers scale in — is noticed there, removed from the
//     group with a scaling activity, and replaced unless the desired capacity was lowered.
//
// Both are deterministic: a replay re-issues the same requests at the same clock readings,
// and every identifier the reconciliation mints comes from the triggering request's
// [IDMint]. The inner EC2 calls are not recorded as events of their own, because replaying
// the triggering request is what re-derives them; recording both would apply each twice.
//
// # Why EC2's state is read rather than described
//
// Membership is checked by reading EC2's instance record, not by routing DescribeInstances.
// A DescribeInstances is an *observation* in EC2's seeded progression (#514), so a group
// checking its members would spend a test's seeded `pending` observations on the test's
// behalf, and the instance the test then describes would already read `running`. Reading the
// record is the settled state, which is the only thing membership depends on.

// autoscalingNamespace is the state namespace Auto Scaling records live in.
const autoscalingNamespace = "autoscaling"

// autoscalingXMLNS is the document namespace every Auto Scaling response carries. It is the
// `xmlNamespace` of botocore's autoscaling 2011-01-01 model; the API reference's sample
// responses spell it with `https://`, which no SDK compares.
const autoscalingXMLNS = "http://autoscaling.amazonaws.com/doc/2011-01-01/"

// AutoScalingPlugin emulates Amazon EC2 Auto Scaling over the AWS Query protocol.
type AutoScalingPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry
	auth     *AuthController

	// mu serializes every Auto Scaling request. A reconciliation reads a group, launches or
	// terminates through EC2 and writes the group back, and two concurrent ones over the
	// same group would each launch for the same shortfall. The lock is per plugin instance,
	// so it is not global state, and it is never held across a call back into this plugin.
	mu sync.Mutex
}

// Name returns the service name "autoscaling".
func (p *AutoScalingPlugin) Name() string { return autoscalingNamespace }

// Initialize sets up the AutoScalingPlugin with the provided configuration.
//
// Options["registry"] is how the plugin reaches EC2; without it every launch fails as a
// scaling activity. Options["auth_controller"] is optional and, as for CreateFleet and
// CloudFormation, makes CreateAutoScalingGroup and UpdateAutoScalingGroup check the
// caller's ec2:RunInstances permission before anything is written.
func (p *AutoScalingPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.registry, _ = cfg.Options["registry"].(*PluginRegistry)
	p.auth, _ = cfg.Options["auth_controller"].(*AuthController)
	return nil
}

// Shutdown is a no-op for AutoScalingPlugin.
func (p *AutoScalingPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an Auto Scaling query-protocol request to its handler.
func (p *AutoScalingPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// The account's default VPC is seeded first, exactly as an EC2 request seeds it (#1464):
	// a group naming AvailabilityZones and no VPCZoneIdentifier launches "into the default VPC
	// subnet in each Availability Zone" (API_CreateAutoScalingGroup), and an account whose
	// first EC2-touching call is an Auto Scaling one must already have that VPC, as a real
	// account does. The launches below call EC2's launch path directly, which does not seed.
	if ec2, err := p.ec2(); err == nil {
		if err := ec2.seedDefaultVPC(ctx); err != nil {
			return nil, err
		}
	}
	switch action {
	case "CreateAutoScalingGroup":
		return p.createAutoScalingGroup(ctx, req)
	case "DescribeAutoScalingGroups":
		return p.describeAutoScalingGroups(ctx, req)
	case "UpdateAutoScalingGroup":
		return p.updateAutoScalingGroup(ctx, req)
	case "DeleteAutoScalingGroup":
		return p.deleteAutoScalingGroup(ctx, req)
	case "SetDesiredCapacity":
		return p.setDesiredCapacity(ctx, req)
	case "TerminateInstanceInAutoScalingGroup":
		return p.terminateInstanceInAutoScalingGroup(ctx, req)
	case "SuspendProcesses":
		return p.suspendProcesses(ctx, req)
	case "ResumeProcesses":
		return p.resumeProcesses(ctx, req)
	case "DescribeScalingActivities":
		return p.describeScalingActivities(ctx, req)
	case "SetInstanceProtection":
		return p.setInstanceProtection(ctx, req)
	case "CreateOrUpdateTags":
		return p.createOrUpdateTags(ctx, req)
	case "DeleteTags":
		return p.deleteTags(ctx, req)
	case "DescribeTags":
		return p.describeTags(ctx, req)
	case "DescribeAutoScalingInstances":
		return p.describeAutoScalingInstances(ctx, req)
	case "EnableMetricsCollection":
		return p.enableMetricsCollection(ctx, req)
	case "DisableMetricsCollection":
		return p.disableMetricsCollection(ctx, req)
	case "DescribeMetricCollectionTypes":
		return p.describeMetricCollectionTypes(ctx)
	default:
		return nil, unknownActionError(p.Name(), action)
	}
}

// --- Response envelope -------------------------------------------------------

// asgOKResponse answers an operation with result under `<{operation}Result>` and the
// metadata carrying the context's request ID, so a replay renders identical bytes.
//
// The document is the shared awsQuery one, [queryOKResponse]. A nil result omits the result
// element, which is the class for an operation whose botocore output shape is absent —
// CreateAutoScalingGroup, UpdateAutoScalingGroup, DeleteAutoScalingGroup,
// SetDesiredCapacity, Suspend/ResumeProcesses, CreateOrUpdateTags and DeleteTags — while
// SetInstanceProtection, whose output shape is the memberless SetInstanceProtectionAnswer
// under a resultWrapper, answers with an empty one. The two are not interchangeable:
// botocore looks the wrapper up by name for the second class only.
func asgOKResponse(reqCtx *RequestContext, operation string, result any) (*AWSResponse, error) {
	return queryOKResponse(reqCtx, operation, autoscalingXMLNS, result)
}

// asgNoResultResponse answers an operation whose botocore model declares no output shape.
func asgNoResultResponse(reqCtx *RequestContext, operation string) (*AWSResponse, error) {
	return asgOKResponse(reqCtx, operation, nil)
}

// --- Errors ------------------------------------------------------------------

// asgValidationError is the common ValidationError, HTTP 400, which the Common Error Types
// page publishes as "The input doesn't meet the required format or constraints".
//
// Its messages are not published per operation. Where a message here reproduces real Auto
// Scaling wording it is noted at the call site as observed rather than documented; a
// consumer must dispatch on the code.
func asgValidationError(format string, args ...any) *AWSError {
	return &AWSError{Code: "ValidationError", Message: fmt.Sprintf(format, args...), HTTPStatus: http.StatusBadRequest}
}

// asgGroupNotFound is the refusal for an operation naming a group that does not exist. The
// wording is real Auto Scaling's, observed rather than published.
func asgGroupNotFound(name string) *AWSError {
	return asgValidationError("AutoScalingGroup name not found - AutoScalingGroup '%s' not found", name)
}

// asgMissingParameter refuses a request lacking a member the reference marks Required.
func asgMissingParameter(member string) *AWSError {
	return asgValidationError("1 validation error detected: Value null at '%s' failed to satisfy constraint: Member must not be null",
		lowerFirst(member))
}

// lowerFirst lowercases a member name's first letter, the spelling the Coral validation
// messages use for a member ("maxRecords", "autoScalingGroupName").
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// --- Parameter helpers ---------------------------------------------------------

// asgInt reads an optional integer member. present reports whether the member was sent at
// all, which Update needs: an absent MinSize is "unchanged", not zero.
func asgInt(params map[string]string, key string) (value int, present bool, awsErr *AWSError) {
	raw, ok := params[key]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, true, asgValidationError("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must be an integer",
			raw, lowerFirst(key))
	}
	return n, true, nil
}

// asgBool reads an optional boolean member and whether it was sent.
func asgBool(params map[string]string, key string) (value, present bool) {
	raw, ok := params[key]
	if !ok {
		return false, false
	}
	return strings.EqualFold(strings.TrimSpace(raw), "true"), true
}

// asgTimestamp renders a timestamp the way the Query protocol publishes it, matching the
// API reference's samples ("2023-05-06T17:47:15.107Z").
func asgTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// asgCauseTime renders the instant a scaling activity's Cause names, which the published
// samples spell to the second ("At 2019-04-12T17:31:30Z a user request …").
func asgCauseTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
