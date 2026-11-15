package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Reconciliation: how a group's desired capacity becomes EC2 instances, and back (#1460).
// The package comment on autoscaling_plugin.go says when it runs; this file is what it does.

// asgPlacement is one place a group can launch into: a subnet and its zone, or a zone alone
// for a group that named AvailabilityZones and no VPCZoneIdentifier.
type asgPlacement struct {
	subnetID string
	az       string
}

// asgCandidate is one launch configuration a group can launch: a resolved template version
// plus the instance type and AMI an override contributes.
type asgCandidate struct {
	// spec is the template with its version resolved to a number, which is what an instance
	// records as its own LaunchTemplate.
	spec AutoScalingLaunchTemplateSpec

	// instanceType is the override's type, empty to take the template's.
	instanceType string

	// imageID is the override's AMI, empty to take the template's.
	imageID string
}

// asgAutoScalingGroupTagKey is the reserved tag every instance a group launches carries:
// "The Auto Scaling group automatically adds a tag to instances with a key of
// aws:autoscaling:groupName and a value of the Auto Scaling group name" (tagging guide). It
// reaches EC2 the way [ec2FleetIDTagKey] does — appended to the already-parsed tag slice
// handed to [EC2Plugin.runInstancesWithTags], never as a TagSpecification parameter — so the
// reserved-key rule a caller's request is held to is untouched.
const asgAutoScalingGroupTagKey = "aws:autoscaling:groupName"

// asgProcessNames are the scaling processes SuspendProcesses accepts.
var asgProcessNames = []string{ //nolint:gochecknoglobals // read-only reference data from API_SuspendProcesses.
	"Launch", "Terminate", "AddToLoadBalancer", "AlarmNotification", "AZRebalance",
	"HealthCheck", "InstanceRefresh", "ReplaceUnhealthy", "ScheduledActions",
}

// asgResolveSpec resolves a group's template specification to the template and the version
// number a launch now would use. The version defaults to $Default, as
// API_LaunchTemplateSpecification documents.
//
// The refusals are ValidationError, with real Auto Scaling's wording for a template that does
// not exist (observed, not published).
func (p *AutoScalingPlugin) asgResolveSpec(reqCtx *RequestContext, spec AutoScalingLaunchTemplateSpec) (AutoScalingLaunchTemplateSpec, *AWSError) {
	if spec.LaunchTemplateID == "" && spec.LaunchTemplateName == "" {
		return spec, asgValidationError("You must specify either LaunchTemplateId or LaunchTemplateName in the launch template specification.")
	}
	lt := ec2LookupLaunchTemplate(context.Background(), p.state, reqCtx, spec.LaunchTemplateID, spec.LaunchTemplateName)
	if lt == nil {
		if spec.LaunchTemplateID != "" {
			return spec, asgValidationError("You must use a valid fully-formed launch template. The specified launch template, with template ID %s, does not exist.", spec.LaunchTemplateID)
		}
		return spec, asgValidationError("You must use a valid fully-formed launch template. The specified launch template, with template name %s, does not exist.", spec.LaunchTemplateName)
	}
	version, awsErr := ec2ResolveTemplateVersion(lt, spec.Version)
	if awsErr != nil {
		return spec, asgValidationError("You must use a valid fully-formed launch template. The specified launch template version does not exist: %s", awsErr.Message)
	}
	return AutoScalingLaunchTemplateSpec{
		LaunchTemplateID:   lt.LaunchTemplateID,
		LaunchTemplateName: lt.LaunchTemplateName,
		Version:            strconv.FormatInt(version.VersionNumber, 10),
	}, nil
}

// asgCandidates returns every launch candidate a group has, in priority order. A
// mixed-instances group with no override has one candidate, its template's own type.
func (p *AutoScalingPlugin) asgCandidates(reqCtx *RequestContext, g *AutoScalingGroup) ([]asgCandidate, *AWSError) {
	if g.LaunchTemplate != nil {
		spec, awsErr := p.asgResolveSpec(reqCtx, *g.LaunchTemplate)
		if awsErr != nil {
			return nil, awsErr
		}
		return []asgCandidate{{spec: spec}}, nil
	}
	m := g.MixedInstancesPolicy
	if m == nil {
		return nil, asgValidationError("The group has no launch template.")
	}
	base, awsErr := p.asgResolveSpec(reqCtx, m.LaunchTemplateSpecification)
	if awsErr != nil {
		return nil, awsErr
	}
	if len(m.Overrides) == 0 {
		return []asgCandidate{{spec: base}}, nil
	}
	var out []asgCandidate
	for _, ov := range m.Overrides {
		spec := base
		if ov.LaunchTemplateSpecification != nil {
			if spec, awsErr = p.asgResolveSpec(reqCtx, *ov.LaunchTemplateSpecification); awsErr != nil {
				return nil, awsErr
			}
		}
		if ov.InstanceRequirements == nil {
			out = append(out, asgCandidate{spec: spec, instanceType: ov.InstanceType, imageID: ov.ImageID})
			continue
		}
		arch := p.asgImageArchitecture(reqCtx, spec, ov.ImageID)
		for _, info := range asgMatchInstanceTypes(ov.InstanceRequirements, arch) {
			out = append(out, asgCandidate{spec: spec, instanceType: info.InstanceType, imageID: ov.ImageID})
		}
	}
	return out, nil
}

// asgImageArchitecture returns the architecture of the AMI a candidate would launch — the
// override's ImageId when it names one, the resolved template version's otherwise — or ""
// when there is no AMI or it does not resolve.
//
// It is what attribute-based selection narrows by (#1497); see [asgMatchInstanceTypes]. The
// AMI is resolved through [EC2Plugin.resolveImage], the lookup the launch itself makes, so a
// type chosen here is chosen for the image the launch then uses. "" never refuses anything
// here: an AMI that does not resolve fails the launch with EC2's own error, which names the
// AMI, where narrowing to nothing would report a mismatch the caller cannot see the cause of.
func (p *AutoScalingPlugin) asgImageArchitecture(reqCtx *RequestContext, spec AutoScalingLaunchTemplateSpec, imageID string) string {
	if imageID == "" {
		lt := ec2LookupLaunchTemplate(context.Background(), p.state, reqCtx, spec.LaunchTemplateID, spec.LaunchTemplateName)
		if lt == nil {
			return ""
		}
		version, awsErr := ec2ResolveTemplateVersion(lt, spec.Version)
		if awsErr != nil {
			return ""
		}
		imageID = version.Data.ImageID
	}
	ec2, err := p.ec2()
	if err != nil {
		return ""
	}
	img, ok := ec2.resolveImage(reqCtx, imageID)
	if !ok {
		return ""
	}
	return img.Architecture
}

// asgPlacements resolves where a group can launch. Every subnet in VPCZoneIdentifier must
// exist; its zone is the subnet's own.
func (p *AutoScalingPlugin) asgPlacements(reqCtx *RequestContext, g *AutoScalingGroup) ([]asgPlacement, *AWSError) {
	subnets := asgSplitSubnets(g.VPCZoneIdentifier)
	if len(subnets) == 0 {
		if len(g.AvailabilityZones) == 0 {
			// Observed wording; the reference publishes the members, not the refusal.
			return nil, asgValidationError("At least one Availability Zone or VPC Subnet is required.")
		}
		out := make([]asgPlacement, 0, len(g.AvailabilityZones))
		for _, az := range g.AvailabilityZones {
			out = append(out, asgPlacement{az: az})
		}
		return out, nil
	}
	out := make([]asgPlacement, 0, len(subnets))
	for _, id := range subnets {
		data, err := p.state.Get(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+id)
		if err != nil || data == nil {
			return nil, asgValidationError("The subnet ID '%s' does not exist", id)
		}
		var sub EC2Subnet
		if err := json.Unmarshal(data, &sub); err != nil {
			return nil, asgValidationError("The subnet ID '%s' does not exist", id)
		}
		out = append(out, asgPlacement{subnetID: id, az: sub.AvailabilityZone})
	}
	return out, nil
}

// asgZones returns the distinct zones of a placement list, sorted, which is the group's
// AvailabilityZones member.
func asgZones(placements []asgPlacement) []string {
	var out []string
	for _, pl := range placements {
		if !containsStr(out, pl.az) {
			out = append(out, pl.az)
		}
	}
	sort.Strings(out)
	return out
}

// asgLaunchTags is the tag set one launch applies: the template version's instance tags,
// then the group's PropagateAtLaunch tags — which win on a shared key, as the tagging guide
// states ("the tag value from the group takes precedence") — then the reserved group-name tag.
//
// Handing EC2 a non-empty tag set means EC2 does not apply the template's tags itself, so the
// rules EC2 applies to them on that path (#471) are applied here instead, to the merged set:
// [ec2CheckTagRules] refuses a reserved key or a malformed tag from the template, and
// [ec2CheckTagLimit] counts the merged set against EC2's 50. The group-name tag is appended
// after both checks and is the only reserved key that bypasses them; the limit check excludes
// reserved keys from its count regardless, as EC2's tagging rules do.
func (p *AutoScalingPlugin) asgLaunchTags(reqCtx *RequestContext, g *AutoScalingGroup, spec AutoScalingLaunchTemplateSpec) ([]EC2Tag, *AWSError) {
	byKey := map[string]string{}
	var order []string
	set := func(k, v string) {
		if _, ok := byKey[k]; !ok {
			order = append(order, k)
		}
		byKey[k] = v
	}
	if lt := ec2LookupLaunchTemplate(context.Background(), p.state, reqCtx, spec.LaunchTemplateID, ""); lt != nil {
		if version, awsErr := ec2ResolveTemplateVersion(lt, spec.Version); awsErr == nil {
			for _, t := range version.Data.TagSpecifications {
				set(t.Key, t.Value)
			}
		}
	}
	for _, t := range g.Tags {
		if t.PropagateAtLaunch {
			set(t.Key, t.Value)
		}
	}
	out := make([]EC2Tag, 0, len(order)+1)
	for _, k := range order {
		out = append(out, EC2Tag{Key: k, Value: byKey[k]})
	}
	if awsErr := ec2CheckTagRules(out); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, out); awsErr != nil {
		return nil, awsErr
	}
	return append(out, EC2Tag{Key: asgAutoScalingGroupTagKey, Value: g.Name}), nil
}

// asgLaunchRequest is the RunInstances request one group launch sends. It is built in one
// place so the authorization decision CreateAutoScalingGroup makes and the launch it
// authorizes are about the same request, as [ec2FleetLaunchRequest] is for CreateFleet.
//
// The group's propagated tags travel as instance-scoped TagSpecification parameters too.
// The launch does not read them — its tags are handed to runInstancesWithTags already merged
// — but the authorization decision does: aws:RequestTag/<key> is what a "may only launch
// tagged instances" policy conditions on.
func asgLaunchRequest(g *AutoScalingGroup, cand asgCandidate, pl asgPlacement, spot bool) *AWSRequest {
	params := map[string]string{
		"Action":                          "RunInstances",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"LaunchTemplate.LaunchTemplateId": cand.spec.LaunchTemplateID,
		"LaunchTemplate.Version":          cand.spec.Version,
	}
	if cand.instanceType != "" {
		params["InstanceType"] = cand.instanceType
	}
	if cand.imageID != "" {
		params["ImageId"] = cand.imageID
	}
	if pl.subnetID != "" {
		params["SubnetId"] = pl.subnetID
	} else if pl.az != "" {
		params["Placement.AvailabilityZone"] = pl.az
	}
	if g.PlacementGroup != "" {
		params["Placement.GroupName"] = g.PlacementGroup
	}
	if spot {
		params["InstanceMarketOptions.MarketType"] = "spot"
	}
	n := 0
	for _, t := range g.Tags {
		if !t.PropagateAtLaunch {
			continue
		}
		n++
		if n == 1 {
			params["TagSpecification.1.ResourceType"] = "instance"
		}
		params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", n)] = t.Key
		params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", n)] = t.Value
	}
	return &AWSRequest{Service: "ec2", Operation: "RunInstances", Params: params, Headers: map[string]string{}}
}

// authorizeLaunches checks the caller's permission to launch what the group would launch,
// before anything is written.
//
// The Auto Scaling user guide is the authority: "When a user calls the
// CreateAutoScalingGroup, UpdateAutoScalingGroup, or StartInstanceRefresh APIs, Amazon EC2
// Auto Scaling checks their permissions against the version of the launch template that is
// the latest or default version at that time … we issue an Amazon EC2 RunInstances dry run
// call". After that, "whenever instances launch, Amazon EC2 Auto Scaling launches them …
// using the permissions of its service-linked role" — so SetDesiredCapacity, a replacement
// and a scale-out on observation are not authorized against the caller, and neither are they
// here. One decision per distinct template, against the first placement, as one dry run per
// template is what the guide describes.
//
// A nil controller or a nil principal is not enforced, matching CreateFleet and
// CloudFormation.
//
// A refusal is not EC2's: the dry run's denial is reported as the Auto Scaling operation's
// own `AccessDenied`, HTTP 403, "You are not authorized to use launch template: <template>".
// No page publishes it; it is the response real Auto Scaling returns, as reported verbatim by
// SDK users (aws-sdk-go-v2: "api error AccessDenied: You are not authorized to use launch
// template: lt-06f8064db4d66903f", StatusCode 403; boto3: "An error occurred (AccessDenied)
// when calling the CreateAutoScalingGroup operation: You are not authorized to use launch
// template: …"). The template is named the way the request named it.
func (p *AutoScalingPlugin) authorizeLaunches(reqCtx *RequestContext, req *AWSRequest, g *AutoScalingGroup, cands []asgCandidate, pls []asgPlacement) error {
	if p.auth == nil || reqCtx.Principal == nil || len(pls) == 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, c := range cands {
		if seen[c.spec.LaunchTemplateID] {
			continue
		}
		seen[c.spec.LaunchTemplateID] = true
		if err := p.auth.CheckAccess(reqCtx, asgLaunchRequest(g, c, pls[0], false)); err != nil {
			var awsErr *AWSError
			if !errors.As(err, &awsErr) {
				return err
			}
			return &AWSError{
				Code:       "AccessDenied",
				Message:    "You are not authorized to use launch template: " + asgTemplateLabel(req, c.spec),
				HTTPStatus: http.StatusForbidden,
			}
		}
	}
	return nil
}

// asgTemplateLabel names a template as the request named it: by name when some
// LaunchTemplateName member in the request carries it, by ID otherwise.
func asgTemplateLabel(req *AWSRequest, spec AutoScalingLaunchTemplateSpec) string {
	for k, v := range req.Params {
		if strings.HasSuffix(k, "LaunchTemplateName") && v == spec.LaunchTemplateName && v != "" {
			return v
		}
	}
	return spec.LaunchTemplateID
}

// ec2 returns the EC2 plugin the registry holds, which every launch runs through.
func (p *AutoScalingPlugin) ec2() (*EC2Plugin, error) {
	if p.registry == nil {
		return nil, errors.New("autoscaling: no plugin registry to reach EC2 through")
	}
	plugin, ok := p.registry.Plugin("ec2")
	if !ok {
		return nil, errors.New("autoscaling: EC2 is not registered")
	}
	ec2, ok := plugin.(*EC2Plugin)
	if !ok {
		return nil, fmt.Errorf("autoscaling: the registered ec2 plugin is %T, not *EC2Plugin", plugin)
	}
	return ec2, nil
}

// launchOne runs one launch for the group and returns the member record.
//
// It calls [EC2Plugin.launchInstances] rather than routing the request, for the reason
// [asgAutoScalingGroupTagKey] gives: the reserved tag cannot be expressed as a request
// parameter, because EC2 refuses a caller's reserved keys and must keep refusing them.
// Everything else about the launch is EC2's own path — template merge, AMI and subnet checks,
// default VPC — so a group instance is indistinguishable from any other to DescribeInstances.
//
// A Spot launch carries InstanceMarketOptions.MarketType=spot on the request — which is what
// the authorization decision sees — and is launched as CreateFleet launches a Spot pool: the
// instance reports instanceLifecycle spot and no sir- request is written, because the group,
// like a fleet, is the record that tracks the capacity (#1436).
func (p *AutoScalingPlugin) launchOne(reqCtx *RequestContext, g *AutoScalingGroup, cand asgCandidate, pl asgPlacement, spot bool) (AutoScalingInstance, error) {
	ec2, err := p.ec2()
	if err != nil {
		return AutoScalingInstance{}, err
	}
	tags, awsErr := p.asgLaunchTags(reqCtx, g, cand.spec)
	if awsErr != nil {
		return AutoScalingInstance{}, awsErr
	}
	var market *ec2SpotLaunch
	if spot {
		market = &ec2SpotLaunch{fleetOnly: true}
	}
	instances, _, _, err := ec2.launchInstances(reqCtx, asgLaunchRequest(g, cand, pl, spot), tags, market)
	if err != nil {
		return AutoScalingInstance{}, err
	}
	if len(instances) != 1 {
		return AutoScalingInstance{}, fmt.Errorf("autoscaling: the launch wrote %d instances, want 1", len(instances))
	}
	got := instances[0]
	g.LaunchSeq++
	return AutoScalingInstance{
		InstanceID:           got.InstanceID,
		InstanceType:         got.InstanceType,
		AvailabilityZone:     got.AvailabilityZone,
		SubnetID:             got.SubnetID,
		LaunchTemplate:       cand.spec,
		ImageID:              cand.imageID,
		ProtectedFromScaleIn: g.NewInstancesProtectedFromScaleIn,
		HealthStatus:         "Healthy",
		Spot:                 spot,
		LaunchSeq:            g.LaunchSeq,
		LaunchTime:           p.tc.Now().UTC(),
	}, nil
}

// terminateEC2 terminates instances through EC2's own termination path, so volume settlement
// and the shutting-down transition apply as they would to a caller's TerminateInstances.
//
// It is not routed as a TerminateInstances request, because a group's termination is not
// one: "Enabling termination protection does not prevent Amazon EC2 Auto Scaling from
// terminating an instance when the instance is unhealthy or during scale-in events" (EC2
// user guide). [EC2Plugin.terminateForAutoScaling] is that path. Substrate applies it to every
// termination a group makes — scale-in, health replacement, TerminateInstanceInAutoScalingGroup
// and ForceDelete — since all run as the group's service-linked role; the guide names the first
// two. Every named instance is resolved before any is terminated, so a batch fails whole.
func (p *AutoScalingPlugin) terminateEC2(reqCtx *RequestContext, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ec2, err := p.ec2()
	if err != nil {
		return err
	}
	return ec2.terminateForAutoScaling(reqCtx, ids)
}

// asgActivityMessage is the StatusMessage a failed launch records: the EC2 refusal followed
// by the published suffix, as DescribeScalingActivities' sample shows ("The image id
// 'ami-xxxxxxxx' does not exist. Launching EC2 instance failed.").
func asgActivityMessage(err error) string {
	msg := err.Error()
	var awsErr *AWSError
	if errors.As(err, &awsErr) {
		msg = awsErr.Message
	}
	msg = strings.TrimSuffix(strings.TrimSpace(msg), ".")
	return msg + ". Launching EC2 instance failed."
}

// asgDetails renders an activity's Details member in the published key order.
func asgDetails(az, subnetID string) string {
	if az == "" {
		return "{}"
	}
	if subnetID == "" {
		return fmt.Sprintf(`{"Availability Zone":%q}`, az)
	}
	return fmt.Sprintf(`{"Availability Zone":%q,"SubnetID":%q}`, az, subnetID)
}

// record writes a settled activity for g.
func (p *AutoScalingPlugin) record(reqCtx *RequestContext, g *AutoScalingGroup, status, description, cause, details, message string) (*AutoScalingActivity, error) {
	now := p.tc.Now().UTC()
	a := &AutoScalingActivity{
		ActivityID:    reqCtx.IDs.HexUUID(),
		GroupName:     g.Name,
		GroupARN:      g.ARN,
		Description:   description,
		Cause:         cause,
		Details:       details,
		StatusCode:    status,
		StatusMessage: message,
		Progress:      100,
		StartTime:     now,
		EndTime:       now,
	}
	if err := p.putActivity(reqCtx, a); err != nil {
		return nil, err
	}
	g.LastActivityTime = now
	return a, nil
}

// refreshMembership drops members whose EC2 instance is gone, recording each as a
// termination, and reports how many it dropped.
//
// An instance terminated, shutting down, stopped or stopping fails the EC2 health check a
// group always runs. Suspending HealthCheck stops the group noticing at all; suspending
// ReplaceUnhealthy leaves it a member marked Unhealthy, which is how real Auto Scaling
// behaves with that process suspended. A stopped instance is terminated through EC2, since a
// group replaces rather than restarts.
func (p *AutoScalingPlugin) refreshMembership(reqCtx *RequestContext, g *AutoScalingGroup) error {
	if g.suspended("HealthCheck") {
		return nil
	}
	kept := g.Instances[:0:0]
	for _, inst := range g.Instances {
		state, err := p.ec2InstanceState(reqCtx, inst.InstanceID)
		if err != nil {
			return err
		}
		switch state {
		case "pending", "running":
			kept = append(kept, inst)
			continue
		}
		if g.suspended("ReplaceUnhealthy") {
			inst.HealthStatus = "Unhealthy"
			kept = append(kept, inst)
			continue
		}
		if state == "stopped" || state == "stopping" {
			if err := p.terminateEC2(reqCtx, []string{inst.InstanceID}); err != nil {
				kept = append(kept, inst)
				continue
			}
		}
		now := asgCauseTime(p.tc.Now())
		if _, err := p.record(reqCtx, g, "Successful",
			"Terminating EC2 instance: "+inst.InstanceID,
			"At "+now+" an instance was taken out of service in response to an EC2 health check indicating it has been terminated or stopped.",
			asgDetails(inst.AvailabilityZone, inst.SubnetID), ""); err != nil {
			return err
		}
	}
	g.Instances = kept
	return nil
}

// ec2InstanceState reads an instance's settled state from EC2's record; an instance with no
// record reads as terminated, since nothing a group could replace it with is there.
func (p *AutoScalingPlugin) ec2InstanceState(reqCtx *RequestContext, id string) (string, error) {
	data, err := p.state.Get(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+id)
	if err != nil {
		return "", fmt.Errorf("autoscaling read ec2 instance %s: %w", id, err)
	}
	if data == nil {
		return ec2StateTerminated, nil
	}
	var inst EC2Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return "", fmt.Errorf("autoscaling unmarshal ec2 instance %s: %w", id, err)
	}
	return inst.State.Name, nil
}

// reconcile closes the gap between g's desired capacity and its members, then saves g.
//
// cause is the user-request clause of every activity this records ("At … a user request
// explicitly set group desired capacity …"), empty when the reconciliation is an
// observation's. Launch and Terminate suspensions are honored: a suspended process leaves
// the gap open, exactly as a describe of a real group with that process suspended shows.
//
// The group is always written back: every caller of reconcile is a handler that has just
// changed it. An observation goes through [AutoScalingPlugin.observe], which writes only
// when the reconciliation itself changed something.
func (p *AutoScalingPlugin) reconcile(reqCtx *RequestContext, g *AutoScalingGroup, cause string) error {
	return p.reconcileGroup(reqCtx, g, cause, true)
}

// observe is an observation's reconciliation: the group is written back only when it changed,
// so describing a settled group writes nothing — no state churn, and no state-hash change for a
// replay to compare.
func (p *AutoScalingPlugin) observe(reqCtx *RequestContext, g *AutoScalingGroup) error {
	return p.reconcileGroup(reqCtx, g, "", false)
}

// reconcileGroup is the reconciliation both share.
func (p *AutoScalingPlugin) reconcileGroup(reqCtx *RequestContext, g *AutoScalingGroup, cause string, alwaysSave bool) error {
	before, err := json.Marshal(g)
	if err != nil {
		return fmt.Errorf("autoscaling marshal group %s: %w", g.Name, err)
	}
	if err := p.refreshMembership(reqCtx, g); err != nil {
		return err
	}
	switch have := len(g.Instances); {
	case have < g.DesiredCapacity && !g.suspended("Launch"):
		if err := p.scaleOut(reqCtx, g, cause); err != nil {
			return err
		}
	case have > g.DesiredCapacity && !g.suspended("Terminate"):
		if err := p.scaleIn(reqCtx, g, cause); err != nil {
			return err
		}
	}
	after, err := json.Marshal(g)
	if err != nil {
		return fmt.Errorf("autoscaling marshal group %s: %w", g.Name, err)
	}
	if !alwaysSave && bytes.Equal(before, after) {
		return nil
	}
	return p.saveGroup(reqCtx, g)
}

// asgPrefixCause joins the user-request clause to an activity's own clause, two spaces
// apart as the published samples join them.
func asgPrefixCause(cause, clause string) string {
	if cause == "" {
		return clause
	}
	return cause + "  " + clause
}

// scaleOut launches until the group reaches its desired capacity, or until one launch fails.
//
// A failure — seeded through `POST /v1/autoscaling/launch-failure`, or EC2's own refusal of
// the launch — records one Failed activity and stops, leaving the capacity unmet. Every later
// observation tries again, so clearing the seed lets the next describe launch; but an
// observation whose attempt fails exactly as the recorded one did (same desired capacity,
// same StatusMessage) records nothing new. AWS publishes no retry cadence to model as a
// countdown, so the rule is one Failed activity per desired-capacity change: a request that
// changes capacity always records its attempt, and a poll loop over a seeded group sees one
// failure rather than one per poll.
func (p *AutoScalingPlugin) scaleOut(reqCtx *RequestContext, g *AutoScalingGroup, cause string) error {
	from := len(g.Instances)
	to := g.DesiredCapacity
	now := asgCauseTime(p.tc.Now())
	clause := fmt.Sprintf("At %s an instance was started in response to a difference between desired and actual capacity, increasing the capacity from %d to %d.", now, from, to)

	fail := func(message string) error {
		marker := strconv.Itoa(g.DesiredCapacity) + "|" + message
		if cause == "" && g.LastLaunchFailure == marker {
			return nil
		}
		g.LastLaunchFailure = marker
		_, err := p.record(reqCtx, g, "Failed",
			"Launching a new EC2 instance.  Status Reason: "+message,
			asgPrefixCause(cause, clause), "{}", message)
		return err
	}

	if seed, err := p.launchFailureSeed(g.Name); err != nil {
		return err
	} else if seed != nil {
		return fail(seed.message())
	}
	cands, awsErr := p.asgCandidates(reqCtx, g)
	if awsErr != nil {
		return fail(asgActivityMessage(awsErr))
	}
	if len(cands) == 0 {
		return fail("No instance types match the instance requirements of the mixed instances policy. Launching EC2 instance failed.")
	}
	pls, awsErr := p.asgPlacements(reqCtx, g)
	if awsErr != nil {
		return fail(asgActivityMessage(awsErr))
	}

	for len(g.Instances) < g.DesiredCapacity {
		pl := asgChoosePlacement(g, pls)
		spot := asgLaunchSpot(g)
		inst, err := p.launchOne(reqCtx, g, cands[0], pl, spot)
		if err != nil {
			var launchErr *AWSError
			if !errors.As(err, &launchErr) {
				return err
			}
			return fail(asgActivityMessage(launchErr))
		}
		g.Instances = append(g.Instances, inst)
		g.LastLaunchFailure = ""
		if _, err := p.record(reqCtx, g, "Successful",
			"Launching a new EC2 instance: "+inst.InstanceID,
			asgPrefixCause(cause, clause),
			asgDetails(inst.AvailabilityZone, inst.SubnetID), ""); err != nil {
			return err
		}
	}
	return nil
}

// asgChoosePlacement picks where the next launch goes: the zone holding the fewest members,
// ties to the zone listed first, then that zone's least-used subnet. Balancing across zones
// is what "attempts to balance your capacity evenly across your specified Availability
// Zones" asks; the tie-break is substrate's, chosen so the answer is stable.
func asgChoosePlacement(g *AutoScalingGroup, pls []asgPlacement) asgPlacement {
	perAZ := map[string]int{}
	perSubnet := map[string]int{}
	for _, inst := range g.Instances {
		perAZ[inst.AvailabilityZone]++
		perSubnet[inst.SubnetID]++
	}
	best := -1
	for i, pl := range pls {
		if best < 0 {
			best = i
			continue
		}
		cur := pls[best]
		switch {
		case perAZ[pl.az] < perAZ[cur.az]:
			best = i
		case pl.az == cur.az && perSubnet[pl.subnetID] < perSubnet[cur.subnetID]:
			best = i
		}
	}
	return pls[best]
}

// asgLaunchSpot reports whether the group's next launch is a Spot Instance: it is when the
// group already holds the On-Demand count [asgTargetOnDemand] asks for at its next size.
func asgLaunchSpot(g *AutoScalingGroup) bool {
	if g.MixedInstancesPolicy == nil {
		return false
	}
	onDemand := 0
	for _, inst := range g.Instances {
		if !inst.Spot {
			onDemand++
		}
	}
	return onDemand >= asgTargetOnDemand(len(g.Instances)+1, g.MixedInstancesPolicy.InstancesDistribution)
}

// scaleIn terminates unprotected members until the group is down to its desired capacity.
// A group whose remaining members are all protected stays above it, as real Auto Scaling
// does — protection is "from termination by Amazon EC2 Auto Scaling when scaling in".
func (p *AutoScalingPlugin) scaleIn(reqCtx *RequestContext, g *AutoScalingGroup, cause string) error {
	from := len(g.Instances)
	to := g.DesiredCapacity
	for len(g.Instances) > g.DesiredCapacity {
		victim := p.asgSelectVictim(g)
		if victim < 0 {
			return nil
		}
		inst := g.Instances[victim]
		now := asgCauseTime(p.tc.Now())
		clause := fmt.Sprintf("At %s an instance was taken out of service in response to a difference between desired and actual capacity, shrinking the capacity from %d to %d.  At %s instance %s was selected for termination.",
			now, from, to, now, inst.InstanceID)
		if err := p.terminateEC2(reqCtx, []string{inst.InstanceID}); err != nil {
			var awsErr *AWSError
			if !errors.As(err, &awsErr) {
				return err
			}
			// EC2 refused — termination protection, say. The instance stays a member and the
			// group stays above its desired capacity, which is what the Failed activity says.
			_, recErr := p.record(reqCtx, g, "Failed", "Terminating EC2 instance: "+inst.InstanceID,
				asgPrefixCause(cause, clause), asgDetails(inst.AvailabilityZone, inst.SubnetID), awsErr.Message)
			return recErr
		}
		g.Instances = append(g.Instances[:victim:victim], g.Instances[victim+1:]...)
		if _, err := p.record(reqCtx, g, "Successful", "Terminating EC2 instance: "+inst.InstanceID,
			asgPrefixCause(cause, clause), asgDetails(inst.AvailabilityZone, inst.SubnetID), ""); err != nil {
			return err
		}
	}
	return nil
}

// asgSelectVictim returns the index of the member the group's termination policies choose,
// or -1 when every member is protected.
//
// The order is the termination-policy guide's, as far as it is observable here. For a
// mixed-instances group the purchase option is chosen first, so the group "trends toward the
// specified ratio of Spot and On-Demand instances". Then zonal balance, which "takes
// precedence over the termination policy": the zone with the most members and at least one
// unprotected one, ties to the zone name sorting first. Then the policies, in the group's
// order, each narrowing the field; Default is outdated launch template, then oldest version
// of the current one, then closest to the next billing hour. Where real Auto Scaling would
// "terminate one at random", substrate takes the lowest instance ID, so a replay chooses the
// same instance.
func (p *AutoScalingPlugin) asgSelectVictim(g *AutoScalingGroup) int {
	var field []int
	for i, inst := range g.Instances {
		if !inst.ProtectedFromScaleIn {
			field = append(field, i)
		}
	}
	if len(field) == 0 {
		return -1
	}

	if m := g.MixedInstancesPolicy; m != nil {
		onDemand := 0
		for _, inst := range g.Instances {
			if !inst.Spot {
				onDemand++
			}
		}
		wantSpot := onDemand <= asgTargetOnDemand(len(g.Instances)-1, m.InstancesDistribution)
		if narrowed := asgFilter(field, func(i int) bool { return g.Instances[i].Spot == wantSpot }); len(narrowed) > 0 {
			field = narrowed
		}
	}

	perAZ := map[string]int{}
	for _, inst := range g.Instances {
		perAZ[inst.AvailabilityZone]++
	}
	bestAZ := ""
	for _, i := range field {
		az := g.Instances[i].AvailabilityZone
		if bestAZ == "" || perAZ[az] > perAZ[bestAZ] || (perAZ[az] == perAZ[bestAZ] && az < bestAZ) {
			bestAZ = az
		}
	}
	field = asgFilter(field, func(i int) bool { return g.Instances[i].AvailabilityZone == bestAZ })

	policies := g.TerminationPolicies
	if len(policies) == 0 {
		policies = []string{"Default"}
	}
	for _, policy := range policies {
		if len(field) <= 1 {
			break
		}
		field = p.asgApplyPolicy(g, field, policy)
	}
	if len(field) > 1 {
		field = p.asgApplyPolicy(g, field, "Default")
	}
	sort.Slice(field, func(a, b int) bool { return g.Instances[field[a]].InstanceID < g.Instances[field[b]].InstanceID })
	return field[0]
}

// asgFilter returns the members of field satisfying keep.
func asgFilter(field []int, keep func(int) bool) []int {
	var out []int
	for _, i := range field {
		if keep(i) {
			out = append(out, i)
		}
	}
	return out
}

// asgBest narrows field to the members scoring highest.
func asgBest(field []int, score func(int) int64) []int {
	var out []int
	var best int64
	for _, i := range field {
		s := score(i)
		switch {
		case len(out) == 0 || s > best:
			out, best = []int{i}, s
		case s == best:
			out = append(out, i)
		}
	}
	return out
}

// asgApplyPolicy narrows field by one termination policy. A policy whose criterion
// substrate cannot observe — AllocationStrategy, OldestLaunchConfiguration, a Lambda ARN —
// narrows nothing, which leaves the decision to the policies after it and finally Default.
func (p *AutoScalingPlugin) asgApplyPolicy(g *AutoScalingGroup, field []int, policy string) []int {
	switch policy {
	case "Default":
		field = p.asgApplyPolicy(g, field, "OldestLaunchTemplate")
		return p.asgApplyPolicy(g, field, "ClosestToNextInstanceHour")
	case "OldestLaunchTemplate":
		current := g.currentTemplateIDs()
		if outdated := asgFilter(field, func(i int) bool {
			return !containsStr(current, g.Instances[i].LaunchTemplate.LaunchTemplateID)
		}); len(outdated) > 0 {
			return outdated
		}
		return asgBest(field, func(i int) int64 {
			v, _ := strconv.ParseInt(g.Instances[i].LaunchTemplate.Version, 10, 64)
			return -v
		})
	case "ClosestToNextInstanceHour":
		now := p.tc.Now()
		return asgBest(field, func(i int) int64 {
			return int64(now.Sub(g.Instances[i].LaunchTime) % time.Hour)
		})
	case "OldestInstance":
		return asgBest(field, func(i int) int64 { return -g.Instances[i].LaunchSeq })
	case "NewestInstance":
		return asgBest(field, func(i int) int64 { return g.Instances[i].LaunchSeq })
	}
	return field
}

// currentTemplateIDs returns the launch template IDs the group's configuration names now.
// Every stored specification carries its template's ID, whichever identifier the request
// used, because Create and Update fill it in (asgFillSpec).
func (g *AutoScalingGroup) currentTemplateIDs() []string {
	var out []string
	add := func(s *AutoScalingLaunchTemplateSpec) {
		if s != nil && s.LaunchTemplateID != "" {
			out = append(out, s.LaunchTemplateID)
		}
	}
	add(g.LaunchTemplate)
	if m := g.MixedInstancesPolicy; m != nil {
		add(&m.LaunchTemplateSpecification)
		for _, ov := range m.Overrides {
			add(ov.LaunchTemplateSpecification)
		}
	}
	return out
}
