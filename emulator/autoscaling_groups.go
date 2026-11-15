package emulator

import (
	"context"
	"fmt"
	"strings"
)

// asgDefaultCooldown is DefaultCooldown's documented default: "Default: 300 seconds".
const asgDefaultCooldown = 300

// asgServiceLinkedRoleARN is the default role a group names, AWSServiceRoleForAutoScaling.
// The role is not created in IAM: nothing substrate does on a group's behalf is authorized
// against it, so a record would be an observation with nothing behind it.
func asgServiceLinkedRoleARN(account string) string {
	return "arn:aws:iam::" + account + ":role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling"
}

// asgGroupARN is a group's ARN, in the format the Service Authorization Reference publishes:
// arn:${Partition}:autoscaling:${Region}:${Account}:autoScalingGroup:${GroupId}:autoScalingGroupName/${GroupFriendlyName}.
func asgGroupARN(region, account, groupID, name string) string {
	return "arn:aws:autoscaling:" + region + ":" + account + ":autoScalingGroup:" + groupID + ":autoScalingGroupName/" + name
}

// asgCheckSizes refuses sizes that contradict each other. The wording is real Auto
// Scaling's, observed rather than published.
func asgCheckSizes(minSize, maxSize, desired int) *AWSError {
	if minSize < 0 || maxSize < 0 || desired < 0 {
		return asgValidationError("MinSize, MaxSize and DesiredCapacity must be greater than or equal to 0.")
	}
	if maxSize < minSize {
		return asgValidationError("Max bound, %d, must be greater than or equal to min bound, %d", maxSize, minSize)
	}
	if desired < minSize || desired > maxSize {
		return asgValidationError("Desired capacity:%d must be between the specified min size:%d and max size:%d", desired, minSize, maxSize)
	}
	return nil
}

// asgApplyLaunchConfig reads the launch-configuration members of a Create or Update into g:
// LaunchTemplate or MixedInstancesPolicy, exactly one of which a group launches from.
//
// A launch configuration — LaunchConfigurationName or InstanceId — is refused. Launch
// configurations are out of scope (#1460), and accepting one would make a group that can
// never launch look valid.
func asgApplyLaunchConfig(params map[string]string, g *AutoScalingGroup, creating bool) *AWSError {
	if params["LaunchConfigurationName"] != "" || params["InstanceId"] != "" {
		return asgValidationError("Launch configurations are not emulated by substrate; specify LaunchTemplate or MixedInstancesPolicy.")
	}
	lt := asgParseLaunchTemplateSpec(params, "LaunchTemplate.")
	mixedSent := hasParamPrefix(params, "MixedInstancesPolicy.")
	switch {
	case lt != nil && mixedSent:
		// Observed wording.
		return asgValidationError("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or MixedInstancesPolicy parameter.")
	case lt != nil:
		// An update naming only LaunchTemplate.Version re-points the group's template, which
		// is "any properties that you don't specify are not changed" applied to the spec.
		if lt.LaunchTemplateID == "" && lt.LaunchTemplateName == "" && g.LaunchTemplate != nil {
			spec := *g.LaunchTemplate
			spec.Version = lt.Version
			lt = &spec
		}
		g.LaunchTemplate, g.MixedInstancesPolicy = lt, nil
	case mixedSent:
		mixed, awsErr := asgApplyMixedInstancesPolicy(params, g.MixedInstancesPolicy)
		if awsErr != nil {
			return awsErr
		}
		g.LaunchTemplate, g.MixedInstancesPolicy = nil, mixed
	case creating:
		return asgValidationError("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or MixedInstancesPolicy parameter.")
	}
	return nil
}

// asgApplyCommon reads the members Create and Update share and that need no validation
// beyond their type.
func asgApplyCommon(params map[string]string, g *AutoScalingGroup) *AWSError {
	ints := []struct {
		key string
		dst *int
	}{
		{"DefaultCooldown", &g.DefaultCooldown},
		{"HealthCheckGracePeriod", &g.HealthCheckGracePeriod},
	}
	for _, f := range ints {
		v, ok, awsErr := asgInt(params, f.key)
		if awsErr != nil {
			return awsErr
		}
		if ok {
			*f.dst = v
		}
	}
	for _, f := range []struct {
		key string
		dst **int
	}{{"MaxInstanceLifetime", &g.MaxInstanceLifetime}, {"DefaultInstanceWarmup", &g.DefaultInstanceWarmup}} {
		v, awsErr := asgParseOptionalInt(params, f.key)
		if awsErr != nil {
			return awsErr
		}
		if v != nil {
			*f.dst = v
		}
	}
	if v, ok := params["HealthCheckType"]; ok && v != "" {
		g.HealthCheckType = v
	}
	if v, ok := params["VPCZoneIdentifier"]; ok {
		g.VPCZoneIdentifier = v
	}
	if azs := iamMemberList(params, "AvailabilityZones"); len(azs) > 0 {
		g.AvailabilityZones = azs
	}
	if v, ok := params["PlacementGroup"]; ok {
		g.PlacementGroup = v
	}
	if v, ok := params["DesiredCapacityType"]; ok && v != "" {
		switch v {
		case asgCapacityUnits, asgCapacityVCPU, asgCapacityMemoryMiB:
		default:
			return asgValidationError("DesiredCapacityType must be one of units, vcpu or memory-mib.")
		}
		g.DesiredCapacityType = v
	}
	if v, ok := params["ServiceLinkedRoleARN"]; ok && v != "" {
		g.ServiceLinkedRoleARN = v
	}
	if v, ok := params["Context"]; ok {
		g.Context = v
	}
	if v, ok := asgBool(params, "CapacityRebalance"); ok {
		g.CapacityRebalance = v
	}
	if v, ok := asgBool(params, "NewInstancesProtectedFromScaleIn"); ok {
		g.NewInstancesProtectedFromScaleIn = v
	}
	if policies := iamMemberList(params, "TerminationPolicies"); len(policies) > 0 {
		if awsErr := asgCheckTerminationPolicies(policies); awsErr != nil {
			return awsErr
		}
		g.TerminationPolicies = policies
	}
	return nil
}

// asgFillSpec completes a group's stored template specification with the template's ID and
// name, keeping the version as the request named it: DescribeAutoScalingGroups echoes all
// three, and [AutoScalingGroup.currentTemplateIDs] compares on the ID.
func (p *AutoScalingPlugin) asgFillSpec(reqCtx *RequestContext, spec *AutoScalingLaunchTemplateSpec) *AWSError {
	if spec == nil {
		return nil
	}
	resolved, awsErr := p.asgResolveSpec(reqCtx, *spec)
	if awsErr != nil {
		return awsErr
	}
	spec.LaunchTemplateID = resolved.LaunchTemplateID
	spec.LaunchTemplateName = resolved.LaunchTemplateName
	if spec.Version == "" {
		spec.Version = "$Default"
	}
	return nil
}

// asgPrepareLaunch validates and completes a group's launch configuration, returning the
// candidates and placements a launch would use so the caller can authorize them.
func (p *AutoScalingPlugin) asgPrepareLaunch(reqCtx *RequestContext, g *AutoScalingGroup) ([]asgCandidate, []asgPlacement, *AWSError) {
	if awsErr := p.asgFillSpec(reqCtx, g.LaunchTemplate); awsErr != nil {
		return nil, nil, awsErr
	}
	if m := g.MixedInstancesPolicy; m != nil {
		if awsErr := p.asgFillSpec(reqCtx, &m.LaunchTemplateSpecification); awsErr != nil {
			return nil, nil, awsErr
		}
		for i := range m.Overrides {
			if awsErr := p.asgFillSpec(reqCtx, m.Overrides[i].LaunchTemplateSpecification); awsErr != nil {
				return nil, nil, awsErr
			}
		}
	}
	// Create and update are where an attribute-based policy on a resolve:ssm: template is
	// refused (#1506); see [AutoScalingPlugin.asgCandidatesFor].
	cands, awsErr := p.asgCandidatesFor(reqCtx, g, true)
	if awsErr != nil {
		return nil, nil, awsErr
	}
	pls, awsErr := p.asgPlacements(reqCtx, g)
	if awsErr != nil {
		return nil, nil, awsErr
	}
	if azs := asgZones(pls); len(azs) > 0 && g.VPCZoneIdentifier != "" {
		g.AvailabilityZones = azs
	}
	return cands, pls, nil
}

func (p *AutoScalingPlugin) createAutoScalingGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["AutoScalingGroupName"]
	if name == "" {
		return nil, asgMissingParameter("AutoScalingGroupName")
	}
	if strings.Contains(name, ":") {
		// API_CreateAutoScalingGroup: "You cannot use a colon (:) in the name."
		return nil, asgValidationError("AutoScalingGroupName cannot contain a colon (:).")
	}
	if len(name) > 255 {
		return nil, asgValidationError("AutoScalingGroupName must be at most 255 characters.")
	}
	minSize, minOK, awsErr := asgInt(req.Params, "MinSize")
	if awsErr != nil {
		return nil, awsErr
	}
	maxSize, maxOK, awsErr := asgInt(req.Params, "MaxSize")
	if awsErr != nil {
		return nil, awsErr
	}
	if !minOK {
		return nil, asgMissingParameter("MinSize")
	}
	if !maxOK {
		return nil, asgMissingParameter("MaxSize")
	}
	desired, desiredOK, awsErr := asgInt(req.Params, "DesiredCapacity")
	if awsErr != nil {
		return nil, awsErr
	}
	if !desiredOK {
		// "If you do not specify a desired capacity, the default is the minimum size of the
		// group."
		desired = minSize
	}
	if awsErr := asgCheckSizes(minSize, maxSize, desired); awsErr != nil {
		return nil, awsErr
	}

	existing, err := p.loadGroup(reqCtx, name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// AlreadyExists, HTTP 400, is on this operation's Errors list; the wording is observed.
		return nil, &AWSError{Code: "AlreadyExists", Message: "AutoScalingGroup by this name already exists - A group with the name " + name + " already exists", HTTPStatus: 400}
	}

	g := &AutoScalingGroup{
		Name:                 name,
		MinSize:              minSize,
		MaxSize:              maxSize,
		DesiredCapacity:      desired,
		DefaultCooldown:      asgDefaultCooldown,
		HealthCheckType:      "EC2",
		ServiceLinkedRoleARN: asgServiceLinkedRoleARN(reqCtx.AccountID),
		TerminationPolicies:  []string{"Default"},
		CreatedTime:          p.tc.Now().UTC(),
		LoadBalancerNames:    iamMemberList(req.Params, "LoadBalancerNames"),
		TargetGroupARNs:      iamMemberList(req.Params, "TargetGroupARNs"),
	}
	if awsErr := asgApplyLaunchConfig(req.Params, g, true); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := asgApplyCommon(req.Params, g); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := asgCheckCapacityType(g); awsErr != nil {
		return nil, awsErr
	}

	tags := asgParseTags(req.Params)
	if awsErr := asgCheckTagKeys(tags); awsErr != nil {
		return nil, awsErr
	}
	plain := make([]AutoScalingTag, 0, len(tags))
	for _, t := range tags {
		plain = append(plain, t.AutoScalingTag)
	}
	g.Tags = asgMergeTags(nil, plain)
	if len(g.Tags) > asgMaxTags {
		return nil, &AWSError{Code: "LimitExceeded", Message: fmt.Sprintf("The number of tags exceeds the limit of %d.", asgMaxTags), HTTPStatus: 400}
	}

	cands, pls, awsErr := p.asgPrepareLaunch(reqCtx, g)
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.authorizeLaunches(reqCtx, req, g, cands, pls); err != nil {
		return nil, err
	}

	g.ARN = asgGroupARN(reqCtx.Region, reqCtx.AccountID, reqCtx.IDs.HexUUID(), name)
	cause := fmt.Sprintf("At %s a user request created an AutoScalingGroup changing the desired capacity from 0 to %d.",
		asgCauseTime(p.tc.Now()), desired)
	if err := p.reconcile(reqCtx, g, cause); err != nil {
		return nil, err
	}
	return asgNoResultResponse(reqCtx, "CreateAutoScalingGroup")
}

// requireGroup loads the group a request names in AutoScalingGroupName, refusing a missing
// name or group.
func (p *AutoScalingPlugin) requireGroup(reqCtx *RequestContext, req *AWSRequest) (*AutoScalingGroup, error) {
	name := req.Params["AutoScalingGroupName"]
	if name == "" {
		return nil, asgMissingParameter("AutoScalingGroupName")
	}
	g, err := p.loadGroup(reqCtx, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, asgGroupNotFound(name)
	}
	return g, nil
}

func (p *AutoScalingPlugin) updateAutoScalingGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	prevDesired := g.DesiredCapacity
	minSize, minOK, awsErr := asgInt(req.Params, "MinSize")
	if awsErr != nil {
		return nil, awsErr
	}
	maxSize, maxOK, awsErr := asgInt(req.Params, "MaxSize")
	if awsErr != nil {
		return nil, awsErr
	}
	desired, desiredOK, awsErr := asgInt(req.Params, "DesiredCapacity")
	if awsErr != nil {
		return nil, awsErr
	}
	if !minOK {
		minSize = g.MinSize
	}
	if !maxOK {
		maxSize = g.MaxSize
	}
	if !desiredOK {
		// API_UpdateAutoScalingGroup: a new MinSize above the group's size raises
		// DesiredCapacity to it, and a new MaxSize below the size lowers it, when the request
		// names no DesiredCapacity of its own.
		desired = g.DesiredCapacity
		if minOK && minSize > desired {
			desired = minSize
		}
		if maxOK && maxSize < desired {
			desired = maxSize
		}
	}
	if awsErr := asgCheckSizes(minSize, maxSize, desired); awsErr != nil {
		return nil, awsErr
	}

	next := *g
	next.MinSize, next.MaxSize, next.DesiredCapacity = minSize, maxSize, desired
	launchChanged := req.Params["LaunchTemplate.LaunchTemplateId"] != "" || req.Params["LaunchTemplate.LaunchTemplateName"] != "" ||
		req.Params["LaunchTemplate.Version"] != "" || hasParamPrefix(req.Params, "MixedInstancesPolicy.") ||
		req.Params["LaunchConfigurationName"] != ""
	if launchChanged {
		if awsErr := asgApplyLaunchConfig(req.Params, &next, false); awsErr != nil {
			return nil, awsErr
		}
	}
	if awsErr := asgApplyCommon(req.Params, &next); awsErr != nil {
		return nil, awsErr
	}
	// The capacity type is checked only when the request changes it or the launch
	// configuration it depends on. A vcpu or memory-mib group recorded before the check
	// existed (#1502) is held to it when it is next reconfigured, not on every update: it
	// counts instances meanwhile (see [AutoScalingGroup.weightBasis]).
	if _, typeSent := req.Params["DesiredCapacityType"]; typeSent || launchChanged {
		if awsErr := asgCheckCapacityType(&next); awsErr != nil {
			return nil, awsErr
		}
	}
	// The launch configuration is re-validated only when the request touches it or where the
	// group launches, so an update of, say, HealthCheckType does not start failing because
	// the template was deleted since; that group's next launch reports the template instead,
	// as a Failed activity.
	_, placementChanged := req.Params["VPCZoneIdentifier"]
	placementChanged = placementChanged || hasParamPrefix(req.Params, "AvailabilityZones.")
	if launchChanged || placementChanged {
		cands, pls, awsErr := p.asgPrepareLaunch(reqCtx, &next)
		if awsErr != nil {
			return nil, awsErr
		}
		if launchChanged {
			if err := p.authorizeLaunches(reqCtx, req, &next, cands, pls); err != nil {
				return nil, err
			}
		}
	}

	cause := ""
	if desired != prevDesired {
		cause = fmt.Sprintf("At %s a user request update of AutoScalingGroup constraints to min: %d, max: %d, desired: %d changing the desired capacity from %d to %d.",
			asgCauseTime(p.tc.Now()), minSize, maxSize, desired, prevDesired, desired)
	}
	if err := p.reconcile(reqCtx, &next, cause); err != nil {
		return nil, err
	}
	return asgNoResultResponse(reqCtx, "UpdateAutoScalingGroup")
}

// hasParamPrefix reports whether any parameter sits under prefix.
func hasParamPrefix(params map[string]string, prefix string) bool {
	for k := range params {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func (p *AutoScalingPlugin) deleteAutoScalingGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	if err := p.refreshMembership(reqCtx, g); err != nil {
		return nil, err
	}
	force, _ := asgBool(req.Params, "ForceDelete")
	if len(g.Instances) > 0 && !force {
		if err := p.saveGroup(reqCtx, g); err != nil {
			return nil, err
		}
		// API_DeleteAutoScalingGroup: "If the group has instances or scaling activities in
		// progress, you must specify the option to force the deletion in order for it to
		// succeed." ResourceInUse, HTTP 400, is on the Errors list; the wording is observed.
		return nil, &AWSError{
			Code:       "ResourceInUse",
			Message:    "You cannot delete an AutoScalingGroup while there are instances or pending Spot instance request(s) still in the group.",
			HTTPStatus: 400,
		}
	}
	ids := make([]string, 0, len(g.Instances))
	for _, inst := range g.Instances {
		ids = append(ids, inst.InstanceID)
	}
	// "The force delete operation will also terminate the EC2 instances." Termination
	// protection does not stop a group's terminations (see terminateEC2), and EC2 resolves the
	// whole batch before terminating any of it, so the batch terminates entirely or not at all.
	// When it does not, the request fails and the group stays, still owning its instances:
	// deleting it anyway would leave them running with nothing that manages them, and
	// recording them as terminated would be an activity log contradicting EC2.
	if len(ids) > 0 {
		if err := p.terminateEC2(reqCtx, ids); err != nil {
			// The membership refresh above may have dropped members and recorded it; keep the
			// group consistent with those activities before refusing.
			if saveErr := p.saveGroup(reqCtx, g); saveErr != nil {
				return nil, saveErr
			}
			return nil, err
		}
		now := asgCauseTime(p.tc.Now())
		for _, inst := range g.Instances {
			if _, err := p.record(reqCtx, g, "Successful", "Terminating EC2 instance: "+inst.InstanceID,
				"At "+now+" a user request deleted the AutoScalingGroup, terminating instance "+inst.InstanceID+".",
				asgDetails(inst.AvailabilityZone, inst.SubnetID), ""); err != nil {
				return nil, err
			}
		}
	}
	if err := p.state.Delete(context.Background(), autoscalingNamespace, asgGroupKey(reqCtx.AccountID, reqCtx.Region, g.Name)); err != nil {
		return nil, fmt.Errorf("autoscaling delete group %s: %w", g.Name, err)
	}
	return asgNoResultResponse(reqCtx, "DeleteAutoScalingGroup")
}

func (p *AutoScalingPlugin) setDesiredCapacity(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	desired, ok, awsErr := asgInt(req.Params, "DesiredCapacity")
	if awsErr != nil {
		return nil, awsErr
	}
	if !ok {
		return nil, asgMissingParameter("DesiredCapacity")
	}
	// Observed wording, both directions.
	if desired > g.MaxSize {
		return nil, asgValidationError("New SetDesiredCapacity value %d is above max value %d for the AutoScalingGroup.", desired, g.MaxSize)
	}
	if desired < g.MinSize {
		return nil, asgValidationError("New SetDesiredCapacity value %d is below min value %d for the AutoScalingGroup.", desired, g.MinSize)
	}
	// HonorCooldown: "Indicates whether Amazon EC2 Auto Scaling waits for the cooldown period
	// to complete before initiating a scaling activity … By default, Amazon EC2 Auto Scaling
	// does not honor the cooldown period during manual scaling activities." A real group would
	// wait; a request cannot, so it is refused with the operation's published
	// ScalingActivityInProgress. The clock is the simulated one, so the refusal replays.
	if honor, _ := asgBool(req.Params, "HonorCooldown"); honor && !g.LastActivityTime.IsZero() {
		if until := g.LastActivityTime.Add(secondsDuration(g.DefaultCooldown)); p.tc.Now().Before(until) {
			return nil, &AWSError{
				Code:       "ScalingActivityInProgress",
				Message:    fmt.Sprintf("The group %s is in its cooldown period until %s.", g.Name, asgCauseTime(until)),
				HTTPStatus: 400,
			}
		}
	}
	prev := g.DesiredCapacity
	g.DesiredCapacity = desired
	cause := fmt.Sprintf("At %s a user request explicitly set group desired capacity changing the desired capacity from %d to %d.",
		asgCauseTime(p.tc.Now()), prev, desired)
	if err := p.reconcile(reqCtx, g, cause); err != nil {
		return nil, err
	}
	return asgNoResultResponse(reqCtx, "SetDesiredCapacity")
}

func (p *AutoScalingPlugin) suspendProcesses(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeProcesses(reqCtx, req, "SuspendProcesses", true)
}

func (p *AutoScalingPlugin) resumeProcesses(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeProcesses(reqCtx, req, "ResumeProcesses", false)
}

// changeProcesses suspends or resumes the named processes, or all of them: "If you omit
// this property, all processes are specified" (API_SuspendProcesses).
func (p *AutoScalingPlugin) changeProcesses(reqCtx *RequestContext, req *AWSRequest, operation string, suspend bool) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	names := iamMemberList(req.Params, "ScalingProcesses")
	if len(names) == 0 {
		names = asgProcessNames
	}
	for _, n := range names {
		if !containsStr(asgProcessNames, n) {
			return nil, asgValidationError("Process %s is not a valid scaling process.", n)
		}
	}
	kept := make([]AutoScalingSuspendedProcess, 0, len(g.SuspendedProcesses)+len(names))
	for _, sp := range g.SuspendedProcesses {
		if !containsStr(names, sp.ProcessName) {
			kept = append(kept, sp)
		}
	}
	if suspend {
		reason := "User suspended at " + asgCauseTime(p.tc.Now())
		for _, sp := range g.SuspendedProcesses {
			if containsStr(names, sp.ProcessName) {
				kept = append(kept, sp) // an already-suspended process keeps its first reason
			}
		}
		for _, n := range names {
			if !g.suspended(n) {
				kept = append(kept, AutoScalingSuspendedProcess{ProcessName: n, SuspensionReason: reason})
			}
		}
	}
	sortSuspended(kept)
	g.SuspendedProcesses = kept
	if err := p.saveGroup(reqCtx, g); err != nil {
		return nil, err
	}
	return asgNoResultResponse(reqCtx, operation)
}

// sortSuspended orders suspended processes by name, so a describe is stable.
func sortSuspended(sp []AutoScalingSuspendedProcess) {
	for i := 1; i < len(sp); i++ {
		for j := i; j > 0 && sp[j].ProcessName < sp[j-1].ProcessName; j-- {
			sp[j], sp[j-1] = sp[j-1], sp[j]
		}
	}
}

func (p *AutoScalingPlugin) setInstanceProtection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	ids := iamMemberList(req.Params, "InstanceIds")
	if len(ids) == 0 {
		return nil, asgMissingParameter("InstanceIds")
	}
	if len(ids) > 50 {
		// "If you exceed your maximum limit of instance IDs, which is 50 per Auto Scaling
		// group, the call fails." LimitExceeded is on the Errors list.
		return nil, &AWSError{Code: "LimitExceeded", Message: "You can specify up to 50 instance IDs.", HTTPStatus: 400}
	}
	protected, ok := asgBool(req.Params, "ProtectedFromScaleIn")
	if !ok {
		return nil, asgMissingParameter("ProtectedFromScaleIn")
	}
	for _, id := range ids {
		if g.instanceIndex(id) < 0 {
			// Observed wording.
			return nil, asgValidationError("The instance %s is not part of Auto Scaling group %s.", id, g.Name)
		}
	}
	for _, id := range ids {
		g.Instances[g.instanceIndex(id)].ProtectedFromScaleIn = protected
	}
	if err := p.saveGroup(reqCtx, g); err != nil {
		return nil, err
	}
	return asgOKResponse(reqCtx, "SetInstanceProtection", struct{}{})
}
