package emulator

// The wire is a different thing from the state, and the three types below exist to keep
// them apart.
//
// CodeDeployApp, CodeDeployGroup and CodeDeployDeployment (codedeploy_types.go) are
// persisted state records, and each was handed straight to the caller at the one site that
// answers it: getApplication under `application`, getDeploymentGroup under
// `deploymentGroupInfo`, getDeployment under `deploymentInfo`. Two fields of each are
// substrate's own and neither carries omitempty, so all three responses answered accountID
// and region, which no CodeDeploy shape publishes (#756). CodeDeploy is the one service in
// this family where every record leaked: the six remaining routed operations build a map of
// one or two published members and render no record at all.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and
// leaves the next one to be remembered rather than prevented, and it changes the format of
// every recorded run, because MemoryStateManager snapshots those bytes and a replay reads
// them back. For the same reason CreateTime and CompleteTime stay time.Time in the records
// and are converted on projection rather than being retyped in place — which is also why
// this fix needs no RFC3339 fallback for an existing event log: the stored bytes are
// unchanged, so a run recorded before it replays identically (#1207 asks for the fallback
// because it assumed a stored-shape change).
//
// # Why CodeDeploy's dates are EpochSeconds
//
// CodeDeploy speaks awsJson1_1, where a Timestamp is published as epoch seconds with
// fractional precision rather than an RFC3339 string. API_ApplicationInfo types createTime
// as Timestamp; API_GetApplication renders it `"createTime": number` and its own sample
// response is `"createTime": 1446229001.211`; API_GetDeployment renders
// `"createTime": number` and `"completeTime": number` and samples them as
// `1446232639.487` and `1446232681.319`. A Go time.Time marshals to RFC3339, which is what
// both sites answered — and because an awsJson1_1 timestamp deserializer expects a number,
// that was not a wrong value but a refusal to decode: the whole response failed in a typed
// SDK before a consumer's assertion ran. EpochSeconds (epochseconds.go) marshals to exactly
// three decimals, which is the precision AWS's own samples carry (#1207).

// codedeployAppOut is the application element of GetApplication's response.
//
// Five of API_ApplicationInfo's six members, every one Required: No. linkedToGitHub is always false,
// which is true of every application substrate holds: no GitHub connection is modeled, and AWS's own
// GetApplication sample reports the member as false for an unlinked application rather than omitting
// it. gitHubAccountName is absent for the same reason — there is no connection to name — and it is
// absent rather than present and empty (#1013's rule, #1199).
type codedeployAppOut struct {
	ApplicationID   string       `json:"applicationId"`
	ApplicationName string       `json:"applicationName"`
	ComputePlatform string       `json:"computePlatform,omitempty"`
	CreateTime      EpochSeconds `json:"createTime"`
	LinkedToGitHub  bool         `json:"linkedToGitHub"`
}

// codedeployAppToWire projects a persisted application onto the published shape.
func codedeployAppToWire(app CodeDeployApp) codedeployAppOut {
	return codedeployAppOut{
		ApplicationID:   app.ApplicationID,
		ApplicationName: app.ApplicationName,
		ComputePlatform: app.ComputePlatform,
		CreateTime:      EpochSeconds(app.CreateTime),
	}
}

// codedeployGroupEchoMembers are the members CreateDeploymentGroup takes and API_DeploymentGroupInfo
// answers under the same name and shape, so the group records them as sent and answers them back.
//
// autoScalingGroups is not among them because its shape differs: the request sends names and the
// response answers AutoScalingGroup objects, so it is converted (see [codedeployGroupToWire]).
var codedeployGroupEchoMembers = []string{
	"alarmConfiguration", "autoRollbackConfiguration", "blueGreenDeploymentConfiguration",
	"deploymentStyle", "ec2TagFilters", "ec2TagSet", "ecsServices", "loadBalancerInfo",
	"onPremisesInstanceTagFilters", "onPremisesTagSet", "outdatedInstancesStrategy",
	"terminationHookEnabled", "triggerConfigurations",
}

// codedeployLastDeploymentOut is API_LastDeploymentInfo, the shape of lastAttemptedDeployment and
// lastSuccessfulDeployment.
type codedeployLastDeploymentOut struct {
	CreateTime   EpochSeconds  `json:"createTime"`
	DeploymentID string        `json:"deploymentId"`
	EndTime      *EpochSeconds `json:"endTime,omitempty"`
	Status       string        `json:"status"`
}

// codedeployGroupToWire projects a persisted deployment group onto API_DeploymentGroupInfo.
//
// It can answer all twenty-three of the shape's members. The thirteen in [codedeployGroupEchoMembers]
// appear only when the create sent them, since each is Required: No; lastAttemptedDeployment,
// lastSuccessfulDeployment and targetRevision appear once a deployment has run in the group (#1199).
//
// A map rather than a struct, because thirteen of the members are the caller's own JSON answered
// back verbatim; [encoding/json] sorts the keys, so the rendering is stable for a replay.
//
// attempted and successful are the group's last deployments as observed now, which
// [CodeDeployPlugin.groupLastDeployments] derives; the group's stored references are not read here,
// because they record each deployment's status at creation rather than what it reports (#1400).
func codedeployGroupToWire(group CodeDeployGroup, attempted, successful *CodeDeployDeploymentRef) map[string]any {
	out := map[string]any{
		"applicationName":     group.ApplicationName,
		"deploymentGroupId":   group.DeploymentGroupID,
		"deploymentGroupName": group.DeploymentGroupName,
	}
	if group.ServiceRoleArn != "" {
		out["serviceRoleArn"] = group.ServiceRoleArn
	}
	if group.ComputePlatform != "" {
		out["computePlatform"] = group.ComputePlatform
	}
	if group.DeploymentConfigName != "" {
		out["deploymentConfigName"] = group.DeploymentConfigName
	}
	// API_AutoScalingGroup publishes name, hook and terminationHook. Only the name is known: no hook is
	// installed, because no Auto Scaling group is acted on, so the hook names are absent.
	asgs := make([]map[string]string, 0, len(group.AutoScalingGroups))
	for _, name := range group.AutoScalingGroups {
		asgs = append(asgs, map[string]string{"name": name})
	}
	out["autoScalingGroups"] = asgs
	for _, member := range codedeployGroupEchoMembers {
		if raw, ok := group.Config[member]; ok {
			out[member] = raw
		}
	}
	if attempted != nil {
		out["lastAttemptedDeployment"] = codedeployLastDeploymentToWire(*attempted)
	}
	if successful != nil {
		out["lastSuccessfulDeployment"] = codedeployLastDeploymentToWire(*successful)
	}
	if len(group.TargetRevision) > 0 {
		out["targetRevision"] = group.TargetRevision
	}
	return out
}

// codedeployLastDeploymentToWire projects a deployment reference onto API_LastDeploymentInfo.
//
// endTime is "when the most recent deployment to the deployment group was complete", so a reference
// whose end time is zero — a deployment the group observes as not yet terminal — answers none, as
// GetDeployment's completeTime is withheld for the same deployment.
func codedeployLastDeploymentToWire(ref CodeDeployDeploymentRef) codedeployLastDeploymentOut {
	out := codedeployLastDeploymentOut{
		CreateTime:   EpochSeconds(ref.CreateTime),
		DeploymentID: ref.DeploymentID,
		Status:       ref.Status,
	}
	if !ref.EndTime.IsZero() {
		end := EpochSeconds(ref.EndTime)
		out.EndTime = &end
	}
	return out
}

// codedeployDeploymentEchoMembers are the members CreateDeployment takes and API_DeploymentInfo
// answers under the same name and shape. deploymentStyle and loadBalancerInfo are not request
// members; they are copied from the group at create, which is where DeploymentInfo's come from.
var codedeployDeploymentEchoMembers = []string{
	"autoRollbackConfiguration", "deploymentMode", "deploymentStyle", "description",
	"fileExistsBehavior", "ignoreApplicationStopFailures", "loadBalancerInfo",
	"overrideAlarmConfiguration", "revision", "targetInstances", "updateOutdatedInstancesOnly",
}

// codedeployDeploymentToWire projects a persisted deployment onto API_DeploymentInfo.
//
// Of the shape's thirty-one members, this can answer the twenty-one substrate holds a value for; the
// eleven in [codedeployDeploymentEchoMembers] appear only when sent. The ten others are absent, each for
// a reason:
//
//   - deploymentOverview, deploymentStatusMessages and instanceTerminationWaitTimeStarted describe
//     the targets a deployment ran on, and substrate runs on none, so any count would be invented.
//     A consumer asserting a deployment landed reads status, which a seed can hold non-terminal for
//     a number of observations and end Failed or Stopped (#1196; see codedeploy_progression.go).
//   - errorInformation is answered only for a seeded Failed or Stopped deployment, by
//     [CodeDeployPlugin.observeDeployment]; rollbackInfo describes a rollback, and none happens.
//   - previousRevision, relatedDeployments, blueGreenDeploymentConfiguration and externalId are not
//     modeled; additionalDeploymentStatusInfo is deprecated.
//   - deploymentMode is answered only when the request sent RESTART: the page says the member "is
//     absent … for STANDARD deployments", so a STANDARD one is not echoed (see createDeployment).
func codedeployDeploymentToWire(deployment CodeDeployDeployment) map[string]any {
	out := map[string]any{
		"applicationName":     deployment.ApplicationName,
		"completeTime":        EpochSeconds(deployment.CompleteTime),
		"createTime":          EpochSeconds(deployment.CreateTime),
		"deploymentGroupName": deployment.DeploymentGroupName,
		"deploymentId":        deployment.DeploymentID,
		"status":              deployment.Status,
	}
	if !deployment.StartTime.IsZero() {
		out["startTime"] = EpochSeconds(deployment.StartTime)
	}
	if deployment.Creator != "" {
		out["creator"] = deployment.Creator
	}
	if deployment.ComputePlatform != "" {
		out["computePlatform"] = deployment.ComputePlatform
	}
	if deployment.DeploymentConfigName != "" {
		out["deploymentConfigName"] = deployment.DeploymentConfigName
	}
	for _, member := range codedeployDeploymentEchoMembers {
		if raw, ok := deployment.Config[member]; ok {
			out[member] = raw
		}
	}
	return out
}
