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
// Four of API_ApplicationInfo's six members. gitHubAccountName and linkedToGitHub are not
// modeled and are therefore simply absent from the type rather than present and empty, so
// this reports nothing AWS would not (#1013's rule); every member of the shape is
// Required: No. That gap is #1199's class and is recorded in docs/services.md.
type codedeployAppOut struct {
	ApplicationID   string       `json:"applicationId"`
	ApplicationName string       `json:"applicationName"`
	ComputePlatform string       `json:"computePlatform,omitempty"`
	CreateTime      EpochSeconds `json:"createTime"`
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

// codedeployGroupOut is the deploymentGroupInfo element of GetDeploymentGroup's response.
//
// Four of the twenty-three members API_GetDeploymentGroup publishes, the same #1199 gap.
//
// It carries no date, and the asymmetry with the other two types here is deliberate:
// API_GetDeploymentGroup publishes no top-level timestamp at all. The only dates in the
// shape are nested inside lastAttemptedDeployment and lastSuccessfulDeployment, which
// substrate does not model, so CodeDeployGroup declares no time field and there is nothing
// to convert. Adding one would invent a member rather than project one.
type codedeployGroupOut struct {
	DeploymentGroupID   string `json:"deploymentGroupId"`
	DeploymentGroupName string `json:"deploymentGroupName"`
	ApplicationName     string `json:"applicationName"`
	ServiceRoleArn      string `json:"serviceRoleArn,omitempty"`
}

// codedeployGroupToWire projects a persisted deployment group onto the published shape.
func codedeployGroupToWire(group CodeDeployGroup) codedeployGroupOut {
	return codedeployGroupOut{
		DeploymentGroupID:   group.DeploymentGroupID,
		DeploymentGroupName: group.DeploymentGroupName,
		ApplicationName:     group.ApplicationName,
		ServiceRoleArn:      group.ServiceRoleArn,
	}
}

// codedeployDeploymentOut is the deploymentInfo element of GetDeployment's response.
//
// Six of the thirty-one members API_GetDeployment publishes. startTime and
// deploymentOverview are published and absent here on purpose: reporting either would mean
// inventing a value for work substrate does not run, which is #1196's and #1199's scope
// rather than this type's.
type codedeployDeploymentOut struct {
	DeploymentID        string       `json:"deploymentId"`
	ApplicationName     string       `json:"applicationName"`
	DeploymentGroupName string       `json:"deploymentGroupName"`
	Status              string       `json:"status"`
	CreateTime          EpochSeconds `json:"createTime"`
	CompleteTime        EpochSeconds `json:"completeTime"`
}

// codedeployDeploymentToWire projects a persisted deployment onto the published shape.
func codedeployDeploymentToWire(deployment CodeDeployDeployment) codedeployDeploymentOut {
	return codedeployDeploymentOut{
		DeploymentID:        deployment.DeploymentID,
		ApplicationName:     deployment.ApplicationName,
		DeploymentGroupName: deployment.DeploymentGroupName,
		Status:              deployment.Status,
		CreateTime:          EpochSeconds(deployment.CreateTime),
		CompleteTime:        EpochSeconds(deployment.CompleteTime),
	}
}
