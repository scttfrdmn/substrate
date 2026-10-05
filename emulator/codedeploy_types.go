package emulator

import (
	"encoding/json"
	"time"
)

// codedeployNamespace is the state namespace for AWS CodeDeploy resources.
const codedeployNamespace = "codedeploy"

// CodeDeployApp represents an AWS CodeDeploy application.
type CodeDeployApp struct {
	// ApplicationID is the unique identifier for the application.
	ApplicationID string `json:"applicationId"`
	// ApplicationName is the name of the application.
	ApplicationName string `json:"applicationName"`
	// ComputePlatform is the destination platform (Server, Lambda, or ECS).
	ComputePlatform string `json:"computePlatform,omitempty"`
	// CreateTime is when the application was created.
	CreateTime time.Time `json:"createTime"`
	// AccountID is the AWS account that owns this application.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the application exists.
	Region string `json:"region"`
}

// CodeDeployGroup represents an AWS CodeDeploy deployment group.
type CodeDeployGroup struct {
	// DeploymentGroupID is the unique identifier for the deployment group.
	DeploymentGroupID string `json:"deploymentGroupId"`
	// DeploymentGroupName is the name of the deployment group.
	DeploymentGroupName string `json:"deploymentGroupName"`
	// ApplicationName is the application this group belongs to.
	ApplicationName string `json:"applicationName"`
	// ServiceRoleArn is the IAM role ARN used for deployments.
	ServiceRoleArn string `json:"serviceRoleArn,omitempty"`
	// ComputePlatform is the owning application's platform, which DeploymentGroupInfo reports.
	ComputePlatform string `json:"computePlatform,omitempty"`
	// DeploymentConfigName is the group's deployment configuration, the published default when the
	// create named none.
	DeploymentConfigName string `json:"deploymentConfigName,omitempty"`
	// AutoScalingGroups are the Auto Scaling group names the create named.
	AutoScalingGroups []string `json:"autoScalingGroups,omitempty"`
	// Config holds the published members CreateDeploymentGroup takes and DeploymentGroupInfo answers
	// back unchanged, keyed by their published names. They are recorded intent: substrate targets no
	// instance and installs no hook, so it stores what was sent rather than modeling each one.
	Config map[string]json.RawMessage `json:"config,omitempty"`
	// LastAttemptedDeployment is the most recent deployment created in the group.
	LastAttemptedDeployment *CodeDeployDeploymentRef `json:"lastAttemptedDeployment,omitempty"`
	// LastSuccessfulDeployment is the most recent deployment in the group that succeeded.
	LastSuccessfulDeployment *CodeDeployDeploymentRef `json:"lastSuccessfulDeployment,omitempty"`
	// Deployments are the group's deployments in creation order, each with its status at creation.
	// GetDeploymentGroup observes them to answer lastAttemptedDeployment and lastSuccessfulDeployment
	// (#1400); a group recorded before that has none and answers the two stored references instead.
	Deployments []CodeDeployDeploymentRef `json:"deployments,omitempty"`
	// TargetRevision is the revision of the group's last successful deployment.
	TargetRevision json.RawMessage `json:"targetRevision,omitempty"`
	// AccountID is the AWS account that owns this deployment group.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the deployment group exists.
	Region string `json:"region"`
}

// CodeDeployDeployment represents an AWS CodeDeploy deployment.
type CodeDeployDeployment struct {
	// DeploymentID is the unique identifier (format: d-XXXXXXXXX).
	DeploymentID string `json:"deploymentId"`
	// ApplicationName is the application being deployed.
	ApplicationName string `json:"applicationName"`
	// DeploymentGroupName is the deployment group receiving the deployment.
	DeploymentGroupName string `json:"deploymentGroupName"`
	// Status is the deployment status.
	Status string `json:"status"` // Succeeded
	// CreateTime is when the deployment was initiated.
	CreateTime time.Time `json:"createTime"`
	// CompleteTime is when the deployment completed.
	CompleteTime time.Time `json:"completeTime"`
	// StartTime is when the deployment was deployed to the group. Zero on a record written before
	// it was modeled.
	StartTime time.Time `json:"startTime,omitzero"`
	// Creator is how the deployment was created; every deployment substrate creates is a caller's.
	Creator string `json:"creator,omitempty"`
	// ComputePlatform is the application's platform.
	ComputePlatform string `json:"computePlatform,omitempty"`
	// DeploymentConfigName is the configuration the deployment ran under: the request's, else the
	// group's, else the published default.
	DeploymentConfigName string `json:"deploymentConfigName,omitempty"`
	// Config holds the published members CreateDeployment takes and DeploymentInfo answers back
	// unchanged, keyed by their published names, plus the group's deploymentStyle and
	// loadBalancerInfo, which DeploymentInfo also reports.
	Config map[string]json.RawMessage `json:"config,omitempty"`
	// AccountID is the AWS account that owns this deployment.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the deployment ran.
	Region string `json:"region"`
}

// codedeployDeploymentIDChars is the alphabet a `d-` deployment ID is rendered in: uppercase
// letters and digits, which is what AWS's own sample responses show and the narrowest alphabet
// consistent with them. See [generateCodeDeployDeploymentID] for why the alphabet is observed
// rather than published.
const codedeployDeploymentIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// generateCodeDeployDeploymentID mints a deployment ID in the form `d-XXXXXXXXX` from m, derived
// from the request id so a replayed CreateDeployment returns the ID the recording returned (#856).
//
// This is the one identifier in the CI/CD family that is *addressed* rather than reported:
// `GetDeployment`, `StopDeployment` and `ListDeploymentTargets` all take `deploymentId`, and it is
// the only handle CreateDeployment hands back. A re-minted one made the recorded `GetDeployment`
// answer `DeploymentDoesNotExistException`, so a consumer's wait-for-`Succeeded` loop failed on a
// deployment the replay had just created.
//
// The shape has *observed* provenance, not published: `CreateDeployment` gives `deploymentId` as
// String with no pattern and no length constraints, but the page's own sample response is
// `{"deploymentId": "d-IIMHK0NHC"}` — the `d-` prefix, nine characters, uppercase alphanumeric.
// #671 forbids inventing a bound from a sibling operation; it does not forbid reading AWS's own
// example, and that example is all substrate has here. The rendering is therefore unchanged from
// the crypto/rand form: [IDMint.Chars] over the same alphabet, which reproduces its modulo
// mapping byte for byte.
func generateCodeDeployDeploymentID(m *IDMint) string {
	return "d-" + m.Chars(9, codedeployDeploymentIDChars)
}

// CodeDeployDeploymentRef is a deployment as DeploymentGroupInfo's lastAttemptedDeployment and
// lastSuccessfulDeployment report it (API_LastDeploymentInfo).
type CodeDeployDeploymentRef struct {
	// DeploymentID is the deployment's ID.
	DeploymentID string `json:"deploymentId"`
	// Status is the deployment's status.
	Status string `json:"status"`
	// CreateTime is when the deployment was created.
	CreateTime time.Time `json:"createTime"`
	// EndTime is when the deployment ended.
	EndTime time.Time `json:"endTime"`
}
