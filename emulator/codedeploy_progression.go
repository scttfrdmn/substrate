package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

// A CodeDeploy deployment's seeded status progression (#1196).
//
// Every deployment substrate creates is recorded Succeeded, because substrate does not run the
// deployment — installing a revision on instances is resource-internal work CLAUDE.md puts out of
// scope. What a consumer's wait loop observes is GetDeployment's status, and until #1196 that was
// Succeeded on the first read, so a `wait deployment-successful` loop exited before it polled and the
// Failed and Stopped branches it exists for could not be produced at all.
//
// A seed now says how many GetDeployment observations report a non-terminal status before the
// deployment reports its final one. It is the shared progression in emulator/progression.go: keyed by
// deployment ID or "*", counted per deployment, spent only by GetDeployment, and recorded and replayed
// as a control-plane write. An unseeded deployment reads exactly as before — Succeeded — so every
// existing fixture is unchanged.
//
// API_DeploymentInfo publishes eight statuses: Created, Queued, InProgress, Baking, Succeeded, Failed,
// Stopped and Ready. A seed may name any of them as the transient or the final status; the defaults
// are InProgress and the record's own Succeeded. A Failed or Stopped final status may carry an
// errorInformation code and message, the member API_ErrorInformation publishes, with the code held to
// that page's Valid Values. While the status is not terminal, completeTime — "when the deployment was
// complete" — is not answered.
//
// CreateDeployment answers only deploymentId, so there is no initial status for it to report; the
// first status a caller can see is GetDeployment's.

// codedeployDeploymentStatuses is API_DeploymentInfo's status Valid Values.
var codedeployDeploymentStatuses = []string{"Created", "Queued", "InProgress", "Baking", "Succeeded", "Failed", "Stopped", "Ready"}

// codedeployTerminalStatuses are the statuses after which a deployment no longer changes: the ones a
// wait loop exits on, and the only ones a seed's errorInformation may accompany when Failed or Stopped.
var codedeployTerminalStatuses = []string{"Succeeded", "Failed", "Stopped"}

// codedeployErrorCodes is API_ErrorInformation's code Valid Values.
var codedeployErrorCodes = []string{
	"AGENT_ISSUE", "ALARM_ACTIVE", "APPLICATION_MISSING", "AUTOSCALING_VALIDATION_ERROR",
	"AUTO_SCALING_CONFIGURATION", "AUTO_SCALING_IAM_ROLE_PERMISSIONS", "CODEDEPLOY_RESOURCE_CANNOT_BE_FOUND",
	"CUSTOMER_APPLICATION_UNHEALTHY", "DEPLOYMENT_GROUP_MISSING", "ECS_UPDATE_ERROR", "ELASTIC_LOAD_BALANCING_INVALID",
	"ELB_INVALID_INSTANCE", "HEALTH_CONSTRAINTS", "HEALTH_CONSTRAINTS_INVALID", "HOOK_EXECUTION_FAILURE",
	"IAM_ROLE_MISSING", "IAM_ROLE_PERMISSIONS", "INTERNAL_ERROR", "INVALID_ECS_SERVICE",
	"INVALID_LAMBDA_CONFIGURATION", "INVALID_LAMBDA_FUNCTION", "INVALID_REVISION", "MANUAL_STOP",
	"MISSING_BLUE_GREEN_DEPLOYMENT_CONFIGURATION", "MISSING_ELB_INFORMATION", "MISSING_GITHUB_TOKEN",
	"NO_EC2_SUBSCRIPTION", "NO_INSTANCES", "OVER_MAX_INSTANCES", "RESOURCE_LIMIT_EXCEEDED", "REVISION_MISSING",
	"THROTTLED", "TIMEOUT", "CLOUDFORMATION_STACK_FAILURE", "INVALID_EKS_CLUSTER", "KUBERNETES_UPDATE_ERROR",
}

// codedeployDeploymentStatusSeed is the body of POST /v1/codedeploy/deployment-status.
type codedeployDeploymentStatusSeed struct {
	// DeploymentID is the deployment the seed targets; "" or "*" means every deployment.
	DeploymentID string `json:"deploymentId"`
	// PendingObservations is how many GetDeployment observations report State before FinalState.
	PendingObservations int `json:"pendingObservations"`
	// State is the non-terminal status those observations report; InProgress when empty.
	State string `json:"state"`
	// FinalState is the status reported once the countdown is spent; the record's own when empty.
	FinalState string `json:"finalState"`
	// ErrorCode and ErrorMessage are the errorInformation a Failed or Stopped final status reports.
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// codedeployDeploymentProgressions is the deployment kind's seeded countdown.
var codedeployDeploymentProgressions = newProgression[codedeployDeploymentStatusSeed]("codedeploy-deploy-ctrl", "deploymentId", "pendingObservations")

// progressionID implements [progressionSeed].
func (seed codedeployDeploymentStatusSeed) progressionID() string { return seed.DeploymentID }

// progressionObservations implements [progressionSeed].
func (seed codedeployDeploymentStatusSeed) progressionObservations() int {
	return seed.PendingObservations
}

// validateProgression implements [progressionSeed]: both statuses must be published, the final one
// must be terminal, and errorInformation may only accompany a Failed or Stopped final status and must
// name a published code.
func (seed codedeployDeploymentStatusSeed) validateProgression() error {
	if err := progressionStates("deployment", codedeployDeploymentStatuses, seed.State, seed.FinalState); err != nil {
		return err
	}
	if seed.FinalState != "" && !slices.Contains(codedeployTerminalStatuses, seed.FinalState) {
		return fmt.Errorf("finalState %q is not terminal, want one of %v", seed.FinalState, codedeployTerminalStatuses)
	}
	if seed.State != "" && slices.Contains(codedeployTerminalStatuses, seed.State) {
		return fmt.Errorf("state %q is terminal; a transient state must be one of Created, Queued, InProgress, Baking or Ready", seed.State)
	}
	if seed.ErrorCode != "" || seed.ErrorMessage != "" {
		if seed.FinalState != "Failed" && seed.FinalState != "Stopped" {
			return fmt.Errorf("errorCode and errorMessage describe a Failed or Stopped deployment, not %q", seed.FinalState)
		}
		if seed.ErrorCode != "" && !slices.Contains(codedeployErrorCodes, seed.ErrorCode) {
			return fmt.Errorf("unknown errorCode %q, want one of %v", seed.ErrorCode, codedeployErrorCodes)
		}
	}
	return nil
}

// observeDeployment applies the governing seed to one GetDeployment observation, spending it: the
// status, and whether completeTime and errorInformation are answered, follow the countdown.
func (p *CodeDeployPlugin) observeDeployment(deployment CodeDeployDeployment) (map[string]any, error) {
	out := codedeployDeploymentToWire(deployment)
	seed, seen, err := codedeployDeploymentProgressions.observe(context.Background(), p.state, &p.seedMu, deployment.DeploymentID)
	if err != nil {
		return nil, fmt.Errorf("codedeploy observeDeployment: %w", err)
	}
	if seed == nil {
		return out, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "InProgress", deployment.Status)
	out["status"] = status
	if !terminal {
		delete(out, "completeTime")
		return out, nil
	}
	if seed.ErrorCode != "" || seed.ErrorMessage != "" {
		info := map[string]string{}
		if seed.ErrorCode != "" {
			info["code"] = seed.ErrorCode
		}
		if seed.ErrorMessage != "" {
			info["message"] = seed.ErrorMessage
		}
		out["errorInformation"] = info
	}
	return out, nil
}

// handleCodeDeploySeedDeploymentStatus handles POST /v1/codedeploy/deployment-status. Body:
// {"deploymentId","pendingObservations","state","finalState","errorCode","errorMessage"}.
func (s *Server) handleCodeDeploySeedDeploymentStatus(w http.ResponseWriter, r *http.Request) {
	codedeployDeploymentProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleCodeDeployClearDeploymentStatus handles DELETE /v1/codedeploy/deployment-status; with
// ?deploymentId=… it removes that seed, without it every one.
func (s *Server) handleCodeDeployClearDeploymentStatus(w http.ResponseWriter, r *http.Request) {
	codedeployDeploymentProgressions.serveClear(w, r, s.state, s.logger)
}
