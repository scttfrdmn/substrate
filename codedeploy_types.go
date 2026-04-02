package substrate

import (
	"crypto/rand"
	"fmt"
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
	// AccountID is the AWS account that owns this deployment.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the deployment ran.
	Region string `json:"region"`
}

// generateCodeDeployDeploymentID generates a deployment ID in the form d-XXXXXXXXX
// using 9 random uppercase alphanumeric characters, matching the real CodeDeploy format.
func generateCodeDeployDeploymentID() string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 9)
	rnd := make([]byte, 9)
	_, _ = rand.Read(rnd)
	for i := range b {
		b[i] = chars[int(rnd[i])%len(chars)]
	}
	return fmt.Sprintf("d-%s", b)
}
