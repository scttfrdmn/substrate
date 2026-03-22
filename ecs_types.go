package substrate

// No imports needed — EpochSeconds is defined in epochseconds.go in this package.

// ecsNamespace is the state namespace for ECS.
const ecsNamespace = "ecs"

// ECSCluster holds persisted state for an ECS cluster.
type ECSCluster struct {
	ClusterArn  string   `json:"clusterArn"`
	ClusterName string   `json:"clusterName"`
	Status      string   `json:"status"` // ACTIVE, INACTIVE
	Tags        []ECSTag `json:"tags,omitempty"`
	AccountID   string   `json:"AccountID"`
	Region      string   `json:"Region"`
}

// ECSTag is a key-value tag for ECS resources.
type ECSTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ECSTaskDefinition holds persisted state for an ECS task definition.
type ECSTaskDefinition struct {
	TaskDefinitionArn       string        `json:"taskDefinitionArn"`
	Family                  string        `json:"family"`
	Revision                int           `json:"revision"`
	Status                  string        `json:"status"` // ACTIVE, INACTIVE
	ContainerDefinitions    []interface{} `json:"containerDefinitions,omitempty"`
	NetworkMode             string        `json:"networkMode,omitempty"`
	RequiresCompatibilities []string      `json:"requiresCompatibilities,omitempty"`
	CPU                     string        `json:"cpu,omitempty"`
	Memory                  string        `json:"memory,omitempty"`
	ExecutionRoleArn        string        `json:"executionRoleArn,omitempty"`
	TaskRoleArn             string        `json:"taskRoleArn,omitempty"`
	Tags                    []ECSTag      `json:"tags,omitempty"`
	RegisteredAt            EpochSeconds  `json:"registeredAt"`
	AccountID               string        `json:"AccountID"`
	Region                  string        `json:"Region"`
}

// ECSService holds persisted state for an ECS service.
type ECSService struct {
	ServiceArn     string    `json:"serviceArn"`
	ServiceName    string    `json:"serviceName"`
	ClusterArn     string    `json:"clusterArn"`
	TaskDefinition string    `json:"taskDefinition"` // ARN
	DesiredCount   int       `json:"desiredCount"`
	RunningCount   int       `json:"runningCount"`
	Status         string    `json:"status"`               // ACTIVE, DRAINING, INACTIVE
	LaunchType     string    `json:"launchType,omitempty"` // FARGATE, EC2
	Tags           []ECSTag  `json:"tags,omitempty"`
	CreatedAt      EpochSeconds `json:"createdAt"`
	ClusterName    string    `json:"clusterName"`
	AccountID      string    `json:"AccountID"`
	Region         string    `json:"Region"`
}

// ECSTask holds persisted state for an ECS task.
type ECSTask struct {
	TaskArn           string    `json:"taskArn"`
	TaskDefinitionArn string    `json:"taskDefinitionArn"`
	ClusterArn        string    `json:"clusterArn"`
	LastStatus        string    `json:"lastStatus"` // PROVISIONING, RUNNING, STOPPED
	DesiredStatus     string    `json:"desiredStatus"`
	LaunchType        string    `json:"launchType,omitempty"`
	StartedAt         EpochSeconds `json:"startedAt,omitempty"`
	StoppedAt         EpochSeconds `json:"stoppedAt,omitempty"`
	StoppedReason     string    `json:"stoppedReason,omitempty"`
	Tags              []ECSTag  `json:"tags,omitempty"`
	AccountID         string    `json:"AccountID"`
	Region            string    `json:"Region"`
}
