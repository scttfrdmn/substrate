package emulator

// ECS answered its persisted records straight to the caller. The types below are the
// published shapes, projected from those records, on the pattern emulator/ecr_wire.go
// established (#1090) and emulator/efs_wire.go followed (#1304).
//
// All four ECS records — ECSCluster, ECSTaskDefinition, ECSService and ECSTask
// (ecs_types.go) — were handed to ecsJSONResponse directly at every one of the thirteen
// sites that answer a resource, so every one of those responses carried AccountID and
// Region unconditionally (neither has omitempty) and `ever_tagged` from whichever record
// had been tagged. API_Cluster, API_TaskDefinition, API_Service and API_Task publish none
// of the three. Twelve baseline lines, the largest single block left on #756.
//
// ECS is the cheapest of the remaining services to project and the one with the most to
// gain, because none of its four timestamps needs converting: RegisteredAt and CreatedAt
// are already EpochSeconds in the record, which is what all four operations' JSON protocol
// publishes. That is the divergence #1305 tracks on ACM, Redshift Data and Firehose, and
// ECS is the counter-example those three are measured against.
//
// # Two members AWS does not publish on the shape substrate put them on
//
// Projecting is where a response's membership is decided, so a member that is not on the
// published shape cannot be carried across into a type whose whole purpose is to hold only
// published members. Two had to be resolved rather than copied:
//
//   - ECSService.ClusterName, rendered `clusterName`. API_Service publishes clusterArn and
//     not clusterName; the full member list has no such entry. The field stays on the
//     record — resolveClusterName derives it from the request and the state key is built
//     from it — and is absent from ecsServiceOut. A caller wanting the name has the ARN,
//     whose last segment is it, which is how the real API expects to be read.
//
//   - ECSTaskDefinition.Tags, rendered `tags` inside the taskDefinition object.
//     API_TaskDefinition has no tags member at all. Task-definition tags are published one
//     level up: RegisterTaskDefinition and DescribeTaskDefinition each publish `tags` as a
//     **top-level response element, a sibling of taskDefinition**, and
//     DeregisterTaskDefinition publishes no tags at all. So the member is not dropped, it
//     moves — see ecsTaskDefinitionTagsOrNil and the two call sites. A consumer reading
//     `taskDefinition.tags` off a substrate response was reading a member the SDK's own
//     decoder discards, because the shape it decodes into has no field for it.
//
// DescribeTaskDefinition publishes those tags only when the request asks for them:
// `include` is "Determines whether to see the resource tags for the task definition. If
// TAGS is specified, the tags are included in the response. If this field is omitted, tags
// aren't included in the response." Substrate read no `include` before, because the tags
// were nested where the caller got them unconditionally. Lifting them to the top level
// without the gate would answer tags where AWS answers none, so describeTaskDefinition now
// reads `include` too. RegisterTaskDefinition has no include parameter and always publishes
// the tags it was handed.
//
// DescribeClusters and DescribeServices carry the same include: ["TAGS"] gate and substrate
// does not honor it either. Those two are left alone deliberately: Cluster.tags and
// Service.tags *are* published members of their shapes, so substrate reporting them
// unconditionally is an over-report of a real member rather than a member AWS does not
// have, and changing it is a behavior change this projection does not need. #756 is the
// bookkeeping leak; that is its own defect.
//
// # What stays on the records
//
// AccountID, Region and EverTagged stay in ecs_types.go, untouched. The state keys already
// scope by account and Region so nothing reads those two back, but a persisted member is
// not free to remove: ecr_wire.go's rule is that the record's encoding is what
// MemoryStateManager snapshots and a replay reads, so a projection changes the response and
// leaves the record alone. EverTagged is read across a service boundary besides — the
// TaggingPlugin's ecsNamespace arm writes it and its scans report it. The twelve baseline
// lines therefore stay in scripts/wire-bookkeeping-baseline.txt and are discharged in
// scripts/wire-bookkeeping-projected.txt instead.
//
// ECSService.CreatedAt is the one field of the twelve that is already excluded from the
// baseline, because API_Service publishes `createdAt` and the exclusion is per field rather
// than per struct (see scripts/check-wire-bookkeeping.sh's header). It is projected here
// like any other published member.

// ecsClusterOut is the Cluster of CreateCluster, DescribeClusters (under `clusters`) and
// DeleteCluster.
//
// Member names follow API_Cluster, on which every member is Required: No. The members
// substrate does not model — activeServicesCount, attachments, attachmentsStatus,
// capacityProviders, configuration, defaultCapacityProviderStrategy, pendingTasksCount,
// registeredContainerInstancesCount, runningTasksCount, serviceConnectDefaults, settings
// and statistics — are absent from the type rather than present and zero. A count reported
// as 0 reads as a measurement of an empty cluster; substrate has not measured anything, and
// #1013's rule is to report nothing AWS would not.
type ecsClusterOut struct {
	ClusterArn  string   `json:"clusterArn"`
	ClusterName string   `json:"clusterName"`
	Status      string   `json:"status"`
	Tags        []ECSTag `json:"tags,omitempty"`
}

// ecsClusterToWire projects a persisted cluster onto the published shape.
func ecsClusterToWire(c ECSCluster) ecsClusterOut {
	return ecsClusterOut{
		ClusterArn:  c.ClusterArn,
		ClusterName: c.ClusterName,
		Status:      c.Status,
		Tags:        c.Tags,
	}
}

// ecsClustersToWire projects a slice of persisted clusters, for DescribeClusters.
func ecsClustersToWire(clusters []ECSCluster) []ecsClusterOut {
	out := make([]ecsClusterOut, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, ecsClusterToWire(c))
	}
	return out
}

// ecsTaskDefinitionOut is the TaskDefinition of RegisterTaskDefinition,
// DescribeTaskDefinition and DeregisterTaskDefinition.
//
// Member names follow API_TaskDefinition, on which every member is Required: No. There is
// deliberately no tags member: API_TaskDefinition has none, and the header above records
// where task-definition tags are published instead.
//
// The members substrate does not model — compatibilities, deleteRequestedAt,
// deregisteredAt, enableFaultInjection, ephemeralStorage, inferenceAccelerators, ipcMode,
// pidMode, placementConstraints, proxyConfiguration, registeredBy, requiresAttributes,
// runtimePlatform and volumes — are absent rather than present and empty. compatibilities
// is the one worth naming: it is the launch types ECS validated the definition against,
// which substrate performs no validation to determine, so reporting the caller's
// requiresCompatibilities back under that name would be a claim substrate cannot make.
type ecsTaskDefinitionOut struct {
	TaskDefinitionArn       string        `json:"taskDefinitionArn"`
	Family                  string        `json:"family"`
	Revision                int           `json:"revision"`
	Status                  string        `json:"status"`
	ContainerDefinitions    []interface{} `json:"containerDefinitions,omitempty"`
	NetworkMode             string        `json:"networkMode,omitempty"`
	RequiresCompatibilities []string      `json:"requiresCompatibilities,omitempty"`
	CPU                     string        `json:"cpu,omitempty"`
	Memory                  string        `json:"memory,omitempty"`
	ExecutionRoleArn        string        `json:"executionRoleArn,omitempty"`
	TaskRoleArn             string        `json:"taskRoleArn,omitempty"`
	RegisteredAt            EpochSeconds  `json:"registeredAt"`
}

// ecsTaskDefinitionToWire projects a persisted task definition onto the published shape.
// The record's Tags are not carried: see ecsTaskDefinitionTagsOrNil.
func ecsTaskDefinitionToWire(td ECSTaskDefinition) ecsTaskDefinitionOut {
	return ecsTaskDefinitionOut{
		TaskDefinitionArn:       td.TaskDefinitionArn,
		Family:                  td.Family,
		Revision:                td.Revision,
		Status:                  td.Status,
		ContainerDefinitions:    td.ContainerDefinitions,
		NetworkMode:             td.NetworkMode,
		RequiresCompatibilities: td.RequiresCompatibilities,
		CPU:                     td.CPU,
		Memory:                  td.Memory,
		ExecutionRoleArn:        td.ExecutionRoleArn,
		TaskRoleArn:             td.TaskRoleArn,
		RegisteredAt:            td.RegisteredAt,
	}
}

// ecsTaskDefinitionTagsOrNil returns the task definition's tags for the top-level `tags`
// response element of RegisterTaskDefinition and DescribeTaskDefinition, or nil so the
// member is omitted.
//
// A nil result and an empty-slice result differ on the wire and the difference is the point:
// `tags` is Required: No on both operations, so a definition registered without tags must
// answer no tags member at all rather than `[]`. That is the opposite of
// efsTagsOrEmpty, where API_FileSystemDescription marks Tags Required: Yes and an empty
// file system must answer `[]`; the two helpers look contradictory and are each what their
// own reference states.
func ecsTaskDefinitionTagsOrNil(tags []ECSTag) []ECSTag {
	if len(tags) == 0 {
		return nil
	}
	return tags
}

// ecsServiceOut is the Service of CreateService, UpdateService, DescribeServices (under
// `services`) and DeleteService.
//
// Member names follow API_Service, on which every member is Required: No. There is
// deliberately no clusterName member; the header above records why.
//
// pendingCount is absent rather than 0 for the reason ecsClusterOut's counts are: substrate
// starts a task RUNNING and never reports one PENDING, so a 0 would be a measurement it did
// not take. The rest of API_Service that substrate does not model — availabilityZoneRebalancing,
// capacityProviderStrategy, createdBy, currentServiceDeployment, currentServiceRevisions,
// deploymentConfiguration, deploymentController, deployments, enableECSManagedTags,
// enableExecuteCommand, events, healthCheckGracePeriodSeconds, loadBalancers,
// networkConfiguration, placementConstraints, placementStrategy, platformFamily,
// platformVersion, propagateTags, resourceManagementType, roleArn, schedulingStrategy,
// serviceRegistries and taskSets — is absent on the same rule.
type ecsServiceOut struct {
	ServiceArn     string       `json:"serviceArn"`
	ServiceName    string       `json:"serviceName"`
	ClusterArn     string       `json:"clusterArn"`
	TaskDefinition string       `json:"taskDefinition"`
	DesiredCount   int          `json:"desiredCount"`
	RunningCount   int          `json:"runningCount"`
	Status         string       `json:"status"`
	LaunchType     string       `json:"launchType,omitempty"`
	Tags           []ECSTag     `json:"tags,omitempty"`
	CreatedAt      EpochSeconds `json:"createdAt"`
}

// ecsServiceToWire projects a persisted service onto the published shape.
func ecsServiceToWire(s ECSService) ecsServiceOut {
	return ecsServiceOut{
		ServiceArn:     s.ServiceArn,
		ServiceName:    s.ServiceName,
		ClusterArn:     s.ClusterArn,
		TaskDefinition: s.TaskDefinition,
		DesiredCount:   s.DesiredCount,
		RunningCount:   s.RunningCount,
		Status:         s.Status,
		LaunchType:     s.LaunchType,
		Tags:           s.Tags,
		CreatedAt:      s.CreatedAt,
	}
}

// ecsServicesToWire projects a slice of persisted services, for DescribeServices.
func ecsServicesToWire(services []ECSService) []ecsServiceOut {
	out := make([]ecsServiceOut, 0, len(services))
	for _, s := range services {
		out = append(out, ecsServiceToWire(s))
	}
	return out
}

// ecsTaskOut is the Task of RunTask and DescribeTasks (under `tasks`) and StopTask (under
// `task`).
//
// Member names follow API_Task, on which every member is Required: No.
//
// StartedAt and StoppedAt are pointers where the record's are plain EpochSeconds. The
// record's `startedAt,omitempty` never omitted anything: omitempty has no effect on a
// struct type, and EpochSeconds.MarshalJSON renders the zero time as JSON null, so a task
// that had not stopped reported `"stoppedAt":null` — a member AWS omits. A pointer is what
// makes the omission work, and it is available here because the wire type is new and has no
// recorded encoding to preserve; ecr_wire.go's rule forbids retyping the *record's* field,
// not the projection's.
//
// The members substrate does not model — attachments, attributes, availabilityZone,
// capacityProviderName, connectivity, connectivityAt, containerInstanceArn, containers,
// cpu, createdAt, enableExecuteCommand, ephemeralStorage, executionStoppedAt,
// fargateEphemeralStorage, group, healthStatus, memory, overrides, platformFamily,
// platformVersion, pullStartedAt, pullStoppedAt, startedBy, stopCode, stoppingAt and
// version — are absent rather than present and empty. containers is the one a consumer is
// most likely to look for, and its absence is the scope boundary rather than an oversight:
// substrate does not run the workload, so it has no container to describe.
type ecsTaskOut struct {
	TaskArn           string        `json:"taskArn"`
	TaskDefinitionArn string        `json:"taskDefinitionArn"`
	ClusterArn        string        `json:"clusterArn"`
	LastStatus        string        `json:"lastStatus"`
	DesiredStatus     string        `json:"desiredStatus"`
	LaunchType        string        `json:"launchType,omitempty"`
	StartedAt         *EpochSeconds `json:"startedAt,omitempty"`
	StoppedAt         *EpochSeconds `json:"stoppedAt,omitempty"`
	StoppedReason     string        `json:"stoppedReason,omitempty"`
	Tags              []ECSTag      `json:"tags,omitempty"`
}

// ecsTaskToWire projects a persisted task onto the published shape.
func ecsTaskToWire(t ECSTask) ecsTaskOut {
	return ecsTaskOut{
		TaskArn:           t.TaskArn,
		TaskDefinitionArn: t.TaskDefinitionArn,
		ClusterArn:        t.ClusterArn,
		LastStatus:        t.LastStatus,
		DesiredStatus:     t.DesiredStatus,
		LaunchType:        t.LaunchType,
		StartedAt:         ecsTimeOrNil(t.StartedAt),
		StoppedAt:         ecsTimeOrNil(t.StoppedAt),
		StoppedReason:     t.StoppedReason,
		Tags:              t.Tags,
	}
}

// ecsTasksToWire projects a slice of persisted tasks, for RunTask and DescribeTasks.
func ecsTasksToWire(tasks []ECSTask) []ecsTaskOut {
	out := make([]ecsTaskOut, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, ecsTaskToWire(t))
	}
	return out
}

// ecsTimeOrNil returns nil for the zero time, so an unset optional timestamp is omitted
// from the body rather than reported as JSON null.
func ecsTimeOrNil(t EpochSeconds) *EpochSeconds {
	if t.IsZero() {
		return nil
	}
	return &t
}
