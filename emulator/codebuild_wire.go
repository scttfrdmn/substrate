package emulator

// The wire is a different thing from the state, and the two types below exist to keep them
// apart.
//
// CodeBuildProject and CodeBuildBuild (codebuild_types.go) are persisted state records, and each
// was handed straight to the caller: createProject and updateProject under `project`,
// batchGetProjects under `projects`, startBuild under `build`, batchGetBuilds under `builds`. Two
// fields of each are substrate's own, accountID and region, and no CodeBuild shape publishes
// either (#756).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. For the same
// reason the dates stay time.Time in the records and are converted on projection rather than being
// retyped in place.
//
// # Why CodeBuild's dates are EpochSeconds
//
// CodeBuild speaks awsJson1_1, where a Timestamp is published as epoch seconds with fractional
// precision: API_BatchGetProjects renders `"created": number` and `"lastModified": number`, and
// API_BatchGetBuilds renders `"startTime": number` and `"endTime": number`. A Go time.Time marshals
// to RFC3339, which every one of these sites answered, and an awsJson1_1 timestamp deserializer
// expects a number, so a typed SDK failed to decode the whole response (#1338).

// codebuildProjectOut is one element of a project response.
//
// Nine of API_Project's members — the ones the record models. The rest are absent rather than
// present and empty (#1013's rule, #1199's gap).
type codebuildProjectOut struct {
	ARN          string                 `json:"arn"`
	Artifacts    map[string]interface{} `json:"artifacts,omitempty"`
	Created      EpochSeconds           `json:"created"`
	Description  string                 `json:"description,omitempty"`
	Environment  map[string]interface{} `json:"environment,omitempty"`
	LastModified EpochSeconds           `json:"lastModified"`
	Name         string                 `json:"name"`
	ServiceRole  string                 `json:"serviceRole,omitempty"`
	Source       map[string]interface{} `json:"source,omitempty"`
}

// codebuildProjectToWire projects a persisted project onto the published shape.
func codebuildProjectToWire(project CodeBuildProject) codebuildProjectOut {
	return codebuildProjectOut{
		ARN:          project.ARN,
		Artifacts:    project.Artifacts,
		Created:      EpochSeconds(project.Created),
		Description:  project.Description,
		Environment:  project.Environment,
		LastModified: EpochSeconds(project.LastModified),
		Name:         project.Name,
		ServiceRole:  project.ServiceRole,
		Source:       project.Source,
	}
}

// codebuildProjectsToWire projects a list of persisted projects, preserving its order.
func codebuildProjectsToWire(projects []CodeBuildProject) []codebuildProjectOut {
	out := make([]codebuildProjectOut, 0, len(projects))
	for _, project := range projects {
		out = append(out, codebuildProjectToWire(project))
	}
	return out
}

// codebuildBuildOut is one element of a build response.
//
// Seven of API_Build's members — the ones the record models.
type codebuildBuildOut struct {
	ARN          string       `json:"arn"`
	BuildStatus  string       `json:"buildStatus"`
	CurrentPhase string       `json:"currentPhase"`
	EndTime      EpochSeconds `json:"endTime"`
	ID           string       `json:"id"`
	ProjectName  string       `json:"projectName"`
	StartTime    EpochSeconds `json:"startTime"`
}

// codebuildBuildToWire projects a persisted build onto the published shape.
func codebuildBuildToWire(build CodeBuildBuild) codebuildBuildOut {
	return codebuildBuildOut{
		ARN:          build.ARN,
		BuildStatus:  build.BuildStatus,
		CurrentPhase: build.CurrentPhase,
		EndTime:      EpochSeconds(build.EndTime),
		ID:           build.ID,
		ProjectName:  build.ProjectName,
		StartTime:    EpochSeconds(build.StartTime),
	}
}

// codebuildBuildsToWire projects a list of persisted builds, preserving its order.
func codebuildBuildsToWire(builds []CodeBuildBuild) []codebuildBuildOut {
	out := make([]codebuildBuildOut, 0, len(builds))
	for _, build := range builds {
		out = append(out, codebuildBuildToWire(build))
	}
	return out
}
