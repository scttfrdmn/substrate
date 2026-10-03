package emulator

// The wire is a different thing from the state, and the types below exist to keep them apart.
//
// SageMakerApp and SageMakerTrainingJob (sagemaker_plugin.go) are persisted state records, and three
// sites handed them straight to the caller: describeApp and describeTrainingJob answered the record as
// the whole response body, and listApps answered `Apps` as a list of records. Two fields of each are
// substrate's own, AccountID and Region, and no SageMaker shape publishes either (#756). ListApps also
// answered AppArn in every element, and API_AppDetails — the shape of that element — publishes no
// AppArn: DescribeApp does, so the two need different types.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back. Projecting leaves the
// stored bytes unchanged, so a run recorded before this replays identically.

// sagemakerAppOut is DescribeApp's response body: six of its published members, the ones the record
// models (#1013's rule, #1199's gap).
type sagemakerAppOut struct {
	AppArn          string `json:"AppArn"`
	AppName         string `json:"AppName"`
	AppType         string `json:"AppType"`
	DomainID        string `json:"DomainId"`
	Status          string `json:"Status"`
	UserProfileName string `json:"UserProfileName"`
}

// sagemakerAppToWire projects a persisted app onto DescribeApp's published shape.
func sagemakerAppToWire(app SageMakerApp) sagemakerAppOut {
	return sagemakerAppOut{
		AppArn:          app.AppArn,
		AppName:         app.AppName,
		AppType:         app.AppType,
		DomainID:        app.DomainID,
		Status:          app.Status,
		UserProfileName: app.UserProfileName,
	}
}

// sagemakerAppDetailsOut is one element of ListApps' Apps array: five of API_AppDetails' members, and
// no AppArn, which that shape does not publish.
type sagemakerAppDetailsOut struct {
	AppName         string `json:"AppName"`
	AppType         string `json:"AppType"`
	DomainID        string `json:"DomainId"`
	Status          string `json:"Status"`
	UserProfileName string `json:"UserProfileName"`
}

// sagemakerAppsToDetails projects a list of persisted apps onto ListApps' element shape, preserving
// its order.
func sagemakerAppsToDetails(apps []SageMakerApp) []sagemakerAppDetailsOut {
	out := make([]sagemakerAppDetailsOut, 0, len(apps))
	for _, app := range apps {
		out = append(out, sagemakerAppDetailsOut{
			AppName:         app.AppName,
			AppType:         app.AppType,
			DomainID:        app.DomainID,
			Status:          app.Status,
			UserProfileName: app.UserProfileName,
		})
	}
	return out
}

// sagemakerTrainingJobOut is DescribeTrainingJob's response body: five of its published members, the
// ones the record models. CreationTime is already epoch seconds in the record, which is the published
// form.
type sagemakerTrainingJobOut struct {
	CreationTime      float64 `json:"CreationTime"`
	FailureReason     string  `json:"FailureReason,omitempty"`
	TrainingJobArn    string  `json:"TrainingJobArn"`
	TrainingJobName   string  `json:"TrainingJobName"`
	TrainingJobStatus string  `json:"TrainingJobStatus"`
}

// sagemakerTrainingJobToWire projects a persisted training job onto the published shape.
func sagemakerTrainingJobToWire(job SageMakerTrainingJob) sagemakerTrainingJobOut {
	return sagemakerTrainingJobOut{
		CreationTime:      job.CreationTime,
		FailureReason:     job.FailureReason,
		TrainingJobArn:    job.TrainingJobArn,
		TrainingJobName:   job.TrainingJobName,
		TrainingJobStatus: job.TrainingJobStatus,
	}
}
