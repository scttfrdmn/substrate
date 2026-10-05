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

// sagemakerAppOut is DescribeApp's response body: seven of its published members, the ones the record
// models (#1013's rule, #1199's gap). CreationTime is absent for an app recorded before #1400, which
// has none.
type sagemakerAppOut struct {
	AppArn          string  `json:"AppArn"`
	AppName         string  `json:"AppName"`
	AppType         string  `json:"AppType"`
	CreationTime    float64 `json:"CreationTime,omitempty"`
	DomainID        string  `json:"DomainId"`
	Status          string  `json:"Status"`
	UserProfileName string  `json:"UserProfileName"`
}

// sagemakerAppToWire projects a persisted app onto DescribeApp's published shape.
func sagemakerAppToWire(app SageMakerApp) sagemakerAppOut {
	return sagemakerAppOut{
		AppArn:          app.AppArn,
		AppName:         app.AppName,
		AppType:         app.AppType,
		CreationTime:    app.CreationTime,
		DomainID:        app.DomainID,
		Status:          app.Status,
		UserProfileName: app.UserProfileName,
	}
}

// sagemakerAppDetailsOut is one element of ListApps' Apps array: six of API_AppDetails' members, and
// no AppArn, which that shape does not publish.
type sagemakerAppDetailsOut struct {
	AppName         string  `json:"AppName"`
	AppType         string  `json:"AppType"`
	CreationTime    float64 `json:"CreationTime,omitempty"`
	DomainID        string  `json:"DomainId"`
	Status          string  `json:"Status"`
	UserProfileName string  `json:"UserProfileName"`
}

// sagemakerAppsToDetails projects a list of persisted apps onto ListApps' element shape, preserving
// its order.
func sagemakerAppsToDetails(apps []SageMakerApp) []sagemakerAppDetailsOut {
	out := make([]sagemakerAppDetailsOut, 0, len(apps))
	for _, app := range apps {
		out = append(out, sagemakerAppDetailsOut{
			AppName:         app.AppName,
			AppType:         app.AppType,
			CreationTime:    app.CreationTime,
			DomainID:        app.DomainID,
			Status:          app.Status,
			UserProfileName: app.UserProfileName,
		})
	}
	return out
}

// sagemakerTrainingJobOut is DescribeTrainingJob's response body: six of its published members, the
// ones the record models. CreationTime is already epoch seconds in the record, which is the published
// form.
type sagemakerTrainingJobOut struct {
	CreationTime      float64 `json:"CreationTime"`
	FailureReason     string  `json:"FailureReason,omitempty"`
	SecondaryStatus   string  `json:"SecondaryStatus"`
	TrainingJobArn    string  `json:"TrainingJobArn"`
	TrainingJobName   string  `json:"TrainingJobName"`
	TrainingJobStatus string  `json:"TrainingJobStatus"`
}

// sagemakerTrainingJobToWire projects a persisted training job onto the published shape. job is the
// observed job, so SecondaryStatus is derived from the status this observation reports rather than
// from the record's.
func sagemakerTrainingJobToWire(job SageMakerTrainingJob) sagemakerTrainingJobOut {
	return sagemakerTrainingJobOut{
		CreationTime:      job.CreationTime,
		FailureReason:     job.FailureReason,
		SecondaryStatus:   sagemakerSecondaryStatus(job.TrainingJobStatus),
		TrainingJobArn:    job.TrainingJobArn,
		TrainingJobName:   job.TrainingJobName,
		TrainingJobStatus: job.TrainingJobStatus,
	}
}

// sagemakerSecondaryStatus is the SecondaryStatus a job reports alongside a TrainingJobStatus.
//
// API_DescribeTrainingJob publishes SecondaryStatus as Required and groups its values under the
// primary status each applies to: InProgress has Starting, Pending, Downloading, Training, Interrupted
// and Uploading; Completed has Completed; Failed has Failed; Stopped has MaxRuntimeExceeded,
// MaxWaitTimeExceeded and Stopped; Stopping has Stopping. The four one-to-one groups answer their own
// name. For InProgress, Training — "Training is in progress" — is substrate's choice among the six:
// the page glosses InProgress itself as "The training is in progress", and the stages around it
// (capacity, downloads, uploads, spot interruption) describe the workload's internals, which substrate
// does not run. Stopped answers Stopped, because substrate models no runtime limit to exceed.
//
// A status outside those groups — none is reachable, since seeds are refused outside the five — is
// answered as itself rather than as an empty string, which the Required member does not permit.
func sagemakerSecondaryStatus(status string) string {
	if status == "InProgress" {
		return "Training"
	}
	return status
}

// sagemakerTrainingJobSummaryOut is one element of ListTrainingJobs' TrainingJobSummaries. The
// summary carries the same observed status pair as the describe, so the two agree on one job.
type sagemakerTrainingJobSummaryOut struct {
	CreationTime      float64 `json:"CreationTime"`
	SecondaryStatus   string  `json:"SecondaryStatus"`
	TrainingJobArn    string  `json:"TrainingJobArn"`
	TrainingJobName   string  `json:"TrainingJobName"`
	TrainingJobStatus string  `json:"TrainingJobStatus"`
}

// sagemakerTrainingJobToSummary projects an observed training job onto the summary shape.
func sagemakerTrainingJobToSummary(job SageMakerTrainingJob) sagemakerTrainingJobSummaryOut {
	return sagemakerTrainingJobSummaryOut{
		CreationTime:      job.CreationTime,
		SecondaryStatus:   sagemakerSecondaryStatus(job.TrainingJobStatus),
		TrainingJobArn:    job.TrainingJobArn,
		TrainingJobName:   job.TrainingJobName,
		TrainingJobStatus: job.TrainingJobStatus,
	}
}
