package emulator

import "fmt"

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// BatchJob (batch_plugin.go) is a persisted state record, and describeJobs handed it straight to the
// caller in the `jobs` list. Two of its fields are substrate's own: AccountID and Region, which scope
// the state key. Neither is a member of API_JobDetail (#756).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back.

// batchJobDetailOut is one job as DescribeJobs answers it.
//
// Eight of API_JobDetail's members: the seven the record models, plus jobArn, which the record's
// account, Region and ID determine and which SubmitJob already answers in the same form. createdAt is
// the record's own value, epoch milliseconds, which is the published Long. The rest (startedAt,
// stoppedAt, container, attempts and the others) are absent rather than present and empty (#1013's
// rule, #1199's gap).
type batchJobDetailOut struct {
	CreatedAt     int64  `json:"createdAt"`
	JobArn        string `json:"jobArn"`
	JobDefinition string `json:"jobDefinition"`
	JobID         string `json:"jobId"`
	JobName       string `json:"jobName"`
	JobQueue      string `json:"jobQueue"`
	Status        string `json:"status"`
	StatusReason  string `json:"statusReason,omitempty"`
}

// batchJobToWire projects a persisted job onto the published shape.
func batchJobToWire(j BatchJob) batchJobDetailOut {
	return batchJobDetailOut{
		CreatedAt:     j.CreatedAt,
		JobArn:        fmt.Sprintf("arn:aws:batch:%s:%s:job/%s", j.Region, j.AccountID, j.JobID),
		JobDefinition: j.JobDefinition,
		JobID:         j.JobID,
		JobName:       j.JobName,
		JobQueue:      j.JobQueue,
		Status:        j.Status,
		StatusReason:  j.StatusReason,
	}
}
