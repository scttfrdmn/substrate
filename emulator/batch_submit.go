package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// What SubmitJob checks before it accepts a job, and what deleting a queue does to its jobs (#1422).
//
// API_SubmitJob publishes jobName, jobQueue and jobDefinition as Required: Yes and two errors,
// ClientException/400 — "Another cause is specifying an identifier that's not valid" — and
// ServerException/500. Substrate answered a missing jobName with MissingParameter, a code Batch
// publishes nowhere, and checked neither the queue nor the definition, so a job could be submitted to
// a queue that was disabled, deleted or never created. Now, in order:
//
//   - **A missing jobName, jobQueue or jobDefinition** is ClientException, before any state is read.
//   - **The queue** is resolved by name or ARN ("You can specify either the name or the Amazon Resource
//     Name (ARN) of the queue") and must exist, be ENABLED and be VALID, or the job is refused with
//     ClientException. UpdateJobQueue's state member says a DISABLED queue "can't accept new jobs".
//   - **The definition** is resolved as the page states: "definition-name, definition-name:revision, or
//     the Amazon Resource Name (ARN) for the job definition, with or without the revision … If the
//     revision is not specified, then the latest active revision is used." A named revision must exist
//     and be ACTIVE; a bare name needs at least one ACTIVE revision. Otherwise ClientException.
//
// The job keeps the queue and definition identifiers as sent, so a describe reports what the caller
// sent, as before. The messages are substrate's own.
//
// API_DeleteJobQueue: "All jobs in the queue are eventually terminated when you delete a job queue."
// Substrate ends them **at delete time**, since the page publishes no interval and a delete is the one
// request that can do it deterministically: every job of the queue that an observation would still
// report as SUBMITTED, PENDING, RUNNABLE, STARTING or RUNNING is written FAILED, which is what
// API_TerminateJob says terminating a job does. The job is peeked, not observed, so the delete spends
// none of a caller's polling budget, and a job that already reports SUCCEEDED or FAILED is left as it
// is. No page publishes a statusReason for such a job, so [batchQueueDeletedReason] is substrate's.

// batchQueueDeletedReason is the statusReason a job ended by its queue's deletion reports. Substrate's
// wording; no Batch page publishes one.
const batchQueueDeletedReason = "Job terminated because its job queue was deleted."

// batchRequireSubmittableQueue resolves a SubmitJob jobQueue and refuses one that cannot take a job.
func (p *BatchPlugin) batchRequireSubmittableQueue(ctx *RequestContext, identifier string) error {
	name := batchNameFromIdentifier(identifier)
	var queue BatchJobQueue
	found, err := p.loadBatchRecord(ctx, "job-queue", name, &queue)
	if err != nil {
		return err
	}
	if !found {
		return batchClientError(fmt.Sprintf("job queue %s does not exist", identifier))
	}
	if queue.State != batchStateEnabled {
		return batchClientError(fmt.Sprintf("job queue %s is %s and cannot accept new jobs", name, queue.State))
	}
	if queue.Status != "" && queue.Status != "VALID" {
		return batchClientError(fmt.Sprintf("job queue %s is %s and cannot accept new jobs", name, queue.Status))
	}
	return nil
}

// batchRequireActiveDefinition resolves a SubmitJob jobDefinition to an ACTIVE revision.
func (p *BatchPlugin) batchRequireActiveDefinition(ctx *RequestContext, identifier string) error {
	name := batchNameFromIdentifier(identifier)
	if colon := strings.LastIndexByte(name, ':'); colon >= 0 {
		if _, err := strconv.Atoi(name[colon+1:]); err != nil {
			return batchClientError(fmt.Sprintf("job definition %s does not name a revision", identifier))
		}
		var def BatchJobDefinition
		found, err := p.loadBatchRecord(ctx, "job-definition", name, &def)
		if err != nil {
			return err
		}
		if !found {
			return batchClientError(fmt.Sprintf("job definition %s does not exist", identifier))
		}
		if def.Status != "ACTIVE" {
			return batchClientError(fmt.Sprintf("job definition %s is %s", identifier, def.Status))
		}
		return nil
	}

	// A bare name is its latest ACTIVE revision, so the revisions are tried from the highest down.
	highest, err := p.highestJobDefinitionRevision(ctx, name)
	if err != nil {
		return err
	}
	for revision := highest; revision >= 1; revision-- {
		var def BatchJobDefinition
		found, err := p.loadBatchRecord(ctx, "job-definition", fmt.Sprintf("%s:%d", name, revision), &def)
		if err != nil {
			return err
		}
		if found && def.Status == "ACTIVE" {
			return nil
		}
	}
	return batchClientError(fmt.Sprintf("job definition %s has no ACTIVE revision", identifier))
}

// highestJobDefinitionRevision reads the highest revision a name has been registered at, or 0. It
// reads the counter [BatchPlugin.nextJobDefinitionRevision] advances, without advancing it.
func (p *BatchPlugin) highestJobDefinitionRevision(ctx *RequestContext, name string) (int, error) {
	key := "job-definition_revisions:" + ctx.AccountID + "/" + ctx.Region + "/" + name
	data, err := p.state.Get(context.Background(), batchNamespace, key)
	if err != nil {
		return 0, fmt.Errorf("batch job definition revisions get: %w", err)
	}
	if data == nil {
		return 0, nil
	}
	var highest int
	if err := json.Unmarshal(data, &highest); err != nil {
		return 0, fmt.Errorf("batch job definition revisions unmarshal: %w", err)
	}
	return highest, nil
}

// failQueueJobs ends every job of a deleted queue that an observation would still report as
// unfinished, writing it FAILED with [batchQueueDeletedReason]. See the file comment.
func (p *BatchPlugin) failQueueJobs(ctx *RequestContext, queueName string) error {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, batchNamespace, "job_ids:"+ctx.AccountID+"/"+ctx.Region)
	if err != nil {
		return fmt.Errorf("batch deleteJobQueue job index: %w", err)
	}
	for _, id := range ids {
		key := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + id
		data, err := p.state.Get(goCtx, batchNamespace, key)
		if err != nil {
			return fmt.Errorf("batch deleteJobQueue get job %s: %w", id, err)
		}
		if data == nil {
			continue
		}
		var job BatchJob
		if err := json.Unmarshal(data, &job); err != nil {
			return fmt.Errorf("batch deleteJobQueue unmarshal job %s: %w", id, err)
		}
		if batchNameFromIdentifier(job.JobQueue) != queueName {
			continue
		}
		observed, err := batchJobObserved(goCtx, p.state, &p.seedMu, job, false)
		if err != nil {
			return fmt.Errorf("batch deleteJobQueue peek job %s: %w", id, err)
		}
		if batchJobTerminal(observed.Status) {
			continue
		}
		job.Status = "FAILED"
		job.StatusReason = batchQueueDeletedReason
		updated, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("batch deleteJobQueue marshal job %s: %w", id, err)
		}
		if err := p.state.Put(goCtx, batchNamespace, key, updated); err != nil {
			return fmt.Errorf("batch deleteJobQueue put job %s: %w", id, err)
		}
	}
	return nil
}
