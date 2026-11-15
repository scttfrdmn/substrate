package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"sync"
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
//     and be ACTIVE; a bare name needs at least one ACTIVE revision. Otherwise ClientException. A bare
//     name resolves through the name's index of ACTIVE revisions ([batchActiveRevisionsKey]), which
//     RegisterJobDefinition adds to and DeregisterJobDefinition removes from, so resolution is two
//     reads at most.
//
// The job keeps the queue and definition identifiers as sent, so a describe reports what the caller
// sent, as before. The messages are substrate's own.
//
// API_DeleteJobQueue: "All jobs in the queue are eventually terminated when you delete a job queue."
// Substrate ends them **at delete time**, since the page publishes no interval and a delete is the one
// request that can do it deterministically. Each job of the queue is ended by [BatchPlugin.endJob],
// the rule TerminateJob uses — a job that would report SUBMITTED, PENDING, RUNNABLE, STARTING or
// RUNNING is written FAILED, and one TerminateJob or CancelJob already ended is left — so the two
// operations agree on what ending a job means. The jobs are failed before the queue is deleted, so a
// delete that fails partway leaves the queue in place for a retry to finish. No page publishes a
// statusReason for such a job, so [batchQueueDeletedReason] is substrate's.
//
// A queue's check in SubmitJob and its job's write hold the queue's stripe of [batchQueueMutex], as
// DeleteJobQueue does from its check to its delete, so a job cannot be submitted to a queue between
// the delete's job sweep and the queue's removal.

// batchQueueDeletedReason is the statusReason a job ended by its queue's deletion reports. Substrate's
// wording; no Batch page publishes one.
const batchQueueDeletedReason = "Job terminated because its job queue was deleted."

// batchQueueLockStripes is the number of mutexes in [batchQueueMutex].
const batchQueueLockStripes = 64

// batchQueueMutex is a striped mutex set serializing SubmitJob against DeleteJobQueue per queue, the
// pattern of [s3KeyMutex]. It is process-local, as that one is.
type batchQueueMutex struct {
	stripes [batchQueueLockStripes]sync.Mutex
}

// lock acquires the stripe guarding one queue and returns its unlock function.
func (m *batchQueueMutex) lock(ctx *RequestContext, queueName string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ctx.AccountID + "/" + ctx.Region + "/" + queueName))
	mu := &m.stripes[h.Sum32()%batchQueueLockStripes]
	mu.Lock()
	return mu.Unlock
}

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
	} else {
		// A bare name is its latest ACTIVE revision.
		active, err := p.loadActiveRevisions(ctx, name)
		if err != nil {
			return err
		}
		if len(active) == 0 {
			return batchClientError(fmt.Sprintf("job definition %s has no ACTIVE revision", identifier))
		}
		name = fmt.Sprintf("%s:%d", name, active[len(active)-1])
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

// batchActiveRevisionsKey is the state key of a job definition name's ACTIVE revisions, ascending.
func batchActiveRevisionsKey(ctx *RequestContext, name string) string {
	return "job-definition_active:" + ctx.AccountID + "/" + ctx.Region + "/" + name
}

// loadActiveRevisions reads a name's ACTIVE revisions, ascending.
func (p *BatchPlugin) loadActiveRevisions(ctx *RequestContext, name string) ([]int, error) {
	data, err := p.state.Get(context.Background(), batchNamespace, batchActiveRevisionsKey(ctx, name))
	if err != nil {
		return nil, fmt.Errorf("batch active revisions get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var revisions []int
	if err := json.Unmarshal(data, &revisions); err != nil {
		return nil, fmt.Errorf("batch active revisions unmarshal: %w", err)
	}
	return revisions, nil
}

// setRevisionActive adds a revision to, or removes it from, its name's ACTIVE revisions.
func (p *BatchPlugin) setRevisionActive(ctx *RequestContext, name string, revision int, active bool) error {
	revisions, err := p.loadActiveRevisions(ctx, name)
	if err != nil {
		return err
	}
	revisions = slices.DeleteFunc(revisions, func(r int) bool { return r == revision })
	if active {
		revisions = append(revisions, revision)
		slices.Sort(revisions)
	}
	data, err := json.Marshal(revisions)
	if err != nil {
		return fmt.Errorf("batch active revisions marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), batchNamespace, batchActiveRevisionsKey(ctx, name), data); err != nil {
		return fmt.Errorf("batch active revisions put: %w", err)
	}
	return nil
}

// failQueueJobs ends every job of a deleted queue, by name or ARN, through [BatchPlugin.endJob] with
// [batchQueueDeletedReason]. See the file comment.
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
		if err := p.endJob(goCtx, key, job, batchQueueDeletedReason, batchJobTransientStates); err != nil {
			return fmt.Errorf("batch deleteJobQueue end job %s: %w", id, err)
		}
	}
	return nil
}
