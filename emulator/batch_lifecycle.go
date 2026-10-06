package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// The lifecycle half of Batch's three resource types (#555).
//
// #530 made the creates persist and routed the describes, which left every Batch resource
// able to enter service and never leave it: nothing could disable a queue or an environment,
// delete either, or deregister a job definition. That made DescribeJobDefinitions' documented
// INACTIVE filter unexercisable, because deregistration is the only writer of INACTIVE.
//
// Every operation here documents the same two errors as the rest of Batch, ClientException
// (400) and ServerException (500), so each refusal goes through [batchClientError]. A store
// failure is returned wrapped, which the server answers as a 500.

// batchStateEnabled and batchStateDisabled are the two values UpdateJobQueue's and
// UpdateComputeEnvironment's state member publishes: "Valid Values: ENABLED | DISABLED".
const (
	batchStateEnabled  = "ENABLED"
	batchStateDisabled = "DISABLED"
)

// loadBatchRecord reads one Batch resource into out, reporting whether it exists.
func (p *BatchPlugin) loadBatchRecord(ctx *RequestContext, resource, name string, out interface{}) (bool, error) {
	data, err := p.state.Get(context.Background(), batchNamespace, batchRecordKey(ctx, resource, name))
	if err != nil {
		return false, fmt.Errorf("batch %s get: %w", resource, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("batch %s unmarshal: %w", resource, err)
	}
	return true, nil
}

// saveBatchRecord overwrites an existing Batch resource. Unlike [BatchPlugin.putBatchRecord]
// it does not touch the name index, which already lists the resource.
func (p *BatchPlugin) saveBatchRecord(ctx *RequestContext, resource, name string, record interface{}) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("batch %s marshal: %w", resource, err)
	}
	if err := p.state.Put(context.Background(), batchNamespace, batchRecordKey(ctx, resource, name), data); err != nil {
		return fmt.Errorf("batch %s put: %w", resource, err)
	}
	return nil
}

// deleteBatchRecord removes a Batch resource and its entry in the name index, so neither a
// describe by name nor an unfiltered describe reports it afterwards.
//
// AWS reports a deleted queue or environment as DELETING and then DELETED before it stops
// being listed. Substrate removes it at once: the reference publishes the states as values of
// status but no observation count or interval for the transition, so there is nothing to make
// a progression assertable against.
func (p *BatchPlugin) deleteBatchRecord(ctx *RequestContext, resource, name string) error {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, batchNamespace, batchRecordKey(ctx, resource, name)); err != nil {
		return fmt.Errorf("batch %s delete: %w", resource, err)
	}
	if err := removeFromStringIndex(goCtx, p.state, batchNamespace, batchIndexKey(ctx, resource), name); err != nil {
		return fmt.Errorf("batch %s delete index: %w", resource, err)
	}
	return nil
}

// decodeBatchBody unmarshals a lifecycle request body, refusing an unreadable one with the
// ClientException every Batch page publishes.
func decodeBatchBody(req *AWSRequest, out interface{}) error {
	if err := json.Unmarshal(req.Body, out); err != nil {
		return batchClientError("invalid request body")
	}
	return nil
}

// batchValidState refuses a state outside the published ENABLED | DISABLED. An empty state
// is valid, because the member is Required: No and an absent one leaves the state unchanged.
func batchValidState(state string) error {
	switch state {
	case "", batchStateEnabled, batchStateDisabled:
		return nil
	}
	return batchClientError(fmt.Sprintf("state %q is not valid; valid values are ENABLED | DISABLED", state))
}

// deregisterJobDefinition marks one revision of a job definition INACTIVE.
//
// The revision is kept rather than deleted: "Job definitions are permanently deleted after 180
// days", and DescribeJobDefinitions publishes INACTIVE as a status filter value, so a
// deregistered revision stays describable. jobDefinition is "the name and revision
// (name:revision) or full Amazon Resource Name (ARN)", so a bare name, which addresses no
// single revision, is refused as an identifier that is not valid.
func (p *BatchPlugin) deregisterJobDefinition(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobDefinition string `json:"jobDefinition"`
	}
	if err := decodeBatchBody(req, &body); err != nil {
		return nil, err
	}
	if body.JobDefinition == "" {
		return nil, batchClientError("jobDefinition is required")
	}
	name := batchNameFromIdentifier(body.JobDefinition)
	if !strings.Contains(name, ":") {
		return nil, batchClientError(fmt.Sprintf(
			"jobDefinition %q must be name:revision or a job definition ARN", body.JobDefinition))
	}

	var def BatchJobDefinition
	found, err := p.loadBatchRecord(ctx, "job-definition", name, &def)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, batchClientError(fmt.Sprintf("job definition %s does not exist", body.JobDefinition))
	}
	def.Status = "INACTIVE"
	if err := p.saveBatchRecord(ctx, "job-definition", name, def); err != nil {
		return nil, err
	}
	// "If the action is successful, the service sends back an HTTP 200 response with an empty
	// HTTP body"; the page's own sample response is {}.
	return batchJSONResponse(http.StatusOK, map[string]string{})
}

// updateJobQueue changes a job queue's state, priority, scheduling policy or compute
// environment order. An absent member leaves its value unchanged.
func (p *BatchPlugin) updateJobQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobQueue                string                   `json:"jobQueue"`
		State                   string                   `json:"state"`
		Priority                *int                     `json:"priority"`
		SchedulingPolicyARN     string                   `json:"schedulingPolicyArn"`
		ComputeEnvironmentOrder []map[string]interface{} `json:"computeEnvironmentOrder"`
	}
	if err := decodeBatchBody(req, &body); err != nil {
		return nil, err
	}
	if body.JobQueue == "" {
		return nil, batchClientError("jobQueue is required")
	}
	if err := batchValidState(body.State); err != nil {
		return nil, err
	}

	name := batchNameFromIdentifier(body.JobQueue)
	var queue BatchJobQueue
	found, err := p.loadBatchRecord(ctx, "job-queue", name, &queue)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, batchClientError(fmt.Sprintf("job queue %s does not exist", body.JobQueue))
	}

	if body.State != "" {
		queue.State = body.State
	}
	if body.Priority != nil {
		queue.Priority = *body.Priority
	}
	if body.SchedulingPolicyARN != "" {
		queue.SchedulingPolicyARN = body.SchedulingPolicyARN
	}
	if body.ComputeEnvironmentOrder != nil {
		queue.ComputeEnvironmentOrder = body.ComputeEnvironmentOrder
	}
	if err := p.saveBatchRecord(ctx, "job-queue", name, queue); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{
		"jobQueueArn":  queue.JobQueueARN,
		"jobQueueName": queue.JobQueueName,
	})
}

// updateComputeEnvironment changes a compute environment's state, service role, unmanaged
// vCPU reservation or compute resources. An absent member leaves its value unchanged, and a
// computeResources update merges into the recorded resources, because ComputeResourceUpdate's
// members are each optional and name only what changes.
func (p *BatchPlugin) updateComputeEnvironment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ComputeEnvironment string                 `json:"computeEnvironment"`
		State              string                 `json:"state"`
		ServiceRole        string                 `json:"serviceRole"`
		UnmanagedvCPUs     *int                   `json:"unmanagedvCpus"`
		ComputeResources   map[string]interface{} `json:"computeResources"`
	}
	if err := decodeBatchBody(req, &body); err != nil {
		return nil, err
	}
	if body.ComputeEnvironment == "" {
		return nil, batchClientError("computeEnvironment is required")
	}
	if err := batchValidState(body.State); err != nil {
		return nil, err
	}

	name := batchNameFromIdentifier(body.ComputeEnvironment)
	var env BatchComputeEnvironment
	found, err := p.loadBatchRecord(ctx, "compute-environment", name, &env)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, batchClientError(fmt.Sprintf("compute environment %s does not exist", body.ComputeEnvironment))
	}

	if body.State != "" {
		env.State = body.State
	}
	if body.ServiceRole != "" {
		env.ServiceRole = body.ServiceRole
	}
	if body.UnmanagedvCPUs != nil {
		env.UnmanagedvCPUs = *body.UnmanagedvCPUs
	}
	if len(body.ComputeResources) > 0 {
		if env.ComputeResources == nil {
			env.ComputeResources = make(map[string]interface{}, len(body.ComputeResources))
		}
		for k, v := range body.ComputeResources {
			env.ComputeResources[k] = v
		}
	}
	if err := p.saveBatchRecord(ctx, "compute-environment", name, env); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{
		"computeEnvironmentArn":  env.ComputeEnvironmentARN,
		"computeEnvironmentName": env.ComputeEnvironmentName,
	})
}

// deleteJobQueue deletes a job queue, which must already be DISABLED: "You must first disable
// submissions for a queue with the UpdateJobQueue operation." Its compute environments need
// not be disassociated first — "It's not necessary to disassociate compute environments from a
// queue before submitting a DeleteJobQueue request." The queue's jobs are left as they are.
func (p *BatchPlugin) deleteJobQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobQueue string `json:"jobQueue"`
	}
	if err := decodeBatchBody(req, &body); err != nil {
		return nil, err
	}
	if body.JobQueue == "" {
		return nil, batchClientError("jobQueue is required")
	}

	name := batchNameFromIdentifier(body.JobQueue)
	var queue BatchJobQueue
	found, err := p.loadBatchRecord(ctx, "job-queue", name, &queue)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, batchClientError(fmt.Sprintf("job queue %s does not exist", body.JobQueue))
	}
	if queue.State != batchStateDisabled {
		return nil, batchClientError(fmt.Sprintf(
			"job queue %s must be DISABLED before it can be deleted; its state is %s", name, queue.State))
	}
	if err := p.deleteBatchRecord(ctx, "job-queue", name); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{})
}

// deleteComputeEnvironment deletes a compute environment. "Before you can delete a compute
// environment, you must set its state to DISABLED with the UpdateComputeEnvironment API
// operation and disassociate it from any job queues with the UpdateJobQueue API operation", so
// both preconditions are refused as ClientException.
func (p *BatchPlugin) deleteComputeEnvironment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ComputeEnvironment string `json:"computeEnvironment"`
	}
	if err := decodeBatchBody(req, &body); err != nil {
		return nil, err
	}
	if body.ComputeEnvironment == "" {
		return nil, batchClientError("computeEnvironment is required")
	}

	name := batchNameFromIdentifier(body.ComputeEnvironment)
	var env BatchComputeEnvironment
	found, err := p.loadBatchRecord(ctx, "compute-environment", name, &env)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, batchClientError(fmt.Sprintf("compute environment %s does not exist", body.ComputeEnvironment))
	}
	if env.State != batchStateDisabled {
		return nil, batchClientError(fmt.Sprintf(
			"compute environment %s must be DISABLED before it can be deleted; its state is %s", name, env.State))
	}
	queue, err := p.queueUsingComputeEnvironment(ctx, env)
	if err != nil {
		return nil, err
	}
	if queue != "" {
		return nil, batchClientError(fmt.Sprintf(
			"compute environment %s is associated with job queue %s; disassociate it with UpdateJobQueue first",
			name, queue))
	}
	if err := p.deleteBatchRecord(ctx, "compute-environment", name); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{})
}

// queueUsingComputeEnvironment returns the name of the first job queue whose
// computeEnvironmentOrder names env, by name or ARN, or "" when none does.
func (p *BatchPlugin) queueUsingComputeEnvironment(ctx *RequestContext, env BatchComputeEnvironment) (string, error) {
	names, err := loadStringIndex(context.Background(), p.state, batchNamespace, batchIndexKey(ctx, "job-queue"))
	if err != nil {
		return "", fmt.Errorf("batch job-queue index: %w", err)
	}
	for _, name := range names {
		var queue BatchJobQueue
		found, err := p.loadBatchRecord(ctx, "job-queue", name, &queue)
		if err != nil {
			return "", err
		}
		if !found {
			continue
		}
		for _, entry := range queue.ComputeEnvironmentOrder {
			ref, _ := entry["computeEnvironment"].(string)
			if ref == env.ComputeEnvironmentName || ref == env.ComputeEnvironmentARN {
				return name, nil
			}
		}
	}
	return "", nil
}
