package emulator

// StackDeployer helpers for CloudFormation's SQS resource types. The rules every in-place updater
// follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"encoding/xml"
	"fmt"
	"slices"
)

// ----- AWS::SQS::Queue ------------------------------------------------------------------

// updateSQSQueue applies a changed AWS::SQS::Queue's attributes through SetQueueAttributes, for the
// five the create forwards (sqsDriftAttributes). QueueName and FifoQueue are Replacement.
//
// A removed attribute resets to its default, which the page publishes for each — VisibilityTimeout
// "If you don't specify a value, AWS CloudFormation uses the default value of 30 seconds",
// DelaySeconds "The default value is 0", MessageRetentionPeriod 345,600 seconds,
// MaximumMessageSize 1 MiB, ReceiveMessageWaitTimeSeconds short polling (0). The values are SQS's
// own (sqsAttributeDefaults), so the queue reads back what a queue created without the attribute
// reports.
func (d *StackDeployer) updateSQSQueue(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	params := map[string]string{
		"Action":   "SetQueueAttributes",
		"QueueUrl": sqsQueueURL(cctx.region, cctx.accountID, prior.PhysicalID),
	}
	n := 0
	for _, name := range sqsDriftAttributes {
		if !slices.Contains(changed, name) {
			continue
		}
		value := sqsAttributeDefaults[name]
		if _, declared := props[name]; declared {
			value = resolveValue(props[name], cctx)
		}
		n++
		params[fmt.Sprintf("Attribute.%d.Name", n)] = name
		params[fmt.Sprintf("Attribute.%d.Value", n)] = value
	}
	if n == 0 {
		return dr, 0, nil
	}
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sqs", Operation: "SetQueueAttributes",
		Headers: map[string]string{}, Params: params}, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- AWS::SQS::QueuePolicy ---------------------------------------------------------------------

// cfnQueuePolicyQueues returns the queue URLs a QueuePolicy's Queues property names — "The URLs of
// the queues to which you want to add the policy", which a template usually writes as Refs to the
// queues, whose Ref is the URL.
func cfnQueuePolicyQueues(props map[string]interface{}, cctx *cfnContext) []string {
	return resolveValueList(props["Queues"], cctx)
}

// cfnQueuePolicyOwnerKey is the cloudformation-namespace key recording which QueuePolicy last
// applied a queue's policy. A queue's policy is one attribute, so two QueuePolicy resources naming
// one queue — the old and new logical IDs of a renamed CDK construct, which deploys the new one
// before it deletes the old — contend for it, and only the one that set it last may clear it.
func (d *StackDeployer) cfnQueuePolicyOwnerKey(queueURL string) string {
	return "queuepolicy_owner:" + d.identity.accountID + "/" + d.identity.region + "|" + queueURL
}

// setSQSQueuePolicy sets (or, with an empty policy, clears) one queue's Policy attribute.
func (d *StackDeployer) setSQSQueuePolicy(ctx context.Context, queueURL, policy, streamID string, cost *float64) error {
	return d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sqs", Operation: "SetQueueAttributes",
		Headers: map[string]string{}, Params: map[string]string{
			"Action": "SetQueueAttributes", "QueueUrl": queueURL,
			"Attribute.1.Name": "Policy", "Attribute.1.Value": policy,
		}}, streamID, cost)
}

// claimQueuePolicy sets the policy on a queue and records owner as the resource that set it.
func (d *StackDeployer) claimQueuePolicy(ctx context.Context, queueURL, policy, owner, streamID string, cost *float64) error {
	if err := d.setSQSQueuePolicy(ctx, queueURL, policy, streamID, cost); err != nil {
		return err
	}
	if d.state == nil {
		return nil
	}
	if err := d.state.Put(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL), []byte(owner)); err != nil {
		return fmt.Errorf("cfn record queue policy owner: %w", err)
	}
	return nil
}

// releaseQueuePolicy clears the policy owner applied to a queue — only when owner is still the
// resource that last set it and the queue's live policy is still the one owner applied, so a policy
// another resource or a caller has since put on the queue is left alone. A queue already deleted is
// done.
func (d *StackDeployer) releaseQueuePolicy(ctx context.Context, queueURL, applied, owner, streamID string, cost *float64) error {
	if d.state != nil {
		recorded, err := d.state.Get(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL))
		if err != nil {
			return fmt.Errorf("cfn read queue policy owner: %w", err)
		}
		if recorded != nil && string(recorded) != owner {
			return nil
		}
	}
	live, err := d.liveQueuePolicy(ctx, queueURL, streamID, cost)
	if err != nil {
		if cfnDeleteIsAbsent(err) {
			return nil
		}
		return err
	}
	if applied != "" && live != applied {
		return nil
	}
	if err := d.setSQSQueuePolicy(ctx, queueURL, "", streamID, cost); err != nil && !cfnDeleteIsAbsent(err) {
		return err
	}
	if d.state != nil {
		if err := d.state.Delete(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL)); err != nil {
			return fmt.Errorf("cfn clear queue policy owner: %w", err)
		}
	}
	return nil
}

// liveQueuePolicy reads a queue's Policy attribute through GetQueueAttributes.
func (d *StackDeployer) liveQueuePolicy(ctx context.Context, queueURL, streamID string, cost *float64) (string, error) {
	resp, c, err := d.dispatch(ctx, &AWSRequest{Service: "sqs", Operation: "GetQueueAttributes",
		Headers: map[string]string{}, Params: map[string]string{
			"Action": "GetQueueAttributes", "QueueUrl": queueURL, "AttributeName.1": "Policy",
		}}, streamID)
	*cost += c
	if err != nil {
		return "", err
	}
	var out struct {
		Attributes []struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	if err := xml.Unmarshal(resp.Body, &out); err != nil {
		return "", fmt.Errorf("cfn read queue policy: %w", err)
	}
	for _, a := range out.Attributes {
		if a.Name == "Policy" {
			return a.Value, nil
		}
	}
	return "", nil
}

// deployQueuePolicy applies the policy to each queue through SetQueueAttributes. The queues and
// the policy are recorded, so an update can release the queues it drops and the delete can release
// them all.
//
// A create that fails partway releases the queues it already set, since a failed resource is not
// one a rollback deletes. The physical ID and Fn::GetAtt Id are generated per stack, as
// CloudFormation generates the policy's ID; AWS publishes no format for it.
func (d *StackDeployer) deployQueuePolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	id := cfnGeneratedName(cctx, "AWS::SQS::QueuePolicy", logicalID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SQS::QueuePolicy", PhysicalID: id,
		Metadata: map[string]interface{}{"Id": id}}
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	var cost float64
	set, failure := d.setQueuePolicies(ctx, cfnQueuePolicyQueues(props, cctx), policy, id, streamID, &cost)
	if failure != "" {
		dr.Error = failure
		for _, q := range set {
			if err := d.releaseQueuePolicy(ctx, q, policy, id, streamID, &cost); err != nil {
				dr.Error += " (and the policy could not be cleared from " + q + ": " + err.Error() + ")"
			}
		}
		return dr, cost, nil
	}
	dr.Metadata["Queues"] = set
	dr.Metadata["Policy"] = policy
	return dr, cost, nil
}

// updateQueuePolicy re-applies a changed policy in place — both properties are "Update requires:
// No interruption" — and releases any queue the update drops. A failure partway records every queue
// the resource has touched, so the rollback and the delete still reach each one.
func (d *StackDeployer) updateQueuePolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	dr.Metadata = cfnCopyMetadata(dr.Metadata)
	previous, _ := cfnMetadataList(prior, "Queues")
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	var cost float64
	set, failure := d.setQueuePolicies(ctx, cfnQueuePolicyQueues(props, cctx), policy,
		prior.PhysicalID, streamID, &cost)
	if failure != "" {
		dr.Error = failure
		dr.Metadata["Queues"] = cfnUnion(previous, set)
		return dr, cost, nil
	}
	if failure := d.releaseDroppedQueues(ctx, previous, set, cfnRefMetadata(prior, "Policy"), prior.PhysicalID,
		streamID, &cost); failure != "" {
		dr.Error = failure
		dr.Metadata["Queues"] = cfnUnion(previous, set)
		return dr, cost, nil
	}
	dr.Metadata["Queues"] = set
	dr.Metadata["Policy"] = policy
	return dr, cost, nil
}

// releaseDroppedQueues releases each queue in previous that kept does not name, answering the first
// refusal's text, or "".
func (d *StackDeployer) releaseDroppedQueues(
	ctx context.Context, previous, kept []string, applied, owner, streamID string, cost *float64,
) string {
	for _, q := range previous {
		if slices.Contains(kept, q) {
			continue
		}
		if err := d.releaseQueuePolicy(ctx, q, applied, owner, streamID, cost); err != nil {
			return err.Error()
		}
	}
	return ""
}

// setQueuePolicies sets policy on every queue in queues, as owner, answering the queues it set and
// the first refusal's text, or "".
func (d *StackDeployer) setQueuePolicies(
	ctx context.Context, queues []string, policy, owner, streamID string, cost *float64,
) ([]string, string) {
	set := make([]string, 0, len(queues))
	for _, q := range queues {
		if err := d.claimQueuePolicy(ctx, q, policy, owner, streamID, cost); err != nil {
			return set, err.Error()
		}
		set = append(set, q)
	}
	return set, ""
}

// cfnUnion returns a's members followed by those of b not in a.
func cfnUnion(a, b []string) []string {
	out := append([]string{}, a...)
	for _, v := range b {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

// cfnDeleteQueuePolicy releases the policy from every queue the resource recorded. The policy
// depends on its queues, so the sweep deletes it before any queue the stack also owns; a queue that
// is already gone — one this stack does not own, deleted elsewhere — is skipped, as an absent
// resource is everywhere in the sweep.
func cfnDeleteQueuePolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext, streamID string) *CFNResourceDeletion {
	queues, _ := cfnMetadataList(dr, "Queues")
	var cost float64
	for _, q := range queues {
		if err := d.releaseQueuePolicy(ctx, q, cfnRefMetadata(dr, "Policy"), dr.PhysicalID, streamID, &cost); err != nil {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
	}
	return nil
}
