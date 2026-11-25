package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ----- Scaling policies ----------------------------------------------------------------------------

// aasPolicyTypes are the PolicyType enum's values.
var aasPolicyTypes = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"StepScaling": true, "TargetTrackingScaling": true, "PredictiveScaling": true,
}

// aasPutPolicyInput is PutScalingPolicy's input. The configurations are kept as sent.
type aasPutPolicyInput struct {
	aasTargetRef
	PolicyName   string          `json:"PolicyName"`
	PolicyType   string          `json:"PolicyType"`
	StepConfig   json.RawMessage `json:"StepScalingPolicyConfiguration"`
	TargetConfig json.RawMessage `json:"TargetTrackingScalingPolicyConfiguration"`
	Predictive   json.RawMessage `json:"PredictiveScalingPolicyConfiguration"`
}

// aasTargetTrackingConfig is the part of a TargetTrackingScalingPolicyConfiguration substrate reads
// to define the policy's alarms.
type aasTargetTrackingConfig struct {
	TargetValue                   *float64 `json:"TargetValue"`
	PredefinedMetricSpecification *struct {
		PredefinedMetricType string `json:"PredefinedMetricType"`
		ResourceLabel        string `json:"ResourceLabel"`
	} `json:"PredefinedMetricSpecification"`
	CustomizedMetricSpecification *struct {
		MetricName string `json:"MetricName"`
		Namespace  string `json:"Namespace"`
		Statistic  string `json:"Statistic"`
		Dimensions []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"Dimensions"`
	} `json:"CustomizedMetricSpecification"`
}

// aasCompact returns raw compacted, or nil for an absent or null member, so a stored configuration
// is byte-stable whatever whitespace the caller sent.
func aasCompact(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, fmt.Errorf("application-autoscaling compact configuration: %w", err)
	}
	return buf.Bytes(), nil
}

// aasCheckName validates a policy or scheduled action name: 1 to 256 printable characters, the
// PolicyName shape's constraints. The message is substrate's own.
func aasCheckName(member, name string) *AWSError {
	if name == "" {
		return aasValidation(member + " is required.")
	}
	if len(name) > 256 {
		return aasValidation(member + " must have length less than or equal to 256.")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return aasValidation(member + " must contain only printable characters.")
		}
	}
	return nil
}

// putScalingPolicy handles PutScalingPolicy, which "Creates or updates a scaling policy". A policy
// on an unregistered target is refused ("You cannot create a scaling policy until you have
// registered the resource as a scalable target"). A new policy needs its type, and the type's
// configuration ("This parameter is required if you are creating a policy and the policy type is
// StepScaling"). A target tracking policy creates its two alarms in CloudWatch and answers them in
// Alarms; an update keeps the alarms' names and redefines them.
func (p *ApplicationAutoScalingPlugin) putScalingPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasPutPolicyInput
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckName("PolicyName", in.PolicyName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	if in.PolicyType != "" && !aasPolicyTypes[in.PolicyType] {
		return nil, aasValidation(fmt.Sprintf("Unsupported policy type: %s.", in.PolicyType))
	}
	step, err := aasCompact(in.StepConfig)
	if err != nil {
		return nil, aasValidation("StepScalingPolicyConfiguration is not well-formed.")
	}
	tracking, err := aasCompact(in.TargetConfig)
	if err != nil {
		return nil, aasValidation("TargetTrackingScalingPolicyConfiguration is not well-formed.")
	}
	predictive, err := aasCompact(in.Predictive)
	if err != nil {
		return nil, aasValidation("PredictiveScalingPolicyConfiguration is not well-formed.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, aasTargetNotFound(in.ServiceNamespace, in.ResourceID, in.ScalableDimension)
	}
	idx := slices.IndexFunc(t.Policies, func(pol aasPolicy) bool { return pol.PolicyName == in.PolicyName })
	pol := aasPolicy{PolicyName: in.PolicyName, CreationTime: p.tc.Now().UTC()}
	if idx >= 0 {
		pol = t.Policies[idx]
	}
	stored := pol
	if in.PolicyType != "" && in.PolicyType != pol.PolicyType {
		// A policy that changes type carries only the new type's configuration: the stored
		// configuration of the old type no longer describes it.
		pol.PolicyType = in.PolicyType
		pol.StepConfig, pol.TargetConfig, pol.Predictive = nil, nil, nil
	}
	if step != nil {
		pol.StepConfig = step
	}
	if tracking != nil {
		pol.TargetConfig = tracking
	}
	if predictive != nil {
		pol.Predictive = predictive
	}
	if awsErr := aasCheckPolicy(t.ServiceNamespace, pol); awsErr != nil {
		return nil, awsErr
	}
	if idx < 0 {
		pol.PolicyARN = fmt.Sprintf("arn:aws:autoscaling:%s:%s:scalingPolicy:%s:resource/%s/%s:policyName/%s",
			ctx.Region, ctx.AccountID, ctx.IDs.UUID(), t.ServiceNamespace, t.ResourceID, pol.PolicyName)
	}

	// Alarms this request creates are undone if a later step fails; alarms the policy stops owning
	// are deleted only once the record no longer names them.
	var created, retired []aasAlarm
	if pol.PolicyType == "TargetTrackingScaling" {
		if len(pol.Alarms) == 0 {
			prefix := "TargetTracking-" + t.ResourceID
			for _, suffix := range aasAlarmSuffixes(t.ServiceNamespace) {
				name := prefix + "-" + suffix + "-" + ctx.IDs.UUID()
				pol.Alarms = append(pol.Alarms, aasAlarm{
					AlarmName: name,
					AlarmARN:  "arn:aws:cloudwatch:" + ctx.Region + ":" + ctx.AccountID + ":alarm:" + name,
				})
			}
			created = pol.Alarms
		}
		if err := p.putTrackingAlarms(ctx, t, pol); err != nil {
			p.deleteAlarms(ctx, created)
			if created == nil && stored.PolicyType == "TargetTrackingScaling" {
				// A re-put that failed partway redefined some of the policy's alarms; put them back.
				p.restoreTrackingAlarms(ctx, t, stored)
			}
			return nil, err
		}
	} else if len(pol.Alarms) > 0 {
		retired, pol.Alarms = pol.Alarms, nil
	}

	if idx >= 0 {
		t.Policies[idx] = pol
	} else {
		t.Policies = append(t.Policies, pol)
	}
	if err := p.storeTarget(key, t); err != nil {
		p.deleteAlarms(ctx, created)
		return nil, err
	}
	p.deleteAlarms(ctx, retired)
	out := struct {
		PolicyARN string     `json:"PolicyARN"`
		Alarms    []aasAlarm `json:"Alarms,omitempty"`
	}{PolicyARN: pol.PolicyARN, Alarms: pol.Alarms}
	return aasJSON(out)
}

// aasCheckPolicy validates a policy as it will be stored: it has a type, the type is one the
// target's namespace supports, and the type's configuration is present. The support rules are
// API_PutScalingPolicy's PolicyType list; the messages are substrate's own.
func aasCheckPolicy(namespace string, pol aasPolicy) *AWSError {
	switch pol.PolicyType {
	case "":
		return aasValidation("PolicyType is required when creating a scaling policy.")
	case "StepScaling":
		if aasStepUnsupported[namespace] {
			return aasValidation(fmt.Sprintf("StepScaling is not supported for service namespace %s.", namespace))
		}
		if pol.StepConfig == nil {
			return aasValidation("StepScalingPolicyConfiguration is required for a StepScaling policy.")
		}
	case "TargetTrackingScaling":
		if namespace == "elasticmapreduce" {
			return aasValidation("TargetTrackingScaling is not supported for service namespace elasticmapreduce.")
		}
		if pol.TargetConfig == nil {
			return aasValidation("TargetTrackingScalingPolicyConfiguration is required for a TargetTrackingScaling policy.")
		}
		var cfg aasTargetTrackingConfig
		if err := json.Unmarshal(pol.TargetConfig, &cfg); err != nil || cfg.TargetValue == nil {
			return aasValidation("TargetTrackingScalingPolicyConfiguration.TargetValue is required.")
		}
		if cfg.PredefinedMetricSpecification == nil && cfg.CustomizedMetricSpecification == nil {
			return aasValidation("TargetTrackingScalingPolicyConfiguration needs a PredefinedMetricSpecification or a CustomizedMetricSpecification.")
		}
	case "PredictiveScaling":
		if namespace != "ecs" {
			return aasValidation(fmt.Sprintf("PredictiveScaling is not supported for service namespace %s.", namespace))
		}
		if pol.Predictive == nil {
			return aasValidation("PredictiveScalingPolicyConfiguration is required for a PredictiveScaling policy.")
		}
	}
	return nil
}

// aasPredefinedMetrics maps the predefined metric types whose CloudWatch metric AWS documents for a
// target tracking policy to that metric. A type not listed here, or a customized metric built from
// metric math, gets an alarm on substrate's placeholder namespace instead. Alarm definitions are
// substrate's approximation: AWS publishes the alarms' names, not their metric, threshold or
// evaluation settings.
var aasPredefinedMetrics = map[string]struct{ namespace, metric, statistic string }{ //nolint:gochecknoglobals // immutable lookup table
	"DynamoDBReadCapacityUtilization":    {"AWS/DynamoDB", "ConsumedReadCapacityUnits", "Sum"},
	"DynamoDBWriteCapacityUtilization":   {"AWS/DynamoDB", "ConsumedWriteCapacityUnits", "Sum"},
	"ECSServiceAverageCPUUtilization":    {"AWS/ECS", "CPUUtilization", "Average"},
	"ECSServiceAverageMemoryUtilization": {"AWS/ECS", "MemoryUtilization", "Average"},
}

// aasAlarmSuffixes names the alarms a target tracking policy creates, in the order Alarms lists
// them: AlarmHigh and AlarmLow for every namespace, and for DynamoDB also ProvisionedCapacityHigh and
// ProvisionedCapacityLow — "four CloudWatch alarms are created for each read and write auto-scaling
// event: ProvisionedCapacity alarms: ProvisionedCapacityLow, ProvisionedCapacityHigh and
// ConsumedCapacity alarms: AlarmHigh, AlarmLow" (the DynamoDB developer guide, "Managing throughput
// capacity automatically with DynamoDB auto scaling").
func aasAlarmSuffixes(namespace string) []string {
	if namespace == "dynamodb" {
		return []string{"AlarmHigh", "AlarmLow", "ProvisionedCapacityHigh", "ProvisionedCapacityLow"}
	}
	return []string{"AlarmHigh", "AlarmLow"}
}

// aasPlaceholderMetricNamespace is the namespace of an alarm whose metric substrate does not map.
// It is deliberately not an AWS/ namespace, so no reader mistakes it for AWS's choice.
const aasPlaceholderMetricNamespace = "Substrate/ApplicationAutoScaling"

// putTrackingAlarms defines pol's alarms in CloudWatch with PutMetricAlarm, which creates an alarm
// or redefines one of the same name. AlarmHigh breaches above the target value and AlarmLow below
// 90% of it, on the tracked metric; a DynamoDB target's ProvisionedCapacityHigh and
// ProvisionedCapacityLow breach when the provisioned capacity leaves the target's range. All act on
// the policy's ARN. Without a registry, or without a CloudWatch plugin, the alarms are named and not
// created. A CloudWatch refusal is FailedResourceAccessException; any other error is returned as is.
func (p *ApplicationAutoScalingPlugin) putTrackingAlarms(ctx *RequestContext, t *aasTarget, pol aasPolicy) error {
	if p.registry == nil {
		return nil
	}
	if _, ok := p.registry.Plugin(monitoringNamespace); !ok {
		return nil
	}
	var cfg aasTargetTrackingConfig
	if err := json.Unmarshal(pol.TargetConfig, &cfg); err != nil || cfg.TargetValue == nil {
		return aasValidation("TargetTrackingScalingPolicyConfiguration.TargetValue is required.")
	}
	namespace, metric, statistic := aasPlaceholderMetricNamespace, "TargetTrackingMetric", "Average"
	var dims [][2]string
	switch {
	case cfg.CustomizedMetricSpecification != nil && cfg.CustomizedMetricSpecification.MetricName != "":
		c := cfg.CustomizedMetricSpecification
		namespace, metric = c.Namespace, c.MetricName
		if c.Statistic != "" {
			statistic = c.Statistic
		}
		for _, d := range c.Dimensions {
			dims = append(dims, [2]string{d.Name, d.Value})
		}
	case cfg.PredefinedMetricSpecification != nil:
		if m, ok := aasPredefinedMetrics[cfg.PredefinedMetricSpecification.PredefinedMetricType]; ok {
			namespace, metric, statistic = m.namespace, m.metric, m.statistic
			dims = aasTargetDimensions(t)
		} else {
			metric = cfg.PredefinedMetricSpecification.PredefinedMetricType
		}
	}
	provisioned := "ProvisionedWriteCapacityUnits"
	if strings.HasSuffix(t.ScalableDimension, ":ReadCapacityUnits") {
		provisioned = "ProvisionedReadCapacityUnits"
	}
	type alarmSpec struct {
		namespace, metric, statistic, op, periods string
		threshold                                 float64
		dims                                      [][2]string
	}
	specs := map[string]alarmSpec{
		"AlarmHigh": {namespace, metric, statistic, "GreaterThanThreshold", "3", *cfg.TargetValue, dims},
		"AlarmLow":  {namespace, metric, statistic, "LessThanThreshold", "15", *cfg.TargetValue * 0.9, dims},
		"ProvisionedCapacityHigh": {"AWS/DynamoDB", provisioned, "Average", "GreaterThanThreshold", "3",
			float64(t.MaxCapacity), aasTargetDimensions(t)},
		"ProvisionedCapacityLow": {"AWS/DynamoDB", provisioned, "Average", "LessThanThreshold", "3",
			float64(t.MinCapacity), aasTargetDimensions(t)},
	}
	for _, alarm := range pol.Alarms {
		spec, ok := specs[aasAlarmKind(t, alarm.AlarmName)]
		if !ok {
			continue
		}
		params := map[string]string{
			"Action": "PutMetricAlarm", "AlarmName": alarm.AlarmName,
			"Namespace": spec.namespace, "MetricName": spec.metric, "Statistic": spec.statistic,
			"Period": "60", "EvaluationPeriods": spec.periods,
			"Threshold":             strconv.FormatFloat(spec.threshold, 'f', -1, 64),
			"ComparisonOperator":    spec.op,
			"AlarmActions.member.1": pol.PolicyARN,
		}
		for j, d := range spec.dims {
			base := "Dimensions.member." + strconv.Itoa(j+1) + "."
			params[base+"Name"], params[base+"Value"] = d[0], d[1]
		}
		_, err := p.registry.RouteRequest(ctx, &AWSRequest{
			Service: monitoringNamespace, Operation: "PutMetricAlarm", Headers: map[string]string{}, Params: params,
		})
		var awsErr *AWSError
		if errors.As(err, &awsErr) {
			// FailedResourceAccessException is the code the page gives for a failure reaching the
			// policy's alarms; the message is substrate's own.
			return &AWSError{Code: "FailedResourceAccessException",
				Message: "Failed to create the alarms for the target tracking policy: " + awsErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if err != nil {
			return fmt.Errorf("application-autoscaling put alarm %s: %w", alarm.AlarmName, err)
		}
	}
	return nil
}

// aasAlarmKind returns which of [aasAlarmSuffixes] an alarm of target t carries, read from the
// part of the name after "TargetTracking-{ResourceId}-" and before the 36-character UUID, so a
// resource ID that itself contains a suffix's text cannot be mistaken for one.
func aasAlarmKind(t *aasTarget, name string) string {
	rest, ok := strings.CutPrefix(name, "TargetTracking-"+t.ResourceID+"-")
	if !ok || len(rest) < 38 || rest[len(rest)-37] != '-' {
		return ""
	}
	return rest[:len(rest)-37]
}

// restoreTrackingAlarms puts back the definitions pol's alarms had before a failed re-put. It is
// best effort: the request is already failing with the error that caused it.
func (p *ApplicationAutoScalingPlugin) restoreTrackingAlarms(ctx *RequestContext, t *aasTarget, pol aasPolicy) {
	if err := p.putTrackingAlarms(ctx, t, pol); err != nil && p.logger != nil {
		p.logger.Warn("application-autoscaling: restore policy alarms", "policy", pol.PolicyName, "error", err)
	}
}

// redefineTrackingAlarms re-puts the alarms of t's target tracking policies after its capacity range
// changed, because a DynamoDB target's provisioned-capacity alarms are defined on it. If one fails,
// the alarms already redefined are put back on the old range and the error is returned.
func (p *ApplicationAutoScalingPlugin) redefineTrackingAlarms(ctx *RequestContext, t *aasTarget, oldMin, oldMax int32) error {
	for i, pol := range t.Policies {
		if pol.PolicyType != "TargetTrackingScaling" || len(pol.Alarms) == 0 {
			continue
		}
		if err := p.putTrackingAlarms(ctx, t, pol); err != nil {
			old := *t
			old.MinCapacity, old.MaxCapacity = oldMin, oldMax
			for _, done := range t.Policies[:i+1] {
				if done.PolicyType == "TargetTrackingScaling" {
					p.restoreTrackingAlarms(ctx, &old, done)
				}
			}
			return err
		}
	}
	return nil
}

// aasTargetDimensions returns the CloudWatch dimensions that identify t's resource for the metrics
// [aasPredefinedMetrics] maps: TableName (and GlobalSecondaryIndexName) for DynamoDB, and
// ClusterName and ServiceName for ECS.
func aasTargetDimensions(t *aasTarget) [][2]string {
	switch t.ServiceNamespace {
	case "dynamodb":
		parts := strings.Split(strings.TrimPrefix(t.ResourceID, "table/"), "/index/")
		dims := [][2]string{{"TableName", parts[0]}}
		if len(parts) == 2 {
			dims = append(dims, [2]string{"GlobalSecondaryIndexName", parts[1]})
		}
		return dims
	case "ecs":
		parts := strings.SplitN(strings.TrimPrefix(t.ResourceID, "service/"), "/", 2)
		if len(parts) == 2 {
			return [][2]string{{"ClusterName", parts[0]}, {"ServiceName", parts[1]}}
		}
	}
	return nil
}

// deleteAlarms removes alarms from CloudWatch with DeleteAlarms. An alarm already gone is not an
// error, and a failure is logged rather than answered: the policy is deleted either way.
func (p *ApplicationAutoScalingPlugin) deleteAlarms(ctx *RequestContext, alarms []aasAlarm) {
	if p.registry == nil || len(alarms) == 0 {
		return
	}
	if _, ok := p.registry.Plugin(monitoringNamespace); !ok {
		return
	}
	params := map[string]string{"Action": "DeleteAlarms"}
	for i, a := range alarms {
		params["AlarmNames.member."+strconv.Itoa(i+1)] = a.AlarmName
	}
	if _, err := p.registry.RouteRequest(ctx, &AWSRequest{
		Service: monitoringNamespace, Operation: "DeleteAlarms", Headers: map[string]string{}, Params: params,
	}); err != nil && p.logger != nil {
		p.logger.Warn("application-autoscaling: delete policy alarms", "error", err)
	}
}

// aasDeletePolicyInput is DeleteScalingPolicy's input.
type aasDeletePolicyInput struct {
	aasTargetRef
	PolicyName string `json:"PolicyName"`
}

// deleteScalingPolicy handles DeleteScalingPolicy. A target tracking policy's alarms are the
// policy's own, so they are deleted with it; a step scaling policy has none.
func (p *ApplicationAutoScalingPlugin) deleteScalingPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasDeletePolicyInput
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckName("PolicyName", in.PolicyName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	idx := -1
	if found {
		idx = slices.IndexFunc(t.Policies, func(pol aasPolicy) bool { return pol.PolicyName == in.PolicyName })
	}
	if idx < 0 {
		return nil, aasObjectNotFound(fmt.Sprintf(
			"No scaling policy found for service namespace: %s, resource ID: %s, scalable dimension: %s, policy name: %s",
			in.ServiceNamespace, in.ResourceID, in.ScalableDimension, in.PolicyName))
	}
	alarms := t.Policies[idx].Alarms
	t.Policies = slices.Delete(t.Policies, idx, idx+1)
	if err := p.storeTarget(key, t); err != nil {
		return nil, err
	}
	p.deleteAlarms(ctx, alarms)
	return aasJSON(struct{}{})
}

// aasDescribeInput is the input DescribeScalingPolicies and DescribeScheduledActions share: a
// namespace, optional name, resource and dimension filters, and a page.
type aasDescribeInput struct {
	Names             []string `json:"-"`
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId"`
	ScalableDimension string   `json:"ScalableDimension"`
	MaxResults        *int32   `json:"MaxResults"`
	NextToken         string   `json:"NextToken"`
}

// aasTargetsFor returns the targets in in's namespace that match its resource and dimension
// filters.
func (p *ApplicationAutoScalingPlugin) aasTargetsFor(ctx *RequestContext, in aasDescribeInput) ([]*aasTarget, error) {
	targets, _, err := aasListTargets(context.Background(), p.state, aasNamespacePrefix(ctx.AccountID, ctx.Region, in.ServiceNamespace))
	if err != nil {
		return nil, err
	}
	out := targets[:0]
	for _, t := range targets {
		if (in.ResourceID == "" || t.ResourceID == in.ResourceID) &&
			(in.ScalableDimension == "" || t.ScalableDimension == in.ScalableDimension) {
			out = append(out, t)
		}
	}
	return out, nil
}

// aasOwnedPolicy is a policy with the target that holds it.
type aasOwnedPolicy struct {
	target *aasTarget
	policy aasPolicy
}

// describeScalingPolicies handles DescribeScalingPolicies: the namespace's policies narrowed by
// PolicyNames, ResourceId and ScalableDimension, ten to a page by default.
func (p *ApplicationAutoScalingPlugin) describeScalingPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		aasDescribeInput
		PolicyNames []string `json:"PolicyNames"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	in.Names = in.PolicyNames
	if awsErr := aasCheckFilter(in.ServiceNamespace, in.ScalableDimension); awsErr != nil {
		return nil, awsErr
	}
	if len(in.Names) > 50 {
		return nil, aasValidation("PolicyNames must contain no more than 50 items.")
	}
	size, awsErr := aasPageSize(in.MaxResults, 10, 10)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := aasDescribePage("DescribeScalingPolicies", in.aasDescribeInput, size)
	if err := preq.validate(); err != nil {
		return nil, aasInvalidNextToken()
	}
	targets, err := p.aasTargetsFor(ctx, in.aasDescribeInput)
	if err != nil {
		return nil, err
	}
	var matched []aasOwnedPolicy
	for _, t := range targets {
		for _, pol := range t.Policies {
			if len(in.Names) == 0 || slices.Contains(in.Names, pol.PolicyName) {
				matched = append(matched, aasOwnedPolicy{target: t, policy: pol})
			}
		}
	}
	res, err := page(matched, func(o aasOwnedPolicy) cursorKey {
		return cursorKey{Sort: o.target.ResourceID + "\x00" + o.target.ScalableDimension, Tie: o.policy.PolicyName}
	}, preq)
	if err != nil {
		return nil, aasInvalidNextToken()
	}
	out := aasDescribePoliciesOut{ScalingPolicies: make([]aasPolicyOut, 0, len(res.Items)), NextToken: res.Next}
	for _, o := range res.Items {
		out.ScalingPolicies = append(out.ScalingPolicies, aasPolicyToWire(o.target, o.policy))
	}
	return aasJSON(out)
}

// aasDescribePage is the page request of a policy or scheduled action listing, whose filter is the
// request's every selecting member.
func aasDescribePage(op string, in aasDescribeInput, size int) pageRequest {
	return pageRequest{
		Op: op, Token: in.NextToken, Size: size,
		Filter: map[string][]string{
			"ServiceNamespace": {in.ServiceNamespace}, "ResourceId": {in.ResourceID},
			"ScalableDimension": {in.ScalableDimension}, "Names": in.Names,
		},
	}
}

// ----- Scheduled actions ---------------------------------------------------------------------------

// aasPutScheduledInput is PutScheduledAction's input.
type aasPutScheduledInput struct {
	aasTargetRef
	ScheduledActionName  string                   `json:"ScheduledActionName"`
	Schedule             *string                  `json:"Schedule"`
	Timezone             *string                  `json:"Timezone"`
	StartTime            *EpochSeconds            `json:"StartTime"`
	EndTime              *EpochSeconds            `json:"EndTime"`
	ScalableTargetAction *aasScalableTargetAction `json:"ScalableTargetAction"`
}

// aasCheckSchedule validates a Schedule's form: "at(yyyy-mm-ddThh:mm:ss)", "rate(value unit)" or
// "cron(fields)", the three the member's documentation lists. The message is substrate's own.
func aasCheckSchedule(schedule string) *AWSError {
	for _, prefix := range []string{"at(", "rate(", "cron("} {
		if strings.HasPrefix(schedule, prefix) && strings.HasSuffix(schedule, ")") && len(schedule) > len(prefix)+1 {
			return nil
		}
	}
	return aasValidation(fmt.Sprintf("Unsupported schedule expression: %s.", schedule))
}

// putScheduledAction handles PutScheduledAction, which "Creates or updates a scheduled action". A new
// action needs a Schedule. An update changes the members it names, except the start and end times:
// "If you don't specify start and end times, the old values are deleted." The action is recorded and
// never run.
func (p *ApplicationAutoScalingPlugin) putScheduledAction(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in aasPutScheduledInput
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckName("ScheduledActionName", in.ScheduledActionName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	if in.Schedule != nil {
		if awsErr := aasCheckSchedule(*in.Schedule); awsErr != nil {
			return nil, awsErr
		}
	}
	if a := in.ScalableTargetAction; a != nil && a.MinCapacity != nil && a.MaxCapacity != nil && *a.MinCapacity > *a.MaxCapacity {
		return nil, aasValidation("ScalableTargetAction.MinCapacity cannot be greater than ScalableTargetAction.MaxCapacity.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, aasTargetNotFound(in.ServiceNamespace, in.ResourceID, in.ScalableDimension)
	}
	idx := slices.IndexFunc(t.ScheduledActions, func(a aasScheduledAction) bool {
		return a.ScheduledActionName == in.ScheduledActionName
	})
	var action aasScheduledAction
	if idx >= 0 {
		action = t.ScheduledActions[idx]
	} else {
		if in.Schedule == nil {
			return nil, aasValidation("Schedule is required when creating a scheduled action.")
		}
		action = aasScheduledAction{
			ScheduledActionName: in.ScheduledActionName,
			ScheduledActionARN: fmt.Sprintf("arn:aws:autoscaling:%s:%s:scheduledAction:%s:resource/%s/%s:scheduledActionName/%s",
				ctx.Region, ctx.AccountID, ctx.IDs.UUID(), t.ServiceNamespace, t.ResourceID, in.ScheduledActionName),
			CreationTime: p.tc.Now().UTC(),
		}
	}
	if in.Schedule != nil {
		action.Schedule = *in.Schedule
	}
	if in.Timezone != nil {
		action.Timezone = *in.Timezone
	}
	if in.ScalableTargetAction != nil {
		action.ScalableTargetAction = in.ScalableTargetAction
	}
	action.StartTime, action.EndTime = aasTimeOrNil(in.StartTime), aasTimeOrNil(in.EndTime)
	if idx >= 0 {
		t.ScheduledActions[idx] = action
	} else {
		t.ScheduledActions = append(t.ScheduledActions, action)
	}
	if err := p.storeTarget(key, t); err != nil {
		return nil, err
	}
	return aasJSON(struct{}{})
}

// aasTimeOrNil converts an optional request timestamp to the stored form.
func aasTimeOrNil(e *EpochSeconds) *time.Time {
	if e == nil || e.IsZero() {
		return nil
	}
	t := e.Time().UTC()
	return &t
}

// deleteScheduledAction handles DeleteScheduledAction.
func (p *ApplicationAutoScalingPlugin) deleteScheduledAction(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		aasTargetRef
		ScheduledActionName string `json:"ScheduledActionName"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := aasCheckName("ScheduledActionName", in.ScheduledActionName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := aasCheckTarget(in.ServiceNamespace, in.ScalableDimension, in.ResourceID); awsErr != nil {
		return nil, awsErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := aasTargetKey(ctx.AccountID, ctx.Region, in.ServiceNamespace, in.ScalableDimension, in.ResourceID)
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	idx := -1
	if found {
		idx = slices.IndexFunc(t.ScheduledActions, func(a aasScheduledAction) bool {
			return a.ScheduledActionName == in.ScheduledActionName
		})
	}
	if idx < 0 {
		return nil, aasObjectNotFound(fmt.Sprintf(
			"No scheduled action found for service namespace: %s, resource ID: %s, scalable dimension: %s, scheduled action name: %s",
			in.ServiceNamespace, in.ResourceID, in.ScalableDimension, in.ScheduledActionName))
	}
	t.ScheduledActions = slices.Delete(t.ScheduledActions, idx, idx+1)
	if err := p.storeTarget(key, t); err != nil {
		return nil, err
	}
	return aasJSON(struct{}{})
}

// aasOwnedAction is a scheduled action with the target that holds it.
type aasOwnedAction struct {
	target *aasTarget
	action aasScheduledAction
}

// describeScheduledActions handles DescribeScheduledActions: the namespace's scheduled actions
// narrowed by ScheduledActionNames, ResourceId and ScalableDimension, 50 to a page by default.
func (p *ApplicationAutoScalingPlugin) describeScheduledActions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		aasDescribeInput
		ScheduledActionNames []string `json:"ScheduledActionNames"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	in.Names = in.ScheduledActionNames
	if awsErr := aasCheckFilter(in.ServiceNamespace, in.ScalableDimension); awsErr != nil {
		return nil, awsErr
	}
	if len(in.Names) > 50 {
		return nil, aasValidation("ScheduledActionNames must contain no more than 50 items.")
	}
	size, awsErr := aasPageSize(in.MaxResults, 50, 50)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := aasDescribePage("DescribeScheduledActions", in.aasDescribeInput, size)
	if err := preq.validate(); err != nil {
		return nil, aasInvalidNextToken()
	}
	targets, err := p.aasTargetsFor(ctx, in.aasDescribeInput)
	if err != nil {
		return nil, err
	}
	var matched []aasOwnedAction
	for _, t := range targets {
		for _, a := range t.ScheduledActions {
			if len(in.Names) == 0 || slices.Contains(in.Names, a.ScheduledActionName) {
				matched = append(matched, aasOwnedAction{target: t, action: a})
			}
		}
	}
	res, err := page(matched, func(o aasOwnedAction) cursorKey {
		return cursorKey{Sort: o.target.ResourceID + "\x00" + o.target.ScalableDimension, Tie: o.action.ScheduledActionName}
	}, preq)
	if err != nil {
		return nil, aasInvalidNextToken()
	}
	out := aasDescribeScheduledOut{ScheduledActions: make([]aasScheduledActionOut, 0, len(res.Items)), NextToken: res.Next}
	for _, o := range res.Items {
		out.ScheduledActions = append(out.ScheduledActions, aasScheduledActionToWire(o.target, o.action))
	}
	return aasJSON(out)
}
