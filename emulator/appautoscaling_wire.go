package emulator

import "encoding/json"

// The Application Auto Scaling responses are rendered from these types, not from the persisted
// [aasTarget], so the record's bookkeeping member (EverTagged) and its nested policies and
// scheduled actions reach no response that does not publish them. Member names follow the model's
// ScalableTarget, ScalingPolicy and ScheduledAction shapes. Timestamps are EpochSeconds, because the
// API speaks application/x-amz-json-1.1, whose timestamps are epoch seconds.

// aasTargetOut is the ScalableTarget structure.
type aasTargetOut struct {
	ServiceNamespace  string            `json:"ServiceNamespace"`
	ResourceID        string            `json:"ResourceId"`
	ScalableDimension string            `json:"ScalableDimension"`
	MinCapacity       int32             `json:"MinCapacity"`
	MaxCapacity       int32             `json:"MaxCapacity"`
	RoleARN           string            `json:"RoleARN"`
	CreationTime      EpochSeconds      `json:"CreationTime"`
	SuspendedState    aasSuspendedState `json:"SuspendedState"`
	ScalableTargetARN string            `json:"ScalableTargetARN"`
}

// aasDescribeTargetsOut is DescribeScalableTargets' output.
type aasDescribeTargetsOut struct {
	ScalableTargets []aasTargetOut `json:"ScalableTargets"`
	NextToken       string         `json:"NextToken,omitempty"`
}

// aasTargetToWire projects a target record onto the ScalableTarget structure.
func aasTargetToWire(t *aasTarget) aasTargetOut {
	out := aasTargetOut{
		ServiceNamespace:  t.ServiceNamespace,
		ResourceID:        t.ResourceID,
		ScalableDimension: t.ScalableDimension,
		MinCapacity:       t.MinCapacity,
		MaxCapacity:       t.MaxCapacity,
		RoleARN:           t.RoleARN,
		CreationTime:      EpochSeconds(t.CreationTime),
		ScalableTargetARN: t.ScalableTargetARN,
	}
	if t.SuspendedState != nil {
		out.SuspendedState = *t.SuspendedState
	}
	return out
}

// aasPolicyOut is the ScalingPolicy structure. Alarms is always present, and empty for a policy that
// created none.
type aasPolicyOut struct {
	PolicyARN         string          `json:"PolicyARN"`
	PolicyName        string          `json:"PolicyName"`
	ServiceNamespace  string          `json:"ServiceNamespace"`
	ResourceID        string          `json:"ResourceId"`
	ScalableDimension string          `json:"ScalableDimension"`
	PolicyType        string          `json:"PolicyType"`
	StepConfig        json.RawMessage `json:"StepScalingPolicyConfiguration,omitempty"`
	TargetConfig      json.RawMessage `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	Predictive        json.RawMessage `json:"PredictiveScalingPolicyConfiguration,omitempty"`
	Alarms            []aasAlarm      `json:"Alarms"`
	CreationTime      EpochSeconds    `json:"CreationTime"`
}

// aasDescribePoliciesOut is DescribeScalingPolicies' output.
type aasDescribePoliciesOut struct {
	ScalingPolicies []aasPolicyOut `json:"ScalingPolicies"`
	NextToken       string         `json:"NextToken,omitempty"`
}

// aasPolicyToWire projects a policy and the target holding it onto the ScalingPolicy structure. Only
// the configuration the policy's type uses is answered.
func aasPolicyToWire(t *aasTarget, pol aasPolicy) aasPolicyOut {
	out := aasPolicyOut{
		PolicyARN:         pol.PolicyARN,
		PolicyName:        pol.PolicyName,
		ServiceNamespace:  t.ServiceNamespace,
		ResourceID:        t.ResourceID,
		ScalableDimension: t.ScalableDimension,
		PolicyType:        pol.PolicyType,
		Alarms:            pol.Alarms,
		CreationTime:      EpochSeconds(pol.CreationTime),
	}
	switch pol.PolicyType {
	case "StepScaling":
		out.StepConfig = pol.StepConfig
	case "TargetTrackingScaling":
		out.TargetConfig = pol.TargetConfig
	case "PredictiveScaling":
		out.Predictive = pol.Predictive
	}
	if out.Alarms == nil {
		out.Alarms = []aasAlarm{}
	}
	return out
}

// aasScheduledActionOut is the ScheduledAction structure.
type aasScheduledActionOut struct {
	ScheduledActionName  string                   `json:"ScheduledActionName"`
	ScheduledActionARN   string                   `json:"ScheduledActionARN"`
	ServiceNamespace     string                   `json:"ServiceNamespace"`
	Schedule             string                   `json:"Schedule"`
	Timezone             string                   `json:"Timezone,omitempty"`
	ResourceID           string                   `json:"ResourceId"`
	ScalableDimension    string                   `json:"ScalableDimension"`
	StartTime            *EpochSeconds            `json:"StartTime,omitempty"`
	EndTime              *EpochSeconds            `json:"EndTime,omitempty"`
	ScalableTargetAction *aasScalableTargetAction `json:"ScalableTargetAction,omitempty"`
	CreationTime         EpochSeconds             `json:"CreationTime"`
}

// aasDescribeScheduledOut is DescribeScheduledActions' output.
type aasDescribeScheduledOut struct {
	ScheduledActions []aasScheduledActionOut `json:"ScheduledActions"`
	NextToken        string                  `json:"NextToken,omitempty"`
}

// aasScheduledActionToWire projects a scheduled action and its target onto the ScheduledAction
// structure.
func aasScheduledActionToWire(t *aasTarget, a aasScheduledAction) aasScheduledActionOut {
	out := aasScheduledActionOut{
		ScheduledActionName:  a.ScheduledActionName,
		ScheduledActionARN:   a.ScheduledActionARN,
		ServiceNamespace:     t.ServiceNamespace,
		Schedule:             a.Schedule,
		Timezone:             a.Timezone,
		ResourceID:           t.ResourceID,
		ScalableDimension:    t.ScalableDimension,
		ScalableTargetAction: a.ScalableTargetAction,
		CreationTime:         EpochSeconds(a.CreationTime),
	}
	if a.StartTime != nil {
		s := EpochSeconds(*a.StartTime)
		out.StartTime = &s
	}
	if a.EndTime != nil {
		e := EpochSeconds(*a.EndTime)
		out.EndTime = &e
	}
	return out
}
