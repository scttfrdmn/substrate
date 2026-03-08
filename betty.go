package substrate

import (
	"context"
	"fmt"
	"time"
)

// Intent expresses constraints that BettyClient checks during validation.
type Intent struct {
	// MaxCost is the maximum allowed total cost in USD. Zero means unchecked.
	MaxCost float64
}

// DeployedResource describes a single resource created by DeployStack.
type DeployedResource struct {
	// LogicalID is the CloudFormation logical resource ID.
	LogicalID string

	// Type is the CloudFormation resource type, e.g. "AWS::S3::Bucket".
	Type string

	// PhysicalID is the actual name used (bucket name, role name, etc.).
	PhysicalID string

	// ARN is populated for IAM resources; empty for S3.
	ARN string

	// Error is non-empty if this resource failed to create.
	Error string
}

// DeployResult is returned by DeployStack and BettyClient.Deploy.
type DeployResult struct {
	// StackName is the name of the deployed CloudFormation stack.
	StackName string

	// Resources is the list of resources that were deployed.
	Resources []DeployedResource

	// StreamID identifies the event stream containing deployment events.
	StreamID string

	// TotalCost is the estimated total USD cost for all deploy requests.
	TotalCost float64

	// Duration is the time taken to deploy all resources.
	Duration time.Duration
}

// BettyClient is a convenience wrapper for the full Betty.codes validation workflow.
// It orchestrates CloudFormation deployment, event recording, and validation analysis
// without requiring an HTTP server.
type BettyClient struct {
	registry *PluginRegistry
	store    *EventStore
	state    StateManager
	tc       *TimeController
	logger   Logger
	costs    *CostController
	intent   Intent
}

// NewBettyClient creates a BettyClient wired to the provided dependencies.
func NewBettyClient(
	registry *PluginRegistry,
	store *EventStore,
	state StateManager,
	tc *TimeController,
	logger Logger,
) *BettyClient {
	return &BettyClient{
		registry: registry,
		store:    store,
		state:    state,
		tc:       tc,
		logger:   logger,
		costs:    NewCostController(CostConfig{Enabled: true}),
	}
}

// Deploy parses cfn (JSON or YAML CloudFormation template), creates all described
// resources in order, records events under a generated stream, and validates the
// result against intent.
func (b *BettyClient) Deploy(ctx context.Context, cfn string, intent Intent) (*DeployResult, error) {
	b.intent = intent

	deployer := &StackDeployer{
		registry: b.registry,
		store:    b.store,
		tc:       b.tc,
		logger:   b.logger,
		costs:    b.costs,
	}

	streamID := fmt.Sprintf("deploy-%d", b.tc.Now().UnixNano())
	result, err := deployer.Deploy(ctx, cfn, streamID)
	if err != nil {
		return nil, err
	}

	if intent.MaxCost > 0 && result.TotalCost > intent.MaxCost {
		b.logger.Warn("deploy cost exceeds intent MaxCost",
			"total_cost", result.TotalCost,
			"max_cost", intent.MaxCost,
		)
	}

	return result, nil
}

// StartRecording opens a named recording session. Events recorded while the session
// is open are tagged with the session's StreamID.
func (b *BettyClient) StartRecording(ctx context.Context, name string) (*RecordingSession, error) {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	return engine.StartRecording(ctx, name)
}

// StopRecording closes session and returns a ValidationReport for its stream.
func (b *BettyClient) StopRecording(ctx context.Context, session *RecordingSession) (*ValidationReport, error) {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	if _, err := engine.StopRecording(ctx, session); err != nil {
		return nil, fmt.Errorf("stop recording: %w", err)
	}
	return ValidateRecording(ctx, b.store, session.StreamID, b.intent)
}

// Validate analyses the event stream identified by streamID and returns a full
// ValidationReport. The stored intent (from the most recent Deploy call) is applied.
func (b *BettyClient) Validate(ctx context.Context, streamID string) (*ValidationReport, error) {
	return ValidateRecording(ctx, b.store, streamID, b.intent)
}

// NewDebugSession returns a DebugSession initialised for time-travel inspection
// of the recorded stream identified by streamID.
func (b *BettyClient) NewDebugSession(streamID string) *DebugSession {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	return &DebugSession{
		engine:   engine,
		streamID: streamID,
	}
}
