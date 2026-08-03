package emulator

import (
	"context"
	"fmt"
	"time"
)

// Intent expresses constraints that Client checks during validation.
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

	// Metadata holds additional GetAtt-resolvable attributes for this resource,
	// such as RootResourceId, InvokeURL, RepositoryUri, etc.
	Metadata map[string]interface{}
}

// DeployResult is returned by DeployStack and Client.Deploy.
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

	// Outputs contains the resolved CloudFormation Outputs section values.
	Outputs map[string]string

	// Status is the terminal stack status the deploy reached: CREATE_COMPLETE,
	// UPDATE_COMPLETE, or one of the failure statuses when a resource failed —
	// CREATE_FAILED, ROLLBACK_COMPLETE, ROLLBACK_FAILED, DELETE_COMPLETE,
	// UPDATE_ROLLBACK_COMPLETE, UPDATE_ROLLBACK_FAILED.
	//
	// A failed create is reported here rather than as an error, the way the API
	// reports it: real CreateStack has already returned its StackId before the
	// rollback happens, so a rolled-back stack is not a failed call. For
	// OnFailure=DELETE this is the *only* place the outcome is readable, since the
	// stack record is gone.
	Status string

	// StatusReason explains a non-successful Status, and is empty otherwise.
	StatusReason string

	// ResourceDeletions records what a rollback did with each resource the failed
	// create had created, and is empty when nothing was rolled back.
	ResourceDeletions []CFNResourceDeletion
}

// Client is a convenience wrapper for the full in-process validation workflow. It
// orchestrates CloudFormation deployment, event recording, and validation analysis
// without requiring an HTTP server.
type Client struct {
	registry *PluginRegistry
	store    *EventStore
	state    StateManager
	tc       *TimeController
	logger   Logger
	costs    *CostController
	intent   Intent
}

// NewClient creates a Client wired to the provided dependencies.
func NewClient(
	registry *PluginRegistry,
	store *EventStore,
	state StateManager,
	tc *TimeController,
	logger Logger,
) *Client {
	return &Client{
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
func (b *Client) Deploy(ctx context.Context, cfn string, intent Intent) (*DeployResult, error) {
	b.intent = intent

	// Built through the constructor rather than as a struct literal so there is one
	// construction path and the deployer's defaults cannot drift away from it. A
	// literal left the account and region empty, which was harmless until the
	// deployer started reading them.
	//
	// No identity option: Client is the in-process validation client and its
	// callers never sign a request, so there is no caller whose identity could be
	// threaded — substrate's defaults are the right answer here rather than a
	// placeholder for one. An in-process caller that does need another partition
	// can pass WithDeployerIdentity to NewStackDeployer directly.
	deployer := NewStackDeployer(b.registry, b.store, b.state, b.tc, b.logger, b.costs)

	streamID := fmt.Sprintf("deploy-%d", b.tc.Now().UnixNano())
	result, err := deployer.Deploy(ctx, cfn, streamID, nil)
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
func (b *Client) StartRecording(ctx context.Context, name string) (*RecordingSession, error) {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	return engine.StartRecording(ctx, name)
}

// StopRecording closes session and returns a ValidationReport for its stream.
func (b *Client) StopRecording(ctx context.Context, session *RecordingSession) (*ValidationReport, error) {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	if _, err := engine.StopRecording(ctx, session); err != nil {
		return nil, fmt.Errorf("stop recording: %w", err)
	}
	return ValidateRecording(ctx, b.store, session.StreamID, b.intent)
}

// Validate analyses the event stream identified by streamID and returns a full
// ValidationReport. The stored intent (from the most recent Deploy call) is applied.
func (b *Client) Validate(ctx context.Context, streamID string) (*ValidationReport, error) {
	return ValidateRecording(ctx, b.store, streamID, b.intent)
}

// NewDebugSession returns a DebugSession initialized for time-travel inspection
// of the recorded stream identified by streamID.
func (b *Client) NewDebugSession(streamID string) *DebugSession {
	engine := NewReplayEngine(b.store, b.state, b.tc, b.registry, ReplayConfig{}, b.logger)
	return &DebugSession{
		engine:   engine,
		streamID: streamID,
	}
}

// BettyClient is the former name of [Client].
//
// Deprecated: use [Client]. The name referred to an unrelated project and says
// nothing about what the type does; BettyClient will be removed in v1.0.0, which is
// the only point a Go module can drop an exported symbol without breaking the import
// path for every consumer.
//
// It is an alias rather than a distinct type on purpose: a consumer holding both a
// *BettyClient and a *Client, or passing one where the other is expected, keeps
// compiling for the whole deprecation window. A defined type would have made the
// rename a breaking change dressed as a deprecation.
type BettyClient = Client

// NewBettyClient is the former name of [NewClient].
//
// Deprecated: use [NewClient]. Will be removed in v1.0.0. See [BettyClient].
func NewBettyClient(
	registry *PluginRegistry,
	store *EventStore,
	state StateManager,
	tc *TimeController,
	logger Logger,
) *Client {
	return NewClient(registry, store, state, tc, logger)
}
