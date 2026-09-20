package emulator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// AWSRequest represents a parsed AWS API request received by the emulator.
type AWSRequest struct {
	// Service is the AWS service name (e.g., "s3", "iam", "dynamodb").
	Service string

	// Operation is the API operation name (e.g., "PutObject", "CreateUser").
	//
	// For a REST service the operation is carried by the shape of the URL and
	// nothing else, so ParseAWSRequest derives it from Method and Path (#480,
	// #572). Until that resolution succeeds — and for an in-process caller that
	// builds an AWSRequest by hand — this may still hold a bare HTTP method, which
	// is why HTTPMethod exists separately: resolving Operation must not destroy the
	// verb the resolvers need.
	Operation string

	// HTTPMethod is the verb of the underlying HTTP request ("GET", "PUT", …).
	//
	// It is set by ParseAWSRequest and is what a REST plugin's path resolver keys
	// on, because a REST operation is identified by verb *and* path: POST and
	// DELETE on /v1/apis/{id} are different operations. Empty for an in-process
	// AWSRequest built by hand, in which case Operation carries the verb; use
	// requestMethod to read whichever holds it.
	HTTPMethod string

	// Headers contains HTTP request headers, including AWS authentication headers.
	Headers map[string]string

	// Body contains the raw request body bytes.
	Body []byte

	// Params contains parsed query-string or form parameters.
	Params map[string]string

	// Path is the effective URL path of the HTTP request. For S3 virtual-hosted
	// requests the bucket is prepended so the plugin always sees /bucket[/key].
	//
	// On a recorded control-plane write — an AWSRequest no plugin ever sees, built by
	// [Server.recordControlPlaneWrites] — it is the full request target including the
	// query string, because replaying such a write means re-issuing that exact request
	// and most of the clear endpoints carry their key in the query (#1140).
	Path string

	// Protocol is the wire serialization the caller used, classified by
	// [detectWireProtocol] during parsing.
	//
	// It matters for a service whose model declares more than one — CloudWatch
	// declares awsQuery, awsJson1_0 and rpcv2Cbor at once, and its clients disagree
	// about which to send — because the answer's serialization has to follow the
	// question's. A single-protocol plugin can ignore this field; the zero value,
	// [WireQuery], is what a request carrying none of the newer protocols' markers
	// is (#757).
	Protocol WireProtocol

	// QueryMode reports whether the caller sent X-Amzn-Query-Mode: true, which a
	// client of an aws.protocols#awsQueryCompatible service sets when its own callers
	// may still be matching on the Query protocol's error codes. A server seeing it
	// must return those codes in an x-amzn-query-error response header; substrate does
	// that in [marshalAWSError] rather than in each plugin.
	QueryMode bool
}

// AWSResponse represents an AWS API response produced by the emulator.
type AWSResponse struct {
	// StatusCode is the HTTP status code of the response.
	StatusCode int

	// Headers contains HTTP response headers.
	Headers map[string]string

	// Body contains the raw response body bytes.
	Body []byte
}

// AWSError represents a structured AWS-style error response.
type AWSError struct {
	// Code is the AWS error code (e.g., "NoSuchBucket", "AccessDenied").
	Code string

	// Message is the human-readable error description.
	Message string

	// HTTPStatus is the HTTP status code associated with this error.
	HTTPStatus int
}

// Error implements the error interface.
func (e *AWSError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// RequestContext holds per-request metadata propagated through the handling chain.
type RequestContext struct {
	// RequestID is a unique identifier for this request.
	RequestID string

	// AccountID is the AWS account ID associated with the request.
	AccountID string

	// Region is the AWS region targeted by the request.
	Region string

	// Timestamp is the time at which the request was received.
	Timestamp time.Time

	// Principal is the authenticated caller, or nil for unauthenticated requests.
	Principal *Principal

	// Metadata holds arbitrary key-value pairs for cross-cutting concerns
	// such as stream IDs and replay context.
	Metadata map[string]interface{}
}

// Principal represents the authenticated caller of an AWS API request.
type Principal struct {
	// ARN is the Amazon Resource Name that identifies the caller.
	ARN string

	// Type is the principal type: "User", "Role", "Service", or "AssumedRole".
	Type string

	// UserName is the IAM user name behind the request, or empty when there is
	// none.
	//
	// It is what [authzPrincipalContext] publishes as `aws:username`, and it is
	// carried here rather than parsed back out of ARN because not every ARN in
	// that field names a user: a registry hit with no IAM entity behind it
	// synthesizes `…:user/<access-key-id>` (see [Server.resolveCallerPrincipal]),
	// and publishing that as a user name would put a credential ID in a policy
	// comparison. Empty for every principal for which AWS publishes no
	// `aws:username` — the account root, an assumed role, and a federated or
	// service-linked caller (#745).
	UserName string

	// UserID is the caller's unique identifier — the `aws:userid` value, already in
	// the form AWS documents for this caller's own principal kind — or empty when
	// substrate has none to report.
	//
	// AWS publishes a different shape per kind: an IAM user's `AIDA…`, and
	// `<role-id>:<session-name>` for an assumed role, an EC2 instance role and a
	// federated session. So the value is *recorded* when the credential is minted
	// rather than assembled here — the pairing an assumed role publishes is known
	// only to the `AssumeRole` call that made it, since the session's ARN carries the
	// role's name and not its ID.
	//
	// [authzPrincipalContext] publishes no `aws:userid` at all when this is empty,
	// which is the #737/#745 fallback shape: a credential resolved from a registry
	// entry with no IAM entity behind it has no unique ID, nor does a record written
	// before #771, and a policy can test an absent key with `Null` where a guessed
	// one would silently match or silently refuse.
	//
	// The account root is the one kind AWS documents that substrate cannot carry
	// here, because it models no root principal: an unauthenticated caller resolves
	// to a nil *Principal, which [AuthController.CheckAccess] leaves unenforced and
	// `sts:GetCallerIdentity` reports as `…:root`.
	UserID string

	// Tags are the IAM tags on the entity behind the request, keyed by tag key, and
	// empty when it carries none or is not an entity that holds tags.
	//
	// [authzPrincipalContext] publishes one `aws:PrincipalTag/<key>` per entry. They
	// are read from the entity's own record when the credential is resolved rather
	// than recorded beside the credential, because `TagUser` and `UntagUser` change
	// them after a key is minted: a snapshot taken at `CreateAccessKey` time would
	// authorize a long-lived key against tags its principal no longer has.
	//
	// Session tags are not modeled. Substrate's `AssumeRole` reads no `Tags`
	// parameter, so an assumed role's tags here are the *role's* own, where AWS would
	// also publish whatever the session passed (#771).
	Tags map[string]string
}

// StateManager defines the interface for reading and writing emulator state.
// Implementations may store state in memory (for testing) or SQLite (for
// persistence across runs).
type StateManager interface {
	// Get retrieves the value stored at namespace/key.
	// Returns (nil, nil) if the key does not exist.
	Get(ctx context.Context, namespace, key string) ([]byte, error)

	// Put stores value at namespace/key, creating or overwriting as needed.
	Put(ctx context.Context, namespace, key string, value []byte) error

	// Delete removes namespace/key. No error is returned if the key is absent.
	Delete(ctx context.Context, namespace, key string) error

	// List returns all keys in namespace that share the given prefix, sorted
	// lexicographically.
	//
	// The ordering is part of the contract: a caller may rely on it, and an
	// implementation that returns keys in an arbitrary order — Go map iteration
	// order, say — does not satisfy this interface. Substrate's replay guarantee
	// depends on it, because a listing rendered into a response body, or paged
	// over with a cursor, cannot reproduce byte-for-byte otherwise (#865).
	List(ctx context.Context, namespace, prefix string) ([]string, error)
}

// SnapshotableStateManager extends [StateManager] with the ability to capture
// and restore its entire contents as opaque bytes, and to wipe all state.
type SnapshotableStateManager interface {
	StateManager

	// Snapshot serializes the entire state into opaque bytes.
	Snapshot(ctx context.Context) ([]byte, error)

	// Restore deserializes previously snapshotted bytes back into the manager,
	// replacing all existing state.
	Restore(ctx context.Context, data []byte) error

	// Reset wipes all state, leaving the manager empty.
	Reset(ctx context.Context) error
}

// TimeController provides a controllable clock for deterministic testing and
// time-accelerated simulation.
//
// By replacing the system clock, Substrate produces identical event timestamps
// across replay runs.  When a scale factor greater than 1.0 is set via
// [TimeController.SetScale], the controlled clock advances faster than wall
// time: a scale of 3600 makes one real second equal one simulated hour.
//
// Implementation: the controller stores a (simulated baseline, wall baseline)
// pair.  Now() computes:
//
//	simulated_baseline + (wall_now - wall_baseline) * scale
//
// Calling SetTime or SetScale resets both baselines atomically so the new
// value takes effect immediately without a discontinuous jump.
//
// The first sentence above is true only while the clock is **frozen**, which is
// what [TimeController.Freeze] is for. A baseline that advances with wall time
// means SetTime sets where the clock starts from, not what it reads: two reads of
// Now() after the same SetTime differ by however long the code between them took,
// so a value rendered from the clock is reproducible across runs only to within
// that latency. At second resolution — which is what RFC3339 without fractional
// seconds gives, and what most AWS timestamps are rendered at — that is a
// difference whenever the two reads straddle a second boundary, and identical
// otherwise. [ReplayEngine.replayEvent] freezes for exactly this reason (#1217).
type TimeController struct {
	mu           sync.RWMutex
	simBaseline  time.Time // simulated time at last SetTime/SetScale/Freeze call
	wallBaseline time.Time // real wall time at last SetTime/SetScale/Unfreeze call
	scale        float64
	frozen       bool // when set, Now returns simBaseline and ignores wall time
}

// NewTimeController creates a TimeController whose simulated clock starts at t
// with a scale factor of 1.0 (real-time).
func NewTimeController(t time.Time) *TimeController {
	return &TimeController{
		simBaseline:  t,
		wallBaseline: time.Now(),
		scale:        1.0,
	}
}

// Now returns the current controlled time, advanced from the last SetTime or
// SetScale call by (wall elapsed) * scale.
//
// While the clock is frozen it returns the simulated baseline itself, so every
// read between a [TimeController.Freeze] and the matching
// [TimeController.Unfreeze] returns the same instant no matter how long the code
// between them takes.
func (c *TimeController) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.frozen {
		return c.simBaseline
	}
	elapsed := time.Since(c.wallBaseline)
	return c.simBaseline.Add(time.Duration(float64(elapsed) * c.scale))
}

// SetTime sets the simulated clock to ts.  The scale factor is preserved and
// wall-time tracking restarts from this point, so subsequent Now() calls
// advance from ts.
//
// The frozen state is preserved too, which is the combination a replay wants:
// freeze, then SetTime to the recorded timestamp, and every read of the clock
// returns that timestamp exactly rather than advancing from it.
func (c *TimeController) SetTime(ts time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.simBaseline = ts
	c.wallBaseline = time.Now()
}

// Freeze stops the simulated clock at the instant Now() currently reports, so
// every subsequent read returns that same instant until Unfreeze.  Freezing an
// already-frozen clock changes nothing.
//
// This is the mechanism that makes a value rendered from the clock *exactly*
// reproducible rather than nearly so (#1217). It is a distinct state rather than a
// scale of zero, because the scale is reported over the control plane
// (GET /v1/control/time) and that endpoint refuses to set a scale of zero — so a
// frozen clock reported as scale 0 would be a state a caller could read and not
// restore, and would conflate "stopped for the duration of one replayed event"
// with "running at a factor the API says is invalid".
//
// To stop the clock at a *chosen* instant, call Freeze and then SetTime, in that
// order. The reverse advances the clock by the wall interval between the two calls
// before stopping it, because Freeze stops the clock where it currently reads —
// tens of nanoseconds, which is invisible until something renders the difference.
// [TestServer.FreezeTimeAt] is the ordering written down once.
func (c *TimeController) Freeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	// Capture the time the clock currently reads, so freezing is not itself a jump.
	elapsed := time.Since(c.wallBaseline)
	c.simBaseline = c.simBaseline.Add(time.Duration(float64(elapsed) * c.scale))
	c.wallBaseline = time.Now()
	c.frozen = true
}

// Unfreeze resumes advance from the instant the clock was stopped at, at the
// scale it was configured with.  Unfreezing a clock that is not frozen changes
// nothing.
func (c *TimeController) Unfreeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.frozen {
		return
	}
	// Restart wall tracking here rather than from the Freeze call, so the interval
	// the clock was stopped for is not paid back in one step.
	c.wallBaseline = time.Now()
	c.frozen = false
}

// Frozen reports whether the simulated clock is stopped.
func (c *TimeController) Frozen() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.frozen
}

// Scale returns the current time acceleration factor.
func (c *TimeController) Scale() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.scale
}

// SetScale sets the time acceleration factor.  A scale of 1.0 is real-time;
// 3600.0 makes one real second equal one simulated hour; 86400.0 makes one
// real second equal one simulated day.  The current simulated time is
// captured atomically so there is no jump at the transition.
//
// A frozen clock stays frozen and does not advance: the capture below is skipped,
// because while frozen the simulated baseline already *is* the time the clock
// reads, and advancing it by the wall interval since the freeze would undo the
// freeze in the act of changing the scale.
func (c *TimeController) SetScale(scale float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.frozen {
		// Capture current simulated time before changing scale.
		elapsed := time.Since(c.wallBaseline)
		c.simBaseline = c.simBaseline.Add(time.Duration(float64(elapsed) * c.scale))
		c.wallBaseline = time.Now()
	}
	c.scale = scale
}

// Logger is the structured logging interface used throughout Substrate.
// Arguments follow the key-value convention used by [log/slog].
type Logger interface {
	// Debug logs a message at debug level.
	Debug(msg string, args ...any)

	// Info logs a message at info level.
	Info(msg string, args ...any)

	// Warn logs a message at warning level.
	Warn(msg string, args ...any)

	// Error logs a message at error level.
	Error(msg string, args ...any)
}

// PluginConfig holds configuration passed to a [Plugin] during initialization.
type PluginConfig struct {
	// State is the state manager the plugin should use for persistence.
	State StateManager

	// Logger is the logger the plugin should use.
	Logger Logger

	// Options holds plugin-specific configuration values.
	Options map[string]any
}

// Plugin is the interface that all AWS service emulation plugins must implement.
type Plugin interface {
	// Name returns the AWS service name handled by this plugin (e.g., "s3").
	Name() string

	// Initialize sets up the plugin with the provided configuration.
	Initialize(ctx context.Context, config PluginConfig) error

	// HandleRequest processes an AWS API request and returns a response.
	HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error)

	// Shutdown releases any resources held by the plugin.
	Shutdown(ctx context.Context) error
}

// ResettablePlugin is an optional capability a [Plugin] may implement when it
// keeps mutable state of its own — a sequence counter, a cache, a random source —
// outside the [StateManager].
//
// It exists because resetting the state manager is not the whole of resetting the
// emulator. A plugin that mints an identifier from a counter on its own struct
// carries that counter across a state reset, so a replay of one recorded stream
// produced different identifiers each time it ran in the same process (#886).
// Follow the [SnapshotableStateManager] pattern: the capability is discovered by
// type assertion, so the plugins that keep everything in the state manager — most
// of them — need no method at all.
type ResettablePlugin interface {
	// ResetForRun returns the plugin's own mutable state to the value it had at the
	// start of a run. It must be safe to call on a plugin that has never handled a
	// request, and safe to call concurrently with request handling.
	ResetForRun(ctx context.Context) error
}

// PluginRegistry routes incoming AWS API requests to the appropriate [Plugin].
type PluginRegistry struct {
	mu      sync.RWMutex
	plugins map[string]Plugin
}

// NewPluginRegistry creates an empty PluginRegistry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{plugins: make(map[string]Plugin)}
}

// Register adds p to the registry, keyed by [Plugin.Name].
// Registering two plugins with the same name replaces the first.
func (r *PluginRegistry) Register(p Plugin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.plugins[p.Name()] = p
}

// RouteRequest dispatches req to the plugin registered for req.Service.
// Returns an [*AWSError] with code "ServiceNotAvailable" if no matching
// plugin is registered.
func (r *PluginRegistry) RouteRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	r.mu.RLock()
	p, ok := r.plugins[req.Service]
	r.mu.RUnlock()
	if !ok {
		return nil, &AWSError{
			Code:       "ServiceNotAvailable",
			Message:    "service not emulated: " + req.Service,
			HTTPStatus: 501,
		}
	}
	return p.HandleRequest(ctx, req)
}

// Plugin returns the plugin registered under name, and whether one is registered.
// It is how a caller asks what a registered plugin can do — for instance whether it
// implements [ResettablePlugin] — without routing a request to it.
func (r *PluginRegistry) Plugin(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.plugins[name]
	return p, ok
}

// ResetPlugins returns every registered [ResettablePlugin] to its start-of-run
// state. A plugin that does not implement [ResettablePlugin] is skipped, which is
// almost all of them: a plugin whose only mutable state lives in the
// [StateManager] is already reset by resetting the state manager.
//
// Every resettable plugin is visited even after one of them fails, and the errors
// are joined. Stopping at the first failure would leave the remaining plugins
// holding the previous run's counters — the exact condition this exists to
// remove — while reporting that a reset had been attempted.
//
// Plugins are visited in sorted name order so that a multi-plugin failure reports
// the same way on every run.
func (r *PluginRegistry) ResetPlugins(ctx context.Context) error {
	// The registry lock is released before any plugin is called: a plugin's reset may
	// re-enter the registry (S3 notifications route through it), and holding the read
	// lock across a callback risks deadlocking against a concurrent Register.
	r.mu.RLock()
	targets := make(map[string]ResettablePlugin, len(r.plugins))
	for name, p := range r.plugins {
		if rp, ok := p.(ResettablePlugin); ok {
			targets[name] = rp
		}
	}
	r.mu.RUnlock()

	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)

	var errs []error
	for _, name := range names {
		if err := targets[name].ResetForRun(ctx); err != nil {
			errs = append(errs, fmt.Errorf("reset plugin %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// Names returns the sorted list of service names registered in the registry.
func (r *PluginRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.plugins))
	for name := range r.plugins {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
