package substrate

import (
	"context"
	"fmt"
	"time"
)

// AWSRequest represents a parsed AWS API request received by the emulator.
type AWSRequest struct {
	// Service is the AWS service name (e.g., "s3", "iam", "dynamodb").
	Service string

	// Operation is the API operation name (e.g., "PutObject", "CreateUser").
	Operation string

	// Headers contains HTTP request headers, including AWS authentication headers.
	Headers map[string]string

	// Body contains the raw request body bytes.
	Body []byte

	// Params contains parsed query-string or form parameters.
	Params map[string]string
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

	// List returns all keys in namespace that share the given prefix.
	List(ctx context.Context, namespace, prefix string) ([]string, error)
}

// TimeController provides a controllable clock for deterministic testing.
// By replacing the system clock, Substrate produces identical event timestamps
// across replay runs.
type TimeController struct {
	current time.Time
	scale   float64
}

// NewTimeController creates a TimeController whose clock starts at t.
func NewTimeController(t time.Time) *TimeController {
	return &TimeController{current: t, scale: 1.0}
}

// Now returns the current controlled time.
func (c *TimeController) Now() time.Time {
	return c.current
}

// SetTime sets the controller's current time to ts.
func (c *TimeController) SetTime(ts time.Time) {
	c.current = ts
}

// SetScale sets the time acceleration factor. A scale of 86400 advances one
// real second for each simulated day.
func (c *TimeController) SetScale(scale float64) {
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

// PluginRegistry routes incoming AWS API requests to the appropriate [Plugin].
type PluginRegistry struct {
	plugins map[string]Plugin
}

// NewPluginRegistry creates an empty PluginRegistry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{plugins: make(map[string]Plugin)}
}

// Register adds p to the registry, keyed by [Plugin.Name].
// Registering two plugins with the same name replaces the first.
func (r *PluginRegistry) Register(p Plugin) {
	r.plugins[p.Name()] = p
}

// RouteRequest dispatches req to the plugin registered for req.Service.
// Returns an [*AWSError] with code "ServiceNotAvailable" if no matching
// plugin is registered.
func (r *PluginRegistry) RouteRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	p, ok := r.plugins[req.Service]
	if !ok {
		return nil, &AWSError{
			Code:       "ServiceNotAvailable",
			Message:    "service not emulated: " + req.Service,
			HTTPStatus: 501,
		}
	}
	return p.HandleRequest(ctx, req)
}
