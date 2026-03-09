// Package substrate — plugin developer guide.
//
// # Building a Substrate Plugin
//
// A plugin is any type that satisfies the [Plugin] interface:
//
//	type Plugin interface {
//	    Name() string
//	    Initialize(ctx context.Context, cfg PluginConfig) error
//	    HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error)
//	    Shutdown(ctx context.Context) error
//	}
//
// # PluginConfig
//
// The [PluginConfig] struct is passed to [Plugin.Initialize]. It provides:
//
//   - State — a [StateManager] for reading and writing service state.
//   - Logger — a [Logger] for structured log output.
//   - Options — a map[string]any for plugin-specific configuration such as
//     a TimeController or a PluginRegistry reference for cross-plugin dispatch.
//
// # State Key Naming Conventions
//
// To avoid collisions, state keys should follow the pattern:
//
//	"resource_type:accountID/resourceName"
//
// For example, the IAM plugin uses "iam_user:123456789012/alice" and the
// S3 plugin uses "bucket:my-bucket".
//
// # AWSRequest / AWSResponse / AWSError
//
// Incoming requests arrive as [AWSRequest] values with the following fields:
//
//   - Service — lowercase service name (e.g. "s3", "iam", "dynamodb").
//   - Operation — PascalCase operation name (e.g. "PutObject", "CreateUser").
//   - Params — decoded form/query parameters (query-protocol services).
//   - Body — raw request body bytes (REST-protocol services).
//   - Headers — HTTP headers from the original request.
//   - Region, AccountID — extracted from the Authorization header.
//
// Return an [AWSResponse] with StatusCode, Headers, and Body. On failure,
// return an [AWSError] with Code, Message, and HTTPStatus fields. Use
// standard AWS error codes (e.g. "NoSuchBucket", "ResourceNotFoundException").
//
// # Testing Patterns
//
// Unit-test a plugin using [NewMemoryStateManager] for state and
// [NewDefaultLogger] for logging:
//
//	func TestMyPlugin_GetWidget(t *testing.T) {
//	    state := substrate.NewMemoryStateManager()
//	    logger := substrate.NewDefaultLogger(slog.LevelDebug, false)
//	    p := &MyPlugin{}
//	    err := p.Initialize(context.Background(), substrate.PluginConfig{
//	        State:  state,
//	        Logger: logger,
//	    })
//	    require.NoError(t, err)
//
//	    req := &substrate.AWSRequest{
//	        Service:   "myservice",
//	        Operation: "GetWidget",
//	        Params:    map[string]string{"WidgetName": "foo"},
//	    }
//	    reqCtx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}
//	    resp, err := p.HandleRequest(reqCtx, req)
//	    require.NoError(t, err)
//	    assert.Equal(t, 200, resp.StatusCode)
//	}
//
// For integration tests use [NewServer] + httptest.NewServer:
//
//	registry := substrate.NewPluginRegistry()
//	registry.Register(&MyPlugin{})
//	srv := substrate.NewServer(cfg, registry, store, state, tc, logger)
//	ts := httptest.NewServer(srv)
//	defer ts.Close()
//
// See examples/custom_plugin/main.go for a complete minimal plugin example.
//
// # Canonical Examples
//
// Refer to the following plugins as idiomatic implementations:
//
//   - [IAMPlugin] — query-protocol, complex state, policy evaluation.
//   - [S3Plugin] — REST-protocol, binary bodies, path-based routing.
//   - [DynamoDBPlugin] — JSON-protocol, expression evaluation, GSI/LSI.
//   - [SQSPlugin] — queue-based state, message visibility, query-protocol.
package substrate
