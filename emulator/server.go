package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ServerOptions holds optional middleware controllers for the server pipeline.
// A nil field disables that feature.
type ServerOptions struct {
	// Quota enforces per-service and per-operation rate limits. Nil disables
	// quota checking.
	Quota *QuotaController

	// Consistency simulates eventual-consistency propagation delays. Nil
	// disables consistency simulation.
	Consistency *ConsistencyController

	// Costs computes per-request cost estimates recorded in the event store.
	// Nil means cost=0 for every request.
	Costs *CostController

	// Credentials resolves access key IDs to account IDs and secrets.
	// When non-nil, a request whose access key the registry holds is attributed
	// to that key's account. Signature verification is a separate decision; see
	// VerifySignatures.
	Credentials *CredentialRegistry

	// VerifySignatures enforces SigV4 on every request carrying an
	// Authorization header, refusing an access key Credentials does not hold
	// with InvalidClientTokenId 403.
	//
	// This is separate from Credentials because the two are unrelated questions
	// — "which account is this key's" and "is this signature valid" — and
	// answering only the first is the combination a multi-account test needs
	// (#630). While one field meant both, wiring a registry in order to
	// attribute accounts also refused every credential substrate documents.
	//
	// True with a nil Credentials has no key material to check against, so
	// [NewServer] logs a warning and downgrades it to off rather than returning
	// a server that refuses every signed request. Refusing at construction
	// would be the better answer, but NewServer returns no error and changing
	// its signature is out of proportion to the mistake.
	VerifySignatures bool

	// Auth enforces IAM policy decisions across all services.
	// When non-nil, every request is checked against the caller's attached
	// policies before being routed to a plugin.
	Auth *AuthController

	// Metrics collects and exposes Prometheus-format operational metrics.
	// When non-nil and cfg.Metrics.Enabled is true, the /metrics endpoint is
	// registered and request counters are updated on every request.
	Metrics *MetricsCollector

	// Tracer emits OpenTelemetry distributed traces for each request. Nil
	// disables tracing.
	Tracer *Tracer

	// Fault injects configurable errors and latency into the request pipeline.
	// When non-nil and FaultConfig.Enabled is true, faults are evaluated after
	// the consistency check (Step 4) and before plugin dispatch (Step 5).
	Fault *FaultController

	// DisableKeepAlives closes every connection after one response, so no client
	// can pool a connection to this server and reuse it later.
	//
	// Off by default: a real emulator run wants keep-alives, and an AWS SDK opens
	// one connection per client and reuses it for every call. It exists for
	// [StartTestServer], where the hazard is the other way round (#798): a test
	// server is torn down at the end of its test, ports are recycled inside one
	// `go test` process, and a connection pooled against a dead server fails on
	// *read* — after the request was written — which surfaces as a bare
	// "connection reset by peer" in whichever later test drew the recycled port,
	// not in the test that leaked it. Closing server-side is what makes that
	// impossible for all 149 call sites at once rather than per client.
	DisableKeepAlives bool
}

// Server is the Substrate HTTP server. It receives AWS SDK requests, parses
// them, dispatches them to the appropriate [Plugin] via a [PluginRegistry],
// records events in an [EventStore], and writes the HTTP response.
type Server struct {
	config   Config
	router   *chi.Mux
	registry *PluginRegistry
	store    *EventStore
	state    StateManager
	tc       *TimeController
	logger   Logger
	opts     ServerOptions
	httpSrv  *http.Server
}

// NewServer creates a Server wired to the provided dependencies. Start must be
// called to begin accepting connections.
func NewServer(
	cfg Config,
	registry *PluginRegistry,
	store *EventStore,
	state StateManager,
	tc *TimeController,
	logger Logger,
	opts ...ServerOptions,
) *Server {
	s := &Server{
		config:   cfg,
		registry: registry,
		store:    store,
		state:    state,
		tc:       tc,
		logger:   logger,
	}
	if len(opts) > 0 {
		s.opts = opts[0]
	}
	if s.opts.VerifySignatures && s.opts.Credentials == nil {
		// Nothing to verify against: every signed request would be refused with
		// InvalidClientTokenId. Downgrade rather than serve that, and say so —
		// see [ServerOptions.VerifySignatures] for why this is not an error.
		s.opts.VerifySignatures = false
		if s.logger != nil {
			s.logger.Warn("verify_signatures is set with no credential registry; " +
				"signature verification is off")
		}
	}
	s.router = s.buildRouter()
	return s
}

// Start binds the listener and begins accepting requests. It blocks until ctx
// is canceled or an unrecoverable error occurs. A nil error is returned only
// when shutdown is initiated via ctx cancellation or [Server.Stop].
//
// All three server timeouts are parsed up front, so an unusable duration is a
// startup error rather than a surprise later. shutdown_timeout in particular is only
// read by Stop, and a Stop that failed on it returned before closing the listener —
// which left Start blocked forever on a canceled context, and now with the flush wait
// would have blocked there too.
func (s *Server) Start(ctx context.Context) error {
	readTimeout, err := time.ParseDuration(s.config.Server.ReadTimeout)
	if err != nil {
		return fmt.Errorf("parse read_timeout: %w", err)
	}
	writeTimeout, err := time.ParseDuration(s.config.Server.WriteTimeout)
	if err != nil {
		return fmt.Errorf("parse write_timeout: %w", err)
	}
	if _, err := time.ParseDuration(s.config.Server.ShutdownTimeout); err != nil {
		return fmt.Errorf("parse shutdown_timeout: %w", err)
	}

	s.httpSrv = &http.Server{
		Addr:         s.config.Server.Address,
		Handler:      s.router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	if s.opts.DisableKeepAlives {
		s.httpSrv.SetKeepAlivesEnabled(false)
	}

	s.logger.Info("substrate server starting", "address", s.config.Server.Address)

	done := s.stopOnCancel(ctx)

	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen and serve: %w", err)
	}
	s.awaitStop(ctx, done)
	return nil
}

// Serve accepts connections on the provided listener. Unlike [Server.Start],
// it does not create a new listener, eliminating the TOCTOU race between
// port reservation and binding. It blocks until ctx is canceled or an
// unrecoverable error occurs.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	readTimeout, err := time.ParseDuration(s.config.Server.ReadTimeout)
	if err != nil {
		return fmt.Errorf("parse read_timeout: %w", err)
	}
	writeTimeout, err := time.ParseDuration(s.config.Server.WriteTimeout)
	if err != nil {
		return fmt.Errorf("parse write_timeout: %w", err)
	}
	if _, err := time.ParseDuration(s.config.Server.ShutdownTimeout); err != nil {
		return fmt.Errorf("parse shutdown_timeout: %w", err)
	}

	s.httpSrv = &http.Server{
		Handler:      s.router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	if s.opts.DisableKeepAlives {
		s.httpSrv.SetKeepAlivesEnabled(false)
	}

	s.logger.Info("substrate server starting", "address", ln.Addr().String())

	done := s.stopOnCancel(ctx)

	if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("serve: %w", err)
	}
	s.awaitStop(ctx, done)
	return nil
}

// stopOnCancel calls [Server.Stop] when ctx is canceled, returning a channel closed
// once that Stop has returned. Pass it to [Server.awaitStop].
func (s *Server) stopOnCancel(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		if stopErr := s.Stop(context.Background()); stopErr != nil {
			s.logger.Error("graceful shutdown error", "err", stopErr)
		}
	}()
	return done
}

// awaitStop waits for a cancellation-triggered [Server.Stop] to finish before Start
// or Serve returns.
//
// Necessary because http.Server.Shutdown makes Serve return as soon as the listener
// closes, while the rest of Stop — notably the event-store flush (#599) — is still
// running on the goroutine. Without this wait, main returns and the process exits
// first, so a SIGTERM'd server dropped everything recorded since the last automatic
// flush. The unit test for Stop could not catch that: it calls Stop directly.
//
// Bounded by the same ShutdownTimeout Stop honors, so a wedged flush delays exit
// rather than hanging it. Only waits when ctx is what ended the serve loop; a Stop
// called directly by the caller has already completed by the time it returns.
func (s *Server) awaitStop(ctx context.Context, done <-chan struct{}) {
	if ctx.Err() == nil {
		return
	}
	// Start and Serve both parse this before reaching here, so the error cannot
	// happen; the fallback keeps a wedge from becoming an unbounded wait if that ever
	// stops being true.
	timeout, err := time.ParseDuration(s.config.Server.ShutdownTimeout)
	if err != nil {
		timeout = 30 * time.Second
	}
	select {
	case <-done:
	case <-time.After(timeout):
		s.logger.Warn("shutdown did not finish within shutdown_timeout",
			"timeout", timeout)
	}
}

// Stop initiates graceful shutdown, waiting up to the configured
// ShutdownTimeout for active connections to finish, then flushing the event store
// so events recorded since the last automatic flush are persisted rather than lost
// on exit.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	shutdownTimeout, err := time.ParseDuration(s.config.Server.ShutdownTimeout)
	if err != nil {
		return fmt.Errorf("parse shutdown_timeout: %w", err)
	}
	shutCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	s.logger.Info("substrate server shutting down")
	if err := s.httpSrv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	// Persist what the automatic threshold has not reached yet (#599). Without this
	// the events recorded since the last crossing are lost on exit, and with the
	// default max_in_memory of 1000 a short run would persist nothing at all despite
	// a file or sqlite backend being configured. Run after Shutdown returns, so no
	// request is still recording. A failure here loses recorded history rather than
	// breaking a request, so unlike an automatic flush it is reported to the caller.
	//
	// On its own deadline rather than shutCtx, which Shutdown may have already
	// consumed — a flush that inherited an expired context would fail every time the
	// drain used its full budget, which is exactly when there is most to write. And
	// detached from ctx's cancellation, because the usual caller is the signal
	// handler, whose ctx is canceled by definition.
	if s.store != nil {
		flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer flushCancel()
		if err := s.store.Flush(flushCtx); err != nil {
			return fmt.Errorf("flush event store: %w", err)
		}
	}
	return nil
}

// ServeHTTP implements [http.Handler], allowing the server to be used directly
// in httptest scenarios without calling [Server.Start].
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// buildRouter constructs the chi router with health/ready endpoints and a
// catch-all AWS request handler.
func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	// CORS for browser-based AWS SDK clients (opt-in). The AWS SDK issues a
	// preflight OPTIONS before non-simple requests and sends many x-amz-*/
	// amz-sdk-* headers; reflect them and expose the request-id/error-type
	// headers the SDK reads. Off by default — non-browser callers need no CORS.
	if s.config.Server.CORS.Enabled {
		origins := s.config.Server.CORS.AllowedOrigins
		if len(origins) == 0 {
			origins = []string{"*"}
		}
		r.Use(cors.Handler(cors.Options{
			AllowedOrigins:   origins,
			AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"},
			AllowedHeaders:   []string{"*"},
			ExposedHeaders:   []string{"x-amzn-RequestId", "x-amz-request-id", "x-amzn-ErrorType", "x-amz-id-2"},
			AllowCredentials: false,
			MaxAge:           300,
		}))
	}

	healthPath := s.config.Server.HealthPath
	if healthPath == "" {
		healthPath = "/health"
	}
	readyPath := s.config.Server.ReadyPath
	if readyPath == "" {
		readyPath = "/ready"
	}
	r.Get(healthPath, s.handleHealth)
	r.Get(readyPath, s.handleReady)

	r.Get("/_localstack/health", s.handleLocalStackHealth)
	r.Get("/_localstack/info", s.handleLocalStackHealth)
	r.Post("/v1/state/reset", s.handleStateReset)
	r.Post("/_substrate/reset", s.handleStateReset)
	r.Get("/v1/emails", s.handleEmails)

	r.Get("/v1/control/time", s.handleGetTime)
	r.Post("/v1/control/time", s.handleSetTime)
	r.Post("/v1/control/scale", s.handleSetScale)

	r.Post("/v1/redshift-data/results", s.handleRedshiftDataSeedResult)
	r.Delete("/v1/redshift-data/results", s.handleRedshiftDataClearResults)
	r.Post("/v1/redshift-data/status", s.handleRedshiftDataSetStatus)

	r.Post("/v1/athena/results", s.handleAthenaSeedResult)
	r.Delete("/v1/athena/results", s.handleAthenaClearResults)

	r.Post("/v1/timestream-query/results", s.handleTimestreamSeedResult)
	r.Delete("/v1/timestream-query/results", s.handleTimestreamClearResults)

	r.Post("/v1/sagemaker/training-job-status", s.handleSageMakerSeedTrainingJobStatus)
	r.Delete("/v1/sagemaker/training-job-status", s.handleSageMakerClearTrainingJobStatus)

	r.Post("/v1/ssm/command-invocation", s.handleSSMSeedCommandInvocation)
	r.Delete("/v1/ssm/command-invocation", s.handleSSMClearCommandInvocation)

	// IAM control-plane endpoints (#747).
	r.Post("/v1/iam/slr-deletion-status", s.handleIAMSeedSLRDeletionStatus)
	r.Delete("/v1/iam/slr-deletion-status", s.handleIAMClearSLRDeletionStatus)

	// Organizations control-plane endpoints (#578).
	r.Post("/v1/organizations/feature-set", s.handleOrganizationsSeedFeatureSet)
	r.Delete("/v1/organizations/feature-set", s.handleOrganizationsClearFeatureSet)
	r.Post("/v1/organizations/create-account-failure", s.handleOrganizationsSeedCreateAccountFailure)
	r.Delete("/v1/organizations/create-account-failure", s.handleOrganizationsClearCreateAccountFailure)

	// Account Management control-plane endpoints (#629).
	r.Post("/v1/account/region-opt-status", s.handleAccountSeedRegionOptStatus)
	r.Delete("/v1/account/region-opt-status", s.handleAccountClearRegionOptStatus)

	// AWS Config control-plane endpoints (#580).
	r.Post("/v1/config/recorder-status", s.handleConfigSeedRecorderStatus)
	r.Delete("/v1/config/recorder-status", s.handleConfigClearRecorderStatus)
	r.Post("/v1/config/delivery-status", s.handleConfigSeedDeliveryStatus)
	r.Delete("/v1/config/delivery-status", s.handleConfigClearDeliveryStatus)
	r.Post("/v1/config/delivery-policy", s.handleConfigSeedDeliveryPolicy)
	r.Delete("/v1/config/delivery-policy", s.handleConfigClearDeliveryPolicy)
	r.Post("/v1/config/rule-compliance/{name}", s.handleConfigSeedRuleCompliance)
	r.Delete("/v1/config/rule-compliance/{name}", s.handleConfigClearRuleCompliance)
	r.Delete("/v1/config/rule-compliance", s.handleConfigClearRuleCompliance)
	r.Post("/v1/config/pack-status/{name}", s.handleConfigSeedPackStatus)
	r.Delete("/v1/config/pack-status/{name}", s.handleConfigClearPackStatus)
	r.Delete("/v1/config/pack-status", s.handleConfigClearPackStatus)
	r.Post("/v1/config/pack-compliance/{name}", s.handleConfigSeedPackCompliance)
	r.Delete("/v1/config/pack-compliance/{name}", s.handleConfigClearPackCompliance)
	r.Delete("/v1/config/pack-compliance", s.handleConfigClearPackCompliance)

	// Lambda control-plane endpoints (#393).
	r.Post("/v1/lambda/invoke-error", s.handleLambdaSeedInvokeError)
	r.Delete("/v1/lambda/invoke-error", s.handleLambdaClearInvokeError)

	// SQS control-plane endpoints (#413).
	r.Post("/v1/sqs/consistency", s.handleSQSSeedConsistency)
	r.Delete("/v1/sqs/consistency", s.handleSQSClearConsistency)

	// EC2 Fleet partial-fulfillment control-plane endpoints (#387).
	r.Post("/v1/ec2/fleet-shortfall", s.handleEC2SeedFleetShortfall)
	r.Delete("/v1/ec2/fleet-shortfall", s.handleEC2ClearFleetShortfall)

	// EC2 snapshot progression control-plane endpoints (#715).
	r.Post("/v1/ec2/snapshot-status", s.handleEC2SeedSnapshotStatus)
	r.Delete("/v1/ec2/snapshot-status", s.handleEC2ClearSnapshotStatus)

	// EC2 spot-placement-score control-plane endpoints (#892).
	r.Post("/v1/ec2/spot-placement-scores", s.handleEC2SeedSpotPlacementScore)
	r.Delete("/v1/ec2/spot-placement-scores", s.handleEC2ClearSpotPlacementScore)

	// EC2 Capacity Reservation outcome control-plane endpoints (#891).
	r.Post("/v1/ec2/capacity-reservation-outcomes", s.handleEC2SeedCapacityReservationOutcome)
	r.Delete("/v1/ec2/capacity-reservation-outcomes", s.handleEC2ClearCapacityReservationOutcome)

	// ELB account-limit control-plane endpoints (#885).
	r.Post("/v1/elb/account-limits", s.handleELBSeedAccountLimit)
	r.Delete("/v1/elb/account-limits", s.handleELBClearAccountLimit)

	// spore.host spawn task-completion control-plane endpoints (#360).
	r.Post("/v1/spawn/task-completion", s.handleSpawnSeedTaskCompletion)
	r.Delete("/v1/spawn/task-completion", s.handleSpawnClearTaskCompletion)

	// Bedrock Runtime control-plane endpoints.
	r.Post("/v1/bedrock-runtime/responses", s.handleBedrockRuntimeSeedResponse)
	r.Delete("/v1/bedrock-runtime/responses", s.handleBedrockRuntimeClearResponses)
	r.Post("/v1/bedrock/model-invocation-job-status", s.handleBedrockRuntimeSeedJobStatus)
	r.Delete("/v1/bedrock/model-invocation-job-status", s.handleBedrockRuntimeClearJobStatus)

	// Fault injection control-plane endpoints.
	r.Post("/v1/fault/rules", s.handleFaultSetRules)
	r.Delete("/v1/fault/rules", s.handleFaultClearRules)
	r.Get("/v1/fault/rules", s.handleFaultGetRules)

	// Pricing control-plane endpoints.
	r.Post("/v1/pricing/refresh", s.handlePricingRefresh)
	r.Get("/v1/pricing", s.handlePricingGet)
	r.Get("/v1/pricing/lookup", s.handlePricingLookup)
	r.Post("/v1/pricing/discounts", s.handlePricingSetDiscounts)
	r.Get("/v1/pricing/discounts", s.handlePricingGetDiscounts)
	r.Delete("/v1/pricing/discounts", s.handlePricingClearDiscounts)
	r.Post("/v1/pricing/credits", s.handlePricingAddCredit)
	r.Get("/v1/pricing/credits", s.handlePricingListCredits)
	r.Delete("/v1/pricing/credits/{id}", s.handlePricingRemoveCredit)
	r.Post("/v1/pricing/query-failures", s.handlePricingSeedQueryFailure)
	r.Delete("/v1/pricing/query-failures", s.handlePricingClearQueryFailures)

	// Health control-plane endpoints.
	r.Post("/v1/health/events", s.handleHealthSeedEvents)
	r.Delete("/v1/health/events", s.handleHealthClearEvents)

	// S3 control-plane endpoints.
	r.Post("/v1/s3/presign", s.handleS3Presign)
	r.Post("/v1/s3/conditional-conflict", s.handleS3SeedConditionalConflict)
	r.Delete("/v1/s3/conditional-conflict", s.handleS3ClearConditionalConflict)

	r.Get("/ui", s.handleDebugUI)
	r.Get("/v1/debug/events", s.handleDebugEvents)
	r.Get("/v1/debug/events/{seq}/state", s.handleDebugStateAt)
	r.Get("/v1/debug/state/diff", s.handleDebugStateDiff)
	r.Get("/v1/debug/costs", s.handleDebugCosts)
	r.Get("/v1/debug/export", s.handleDebugExport)

	if s.opts.Metrics != nil && s.config.Metrics.Enabled {
		metricsPath := s.config.Metrics.Path
		if metricsPath == "" {
			metricsPath = "/metrics"
		}
		r.Get(metricsPath, s.handleMetrics)
	}

	r.HandleFunc("/*", s.handleAWSRequest)
	return r
}

// handleLocalStackHealth returns a LocalStack-compatible health response listing
// all registered plugins as available services. This allows tools that poll
// /_localstack/health (e.g. Prism) to use Substrate as a drop-in replacement.
func (s *Server) handleLocalStackHealth(w http.ResponseWriter, _ *http.Request) {
	names := s.registry.Names()
	services := make(map[string]string, len(names))
	for _, name := range names {
		services[name] = "available"
	}
	resp := struct {
		Services map[string]string `json:"services"`
		Version  string            `json:"version"`
	}{Services: services, Version: Version}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Warn("failed to write localstack health response", "err", err)
	}
}

// handleStateReset wipes emulator state: the [StateManager]'s contents, any
// mutable state the plugins keep on themselves ([ResettablePlugin]), and any active
// fault rules. Only available when the [StateManager] implements
// [SnapshotableStateManager]. Returns 501 otherwise. Primarily used by
// [TestServer.ResetState] between test cases.
//
// The plugin half is the same reset a replay performs (#886) and is here for the
// same reason: a minting counter that survived the reset made the identifiers a
// test case observes depend on how many test cases ran before it. It now also
// releases what a plugin holds outside its own struct — S3's object payloads
// (#902), Lambda's event-source-mapping pollers and warm containers, and RDS's
// Postgres containers (#903).
//
// The one exception is a filesystem a caller injected into the S3 plugin as
// Options["filesystem"], which substrate does not own and leaves alone; it cannot
// be injected through this server at all, only by an in-process embedder that
// builds the registry itself.
func (s *Server) handleStateReset(w http.ResponseWriter, r *http.Request) {
	sm, ok := s.state.(SnapshotableStateManager)
	if !ok {
		http.Error(w, `{"error":"state manager does not support reset"}`, http.StatusNotImplemented)
		return
	}
	if err := sm.Reset(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	// Plugins after the state manager, matching [ReplayEngine.resetState]: a plugin's
	// reset may write its start-of-run state into the state manager, and clearing the
	// store afterwards would erase it.
	if s.registry != nil {
		if err := s.registry.ResetPlugins(r.Context()); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	// Also clear any active fault injection rules so tests start clean.
	if s.opts.Fault != nil {
		s.opts.Fault.UpdateConfig(FaultConfig{Enabled: false})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleEmails returns all captured SESv2 outbound emails as JSON. It is
// intended for test assertions and accepts optional ?to= and ?subject= query
// parameters for substring filtering.
func (s *Server) handleEmails(w http.ResponseWriter, r *http.Request) {
	filterTo := r.URL.Query().Get("to")
	filterSubject := r.URL.Query().Get("subject")

	keys, err := s.state.List(r.Context(), sesv2Namespace, "captured_email:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	var emails []SESv2CapturedEmail
	for _, k := range keys {
		data, getErr := s.state.Get(r.Context(), sesv2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var email SESv2CapturedEmail
		if json.Unmarshal(data, &email) != nil {
			continue
		}
		// Apply to filter.
		if filterTo != "" {
			matched := false
			for _, addr := range email.To {
				if strings.Contains(addr, filterTo) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		// Apply subject filter.
		if filterSubject != "" && !strings.Contains(email.Subject, filterSubject) {
			continue
		}
		emails = append(emails, email)
	}
	if emails == nil {
		emails = []SESv2CapturedEmail{}
	}

	result := map[string]interface{}{
		"Emails": emails,
		"Count":  len(emails),
	}
	body, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil { // nosemgrep
		s.logger.Warn("failed to write emails response", "err", err)
	}
}

// handleMetrics writes a Prometheus text-format v0.0.4 metrics snapshot.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	s.opts.Metrics.Render(w, s.store)
}

// handleHealth returns a JSON liveness response. It always returns 200.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body, _ := json.Marshal(map[string]string{"status": "ok", "version": Version})
	if _, err := w.Write(body); err != nil { // nosemgrep
		s.logger.Warn("failed to write health response", "err", err)
	}
}

// handleReady returns a JSON readiness response listing registered plugins.
// It always returns 200.
func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body, _ := json.Marshal(map[string]interface{}{
		"status":  "ok",
		"plugins": s.registry.Names(),
	})
	if _, err := w.Write(body); err != nil { // nosemgrep
		s.logger.Warn("failed to write ready response", "err", err)
	}
}

// handleAWSRequest is the single catch-all handler for all AWS API requests.
// Pipeline:
//  1. Parse → AWSRequest + RequestContext
//     1.5. Credential resolution — account from Credentials (when non-nil),
//     principal from state (always; see [resolvePrincipal])
//     1.6. SigV4 signature verification (when VerifySignatures is set)
//     1.7. stateBefore := recordedStateHash() (only when the store records hashes)
//  2. auth.CheckAccess()        → 403 AccessDenied / AccessDeniedException
//     (per the service's wire protocol; see accessDeniedCodeFor)
//  3. quota.CheckQuota()        → 429 ThrottlingException, or 503 SlowDown for S3
//  4. consistency.CheckRead()   → 409 InconsistentStateException
//     4.5. fault injection      → the armed rule's own code, or a latency delay
//  5. registry.RouteRequest()   (plugin dispatch)
//  6. cost := costs.CostForRequest(req)
//  7. if success && mutating: consistency.RecordWrite(req)
//  8. store.RecordRequest(…, cost, routeErr, WithStateHashes(before, after))
//  9. write response
//
// Steps 2 through 4.5 are the four controllers of [prePluginGates], which
// [ReplayEngine.replayEvent] runs as well: a refusal before plugin dispatch is part
// of what a recorded run means, so replaying it has to reach the same decisions
// (#833). Everything above step 2 reads the live *http.Request and is skipped on
// replay, with the reason for each recorded on [ReplayEngine.replayEvent].
func (s *Server) handleAWSRequest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Pre-read the body so it is available for both ParseAWSRequest (which
	// may call r.ParseForm for query-protocol services) and SigV4 verification.
	ctx := r.Context()
	var rawBody []byte
	if r.Body != nil {
		var readErr error
		rawBody, readErr = io.ReadAll(r.Body)
		if readErr != nil {
			s.logger.Error("failed to read request body", "err", readErr)
		}
		r.Body = io.NopCloser(bytes.NewReader(rawBody))
	}

	req, reqCtx, parseErr := ParseAWSRequest(r)
	if parseErr != nil {
		s.logger.Error("failed to parse AWS request", "err", parseErr)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Step 1.1: refuse a request that declares RPC v2 CBOR and also carries a target
	// header. The specification requires a server to reject it, and substrate has a
	// concrete reason to: it routes on X-Amz-Target before anything else, so honoring
	// such a request would mean invoking whatever the target named while answering in
	// the serialization the path asked for — silently ignoring the operation the client
	// actually addressed. Refusing is the only answer that cannot be wrong.
	//
	// The code is substrate's choice: the specification mandates the rejection without
	// naming a code, so this is SerializationException, which is what AWS returns for a
	// framing-level protocol violation and what both reference clients classify as a
	// non-retryable client error (#757).
	if rpcV2CBORTargetConflict(r) {
		s.writeError(w, &AWSError{
			Code:       "SerializationException",
			Message:    "a request declaring Smithy-Protocol: rpc-v2-cbor must not carry an X-Amz-Target header",
			HTTPStatus: http.StatusBadRequest,
		}, r, req.Service)
		return
	}

	// [ParseAWSRequest] is pure and has no configuration to read, so it fills in
	// [defaultAccountID]; the configured account, if any, is the server's answer.
	// Step 1.5 below may still override it from a credential registry or an STS
	// session, both of which know more than a config file does.
	if s.config.Account.Default != "" {
		reqCtx.AccountID = s.config.Account.Default
	}

	// Assign body. For S3 and other REST-protocol services rawBody holds the
	// full binary payload. For query-protocol services (IAM, STS) ParseAWSRequest
	// consumes the form body; we rebuild req.Body as JSON from the parsed params.
	//
	// "monitoring" left this list with #757. It was a no-op — a CBOR or JSON-RPC
	// CloudWatch request has no form body and no query string, so req.Params is empty
	// and the guard above never fires — but it said the opposite of what is now true:
	// CloudWatch reads its request body directly on those two protocols, and a rule
	// that would overwrite the body with a re-encoding of the parsed form is exactly
	// the thing that must not be here when it does.
	req.Body = rawBody
	if len(req.Params) > 0 && (req.Service == "iam" || req.Service == "sts" ||
		req.Service == "sqs" || req.Service == "sns") {
		if jsonBody, jsonErr := json.Marshal(req.Params); jsonErr == nil {
			req.Body = jsonBody
		}
	}

	// Start tracing span for this request.
	var reqSpan trace.Span
	if s.opts.Tracer != nil {
		ctx, reqSpan = s.opts.Tracer.StartRequest(ctx, req.Service, req.Operation)
		reqSpan.SetAttributes(
			attribute.String("aws.region", reqCtx.Region),
			attribute.String("aws.account_id", reqCtx.AccountID),
		)
		defer reqSpan.End()
	}

	// Step 1.4: region validation — reject requests targeting disallowed regions.
	if len(s.config.Region.Allowed) > 0 && reqCtx.Region != "" {
		allowed := false
		for _, r2 := range s.config.Region.Allowed {
			if r2 == reqCtx.Region {
				allowed = true
				break
			}
		}
		if !allowed {
			s.writeError(w, &AWSError{
				Code:       "InvalidClientTokenId",
				Message:    fmt.Sprintf("region %q is not in the allowed list", reqCtx.Region),
				HTTPStatus: http.StatusBadRequest,
			}, r, req.Service)
			return
		}
	}

	// Step 1.5: credential resolution — enrich RequestContext with the caller's
	// account and principal.
	//
	// The two are resolved from different places on purpose. The account comes
	// from the registry. The *principal* comes from state, because only IAM's own
	// records map an access key to the entity holding it. See [resolvePrincipal]
	// and #411.
	if accessKey := extractAccessKeyFromAuth(r.Header.Get("Authorization")); accessKey != "" {
		registryHit := false
		if s.opts.Credentials != nil {
			if entry, ok := s.opts.Credentials.Lookup(accessKey); ok {
				reqCtx.AccountID = entry.AccountID
				registryHit = true
			}
		}
		if principal, sessionAccount := resolvePrincipal(ctx, s.state, reqCtx.AccountID, accessKey); principal != nil {
			reqCtx.Principal = principal
			// An STS session names the account it was issued for. Adopt it unless
			// the registry already spoke: the session record is the only thing that
			// knows which account a temporary credential belongs to, and it may
			// differ from the configured default — an AssumeRole across accounts is
			// the case that matters.
			if sessionAccount != "" && !registryHit {
				reqCtx.AccountID = sessionAccount
			}
		} else if registryHit && s.opts.VerifySignatures {
			// A registered key with no IAM entity behind it. The ARN names the key
			// rather than a user, so it resolves to no policies and authorizes
			// nothing — which is the pre-#411 behavior every existing caller of a
			// credential-wired server relies on, and what GetCallerIdentity reports.
			//
			// Gated on verification, not on the registry hit alone, because this
			// fallback belongs to *verification* rather than to account resolution
			// (#630). A key whose signature was checked has proven it holds the
			// secret, so naming it as a principal is a statement about a caller
			// substrate authenticated. A key merely present in a table has proven
			// nothing, and synthesizing a principal for it would flip
			// GetCallerIdentity's ARN from :root and turn GetUser from a validation
			// error into a NoSuchEntity lookup for every test server that wires a
			// registry only to attribute accounts.
			//
			// UserName is deliberately left empty, so aws:username is absent rather
			// than wrong: the ARN's last segment here is the access key ID, and a
			// substituted ${aws:username} carrying a credential ID into a resource
			// comparison would be a worse answer than no key at all (#745).
			reqCtx.Principal = &Principal{
				ARN:  buildCallerARN(reqCtx.AccountID, accessKey),
				Type: "IAMUser",
			}
		}
	}

	// Step 1.6: SigV4 signature verification.
	if s.opts.VerifySignatures {
		if sigErr := VerifySigV4(r, rawBody, s.opts.Credentials); sigErr != nil {
			s.writeError(w, sigErr, r, req.Service)
			return
		}
	}

	// Step 1.65: presigned URL expiry check.
	// Presigned requests carry X-Amz-Algorithm in the query string; verify
	// X-Amz-Date + X-Amz-Expires have not elapsed.
	if checkPresignedExpiry(r.URL.Query(), s.tc.Now()) {
		s.writeError(w, &AWSError{Code: "AccessDenied", Message: "Request has expired.", HTTPStatus: http.StatusForbidden}, r, req.Service)
		return
	}

	// Step 1.7: capture the state hash the recorded event will carry as its
	// "before" value.
	//
	// It is taken here, before the first step that can refuse, so that every
	// recorded event has a before-hash — a refusal changes nothing, so its before
	// and after hashes are the same value, and that is asserted by taking both
	// rather than by assuming it. Empty when the store is not recording hashes, in
	// which case this is free (#833).
	stateBefore := s.recordedStateHash(ctx)

	// Steps 2, 3, 4 and 4.5: authorization, quota, consistency and fault injection,
	// run through the same [prePluginGates] a replay runs — so a request the recording
	// refused before it reached a plugin is refused on replay too, instead of being
	// re-executed and succeeding (#833).
	gate := s.prePluginGates().check(reqCtx, req)
	if gate.latency > 0 {
		// A latency rule delays the response and lets the request through, so this is
		// deliberately not gated on gate.err. A replay does not sleep here; see
		// [prePluginOutcome.latency].
		time.Sleep(gate.latency)
	}
	if gate.err != nil {
		duration := time.Since(start)
		if recordErr := s.store.RecordRequest(ctx, reqCtx, req, nil, duration, 0, gate.err,
			WithStateHashes(stateBefore, s.recordedStateHash(ctx))); recordErr != nil {
			s.logger.Warn("failed to record refused event", "step", string(gate.step), "err", recordErr)
		}
		// Only two of the four steps have ever had a counter of their own; the switch
		// says which rather than every gate publishing one it does not have.
		if s.opts.Metrics != nil {
			switch gate.step {
			case stepQuota:
				s.opts.Metrics.RecordQuotaHit(req.Service, req.Operation)
				// The code comes from the refusal rather than being spelled here,
				// because it is not one code: S3 answers SlowDown where every other
				// service answers ThrottlingException (#818), and a metric naming a
				// code the caller never saw is worse than one naming none.
				s.opts.Metrics.RecordRequest(req.Service, req.Operation, true, refusalErrorCode(gate.err))
			case stepConsistency:
				s.opts.Metrics.RecordConsistencyDelay(req.Service)
				s.opts.Metrics.RecordRequest(req.Service, req.Operation, true, refusalErrorCode(gate.err))
			case stepAuth, stepFault:
			}
		}
		s.writeError(w, gate.err, r, req.Service)
		return
	}

	// Step 5: route to plugin.
	resp, routeErr := s.registry.RouteRequest(reqCtx, req)

	// Step 6: compute cost.
	var cost float64
	if s.opts.Costs != nil {
		cost = s.opts.Costs.CostForRequest(req)
	}

	// Step 7: record write for consistency tracking on success.
	if routeErr == nil && s.opts.Consistency != nil && isMutating(req.Operation) {
		s.opts.Consistency.RecordWrite(reqCtx, req)
	}

	duration := time.Since(start)

	// Step 8: always record the event regardless of routing outcome.
	if recordErr := s.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr,
		WithStateHashes(stateBefore, s.recordedStateHash(ctx))); recordErr != nil {
		s.logger.Warn("failed to record event", "err", recordErr)
	}

	// Record metrics counters and latency histogram.
	if s.opts.Metrics != nil {
		isError := routeErr != nil
		errorCode := ""
		if isError {
			if awsErr, ok := routeErr.(*AWSError); ok {
				errorCode = awsErr.Code
			}
		}
		s.opts.Metrics.RecordRequest(req.Service, req.Operation, isError, errorCode)
		s.opts.Metrics.RecordLatency(req.Service, req.Operation, duration)
	}

	// Attach cost and stream_id to the tracing span.
	if reqSpan != nil {
		reqSpan.SetAttributes(
			attribute.Float64("substrate.cost", cost),
			attribute.String("substrate.stream_id", streamIDFromContext(reqCtx)),
		)
	}

	if routeErr != nil {
		RecordSpanError(reqSpan, routeErr)
		s.writeError(w, routeErr, r, req.Service)
		return
	}

	s.writeResponse(w, resp)
}

// recordedStateHash returns the hash to record on an event, or "" when the store
// is not recording state hashes.
//
// Gated on the store rather than on the server's own config so that the decision
// is read from the same field the store honors; a hash the store would discard is
// never computed, and a full state snapshot is not taken twice per request for
// every consumer who left [EventStoreConfig.IncludeStateHashes] off (#833).
//
// It shares [stateSnapshotHash] with [ReplayEngine.computeStateHash] because the
// recorded value and the replayed value have to be produced identically to be
// comparable at all.
func (s *Server) recordedStateHash(ctx context.Context) string {
	if s.store == nil || !s.store.RecordsStateHashes() {
		return ""
	}
	return stateSnapshotHash(ctx, s.state)
}

// writeResponse serializes resp into the HTTP response writer.
func (s *Server) writeResponse(w http.ResponseWriter, resp *AWSResponse) {
	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}
	// Set Content-Length if the plugin didn't already supply one, so the AWS SDK
	// can drain the body cleanly and reuse the connection (avoids "failed to
	// close HTTP response body" warnings).
	if _, alreadySet := resp.Headers["Content-Length"]; !alreadySet {
		w.Header().Set("Content-Length", strconv.Itoa(len(resp.Body)))
	}
	w.WriteHeader(resp.StatusCode)
	if len(resp.Body) > 0 {
		if _, err := w.Write(resp.Body); err != nil { // nosemgrep
			s.logger.Warn("failed to write response body", "err", err)
		}
	}
}

// writeError converts err into an AWS-style error response, serialized in the
// wire format the target service's protocol uses. The service name selects the
// protocol (see errorProtocolForRequest) because the incoming Content-Type cannot
// distinguish REST-JSON from a plain JSON body, and picking the wrong shape
// leaves the SDK unable to recover the error code at all (#392). For a service whose
// model declares several protocols the request selects it instead (#757).
func (s *Server) writeError(w http.ResponseWriter, err error, r *http.Request, service string) {
	var awsErr *AWSError
	if asAWSErr, ok := err.(*AWSError); ok {
		awsErr = asAWSErr
	} else {
		awsErr = &AWSError{
			Code:       "InternalFailure",
			Message:    err.Error(),
			HTTPStatus: http.StatusInternalServerError,
		}
	}

	body, respCT, extraHeaders := marshalAWSError(awsErr, errorWireContextFor(service, r))
	if body == nil {
		http.Error(w, awsErr.Message, awsErr.HTTPStatus)
		return
	}

	w.Header().Set("Content-Type", respCT)
	for k, v := range extraHeaders {
		w.Header().Set(k, v)
	}
	w.WriteHeader(awsErr.HTTPStatus)
	if _, writeErr := w.Write(body); writeErr != nil { // nosemgrep
		s.logger.Warn("failed to write error body", "err", writeErr)
	}
}

// checkPresignedExpiry reports whether the request is a presigned URL whose
// expiry has elapsed.  A presigned request is identified by the presence of
// X-Amz-Algorithm in the query string (SigV4 pre-signed URL format).
// If X-Amz-Date or X-Amz-Expires are absent or malformed the function returns
// false (conservative — treat as unexpired).
func checkPresignedExpiry(q url.Values, now time.Time) bool {
	if q.Get("X-Amz-Algorithm") == "" {
		return false
	}
	dateStr := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	if dateStr == "" || expiresStr == "" {
		return false
	}
	t, err := time.Parse("20060102T150405Z", dateStr)
	if err != nil {
		return false
	}
	secs, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil {
		return false
	}
	return now.After(t.Add(time.Duration(secs) * time.Second))
}
