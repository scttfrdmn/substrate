package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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
	// When non-nil, the server enriches RequestContext with the caller's
	// account and principal and verifies the SigV4 signature.
	Credentials *CredentialRegistry

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
	s.router = s.buildRouter()
	return s
}

// Start binds the listener and begins accepting requests. It blocks until ctx
// is canceled or an unrecoverable error occurs. A nil error is returned only
// when shutdown is initiated via ctx cancellation or [Server.Stop].
func (s *Server) Start(ctx context.Context) error {
	readTimeout, err := time.ParseDuration(s.config.Server.ReadTimeout)
	if err != nil {
		return fmt.Errorf("parse read_timeout: %w", err)
	}
	writeTimeout, err := time.ParseDuration(s.config.Server.WriteTimeout)
	if err != nil {
		return fmt.Errorf("parse write_timeout: %w", err)
	}

	s.httpSrv = &http.Server{
		Addr:         s.config.Server.Address,
		Handler:      s.router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	s.logger.Info("substrate server starting", "address", s.config.Server.Address)

	// Shutdown when context is canceled.
	go func() {
		<-ctx.Done()
		if stopErr := s.Stop(context.Background()); stopErr != nil {
			s.logger.Error("graceful shutdown error", "err", stopErr)
		}
	}()

	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen and serve: %w", err)
	}
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

	s.httpSrv = &http.Server{
		Handler:      s.router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	s.logger.Info("substrate server starting", "address", ln.Addr().String())

	go func() {
		<-ctx.Done()
		if stopErr := s.Stop(context.Background()); stopErr != nil {
			s.logger.Error("graceful shutdown error", "err", stopErr)
		}
	}()

	if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// Stop initiates graceful shutdown, waiting up to the configured
// ShutdownTimeout for active connections to finish.
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
	return s.httpSrv.Shutdown(shutCtx)
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

// handleStateReset wipes all emulator state. Only available when the
// [StateManager] implements [SnapshotableStateManager]. Returns 501 otherwise.
// Primarily used by [TestServer.ResetState] between test cases.
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
	if _, err := w.Write(body); err != nil {
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
	if _, err := w.Write(body); err != nil {
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
	if _, err := w.Write(body); err != nil {
		s.logger.Warn("failed to write ready response", "err", err)
	}
}

// handleAWSRequest is the single catch-all handler for all AWS API requests.
// Pipeline:
//  1. Parse → AWSRequest + RequestContext
//     1.5. Credential resolution (when Credentials != nil)
//     1.6. SigV4 signature verification (when Credentials != nil)
//  2. auth.CheckAccess()        → 403 AccessDeniedException
//  3. quota.CheckQuota()        → 429 ThrottlingException
//  4. consistency.CheckRead()   → 409 InconsistentStateException
//  5. registry.RouteRequest()   (plugin dispatch)
//  6. cost := costs.CostForRequest(req)
//  7. if success && mutating: consistency.RecordWrite(req)
//  8. store.RecordRequest(…, cost, routeErr)
//  9. write response
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

	// Assign body. For S3 and other REST-protocol services rawBody holds the
	// full binary payload. For query-protocol services (IAM, STS) ParseAWSRequest
	// consumes the form body; we rebuild req.Body as JSON from the parsed params.
	req.Body = rawBody
	if len(req.Params) > 0 && (req.Service == "iam" || req.Service == "sts" ||
		req.Service == "sqs" || req.Service == "sns" || req.Service == "monitoring") {
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
			}, r)
			return
		}
	}

	// Step 1.5: credential resolution — enrich RequestContext with account and
	// principal derived from the registry.
	if s.opts.Credentials != nil {
		accessKey := extractAccessKeyFromAuth(r.Header.Get("Authorization"))
		if entry, ok := s.opts.Credentials.Lookup(accessKey); ok {
			reqCtx.AccountID = entry.AccountID
			reqCtx.Principal = &Principal{
				ARN:  buildCallerARN(entry.AccountID, accessKey),
				Type: "IAMUser",
			}
		}
	}

	// Step 1.6: SigV4 signature verification.
	if s.opts.Credentials != nil {
		if sigErr := VerifySigV4(r, rawBody, s.opts.Credentials); sigErr != nil {
			s.writeError(w, sigErr, r)
			return
		}
	}

	// Step 2: cross-service IAM authorization.
	if s.opts.Auth != nil {
		if authErr := s.opts.Auth.CheckAccess(reqCtx, req); authErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(ctx, reqCtx, req, nil, duration, 0, authErr); recordErr != nil {
				s.logger.Warn("failed to record auth event", "err", recordErr)
			}
			s.writeError(w, authErr, r)
			return
		}
	}

	// Step 3: quota enforcement.
	if s.opts.Quota != nil {
		if quotaErr := s.opts.Quota.CheckQuota(reqCtx, req); quotaErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(ctx, reqCtx, req, nil, duration, 0, quotaErr); recordErr != nil {
				s.logger.Warn("failed to record quota event", "err", recordErr)
			}
			if s.opts.Metrics != nil {
				s.opts.Metrics.RecordQuotaHit(req.Service, req.Operation)
				s.opts.Metrics.RecordRequest(req.Service, req.Operation, true, "ThrottlingException")
			}
			s.writeError(w, quotaErr, r)
			return
		}
	}

	// Step 4: consistency check for reads.
	if s.opts.Consistency != nil {
		if consErr := s.opts.Consistency.CheckRead(reqCtx, req); consErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(ctx, reqCtx, req, nil, duration, 0, consErr); recordErr != nil {
				s.logger.Warn("failed to record consistency event", "err", recordErr)
			}
			if s.opts.Metrics != nil {
				s.opts.Metrics.RecordConsistencyDelay(req.Service)
				s.opts.Metrics.RecordRequest(req.Service, req.Operation, true, "InconsistentStateException")
			}
			s.writeError(w, consErr, r)
			return
		}
	}

	// Step 4.5: fault injection.
	if s.opts.Fault != nil {
		faultErr, faultDelay := s.opts.Fault.InjectFault(reqCtx, req)
		if faultDelay > 0 {
			time.Sleep(faultDelay)
		}
		if faultErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(ctx, reqCtx, req, nil, duration, 0, faultErr); recordErr != nil {
				s.logger.Warn("failed to record fault event", "err", recordErr)
			}
			s.writeError(w, faultErr, r)
			return
		}
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
	if recordErr := s.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr); recordErr != nil {
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

	if routeErr != nil {
		RecordSpanError(reqSpan, routeErr)
		s.writeError(w, routeErr, r)
		return
	}

	s.writeResponse(w, resp)
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
		if _, err := w.Write(resp.Body); err != nil {
			s.logger.Warn("failed to write response body", "err", err)
		}
	}
}

// writeError converts err into an AWS-style error response. For JSON-protocol
// services (identified by Content-Type application/x-amz-json-1.0 on the
// incoming request), the error is serialized as JSON; otherwise XML is used.
func (s *Server) writeError(w http.ResponseWriter, err error, r *http.Request) {
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

	// JSON protocol: any application/x-amz-json variant (1.0 or 1.1).
	ct := ""
	if r != nil {
		ct = r.Header.Get("Content-Type")
	}
	if strings.HasPrefix(ct, "application/x-amz-json") {
		w.Header().Set("Content-Type", ct) // mirror 1.0 or 1.1 back to caller
		w.WriteHeader(awsErr.HTTPStatus)
		body, _ := json.Marshal(map[string]string{"Code": awsErr.Code, "Message": awsErr.Message})
		_, _ = w.Write(body)
		return
	}

	body, marshalErr := xml.Marshal(struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}{
		Error: struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}{
			Code:    awsErr.Code,
			Message: awsErr.Message,
		},
	})
	if marshalErr != nil {
		http.Error(w, awsErr.Message, awsErr.HTTPStatus)
		return
	}

	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(awsErr.HTTPStatus)
	if _, writeErr := w.Write(body); writeErr != nil {
		s.logger.Warn("failed to write error body", "err", writeErr)
	}
}
