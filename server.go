package substrate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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
// is cancelled or an unrecoverable error occurs. A nil error is returned only
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

	// Shutdown when context is cancelled.
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

// buildRouter constructs the chi router with a single catch-all handler.
func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.HandleFunc("/*", s.handleAWSRequest)
	return r
}

// handleAWSRequest is the single catch-all handler for all AWS API requests.
// Pipeline:
//  1. Parse → AWSRequest + RequestContext
//  2. quota.CheckQuota()        → 429 ThrottlingException
//  3. consistency.CheckRead()   → 409 InconsistentStateException
//  4. registry.RouteRequest()   (plugin dispatch)
//  5. cost := costs.CostForRequest(req)
//  6. if success && mutating: consistency.RecordWrite(req)
//  7. store.RecordRequest(…, cost, routeErr)
//  8. write response
func (s *Server) handleAWSRequest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	req, reqCtx, parseErr := ParseAWSRequest(r)
	if parseErr != nil {
		s.logger.Error("failed to parse AWS request", "err", parseErr)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Read body after parsing (ParseAWSRequest uses r.Form, not body).
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.logger.Error("failed to read request body", "err", err)
		} else {
			req.Body = body
		}
	}

	// For query-protocol services (IAM, STS) the AWS SDK sends
	// application/x-www-form-urlencoded bodies. ParseAWSRequest calls
	// r.ParseForm which consumes the body into req.Params; the body read
	// above therefore yields empty bytes. Rebuild req.Body as JSON from
	// the parsed parameters so plugin handlers can use json.Unmarshal.
	if len(req.Body) == 0 && len(req.Params) > 0 &&
		(req.Service == "iam" || req.Service == "sts") {
		if jsonBody, jsonErr := json.Marshal(req.Params); jsonErr == nil {
			req.Body = jsonBody
		}
	}

	// Step 2: quota enforcement.
	if s.opts.Quota != nil {
		if quotaErr := s.opts.Quota.CheckQuota(reqCtx, req); quotaErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(r.Context(), reqCtx, req, nil, duration, 0, quotaErr); recordErr != nil {
				s.logger.Warn("failed to record quota event", "err", recordErr)
			}
			s.writeError(w, quotaErr)
			return
		}
	}

	// Step 3: consistency check for reads.
	if s.opts.Consistency != nil {
		if consErr := s.opts.Consistency.CheckRead(reqCtx, req); consErr != nil {
			duration := time.Since(start)
			if recordErr := s.store.RecordRequest(r.Context(), reqCtx, req, nil, duration, 0, consErr); recordErr != nil {
				s.logger.Warn("failed to record consistency event", "err", recordErr)
			}
			s.writeError(w, consErr)
			return
		}
	}

	// Step 4: route to plugin.
	resp, routeErr := s.registry.RouteRequest(reqCtx, req)

	// Step 5: compute cost.
	var cost float64
	if s.opts.Costs != nil {
		cost = s.opts.Costs.CostForRequest(req)
	}

	// Step 6: record write for consistency tracking on success.
	if routeErr == nil && s.opts.Consistency != nil && isMutating(req.Operation) {
		s.opts.Consistency.RecordWrite(reqCtx, req)
	}

	duration := time.Since(start)

	// Step 7: always record the event regardless of routing outcome.
	if recordErr := s.store.RecordRequest(r.Context(), reqCtx, req, resp, duration, cost, routeErr); recordErr != nil {
		s.logger.Warn("failed to record event", "err", recordErr)
	}

	if routeErr != nil {
		s.writeError(w, routeErr)
		return
	}

	s.writeResponse(w, resp)
}

// writeResponse serialises resp into the HTTP response writer.
func (s *Server) writeResponse(w http.ResponseWriter, resp *AWSResponse) {
	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(resp.StatusCode)
	if len(resp.Body) > 0 {
		if _, err := w.Write(resp.Body); err != nil {
			s.logger.Warn("failed to write response body", "err", err)
		}
	}
}

// writeError converts err into an AWS-style XML error response.
func (s *Server) writeError(w http.ResponseWriter, err error) {
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
