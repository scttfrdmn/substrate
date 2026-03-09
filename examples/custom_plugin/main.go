// Command custom_plugin demonstrates how to build a minimal Substrate plugin.
// It implements a fictitious "weather" service with a single GetWeather operation.
//
// Run with:
//
//	go run ./examples/custom_plugin
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// WeatherPlugin emulates a fictitious "weather" AWS-style service.
// It stores the last queried city in the state manager.
type WeatherPlugin struct {
	state  substrate.StateManager
	logger substrate.Logger
}

// Name returns the service name used for request routing.
func (p *WeatherPlugin) Name() string { return "weather" }

// Initialize stores the state manager and logger for later use.
func (p *WeatherPlugin) Initialize(_ context.Context, cfg substrate.PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	return nil
}

// HandleRequest dispatches to the correct operation handler.
func (p *WeatherPlugin) HandleRequest(_ *substrate.RequestContext, req *substrate.AWSRequest) (*substrate.AWSResponse, error) {
	switch req.Operation {
	case "GetWeather":
		return p.handleGetWeather(req)
	default:
		return nil, &substrate.AWSError{
			Code:       "UnsupportedOperation",
			Message:    fmt.Sprintf("operation %q is not supported by the weather plugin", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// Shutdown is a no-op for this simple plugin.
func (p *WeatherPlugin) Shutdown(_ context.Context) error { return nil }

// handleGetWeather returns a stub temperature for the requested city and
// persists the city name in state.
func (p *WeatherPlugin) handleGetWeather(req *substrate.AWSRequest) (*substrate.AWSResponse, error) {
	city := req.Params["City"]
	if city == "" {
		city = "Unknown"
	}

	// Persist the last queried city using the state manager.
	if err := p.state.Put(context.Background(), "weather", "last_city", []byte(city)); err != nil {
		p.logger.Warn("failed to persist last city", "err", err)
	}

	resp := map[string]interface{}{
		"City":        city,
		"Temperature": 22.5,
		"Unit":        "Celsius",
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal weather response: %w", err)
	}

	return &substrate.AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}

func main() {
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	registry := substrate.NewPluginRegistry()
	plugin := &WeatherPlugin{}
	if err := plugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "initialize weather plugin: %v\n", err)
		os.Exit(1)
	}
	registry.Register(plugin)

	cfg := substrate.DefaultConfig()
	cfg.Server.Address = ":4567"

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("weather plugin server starting", "address", ":4567")
	if err := srv.Start(ctx); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}
