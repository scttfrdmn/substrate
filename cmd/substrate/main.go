// Command substrate is the CLI entry point for the Substrate AWS emulator.
//
// Usage:
//
//	substrate [command] [flags]
//
// Commands:
//
//	server        Start the HTTP server
//	replay        Replay a recorded event stream
//	debug         Inspect events in a recorded stream
//	export        Export recorded events (NDJSON or CSV)
//	validate-plan Validate a Terraform JSON plan
//	status        Show server status and summary
//	inspect       Show recent events for a service
//	pricing       Manage pricing configuration
//	reset         Reset server state
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	substrate "github.com/scttfrdmn/substrate"
)

// version is set at build time via -ldflags "-X main.version=v0.2.0".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "substrate: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	root := buildRootCmd()
	root.SetArgs(args)
	return root.Execute()
}

func buildRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "substrate",
		Short: "Substrate — deterministic AWS emulator for AI-generated IaC testing",
		Long: `Substrate is an event-sourced AWS emulator. It records every API call as
an immutable event, enabling deterministic replay and time-travel debugging
of CDK, CloudFormation, and Terraform infrastructure code.`,
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(newServerCmd())
	root.AddCommand(newReplayCmd())
	root.AddCommand(newDebugCmd())
	root.AddCommand(newExportCmd())
	root.AddCommand(newValidatePlanCmd())
	root.AddCommand(newStatusCmd())
	root.AddCommand(newInspectCmd())
	root.AddCommand(newPricingCmd())
	root.AddCommand(newResetCmd())

	return root
}

func newServerCmd() *cobra.Command {
	var configPath string
	var address string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the Substrate HTTP server",
		Long: `Start the Substrate HTTP server. AWS SDKs and tools pointed at the
configured address will have their requests emulated and recorded.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := substrate.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// Flag overrides config file.
			if cmd.Flags().Changed("address") {
				cfg.Server.Address = address
			}

			logLevel := slog.LevelInfo
			jsonLog := cfg.Log.Format == "json"
			logger := substrate.NewDefaultLogger(logLevel, jsonLog)

			registry := substrate.NewPluginRegistry()
			tc := substrate.NewTimeController(time.Now())
			store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig(), substrate.WithTimeController(tc))
			state := substrate.NewMemoryStateManager()

			initCtx := context.Background()

			if err := substrate.RegisterDefaultPlugins(initCtx, registry, state, tc, logger, store, cfg); err != nil {
				return err
			}

			quotaCtrl := substrate.NewQuotaController(cfg.Quotas.ToQuotaConfig(), tc)

			consistencyCtrl, err := substrate.NewConsistencyController(
				func() substrate.ConsistencyConfig {
					cc, e := cfg.Consistency.ToConsistencyConfig()
					if e != nil {
						// Validate already ran; this should not happen.
						logger.Warn("consistency config parse failed, disabling", "err", e)
						return substrate.ConsistencyConfig{Enabled: false}
					}
					return cc
				}(),
				tc,
			)
			if err != nil {
				return fmt.Errorf("initialize consistency controller: %w", err)
			}

			var pricingProvider substrate.PricingProvider
			switch cfg.Pricing.Provider {
			case "aws":
				pricingProvider = substrate.NewAWSPricingProvider(substrate.AWSPricingConfig{
					CachePath:     cfg.Pricing.CachePath,
					CacheTTLHours: cfg.Pricing.CacheTTLHours,
					Region:        cfg.Pricing.Region,
				})
			default:
				pricingProvider = substrate.NewStaticPricingProvider()
			}
			costCtrl := substrate.NewCostController(cfg.Costs.ToCostConfig(),
				substrate.WithPricingProvider(pricingProvider))

			var faultCtrl *substrate.FaultController
			if cfg.Fault.Enabled || len(cfg.Fault.Rules) > 0 {
				faultCtrl = substrate.NewFaultController(cfg.Fault.ToFaultConfig(), time.Now().UnixNano())
			}

			authCtrl := substrate.NewAuthController(state, logger)

			var metricsCollector *substrate.MetricsCollector
			if cfg.Metrics.Enabled {
				metricsCollector = substrate.NewMetricsCollector()
			}

			var tracer *substrate.Tracer
			if cfg.Tracing.Enabled {
				var tracerShutdown func(context.Context) error
				tracer, tracerShutdown, err = substrate.NewTracer(initCtx, cfg.Tracing.ToTracingConfig())
				if err != nil {
					return fmt.Errorf("initialize tracer: %w", err)
				}
				defer func() { _ = tracerShutdown(context.Background()) }()
			}

			srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
				substrate.ServerOptions{
					Quota:       quotaCtrl,
					Consistency: consistencyCtrl,
					Costs:       costCtrl,
					Auth:        authCtrl,
					Metrics:     metricsCollector,
					Tracer:      tracer,
					Fault:       faultCtrl,
				})

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			// SIGHUP hot-reload: reload config and update controllers in place.
			sighup := make(chan os.Signal, 1)
			signal.Notify(sighup, syscall.SIGHUP)
			go func() {
				for range sighup {
					newCfg, loadErr := substrate.LoadConfig(configPath)
					if loadErr != nil {
						logger.Warn("config reload failed", "err", loadErr)
						continue
					}
					quotaCtrl.UpdateConfig(newCfg.Quotas.ToQuotaConfig())
					if newCC, ccErr := newCfg.Consistency.ToConsistencyConfig(); ccErr == nil {
						consistencyCtrl.UpdateConfig(newCC)
					}
					costCtrl.UpdateConfig(newCfg.Costs.ToCostConfig())
					costCtrl.SetDiscounts(newCfg.Costs.Discounts)
					if faultCtrl != nil {
						faultCtrl.UpdateConfig(newCfg.Fault.ToFaultConfig())
					}
					logger.Info("config reloaded")
				}
			}()

			return srv.Start(ctx)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to substrate.yaml config file")
	cmd.Flags().StringVar(&address, "address", ":4566", "TCP address to listen on")

	return cmd
}

func newReplayCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "replay <stream>",
		Short: "Replay a recorded event stream",
		Long: `Replay a previously recorded event stream against the emulator.
Outputs a summary of total, successful, and failed events along with
any determinism differences.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			streamID := args[0]

			cfg, err := substrate.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			logger := substrate.NewDefaultLogger(slog.LevelInfo, cfg.Log.Format == "json")
			store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
			tc := substrate.NewTimeController(time.Now())
			registry := substrate.NewPluginRegistry()

			engine := substrate.NewReplayEngine(store, nil, tc, registry,
				substrate.ReplayConfig{}, logger)

			ctx := context.Background()
			results, err := engine.Replay(ctx, streamID)
			if err != nil {
				return fmt.Errorf("replay %q: %w", streamID, err)
			}

			fmt.Printf("Replay complete — stream: %s\n", streamID)
			fmt.Printf("  Total:    %d\n", results.TotalEvents)
			fmt.Printf("  Success:  %d\n", results.SuccessEvents)
			fmt.Printf("  Failed:   %d\n", results.FailedEvents)
			fmt.Printf("  Duration: %s\n", results.Duration)
			if len(results.Differences) > 0 {
				fmt.Printf("  Differences: %d\n", len(results.Differences))
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to substrate.yaml config file")

	return cmd
}

func newExportCmd() *cobra.Command {
	var configPath string
	var format string
	var output string
	var streamID string
	var service string
	var start string
	var end string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export recorded events to NDJSON or CSV",
		Long: `Export events from the Substrate event store to stdout or a file.
Supports NDJSON (newline-delimited JSON) and CSV output formats.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := substrate.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())

			filter := substrate.EventFilter{
				StreamID: streamID,
				Service:  service,
			}
			if start != "" {
				t, parseErr := time.Parse(time.RFC3339, start)
				if parseErr != nil {
					return fmt.Errorf("parse --start: %w", parseErr)
				}
				filter.StartTime = t
			}
			if end != "" {
				t, parseErr := time.Parse(time.RFC3339, end)
				if parseErr != nil {
					return fmt.Errorf("parse --end: %w", parseErr)
				}
				filter.EndTime = t
			}

			var w *os.File
			if output == "" || output == "-" {
				w = os.Stdout
			} else {
				f, openErr := os.Create(output)
				if openErr != nil {
					return fmt.Errorf("open output file: %w", openErr)
				}
				defer f.Close() //nolint:errcheck
				w = f
			}

			ctx := context.Background()
			var n int64
			switch format {
			case "csv":
				n, err = store.ExportCSV(ctx, filter, w)
			default:
				n, err = store.ExportNDJSON(ctx, filter, w)
			}
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			if output != "" && output != "-" {
				fmt.Fprintf(os.Stderr, "exported %d events to %s\n", n, output)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to substrate.yaml config file")
	cmd.Flags().StringVar(&format, "format", "ndjson", "output format: ndjson or csv")
	cmd.Flags().StringVar(&output, "output", "-", "output file path; - writes to stdout")
	cmd.Flags().StringVar(&streamID, "stream", "", "filter to a specific stream ID")
	cmd.Flags().StringVar(&service, "service", "", "filter to a specific service")
	cmd.Flags().StringVar(&start, "start", "", "RFC3339 start time (inclusive)")
	cmd.Flags().StringVar(&end, "end", "", "RFC3339 end time (exclusive)")

	return cmd
}

func newValidatePlanCmd() *cobra.Command {
	var configPath string
	var planPath string

	cmd := &cobra.Command{
		Use:   "validate-plan",
		Short: "Validate a Terraform JSON plan",
		Long: `Analyze a Terraform plan (terraform show -json) for estimated cost and
policy concerns. No emulator state is modified.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if planPath == "" {
				return fmt.Errorf("--plan is required")
			}

			f, err := os.Open(planPath)
			if err != nil {
				return fmt.Errorf("open plan file: %w", err)
			}
			defer f.Close() //nolint:errcheck

			plan, err := substrate.ParseTerraformPlan(f)
			if err != nil {
				return fmt.Errorf("parse plan: %w", err)
			}

			var costs *substrate.CostController
			cfg, cfgErr := substrate.LoadConfig(configPath)
			if cfgErr == nil {
				costs = substrate.NewCostController(cfg.Costs.ToCostConfig())
			}

			ctx := context.Background()
			result, err := substrate.ValidateTerraformPlan(ctx, plan, costs)
			if err != nil {
				return fmt.Errorf("validate: %w", err)
			}

			creates := len(result.CreatedResources)
			deletes := len(result.DeletedResources)
			updates := result.ResourceCount - creates - deletes

			fmt.Printf("Resource changes:  %d (%d create, %d update, %d delete)\n",
				result.ResourceCount, creates, updates, deletes)
			fmt.Printf("Estimated monthly: $%.2f\n", result.EstimatedMonthlyCostUSD)

			if len(result.Warnings) > 0 {
				fmt.Println("Warnings:")
				for _, w := range result.Warnings {
					fmt.Printf("  - %s\n", w)
				}
			}
			if len(result.Errors) > 0 {
				fmt.Println("Errors:")
				for _, e := range result.Errors {
					fmt.Printf("  - %s\n", e)
				}
				return fmt.Errorf("validation failed with %d error(s)", len(result.Errors))
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to substrate.yaml config file")
	cmd.Flags().StringVar(&planPath, "plan", "", "path to terraform show -json output file")

	return cmd
}
func newDebugCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "debug <stream>",
		Short: "Inspect events in a recorded stream",
		Long: `List events recorded in a stream and print their details.
Full interactive time-travel debugging will be available in a later release.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			streamID := args[0]

			cfg, err := substrate.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			_ = cfg // used for future persistence options

			store := substrate.NewEventStore(substrate.EventStoreConfig{
				Enabled: true,
				Backend: "memory",
			})

			ctx := context.Background()
			events, err := store.GetStream(ctx, streamID)
			if err != nil {
				return fmt.Errorf("get stream %q: %w", streamID, err)
			}

			if len(events) == 0 {
				fmt.Printf("stream %q contains no events\n", streamID)
				return nil
			}

			fmt.Printf("Stream: %s (%d events)\n\n", streamID, len(events))
			for _, ev := range events {
				errStr := ""
				if ev.Error != "" {
					errStr = "  ERROR: " + ev.Error
				}
				fmt.Printf("  [%d] %s  %s/%s  %s%s\n",
					ev.Sequence,
					ev.Timestamp.Format("2006-01-02T15:04:05Z"),
					ev.Service,
					ev.Operation,
					ev.Duration,
					errStr,
				)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to substrate.yaml config file")

	return cmd
}

// --- HTTP client helpers for CLI subcommands ---

func cliGet(address, path string) ([]byte, error) {
	resp, err := http.Get(address + path) //nolint:gosec,noctx
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	return io.ReadAll(resp.Body)
}

func cliPost(address, path string, body []byte) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	}
	resp, err := http.Post(address+path, "application/json", bodyReader) //nolint:gosec,noctx
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	return io.ReadAll(resp.Body)
}

func newStatusCmd() *cobra.Command {
	var address string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show server status and summary",
		RunE: func(_ *cobra.Command, _ []string) error {
			healthData, err := cliGet(address, "/health")
			if err != nil {
				return fmt.Errorf("server unreachable at %s: %w", address, err)
			}
			var health map[string]interface{}
			_ = json.Unmarshal(healthData, &health)

			readyData, _ := cliGet(address, "/ready")
			var ready map[string]interface{}
			_ = json.Unmarshal(readyData, &ready)
			pluginCount := 0
			if plugins, ok := ready["plugins"].([]interface{}); ok {
				pluginCount = len(plugins)
			}

			timeData, _ := cliGet(address, "/v1/control/time")
			var timeInfo map[string]interface{}
			_ = json.Unmarshal(timeData, &timeInfo)

			costData, _ := cliGet(address, "/v1/debug/costs")
			var costs map[string]interface{}
			_ = json.Unmarshal(costData, &costs)

			fmt.Printf("Server:     %s\n", address)
			if v, ok := health["version"]; ok {
				fmt.Printf("Version:    %v\n", v)
			}
			if s, ok := health["status"]; ok {
				fmt.Printf("Status:     %v\n", s)
			}
			fmt.Printf("Plugins:    %d registered\n", pluginCount)
			if t, ok := timeInfo["simulated_time"]; ok {
				scale := timeInfo["scale"]
				fmt.Printf("Sim. Time:  %v (scale: %vx)\n", t, scale)
			}
			if rc, ok := costs["RequestCount"]; ok {
				fmt.Printf("Requests:   %.0f\n", rc)
			}
			if tc, ok := costs["TotalCost"]; ok {
				fmt.Printf("Total Cost: $%.4f\n", tc)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&address, "address", "http://localhost:4566", "server address")
	return cmd
}

func newInspectCmd() *cobra.Command {
	var address string
	cmd := &cobra.Command{
		Use:   "inspect [service]",
		Short: "Show recent events for a service, or list registered services",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 0 {
				data, err := cliGet(address, "/ready")
				if err != nil {
					return err
				}
				var ready map[string]interface{}
				_ = json.Unmarshal(data, &ready)
				plugins, _ := ready["plugins"].([]interface{})
				fmt.Println("Registered services:")
				for _, p := range plugins {
					fmt.Printf("  %v\n", p)
				}
				return nil
			}
			service := args[0]
			data, err := cliGet(address, "/v1/debug/events?service="+service+"&limit=100")
			if err != nil {
				return err
			}
			var result struct {
				Events []struct {
					Sequence   int64   `json:"seq"`
					Timestamp  string  `json:"timestamp"`
					Operation  string  `json:"operation"`
					StatusCode int     `json:"status_code"`
					Cost       float64 `json:"cost"`
					DurationMS int64   `json:"duration_ms"`
					Error      string  `json:"error"`
				} `json:"events"`
				Count int `json:"count"`
			}
			_ = json.Unmarshal(data, &result)
			fmt.Printf("Recent events for %s (%d):\n", service, result.Count)
			for _, ev := range result.Events {
				errStr := ""
				if ev.Error != "" {
					errStr = "  ERROR: " + ev.Error
				}
				fmt.Printf("  #%-4d %-25s %-30s %3d  $%.7f  %dms%s\n",
					ev.Sequence, ev.Timestamp, ev.Operation,
					ev.StatusCode, ev.Cost, ev.DurationMS, errStr)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&address, "address", "http://localhost:4566", "server address")
	return cmd
}

func newPricingCmd() *cobra.Command {
	var address string
	cmd := &cobra.Command{
		Use:   "pricing",
		Short: "Manage pricing configuration",
	}
	cmd.PersistentFlags().StringVar(&address, "address", "http://localhost:4566", "server address")

	infoCmd := &cobra.Command{
		Use:   "info",
		Short: "Show current pricing source and cache info",
		RunE: func(_ *cobra.Command, _ []string) error {
			data, err := cliGet(address, "/v1/pricing")
			if err != nil {
				return err
			}
			var info map[string]interface{}
			_ = json.Unmarshal(data, &info)
			fmt.Printf("Source:    %v\n", info["source"])
			fmt.Printf("Cache Age: %v\n", info["cacheAge"])
			return nil
		},
	}

	refreshCmd := &cobra.Command{
		Use:   "refresh",
		Short: "Fetch latest pricing data from AWS",
		RunE: func(_ *cobra.Command, _ []string) error {
			data, err := cliPost(address, "/v1/pricing/refresh", nil)
			if err != nil {
				return err
			}
			var result map[string]interface{}
			_ = json.Unmarshal(data, &result)
			fmt.Printf("Pricing refreshed. Source: %v\n", result["source"])
			return nil
		},
	}

	var service, operation string
	lookupCmd := &cobra.Command{
		Use:   "lookup",
		Short: "Look up price for a service/operation",
		RunE: func(_ *cobra.Command, _ []string) error {
			path := fmt.Sprintf("/v1/pricing/lookup?service=%s&operation=%s", service, operation)
			data, err := cliGet(address, path)
			if err != nil {
				return err
			}
			var result map[string]interface{}
			_ = json.Unmarshal(data, &result)
			fmt.Printf("Service:   %v\n", result["service"])
			fmt.Printf("Operation: %v\n", result["operation"])
			fmt.Printf("Price:     $%v\n", result["price"])
			return nil
		},
	}
	lookupCmd.Flags().StringVar(&service, "service", "", "AWS service name (required)")
	lookupCmd.Flags().StringVar(&operation, "operation", "", "operation name")
	_ = lookupCmd.MarkFlagRequired("service")

	cmd.AddCommand(infoCmd, refreshCmd, lookupCmd)
	return cmd
}

func newResetCmd() *cobra.Command {
	var address string
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Reset server state",
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := cliPost(address, "/v1/state/reset", nil)
			if err != nil {
				return err
			}
			fmt.Println("State reset successfully.")
			return nil
		},
	}
	cmd.Flags().StringVar(&address, "address", "http://localhost:4566", "server address")
	return cmd
}
