// Command substrate is the CLI entry point for the Substrate AWS emulator.
//
// Usage:
//
//	substrate [command] [flags]
//
// Commands:
//
//	server   Start the HTTP server
//	replay   Replay a recorded event stream
//	debug    Inspect events in a recorded stream
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
			store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
			state := substrate.NewMemoryStateManager()
			tc := substrate.NewTimeController(time.Now())

			initCtx := context.Background()

			iamPlugin := &substrate.IAMPlugin{}
			if err := iamPlugin.Initialize(initCtx, substrate.PluginConfig{State: state, Logger: logger}); err != nil {
				return fmt.Errorf("initialize iam plugin: %w", err)
			}
			registry.Register(iamPlugin)

			stsPlugin := &substrate.STSPlugin{}
			if err := stsPlugin.Initialize(initCtx, substrate.PluginConfig{
				State:   state,
				Logger:  logger,
				Options: map[string]any{"time_controller": tc},
			}); err != nil {
				return fmt.Errorf("initialize sts plugin: %w", err)
			}
			registry.Register(stsPlugin)

			s3Plugin := &substrate.S3Plugin{}
			if err := s3Plugin.Initialize(initCtx, substrate.PluginConfig{
				State:  state,
				Logger: logger,
				Options: map[string]any{
					"time_controller": tc,
				},
			}); err != nil {
				return fmt.Errorf("initialize s3 plugin: %w", err)
			}
			registry.Register(s3Plugin)

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

			costCtrl := substrate.NewCostController(cfg.Costs.ToCostConfig())

			srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
				substrate.ServerOptions{
					Quota:       quotaCtrl,
					Consistency: consistencyCtrl,
					Costs:       costCtrl,
				})

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

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
