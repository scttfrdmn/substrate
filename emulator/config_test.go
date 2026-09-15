package emulator_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestDefaultConfig(t *testing.T) {
	cfg := emulator.DefaultConfig()
	assert.Equal(t, ":4566", cfg.Server.Address)
	assert.Equal(t, "30s", cfg.Server.ReadTimeout)
	assert.Equal(t, "30s", cfg.Server.WriteTimeout)
	assert.Equal(t, "10s", cfg.Server.ShutdownTimeout)
	assert.True(t, cfg.EventStore.Enabled)
	assert.Equal(t, "memory", cfg.EventStore.Backend)
	assert.Equal(t, "memory", cfg.State.Backend)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Empty path with no substrate.yaml on disk → pure defaults.
	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.Equal(t, ":4566", cfg.Server.Address)
	assert.Equal(t, "memory", cfg.EventStore.Backend)
}

func TestLoadConfig_YAML(t *testing.T) {
	yaml := `
server:
  address: ":9000"
  read_timeout: "60s"
event_store:
  enabled: true
  backend: memory
state:
  backend: memory
log:
  level: debug
  format: json
`
	path := filepath.Join(t.TempDir(), "substrate.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, ":9000", cfg.Server.Address)
	assert.Equal(t, "60s", cfg.Server.ReadTimeout)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
}

func TestLoadConfig_EnvOverride(t *testing.T) {
	t.Setenv("SUBSTRATE_SERVER_ADDRESS", ":1234")
	t.Setenv("SUBSTRATE_LOG_LEVEL", "warn")

	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.Equal(t, ":1234", cfg.Server.Address)
	assert.Equal(t, "warn", cfg.Log.Level)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := emulator.LoadConfig("/nonexistent/path/substrate.yaml")
	require.Error(t, err)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*emulator.Config)
		wantErr string
	}{
		{
			name:    "valid defaults",
			mutate:  func(_ *emulator.Config) {},
			wantErr: "",
		},
		{
			name:    "empty address",
			mutate:  func(c *emulator.Config) { c.Server.Address = "" },
			wantErr: "server.address must not be empty",
		},
		{
			name:    "invalid event store backend",
			mutate:  func(c *emulator.Config) { c.EventStore.Backend = "redis" },
			wantErr: "event_store.backend",
		},
		{
			name:    "invalid state backend",
			mutate:  func(c *emulator.Config) { c.State.Backend = "postgres" },
			wantErr: `state.backend "postgres" is not valid; choose memory`,
		},
		{
			// Accepted until #881 and built by nothing, so it ran in memory: a
			// caller who asked for persistence got none, and no error.
			name:    "sqlite state backend is refused rather than falling back to memory",
			mutate:  func(c *emulator.Config) { c.State.Backend = "sqlite" },
			wantErr: `state.backend "sqlite" is not implemented; choose memory`,
		},
		{
			name:    "state path is refused while no backend reads it",
			mutate:  func(c *emulator.Config) { c.State.Path = "/var/lib/substrate" },
			wantErr: `state.path "/var/lib/substrate" is set but no state backend reads it`,
		},
		{
			name:    "negative replay speed multiplier",
			mutate:  func(c *emulator.Config) { c.Replay.SpeedMultiplier = -1 },
			wantErr: "replay.speed_multiplier -1 is out of range; must be >= 0",
		},
		{
			name:    "zero replay speed multiplier is instant replay, not an error",
			mutate:  func(c *emulator.Config) { c.Replay.SpeedMultiplier = 0 },
			wantErr: "",
		},
		{
			name:    "invalid log level",
			mutate:  func(c *emulator.Config) { c.Log.Level = "verbose" },
			wantErr: "log.level",
		},
		{
			name:    "invalid log format",
			mutate:  func(c *emulator.Config) { c.Log.Format = "xml" },
			wantErr: "log.format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := emulator.DefaultConfig()
			tt.mutate(cfg)
			err := emulator.Validate(cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestEventStoreCfg_ToEventStoreConfig(t *testing.T) {
	cfg := emulator.EventStoreCfg{
		Enabled:            true,
		Backend:            "memory",
		SnapshotInterval:   50,
		MaxInMemory:        1000,
		PersistPath:        "/tmp/events",
		IncludeBodies:      true,
		IncludeStateHashes: true,
	}
	esCfg := cfg.ToEventStoreConfig()
	assert.Equal(t, true, esCfg.Enabled)
	assert.Equal(t, "memory", esCfg.Backend)
	assert.Equal(t, 50, esCfg.SnapshotInterval)
	assert.Equal(t, 1000, esCfg.MaxEventsInMemory)
	assert.Equal(t, "/tmp/events", esCfg.PersistPath)
	assert.True(t, esCfg.IncludeBodies)
	assert.True(t, esCfg.IncludeStateHashes)
}

// TestReplayCfg_Defaults pins the defaults #880 chose for the replay: section.
//
// The equality against a zero ReplayConfig is the compatibility assertion: the
// command passed substrate.ReplayConfig{} before the section existed, so a run with
// no replay: block must still replay exactly as it did.
func TestReplayCfg_Defaults(t *testing.T) {
	cfg := emulator.DefaultConfig()

	assert.Equal(t, 0.0, cfg.Replay.SpeedMultiplier, "0 replays instantly, off the wall clock")
	assert.False(t, cfg.Replay.StopOnError, "a replay reports every failure, not only the first")
	assert.False(t, cfg.Replay.ValidateState,
		"off deliberately: a comparison needs event_store.include_state_hashes, itself off by default")
	assert.False(t, cfg.Replay.UseSnapshots, "a replay re-executes the stream from empty state")
	assert.Equal(t, int64(0), cfg.Replay.RandomSeed, "unseeded, as every replay has been")

	assert.Equal(t, emulator.ReplayConfig{}, cfg.Replay.ToReplayConfig(),
		"the defaults must reproduce the zero value the command used before the section existed")
}

// TestReplayCfg_ToReplayConfig asserts every field reaches the engine. The
// conversion is field-for-field on purpose: a knob in the config file that the
// converter drops is indistinguishable from the defect #880 fixes.
func TestReplayCfg_ToReplayConfig(t *testing.T) {
	rc := emulator.ReplayCfg{
		SpeedMultiplier: 2.5,
		StopOnError:     true,
		ValidateState:   true,
		UseSnapshots:    true,
		RandomSeed:      99,
	}.ToReplayConfig()

	assert.Equal(t, 2.5, rc.SpeedMultiplier)
	assert.True(t, rc.StopOnError)
	assert.True(t, rc.ValidateState)
	assert.True(t, rc.UseSnapshots)
	assert.Equal(t, int64(99), rc.RandomSeed)
}

// TestLoadConfig_ReplaySection covers the round trip the section exists for: a
// substrate.yaml naming these values produces them, and a file that says nothing
// about replay produces the documented defaults.
func TestLoadConfig_ReplaySection(t *testing.T) {
	yaml := `
replay:
  speed_multiplier: 1.5
  stop_on_error: true
  validate_state: true
  use_snapshots: true
  random_seed: 4242
`
	path := filepath.Join(t.TempDir(), "substrate.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, emulator.ReplayConfig{
		SpeedMultiplier: 1.5,
		StopOnError:     true,
		ValidateState:   true,
		UseSnapshots:    true,
		RandomSeed:      4242,
	}, cfg.Replay.ToReplayConfig())

	// A config file with no replay: block keeps the defaults.
	bare := filepath.Join(t.TempDir(), "substrate.yaml")
	require.NoError(t, os.WriteFile(bare, []byte("log:\n  level: debug\n"), 0o600))
	cfg, err = emulator.LoadConfig(bare)
	require.NoError(t, err)
	assert.Equal(t, emulator.DefaultConfig().Replay, cfg.Replay)
}

// TestLoadConfig_UnsupportedStateBackend is #881's startup criterion: a config
// naming a backend nothing implements fails at load, naming the backend, rather
// than starting with a memory manager the caller did not ask for.
func TestLoadConfig_UnsupportedStateBackend(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "substrate.yaml")
			require.NoError(t, os.WriteFile(path,
				[]byte("state:\n  backend: "+backend+"\n"), 0o600))

			cfg, err := emulator.LoadConfig(path)
			require.Error(t, err, "loaded %+v instead of refusing an unimplemented backend", cfg)
			assert.Contains(t, err.Error(), backend, "the error must name the backend that was asked for")
		})
	}
}

func TestQuotaCfg_ToQuotaConfig_Defaults(t *testing.T) {
	// Empty rules — should fall back to built-in defaults.
	cfg := emulator.QuotaCfg{Enabled: true, Rules: nil}
	qc := cfg.ToQuotaConfig()
	assert.True(t, qc.Enabled)
	assert.NotEmpty(t, qc.Rules, "default rules must be populated")
}

func TestQuotaCfg_ToQuotaConfig_CustomRules(t *testing.T) {
	cfg := emulator.QuotaCfg{
		Enabled: true,
		Rules: map[string]emulator.RateRuleCfg{
			"s3/PutObject": {Rate: 10, Burst: 20},
		},
	}
	qc := cfg.ToQuotaConfig()
	assert.True(t, qc.Enabled)
	require.Contains(t, qc.Rules, "s3/PutObject")
	assert.Equal(t, float64(10), qc.Rules["s3/PutObject"].Rate)
	assert.Equal(t, float64(20), qc.Rules["s3/PutObject"].Burst)
}

func TestConsistencyCfg_ToConsistencyConfig_Defaults(t *testing.T) {
	cfg := emulator.ConsistencyCfg{Enabled: false}
	cc, err := cfg.ToConsistencyConfig()
	require.NoError(t, err)
	assert.False(t, cc.Enabled)
	assert.Equal(t, 2*time.Second, cc.PropagationDelay)
}

func TestConsistencyCfg_ToConsistencyConfig_CustomDelay(t *testing.T) {
	cfg := emulator.ConsistencyCfg{
		Enabled:          true,
		PropagationDelay: "500ms",
		AffectedServices: []string{"s3"},
	}
	cc, err := cfg.ToConsistencyConfig()
	require.NoError(t, err)
	assert.True(t, cc.Enabled)
	assert.Equal(t, int64(500_000_000), int64(cc.PropagationDelay))
	assert.Equal(t, []string{"s3"}, cc.AffectedServices)
}

func TestConsistencyCfg_ToConsistencyConfig_BadDelay(t *testing.T) {
	cfg := emulator.ConsistencyCfg{PropagationDelay: "not-a-duration"}
	_, err := cfg.ToConsistencyConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse propagation_delay")
}

func TestCostCfg_ToCostConfig(t *testing.T) {
	cfg := emulator.CostCfg{
		Enabled:   true,
		Overrides: map[string]float64{"s3/PutObject": 0.001},
	}
	cc := cfg.ToCostConfig()
	assert.True(t, cc.Enabled)
	assert.Equal(t, 0.001, cc.Overrides["s3/PutObject"])
}

func TestToFaultConfig(t *testing.T) {
	fc := emulator.FaultCfg{
		Enabled: true,
		Rules: []emulator.FaultRuleCfg{
			{
				Service:     "s3",
				Operation:   "PutObject",
				FaultType:   "error",
				ErrorCode:   "InternalError",
				HTTPStatus:  500,
				ErrorMsg:    "injected",
				Probability: 0.5,
			},
			{
				Service:   "dynamodb",
				FaultType: "latency",
				LatencyMs: 100,
			},
		},
	}
	cfg := fc.ToFaultConfig()
	assert.True(t, cfg.Enabled)
	require.Len(t, cfg.Rules, 2)
	assert.Equal(t, "s3", cfg.Rules[0].Service)
	assert.Equal(t, "PutObject", cfg.Rules[0].Operation)
	assert.Equal(t, "error", cfg.Rules[0].FaultType)
	assert.Equal(t, "InternalError", cfg.Rules[0].ErrorCode)
	assert.Equal(t, float64(0.5), cfg.Rules[0].Probability)
	assert.Equal(t, "dynamodb", cfg.Rules[1].Service)
	assert.Equal(t, "latency", cfg.Rules[1].FaultType)
	assert.Equal(t, 100, cfg.Rules[1].LatencyMs)
}

func TestToTracingConfig(t *testing.T) {
	tc := emulator.TracingCfg{
		Enabled:      true,
		OTLPEndpoint: "http://localhost:4317",
		ServiceName:  "substrate-test",
	}
	cfg := tc.ToTracingConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "http://localhost:4317", cfg.OTLPEndpoint)
	assert.Equal(t, "substrate-test", cfg.ServiceName)
}
