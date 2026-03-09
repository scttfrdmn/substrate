package substrate_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestDefaultConfig(t *testing.T) {
	cfg := substrate.DefaultConfig()
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
	cfg, err := substrate.LoadConfig("")
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

	cfg, err := substrate.LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, ":9000", cfg.Server.Address)
	assert.Equal(t, "60s", cfg.Server.ReadTimeout)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
}

func TestLoadConfig_EnvOverride(t *testing.T) {
	t.Setenv("SUBSTRATE_SERVER_ADDRESS", ":1234")
	t.Setenv("SUBSTRATE_LOG_LEVEL", "warn")

	cfg, err := substrate.LoadConfig("")
	require.NoError(t, err)
	assert.Equal(t, ":1234", cfg.Server.Address)
	assert.Equal(t, "warn", cfg.Log.Level)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := substrate.LoadConfig("/nonexistent/path/substrate.yaml")
	require.Error(t, err)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*substrate.Config)
		wantErr string
	}{
		{
			name:    "valid defaults",
			mutate:  func(_ *substrate.Config) {},
			wantErr: "",
		},
		{
			name:    "empty address",
			mutate:  func(c *substrate.Config) { c.Server.Address = "" },
			wantErr: "server.address must not be empty",
		},
		{
			name:    "invalid event store backend",
			mutate:  func(c *substrate.Config) { c.EventStore.Backend = "redis" },
			wantErr: "event_store.backend",
		},
		{
			name:    "invalid state backend",
			mutate:  func(c *substrate.Config) { c.State.Backend = "postgres" },
			wantErr: "state.backend",
		},
		{
			name:    "invalid log level",
			mutate:  func(c *substrate.Config) { c.Log.Level = "verbose" },
			wantErr: "log.level",
		},
		{
			name:    "invalid log format",
			mutate:  func(c *substrate.Config) { c.Log.Format = "xml" },
			wantErr: "log.format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := substrate.DefaultConfig()
			tt.mutate(cfg)
			err := substrate.Validate(cfg)
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
	cfg := substrate.EventStoreCfg{
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

func TestQuotaCfg_ToQuotaConfig_Defaults(t *testing.T) {
	// Empty rules — should fall back to built-in defaults.
	cfg := substrate.QuotaCfg{Enabled: true, Rules: nil}
	qc := cfg.ToQuotaConfig()
	assert.True(t, qc.Enabled)
	assert.NotEmpty(t, qc.Rules, "default rules must be populated")
}

func TestQuotaCfg_ToQuotaConfig_CustomRules(t *testing.T) {
	cfg := substrate.QuotaCfg{
		Enabled: true,
		Rules: map[string]substrate.RateRuleCfg{
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
	cfg := substrate.ConsistencyCfg{Enabled: false}
	cc, err := cfg.ToConsistencyConfig()
	require.NoError(t, err)
	assert.False(t, cc.Enabled)
	assert.Equal(t, 2*time.Second, cc.PropagationDelay)
}

func TestConsistencyCfg_ToConsistencyConfig_CustomDelay(t *testing.T) {
	cfg := substrate.ConsistencyCfg{
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
	cfg := substrate.ConsistencyCfg{PropagationDelay: "not-a-duration"}
	_, err := cfg.ToConsistencyConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse propagation_delay")
}

func TestCostCfg_ToCostConfig(t *testing.T) {
	cfg := substrate.CostCfg{
		Enabled:   true,
		Overrides: map[string]float64{"s3/PutObject": 0.001},
	}
	cc := cfg.ToCostConfig()
	assert.True(t, cc.Enabled)
	assert.Equal(t, 0.001, cc.Overrides["s3/PutObject"])
}

func TestToFaultConfig(t *testing.T) {
	fc := substrate.FaultCfg{
		Enabled: true,
		Rules: []substrate.FaultRuleCfg{
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
	tc := substrate.TracingCfg{
		Enabled:      true,
		OTLPEndpoint: "http://localhost:4317",
		ServiceName:  "substrate-test",
	}
	cfg := tc.ToTracingConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "http://localhost:4317", cfg.OTLPEndpoint)
	assert.Equal(t, "substrate-test", cfg.ServiceName)
}
