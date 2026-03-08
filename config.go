package substrate

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the full Substrate server configuration.
type Config struct {
	// Server controls network binding and timeout behaviour.
	Server ServerConfig `mapstructure:"server"`

	// EventStore controls event recording and storage.
	EventStore EventStoreCfg `mapstructure:"event_store"`

	// State controls the state manager backend.
	State StateCfg `mapstructure:"state"`

	// Log controls log level and format.
	Log LogCfg `mapstructure:"log"`

	// Quotas controls per-service and per-operation rate limiting.
	Quotas QuotaCfg `mapstructure:"quotas"`

	// Consistency controls eventual-consistency simulation.
	Consistency ConsistencyCfg `mapstructure:"consistency"`

	// Costs controls per-request cost estimation.
	Costs CostCfg `mapstructure:"costs"`
}

// ServerConfig holds HTTP server parameters.
type ServerConfig struct {
	// Address is the TCP address to listen on (e.g. ":4566").
	Address string `mapstructure:"address"`

	// ReadTimeout is the maximum duration for reading the entire request.
	ReadTimeout string `mapstructure:"read_timeout"`

	// WriteTimeout is the maximum duration before timing out writes of the response.
	WriteTimeout string `mapstructure:"write_timeout"`

	// ShutdownTimeout is the maximum duration to wait for active connections to
	// finish during graceful shutdown.
	ShutdownTimeout string `mapstructure:"shutdown_timeout"`
}

// EventStoreCfg is the YAML-friendly configuration for the event store.
// Use [EventStoreCfg.ToEventStoreConfig] to convert it for use with [NewEventStore].
type EventStoreCfg struct {
	// Enabled gates event recording; when false RecordEvent is a no-op.
	Enabled bool `mapstructure:"enabled"`

	// Backend selects the storage driver: "memory", "sqlite", or "file".
	Backend string `mapstructure:"backend"`

	// SnapshotInterval creates a snapshot automatically every N events.
	SnapshotInterval int `mapstructure:"snapshot_interval"`

	// MaxInMemory is the maximum number of events held in memory.
	MaxInMemory int `mapstructure:"max_in_memory"`

	// PersistPath is the filesystem path used by non-memory backends.
	PersistPath string `mapstructure:"persist_path"`

	// IncludeBodies instructs the store to capture raw request/response bodies.
	IncludeBodies bool `mapstructure:"include_bodies"`

	// IncludeStateHashes enables before/after state hashing on each event.
	IncludeStateHashes bool `mapstructure:"include_state_hashes"`
}

// ToEventStoreConfig converts EventStoreCfg to the [EventStoreConfig] type
// used by [NewEventStore].
func (c EventStoreCfg) ToEventStoreConfig() EventStoreConfig {
	return EventStoreConfig{
		Enabled:            c.Enabled,
		Backend:            c.Backend,
		SnapshotInterval:   c.SnapshotInterval,
		MaxEventsInMemory:  c.MaxInMemory,
		PersistPath:        c.PersistPath,
		IncludeBodies:      c.IncludeBodies,
		IncludeStateHashes: c.IncludeStateHashes,
	}
}

// StateCfg controls the state manager backend.
type StateCfg struct {
	// Backend selects the storage driver: "memory" (sqlite deferred to #2).
	Backend string `mapstructure:"backend"`

	// Path is the filesystem path used by non-memory backends.
	Path string `mapstructure:"path"`
}

// LogCfg controls logging behaviour.
type LogCfg struct {
	// Level is the minimum log level: "debug", "info", "warn", or "error".
	Level string `mapstructure:"level"`

	// Format selects the output format: "text" or "json".
	Format string `mapstructure:"format"`
}

// QuotaCfg is the YAML-friendly configuration for quota enforcement.
// Use [QuotaCfg.ToQuotaConfig] to convert it for use with [NewQuotaController].
type QuotaCfg struct {
	// Enabled gates quota enforcement. Default true.
	Enabled bool `mapstructure:"enabled"`

	// Rules maps service or service/operation keys to rate rules.
	// When empty the built-in defaults from [defaultQuotaRules] are used.
	Rules map[string]RateRuleCfg `mapstructure:"rules"`
}

// RateRuleCfg is the YAML representation of a token-bucket rate rule.
type RateRuleCfg struct {
	// Rate is the sustained token replenishment rate in tokens per second.
	Rate float64 `mapstructure:"rate"`

	// Burst is the maximum burst capacity.
	Burst float64 `mapstructure:"burst"`
}

// ToQuotaConfig converts QuotaCfg to the [QuotaConfig] type used by
// [NewQuotaController]. When no rules are configured the built-in defaults
// are used.
func (c QuotaCfg) ToQuotaConfig() QuotaConfig {
	rules := make(map[string]RateRule, len(c.Rules))
	if len(c.Rules) == 0 {
		rules = defaultQuotaRules()
	} else {
		for k, r := range c.Rules {
			rules[k] = RateRule(r)
		}
	}
	return QuotaConfig{
		Enabled: c.Enabled,
		Rules:   rules,
	}
}

// ConsistencyCfg is the YAML-friendly configuration for eventual-consistency
// simulation. Use [ConsistencyCfg.ToConsistencyConfig] to convert it for use
// with [NewConsistencyController].
type ConsistencyCfg struct {
	// Enabled gates consistency simulation. Default false.
	Enabled bool `mapstructure:"enabled"`

	// PropagationDelay is the duration string (e.g. "2s") during which reads
	// to a recently mutated resource are rejected.
	PropagationDelay string `mapstructure:"propagation_delay"`

	// AffectedServices is the list of services that participate in the
	// simulation. Default: ["iam"].
	AffectedServices []string `mapstructure:"affected_services"`
}

// ToConsistencyConfig converts ConsistencyCfg to the [ConsistencyConfig] type
// used by [NewConsistencyController]. It returns an error when
// PropagationDelay is non-empty but cannot be parsed as a duration.
func (c ConsistencyCfg) ToConsistencyConfig() (ConsistencyConfig, error) {
	delay := 2 * time.Second
	if c.PropagationDelay != "" {
		d, err := time.ParseDuration(c.PropagationDelay)
		if err != nil {
			return ConsistencyConfig{}, fmt.Errorf("parse propagation_delay %q: %w", c.PropagationDelay, err)
		}
		delay = d
	}
	return ConsistencyConfig{
		Enabled:          c.Enabled,
		PropagationDelay: delay,
		AffectedServices: c.AffectedServices,
	}, nil
}

// CostCfg is the YAML-friendly configuration for cost tracking.
// Use [CostCfg.ToCostConfig] to convert it for use with [NewCostController].
type CostCfg struct {
	// Enabled gates cost estimation. Default true.
	Enabled bool `mapstructure:"enabled"`

	// Overrides maps "service/operation" or "service" keys to USD per request,
	// overriding the built-in pricing table.
	Overrides map[string]float64 `mapstructure:"overrides"`
}

// ToCostConfig converts CostCfg to the [CostConfig] type used by
// [NewCostController].
func (c CostCfg) ToCostConfig() CostConfig {
	return CostConfig(c)
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Address:         ":4566",
			ReadTimeout:     "30s",
			WriteTimeout:    "30s",
			ShutdownTimeout: "10s",
		},
		EventStore: EventStoreCfg{
			Enabled:            true,
			Backend:            "memory",
			SnapshotInterval:   100,
			MaxInMemory:        0,
			IncludeBodies:      false,
			IncludeStateHashes: false,
		},
		State: StateCfg{
			Backend: "memory",
		},
		Log: LogCfg{
			Level:  "info",
			Format: "text",
		},
		Quotas: QuotaCfg{
			Enabled: true,
			// Rules left empty so ToQuotaConfig falls back to defaultQuotaRules.
		},
		Consistency: ConsistencyCfg{
			Enabled:          false,
			PropagationDelay: "2s",
			AffectedServices: []string{"iam"},
		},
		Costs: CostCfg{
			Enabled: true,
		},
	}
}

// LoadConfig reads configuration from path (YAML). When path is empty,
// viper searches for substrate.yaml in the current working directory.
// Environment variables prefixed with SUBSTRATE_ override file values
// (e.g. SUBSTRATE_SERVER_ADDRESS). Validate is called before returning.
func LoadConfig(path string) (*Config, error) {
	v := viper.New()

	// Seed with defaults so all keys are known to viper.
	defaults := DefaultConfig()
	v.SetDefault("server.address", defaults.Server.Address)
	v.SetDefault("server.read_timeout", defaults.Server.ReadTimeout)
	v.SetDefault("server.write_timeout", defaults.Server.WriteTimeout)
	v.SetDefault("server.shutdown_timeout", defaults.Server.ShutdownTimeout)
	v.SetDefault("event_store.enabled", defaults.EventStore.Enabled)
	v.SetDefault("event_store.backend", defaults.EventStore.Backend)
	v.SetDefault("event_store.snapshot_interval", defaults.EventStore.SnapshotInterval)
	v.SetDefault("event_store.max_in_memory", defaults.EventStore.MaxInMemory)
	v.SetDefault("event_store.persist_path", defaults.EventStore.PersistPath)
	v.SetDefault("event_store.include_bodies", defaults.EventStore.IncludeBodies)
	v.SetDefault("event_store.include_state_hashes", defaults.EventStore.IncludeStateHashes)
	v.SetDefault("state.backend", defaults.State.Backend)
	v.SetDefault("state.path", defaults.State.Path)
	v.SetDefault("log.level", defaults.Log.Level)
	v.SetDefault("log.format", defaults.Log.Format)
	v.SetDefault("quotas.enabled", defaults.Quotas.Enabled)
	v.SetDefault("consistency.enabled", defaults.Consistency.Enabled)
	v.SetDefault("consistency.propagation_delay", defaults.Consistency.PropagationDelay)
	v.SetDefault("consistency.affected_services", defaults.Consistency.AffectedServices)
	v.SetDefault("costs.enabled", defaults.Costs.Enabled)

	// Environment variable overrides.
	v.SetEnvPrefix("SUBSTRATE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Load YAML file if provided or discoverable.
	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("substrate")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		// Missing config file is acceptable — defaults and env vars apply.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := Validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks cfg for invalid or missing values. It returns a descriptive
// error for the first problem found.
func Validate(cfg *Config) error {
	if cfg.Server.Address == "" {
		return fmt.Errorf("server.address must not be empty")
	}

	validBackends := map[string]bool{"memory": true, "sqlite": true, "file": true}
	if !validBackends[cfg.EventStore.Backend] {
		return fmt.Errorf("event_store.backend %q is not valid; choose memory, sqlite, or file", cfg.EventStore.Backend)
	}

	validStateBackends := map[string]bool{"memory": true, "sqlite": true}
	if !validStateBackends[cfg.State.Backend] {
		return fmt.Errorf("state.backend %q is not valid; choose memory or sqlite", cfg.State.Backend)
	}

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[cfg.Log.Level] {
		return fmt.Errorf("log.level %q is not valid; choose debug, info, warn, or error", cfg.Log.Level)
	}

	validFormats := map[string]bool{"text": true, "json": true}
	if !validFormats[cfg.Log.Format] {
		return fmt.Errorf("log.format %q is not valid; choose text or json", cfg.Log.Format)
	}

	for key, rule := range cfg.Quotas.Rules {
		if rule.Rate <= 0 {
			return fmt.Errorf("quotas.rules[%q].rate must be > 0", key)
		}
		if rule.Burst <= 0 {
			return fmt.Errorf("quotas.rules[%q].burst must be > 0", key)
		}
	}

	if cfg.Consistency.PropagationDelay != "" {
		if _, err := time.ParseDuration(cfg.Consistency.PropagationDelay); err != nil {
			return fmt.Errorf("consistency.propagation_delay %q is not a valid duration: %w",
				cfg.Consistency.PropagationDelay, err)
		}
	}

	return nil
}
