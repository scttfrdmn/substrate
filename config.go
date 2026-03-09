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

	// Metrics controls the Prometheus /metrics endpoint.
	Metrics MetricsCfg `mapstructure:"metrics"`

	// Forecast controls cost forecasting and anomaly detection.
	Forecast ForecastCfg `mapstructure:"forecast"`

	// Tracing controls OpenTelemetry distributed tracing.
	Tracing TracingCfg `mapstructure:"tracing"`

	// Fault controls fault injection into the request pipeline.
	Fault FaultCfg `mapstructure:"fault"`

	// Region controls multi-region routing and resource isolation.
	Region RegionCfg `mapstructure:"region"`
}

// MetricsCfg controls Prometheus metrics exposure.
type MetricsCfg struct {
	// Enabled gates the /metrics endpoint. Default false.
	Enabled bool `mapstructure:"enabled"`

	// Path is the HTTP path for the metrics endpoint. Default "/metrics".
	Path string `mapstructure:"path"`
}

// ForecastCfg controls cost forecasting behaviour.
type ForecastCfg struct {
	// Enabled gates the cost forecasting subsystem. Default false.
	Enabled bool `mapstructure:"enabled"`

	// WindowDays is the number of historical days used for regression.
	// Default 30.
	WindowDays int `mapstructure:"window_days"`

	// HorizonDays is the number of future days to project. Default 7.
	HorizonDays int `mapstructure:"horizon_days"`

	// AnomalyThresholdSigma is the number of standard deviations above the
	// mean at which a service cost is flagged as anomalous. Default 2.0.
	AnomalyThresholdSigma float64 `mapstructure:"anomaly_threshold_sigma"`

	// AlertThresholds maps service names to maximum acceptable daily USD cost.
	AlertThresholds map[string]float64 `mapstructure:"alert_thresholds"`
}

// TracingCfg controls OpenTelemetry distributed tracing.
type TracingCfg struct {
	// Enabled gates distributed tracing. Default false.
	Enabled bool `mapstructure:"enabled"`

	// Exporter selects the trace exporter: "noop", "stdout", or "otlp_http".
	// Default "noop".
	Exporter string `mapstructure:"exporter"`

	// OTLPEndpoint is the OTLP/HTTP collector endpoint.
	// Required when Exporter is "otlp_http".
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`

	// ServiceName is the OpenTelemetry service.name attribute. Default "substrate".
	ServiceName string `mapstructure:"service_name"`
}

// ToTracingConfig converts TracingCfg to a [TracingConfig] value suitable for
// [NewTracer].
func (c TracingCfg) ToTracingConfig() TracingConfig {
	return TracingConfig(c)
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

	// HealthPath is the HTTP path for the liveness health endpoint. Defaults to "/health".
	HealthPath string `mapstructure:"health_path"`

	// ReadyPath is the HTTP path for the readiness endpoint. Defaults to "/ready".
	ReadyPath string `mapstructure:"ready_path"`
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

	// MaxFileSizeMB is the maximum NDJSON file size in megabytes before rotation.
	// Zero disables rotation. Only used by the "file" backend.
	MaxFileSizeMB int `mapstructure:"max_file_size_mb"`

	// DSN is the SQLite data source name. Defaults to "substrate.db".
	// Only used by the "sqlite" backend.
	DSN string `mapstructure:"dsn"`
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
		MaxFileSizeMB:      c.MaxFileSizeMB,
		DSN:                c.DSN,
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

// FaultCfg is the YAML-friendly configuration for fault injection.
// Use [FaultCfg.ToFaultConfig] to convert it for use with [NewFaultController].
type FaultCfg struct {
	// Enabled gates fault injection. Default false.
	Enabled bool `mapstructure:"enabled"`

	// Rules is the ordered list of fault injection rules.
	Rules []FaultRuleCfg `mapstructure:"rules"`
}

// FaultRuleCfg is the YAML representation of a single fault injection rule.
type FaultRuleCfg struct {
	// Service restricts the rule to a specific AWS service name. Empty matches all.
	Service string `mapstructure:"service"`

	// Operation restricts the rule to a specific AWS operation. Empty matches all.
	Operation string `mapstructure:"operation"`

	// FaultType selects the fault kind: "error" or "latency".
	FaultType string `mapstructure:"fault_type"`

	// ErrorCode is the AWS error code injected when FaultType is "error".
	ErrorCode string `mapstructure:"error_code"`

	// HTTPStatus is the HTTP status code for injected errors. Default 500.
	HTTPStatus int `mapstructure:"http_status"`

	// ErrorMsg is the human-readable message for injected errors.
	ErrorMsg string `mapstructure:"error_message"`

	// LatencyMs is the injected delay in milliseconds when FaultType is "latency".
	LatencyMs int `mapstructure:"latency_ms"`

	// Probability is the fraction of matching requests that trigger the fault.
	// Range [0.0, 1.0]; default 1.0.
	Probability float64 `mapstructure:"probability"`
}

// ToFaultConfig converts FaultCfg to the [FaultConfig] type used by
// [NewFaultController].
func (c FaultCfg) ToFaultConfig() FaultConfig {
	rules := make([]FaultRule, len(c.Rules))
	for i, r := range c.Rules {
		rules[i] = FaultRule(r)
	}
	return FaultConfig{
		Enabled: c.Enabled,
		Rules:   rules,
	}
}

// RegionCfg controls multi-region request routing.
type RegionCfg struct {
	// Default is the region used when a request carries no region information.
	// Default "us-east-1".
	Default string `mapstructure:"default"`

	// Allowed is the allowlist of accepted regions. An empty slice means all
	// regions are accepted.
	Allowed []string `mapstructure:"allowed"`
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Address:         ":4566",
			ReadTimeout:     "30s",
			WriteTimeout:    "30s",
			ShutdownTimeout: "10s",
			HealthPath:      "/health",
			ReadyPath:       "/ready",
		},
		EventStore: EventStoreCfg{
			Enabled:            true,
			Backend:            "memory",
			SnapshotInterval:   100,
			MaxInMemory:        0,
			IncludeBodies:      false,
			IncludeStateHashes: false,
			MaxFileSizeMB:      0,
			DSN:                "substrate.db",
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
		Metrics: MetricsCfg{
			Enabled: false,
			Path:    "/metrics",
		},
		Forecast: ForecastCfg{
			Enabled:               false,
			WindowDays:            30,
			HorizonDays:           7,
			AnomalyThresholdSigma: 2.0,
		},
		Tracing: TracingCfg{
			Enabled:     false,
			Exporter:    "noop",
			ServiceName: "substrate",
		},
		Fault: FaultCfg{
			Enabled: false,
		},
		Region: RegionCfg{
			Default: "us-east-1",
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
	v.SetDefault("server.health_path", defaults.Server.HealthPath)
	v.SetDefault("server.ready_path", defaults.Server.ReadyPath)
	v.SetDefault("event_store.enabled", defaults.EventStore.Enabled)
	v.SetDefault("event_store.backend", defaults.EventStore.Backend)
	v.SetDefault("event_store.snapshot_interval", defaults.EventStore.SnapshotInterval)
	v.SetDefault("event_store.max_in_memory", defaults.EventStore.MaxInMemory)
	v.SetDefault("event_store.persist_path", defaults.EventStore.PersistPath)
	v.SetDefault("event_store.include_bodies", defaults.EventStore.IncludeBodies)
	v.SetDefault("event_store.include_state_hashes", defaults.EventStore.IncludeStateHashes)
	v.SetDefault("event_store.max_file_size_mb", defaults.EventStore.MaxFileSizeMB)
	v.SetDefault("event_store.dsn", defaults.EventStore.DSN)
	v.SetDefault("state.backend", defaults.State.Backend)
	v.SetDefault("state.path", defaults.State.Path)
	v.SetDefault("log.level", defaults.Log.Level)
	v.SetDefault("log.format", defaults.Log.Format)
	v.SetDefault("quotas.enabled", defaults.Quotas.Enabled)
	v.SetDefault("consistency.enabled", defaults.Consistency.Enabled)
	v.SetDefault("consistency.propagation_delay", defaults.Consistency.PropagationDelay)
	v.SetDefault("consistency.affected_services", defaults.Consistency.AffectedServices)
	v.SetDefault("costs.enabled", defaults.Costs.Enabled)
	v.SetDefault("metrics.enabled", defaults.Metrics.Enabled)
	v.SetDefault("metrics.path", defaults.Metrics.Path)
	v.SetDefault("forecast.enabled", defaults.Forecast.Enabled)
	v.SetDefault("forecast.window_days", defaults.Forecast.WindowDays)
	v.SetDefault("forecast.horizon_days", defaults.Forecast.HorizonDays)
	v.SetDefault("forecast.anomaly_threshold_sigma", defaults.Forecast.AnomalyThresholdSigma)
	v.SetDefault("tracing.enabled", defaults.Tracing.Enabled)
	v.SetDefault("tracing.exporter", defaults.Tracing.Exporter)
	v.SetDefault("tracing.service_name", defaults.Tracing.ServiceName)
	v.SetDefault("fault.enabled", defaults.Fault.Enabled)
	v.SetDefault("region.default", defaults.Region.Default)

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

	if cfg.Metrics.Enabled && cfg.Metrics.Path == "" {
		return fmt.Errorf("metrics.path must not be empty when metrics.enabled is true")
	}

	if cfg.Tracing.Enabled && cfg.Tracing.Exporter == "otlp_http" && cfg.Tracing.OTLPEndpoint == "" {
		return fmt.Errorf("tracing.otlp_endpoint must be set when tracing.exporter is otlp_http")
	}

	for i, rule := range cfg.Fault.Rules {
		if rule.FaultType != "error" && rule.FaultType != "latency" && rule.FaultType != "" {
			return fmt.Errorf("fault.rules[%d].fault_type %q is not valid; choose error or latency", i, rule.FaultType)
		}
		if rule.Probability < 0 || rule.Probability > 1 {
			return fmt.Errorf("fault.rules[%d].probability %g is out of range [0.0, 1.0]", i, rule.Probability)
		}
	}

	return nil
}
