package emulator

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the full Substrate server configuration.
type Config struct {
	// Server controls network binding and timeout behavior.
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

	// Credentials controls the credential registry and SigV4 verification.
	Credentials CredentialsCfg `mapstructure:"credentials"`

	// Costs controls per-request cost estimation.
	Costs CostCfg `mapstructure:"costs"`

	// Pricing controls the pricing data source (static or AWS).
	Pricing PricingCfg `mapstructure:"pricing"`

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

	// Account controls the AWS account requests are attributed to.
	Account AccountCfg `mapstructure:"account"`

	// Lambda controls Lambda Docker execution behavior.
	Lambda LambdaCfg `mapstructure:"lambda"`

	// RDS controls RDS backend behavior.
	RDS RDSCfg `mapstructure:"rds"`
}

// MetricsCfg controls Prometheus metrics exposure.
type MetricsCfg struct {
	// Enabled gates the /metrics endpoint. Default false.
	Enabled bool `mapstructure:"enabled"`

	// Path is the HTTP path for the metrics endpoint. Default "/metrics".
	Path string `mapstructure:"path"`
}

// ForecastCfg controls cost forecasting behavior.
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

	// CORS configures cross-origin resource sharing for browser-based clients.
	CORS CORSConfig `mapstructure:"cors"`
}

// CORSConfig configures CORS headers and preflight handling. It is off by
// default (non-browser use needs no CORS); enable it to let browser-based AWS
// SDK clients (served from another origin) reach the emulator.
type CORSConfig struct {
	// Enabled turns on the CORS middleware and OPTIONS preflight handling.
	Enabled bool `mapstructure:"enabled"`

	// AllowedOrigins is the list of permitted origins. Empty (with Enabled) means
	// reflect any origin ("*") — reasonable for a local dev emulator.
	AllowedOrigins []string `mapstructure:"allowed_origins"`
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

// LogCfg controls logging behavior.
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

// CredentialsCfg is the YAML-friendly configuration for the credential
// registry. Use [CredentialsCfg.ToCredentialRegistry] to build the registry it
// describes.
type CredentialsCfg struct {
	// Enabled gates the whole section. Default false, which wires no registry at
	// all: every caller resolves to Account.Default and no signature is checked.
	Enabled bool `mapstructure:"enabled"`

	// VerifySignatures gates SigV4 enforcement. Default true, because this
	// section has documented `enabled` as "enable SigV4 signature verification"
	// since it was written and `enabled: true` on its own must keep meaning that.
	//
	// Set it to false for the combination [ServerOptions.VerifySignatures] opened
	// up (#630): resolve an account per access key without requiring any request
	// to be signed. That is the useful setting for a multi-account test, where the
	// point of the registry is attribution rather than authentication.
	//
	// It does nothing while Enabled is false — there is no key material to check a
	// signature against.
	VerifySignatures bool `mapstructure:"verify_signatures"`

	// Entries are the credentials the registry holds in addition to the built-in
	// ones [NewCredentialRegistry] seeds. An entry whose AccessKeyID collides with
	// a built-in replaces it, which is how a test moves AKIATEST12345678901 into
	// another account.
	Entries []CredentialEntryCfg `mapstructure:"entries"`
}

// CredentialEntryCfg is one credential in a [CredentialsCfg], the YAML-friendly
// spelling of [CredentialEntry].
type CredentialEntryCfg struct {
	// AccessKeyID is the access key a request signs with. Required.
	AccessKeyID string `mapstructure:"access_key_id"`

	// SecretAccessKey is the secret the signature is checked against. Required
	// when VerifySignatures is on, and pointless without it.
	SecretAccessKey string `mapstructure:"secret_access_key"`

	// AccountID is the account this credential resolves to. An empty value adopts
	// Account.Default, which is the whole point of the field being optional: an
	// entry that exists only to make a key signable needs no account of its own.
	AccountID string `mapstructure:"account_id"`

	// SessionToken is non-empty for a temporary credential. Substrate does not
	// check it; it is recorded so a fixture can carry one.
	SessionToken string `mapstructure:"session_token"`
}

// ToCredentialRegistry builds the [CredentialRegistry] this section describes, or
// nil when Enabled is false. A nil registry is what [ServerOptions.Credentials]
// takes to mean "attribute every caller to the default account".
//
// defaultAccount fills in an entry that names none; pass Account.Default.
func (c CredentialsCfg) ToCredentialRegistry(defaultAccount string) *CredentialRegistry {
	if !c.Enabled {
		return nil
	}
	reg := NewCredentialRegistry()
	c.RegisterInto(reg, defaultAccount)
	return reg
}

// RegisterInto adds this section's entries to an existing registry, replacing any
// entry with the same access key ID. It exists for SIGHUP reload, where the
// registry the server holds cannot be swapped but can be added to — so a
// consumer who appends an entry and signals the process reaches it without a
// restart. Enabled and VerifySignatures are read once at startup and are not
// reconsidered here.
//
// defaultAccount fills in an entry that names none; pass Account.Default.
func (c CredentialsCfg) RegisterInto(reg *CredentialRegistry, defaultAccount string) {
	if reg == nil {
		return
	}
	for _, e := range c.Entries {
		accountID := e.AccountID
		if accountID == "" {
			accountID = defaultAccount
		}
		reg.Register(CredentialEntry{
			AccessKeyID:     e.AccessKeyID,
			SecretAccessKey: e.SecretAccessKey,
			AccountID:       accountID,
			SessionToken:    e.SessionToken,
		})
	}
}

// CostCfg is the YAML-friendly configuration for cost tracking.
// Use [CostCfg.ToCostConfig] to convert it for use with [NewCostController].
type CostCfg struct {
	// Enabled gates cost estimation. Default true.
	Enabled bool `mapstructure:"enabled"`

	// Overrides maps "service/operation" or "service" keys to USD per request,
	// overriding the built-in pricing table.
	Overrides map[string]float64 `mapstructure:"overrides"`

	// Discounts configures percentage and fixed-rate discounts.
	Discounts DiscountConfig `mapstructure:"discounts"`
}

// ToCostConfig converts CostCfg to the [CostConfig] type used by
// [NewCostController].
func (c CostCfg) ToCostConfig() CostConfig {
	return CostConfig(c)
}

// PricingCfg is the YAML-friendly configuration for the pricing data source.
type PricingCfg struct {
	// Provider selects the pricing source: "static" (default) or "aws".
	Provider string `mapstructure:"provider"`

	// CachePath is the file path for the pricing cache. Default
	// ~/.substrate/pricing-cache.json.
	CachePath string `mapstructure:"cachePath"`

	// CacheTTLHours is the cache time-to-live in hours. Default 24.
	CacheTTLHours int `mapstructure:"cacheTTLHours"`

	// Region is the AWS region for pricing lookups. Default us-east-1.
	Region string `mapstructure:"region"`
}

// FaultCfg is the YAML-friendly configuration for fault injection.
// Use [FaultCfg.ToFaultConfig] to convert it for use with [NewFaultController].
type FaultCfg struct {
	// Enabled gates fault injection. Default false.
	//
	// A disabled controller is still constructed, so the /v1/fault/rules
	// endpoints work and a harness can arm rules over the wire on a server whose
	// config file says nothing about faults.
	Enabled bool `mapstructure:"enabled"`

	// Seed controls the per-rule PRNGs behind [FaultRule.Probability]. Default 0,
	// which is deterministic — the same config produces the same run every time,
	// which is what Substrate is for. Set it to vary probabilistic outcomes
	// between runs deliberately.
	Seed int64 `mapstructure:"seed"`

	// Rules is the ordered list of fault injection rules.
	Rules []FaultRuleCfg `mapstructure:"rules"`
}

// FaultRuleCfg is the YAML representation of a single fault injection rule.
type FaultRuleCfg struct {
	// Service restricts the rule to a specific AWS service name. Empty matches all.
	Service string `mapstructure:"service"`

	// Operation restricts the rule to a specific AWS operation. Empty matches all.
	// The name is the semantic operation for every service, S3 included; see
	// [FaultRule.Operation].
	Operation string `mapstructure:"operation"`

	// PathSuffix restricts the rule to requests whose path ends with this string.
	// Empty matches all. See [FaultRule.PathSuffix].
	PathSuffix string `mapstructure:"path_suffix"`

	// QueryKey restricts the rule to requests carrying this query parameter.
	// Empty matches all. See [FaultRule.QueryKey].
	QueryKey string `mapstructure:"query_key"`

	// HeaderPrefix restricts the rule to requests carrying a header whose name
	// begins with this prefix. Empty matches all. See [FaultRule.HeaderPrefix].
	HeaderPrefix string `mapstructure:"header_prefix"`

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

	// Times bounds how many matching requests the rule fires on. Zero means one
	// and a negative value means unlimited; see [FaultRule.Times] for why zero is
	// not unlimited.
	Times int `mapstructure:"times"`
}

// ToFaultConfig converts FaultCfg to the [FaultConfig] type used by
// [NewFaultController].
//
// The fields are copied one by one rather than converted wholesale: [FaultRule]
// carries a Fired count the controller maintains, which has no place in a
// configuration file, so the two structs are deliberately not identical.
func (c FaultCfg) ToFaultConfig() FaultConfig {
	rules := make([]FaultRule, len(c.Rules))
	for i, r := range c.Rules {
		rules[i] = FaultRule{
			Service:      r.Service,
			Operation:    r.Operation,
			PathSuffix:   r.PathSuffix,
			QueryKey:     r.QueryKey,
			HeaderPrefix: r.HeaderPrefix,
			FaultType:    r.FaultType,
			ErrorCode:    r.ErrorCode,
			HTTPStatus:   r.HTTPStatus,
			ErrorMsg:     r.ErrorMsg,
			LatencyMs:    r.LatencyMs,
			Probability:  r.Probability,
			Times:        r.Times,
		}
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

// AccountCfg controls the AWS account a request is attributed to.
type AccountCfg struct {
	// Default is the account ID every caller resolves to. Default "123456789012",
	// AWS's documented example account.
	//
	// It is the *default*, not the answer: a wired [CredentialRegistry] names the
	// account per access key and an STS session record names the account it was
	// issued for, and both take precedence. Set this when a test needs the whole
	// server to sit in a particular account without signing for it — a fixture
	// asserting on ARNs captured from a real account, for one.
	Default string `mapstructure:"default"`
}

// LambdaCfg controls Lambda Docker execution behavior.
type LambdaCfg struct {
	// DockerEnabled gates the Docker-based Lambda execution engine. Default false.
	// When false (or when Docker is unavailable), a stub response is returned.
	DockerEnabled bool `mapstructure:"docker_enabled"`

	// ReplayMode selects how Lambda invocations are served: "live" executes the
	// function container; "recorded" returns a cached result when available.
	// Default "live".
	ReplayMode string `mapstructure:"replay_mode"`

	// WarmPoolTTL is the duration string (e.g. "5m") after which an idle Lambda
	// container is stopped and removed. Default "5m".
	WarmPoolTTL string `mapstructure:"warm_pool_ttl"`
}

// RDSCfg controls RDS backend behavior.
type RDSCfg struct {
	// Engine selects the RDS backend: "stub" returns synthetic endpoints;
	// "container" starts a real Postgres Docker container. Default "stub".
	Engine string `mapstructure:"engine"`
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
		Credentials: CredentialsCfg{
			Enabled: false,
			// True so that `credentials: {enabled: true}` alone means what the
			// section has always documented it to mean (#736).
			VerifySignatures: true,
		},
		Costs: CostCfg{
			Enabled: true,
		},
		Pricing: PricingCfg{
			Provider:      "static",
			CachePath:     "~/.substrate/pricing-cache.json",
			CacheTTLHours: 24,
			Region:        "us-east-1",
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
		Account: AccountCfg{
			Default: defaultAccountID,
		},
		Lambda: LambdaCfg{
			DockerEnabled: false,
			ReplayMode:    "live",
			WarmPoolTTL:   "5m",
		},
		RDS: RDSCfg{
			Engine: "stub",
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
	v.SetDefault("credentials.enabled", defaults.Credentials.Enabled)
	v.SetDefault("credentials.verify_signatures", defaults.Credentials.VerifySignatures)
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
	v.SetDefault("fault.seed", defaults.Fault.Seed)
	v.SetDefault("region.default", defaults.Region.Default)
	v.SetDefault("account.default", defaults.Account.Default)
	v.SetDefault("lambda.docker_enabled", defaults.Lambda.DockerEnabled)
	v.SetDefault("lambda.replay_mode", defaults.Lambda.ReplayMode)
	v.SetDefault("lambda.warm_pool_ttl", defaults.Lambda.WarmPoolTTL)
	v.SetDefault("rds.engine", defaults.RDS.Engine)

	// Environment variable overrides.
	v.SetEnvPrefix("SUBSTRATE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Load YAML file if provided or discoverable.
	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("substrate")
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

	// Entries are checked whether or not the section is enabled: a typo in a
	// credential should not stay hidden until someone flips the flag, and the
	// registry's Register would silently accept every one of these mistakes.
	seenAccessKeys := make(map[string]int, len(cfg.Credentials.Entries))
	for i, entry := range cfg.Credentials.Entries {
		if entry.AccessKeyID == "" {
			return fmt.Errorf("credentials.entries[%d].access_key_id must not be empty", i)
		}
		if first, dup := seenAccessKeys[entry.AccessKeyID]; dup {
			return fmt.Errorf("credentials.entries[%d].access_key_id %q duplicates entries[%d]",
				i, entry.AccessKeyID, first)
		}
		seenAccessKeys[entry.AccessKeyID] = i
		// An empty account_id adopts account.default, so only a non-empty one can
		// be wrong. A malformed one lands in every ARN the caller signing with this
		// key gets back.
		if entry.AccountID != "" && !isAccountIDPattern(entry.AccountID) {
			return fmt.Errorf("credentials.entries[%d].account_id %q is not valid; an AWS account ID is 12 digits",
				i, entry.AccountID)
		}
		// Only refused when it would actually be used: an entry that exists to
		// attribute an account needs no secret while verification is off.
		if cfg.Credentials.Enabled && cfg.Credentials.VerifySignatures && entry.SecretAccessKey == "" {
			return fmt.Errorf("credentials.entries[%d].secret_access_key must not be empty when credentials.verify_signatures is true", i)
		}
	}

	if cfg.Metrics.Enabled && cfg.Metrics.Path == "" {
		return fmt.Errorf("metrics.path must not be empty when metrics.enabled is true")
	}

	if cfg.Tracing.Enabled && cfg.Tracing.Exporter == "otlp_http" && cfg.Tracing.OTLPEndpoint == "" {
		return fmt.Errorf("tracing.otlp_endpoint must be set when tracing.exporter is otlp_http")
	}

	validReplayModes := map[string]bool{"live": true, "recorded": true}
	if !validReplayModes[cfg.Lambda.ReplayMode] {
		return fmt.Errorf("lambda.replay_mode %q is not valid; choose live or recorded", cfg.Lambda.ReplayMode)
	}

	if cfg.Lambda.WarmPoolTTL != "" {
		if _, err := time.ParseDuration(cfg.Lambda.WarmPoolTTL); err != nil {
			return fmt.Errorf("lambda.warm_pool_ttl %q is not a valid duration: %w", cfg.Lambda.WarmPoolTTL, err)
		}
	}

	// An empty value keeps the built-in default; anything else must be a real
	// account ID, because it lands in every ARN the server hands back and a
	// malformed one is a fixture that cannot be compared against AWS.
	if cfg.Account.Default != "" && !isAccountIDPattern(cfg.Account.Default) {
		return fmt.Errorf("account.default %q is not valid; an AWS account ID is 12 digits", cfg.Account.Default)
	}

	validRDSEngines := map[string]bool{"stub": true, "container": true}
	if !validRDSEngines[cfg.RDS.Engine] {
		return fmt.Errorf("rds.engine %q is not valid; choose stub or container", cfg.RDS.Engine)
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
