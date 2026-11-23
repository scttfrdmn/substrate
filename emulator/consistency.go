package emulator

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ConsistencyConfig holds configuration for the eventual-consistency
// simulation controller.
type ConsistencyConfig struct {
	// Enabled gates consistency simulation. Disabled by default to preserve
	// backward compatibility with existing tests that do write→immediate read.
	Enabled bool

	// PropagationDelay is the window after a mutating request during which
	// reads to the same resource key are rejected with 409.
	PropagationDelay time.Duration

	// AffectedServices lists the services that participate in consistency
	// simulation. Requests to other services are always allowed.
	AffectedServices []string
}

// ConsistencyController simulates eventual-consistency propagation delays for
// AWS services. After a mutating request completes, subsequent reads to the
// same resource key within the configured [ConsistencyConfig.PropagationDelay]
// return HTTP 409 InconsistentStateException.
//
// The controller is opt-in (disabled by default). A replay re-decides it: replayed
// writes open windows at their recorded instants and replayed reads are checked
// against them, so a recorded refusal replays as the same refusal (#1601). The replay
// rewinds the controller first; see [ConsistencyController.rewindForReplay]. As with
// [QuotaController.CheckQuota], that is exact only for serialized traffic: overlapping
// requests decide in lock order live and in sequence order on replay.
type ConsistencyController struct {
	mu       sync.RWMutex
	writes   map[string]time.Time // resource key → write expiry time
	cfg      ConsistencyConfig
	tc       *TimeController
	affected map[string]bool
}

// NewConsistencyController creates a ConsistencyController with the provided
// configuration and time source. An error is returned when
// ConsistencyConfig.PropagationDelay is zero and simulation is enabled.
func NewConsistencyController(cfg ConsistencyConfig, tc *TimeController) (*ConsistencyController, error) {
	if cfg.Enabled && cfg.PropagationDelay <= 0 {
		return nil, fmt.Errorf("consistency propagation_delay must be positive when enabled")
	}

	affected := make(map[string]bool, len(cfg.AffectedServices))
	for _, svc := range cfg.AffectedServices {
		affected[strings.ToLower(svc)] = true
	}

	return &ConsistencyController{
		writes:   make(map[string]time.Time),
		cfg:      cfg,
		tc:       tc,
		affected: affected,
	}, nil
}

// UpdateConfig replaces the consistency configuration. Pending write expiry
// entries are preserved; they expire under the old delay. It is safe to call
// concurrently with CheckRead and RecordWrite.
func (c *ConsistencyController) UpdateConfig(cfg ConsistencyConfig) {
	affected := make(map[string]bool, len(cfg.AffectedServices))
	for _, svc := range cfg.AffectedServices {
		affected[strings.ToLower(svc)] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg = cfg
	c.affected = affected
}

// CheckRead returns an [*AWSError] with code InconsistentStateException (HTTP
// 409) when the requested resource was recently mutated and the propagation
// delay has not yet elapsed. It returns nil when simulation is disabled, the
// service is not in the affected list, or the window has passed.
func (c *ConsistencyController) CheckRead(_ *RequestContext, req *AWSRequest) error {
	if !c.cfg.Enabled {
		return nil
	}
	if isMutating(req.Operation) {
		return nil
	}
	if !c.affected[strings.ToLower(req.Service)] {
		return nil
	}

	key := resourceKey(req)
	if key == "" {
		return nil
	}

	now := c.tc.Now()

	c.mu.RLock()
	expiry, ok := c.writes[key]
	c.mu.RUnlock()

	if ok && now.Before(expiry) {
		return &AWSError{
			Code: "InconsistentStateException",
			Message: fmt.Sprintf(
				"resource %q was recently modified; try again after %s",
				key, expiry.Format(time.RFC3339),
			),
			HTTPStatus: http.StatusConflict,
		}
	}

	return nil
}

// RecordWrite records a mutating operation against the resource key derived
// from req. It is a no-op when simulation is disabled or the service is not
// affected.
func (c *ConsistencyController) RecordWrite(_ *RequestContext, req *AWSRequest) {
	if !c.cfg.Enabled {
		return
	}
	if !c.affected[strings.ToLower(req.Service)] {
		return
	}

	key := resourceKey(req)
	if key == "" {
		return
	}

	expiry := c.tc.Now().Add(c.cfg.PropagationDelay)

	c.mu.Lock()
	c.writes[key] = expiry
	c.mu.Unlock()
}

// rewindForReplay forgets every open propagation window, which a run opens as it goes,
// so a replay starts from the controller's start-of-run state.
func (c *ConsistencyController) rewindForReplay() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = make(map[string]time.Time)
}

// enabled reports whether the controller simulates anything. A nil controller does not.
func (c *ConsistencyController) enabled() bool { return c.config().Enabled }

// config returns the controller's configuration, or a disabled one for a nil controller.
func (c *ConsistencyController) config() ConsistencyConfig {
	if c == nil {
		return ConsistencyConfig{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// isMutating reports whether operation is a state-mutating AWS API call using
// a prefix heuristic. Recognized prefixes: Create, Put, Attach, Delete,
// Update, Add, Remove, Set, Tag, Untag, Enable, Disable.
func isMutating(operation string) bool {
	mutatingPrefixes := []string{
		"Create", "Put", "Attach", "Delete", "Update",
		"Add", "Remove", "Set", "Tag", "Untag", "Enable", "Disable",
		"Copy", "Upload", "Complete", "Abort",
	}
	for _, prefix := range mutatingPrefixes {
		if strings.HasPrefix(operation, prefix) {
			return true
		}
	}
	return false
}

// resourceKey derives a stable string key for the resource targeted by req.
// Format: "service/primaryParam". Returns an empty string when no primary
// parameter can be identified.
func resourceKey(req *AWSRequest) string {
	primary := primaryParam(req)
	if primary == "" {
		return ""
	}
	return strings.ToLower(req.Service) + "/" + primary
}

// primaryParam extracts the most relevant identifying parameter from req.
// It checks Params for well-known keys (UserName, RoleName, GroupName,
// BucketName, TableName, FunctionName, PolicyArn, AccessKeyId) in priority
// order.
func primaryParam(req *AWSRequest) string {
	candidates := []string{
		"UserName", "RoleName", "GroupName", "BucketName",
		"TableName", "FunctionName", "PolicyArn", "AccessKeyId",
		"Name",
	}
	for _, k := range candidates {
		if v := req.Params[k]; v != "" {
			return v
		}
	}
	return ""
}
