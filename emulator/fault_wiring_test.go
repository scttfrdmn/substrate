package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// postRules arms a fault config over the wire.
func postRules(t *testing.T, ts *httptest.Server, cfg emulator.FaultConfig) {
	t.Helper()
	body, err := json.Marshal(cfg)
	require.NoError(t, err)
	resp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "POST /v1/fault/rules: %s", raw)
}

// TestFaultCfg_SeedRoundTrips asserts a config file can pin the PRNG seed. Before
// this field existed, the CLI seeded from time.Now().UnixNano(), so a probabilistic
// rule from a config file produced a different run every time — the one property
// this emulator exists to avoid.
func TestFaultCfg_SeedRoundTrips(t *testing.T) {
	cfg := emulator.DefaultConfig()
	assert.Equal(t, int64(0), cfg.Fault.Seed,
		"the default must be deterministic, not wall-clock")

	// The seed reaches the controller, and the same seed produces the same run.
	rules := []emulator.FaultRule{{
		Service:     "s3",
		FaultType:   "error",
		ErrorCode:   "ServiceUnavailable",
		HTTPStatus:  503,
		Probability: 0.5,
		Times:       -1,
	}}
	fcfg := emulator.FaultCfg{Enabled: true, Seed: 1234}
	converted := fcfg.ToFaultConfig()
	converted.Rules = rules

	trace := func(seed int64) []bool {
		fc := emulator.NewFaultController(converted, seed)
		got := make([]bool, 20)
		for i := range got {
			awsErr, _ := fc.InjectFault(&emulator.RequestContext{},
				&emulator.AWSRequest{Service: "s3", Operation: "PutObject"})
			got[i] = awsErr != nil
		}
		return got
	}
	want := trace(fcfg.Seed)
	assert.Equal(t, want, trace(fcfg.Seed), "a pinned seed must reproduce a run")
	assert.NotEqual(t, want, trace(fcfg.Seed+1), "a different seed must differ")

	// Not vacuous: the probe must actually vary, or "reproducible" is trivial.
	assert.Contains(t, want, true)
	assert.Contains(t, want, false)
}

// TestFaultControl_DisabledControllerAcceptsRules is the wiring gate. A server
// started with no fault: block in its config now still has a controller, so a
// harness can arm a rule over the wire — the remedy the docs point at, which
// answered 501 before. The controller is disabled until something arms it, so
// behavior is unchanged for a run that never touches the endpoint.
func TestFaultControl_DisabledControllerAcceptsRules(t *testing.T) {
	ts, fc := newFaultTestServer(t)
	require.NotNil(t, fc)

	// Nothing is injected before a rule is armed.
	awsErr, delay := fc.InjectFault(&emulator.RequestContext{},
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject"})
	assert.Nil(t, awsErr, "a disabled controller must inject nothing")
	assert.Zero(t, delay)

	// Arming over the wire works and the rule then fires.
	postRules(t, ts, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			FaultType:  "error",
			ErrorCode:  "InternalError",
			HTTPStatus: 500,
			Times:      1,
		}},
	})

	awsErr, _ = fc.InjectFault(&emulator.RequestContext{},
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject"})
	require.NotNil(t, awsErr, "a rule armed over the wire must fire")
	assert.Equal(t, "InternalError", awsErr.Code)
	assert.Equal(t, 1, fc.GetConfig().Rules[0].Fired)
}
