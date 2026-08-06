package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// probeRule returns a rule that fires on one service half the time, unbounded, so
// its outcome sequence is a PRNG trace rather than a Times bound.
func probeRule(service string) emulator.FaultRule {
	return emulator.FaultRule{
		Service:     service,
		FaultType:   "error",
		ErrorCode:   "ServiceUnavailable",
		HTTPStatus:  503,
		Probability: 0.5,
		Times:       -1,
	}
}

// outcomes drives n requests against one service and records which fired, which is
// the observable trace of that rule's PRNG stream.
func outcomes(fc *emulator.FaultController, service string, n int) []bool {
	got := make([]bool, n)
	for i := range got {
		awsErr, _ := fc.InjectFault(&emulator.RequestContext{},
			&emulator.AWSRequest{Service: service, Operation: "PutObject"})
		got[i] = awsErr != nil
	}
	return got
}

// TestFaultController_RuleStreamsAreIndependent is #510's acceptance property: a
// rule's outcome sequence must not depend on how many requests *another* rule
// matched. With one shared PRNG it did — every roll rule 1 took shifted rule 2's
// entire sequence — so a fixed seed reproduced a run only while request ordering
// was unchanged, which is the opposite of what a deterministic emulator is for.
func TestFaultController_RuleStreamsAreIndependent(t *testing.T) {
	cfg := func() emulator.FaultConfig {
		return emulator.FaultConfig{
			Enabled: true,
			Rules:   []emulator.FaultRule{probeRule("sqs"), probeRule("s3")},
		}
	}

	// Baseline: rule 1 (sqs) matches nothing, so only rule 2's stream advances.
	baseline := outcomes(emulator.NewFaultController(cfg(), 42), "s3", 20)

	// Now drive rule 1 hard first. Under a shared PRNG this consumed rolls that
	// rule 2 would otherwise have taken, and every s3 outcome moved.
	fc := emulator.NewFaultController(cfg(), 42)
	outcomes(fc, "sqs", 7)
	withNoise := outcomes(fc, "s3", 20)

	assert.Equal(t, baseline, withNoise,
		"rule 2's outcomes must not depend on how many requests rule 1 matched")

	// Interleaving the two must not shift either, for the same reason.
	fc = emulator.NewFaultController(cfg(), 42)
	interleaved := make([]bool, 0, 20)
	for i := 0; i < 20; i++ {
		outcomes(fc, "sqs", 1)
		interleaved = append(interleaved, outcomes(fc, "s3", 1)...)
	}
	assert.Equal(t, baseline, interleaved, "interleaving must not shift a rule's stream")
}

// TestFaultController_UnrelatedRuleDoesNotShiftOutcomes is the config-editing half
// of the same property: adding a rule that never matches must leave the existing
// rules' outcomes alone. Under a shared PRNG this held only because the added rule
// never rolled; a rule added *before* another still shifts it, which is why the
// stream is keyed by index and the docs say so.
func TestFaultController_UnrelatedRuleDoesNotShiftOutcomes(t *testing.T) {
	one := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3")},
	}, 7)
	two := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3"), probeRule("sqs")},
	}, 7)

	assert.Equal(t, outcomes(one, "s3", 15), outcomes(two, "s3", 15),
		"appending a rule must not change an existing rule's outcomes")
}

// TestFaultController_FixedSeedStillReproduces asserts independence did not cost
// reproducibility, which is the whole point of a seed. Two controllers built from
// the same seed and config must trace identically, and a different seed must not.
func TestFaultController_FixedSeedStillReproduces(t *testing.T) {
	cfg := emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3"), probeRule("sqs")},
	}

	first := emulator.NewFaultController(cfg, 99)
	second := emulator.NewFaultController(cfg, 99)
	trace := func(fc *emulator.FaultController) []bool {
		return append(outcomes(fc, "s3", 12), outcomes(fc, "sqs", 12)...)
	}
	want := trace(first)
	assert.Equal(t, want, trace(second), "a fixed seed must reproduce a run exactly")

	// Not vacuous: the trace must actually contain both outcomes, or "identical"
	// would be satisfied by a rule that never fires.
	assert.Contains(t, want, true, "the probe must fire sometimes")
	assert.Contains(t, want, false, "the probe must decline sometimes")

	assert.NotEqual(t, want, trace(emulator.NewFaultController(cfg, 100)),
		"a different seed must produce a different run")
}

// TestFaultController_AdjacentRulesAreNotCorrelated asserts two adjacent rules
// with the same probability do not trace identically, which is what a fixture
// using both to model independent failures depends on. It does not distinguish
// FNV-mixing the index from adding it — measured, both decorrelate adjacent rules
// equally well on this scale — and it is not meant to; see [ruleRNGs] for why the
// mixing is there anyway.
func TestFaultController_AdjacentRulesAreNotCorrelated(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3"), probeRule("sqs")},
	}, 1)

	first := outcomes(fc, "s3", 30)
	second := outcomes(fc, "sqs", 30)
	assert.NotEqual(t, first, second,
		"two rules with the same probability must not trace identically")
}

// TestFaultController_ReArmingResetsStreams asserts the streams are re-derived on
// UpdateConfig, exactly as the fired counts are. A fixture that re-arms between
// phases gets its full budget back; it must get the same rolls back too, or the
// second phase of a two-phase test is not reproducible.
func TestFaultController_ReArmingResetsStreams(t *testing.T) {
	cfg := emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3")},
	}
	fc := emulator.NewFaultController(cfg, 5)
	first := outcomes(fc, "s3", 15)

	fc.UpdateConfig(cfg)
	assert.Equal(t, first, outcomes(fc, "s3", 15),
		"re-arming a config must reset the PRNG streams, as it resets fired counts")

	// A config armed with more rules gets a stream for each of them.
	fc.UpdateConfig(emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{probeRule("s3"), probeRule("sqs")},
	})
	assert.Equal(t, first, outcomes(fc, "s3", 15),
		"rule 0's stream is unchanged by a rule appended after it")
	require.NotEmpty(t, outcomes(fc, "sqs", 5), "the appended rule must be armed")
}
