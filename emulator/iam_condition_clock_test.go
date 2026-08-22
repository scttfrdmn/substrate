package emulator_test

import (
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// aws:CurrentTime and aws:EpochTime come from the simulated clock (#714).
//
// The date operators are useless without a producer for the keys AWS's own examples
// condition on, and there was none: every date condition read a missing key. The clock
// is the emulator's TimeController rather than time.Now(), so a replayed request decides
// the same way it did when it was recorded — replay.go calls SetTime(event.Timestamp)
// before dispatch, and a decision sourced from the wall clock would ignore it.

// iamClockTestTime is the instant these tests seed the controller with. It is far from
// any boundary they assert against, so the sub-second advance TimeController.Now()
// applies cannot flip a comparison.
func iamClockTestTime() time.Time {
	return time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
}

// iamClockCheckAccess authorizes one s3:GetObject through an AuthController carrying the
// inline policy doc, with the clock plumbed in or left out.
func iamClockCheckAccess(t *testing.T, doc *emulator.PolicyDocument, withClock bool) error {
	t.Helper()
	state := newAuthzGroupState(t, "jill", "devs", authzGroupState{UserInline: doc})
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	opts := []emulator.AuthControllerOption{}
	if withClock {
		opts = append(opts, emulator.WithAuthTimeController(emulator.NewTimeController(iamClockTestTime())))
	}
	auth := emulator.NewAuthController(state, logger, opts...)
	return auth.CheckAccess(
		newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill"),
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"})
}

// iamClockPolicy is a one-condition Allow for s3:GetObject.
func iamClockPolicy(operator, condKey, condValue string) *emulator.PolicyDocument {
	return &emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:GetObject"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				operator: {condKey: emulator.StringOrSlice{condValue}},
			},
		}},
	}
}

// TestAuthController_ClockPopulatesTheTimeKeys walks a date and a numeric condition
// against the seeded instant, in both directions.
//
// The Null rows are the load-bearing ones: they assert the key is *populated* without
// reference to its value, so they hold whatever the clock says. The comparison rows then
// assert the rendering is one the date and numeric operators can actually read — a key
// present but written in some form condParseTime rejects would pass the Null rows and
// fail these.
func TestAuthController_ClockPopulatesTheTimeKeys(t *testing.T) {
	t.Parallel()

	now := iamClockTestTime()
	cases := []struct {
		name      string
		operator  string
		condKey   string
		condValue string
		allowed   bool
	}{
		{"CurrentTime exists", "Null", "aws:CurrentTime", "false", true},
		{"EpochTime exists", "Null", "aws:EpochTime", "false", true},
		{"after the seeded instant", "DateGreaterThan", "aws:CurrentTime", "2026-03-04T05:00:00Z", true},
		{"before the seeded instant", "DateLessThan", "aws:CurrentTime", "2026-03-04T05:00:00Z", false},
		{"before a later instant", "DateLessThan", "aws:CurrentTime", "2026-03-04T06:00:00Z", true},
		{"epoch is a number", "NumericGreaterThan", "aws:EpochTime", strconv.FormatInt(now.Add(-time.Hour).Unix(), 10), true},
		{"epoch orders correctly", "NumericGreaterThan", "aws:EpochTime", strconv.FormatInt(now.Add(time.Hour).Unix(), 10), false},
		// The two spellings must name the same instant, which a test comparing each
		// against its own literal would not catch.
		{"epoch read as a date", "DateGreaterThan", "aws:EpochTime", "2026-03-04T05:00:00Z", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := iamClockCheckAccess(t, iamClockPolicy(tc.operator, tc.condKey, tc.condValue), true)
			if tc.allowed {
				assert.NoError(t, err, "%s %s %s", tc.operator, tc.condKey, tc.condValue)
				return
			}
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDenied", awsErr.Code)
		})
	}
}

// TestAuthController_WithoutAClockTheTimeKeysAreAbsent pins the documented false deny.
//
// An AuthController built without a TimeController leaves both keys out of the request
// context rather than falling back to time.Now(). A date condition then reports a
// missing key and denies, which is wrong but safe and replay-stable; sourcing the value
// from the wall clock would make a replayed decision disagree with the recorded one.
func TestAuthController_WithoutAClockTheTimeKeysAreAbsent(t *testing.T) {
	t.Parallel()

	for _, condKey := range []string{"aws:CurrentTime", "aws:EpochTime"} {
		t.Run(condKey, func(t *testing.T) {
			t.Parallel()
			// Null:true is satisfied only by an absent key, so this row asserts absence
			// directly rather than inferring it from a denial that any bug would produce.
			assert.NoError(t, iamClockCheckAccess(t, iamClockPolicy("Null", condKey, "true"), false),
				"%s must be absent without a clock", condKey)
			assert.Error(t, iamClockCheckAccess(t, iamClockPolicy("Null", condKey, "false"), false))
		})
	}
}

// TestSimulate_ClockPopulatesTheTimeKeys is the same pair on the simulation path.
//
// A simulation reporting aws:CurrentTime in MissingContextValues while the enforcement
// gate evaluated it would contradict the enforcement it exists to predict — and the
// simulator is the only place a consumer can see that a key is missing, so the
// disagreement would be visible and misleading.
//
// The assertions are on existence rather than on the instant, because the test server's
// controller is seeded from time.Now(); the enforcement tests above own the rendering.
func TestSimulate_ClockPopulatesTheTimeKeys(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, condKey := range []string{"aws:CurrentTime", "aws:EpochTime"} {
		t.Run(condKey, func(t *testing.T) {
			policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Action":"s3:GetObject","Resource":"*",` +
				`"Condition":{"Null":{"` + condKey + `":"false"}}}]}`
			got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{policy},
			})
			require.Len(t, got.results(), 1)
			assert.Equal(t, "allowed", got.results()[0].Decision,
				"%s must be populated in a simulation too", condKey)
		})

		t.Run(condKey+"/not reported missing", func(t *testing.T) {
			policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Action":"s3:GetObject","Resource":"*",` +
				`"Condition":{"DateGreaterThan":{"` + condKey + `":"2000-01-01T00:00:00Z"}}}]}`
			got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{policy},
			})
			require.Len(t, got.results(), 1)
			assert.Empty(t, got.results()[0].MissingContextValues,
				"the simulator supplies %s itself, so it is not missing", condKey)
		})
	}
}
