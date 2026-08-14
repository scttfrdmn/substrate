package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/account"
	accounttypes "github.com/aws/aws-sdk-go-v2/service/account/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_AccountRegionOptIn is #629 at the SDK level: the Region opt-in
// baseline a consumer walks before it deploys anything.
//
// This level is the one that matters most for a brand-new namespace. The account
// service sends no X-Amz-Target and carries its operation entirely in the URL, so
// routing depends on the Host header, the SigV4 credential scope and an exact path
// match — none of which a unit test driving the plugin through a hand-built request
// exercises. #561 and #610 were both a plugin that was registered, fully
// unit-tested, and unreachable from every SDK.
func TestJourney_AccountRegionOptIn(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so each assertion is about the first response rather than
	// whatever the retry loop settled on.
	acct := account.NewFromConfig(cfg, func(o *account.Options) { o.RetryMaxAttempts = 1 })

	// --- the baseline a consumer reads first ---
	listed, err := acct.ListRegions(ctx, &account.ListRegionsInput{})
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	byName := make(map[string]accounttypes.RegionOptStatus, len(listed.Regions))
	for _, r := range listed.Regions {
		byName[aws.ToString(r.RegionName)] = r.RegionOptStatus
	}
	if len(byName) != 34 {
		t.Fatalf("ListRegions reported %d Regions, want 34 (17 default + 17 opt-in)", len(byName))
	}
	// The statuses come back as SDK enum members rather than strings, which is what
	// a consumer switches on. A value outside the enum would deserialize into
	// something no case matches.
	if got := byName["us-east-1"]; got != accounttypes.RegionOptStatusEnabledByDefault {
		t.Errorf("us-east-1 is %q, want ENABLED_BY_DEFAULT", got)
	}
	if got := byName["af-south-1"]; got != accounttypes.RegionOptStatusDisabled {
		t.Errorf("af-south-1 is %q, want DISABLED", got)
	}

	// --- the asynchronous enable, polled the way a waiter does ---
	// EnableRegion has no output shape in the model, so the SDK's output struct
	// carries only ResultMetadata; the absence of an error is the whole answer.
	if _, err := acct.EnableRegion(ctx, &account.EnableRegionInput{
		RegionName: aws.String("af-south-1"),
	}); err != nil {
		t.Fatalf("EnableRegion(af-south-1): %v", err)
	}

	first, err := acct.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{
		RegionName: aws.String("af-south-1"),
	})
	if err != nil {
		t.Fatalf("GetRegionOptStatus: %v", err)
	}
	if first.RegionOptStatus != accounttypes.RegionOptStatusEnabling {
		t.Errorf("the first poll reads %q, want ENABLING — the in-flight status is only observable here, since EnableRegion has no output", first.RegionOptStatus)
	}
	if got := aws.ToString(first.RegionName); got != "af-south-1" {
		t.Errorf("GetRegionOptStatus answered for %q, want af-south-1", got)
	}

	// Two more polls: the second converges, and the terminal status then holds. A
	// status that flipped back would make a waiter comparing successive polls loop
	// forever, which is the failure this asserts against.
	for i := range 3 {
		got, pollErr := acct.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{
			RegionName: aws.String("af-south-1"),
		})
		if pollErr != nil {
			t.Fatalf("GetRegionOptStatus poll %d: %v", i+2, pollErr)
		}
		if got.RegionOptStatus != accounttypes.RegionOptStatusEnabled {
			t.Fatalf("poll %d reads %q, want ENABLED", i+2, got.RegionOptStatus)
		}
	}

	// --- idempotence: the "ensure" semantics #629 came from ---
	if _, err := acct.EnableRegion(ctx, &account.EnableRegionInput{
		RegionName: aws.String("af-south-1"),
	}); err != nil {
		t.Fatalf("re-enabling an ENABLED Region: %v — an ensure pass must be safe to re-run", err)
	}
	after, err := acct.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{
		RegionName: aws.String("af-south-1"),
	})
	if err != nil {
		t.Fatalf("GetRegionOptStatus after the redundant enable: %v", err)
	}
	if after.RegionOptStatus != accounttypes.RegionOptStatusEnabled {
		t.Errorf("af-south-1 is %q after a redundant enable, want ENABLED — the Region must not go backwards", after.RegionOptStatus)
	}

	// The filter reads live state, not the static table.
	enabled, err := acct.ListRegions(ctx, &account.ListRegionsInput{
		RegionOptStatusContains: []accounttypes.RegionOptStatus{accounttypes.RegionOptStatusEnabled},
	})
	if err != nil {
		t.Fatalf("ListRegions(ENABLED): %v", err)
	}
	if len(enabled.Regions) != 1 || aws.ToString(enabled.Regions[0].RegionName) != "af-south-1" {
		t.Errorf("the ENABLED filter returned %d Regions, want af-south-1 alone", len(enabled.Regions))
	}

	// --- the refusals, through the SDK's own error types ---
	// errors.As is the assertion that matters: it is what a consumer's catch branch
	// does, and it only succeeds if the code reached the client in the place the
	// REST-JSON parser looks for it — the x-amzn-errortype header. A code left in the
	// body alone deserializes to a bare *smithy.GenericAPIError and no branch matches.
	var validation *accounttypes.ValidationException
	_, err = acct.DisableRegion(ctx, &account.DisableRegionInput{RegionName: aws.String("us-east-1")})
	if !errors.As(err, &validation) {
		t.Fatalf("DisableRegion(us-east-1) answered %v, want *ValidationException — not the ConstraintViolationException #629 named, which this model declares nowhere", err)
	}
	if msg := aws.ToString(validation.Message); !strings.Contains(msg, "invalidRegionOptTarget") {
		t.Errorf("the refusal message is %q; it must name the reason, since the REST-JSON error document has no reason member", msg)
	}
	if msg := aws.ToString(validation.Message); !strings.Contains(msg, "enabled by default") {
		t.Errorf("the refusal message is %q; the reason alone does not tell a default Region from an unknown one", msg)
	}

	validation = nil
	_, err = acct.DisableRegion(ctx, &account.DisableRegionInput{RegionName: aws.String("nosuch-region-9")})
	if !errors.As(err, &validation) {
		t.Fatalf("DisableRegion(nosuch-region-9) answered %v, want *ValidationException", err)
	}

	// --- the round trip ---
	if _, err := acct.DisableRegion(ctx, &account.DisableRegionInput{
		RegionName: aws.String("af-south-1"),
	}); err != nil {
		t.Fatalf("DisableRegion(af-south-1): %v", err)
	}
	disabling, err := acct.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{
		RegionName: aws.String("af-south-1"),
	})
	if err != nil {
		t.Fatalf("GetRegionOptStatus after the disable: %v", err)
	}
	if disabling.RegionOptStatus != accounttypes.RegionOptStatusDisabling {
		t.Errorf("the first poll after the disable reads %q, want DISABLING", disabling.RegionOptStatus)
	}
}

// TestJourney_AccountSeededRegionStatus covers the seed through the SDK, and with
// it the one status an unseeded emulator cannot hold: a Region stuck mid-flight.
//
// Because an in-flight opt advances on observation, a consumer's timeout branch and
// the ConflictException an opposite opt gets while one is in progress are both
// unreachable without the seed. That is what CLAUDE.md's seeding rule buys — the
// rare path made instant and reproducible instead of nondeterministic.
func TestJourney_AccountSeededRegionStatus(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	acct := account.NewFromConfig(cfg, func(o *account.Options) { o.RetryMaxAttempts = 1 })

	seedRegionOptStatus(t, ts, `{"regionName":"ap-east-1","status":"DISABLING"}`)

	// Pinned means pinned: the status does not resolve away under repeated polls,
	// which is what lets a waiter's timeout branch actually run.
	for i := range 3 {
		got, pollErr := acct.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{
			RegionName: aws.String("ap-east-1"),
		})
		if pollErr != nil {
			t.Fatalf("GetRegionOptStatus poll %d: %v", i+1, pollErr)
		}
		if got.RegionOptStatus != accounttypes.RegionOptStatusDisabling {
			t.Fatalf("poll %d reads %q, want the seeded DISABLING", i+1, got.RegionOptStatus)
		}
	}

	// And the ConflictException the User Guide describes verbatim: "this happens if
	// you try to enable a Region that is currently being disabled". 409, not 400.
	var conflict *accounttypes.ConflictException
	_, err = acct.EnableRegion(ctx, &account.EnableRegionInput{RegionName: aws.String("ap-east-1")})
	if !errors.As(err, &conflict) {
		t.Fatalf("enabling a DISABLING Region answered %v, want *ConflictException", err)
	}
}

// seedRegionOptStatus posts a Region opt-status seed to the control plane.
func seedRegionOptStatus(t *testing.T, ts *emulator.TestServer, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/account/region-opt-status", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build seed request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed %s: %v", body, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed %s: %d", body, resp.StatusCode)
	}
}
