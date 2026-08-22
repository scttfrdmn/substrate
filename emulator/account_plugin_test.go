package emulator_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file covers #629: the account service namespace, and specifically the four
// Region opt-in operations a consumer calls to baseline which Regions an account
// can use.
//
// Every case here goes through the full server rather than calling the plugin
// directly. That is deliberate: the account model is rest-json with no
// X-Amz-Target, so the operation is carried by the URL and nothing else, and a
// plugin driven in-process would never exercise the routing. #561 and #610 were
// both "registered, unit-tested, and unreachable from any SDK", which is a defect
// no in-process test can see.

// accountRegionOptStatus is the GetRegionOptStatus response shape.
type accountRegionOptStatus struct {
	RegionName      string `json:"RegionName"`
	RegionOptStatus string `json:"RegionOptStatus"`
}

// accountListRegions is the ListRegions response shape.
type accountListRegions struct {
	Regions   []emulator.AccountRegion `json:"Regions"`
	NextToken string                   `json:"NextToken"`
}

// accountRequest posts an Account Management operation the way the SDK does: a
// bare POST to the operation's own path, with no X-Amz-Target, routed by the Host
// header and the credential scope.
func accountRequest(t *testing.T, ts *emulator.TestServer, path string, body any) *http.Response {
	t.Helper()

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", path, err)
	}
	req.Host = "account.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/account/aws4_request, "+
			"SignedHeaders=host, Signature=fake")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// decodeAccountResponse decodes a rest-json response into out, returning the
// status and the error code a refusal carries.
//
// The code comes from the x-amzn-errortype header, which is where the rest-json
// protocol puts it and what botocore's RestJSONParser prefers — not from
// "__type", which is the JSON-RPC spelling and which these responses do not
// carry. The message is returned too, because a ValidationException's reason is
// the only thing distinguishing invalidRegionOptTarget from
// fieldValidationFailed and substrate's error document has no modeled member to
// put it in.
func decodeAccountResponse(t *testing.T, resp *http.Response, out any) (status int, code, message string) {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if errType := resp.Header.Get("x-amzn-errortype"); errType != "" {
		var errShape struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &errShape)
		return resp.StatusCode, errType, errShape.Message
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	return resp.StatusCode, "", ""
}

// accountOptStatus reads one Region's status, failing the test on any refusal.
func accountOptStatus(t *testing.T, ts *emulator.TestServer, region string) string {
	t.Helper()
	var got accountRegionOptStatus
	status, code, msg := decodeAccountResponse(t,
		accountRequest(t, ts, "/getRegionOptStatus", map[string]any{"RegionName": region}), &got)
	if status != http.StatusOK {
		t.Fatalf("GetRegionOptStatus(%s): %d %s %s", region, status, code, msg)
	}
	if got.RegionName != region {
		t.Errorf("GetRegionOptStatus(%s) answered for %q; a caller reads this field to confirm it got the Region it asked about",
			region, got.RegionName)
	}
	return got.RegionOptStatus
}

// --- routing --------------------------------------------------------------

// TestAccount_IsReachableFromAnSDKShapedRequest is the routing guard.
//
// The account service supplies no X-Amz-Target, so a request only reaches this
// plugin if the Host header or the SigV4 credential scope resolves "account" and
// the URL path resolves the operation. #561 (SSO) and #610 were both a plugin that
// was registered, unit-tested and unreachable: every call fell through to
// "service not emulated". So this asserts the two things an in-process test cannot
// — that the request routes, and that the pipeline sees "ListRegions" rather than
// the bare verb "POST" it would otherwise be named (#480/#572).
func TestAccount_IsReachableFromAnSDKShapedRequest(t *testing.T) {
	ts := emulator.StartTestServer(t)

	var listed accountListRegions
	status, code, msg := decodeAccountResponse(t,
		accountRequest(t, ts, "/listRegions", map[string]any{}), &listed)
	if status != http.StatusOK {
		t.Fatalf("ListRegions: %d %s %s — the account service did not route", status, code, msg)
	}
	if len(listed.Regions) == 0 {
		t.Fatal("ListRegions routed but reported no Regions")
	}

	// The recorded operation name is what authorization, fault injection and cost
	// all read, and they read it before any plugin runs.
	events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "account"})
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no account events were recorded, so nothing can confirm the operation name")
	}
	for _, ev := range events {
		if ev.Operation != "ListRegions" {
			t.Errorf("recorded operation %q, want ListRegions — a bare verb here is the #572 defect", ev.Operation)
		}
	}
}

// TestAccount_UnknownPathIsRefused pins the default arm. A path that is not one
// of the four Region routes must not be answered as though it were: the eleven
// contact and account-name operations are deliberately not emulated, and a
// consumer calling one needs to see that rather than a plausible empty success.
//
// Account is REST-JSON, so the answer is the UnknownOperationException/404 that
// AWS's Common Errors page publishes for that protocol (#716), and the message
// names the verb and path because a REST operation is identified by the pair.
func TestAccount_UnknownPathIsRefused(t *testing.T) {
	ts := emulator.StartTestServer(t)

	status, code, message := decodeAccountResponse(t,
		accountRequest(t, ts, "/putAlternateContact", map[string]any{}), nil)
	if status != http.StatusNotFound || code != "UnknownOperationException" {
		t.Errorf("POST /putAlternateContact: %d %q, want 404/UnknownOperationException", status, code)
	}
	if !strings.Contains(message, "POST /putAlternateContact") {
		t.Errorf("message %q does not name the verb and path that resolved to nothing", message)
	}
}

// --- ListRegions ----------------------------------------------------------

// TestAccount_ListRegionsReportsBothTables is #629's first observation: the two
// Region tables and their statuses.
//
// The counts are pinned because they are the whole content of the answer — a
// consumer deciding which Regions to enable reads this list and nothing else, and
// a table that silently lost an entry would make it skip a Region without
// reporting anything. The API model publishes the RegionOptStatus enum but no
// Region list, so both tables come from the Account Management Reference Guide.
func TestAccount_ListRegionsReportsBothTables(t *testing.T) {
	ts := emulator.StartTestServer(t)

	var listed accountListRegions
	if status, code, msg := decodeAccountResponse(t,
		accountRequest(t, ts, "/listRegions", map[string]any{}), &listed); status != http.StatusOK {
		t.Fatalf("ListRegions: %d %s %s", status, code, msg)
	}

	byName := make(map[string]string, len(listed.Regions))
	for _, r := range listed.Regions {
		if prior, dup := byName[r.RegionName]; dup {
			t.Errorf("%s appears twice, as %s and %s", r.RegionName, prior, r.RegionOptStatus)
		}
		byName[r.RegionName] = r.RegionOptStatus
	}
	if len(byName) != 34 {
		t.Errorf("ListRegions reported %d Regions, want 34 (17 default + 17 opt-in)", len(byName))
	}

	// A default Region is ENABLED_BY_DEFAULT and can never be anything else; an
	// unopted opt-in Region is DISABLED. Spot-checking one of each in every
	// partition-adjacent shape the guide lists would just restate the table, so
	// these are the two the repro in #629 names plus the boundary cases: the
	// Regions whose names look like the other table's.
	wantDefault := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-2", "ap-northeast-3"}
	for _, region := range wantDefault {
		if got := byName[region]; got != "ENABLED_BY_DEFAULT" {
			t.Errorf("%s is %q, want ENABLED_BY_DEFAULT", region, got)
		}
	}
	// ap-southeast-3 and ap-south-2 are one digit from a default Region, which is
	// exactly the kind of entry a hand-copied table gets wrong.
	wantOptIn := []string{"af-south-1", "ap-east-1", "ap-southeast-3", "ap-south-2", "mx-central-1", "il-central-1"}
	for _, region := range wantOptIn {
		if got := byName[region]; got != "DISABLED" {
			t.Errorf("%s is %q, want DISABLED", region, got)
		}
	}
}

// TestAccount_ListRegionsFiltersAndPaginates covers the two ListRegions inputs a
// caller actually drives: the status filter and the page size.
func TestAccount_ListRegionsFiltersAndPaginates(t *testing.T) {
	ts := emulator.StartTestServer(t)

	// The filter is how a consumer asks "which Regions are already on", and it must
	// not answer with the opt-in Regions that are merely available.
	var defaults accountListRegions
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
		map[string]any{"RegionOptStatusContains": []string{"ENABLED_BY_DEFAULT"}}),
		&defaults); status != http.StatusOK {
		t.Fatalf("ListRegions filtered: %d %s %s", status, code, msg)
	}
	if len(defaults.Regions) != 17 {
		t.Errorf("the ENABLED_BY_DEFAULT filter returned %d Regions, want 17", len(defaults.Regions))
	}
	for _, r := range defaults.Regions {
		if r.RegionOptStatus != "ENABLED_BY_DEFAULT" {
			t.Errorf("the filter let %s through with status %s", r.RegionName, r.RegionOptStatus)
		}
	}

	// Enabling one Region then filtering on ENABLED is the check the plan's repro
	// ends on: the filter reads live state, not the static table.
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/enableRegion",
		map[string]any{"RegionName": "af-south-1"}), nil); status != http.StatusOK {
		t.Fatalf("EnableRegion(af-south-1): %d %s %s", status, code, msg)
	}
	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLING" {
		t.Fatalf("af-south-1 is %s on the first poll, want ENABLING", got)
	}
	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLED" {
		t.Fatalf("af-south-1 is %s on the second poll, want ENABLED", got)
	}
	var enabled accountListRegions
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
		map[string]any{"RegionOptStatusContains": []string{"ENABLED"}}), &enabled); status != http.StatusOK {
		t.Fatalf("ListRegions ENABLED: %d %s %s", status, code, msg)
	}
	if len(enabled.Regions) != 1 || enabled.Regions[0].RegionName != "af-south-1" {
		t.Errorf("the ENABLED filter returned %+v, want af-south-1 alone", enabled.Regions)
	}

	// Pagination: a caller looping until NextToken is empty must see all 34 exactly
	// once. A token that restarted from the beginning would loop forever, which is
	// why the token encodes the last Region rather than an offset.
	seen := make(map[string]bool, 34)
	token := ""
	for pages := 0; ; pages++ {
		if pages > 34 {
			t.Fatal("ListRegions paginated past 34 pages, so the NextToken never emptied")
		}
		body := map[string]any{"MaxResults": 5}
		if token != "" {
			body["NextToken"] = token
		}
		var page accountListRegions
		if status, code, msg := decodeAccountResponse(t,
			accountRequest(t, ts, "/listRegions", body), &page); status != http.StatusOK {
			t.Fatalf("ListRegions page %d: %d %s %s", pages, status, code, msg)
		}
		if len(page.Regions) > 5 {
			t.Fatalf("page %d holds %d Regions, over the MaxResults of 5", pages, len(page.Regions))
		}
		for _, r := range page.Regions {
			if seen[r.RegionName] {
				t.Errorf("%s appeared on two pages", r.RegionName)
			}
			seen[r.RegionName] = true
		}
		token = page.NextToken
		if token == "" {
			break
		}
	}
	if len(seen) != 34 {
		t.Errorf("paging through saw %d Regions, want 34", len(seen))
	}
}

// TestAccount_ListRegionsRefusesBadInput covers the two ValidationException
// reasons a bad ListRegions input gets, both fieldValidationFailed: the model's
// MaxResults shape bounds it at 1-50, and RegionOptStatusContains takes enum
// members.
//
// A status outside the enum is refused rather than filtering everything out. That
// distinction is the point: a caller that typo'd "Enabled" would otherwise read an
// empty list and conclude no Region was enabled.
func TestAccount_ListRegionsRefusesBadInput(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"MaxResults below the floor", map[string]any{"MaxResults": 0}},
		{"MaxResults above the ceiling", map[string]any{"MaxResults": 51}},
		{"MaxResults negative", map[string]any{"MaxResults": -1}},
		{"a status outside the enum", map[string]any{"RegionOptStatusContains": []string{"Enabled"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions", tc.body), nil)
			if status != http.StatusBadRequest || code != "ValidationException" {
				t.Fatalf("ListRegions %s: %d %q, want 400/ValidationException", tc.name, status, code)
			}
			if !strings.Contains(msg, "fieldValidationFailed") {
				t.Errorf("the message is %q; it must name the reason, since the error document has nowhere else to carry it", msg)
			}
		})
	}

	// MaxResults at both bounds is accepted. An off-by-one in the check would be
	// invisible in a nominal run, and it refuses a request AWS accepts.
	for _, maxResults := range []int{1, 50} {
		var page accountListRegions
		if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
			map[string]any{"MaxResults": maxResults}), &page); status != http.StatusOK {
			t.Errorf("ListRegions MaxResults=%d: %d %s %s, want 200", maxResults, status, code, msg)
		}
	}

	// An unreadable NextToken is a refusal rather than a silent restart: a
	// paginating caller that restarts sees duplicates instead of an error.
	status, code, _ := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
		map[string]any{"NextToken": "not-base64-!!"}), nil)
	if status != http.StatusBadRequest || code != "ValidationException" {
		t.Errorf("ListRegions with a corrupt NextToken: %d %q, want 400/ValidationException", status, code)
	}
	// A well-formed token naming no Region is equally a refusal.
	status, code, _ = decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
		map[string]any{"NextToken": base64.StdEncoding.EncodeToString([]byte("nosuch-region-9"))}), nil)
	if status != http.StatusBadRequest || code != "ValidationException" {
		t.Errorf("ListRegions with a stale NextToken: %d %q, want 400/ValidationException", status, code)
	}
}

// --- Enable / Disable / GetRegionOptStatus --------------------------------

// TestAccount_EnableRegionResolvesOnObservation is the core of #629: the
// asynchronous enable, and the fact that a waiter converges.
//
// Enabling a Region in AWS takes "a few minutes to several hours". Substrate
// resolves ENABLING to ENABLED on first observation instead, which is what makes
// that wait testable at all — and the terminal status then never moves, because a
// status that flipped back would make a waiter comparing successive polls loop
// forever.
func TestAccount_EnableRegionResolvesOnObservation(t *testing.T) {
	ts := emulator.StartTestServer(t)

	if got := accountOptStatus(t, ts, "af-south-1"); got != "DISABLED" {
		t.Fatalf("af-south-1 starts at %s, want DISABLED", got)
	}

	// EnableRegion has no output shape: an empty 200 is the whole answer, and the
	// status is observed separately.
	resp := accountRequest(t, ts, "/enableRegion", map[string]any{"RegionName": "af-south-1"})
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read EnableRegion body: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("EnableRegion: %d %s", resp.StatusCode, raw)
	}
	if got := strings.TrimSpace(string(raw)); got != "{}" {
		t.Errorf("EnableRegion answered %q; the model gives it no output shape", got)
	}

	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLING" {
		t.Errorf("the first poll reads %s, want ENABLING — the enable is asynchronous", got)
	}
	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLED" {
		t.Errorf("the second poll reads %s, want ENABLED", got)
	}
	// And it stays. This is the assertion a clock-driven transition would fail.
	for i := range 3 {
		if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLED" {
			t.Fatalf("poll %d reads %s, want ENABLED — a terminal status must not move", i+3, got)
		}
	}

	// Re-enabling is idempotent against the target state, not the call. This is what
	// makes an "ensure these Regions are on" routine safe to re-run, which is the
	// shape of the consumer #629 came from; writing ENABLING over ENABLED would make
	// the Region go backwards for a waiter that had already finished.
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/enableRegion",
		map[string]any{"RegionName": "af-south-1"}), nil); status != http.StatusOK {
		t.Fatalf("re-enabling an ENABLED Region: %d %s %s, want an empty 200", status, code, msg)
	}
	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLED" {
		t.Errorf("af-south-1 is %s after a redundant enable, want ENABLED", got)
	}

	// ListRegions must agree with GetRegionOptStatus. If it resolved separately, a
	// caller polling through the listing would read ENABLING forever while a
	// GetRegionOptStatus of the same Region said ENABLED.
	var listed accountListRegions
	if status, code, msg := decodeAccountResponse(t,
		accountRequest(t, ts, "/listRegions", map[string]any{}), &listed); status != http.StatusOK {
		t.Fatalf("ListRegions: %d %s %s", status, code, msg)
	}
	for _, r := range listed.Regions {
		if r.RegionName == "af-south-1" && r.RegionOptStatus != "ENABLED" {
			t.Errorf("ListRegions reports af-south-1 as %s while GetRegionOptStatus says ENABLED", r.RegionOptStatus)
		}
	}
}

// TestAccount_DisableRegionResolvesOnObservation is the mirror of the enable, plus
// the round trip: a Region can be turned off again and lands back where it began.
func TestAccount_DisableRegionResolvesOnObservation(t *testing.T) {
	ts := emulator.StartTestServer(t)

	if status, _, _ := decodeAccountResponse(t, accountRequest(t, ts, "/enableRegion",
		map[string]any{"RegionName": "eu-south-1"}), nil); status != http.StatusOK {
		t.Fatalf("EnableRegion(eu-south-1): %d", status)
	}
	if got := accountOptStatus(t, ts, "eu-south-1"); got != "ENABLING" {
		t.Fatalf("eu-south-1 is %s, want ENABLING", got)
	}
	if got := accountOptStatus(t, ts, "eu-south-1"); got != "ENABLED" {
		t.Fatalf("eu-south-1 is %s, want ENABLED", got)
	}

	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/disableRegion",
		map[string]any{"RegionName": "eu-south-1"}), nil); status != http.StatusOK {
		t.Fatalf("DisableRegion(eu-south-1): %d %s %s", status, code, msg)
	}
	if got := accountOptStatus(t, ts, "eu-south-1"); got != "DISABLING" {
		t.Errorf("the first poll after the disable reads %s, want DISABLING", got)
	}
	if got := accountOptStatus(t, ts, "eu-south-1"); got != "DISABLED" {
		t.Errorf("the second poll reads %s, want DISABLED", got)
	}

	// A redundant disable is a no-op success, same as the redundant enable.
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/disableRegion",
		map[string]any{"RegionName": "eu-south-1"}), nil); status != http.StatusOK {
		t.Errorf("re-disabling a DISABLED Region: %d %s %s, want an empty 200", status, code, msg)
	}
	if got := accountOptStatus(t, ts, "eu-south-1"); got != "DISABLED" {
		t.Errorf("eu-south-1 is %s after a redundant disable, want DISABLED", got)
	}
}

// TestAccount_OptTargetRefusals covers the invalidRegionOptTarget cases.
//
// A default Region cannot be disabled — "Default Regions cannot be enabled or
// disabled" — and this is where #629's own framing was wrong: it named
// ConstraintViolationException, which the account/2021-02-01 model declares for
// no operation at all. ValidationException with reason invalidRegionOptTarget is
// the shape that exists, and the reason distinguishes it from a malformed field.
func TestAccount_OptTargetRefusals(t *testing.T) {
	ts := emulator.StartTestServer(t)

	// Both cases are ValidationException/invalidRegionOptTarget — the model gives
	// them no separate code and no separate reason — so the message is the only thing
	// that distinguishes them, and `wants` pins the distinguishing word. Without it,
	// deleting the default-Region check entirely leaves every assertion here passing:
	// the fall-through arm answers the same code and the same reason, and a caller
	// would be told us-east-1 is not a Region.
	for _, tc := range []struct {
		name, path, region, wants string
	}{
		{"disabling a default Region", "/disableRegion", "us-east-1", "enabled by default"},
		{"enabling a default Region", "/enableRegion", "us-east-1", "enabled by default"},
		{"disabling an unknown Region", "/disableRegion", "nosuch-region-9", "not an AWS Region code"},
		{"enabling an unknown Region", "/enableRegion", "nosuch-region-9", "not an AWS Region code"},
		{"reading an unknown Region", "/getRegionOptStatus", "nosuch-region-9", "not an AWS Region code"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := decodeAccountResponse(t,
				accountRequest(t, ts, tc.path, map[string]any{"RegionName": tc.region}), nil)
			if status != http.StatusBadRequest || code != "ValidationException" {
				t.Fatalf("%s: %d %q, want 400/ValidationException — not ConstraintViolationException, which this model does not declare",
					tc.name, status, code)
			}
			if !strings.Contains(msg, "invalidRegionOptTarget") {
				t.Errorf("the message is %q; it must name the reason, since the error document has nowhere else to carry it", msg)
			}
			if !strings.Contains(msg, tc.wants) {
				t.Errorf("the message is %q, want it to say %q — the reason alone does not tell a default Region from an unknown one",
					msg, tc.wants)
			}
		})
	}

	// A default Region still reads back through GetRegionOptStatus: it is only the
	// opt that is refused, not the observation.
	if got := accountOptStatus(t, ts, "us-east-1"); got != "ENABLED_BY_DEFAULT" {
		t.Errorf("us-east-1 reads %s, want ENABLED_BY_DEFAULT", got)
	}

	// RegionName is required by the model on all three Region-named operations.
	for _, path := range []string{"/enableRegion", "/disableRegion", "/getRegionOptStatus"} {
		status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, path, map[string]any{}), nil)
		if status != http.StatusBadRequest || code != "ValidationException" {
			t.Errorf("POST %s with no RegionName: %d %q, want 400/ValidationException", path, status, code)
		}
		if !strings.Contains(msg, "fieldValidationFailed") {
			t.Errorf("POST %s with no RegionName: message %q, want fieldValidationFailed", path, msg)
		}
	}
}

// --- the seed -------------------------------------------------------------

// TestAccount_SeededStatusPinsTheObservation covers the control-plane seed, which
// is the only route to a status a sequence of API calls cannot produce.
//
// Because an in-flight opt resolves on first observation, an unseeded emulator can
// never hold a Region in ENABLING across two polls — so a waiter's timeout branch,
// and the ConflictException an opposite opt gets mid-flight, are unreachable
// without this. That is what CLAUDE.md's seeding rule is for: the rare path made
// instant and reproducible rather than nondeterministic.
func TestAccount_SeededStatusPinsTheObservation(t *testing.T) {
	ts := emulator.StartTestServer(t)

	accountSeedStatus(t, ts, "af-south-1", "ENABLING")

	// Pinned means pinned: repeated polls do not resolve it away.
	for i := range 3 {
		if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLING" {
			t.Fatalf("poll %d reads %s, want the seeded ENABLING", i+1, got)
		}
	}

	// And the ConflictException the User Guide describes verbatim — "this happens if
	// you try to enable a Region that is currently being disabled (in a status of
	// DISABLING)" — which is 409, not 400.
	accountSeedStatus(t, ts, "af-south-1", "DISABLING")
	status, code, _ := decodeAccountResponse(t, accountRequest(t, ts, "/enableRegion",
		map[string]any{"RegionName": "af-south-1"}), nil)
	if status != http.StatusConflict || code != "ConflictException" {
		t.Errorf("enabling a DISABLING Region: %d %q, want 409/ConflictException", status, code)
	}

	accountSeedStatus(t, ts, "af-south-1", "ENABLING")
	status, code, _ = decodeAccountResponse(t, accountRequest(t, ts, "/disableRegion",
		map[string]any{"RegionName": "af-south-1"}), nil)
	if status != http.StatusConflict || code != "ConflictException" {
		t.Errorf("disabling an ENABLING Region: %d %q, want 409/ConflictException", status, code)
	}

	// The seed also drives the listing, so a consumer that reads state through
	// ListRegions rather than a per-Region poll sees the same thing.
	var listed accountListRegions
	if status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/listRegions",
		map[string]any{"RegionOptStatusContains": []string{"ENABLING"}}), &listed); status != http.StatusOK {
		t.Fatalf("ListRegions ENABLING: %d %s %s", status, code, msg)
	}
	if len(listed.Regions) != 1 || listed.Regions[0].RegionName != "af-south-1" {
		t.Errorf("the ENABLING filter returned %+v, want af-south-1 alone", listed.Regions)
	}

	// Clearing restores the stored record, which for a Region never opted is
	// DISABLED.
	accountClearStatus(t, ts, "af-south-1")
	if got := accountOptStatus(t, ts, "af-south-1"); got != "DISABLED" {
		t.Errorf("af-south-1 reads %s after the seed was cleared, want DISABLED", got)
	}
}

// TestAccount_SeedRefusesWhatItCannotAffect pins the seed endpoint's own
// refusals. A seed that is silently ignored is worse than one that is refused: the
// test using it passes while asserting nothing.
func TestAccount_SeedRefusesWhatItCannotAffect(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		name, region, status string
	}{
		{"a status outside the enum", "af-south-1", "Enabling"},
		{"a default Region, whose status is fixed", "us-east-1", "DISABLED"},
		{"a Region in neither table", "nosuch-region-9", "ENABLED"},
		{"an empty status", "af-south-1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{"regionName": tc.region, "status": tc.status})
			if err != nil {
				t.Fatalf("marshal seed: %v", err)
			}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				ts.URL+"/v1/account/region-opt-status", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("build seed request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("seeding %s: %d, want 400", tc.name, resp.StatusCode)
			}
		})
	}
}

// --- AccountId targeting --------------------------------------------------

// TestAccount_AccountIDTargetsAMemberAccount covers the AccountId parameter,
// which is where #623's reverse index pays off a second time.
//
// The model's rules are narrow and each refusal below is one of them: the caller
// may name a member of its own organization, may not name itself ("The management
// account can't specify its own AccountId"), and may not name an account it does
// not manage. Getting this wrong in the permissive direction would let a test
// read or opt another organization's account and see a plausible answer.
func TestAccount_AccountIDTargetsAMemberAccount(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t)
	member := orgVendMember(t, ts, "regions-member", "regions-member@example.com")

	// Management enables a Region *in the member account*. The member's own view is
	// what must change — that is the whole point of the parameter.
	if status, code, msg := decodeAccountResponse(t, accountSignedRequest(t, ts, orgManagementAccount,
		"/enableRegion", map[string]any{"AccountId": member, "RegionName": "ap-east-1"}),
		nil); status != http.StatusOK {
		t.Fatalf("EnableRegion for member %s: %d %s %s", member, status, code, msg)
	}

	var got accountRegionOptStatus
	if status, code, msg := decodeAccountResponse(t, accountSignedRequest(t, ts, orgManagementAccount,
		"/getRegionOptStatus", map[string]any{"AccountId": member, "RegionName": "ap-east-1"}),
		&got); status != http.StatusOK {
		t.Fatalf("GetRegionOptStatus for member: %d %s %s", status, code, msg)
	}
	if got.RegionOptStatus != "ENABLING" {
		t.Errorf("the member's ap-east-1 is %s, want ENABLING", got.RegionOptStatus)
	}

	// Management's own account is untouched: the opt was filed under the member, not
	// under whoever happened to make the call. That is #624's defect in another
	// service, and the reason this assertion is here rather than assumed.
	var mgmt accountRegionOptStatus
	if status, code, msg := decodeAccountResponse(t, accountSignedRequest(t, ts, orgManagementAccount,
		"/getRegionOptStatus", map[string]any{"RegionName": "ap-east-1"}), &mgmt); status != http.StatusOK {
		t.Fatalf("GetRegionOptStatus for management: %d %s %s", status, code, msg)
	}
	if mgmt.RegionOptStatus != "DISABLED" {
		t.Errorf("management's own ap-east-1 is %s, want DISABLED", mgmt.RegionOptStatus)
	}

	for _, tc := range []struct {
		name, caller, accountID string
	}{
		{"the caller names itself", orgManagementAccount, orgManagementAccount},
		{"an account in no organization", orgManagementAccount, "222233334444"},
		{"a malformed account ID", orgManagementAccount, "12345"},
		{"a member names its management account", member, orgManagementAccount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := decodeAccountResponse(t, accountSignedRequest(t, ts, tc.caller,
				"/getRegionOptStatus", map[string]any{"AccountId": tc.accountID, "RegionName": "ap-east-1"}), nil)
			if status != http.StatusBadRequest || code != "ValidationException" {
				t.Errorf("%s: %d %q, want 400/ValidationException — the only 400 this operation declares",
					tc.name, status, code)
			}
		})
	}
}

// accountSignedRequest posts an Account Management operation signed as the given
// account, so a test can be a caller other than the default one.
//
// The rest-json path is part of the canonical request, so the signature covers it;
// signing "/" here would produce a header the server refuses before any plugin
// runs, which is why sigV4Header takes the path.
func accountSignedRequest(t *testing.T, ts *emulator.TestServer, account, path string, body any) *http.Response {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	if !ok {
		t.Fatalf("no credential registered for account %s", account)
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}

	const (
		host     = "account.us-east-1.amazonaws.com"
		dateTime = "20260101T120000Z"
	)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", path, err)
	}
	req.Host = host
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Date", dateTime)
	req.Header.Set("Authorization", sigV4Header(path, host, "account", dateTime, data,
		creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s as %s: %v", path, account, err)
	}
	return resp
}

// TestAccount_ClearAllSeedsRestoresEveryRegion covers the no-argument clear, which
// is what a test fixture calls between cases. A clear that missed a Region would
// leak a pinned status into the next case and produce a failure with no visible
// cause in the test that fails.
func TestAccount_ClearAllSeedsRestoresEveryRegion(t *testing.T) {
	ts := emulator.StartTestServer(t)

	accountSeedStatus(t, ts, "af-south-1", "ENABLING")
	accountSeedStatus(t, ts, "ap-east-1", "ENABLED")
	if got := accountOptStatus(t, ts, "af-south-1"); got != "ENABLING" {
		t.Fatalf("af-south-1 is %s, want the seeded ENABLING", got)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		ts.URL+"/v1/account/region-opt-status", nil)
	if err != nil {
		t.Fatalf("build clear-all request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("clear all: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear all: %d", resp.StatusCode)
	}

	for _, region := range []string{"af-south-1", "ap-east-1"} {
		if got := accountOptStatus(t, ts, region); got != "DISABLED" {
			t.Errorf("%s is %s after clearing every seed, want DISABLED", region, got)
		}
	}
}

// TestAccount_MalformedInputIsRefused covers the two input failures that are not
// about a Region: a body that is not JSON, and a RegionName past the model's
// 50-character bound. Both are ValidationException, the only 400 these operations
// declare, so a caller's catch branch matches either.
func TestAccount_MalformedInputIsRefused(t *testing.T) {
	ts := emulator.StartTestServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/getRegionOptStatus", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "account.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /getRegionOptStatus: %v", err)
	}
	status, code, _ := decodeAccountResponse(t, resp, nil)
	if status != http.StatusBadRequest || code != "ValidationException" {
		t.Errorf("an unparseable body: %d %q, want 400/ValidationException", status, code)
	}

	status, code, msg := decodeAccountResponse(t, accountRequest(t, ts, "/getRegionOptStatus",
		map[string]any{"RegionName": strings.Repeat("z", 51)}), nil)
	if status != http.StatusBadRequest || code != "ValidationException" {
		t.Errorf("a 51-character RegionName: %d %q, want 400/ValidationException", status, code)
	}
	if !strings.Contains(msg, "fieldValidationFailed") {
		t.Errorf("the message is %q, want fieldValidationFailed — the length bound is a field failure, not a bad target", msg)
	}
}

// accountSeedStatus pins a Region's opt status through the control plane.
func accountSeedStatus(t *testing.T, ts *emulator.TestServer, region, status string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"regionName": region, "status": status})
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/account/region-opt-status", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build seed request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed %s=%s: %v", region, status, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed %s=%s: %d %s", region, status, resp.StatusCode, raw)
	}
}

// accountClearStatus removes the seed for one Region.
func accountClearStatus(t *testing.T, ts *emulator.TestServer, region string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		ts.URL+"/v1/account/region-opt-status?regionName="+region, nil)
	if err != nil {
		t.Fatalf("build clear request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("clear %s: %v", region, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear %s: %d", region, resp.StatusCode)
	}
}
