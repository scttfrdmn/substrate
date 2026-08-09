package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// newOrganizationsStateFixture returns a plugin wired straight to a memory state
// manager, for exercising the storage layer without the HTTP surface.
func newOrganizationsStateFixture(t *testing.T) *emulator.OrganizationsPlugin {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	return emulator.NewOrganizationsPluginForTest(state, tc)
}

const orgTestAccount = "000000000000"

// newOrgBadBody returns a body no JSON decoder can parse.
func newOrgBadBody() io.Reader { return strings.NewReader(`{"AccountId":`) }

// orgListRootsID posts ListRoots and returns the single root's ID.
func orgListRootsID(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp := orgsRequest(t, ts, "ListRoots", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListRoots: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Roots []struct {
			ID string `json:"Id"`
		} `json:"Roots"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("ListRoots decode: %v", err)
	}
	if len(out.Roots) != 1 {
		t.Fatalf("expected exactly 1 root, got %d", len(out.Roots))
	}
	return out.Roots[0].ID
}

// TestOrganizations_ListRoots_StableID is #577's repro: two ListRoots calls
// disagreeing about the root's identity meant nothing could reference the root,
// and a caller that attached a policy to it and re-read would conclude the
// attachment had vanished.
func TestOrganizations_ListRoots_StableID(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	first := orgListRootsID(t, ts)
	second := orgListRootsID(t, ts)
	if first != second {
		t.Fatalf("root ID changed between ListRoots calls: %q vs %q", first, second)
	}

	// An unrelated write must not disturb it either.
	createResp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "dev",
		"Email":       "dev@example.com",
	})
	createResp.Body.Close() //nolint:errcheck

	third := orgListRootsID(t, ts)
	if third != first {
		t.Fatalf("root ID changed after an unrelated write: %q vs %q", first, third)
	}
}

// TestOrganizations_ListRoots_ARNMatchesID checks the ARN is built from the
// persisted root rather than a freshly minted one, since an unstable ARN is the
// form of #577 a caller storing the ARN would hit.
func TestOrganizations_ListRoots_ARNMatchesID(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	resp := orgsRequest(t, ts, "ListRoots", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Roots []struct {
			ID          string `json:"Id"`
			Arn         string `json:"Arn"`
			Name        string `json:"Name"`
			PolicyTypes []struct {
				Type   string `json:"Type"`
				Status string `json:"Status"`
			} `json:"PolicyTypes"`
		} `json:"Roots"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(out.Roots))
	}
	root := out.Roots[0]
	if root.Name != "Root" {
		t.Errorf("expected Name=Root, got %q", root.Name)
	}
	if suffix := "/" + root.ID; len(root.Arn) < len(suffix) || root.Arn[len(root.Arn)-len(suffix):] != suffix {
		t.Errorf("ARN %q does not end with the root ID %q", root.Arn, root.ID)
	}
	if len(root.PolicyTypes) != 1 || root.PolicyTypes[0].Type != emulator.OrgPolicyTypeSCPForTest ||
		root.PolicyTypes[0].Status != "ENABLED" {
		t.Errorf("expected SERVICE_CONTROL_POLICY ENABLED, got %+v", root.PolicyTypes)
	}
}

// TestOrganizations_RootPersistsAcrossPlugins checks the root survives a fresh
// plugin over the same state, which is what a replayed run does.
func TestOrganizations_RootPersistsAcrossPlugins(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	first := emulator.NewOrganizationsPluginForTest(state, tc)
	rootA, err := first.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}

	second := emulator.NewOrganizationsPluginForTest(state, tc)
	rootB, err := second.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("reload root: %v", err)
	}
	if rootA.ID != rootB.ID {
		t.Fatalf("root ID changed across plugin instances: %q vs %q", rootA.ID, rootB.ID)
	}
}

// TestOrganizations_ManagementAccountPlacedInRoot pins #578's placement rule:
// an account has a parent from the moment it exists, so a ListParents walk has
// somewhere to start.
func TestOrganizations_ManagementAccountPlacedInRoot(t *testing.T) {
	p := newOrganizationsStateFixture(t)

	root, err := p.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	parent, err := p.LoadParentForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if parent != root.ID {
		t.Errorf("expected management account's parent to be the root %q, got %q", root.ID, parent)
	}
	children, err := p.LoadChildrenForTest(t.Context(), root.ID)
	if err != nil {
		t.Fatalf("load children: %v", err)
	}
	if len(children) != 1 || children[0] != orgTestAccount {
		t.Errorf("expected the root to contain only the management account, got %v", children)
	}
}

// TestOrganizations_FullAWSAccessAttachedOnCreate pins the AWS-managed SCP onto
// the root and the management account. Without it ListPoliciesForTarget is wrong
// on a fresh organization, and the minimum-attachment rule has nothing to hold.
func TestOrganizations_FullAWSAccessAttachedOnCreate(t *testing.T) {
	p := newOrganizationsStateFixture(t)

	root, err := p.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	for _, target := range []string{root.ID, orgTestAccount} {
		attached, attachErr := p.LoadAttachmentsForTest(t.Context(), target)
		if attachErr != nil {
			t.Fatalf("load attachments for %s: %v", target, attachErr)
		}
		if len(attached) != 1 || attached[0] != emulator.OrgFullAWSAccessIDForTest {
			t.Errorf("expected only FullAWSAccess attached to %s, got %v", target, attached)
		}
	}

	targets, err := p.LoadPolicyTargetsForTest(t.Context(), emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load policy targets: %v", err)
	}
	if len(targets) != 2 {
		t.Errorf("expected FullAWSAccess attached to 2 targets, got %v", targets)
	}
}

// TestOrganizations_FullAWSAccessIsSynthesized checks the AWS-managed policy is
// readable without ever having been written, is owned by "aws", and is marked
// AwsManaged — the three properties that make it immutable.
func TestOrganizations_FullAWSAccessIsSynthesized(t *testing.T) {
	p := newOrganizationsStateFixture(t)

	pol, err := p.LoadPolicyForTest(t.Context(), emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load FullAWSAccess: %v", err)
	}
	if pol == nil {
		t.Fatal("expected FullAWSAccess to exist without being created")
	}
	if !pol.PolicySummary.AwsManaged {
		t.Error("expected AwsManaged=true")
	}
	if pol.PolicySummary.Arn != "arn:aws:organizations::aws:policy/service_control_policy/p-FullAWSAccess" {
		t.Errorf("expected the aws-owned ARN, got %q", pol.PolicySummary.Arn)
	}
	// It is not in the organization's own policy index, so it cannot be updated
	// or deleted through the stored-policy path.
	ids, err := p.LoadPolicyIDsForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load policy ids: %v", err)
	}
	for _, id := range ids {
		if id == emulator.OrgFullAWSAccessIDForTest {
			t.Error("FullAWSAccess must not be in the organization's stored policy index")
		}
	}
}

// TestOrganizations_ConsolidatedBillingHasNoPolicyTypes pins the state where an
// SCP cannot exist at all: CreateOrganization documents that a
// CONSOLIDATED_BILLING organization enables no policy type, so the root reports
// none and FullAWSAccess is not attached.
func TestOrganizations_ConsolidatedBillingHasNoPolicyTypes(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := state.Put(t.Context(), "organizations-ctrl", "feature-set",
		[]byte(`{"featureSet":"CONSOLIDATED_BILLING"}`)); err != nil {
		t.Fatalf("seed feature set: %v", err)
	}
	p := emulator.NewOrganizationsPluginForTest(state, tc)

	root, err := p.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	if len(root.PolicyTypes) != 0 {
		t.Errorf("expected no policy types under CONSOLIDATED_BILLING, got %+v", root.PolicyTypes)
	}
	enabled, err := p.SCPEnabledForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("scp enabled: %v", err)
	}
	if enabled {
		t.Error("expected the SCP type to be unavailable under CONSOLIDATED_BILLING")
	}
	attached, err := p.LoadAttachmentsForTest(t.Context(), root.ID)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 0 {
		t.Errorf("expected no attachments under CONSOLIDATED_BILLING, got %v", attached)
	}
}

// TestOrganizations_DescribeOrganization_FeatureSetSeed checks the seed governs
// what a caller observes, including for an organization already created in ALL
// mode — otherwise a test would have to arrange the seed before the very first
// request, which the seed endpoint cannot guarantee.
func TestOrganizations_DescribeOrganization_FeatureSetSeed(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	p := emulator.NewOrganizationsPluginForTest(state, tc)

	if _, err := p.EnsureOrganizationForTest(t.Context(), orgTestAccount); err != nil {
		t.Fatalf("ensure org: %v", err)
	}
	got, err := p.EffectiveFeatureSetForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("effective feature set: %v", err)
	}
	if got != "ALL" {
		t.Fatalf("expected ALL by default, got %q", got)
	}

	if err := state.Put(t.Context(), "organizations-ctrl", "feature-set",
		[]byte(`{"featureSet":"CONSOLIDATED_BILLING"}`)); err != nil {
		t.Fatalf("seed feature set: %v", err)
	}
	got, err = p.EffectiveFeatureSetForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("effective feature set after seed: %v", err)
	}
	if got != "CONSOLIDATED_BILLING" {
		t.Errorf("expected the seed to win over the stored value, got %q", got)
	}
}

// TestOrganizations_ResolveTarget covers every entity kind resolveOrgTarget has
// to name, since the policy and tagging operations both branch on its answer.
func TestOrganizations_ResolveTarget(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	ouID := "ou-" + root.ID[2:] + "-abcd1234"
	if err := p.SaveOUForTest(ctx, orgTestAccount, emulator.OrgOrganizationalUnit{
		ID: ouID, Arn: "arn:aws:organizations::" + orgTestAccount + ":ou/o-x/" + ouID, Name: "prod",
	}); err != nil {
		t.Fatalf("save OU: %v", err)
	}
	if err := p.SavePolicyForTest(ctx, orgTestAccount, emulator.OrgPolicy{
		PolicySummary: emulator.OrgPolicySummary{ID: "p-abcdefgh", Name: "deny", Type: emulator.OrgPolicyTypeSCPForTest},
		Content:       `{}`,
	}); err != nil {
		t.Fatalf("save policy: %v", err)
	}

	tests := []struct {
		name string
		id   string
		want string
	}{
		{"root", root.ID, emulator.OrgKindRootForTest},
		{"ou", ouID, emulator.OrgKindOUForTest},
		{"account", orgTestAccount, emulator.OrgKindAccountForTest},
		{"policy", "p-abcdefgh", emulator.OrgKindPolicyForTest},
		{"aws managed policy", emulator.OrgFullAWSAccessIDForTest, emulator.OrgKindPolicyForTest},
		{"unknown root", "r-zzzz", ""},
		{"unknown ou", "ou-zzzz-99999999", ""},
		{"unknown account", "999999999999", ""},
		{"unknown policy", "p-zzzzzzzz", ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, resolveErr := p.ResolveOrgTargetForTest(ctx, orgTestAccount, tt.id)
			if resolveErr != nil {
				t.Fatalf("resolve %q: %v", tt.id, resolveErr)
			}
			if got != tt.want {
				t.Errorf("resolve %q: expected %q, got %q", tt.id, tt.want, got)
			}
		})
	}
}

// TestOrganizations_PlaceChildMovesExclusively checks a move leaves the child
// reachable from exactly one parent. A stale entry in the old parent's list would
// let ListAccountsForParent report the same account in two places.
func TestOrganizations_PlaceChildMovesExclusively(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	ouID := "ou-" + root.ID[2:] + "-11112222"
	if err := p.PlaceChildForTest(ctx, ouID, orgTestAccount); err != nil {
		t.Fatalf("place child: %v", err)
	}

	rootChildren, err := p.LoadChildrenForTest(ctx, root.ID)
	if err != nil {
		t.Fatalf("load root children: %v", err)
	}
	if len(rootChildren) != 0 {
		t.Errorf("expected the account to have left the root, still there: %v", rootChildren)
	}
	ouChildren, err := p.LoadChildrenForTest(ctx, ouID)
	if err != nil {
		t.Fatalf("load OU children: %v", err)
	}
	if len(ouChildren) != 1 || ouChildren[0] != orgTestAccount {
		t.Errorf("expected the account under the OU, got %v", ouChildren)
	}
	parent, err := p.LoadParentForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if parent != ouID {
		t.Errorf("expected parent %q, got %q", ouID, parent)
	}

	// Re-placing under the same parent is idempotent, not a duplicate.
	if err := p.PlaceChildForTest(ctx, ouID, orgTestAccount); err != nil {
		t.Fatalf("re-place child: %v", err)
	}
	ouChildren, err = p.LoadChildrenForTest(ctx, ouID)
	if err != nil {
		t.Fatalf("reload OU children: %v", err)
	}
	if len(ouChildren) != 1 {
		t.Errorf("expected no duplicate child entry, got %v", ouChildren)
	}
}

// TestOrganizations_OUDepth walks a chain to the documented maximum. The depth
// limit is a boundary an off-by-one hides: a 5-deep tree is legal and a 6th level
// is not, and both nominal runs look identical.
func TestOrganizations_OUDepth(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	if depth, depthErr := p.OUDepthForTest(ctx, root.ID); depthErr != nil || depth != 0 {
		t.Fatalf("expected the root at depth 0, got %d (err %v)", depth, depthErr)
	}

	parent := root.ID
	suffixes := []string{"aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd", "eeeeeeee", "ffffffff"}
	for i, suffix := range suffixes {
		ouID := "ou-" + root.ID[2:] + "-" + suffix
		if err := p.PlaceChildForTest(ctx, parent, ouID); err != nil {
			t.Fatalf("place OU %d: %v", i, err)
		}
		depth, depthErr := p.OUDepthForTest(ctx, ouID)
		if depthErr != nil {
			t.Fatalf("depth of OU %d: %v", i, depthErr)
		}
		if depth != i+1 {
			t.Errorf("OU %d: expected depth %d, got %d", i, i+1, depth)
		}
		parent = ouID
	}
	// The chain reached six levels, one past the documented limit, so a handler
	// comparing against orgMaxOUDepth refuses the last one.
	if emulator.OrgMaxOUDepthForTest != 5 {
		t.Errorf("expected the documented OU depth limit of 5, got %d", emulator.OrgMaxOUDepthForTest)
	}
}

// TestOrganizations_Paginate covers the clamp, termination, and the token a
// caller loops on. A truncated listing that never terminates or silently
// restarts is the failure mode hardest for a consumer to notice.
func TestOrganizations_Paginate(t *testing.T) {
	ids := []string{"e", "d", "c", "b", "a"}

	t.Run("sorts and returns everything by default", func(t *testing.T) {
		page, next, err := emulator.OrgPaginateForTest(ids, "", 0)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		if next != "" {
			t.Errorf("expected no NextToken, got %q", next)
		}
		if got := len(page); got != 5 {
			t.Fatalf("expected 5 items, got %d", got)
		}
		if page[0] != "a" || page[4] != "e" {
			t.Errorf("expected sorted order, got %v", page)
		}
	})

	t.Run("honors MaxResults and terminates", func(t *testing.T) {
		var seen []string
		token := ""
		for i := 0; i < 10; i++ {
			page, next, err := emulator.OrgPaginateForTest(ids, token, 2)
			if err != nil {
				t.Fatalf("paginate: %v", err)
			}
			if len(page) > 2 {
				t.Fatalf("expected at most 2 per page, got %d", len(page))
			}
			seen = append(seen, page...)
			if next == "" {
				break
			}
			token = next
		}
		if len(seen) != 5 {
			t.Errorf("expected to walk all 5 items, saw %v", seen)
		}
	})

	t.Run("clamps MaxResults to the model ceiling", func(t *testing.T) {
		many := make([]string, 40)
		for i := range many {
			many[i] = string(rune('a'+i/26)) + string(rune('a'+i%26))
		}
		page, next, err := emulator.OrgPaginateForTest(many, "", 1000)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		if len(page) != emulator.OrgMaxResultsForTest {
			t.Errorf("expected the page clamped to %d, got %d", emulator.OrgMaxResultsForTest, len(page))
		}
		if next == "" {
			t.Error("expected a NextToken for a truncated listing")
		}
	})

	t.Run("rejects an unreadable token", func(t *testing.T) {
		if _, _, err := emulator.OrgPaginateForTest(ids, "not-base64!!", 0); err == nil {
			t.Fatal("expected an error for a malformed NextToken")
		}
		if _, _, err := emulator.OrgPaginateForTest(ids, "enp6eg==", 0); err == nil {
			t.Fatal("expected an error for a token naming an unknown item")
		}
	})
}

// TestOrganizations_MalformedBodyIsInvalidInput pins the code for an unparseable
// request. The Organizations model has no generic malformed-data exception, so a
// caller's catch branch is written against InvalidInputException.
func TestOrganizations_MalformedBodyIsInvalidInput(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		newOrgBadBody())
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Organizations_20161128.DescribeAccount")
	req.Host = "organizations.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if code, _ := out["__type"].(string); code != "InvalidInputException" {
		t.Errorf("expected __type=InvalidInputException, got %q", code)
	}
}

// TestOrganizations_UnsupportedOperation checks the claim chain's fallthrough:
// an operation no cluster claims is InvalidAction rather than a panic or a
// silent empty success.
func TestOrganizations_UnsupportedOperation(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	resp := orgsRequest(t, ts, "LeaveOrganization", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if code, _ := out["__type"].(string); code != "InvalidAction" {
		t.Errorf("expected __type=InvalidAction, got %q", code)
	}
}

// TestOrganizations_ListAccountsPaginates checks the pagination wiring on a real
// operation, not just the helper.
func TestOrganizations_ListAccountsPaginates(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for i := 0; i < 3; i++ {
		resp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
			"AccountName": "acct",
			"Email":       "a@example.com",
		})
		resp.Body.Close() //nolint:errcheck
	}

	seen := map[string]bool{}
	token := ""
	for i := 0; i < 10; i++ {
		body := map[string]interface{}{"MaxResults": 2}
		if token != "" {
			body["NextToken"] = token
		}
		resp := orgsRequest(t, ts, "ListAccounts", body)
		var out struct {
			Accounts []struct {
				ID string `json:"Id"`
			} `json:"Accounts"`
			NextToken string `json:"NextToken"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			resp.Body.Close() //nolint:errcheck
			t.Fatalf("decode: %v", err)
		}
		resp.Body.Close() //nolint:errcheck
		if len(out.Accounts) > 2 {
			t.Fatalf("expected at most 2 accounts per page, got %d", len(out.Accounts))
		}
		for _, a := range out.Accounts {
			seen[a.ID] = true
		}
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	// Three created plus the management account.
	if len(seen) != 4 {
		t.Errorf("expected to page through 4 accounts, saw %d", len(seen))
	}
}
