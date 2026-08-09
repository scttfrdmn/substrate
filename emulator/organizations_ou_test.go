package emulator_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// newOrgOUFixture returns a server together with the plugin and state manager it
// shares, so a test can drive the OU operations over the wire and then read the
// indexes underneath them. A refusal is only half of the contract: what the
// indexes look like afterwards is the other half, and no API call reports it.
func newOrgOUFixture(t *testing.T) (*httptest.Server, *emulator.OrganizationsPlugin, emulator.StateManager) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize organizations plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts, p, state
}

// orgOUCall posts an OU operation and decodes the response into out, returning
// the status and the error code the body carries (empty on success).
func orgOUCall(t *testing.T, ts *httptest.Server, op string, body, out interface{}) (int, string) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("%s decode: %v", op, err)
	}
	if resp.StatusCode != http.StatusOK {
		var fault struct {
			Type    string `json:"__type"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(raw, &fault); err != nil {
			t.Fatalf("%s decode fault: %v", op, err)
		}
		return resp.StatusCode, fault.Type
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s unmarshal output: %v", op, err)
		}
	}
	return resp.StatusCode, ""
}

// orgOUResponse is the CreateOrganizationalUnit / DescribeOrganizationalUnit /
// UpdateOrganizationalUnit output shape. The model's OrganizationalUnit has
// exactly three members, so the test decodes exactly those.
type orgOUResponse struct {
	OrganizationalUnit struct {
		ID   string `json:"Id"`
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	} `json:"OrganizationalUnit"`
}

// createOU creates an OU under parent and returns its ID, failing the test on any
// refusal.
func createOU(t *testing.T, ts *httptest.Server, parent, name string) string {
	t.Helper()
	var out orgOUResponse
	status, code := orgOUCall(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": parent, "Name": name}, &out)
	if status != http.StatusOK {
		t.Fatalf("CreateOrganizationalUnit(%s, %s): expected 200, got %d (%s)", parent, name, status, code)
	}
	if out.OrganizationalUnit.ID == "" {
		t.Fatalf("CreateOrganizationalUnit(%s, %s): no OU ID in the response", parent, name)
	}
	return out.OrganizationalUnit.ID
}

// TestOrganizations_CreateOrganizationalUnit_Shape pins the identity the rest of
// the release references. The middle segment of an OU ID is the root's suffix, so
// an OU created under a nested parent still names the root it belongs to; a caller
// that parses the ID to find its root would otherwise be reading the parent's
// suffix from the second level down.
func TestOrganizations_CreateOrganizationalUnit_Shape(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	var out orgOUResponse
	status, code := orgOUCall(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": rootID, "Name": "Workloads"}, &out)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", status, code)
	}
	ou := out.OrganizationalUnit
	if ou.Name != "Workloads" {
		t.Errorf("expected Name=Workloads, got %q", ou.Name)
	}
	wantPrefix := "ou-" + rootID[2:] + "-"
	if !strings.HasPrefix(ou.ID, wantPrefix) {
		t.Errorf("expected the OU ID to carry the root suffix %q, got %q", wantPrefix, ou.ID)
	}
	if suffix := strings.TrimPrefix(ou.ID, wantPrefix); len(suffix) != 8 {
		t.Errorf("expected an 8-character OU suffix, got %q", suffix)
	}
	if !strings.HasSuffix(ou.Arn, "/"+ou.ID) || !strings.HasPrefix(ou.Arn, "arn:aws:organizations::") {
		t.Errorf("expected an OU ARN ending in the OU ID, got %q", ou.Arn)
	}
	if !strings.Contains(ou.Arn, ":ou/o-") {
		t.Errorf("expected the ARN to carry the organization ID, got %q", ou.Arn)
	}

	// The model's OrganizationalUnit has no Path member, so a caller cannot come to
	// depend on one that AWS would not send.
	var raw map[string]map[string]interface{}
	if _, code := orgOUCall(t, ts, "DescribeOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ou.ID}, &raw); code != "" {
		t.Fatalf("DescribeOrganizationalUnit: %s", code)
	}
	for member := range raw["OrganizationalUnit"] {
		switch member {
		case "Id", "Arn", "Name":
		default:
			t.Errorf("OrganizationalUnit carries %q, which is not in the API model's shape", member)
		}
	}
}

// TestOrganizations_CreateOrganizationalUnit_NestedIDCarriesRootSuffix is the
// nesting half of the identity check: a second-level OU's ID must still name the
// root, not its parent OU.
func TestOrganizations_CreateOrganizationalUnit_NestedIDCarriesRootSuffix(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	first := createOU(t, ts, rootID, "Level1")
	second := createOU(t, ts, first, "Level2")
	if want := "ou-" + rootID[2:] + "-"; !strings.HasPrefix(second, want) {
		t.Errorf("expected the nested OU ID to carry the root suffix %q, got %q", want, second)
	}
}

// TestOrganizations_CreateOrganizationalUnit_AttachesFullAWSAccess pins the
// attachment AWS makes at creation. Without it ListPoliciesForTarget is wrong on a
// brand-new OU, and the minimum-attachment rule has nothing to hold — so a
// detach that should refuse would succeed and leave the OU denying everything.
func TestOrganizations_CreateOrganizationalUnit_AttachesFullAWSAccess(t *testing.T) {
	ts, p, _ := newOrgOUFixture(t)
	ouID := createOU(t, ts, orgListRootsID(t, ts), "Workloads")

	attached, err := p.LoadAttachmentsForTest(t.Context(), ouID)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 1 || attached[0] != emulator.OrgFullAWSAccessIDForTest {
		t.Errorf("expected only FullAWSAccess attached to the new OU, got %v", attached)
	}
	targets, err := p.LoadPolicyTargetsForTest(t.Context(), emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load policy targets: %v", err)
	}
	found := false
	for _, target := range targets {
		if target == ouID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected the OU in FullAWSAccess's target index, got %v", targets)
	}
}

// TestOrganizations_CreateOrganizationalUnit_NoAttachmentUnderConsolidatedBilling
// covers the other half: an organization where no policy type exists creates OUs
// with no SCP at all, matching an organization whose entities never carried one.
func TestOrganizations_CreateOrganizationalUnit_NoAttachmentUnderConsolidatedBilling(t *testing.T) {
	ts, p, state := newOrgOUFixture(t)
	if err := state.Put(t.Context(), "organizations-ctrl", "feature-set",
		[]byte(`{"featureSet":"CONSOLIDATED_BILLING"}`)); err != nil {
		t.Fatalf("seed feature set: %v", err)
	}
	ouID := createOU(t, ts, orgListRootsID(t, ts), "Workloads")

	attached, err := p.LoadAttachmentsForTest(t.Context(), ouID)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 0 {
		t.Errorf("expected no attachments under CONSOLIDATED_BILLING, got %v", attached)
	}
}

// TestOrganizations_OUDepthLimit is the decisive boundary test. A legal 5-deep
// tree and an illegal 6th level are indistinguishable on a nominal run, so this is
// the only thing that catches an off-by-one in either direction: a limit set one
// too low would refuse a layout AWS accepts, and one too high would let a caller
// build a tree AWS would reject at deploy time.
func TestOrganizations_OUDepthLimit(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	parent := rootID
	for level := 1; level <= emulator.OrgMaxOUDepthForTest; level++ {
		parent = createOU(t, ts, parent, fmt.Sprintf("Level%d", level))
	}

	status, code := orgOUCall(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": parent, "Name": "TooDeep"}, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for a level past the limit, got %d", status)
	}
	if code != "ConstraintViolationException" {
		t.Errorf("expected ConstraintViolationException, got %q", code)
	}

	// The reason travels in the message, since the JSON-RPC error document has no
	// Reason member — a caller distinguishing two ConstraintViolations has nothing
	// else to read.
	resp := orgsRequest(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": parent, "Name": "TooDeep"})
	defer resp.Body.Close() //nolint:errcheck
	var fault struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&fault); err != nil {
		t.Fatalf("decode fault: %v", err)
	}
	if !strings.Contains(fault.Message, "OU_DEPTH_LIMIT_EXCEEDED") {
		t.Errorf("expected OU_DEPTH_LIMIT_EXCEEDED in the message, got %q", fault.Message)
	}
	if emulator.OrgMaxOUDepthForTest != 5 {
		t.Errorf("expected the documented OU depth limit of 5, got %d", emulator.OrgMaxOUDepthForTest)
	}
}

// TestOrganizations_ListParentsReachesTheSameRoot is the #577/#578 join. Walking
// upward from a member account has to terminate at the very root ListRoots
// reports. Before the root was persisted that walk could never match, because
// every ListRoots minted a different ID — this is the property the rest of the
// release is built on, and the one a governance tool uses to decide whether an
// account is inside the organization it thinks it is.
func TestOrganizations_ListParentsReachesTheSameRoot(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	l1 := createOU(t, ts, rootID, "BusinessUnit")
	l2 := createOU(t, ts, l1, "Workloads")

	// ListParents on an OU walks up one level at a time.
	if got := orgListParent(t, ts, l2); got.ID != l1 || got.Type != emulator.OrgKindOUForTest {
		t.Fatalf("expected %s's parent to be the OU %s, got %+v", l2, l1, got)
	}
	if got := orgListParent(t, ts, l1); got.ID != rootID || got.Type != emulator.OrgKindRootForTest {
		t.Fatalf("expected %s's parent to be the root %s, got %+v", l1, rootID, got)
	}

	// And the same walk from the management account, which was placed in the root at
	// auto-creation, terminates at the identical root ID.
	current := orgTestAccount
	seenRoot := ""
	for step := 0; step < emulator.OrgMaxOUDepthForTest+2; step++ {
		parent := orgListParent(t, ts, current)
		if parent.Type == emulator.OrgKindRootForTest {
			seenRoot = parent.ID
			break
		}
		current = parent.ID
	}
	if seenRoot != rootID {
		t.Fatalf("the upward walk ended at %q, but ListRoots reports %q", seenRoot, rootID)
	}
}

// orgListParent posts ListParents and returns the single parent, asserting the
// API's "a child has exactly one parent" invariant on the way.
func orgListParent(t *testing.T, ts *httptest.Server, childID string) emulator.OrgParent {
	t.Helper()
	var out struct {
		Parents []struct {
			ID   string `json:"Id"`
			Type string `json:"Type"`
		} `json:"Parents"`
		NextToken string `json:"NextToken"`
	}
	status, code := orgOUCall(t, ts, "ListParents", map[string]interface{}{"ChildId": childID}, &out)
	if status != http.StatusOK {
		t.Fatalf("ListParents(%s): expected 200, got %d (%s)", childID, status, code)
	}
	if len(out.Parents) != 1 {
		t.Fatalf("ListParents(%s): expected exactly 1 parent, got %d", childID, len(out.Parents))
	}
	if out.NextToken != "" {
		t.Errorf("ListParents(%s): expected no NextToken for a single parent, got %q", childID, out.NextToken)
	}
	return emulator.OrgParent{ID: out.Parents[0].ID, Type: out.Parents[0].Type}
}

// TestOrganizations_DuplicateOUNameIsScopedToTheParent pins that uniqueness is
// per-parent. The common layout gives every business unit an identically named
// child ("Sandbox", "Prod"), so refusing the second one organization-wide would
// break a legal tree — and letting the same name repeat under one parent would
// make an OU unaddressable by name.
func TestOrganizations_DuplicateOUNameIsScopedToTheParent(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	bu1 := createOU(t, ts, rootID, "BusinessUnit1")
	bu2 := createOU(t, ts, rootID, "BusinessUnit2")

	// The same name under two different parents is legal.
	firstSandbox := createOU(t, ts, bu1, "Sandbox")
	secondSandbox := createOU(t, ts, bu2, "Sandbox")
	if firstSandbox == secondSandbox {
		t.Fatal("expected two distinct OUs for the same name under different parents")
	}

	// The same name under the same parent is not.
	status, code := orgOUCall(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": bu1, "Name": "Sandbox"}, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for a duplicate sibling name, got %d", status)
	}
	if code != "DuplicateOrganizationalUnitException" {
		t.Errorf("expected DuplicateOrganizationalUnitException, got %q", code)
	}
}

// TestOrganizations_DeleteOrganizationalUnit covers the emptiness rule and what
// the state looks like afterwards. Deleting a populated OU would orphan its
// contents somewhere no listing reaches, and a delete that left an index entry
// behind would leave an OU that appears in one listing and not another — a state
// no sequence of API calls can produce, so nothing downstream handles it.
func TestOrganizations_DeleteOrganizationalUnit(t *testing.T) {
	ts, p, _ := newOrgOUFixture(t)
	ctx := t.Context()
	rootID := orgListRootsID(t, ts)

	parentOU := createOU(t, ts, rootID, "BusinessUnit")
	childOU := createOU(t, ts, parentOU, "Sandbox")

	// A child OU makes the parent non-empty.
	status, code := orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": parentOU}, nil)
	if status != http.StatusBadRequest || code != "OrganizationalUnitNotEmptyException" {
		t.Fatalf("expected OrganizationalUnitNotEmptyException for an OU holding an OU, got %d (%s)", status, code)
	}

	// So does an account. The child is emptied and the account placed inside it
	// through the placement index, since MoveAccount is another lane's operation.
	if err := p.PlaceChildForTest(ctx, childOU, orgTestAccount); err != nil {
		t.Fatalf("place account in the OU: %v", err)
	}
	status, code = orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": childOU}, nil)
	if status != http.StatusBadRequest || code != "OrganizationalUnitNotEmptyException" {
		t.Fatalf("expected OrganizationalUnitNotEmptyException for an OU holding an account, got %d (%s)", status, code)
	}

	// Emptied, it deletes.
	if err := p.PlaceChildForTest(ctx, rootID, orgTestAccount); err != nil {
		t.Fatalf("move the account back to the root: %v", err)
	}
	if status, code := orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": childOU}, nil); status != http.StatusOK {
		t.Fatalf("expected 200 for an emptied OU, got %d (%s)", status, code)
	}

	// And it is gone from every direction a caller can look.
	if status, code := orgOUCall(t, ts, "DescribeOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": childOU}, nil); code != "OrganizationalUnitNotFoundException" {
		t.Errorf("expected the deleted OU to be not found, got %d (%s)", status, code)
	}
	if got := orgListOUsForParent(t, ts, parentOU); len(got) != 0 {
		t.Errorf("expected the deleted OU gone from its parent's listing, got %v", got)
	}
	ids, err := p.LoadOUIDsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load OU ids: %v", err)
	}
	for _, id := range ids {
		if id == childOU {
			t.Error("expected the deleted OU gone from the organization's OU index")
		}
	}
	parent, err := p.LoadParentForTest(ctx, childOU)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if parent != "" {
		t.Errorf("expected the deleted OU to have no recorded parent, got %q", parent)
	}
	attached, err := p.LoadAttachmentsForTest(ctx, childOU)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 0 {
		t.Errorf("expected the deleted OU's attachments removed, got %v", attached)
	}
	targets, err := p.LoadPolicyTargetsForTest(ctx, emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load policy targets: %v", err)
	}
	for _, target := range targets {
		if target == childOU {
			t.Error("expected the deleted OU gone from FullAWSAccess's target index")
		}
	}

	// The parent is now empty and deletes in turn, so a teardown that unwinds the
	// tree leaf-first converges.
	if status, code := orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": parentOU}, nil); status != http.StatusOK {
		t.Fatalf("expected the emptied parent to delete, got %d (%s)", status, code)
	}
}

// TestOrganizations_DeleteOrganizationalUnit_IsNotIdempotent pins the second call.
// A re-run of a teardown script gets a distinguishable refusal rather than a
// silent success, which is what lets it tell "I deleted it" from "it was never
// there".
func TestOrganizations_DeleteOrganizationalUnit_IsNotIdempotent(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	ouID := createOU(t, ts, orgListRootsID(t, ts), "Workloads")

	if status, code := orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID}, nil); status != http.StatusOK {
		t.Fatalf("first delete: expected 200, got %d (%s)", status, code)
	}
	status, code := orgOUCall(t, ts, "DeleteOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID}, nil)
	if status != http.StatusBadRequest || code != "OrganizationalUnitNotFoundException" {
		t.Errorf("second delete: expected OrganizationalUnitNotFoundException, got %d (%s)", status, code)
	}
}

// TestOrganizations_UpdateOrganizationalUnit covers the rename. The operation
// documents that the ID, the ARN, the children and the attached policies all
// survive it, so every handle a caller already holds stays valid — which is why a
// rename is safe to issue from a converging script.
func TestOrganizations_UpdateOrganizationalUnit(t *testing.T) {
	ts, p, _ := newOrgOUFixture(t)
	ctx := t.Context()
	rootID := orgListRootsID(t, ts)

	ouID := createOU(t, ts, rootID, "Workloads")
	childID := createOU(t, ts, ouID, "Sandbox")

	var out orgOUResponse
	status, code := orgOUCall(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID, "Name": "Production"}, &out)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", status, code)
	}
	if out.OrganizationalUnit.Name != "Production" {
		t.Errorf("expected the new name, got %q", out.OrganizationalUnit.Name)
	}
	if out.OrganizationalUnit.ID != ouID {
		t.Errorf("the rename changed the OU ID: %q became %q", ouID, out.OrganizationalUnit.ID)
	}

	// The children and the attachments are untouched.
	if got := orgListOUsForParent(t, ts, ouID); len(got) != 1 || got[0] != childID {
		t.Errorf("expected the child OU still in place after the rename, got %v", got)
	}
	attached, err := p.LoadAttachmentsForTest(ctx, ouID)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 1 || attached[0] != emulator.OrgFullAWSAccessIDForTest {
		t.Errorf("expected the attachment to survive the rename, got %v", attached)
	}

	// Renaming to the name it already has is not a duplicate: a converging script
	// reissues the same rename, and refusing the second run would make convergence
	// impossible to express.
	if status, code := orgOUCall(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID, "Name": "Production"}, nil); status != http.StatusOK {
		t.Errorf("expected a no-change rename to succeed, got %d (%s)", status, code)
	}

	// Renaming onto a sibling's name is.
	sibling := createOU(t, ts, rootID, "Shared")
	status, code = orgOUCall(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": sibling, "Name": "Production"}, nil)
	if status != http.StatusBadRequest || code != "DuplicateOrganizationalUnitException" {
		t.Errorf("expected DuplicateOrganizationalUnitException for a sibling's name, got %d (%s)", status, code)
	}

	// An omitted Name is not the same request as an empty one: the first leaves the
	// OU alone, the second asks for a name the model's minimum length forbids.
	var unchanged orgOUResponse
	if status, code := orgOUCall(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID}, &unchanged); status != http.StatusOK {
		t.Errorf("expected an omitted Name to leave the OU alone, got %d (%s)", status, code)
	}
	if unchanged.OrganizationalUnit.Name != "Production" {
		t.Errorf("expected the name unchanged, got %q", unchanged.OrganizationalUnit.Name)
	}
	if status, code := orgOUCall(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID, "Name": ""}, nil); code != "InvalidInputException" {
		t.Errorf("expected InvalidInputException for an empty Name, got %d (%s)", status, code)
	}
}

// TestOrganizations_ListChildrenFiltersByType pins the ChildType filter. The model
// requires it, so the operation never mixes accounts and OUs — a caller that got a
// superset would draw conclusions about entity types from a filter that never
// applied.
func TestOrganizations_ListChildrenFiltersByType(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")

	// The root now holds one OU and the management account.
	ous := orgListChildren(t, ts, rootID, emulator.OrgKindOUForTest)
	if len(ous) != 1 || ous[0] != ouID {
		t.Errorf("expected only the OU for ChildType=ORGANIZATIONAL_UNIT, got %v", ous)
	}
	accounts := orgListChildren(t, ts, rootID, emulator.OrgKindAccountForTest)
	if len(accounts) != 1 || accounts[0] != orgTestAccount {
		t.Errorf("expected only the management account for ChildType=ACCOUNT, got %v", accounts)
	}

	// An empty OU reports an empty list rather than a refusal — "nothing inside" is
	// a legitimate answer, and a caller looping over children has to be able to see
	// zero of them.
	if got := orgListChildren(t, ts, ouID, emulator.OrgKindAccountForTest); len(got) != 0 {
		t.Errorf("expected no accounts in the new OU, got %v", got)
	}

	// ChildType is required, and an unrecognized value is refused rather than
	// treated as "everything".
	for _, childType := range []interface{}{nil, "EVERYTHING"} {
		body := map[string]interface{}{"ParentId": rootID}
		if childType != nil {
			body["ChildType"] = childType
		}
		if status, code := orgOUCall(t, ts, "ListChildren", body, nil); code != "InvalidInputException" {
			t.Errorf("ChildType=%v: expected InvalidInputException, got %d (%s)", childType, status, code)
		}
	}
}

// TestOrganizations_ListAccountsForParent pins that placement is what the listing
// reads, not the organization-wide account index. A new account lands in the root
// and only MoveAccount puts it in an OU, so an OU listing that fell back to every
// account would report accounts as governed by policies that are not attached to
// them.
func TestOrganizations_ListAccountsForParent(t *testing.T) {
	ts, p, _ := newOrgOUFixture(t)
	ctx := t.Context()
	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")

	newAccount := orgVendAccount(t, ts, "dev", "dev@example.com")

	// Both accounts start in the root; the OU has none.
	if got := orgListAccountsForParent(t, ts, rootID); len(got) != 2 {
		t.Errorf("expected both accounts in the root, got %v", got)
	}
	if got := orgListAccountsForParent(t, ts, ouID); len(got) != 0 {
		t.Errorf("expected no accounts in the new OU, got %v", got)
	}

	// Moved into the OU, it appears there and only there.
	if err := p.PlaceChildForTest(ctx, ouID, newAccount); err != nil {
		t.Fatalf("place account: %v", err)
	}
	if got := orgListAccountsForParent(t, ts, ouID); len(got) != 1 || got[0] != newAccount {
		t.Errorf("expected the moved account in the OU, got %v", got)
	}
	if got := orgListAccountsForParent(t, ts, rootID); len(got) != 1 || got[0] != orgTestAccount {
		t.Errorf("expected only the management account left in the root, got %v", got)
	}
}

// TestOrganizations_OUListingsPaginate walks each listing with MaxResults set. A
// caller that ignores NextToken gets a truncated answer with no error, and for a
// tree walk that means concluding an OU has fewer children than it does — so every
// listing has to terminate and to cover everything.
func TestOrganizations_OUListingsPaginate(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	want := map[string]bool{}
	for i := 0; i < 5; i++ {
		want[createOU(t, ts, rootID, fmt.Sprintf("OU%d", i))] = true
	}

	for _, op := range []string{"ListOrganizationalUnitsForParent", "ListChildren"} {
		t.Run(op, func(t *testing.T) {
			seen := map[string]bool{}
			token := ""
			for page := 0; page < 10; page++ {
				body := map[string]interface{}{"ParentId": rootID, "MaxResults": 2}
				if op == "ListChildren" {
					body["ChildType"] = emulator.OrgKindOUForTest
				}
				if token != "" {
					body["NextToken"] = token
				}
				var out struct {
					OrganizationalUnits []struct {
						ID string `json:"Id"`
					} `json:"OrganizationalUnits"`
					Children []struct {
						ID string `json:"Id"`
					} `json:"Children"`
					NextToken string `json:"NextToken"`
				}
				if status, code := orgOUCall(t, ts, op, body, &out); status != http.StatusOK {
					t.Fatalf("%s: expected 200, got %d (%s)", op, status, code)
				}
				items := out.OrganizationalUnits
				if op == "ListChildren" {
					items = out.Children
				}
				if len(items) > 2 {
					t.Fatalf("%s: expected at most 2 per page, got %d", op, len(items))
				}
				for _, item := range items {
					seen[item.ID] = true
				}
				if out.NextToken == "" {
					break
				}
				token = out.NextToken
			}
			if len(seen) != len(want) {
				t.Errorf("%s: expected to page through %d OUs, saw %d", op, len(want), len(seen))
			}
			for id := range want {
				if !seen[id] {
					t.Errorf("%s: OU %s never appeared in any page", op, id)
				}
			}
		})
	}
}

// TestOrganizations_ListAccountsForParentPaginates covers the account listing's
// own NextToken. It is a separate index from the OU listing's, so a token emitted
// on one and not the other is a real possibility, and a caller reading one page of
// an OU's accounts would conclude the rest are ungoverned.
func TestOrganizations_ListAccountsForParentPaginates(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	for i := 0; i < 2; i++ {
		resp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
			"AccountName": fmt.Sprintf("acct%d", i), "Email": fmt.Sprintf("a%d@example.com", i),
		})
		resp.Body.Close() //nolint:errcheck
	}

	var out struct {
		Accounts []struct {
			ID string `json:"Id"`
		} `json:"Accounts"`
		NextToken string `json:"NextToken"`
	}
	if status, code := orgOUCall(t, ts, "ListAccountsForParent",
		map[string]interface{}{"ParentId": rootID, "MaxResults": 1}, &out); status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", status, code)
	}
	if len(out.Accounts) != 1 {
		t.Fatalf("expected 1 account on the first page, got %d", len(out.Accounts))
	}
	if out.NextToken == "" {
		t.Fatal("expected a NextToken for a truncated account listing")
	}
	// Following it reaches all three: the management account plus the two created.
	if got := orgListAccountsForParent(t, ts, rootID); len(got) != 3 {
		t.Errorf("expected the walk to reach 3 accounts, got %v", got)
	}
}

// TestOrganizations_OUListingsRejectAStaleToken pins that a token naming no known
// ID is refused. Restarting from the beginning is the worse answer: the paging
// loop never terminates and the caller sees the first page forever with no error
// to explain it.
func TestOrganizations_OUListingsRejectAStaleToken(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	cases := []struct {
		op   string
		body map[string]interface{}
	}{
		{"ListOrganizationalUnitsForParent", map[string]interface{}{"ParentId": rootID}},
		{"ListChildren", map[string]interface{}{"ParentId": rootID, "ChildType": emulator.OrgKindAccountForTest}},
		{"ListAccountsForParent", map[string]interface{}{"ParentId": rootID}},
		{"ListParents", map[string]interface{}{"ChildId": orgTestAccount}},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			body := map[string]interface{}{"NextToken": "bm90LWEtcmVhbC1pZA=="}
			for k, v := range c.body {
				body[k] = v
			}
			if status, code := orgOUCall(t, ts, c.op, body, nil); status != http.StatusBadRequest ||
				code != "InvalidInputException" {
				t.Errorf("expected InvalidInputException for a stale token, got %d (%s)", status, code)
			}
		})
	}
}

// TestOrganizations_OUNotFoundRefusals covers the per-operation not-found codes.
// They are deliberately different: a caller retrying a tree walk has to tell "the
// OU I am describing is gone" from "the parent I am listing under is gone", and
// the two lead to different recovery.
func TestOrganizations_OUNotFoundRefusals(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)
	missingOU := "ou-" + rootID[2:] + "-99999999"

	cases := []struct {
		name string
		op   string
		body map[string]interface{}
		code string
	}{
		{
			"describe an unknown OU", "DescribeOrganizationalUnit",
			map[string]interface{}{"OrganizationalUnitId": missingOU}, "OrganizationalUnitNotFoundException",
		},
		{
			"update an unknown OU", "UpdateOrganizationalUnit",
			map[string]interface{}{"OrganizationalUnitId": missingOU, "Name": "x"}, "OrganizationalUnitNotFoundException",
		},
		{
			"delete an unknown OU", "DeleteOrganizationalUnit",
			map[string]interface{}{"OrganizationalUnitId": missingOU}, "OrganizationalUnitNotFoundException",
		},
		{
			"create under an unknown parent", "CreateOrganizationalUnit",
			map[string]interface{}{"ParentId": missingOU, "Name": "x"}, "ParentNotFoundException",
		},
		{
			"create under an unknown root", "CreateOrganizationalUnit",
			map[string]interface{}{"ParentId": "r-zzzz", "Name": "x"}, "ParentNotFoundException",
		},
		{
			"list OUs under an unknown parent", "ListOrganizationalUnitsForParent",
			map[string]interface{}{"ParentId": missingOU}, "ParentNotFoundException",
		},
		{
			"list children of an unknown parent", "ListChildren",
			map[string]interface{}{"ParentId": missingOU, "ChildType": emulator.OrgKindAccountForTest},
			"ParentNotFoundException",
		},
		{
			"list accounts under an unknown parent", "ListAccountsForParent",
			map[string]interface{}{"ParentId": missingOU}, "ParentNotFoundException",
		},
		{
			"list parents of an unknown OU", "ListParents",
			map[string]interface{}{"ChildId": missingOU}, "ChildNotFoundException",
		},
		{
			"list parents of an unknown account", "ListParents",
			map[string]interface{}{"ChildId": "999999999999"}, "ChildNotFoundException",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := orgOUCall(t, ts, c.op, c.body, nil)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", status)
			}
			if code != c.code {
				t.Errorf("expected %s, got %q", c.code, code)
			}
		})
	}
}

// TestOrganizations_OUInvalidInputRefusals covers the requests whose IDs are the
// wrong shape rather than merely absent. The distinction matters: a malformed ID
// answered with a not-found code sends the caller looking for an entity that could
// never have existed, instead of telling it the ID it built is wrong.
func TestOrganizations_OUInvalidInputRefusals(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	cases := []struct {
		name string
		op   string
		body map[string]interface{}
	}{
		{"create with no ParentId", "CreateOrganizationalUnit", map[string]interface{}{"Name": "x"}},
		{
			"create with an account as the parent", "CreateOrganizationalUnit",
			map[string]interface{}{"ParentId": orgTestAccount, "Name": "x"},
		},
		{"create with no Name", "CreateOrganizationalUnit", map[string]interface{}{"ParentId": rootID}},
		{
			"create with an over-long Name", "CreateOrganizationalUnit",
			map[string]interface{}{"ParentId": rootID, "Name": strings.Repeat("a", 129)},
		},
		{
			"update with an over-long Name", "UpdateOrganizationalUnit",
			map[string]interface{}{"OrganizationalUnitId": "ou-" + rootID[2:] + "-11112222", "Name": strings.Repeat("a", 129)},
		},
		{"describe with no OU ID", "DescribeOrganizationalUnit", map[string]interface{}{}},
		{
			"describe with a root ID", "DescribeOrganizationalUnit",
			map[string]interface{}{"OrganizationalUnitId": rootID},
		},
		{"update with no OU ID", "UpdateOrganizationalUnit", map[string]interface{}{"Name": "x"}},
		{"delete with no OU ID", "DeleteOrganizationalUnit", map[string]interface{}{}},
		{"list OUs with no ParentId", "ListOrganizationalUnitsForParent", map[string]interface{}{}},
		{
			"list children with no ParentId", "ListChildren",
			map[string]interface{}{"ChildType": emulator.OrgKindAccountForTest},
		},
		{"list accounts with no ParentId", "ListAccountsForParent", map[string]interface{}{}},
		{"list parents with no ChildId", "ListParents", map[string]interface{}{}},
		{"list parents of a root", "ListParents", map[string]interface{}{"ChildId": rootID}},
		{"list parents of a malformed ID", "ListParents", map[string]interface{}{"ChildId": "not-an-id"}},
		{"list parents of a short account ID", "ListParents", map[string]interface{}{"ChildId": "12345"}},
		{"list parents of a non-numeric 12-char ID", "ListParents", map[string]interface{}{"ChildId": "00000000000x"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := orgOUCall(t, ts, c.op, c.body, nil)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", status)
			}
			if code != "InvalidInputException" {
				t.Errorf("expected InvalidInputException, got %q", code)
			}
		})
	}
}

// TestOrganizations_CreateOrganizationalUnit_Tags covers the tags the operation
// accepts inline. The operation documents that an invalid tag fails the whole
// request and leaves no OU behind — otherwise a caller's retry would hit
// DuplicateOrganizationalUnitException for an OU it does not believe it created.
func TestOrganizations_CreateOrganizationalUnit_Tags(t *testing.T) {
	ts, p, _ := newOrgOUFixture(t)
	ctx := t.Context()
	rootID := orgListRootsID(t, ts)

	var out orgOUResponse
	if status, code := orgOUCall(t, ts, "CreateOrganizationalUnit", map[string]interface{}{
		"ParentId": rootID,
		"Name":     "Tagged",
		"Tags":     []map[string]string{{"Key": "Owner", "Value": "platform"}},
	}, &out); status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", status, code)
	}
	tags, err := p.LoadTagsForTest(ctx, out.OrganizationalUnit.ID)
	if err != nil {
		t.Fatalf("load tags: %v", err)
	}
	if len(tags) != 1 || tags[0].Key != "Owner" || tags[0].Value != "platform" {
		t.Errorf("expected the request's tag stored on the OU, got %+v", tags)
	}

	refusals := []struct {
		name string
		tags []map[string]string
		code string
	}{
		{
			"a duplicate tag key", []map[string]string{{"Key": "Owner", "Value": "a"}, {"Key": "Owner", "Value": "b"}},
			"InvalidInputException",
		},
		{"an empty tag key", []map[string]string{{"Key": "", "Value": "a"}}, "InvalidInputException"},
	}
	for _, r := range refusals {
		t.Run(r.name, func(t *testing.T) {
			status, code := orgOUCall(t, ts, "CreateOrganizationalUnit", map[string]interface{}{
				"ParentId": rootID, "Name": "Rejected", "Tags": r.tags,
			}, nil)
			if status != http.StatusBadRequest || code != r.code {
				t.Fatalf("expected %s, got %d (%s)", r.code, status, code)
			}
			// Nothing was created, so the name is still free.
			if got := orgListOUsForParent(t, ts, rootID); len(got) != 1 {
				t.Errorf("expected the refused OU not to exist, parent holds %v", got)
			}
		})
	}

	t.Run("more tags than the quota allows", func(t *testing.T) {
		tags := make([]map[string]string, emulator.OrgMaxTagsPerResourceForTest+1)
		for i := range tags {
			tags[i] = map[string]string{"Key": fmt.Sprintf("k%d", i), "Value": "v"}
		}
		status, code := orgOUCall(t, ts, "CreateOrganizationalUnit", map[string]interface{}{
			"ParentId": rootID, "Name": "Rejected", "Tags": tags,
		}, nil)
		if status != http.StatusBadRequest || code != "ConstraintViolationException" {
			t.Errorf("expected ConstraintViolationException, got %d (%s)", status, code)
		}
	})
}

// TestOrganizations_OUNumberLimit exercises the count quota's comparison without
// creating two thousand OUs: the organization's OU index is filled directly, so
// the handler reads a full organization and refuses the next create. Asserting the
// constant alone would not catch a handler that never consults it.
func TestOrganizations_OUNumberLimit(t *testing.T) {
	ts, _, state := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)

	if emulator.OrgMaxOUsPerOrgForTest != 2000 {
		t.Errorf("expected the documented OU quota of 2000, got %d", emulator.OrgMaxOUsPerOrgForTest)
	}
	ids := make([]string, emulator.OrgMaxOUsPerOrgForTest)
	for i := range ids {
		ids[i] = fmt.Sprintf("ou-%s-%08d", rootID[2:], i)
	}
	index, err := json.Marshal(ids)
	if err != nil {
		t.Fatalf("marshal OU index: %v", err)
	}
	if err := state.Put(t.Context(), "organizations", "ou_ids:"+orgTestAccount, index); err != nil {
		t.Fatalf("fill the OU index: %v", err)
	}

	status, code := orgOUCall(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": rootID, "Name": "OneTooMany"}, nil)
	if status != http.StatusBadRequest || code != "ConstraintViolationException" {
		t.Fatalf("expected ConstraintViolationException at the quota, got %d (%s)", status, code)
	}

	resp := orgsRequest(t, ts, "CreateOrganizationalUnit",
		map[string]interface{}{"ParentId": rootID, "Name": "OneTooMany"})
	defer resp.Body.Close() //nolint:errcheck
	var fault struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&fault); err != nil {
		t.Fatalf("decode fault: %v", err)
	}
	if !strings.Contains(fault.Message, "OU_NUMBER_LIMIT_EXCEEDED") {
		t.Errorf("expected OU_NUMBER_LIMIT_EXCEEDED in the message, got %q", fault.Message)
	}
}

// TestOrganizations_OUListingsSkipVanishedRecords asserts a listing tolerates an
// index entry whose record is gone rather than reporting a zero-valued entity. An
// OU with an empty Id in a listing is worse than a short page: a consumer
// iterating it would call Describe with "" and get a refusal it cannot explain.
func TestOrganizations_OUListingsSkipVanishedRecords(t *testing.T) {
	ts, _, state := newOrgOUFixture(t)
	ctx := t.Context()
	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")

	if err := state.Delete(ctx, "organizations", "ou:"+ouID); err != nil {
		t.Fatalf("delete the OU record: %v", err)
	}
	if got := orgListOUsForParent(t, ts, rootID); len(got) != 0 {
		t.Errorf("expected the vanished OU skipped rather than listed empty, got %v", got)
	}

	if err := state.Delete(ctx, "organizations", "account:"+orgTestAccount); err != nil {
		t.Fatalf("delete the account record: %v", err)
	}
	if got := orgListAccountsForParent(t, ts, rootID); len(got) != 0 {
		t.Errorf("expected the vanished account skipped rather than listed empty, got %v", got)
	}
}

// TestOrganizations_OUStoreFailuresAreInternalFailure pins the wire result of a
// store failure on every OU operation: a 500 an SDK retries, rather than a 400 it
// treats as terminal. Reported as a 400 not-found, a blip would send a governance
// script down the "create it again" path over an OU that already exists.
func TestOrganizations_OUStoreFailuresAreInternalFailure(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "children:", err: errors.New("store unavailable"), onGet: true}

	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)
	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")
	state.armed = true

	cases := []struct {
		op   string
		body map[string]interface{}
	}{
		{"CreateOrganizationalUnit", map[string]interface{}{"ParentId": rootID, "Name": "Another"}},
		{"UpdateOrganizationalUnit", map[string]interface{}{"OrganizationalUnitId": ouID, "Name": "Renamed"}},
		{"DeleteOrganizationalUnit", map[string]interface{}{"OrganizationalUnitId": ouID}},
		{"ListOrganizationalUnitsForParent", map[string]interface{}{"ParentId": rootID}},
		{"ListChildren", map[string]interface{}{"ParentId": rootID, "ChildType": emulator.OrgKindAccountForTest}},
		{"ListAccountsForParent", map[string]interface{}{"ParentId": rootID}},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			resp := orgsRequest(t, ts, c.op, c.body)
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected 500 for a store failure, got %d", gotStatus)
			}
		})
	}
}

// newOrgOUFaultServer returns a server whose state fails one kind of operation for
// keys under a prefix once armed, plus the armer, so a test can let the
// organization be built normally and then break one write.
func newOrgOUFaultServer(t *testing.T, prefix string, onPut, onDelete, onGet bool) (*httptest.Server, func()) {
	t.Helper()
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{
		inner: inner, prefix: prefix, err: errors.New("store unavailable"),
		onPut: onPut, onDelete: onDelete, onGet: onGet,
	}
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)
	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts, func() { state.armed = true }
}

// TestOrganizations_CreateOUWriteFailuresAreRefused walks every write
// CreateOrganizationalUnit performs and asserts a failure at each one is reported
// rather than answered as a successful create. Creation is four or five writes —
// the OU record, the organization's OU index, both directions of the placement, the
// FullAWSAccess attachment, and the inline tags — and a caller told the OU exists
// when only some landed would go on to place accounts inside an OU no listing
// reaches. The prefixes are named individually so a reordering cannot quietly stop
// covering one.
func TestOrganizations_CreateOUWriteFailuresAreRefused(t *testing.T) {
	for _, prefix := range []string{"ou:", "ou_ids:", "children:", "parent:", "attachments:", "tags:"} {
		t.Run(prefix, func(t *testing.T) {
			ts, arm := newOrgOUFaultServer(t, prefix, true, false, false)
			rootID := orgListRootsID(t, ts)
			arm()

			resp := orgsRequest(t, ts, "CreateOrganizationalUnit", map[string]interface{}{
				"ParentId": rootID,
				"Name":     "Workloads",
				"Tags":     []map[string]string{{"Key": "Owner", "Value": "platform"}},
			})
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected a failed %s write to refuse the create, got %d", prefix, gotStatus)
			}
		})
	}
}

// TestOrganizations_OUUnwindFailuresAreRefused covers the deletion's index unwind.
// A delete reported successful with only some indexes unwound leaves an OU that
// exists in one listing and not another, which is the one state no sequence of API
// calls can produce — so nothing downstream is prepared for it, and reporting the
// failure is the only honest answer.
func TestOrganizations_OUUnwindFailuresAreRefused(t *testing.T) {
	cases := []struct {
		prefix   string
		onPut    bool
		onDelete bool
	}{
		{"attachments:", true, false},
		{"policy_targets:", true, false},
		{"children:", true, false},
		{"ou_ids:", true, false},
		{"ou:", false, true},
		{"parent:", false, true},
		{"tags:", false, true},
	}
	for _, c := range cases {
		t.Run(c.prefix, func(t *testing.T) {
			ts, arm := newOrgOUFaultServer(t, c.prefix, c.onPut, c.onDelete, false)
			ouID := createOU(t, ts, orgListRootsID(t, ts), "Workloads")
			arm()

			resp := orgsRequest(t, ts, "DeleteOrganizationalUnit",
				map[string]interface{}{"OrganizationalUnitId": ouID})
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected a failed %s unwind to refuse the delete, got %d", c.prefix, gotStatus)
			}
		})
	}
}

// TestOrganizations_RenameWriteFailureIsRefused covers the rename's single write.
// A rename reported successful that never landed is invisible until the next read,
// and a converging script would keep reissuing it forever without an error to
// explain why the name never changes.
func TestOrganizations_RenameWriteFailureIsRefused(t *testing.T) {
	ts, arm := newOrgOUFaultServer(t, "ou:", true, false, false)
	ouID := createOU(t, ts, orgListRootsID(t, ts), "Workloads")
	arm()

	resp := orgsRequest(t, ts, "UpdateOrganizationalUnit",
		map[string]interface{}{"OrganizationalUnitId": ouID, "Name": "Production"})
	gotStatus := resp.StatusCode
	resp.Body.Close() //nolint:errcheck
	if gotStatus != http.StatusInternalServerError {
		t.Errorf("expected the failed rename write to be reported, got %d", gotStatus)
	}
}

// TestOrganizations_OUReadFailuresAreRefused covers the reads each operation makes
// before it decides anything: the OU record, the organization, and the placement
// index. A swallowed read failure resolves to "no such entity", which every one of
// these turns into a 400 not-found — telling the caller its OU is gone when the
// store merely could not be read, and sending it down a create-again path.
func TestOrganizations_OUReadFailuresAreRefused(t *testing.T) {
	cases := []struct {
		prefix string
		op     string
		body   func(root, ou string) map[string]interface{}
	}{
		{"ou:", "DescribeOrganizationalUnit", func(_, ou string) map[string]interface{} {
			return map[string]interface{}{"OrganizationalUnitId": ou}
		}},
		{"ou:", "CreateOrganizationalUnit", func(root, _ string) map[string]interface{} {
			return map[string]interface{}{"ParentId": root, "Name": "Another"}
		}},
		{"org:", "ListParents", func(_, ou string) map[string]interface{} {
			return map[string]interface{}{"ChildId": ou}
		}},
		{"ou_ids:", "CreateOrganizationalUnit", func(root, _ string) map[string]interface{} {
			return map[string]interface{}{"ParentId": root, "Name": "Another"}
		}},
		{"parent:", "CreateOrganizationalUnit", func(_, ou string) map[string]interface{} {
			return map[string]interface{}{"ParentId": ou, "Name": "Nested"}
		}},
	}
	for _, c := range cases {
		t.Run(c.prefix+" "+c.op, func(t *testing.T) {
			ts, arm := newOrgOUFaultServer(t, c.prefix, false, false, true)
			rootID := orgListRootsID(t, ts)
			ouID := createOU(t, ts, rootID, "Workloads")
			arm()

			resp := orgsRequest(t, ts, c.op, c.body(rootID, ouID))
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected the %s read failure to be reported, got %d", c.prefix, gotStatus)
			}
		})
	}
}

// TestOrganizations_DuplicateScanReadFailureIsRefused covers the sibling-name scan.
// Reading a failure there as "the name is free" would let a second create succeed
// where AWS refuses, producing two OUs a caller cannot tell apart by name.
func TestOrganizations_DuplicateScanReadFailureIsRefused(t *testing.T) {
	ts, arm := newOrgOUFaultServer(t, "ou:", false, false, true)
	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")
	arm()

	for _, c := range []struct {
		op   string
		body map[string]interface{}
	}{
		{"CreateOrganizationalUnit", map[string]interface{}{"ParentId": rootID, "Name": "Workloads"}},
		{"UpdateOrganizationalUnit", map[string]interface{}{"OrganizationalUnitId": ouID, "Name": "Renamed"}},
	} {
		resp := orgsRequest(t, ts, c.op, c.body)
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusInternalServerError {
			t.Errorf("%s: expected the duplicate-scan read failure to be reported, got %d", c.op, gotStatus)
		}
	}
}

// TestOrganizations_ListParentsStoreFailureIsInternalFailure covers the placement
// read specifically. ListParents is the operation the upward walk depends on, and a
// swallowed failure there would report a child as parentless, which reads as "not
// in the organization".
func TestOrganizations_ListParentsStoreFailureIsInternalFailure(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "parent:", err: errors.New("store unavailable"), onGet: true}

	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)
	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	orgListRootsID(t, ts)
	state.armed = true

	resp := orgsRequest(t, ts, "ListParents", map[string]interface{}{"ChildId": orgTestAccount})
	gotStatus := resp.StatusCode
	resp.Body.Close() //nolint:errcheck
	if gotStatus != http.StatusInternalServerError {
		t.Errorf("expected 500 for a placement read failure, got %d", gotStatus)
	}
}

// TestOrganizations_ListParentsOnAnUnplacedChildIsAnError pins the pairing that
// cannot happen through the API: an OU record with no placement. Answering with an
// empty Parents list would silently break the upward walk, and the caller would
// conclude the entity sits outside the organization.
func TestOrganizations_ListParentsOnAnUnplacedChildIsAnError(t *testing.T) {
	ts, _, state := newOrgOUFixture(t)
	rootID := orgListRootsID(t, ts)
	ouID := createOU(t, ts, rootID, "Workloads")

	if err := state.Delete(t.Context(), "organizations", "parent:"+ouID); err != nil {
		t.Fatalf("delete the placement: %v", err)
	}
	resp := orgsRequest(t, ts, "ListParents", map[string]interface{}{"ChildId": ouID})
	gotStatus := resp.StatusCode
	resp.Body.Close() //nolint:errcheck
	if gotStatus != http.StatusInternalServerError {
		t.Errorf("expected 500 for an unplaced child, got %d", gotStatus)
	}
}

// TestOrganizations_OUMalformedBodyPerOperation covers the decode guard on every
// operation this file claims. One that skipped it would decode into a zero-valued
// input and answer as though the caller had asked about the empty OU, which is a
// wrong answer rather than an error.
func TestOrganizations_OUMalformedBodyPerOperation(t *testing.T) {
	ts, _, _ := newOrgOUFixture(t)

	for _, op := range []string{
		"CreateOrganizationalUnit", "DescribeOrganizationalUnit", "UpdateOrganizationalUnit",
		"DeleteOrganizationalUnit", "ListOrganizationalUnitsForParent", "ListParents",
		"ListChildren", "ListAccountsForParent",
	} {
		t.Run(op, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", newOrgBadBody())
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
			req.Host = "organizations.us-east-1.amazonaws.com"
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusBadRequest {
				t.Errorf("expected 400 for an unparseable body, got %d", gotStatus)
			}
		})
	}
}

// --- listing helpers ---

// orgListOUsForParent returns every OU ID under parent, walking NextToken.
func orgListOUsForParent(t *testing.T, ts *httptest.Server, parent string) []string {
	t.Helper()
	var ids []string
	token := ""
	for page := 0; page < 20; page++ {
		body := map[string]interface{}{"ParentId": parent}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			OrganizationalUnits []struct {
				ID string `json:"Id"`
			} `json:"OrganizationalUnits"`
			NextToken string `json:"NextToken"`
		}
		if status, code := orgOUCall(t, ts, "ListOrganizationalUnitsForParent", body, &out); status != http.StatusOK {
			t.Fatalf("ListOrganizationalUnitsForParent(%s): expected 200, got %d (%s)", parent, status, code)
		}
		for _, ou := range out.OrganizationalUnits {
			ids = append(ids, ou.ID)
		}
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	return ids
}

// orgListAccountsForParent returns every account ID under parent, walking NextToken.
func orgListAccountsForParent(t *testing.T, ts *httptest.Server, parent string) []string {
	t.Helper()
	var ids []string
	token := ""
	for page := 0; page < 20; page++ {
		body := map[string]interface{}{"ParentId": parent}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			Accounts []struct {
				ID string `json:"Id"`
			} `json:"Accounts"`
			NextToken string `json:"NextToken"`
		}
		if status, code := orgOUCall(t, ts, "ListAccountsForParent", body, &out); status != http.StatusOK {
			t.Fatalf("ListAccountsForParent(%s): expected 200, got %d (%s)", parent, status, code)
		}
		for _, a := range out.Accounts {
			ids = append(ids, a.ID)
		}
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	return ids
}

// orgListChildren returns every child ID of one type under parent, asserting each
// entry reports the type that was asked for.
func orgListChildren(t *testing.T, ts *httptest.Server, parent, childType string) []string {
	t.Helper()
	var ids []string
	token := ""
	for page := 0; page < 20; page++ {
		body := map[string]interface{}{"ParentId": parent, "ChildType": childType}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			Children []struct {
				ID   string `json:"Id"`
				Type string `json:"Type"`
			} `json:"Children"`
			NextToken string `json:"NextToken"`
		}
		if status, code := orgOUCall(t, ts, "ListChildren", body, &out); status != http.StatusOK {
			t.Fatalf("ListChildren(%s, %s): expected 200, got %d (%s)", parent, childType, status, code)
		}
		for _, child := range out.Children {
			if child.Type != childType {
				t.Errorf("ListChildren(%s, %s) reported a %s child", parent, childType, child.Type)
			}
			ids = append(ids, child.ID)
		}
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	return ids
}
