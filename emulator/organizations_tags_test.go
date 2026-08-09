package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// --- fixtures -------------------------------------------------------------

// orgTagsFixture is a running Organizations surface plus the state and plugin
// behind it, so a test can both call the API and reach past it to create the
// entities the other lanes' operations would create.
type orgTagsFixture struct {
	ts     *httptest.Server
	state  emulator.StateManager
	plugin *emulator.OrganizationsPlugin
	root   string
}

// newOrgTagsFixture returns a server whose state is also directly reachable.
// Tagging spans all four entity kinds, and only the account and the root exist
// without the OU and policy lifecycle operations that other lanes own, so OUs and
// policies are written through the storage layer instead.
func newOrgTagsFixture(t *testing.T) *orgTagsFixture {
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

	root, err := p.LoadRootForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	return &orgTagsFixture{ts: ts, state: state, plugin: p, root: root.ID}
}

// putOU writes an OU through the storage layer, placed in the root, with the ARN
// the API would report.
func (f *orgTagsFixture) putOU(t *testing.T, suffix string) string {
	t.Helper()
	id := "ou-" + f.root[2:] + "-" + suffix
	ou := emulator.OrgOrganizationalUnit{
		ID:   id,
		Arn:  "arn:aws:organizations::" + orgTestAccount + ":ou/o-tagtest/" + id,
		Name: "ou-" + suffix,
	}
	if err := f.plugin.SaveOUForTest(t.Context(), orgTestAccount, ou); err != nil {
		t.Fatalf("save OU: %v", err)
	}
	if err := f.plugin.PlaceChildForTest(t.Context(), f.root, id); err != nil {
		t.Fatalf("place OU: %v", err)
	}
	return id
}

// putPolicy writes an SCP through the storage layer.
func (f *orgTagsFixture) putPolicy(t *testing.T, id string) string {
	t.Helper()
	pol := emulator.OrgPolicy{
		PolicySummary: emulator.OrgPolicySummary{
			ID:   id,
			Arn:  "arn:aws:organizations::" + orgTestAccount + ":policy/o-tagtest/service_control_policy/" + id,
			Name: "policy-" + id,
			Type: emulator.OrgPolicyTypeSCPForTest,
		},
		Content: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
	}
	if err := f.plugin.SavePolicyForTest(t.Context(), orgTestAccount, pol); err != nil {
		t.Fatalf("save policy: %v", err)
	}
	return id
}

// tagResource posts TagResource and returns the response status.
func (f *orgTagsFixture) tagResource(t *testing.T, resourceID string, tags []map[string]any) (int, string) {
	t.Helper()
	return orgTagCall(t, f.ts, "TagResource", map[string]any{"ResourceId": resourceID, "Tags": tags})
}

// untagResource posts UntagResource and returns the response status.
func (f *orgTagsFixture) untagResource(t *testing.T, resourceID string, keys []string) (int, string) {
	t.Helper()
	return orgTagCall(t, f.ts, "UntagResource", map[string]any{"ResourceId": resourceID, "TagKeys": keys})
}

// listTags posts ListTagsForResource and returns the decoded tags and token.
func (f *orgTagsFixture) listTags(t *testing.T, resourceID, nextToken string) (map[string]string, string) {
	t.Helper()
	body := map[string]any{"ResourceId": resourceID}
	if nextToken != "" {
		body["NextToken"] = nextToken
	}
	resp := orgsRequest(t, f.ts, "ListTagsForResource", body)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource %s: expected 200, got %d", resourceID, resp.StatusCode)
	}
	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
		NextToken string `json:"NextToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode ListTagsForResource: %v", err)
	}
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags, out.NextToken
}

// listAllTags walks every page of ListTagsForResource, which is what a test
// asserting on a whole tag set has to do: the operation has no MaxResults, and
// its page ceiling is below the per-resource tag limit, so one page is not the
// whole set.
func (f *orgTagsFixture) listAllTags(t *testing.T, resourceID string) map[string]string {
	t.Helper()
	all := make(map[string]string)
	token := ""
	for pages := 0; ; pages++ {
		if pages > emulator.OrgMaxTagsPerResourceForTest {
			t.Fatal("pagination did not terminate")
		}
		page, next := f.listTags(t, resourceID, token)
		for k, v := range page {
			all[k] = v
		}
		if next == "" {
			return all
		}
		token = next
	}
}

// orgTagCall posts one operation and returns the status and the error code the
// body carries, so a refusal can be asserted by code rather than by status alone
// — every Organizations exception shares HTTP 400.
func orgTagCall(t *testing.T, ts *httptest.Server, op string, body map[string]any) (int, string) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return resp.StatusCode, ""
	}
	if out.Type != "" {
		// The reason travels in the message, since the JSON-RPC error document has
		// no Reason member; return both joined so a test can assert on either.
		return resp.StatusCode, out.Type + "/" + out.Message
	}
	return resp.StatusCode, ""
}

// orgTag is shorthand for one tag in a TagResource body.
func orgTag(key, value string) map[string]any {
	return map[string]any{"Key": key, "Value": value}
}

// --- happy paths ----------------------------------------------------------

// TestOrganizations_TagResource_AllFourKinds asserts every resource AWS lists as
// taggable can actually be tagged. The four kinds resolve through four different
// stores, so one of them silently answering TargetNotFoundException is invisible
// until a consumer tags that kind — and a tag that will not attach cannot gate
// anything.
func TestOrganizations_TagResource_AllFourKinds(t *testing.T) {
	f := newOrgTagsFixture(t)
	ouID := f.putOU(t, "11112222")
	policyID := f.putPolicy(t, "p-tagtest1")

	cases := []struct {
		kind string
		id   string
	}{
		{"account", orgTestAccount},
		{"root", f.root},
		{"organizational unit", ouID},
		{"policy", policyID},
	}
	for _, c := range cases {
		t.Run(c.kind, func(t *testing.T) {
			status, code := f.tagResource(t, c.id, []map[string]any{orgTag("Owner", "platform")})
			if status != http.StatusOK {
				t.Fatalf("TagResource on a %s: expected 200, got %d (%s)", c.kind, status, code)
			}
			tags, next := f.listTags(t, c.id, "")
			if tags["Owner"] != "platform" {
				t.Errorf("expected Owner=platform on the %s, got %v", c.kind, tags)
			}
			if next != "" {
				t.Errorf("expected no NextToken for a single tag, got %q", next)
			}
		})
	}
}

// TestOrganizations_TagResource_OverwritesRatherThanDuplicates pins that
// re-tagging an existing key replaces its value. Two entries for one key would
// make an aws:ResourceTag condition depend on which one is read first, so a
// tag-gated policy would allow or deny the same request depending on nothing the
// caller can see.
func TestOrganizations_TagResource_OverwritesRatherThanDuplicates(t *testing.T) {
	f := newOrgTagsFixture(t)

	if status, code := f.tagResource(t, orgTestAccount, []map[string]any{
		orgTag("Owner", "platform"), orgTag("Env", "dev"),
	}); status != http.StatusOK {
		t.Fatalf("first tag: expected 200, got %d (%s)", status, code)
	}
	if status, code := f.tagResource(t, orgTestAccount, []map[string]any{
		orgTag("Owner", "security"),
	}); status != http.StatusOK {
		t.Fatalf("retag: expected 200, got %d (%s)", status, code)
	}

	tags, _ := f.listTags(t, orgTestAccount, "")
	if len(tags) != 2 {
		t.Errorf("expected the key replaced rather than added, got %v", tags)
	}
	if tags["Owner"] != "security" {
		t.Errorf("expected Owner=security after the retag, got %q", tags["Owner"])
	}
	if tags["Env"] != "dev" {
		t.Errorf("expected the untouched tag preserved, got %v", tags)
	}
}

// TestOrganizations_TagResource_EmptyValueIsLegal covers the one length boundary
// the model states explicitly: TagValue's minimum is 0, so "" is a value, and
// only null is refused. A caller using an empty value to record a key's presence
// would otherwise be refused for a request AWS accepts.
func TestOrganizations_TagResource_EmptyValueIsLegal(t *testing.T) {
	f := newOrgTagsFixture(t)

	if status, code := f.tagResource(t, orgTestAccount, []map[string]any{orgTag("Reviewed", "")}); status != http.StatusOK {
		t.Fatalf("expected an empty value accepted, got %d (%s)", status, code)
	}
	tags, _ := f.listTags(t, orgTestAccount, "")
	value, ok := tags["Reviewed"]
	if !ok || value != "" {
		t.Errorf("expected the empty-valued tag stored, got %v", tags)
	}
}

// TestOrganizations_UntagResource_AbsentKeyIsNotAnError pins UntagResource as
// idempotent: it "removes any tags with the specified keys", so a key that is not
// there is nothing to remove. Refusing would break a cleanup path that runs
// twice, which is exactly what a re-run of a governance script does.
func TestOrganizations_UntagResource_AbsentKeyIsNotAnError(t *testing.T) {
	f := newOrgTagsFixture(t)

	if status, code := f.untagResource(t, orgTestAccount, []string{"NeverSet"}); status != http.StatusOK {
		t.Fatalf("untag of an absent key on an untagged resource: expected 200, got %d (%s)", status, code)
	}

	if status, _ := f.tagResource(t, orgTestAccount, []map[string]any{
		orgTag("Owner", "platform"), orgTag("Env", "dev"),
	}); status != http.StatusOK {
		t.Fatalf("tag: expected 200, got %d", status)
	}
	if status, code := f.untagResource(t, orgTestAccount, []string{"Owner", "NeverSet"}); status != http.StatusOK {
		t.Fatalf("mixed untag: expected 200, got %d (%s)", status, code)
	}
	tags, _ := f.listTags(t, orgTestAccount, "")
	if _, ok := tags["Owner"]; ok {
		t.Errorf("expected Owner removed, got %v", tags)
	}
	if tags["Env"] != "dev" {
		t.Errorf("expected Env untouched by the untag, got %v", tags)
	}

	// Repeating the same untag is still a success.
	if status, code := f.untagResource(t, orgTestAccount, []string{"Owner"}); status != http.StatusOK {
		t.Fatalf("repeated untag: expected 200, got %d (%s)", status, code)
	}
}

// TestOrganizations_ListTagsForResource_Paginates asserts a heavily tagged
// resource pages. The model gives ListTagsForResource a NextToken and no
// MaxResults, and the per-listing ceiling is below the 50-tag limit, so a
// resource can carry more tags than one page holds. A consumer that reads only
// the first page would silently miss the tag its condition is about.
func TestOrganizations_ListTagsForResource_Paginates(t *testing.T) {
	f := newOrgTagsFixture(t)

	const total = emulator.OrgMaxTagsPerResourceForTest
	tags := make([]map[string]any, 0, total)
	for i := range total {
		tags = append(tags, orgTag(fmt.Sprintf("Key%02d", i), fmt.Sprintf("v%02d", i)))
	}
	if status, code := f.tagResource(t, orgTestAccount, tags); status != http.StatusOK {
		t.Fatalf("tag %d keys: expected 200, got %d (%s)", total, status, code)
	}

	seen := make(map[string]string, total)
	token := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
		page, next := f.listTags(t, orgTestAccount, token)
		if len(page) > emulator.OrgMaxResultsForTest {
			t.Fatalf("page %d holds %d tags, above the %d ceiling", pages, len(page), emulator.OrgMaxResultsForTest)
		}
		for k, v := range page {
			if _, dup := seen[k]; dup {
				t.Errorf("tag %q appeared on two pages", k)
			}
			seen[k] = v
		}
		if next == "" {
			if pages == 0 {
				t.Error("expected more than one page for a resource at the tag limit")
			}
			break
		}
		token = next
	}
	if len(seen) != total {
		t.Errorf("expected %d tags across every page, got %d", total, len(seen))
	}
	for i := range total {
		key := fmt.Sprintf("Key%02d", i)
		if seen[key] != fmt.Sprintf("v%02d", i) {
			t.Errorf("tag %q: expected v%02d, got %q", key, i, seen[key])
		}
	}
}

// TestOrganizations_ListTagsForResource_StaleTokenIsRefused asserts a token
// naming no known tag key is refused rather than restarting the listing. A silent
// restart never terminates, and the caller sees the first page forever with no
// error to explain it.
func TestOrganizations_ListTagsForResource_StaleTokenIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	status, code := orgTagCall(t, f.ts, "ListTagsForResource", map[string]any{
		"ResourceId": orgTestAccount,
		"NextToken":  "bm90LWEtcmVhbC1rZXk=",
	})
	if status != http.StatusBadRequest || !strings.Contains(code, "INVALID_NEXT_TOKEN") {
		t.Errorf("expected InvalidInputException/INVALID_NEXT_TOKEN, got %d (%s)", status, code)
	}
}

// TestOrganizations_TagsAreNotSharedBetweenResources pins that two resources have
// independent tag sets. A shared one would let a tag-gated policy written about a
// production OU be satisfied by a tag on a sandbox account.
func TestOrganizations_TagsAreNotSharedBetweenResources(t *testing.T) {
	f := newOrgTagsFixture(t)
	ouA := f.putOU(t, "aaaa1111")
	ouB := f.putOU(t, "bbbb2222")

	if status, _ := f.tagResource(t, ouA, []map[string]any{orgTag("Owner", "platform")}); status != http.StatusOK {
		t.Fatalf("tag ouA: got %d", status)
	}
	if status, _ := f.tagResource(t, ouB, []map[string]any{orgTag("Owner", "security")}); status != http.StatusOK {
		t.Fatalf("tag ouB: got %d", status)
	}
	if status, _ := f.tagResource(t, orgTestAccount, []map[string]any{orgTag("Tier", "management")}); status != http.StatusOK {
		t.Fatalf("tag account: got %d", status)
	}

	if got, _ := f.listTags(t, ouA, ""); len(got) != 1 || got["Owner"] != "platform" {
		t.Errorf("ouA: expected only Owner=platform, got %v", got)
	}
	if got, _ := f.listTags(t, ouB, ""); len(got) != 1 || got["Owner"] != "security" {
		t.Errorf("ouB: expected only Owner=security, got %v", got)
	}
	if got, _ := f.listTags(t, orgTestAccount, ""); len(got) != 1 || got["Tier"] != "management" {
		t.Errorf("account: expected only Tier=management, got %v", got)
	}

	// Untagging one resource leaves the other alone.
	if status, _ := f.untagResource(t, ouA, []string{"Owner"}); status != http.StatusOK {
		t.Fatalf("untag ouA: got %d", status)
	}
	if got, _ := f.listTags(t, ouB, ""); got["Owner"] != "security" {
		t.Errorf("expected ouB's tag untouched by ouA's untag, got %v", got)
	}
}

// TestOrganizations_TagsSurviveReplacement asserts moving an account between
// parents does not disturb its tags. Tags are the input to an ABAC decision, so
// an account silently losing them on a MoveAccount would turn a tag-gated Allow
// into a denial the caller cannot trace to the move.
func TestOrganizations_TagsSurviveReplacement(t *testing.T) {
	f := newOrgTagsFixture(t)
	ouID := f.putOU(t, "cccc3333")

	if status, _ := f.tagResource(t, orgTestAccount, []map[string]any{orgTag("Owner", "platform")}); status != http.StatusOK {
		t.Fatalf("tag account: got %d", status)
	}
	if err := f.plugin.PlaceChildForTest(t.Context(), ouID, orgTestAccount); err != nil {
		t.Fatalf("move account into the OU: %v", err)
	}

	parent, err := f.plugin.LoadParentForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if parent != ouID {
		t.Fatalf("expected the account under %s, got %s", ouID, parent)
	}
	if got, _ := f.listTags(t, orgTestAccount, ""); got["Owner"] != "platform" {
		t.Errorf("expected the tag to survive the re-placement, got %v", got)
	}
}

// --- refusals -------------------------------------------------------------

// TestOrganizations_Tagging_UnknownResourceIsTargetNotFound asserts a well-formed
// ID naming nothing is TargetNotFoundException on all three operations. A silent
// success would store a tag no later call can read, and a caller who mistyped a
// resource ID would find out only when the policy it wrote gated nothing.
func TestOrganizations_Tagging_UnknownResourceIsTargetNotFound(t *testing.T) {
	f := newOrgTagsFixture(t)

	// Each of these is well-formed for its kind and absent from the organization.
	absent := []string{
		"999999999999",
		"r-9999",
		"ou-" + f.root[2:] + "-99999999",
		"p-99999999",
	}
	for _, id := range absent {
		t.Run(id, func(t *testing.T) {
			status, code := f.tagResource(t, id, []map[string]any{orgTag("Owner", "platform")})
			if status != http.StatusBadRequest || !strings.Contains(code, "TargetNotFoundException") {
				t.Errorf("TagResource: expected TargetNotFoundException, got %d (%s)", status, code)
			}
			status, code = f.untagResource(t, id, []string{"Owner"})
			if status != http.StatusBadRequest || !strings.Contains(code, "TargetNotFoundException") {
				t.Errorf("UntagResource: expected TargetNotFoundException, got %d (%s)", status, code)
			}
			status, code = orgTagCall(t, f.ts, "ListTagsForResource", map[string]any{"ResourceId": id})
			if status != http.StatusBadRequest || !strings.Contains(code, "TargetNotFoundException") {
				t.Errorf("ListTagsForResource: expected TargetNotFoundException, got %d (%s)", status, code)
			}
		})
	}
}

// TestOrganizations_Tagging_UnmodelledResourceKindsAreAbsentNotMalformed covers
// the two ID forms the model's TaggableResourceId pattern admits for resources
// substrate does not model — rp- (resource policy) and rt- (responsibility
// transfer). They are well-formed, so INVALID_PATTERN would be the wrong answer;
// they name nothing, so TargetNotFoundException is the same answer AWS gives for
// an organization that has no such resource.
func TestOrganizations_Tagging_UnmodelledResourceKindsAreAbsentNotMalformed(t *testing.T) {
	f := newOrgTagsFixture(t)

	for _, id := range []string{"rp-abcd1234", "rt-abcd1234"} {
		status, code := f.tagResource(t, id, []map[string]any{orgTag("Owner", "platform")})
		if status != http.StatusBadRequest || !strings.Contains(code, "TargetNotFoundException") {
			t.Errorf("%s: expected TargetNotFoundException, got %d (%s)", id, status, code)
		}
		// A malformed one of the same kind is still a pattern error.
		status, code = f.tagResource(t, id[:3]+"x", []map[string]any{orgTag("Owner", "platform")})
		if status != http.StatusBadRequest || !strings.Contains(code, "INVALID_PATTERN") {
			t.Errorf("%sx: expected INVALID_PATTERN, got %d (%s)", id[:3], status, code)
		}
	}
}

// TestOrganizations_Tagging_MalformedResourceIDIsInvalidInput separates a typo
// from a deletion. The model's TaggableResourceId pattern admits six ID forms;
// something that matches none of them is INVALID_PATTERN, which tells the caller
// to fix the string, while TargetNotFoundException would send it looking for a
// resource that never existed.
func TestOrganizations_Tagging_MalformedResourceIDIsInvalidInput(t *testing.T) {
	f := newOrgTagsFixture(t)

	cases := map[string]string{
		"not an ID at all":     "my-account",
		"account too short":    "12345",
		"root suffix too long": "r-" + strings.Repeat("a", 33),
		"OU with no child":     "ou-abcd",
		"OU child too short":   "ou-abcd-1234",
		"policy too short":     "p-1234",
		"uppercase root":       "r-ABCD",
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			status, code := f.tagResource(t, id, []map[string]any{orgTag("Owner", "platform")})
			if status != http.StatusBadRequest || !strings.Contains(code, "INVALID_PATTERN") {
				t.Errorf("expected InvalidInputException/INVALID_PATTERN, got %d (%s)", status, code)
			}
		})
	}
}

// TestOrganizations_Tagging_MissingRequiredMemberIsRefused covers the required
// members. A zero-valued ResourceId would otherwise be treated as a request about
// the empty resource, which is a wrong answer rather than an error.
func TestOrganizations_Tagging_MissingRequiredMemberIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	cases := []struct {
		name string
		op   string
		body map[string]any
	}{
		{"TagResource with no ResourceId", "TagResource", map[string]any{"Tags": []map[string]any{orgTag("Owner", "x")}}},
		{"TagResource with no Tags", "TagResource", map[string]any{"ResourceId": orgTestAccount}},
		{"UntagResource with no ResourceId", "UntagResource", map[string]any{"TagKeys": []string{"Owner"}}},
		{"UntagResource with no TagKeys", "UntagResource", map[string]any{"ResourceId": orgTestAccount}},
		{"ListTagsForResource with no ResourceId", "ListTagsForResource", map[string]any{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := orgTagCall(t, f.ts, c.op, c.body)
			if status != http.StatusBadRequest || !strings.Contains(code, "INPUT_REQUIRED") {
				t.Errorf("expected InvalidInputException/INPUT_REQUIRED, got %d (%s)", status, code)
			}
		})
	}
}

// TestOrganizations_TagResource_DuplicateKeyIsRefused asserts one request naming
// a key twice is refused rather than resolved. Picking a winner would make the
// stored value depend on list order, so the same request could leave two
// different tag sets behind.
func TestOrganizations_TagResource_DuplicateKeyIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	status, code := f.tagResource(t, orgTestAccount, []map[string]any{
		orgTag("Owner", "platform"), orgTag("Owner", "security"),
	})
	if status != http.StatusBadRequest || !strings.Contains(code, "DUPLICATE_TAG_KEY") {
		t.Errorf("expected InvalidInputException/DUPLICATE_TAG_KEY, got %d (%s)", status, code)
	}
	// The refusal is total: nothing from the batch landed.
	if got, _ := f.listTags(t, orgTestAccount, ""); len(got) != 0 {
		t.Errorf("expected the whole request refused, got %v", got)
	}
}

// TestOrganizations_TagResource_NullValueIsRefused pins the distinction the model
// draws: "The value can be an empty string, but you can't set it to null." A null
// read as "" would store a tag for a request AWS refuses, so the caller's error
// path would never be exercised.
func TestOrganizations_TagResource_NullValueIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	cases := []struct {
		name string
		tag  map[string]any
	}{
		{"null value", map[string]any{"Key": "Owner", "Value": nil}},
		{"absent value", map[string]any{"Key": "Owner"}},
		{"null key", map[string]any{"Key": nil, "Value": "platform"}},
		{"absent key", map[string]any{"Value": "platform"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := f.tagResource(t, orgTestAccount, []map[string]any{c.tag})
			if status != http.StatusBadRequest || !strings.Contains(code, "INPUT_REQUIRED") {
				t.Errorf("expected InvalidInputException/INPUT_REQUIRED, got %d (%s)", status, code)
			}
		})
	}
}

// TestOrganizations_TagResource_KeyAndValueConstraints covers the TagKey and
// TagValue shapes. Each bound is a boundary an off-by-one hides: a key one
// character over the limit is accepted by substrate and refused by AWS, so the
// consumer's validation never gets tested.
func TestOrganizations_TagResource_KeyAndValueConstraints(t *testing.T) {
	f := newOrgTagsFixture(t)

	cases := []struct {
		name   string
		key    string
		value  string
		reason string
	}{
		{"empty key", "", "x", "MIN_LENGTH_EXCEEDED"},
		{"key at the limit", strings.Repeat("k", 128), "x", ""},
		{"key over the limit", strings.Repeat("k", 129), "x", "MAX_LENGTH_EXCEEDED"},
		{"value at the limit", "AtLimit", strings.Repeat("v", 256), ""},
		{"value over the limit", "OverLimit", strings.Repeat("v", 257), "MAX_LENGTH_EXCEEDED"},
		{"key with a disallowed character", "Own%er", "x", "INVALID_PATTERN"},
		{"value with a disallowed character", "Owner", "plat*form", "INVALID_PATTERN"},
		{"key in the allowed punctuation class", "cost:center/v1_2=a+b-c@d.e", "x", ""},
		{"non-ASCII letters are allowed", "Propriétaire", "équipe", ""},
		{"system tag key", "aws:cloudformation:stack-name", "x", "INVALID_SYSTEM_TAGS_PARAMETER"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := f.tagResource(t, orgTestAccount, []map[string]any{orgTag(c.key, c.value)})
			if c.reason == "" {
				if status != http.StatusOK {
					t.Errorf("expected the tag accepted, got %d (%s)", status, code)
				}
				return
			}
			if status != http.StatusBadRequest || !strings.Contains(code, c.reason) {
				t.Errorf("expected InvalidInputException/%s, got %d (%s)", c.reason, status, code)
			}
		})
	}
}

// TestOrganizations_UntagResource_ValidatesKeys asserts the key shape is enforced
// on the removal path too. A key that could never have been stored names nothing
// to remove, and answering 200 would tell a caller its cleanup succeeded.
func TestOrganizations_UntagResource_ValidatesKeys(t *testing.T) {
	f := newOrgTagsFixture(t)

	for _, key := range []string{"", strings.Repeat("k", 129), "bad%key"} {
		status, code := f.untagResource(t, orgTestAccount, []string{"Owner", key})
		if status != http.StatusBadRequest || !strings.Contains(code, "InvalidInputException") {
			t.Errorf("key %q: expected InvalidInputException, got %d (%s)", key, status, code)
		}
	}
}

// TestOrganizations_TagResource_MaxTagLimit asserts the 51st tag on one resource
// is refused with the documented reason. The limit is on the resulting set, not
// on the request, so a resource already at the limit can still have an existing
// key rewritten — refusing that would break a governance script that only ever
// rewrites the tags it owns.
func TestOrganizations_TagResource_MaxTagLimit(t *testing.T) {
	f := newOrgTagsFixture(t)

	const limit = emulator.OrgMaxTagsPerResourceForTest
	tags := make([]map[string]any, 0, limit)
	for i := range limit {
		tags = append(tags, orgTag(fmt.Sprintf("Key%02d", i), "v"))
	}
	if status, code := f.tagResource(t, orgTestAccount, tags); status != http.StatusOK {
		t.Fatalf("tagging exactly %d keys: expected 200, got %d (%s)", limit, status, code)
	}

	// One more distinct key crosses the limit.
	status, code := f.tagResource(t, orgTestAccount, []map[string]any{orgTag("OneTooMany", "v")})
	if status != http.StatusBadRequest || !strings.Contains(code, "MAX_TAG_LIMIT_EXCEEDED") {
		t.Errorf("expected ConstraintViolationException/MAX_TAG_LIMIT_EXCEEDED, got %d (%s)", status, code)
	}
	if got := f.listAllTags(t, orgTestAccount); len(got) != limit {
		t.Errorf("expected the refused tag not to land, got %d tags", len(got))
	}

	// Rewriting a key that is already there does not grow the set, so it is allowed.
	if status, code := f.tagResource(t, orgTestAccount, []map[string]any{orgTag("Key00", "rewritten")}); status != http.StatusOK {
		t.Errorf("expected a rewrite at the limit to be allowed, got %d (%s)", status, code)
	}
	got := f.listAllTags(t, orgTestAccount)
	if got["Key00"] != "rewritten" {
		t.Errorf("expected the rewrite applied, got %q", got["Key00"])
	}

	// A single request whose own tags would cross the limit is refused whole.
	fresh := newOrgTagsFixture(t)
	tooMany := make([]map[string]any, 0, limit+1)
	for i := range limit + 1 {
		tooMany = append(tooMany, orgTag(fmt.Sprintf("Key%02d", i), "v"))
	}
	status, code = fresh.tagResource(t, orgTestAccount, tooMany)
	if status != http.StatusBadRequest || !strings.Contains(code, "MAX_TAG_LIMIT_EXCEEDED") {
		t.Errorf("expected one oversized request refused, got %d (%s)", status, code)
	}
	if got, _ := fresh.listTags(t, orgTestAccount, ""); len(got) != 0 {
		t.Errorf("expected none of an oversized request to land, got %d tags", len(got))
	}
}

// TestOrganizations_CreateOperationsShareTagValidation asserts the three
// operations that accept inline Tags — CreateOrganizationalUnit, CreatePolicy and
// CreateAccount — refuse exactly what TagResource refuses. A create that is more
// permissive than TagResource lets a caller plant a tag it could never set
// afterwards, and an "aws:"-prefixed one is the case that matters: it would then
// be readable as aws:ResourceTag by a policy condition, so a tag-gated boundary
// could be crossed by a key AWS reserves for itself and never lets a caller write.
// The refusal has to be synchronous even for CreateAccount, whose success is
// asynchronous — the request is malformed, so there is nothing to vend.
func TestOrganizations_CreateOperationsShareTagValidation(t *testing.T) {
	// Each entry names a create and a body builder taking the tag list, so the same
	// refusal table runs against all three without a per-operation copy of it.
	creates := []struct {
		op   string
		body func(root string, tags []map[string]any) map[string]any
	}{
		{
			op: "CreateOrganizationalUnit",
			body: func(root string, tags []map[string]any) map[string]any {
				return map[string]any{"ParentId": root, "Name": "Rejected", "Tags": tags}
			},
		},
		{
			op: "CreatePolicy",
			body: func(_ string, tags []map[string]any) map[string]any {
				return map[string]any{
					"Name": "rejected", "Description": "d", "Type": emulator.OrgPolicyTypeSCPForTest,
					"Content": orgPolicyDoc, "Tags": tags,
				}
			},
		},
		{
			op: "CreateAccount",
			body: func(_ string, tags []map[string]any) map[string]any {
				return map[string]any{"AccountName": "rejected", "Email": "rejected@example.com", "Tags": tags}
			},
		},
	}
	refusals := []struct {
		name   string
		tags   []map[string]any
		code   string
		reason string
	}{
		{
			"a system tag key", []map[string]any{orgTag("aws:cloudformation:stack-name", "x")},
			"InvalidInputException", "INVALID_SYSTEM_TAGS_PARAMETER",
		},
		{
			"a duplicate tag key", []map[string]any{orgTag("Owner", "a"), orgTag("Owner", "b")},
			"InvalidInputException", "DUPLICATE_TAG_KEY",
		},
		{"an empty tag key", []map[string]any{orgTag("", "a")}, "InvalidInputException", "MIN_LENGTH_EXCEEDED"},
		{
			"a key past the length limit", []map[string]any{orgTag(strings.Repeat("k", 129), "a")},
			"InvalidInputException", "MAX_LENGTH_EXCEEDED",
		},
		{
			"a key outside the allowed pattern", []map[string]any{orgTag("Own%er", "a")},
			"InvalidInputException", "INVALID_PATTERN",
		},
		{
			"a value past the length limit", []map[string]any{orgTag("Owner", strings.Repeat("v", 257))},
			"InvalidInputException", "MAX_LENGTH_EXCEEDED",
		},
	}

	for _, c := range creates {
		t.Run(c.op, func(t *testing.T) {
			for _, r := range refusals {
				t.Run(r.name, func(t *testing.T) {
					// A fresh fixture per case, so a refusal that wrongly created something
					// cannot be mistaken for a name collision with an earlier subtest.
					f := newOrgTagsFixture(t)
					status, code := orgTagCall(t, f.ts, c.op, c.body(f.root, r.tags))
					if status != http.StatusBadRequest ||
						!strings.Contains(code, r.code) || !strings.Contains(code, r.reason) {
						t.Fatalf("expected %s/%s, got %d (%s)", r.code, r.reason, status, code)
					}
				})
			}

			t.Run("more tags than the quota allows", func(t *testing.T) {
				f := newOrgTagsFixture(t)
				tags := make([]map[string]any, 0, emulator.OrgMaxTagsPerResourceForTest+1)
				for i := range emulator.OrgMaxTagsPerResourceForTest + 1 {
					tags = append(tags, orgTag(fmt.Sprintf("Key%02d", i), "v"))
				}
				status, code := orgTagCall(t, f.ts, c.op, c.body(f.root, tags))
				if status != http.StatusBadRequest ||
					!strings.Contains(code, "ConstraintViolationException") ||
					!strings.Contains(code, "MAX_TAG_LIMIT_EXCEEDED") {
					t.Errorf("expected ConstraintViolationException/MAX_TAG_LIMIT_EXCEEDED, got %d (%s)", status, code)
				}
			})

			// The same tag list TagResource accepts has to be accepted here, or the
			// shared validation would be refusing more than it should.
			t.Run("a legal tag is accepted", func(t *testing.T) {
				f := newOrgTagsFixture(t)
				status, code := orgTagCall(t, f.ts, c.op, c.body(f.root, []map[string]any{orgTag("Owner", "platform")}))
				if status != http.StatusOK {
					t.Errorf("expected 200, got %d (%s)", status, code)
				}
			})
		})
	}
}

// TestOrganizations_Tagging_FullAWSAccessIsImmutable asserts the AWS-managed SCP
// cannot be tagged. Its ARN is owned by the "aws" account, not by the
// organization, so a tag stored against it would be visible in one organization
// and nowhere else — a state no sequence of AWS calls can produce, which means
// nothing downstream is prepared for it. Reading its tags is still fine and
// answers empty.
func TestOrganizations_Tagging_FullAWSAccessIsImmutable(t *testing.T) {
	f := newOrgTagsFixture(t)
	managed := emulator.OrgFullAWSAccessIDForTest

	status, code := f.tagResource(t, managed, []map[string]any{orgTag("Owner", "platform")})
	if status != http.StatusBadRequest || !strings.Contains(code, "IMMUTABLE_POLICY") {
		t.Errorf("TagResource: expected InvalidInputException/IMMUTABLE_POLICY, got %d (%s)", status, code)
	}
	status, code = f.untagResource(t, managed, []string{"Owner"})
	if status != http.StatusBadRequest || !strings.Contains(code, "IMMUTABLE_POLICY") {
		t.Errorf("UntagResource: expected InvalidInputException/IMMUTABLE_POLICY, got %d (%s)", status, code)
	}
	if got, _ := f.listTags(t, managed, ""); len(got) != 0 {
		t.Errorf("expected the managed policy to list no tags, got %v", got)
	}
}

// TestOrganizations_Tagging_MalformedBodyIsRefused covers the decode guard on
// each operation, which is the branch a hand-written curl reaches first.
func TestOrganizations_Tagging_MalformedBodyIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	for _, op := range []string{"TagResource", "UntagResource", "ListTagsForResource"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.ts.URL+"/", newOrgBadBody())
		if err != nil {
			t.Fatalf("%s: build request: %v", op, err)
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
		req.Host = "organizations.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusBadRequest {
			t.Errorf("%s: expected 400 for an unparseable body, got %d", op, gotStatus)
		}
	}
}

// TestOrganizations_Tagging_UnknownOperationIsRefused pins that the tag cluster
// claims only its three operations. Claiming a fourth would shadow whichever lane
// owns it.
func TestOrganizations_Tagging_UnknownOperationIsRefused(t *testing.T) {
	f := newOrgTagsFixture(t)

	status, code := orgTagCall(t, f.ts, "TagResourceButNotReally", map[string]any{"ResourceId": orgTestAccount})
	if status == http.StatusOK {
		t.Errorf("expected an unimplemented operation to be refused, got 200 (%s)", code)
	}
}

// --- store failures -------------------------------------------------------

// TestOrganizations_Tagging_StoreFailuresAreInternalFailure asserts a store fault
// reaching a tag operation is a 500 an SDK retries rather than a 400 it treats as
// terminal. The tag read is worse than most: answering "no tags" on a failure
// would hand an authorization decision an empty tag set, which fails open on
// every tag-gated policy.
func TestOrganizations_Tagging_StoreFailuresAreInternalFailure(t *testing.T) {
	cases := []struct {
		name  string
		state func(inner emulator.StateManager) emulator.StateManager
	}{
		{
			name: "tag read fails",
			state: func(inner emulator.StateManager) emulator.StateManager {
				return &errOrgState{inner: inner, prefix: "tags:", err: errors.New("store unavailable"), onGet: true}
			},
		},
		{
			name: "tag write fails",
			state: func(inner emulator.StateManager) emulator.StateManager {
				return &errOrgState{inner: inner, prefix: "tags:", err: errors.New("store unavailable"), onPut: true}
			},
		},
		{
			name: "resource resolution fails",
			state: func(inner emulator.StateManager) emulator.StateManager {
				return &errOrgState{inner: inner, prefix: "account:", err: errors.New("store unavailable"), onGet: true}
			},
		},
		{
			name: "the organization itself is unreadable",
			state: func(inner emulator.StateManager) emulator.StateManager {
				return &errOrgState{inner: inner, prefix: "org:", err: errors.New("store unavailable"), onGet: true}
			},
		},
		{
			name: "tags are unreadable",
			state: func(inner emulator.StateManager) emulator.StateManager {
				return &corruptOrgState{StateManager: inner, prefix: "tags:"}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			wrapped := c.state(inner)
			ts := newOrgTagsServerOver(t, wrapped)

			// Warm the organization up and store one tag before arming, so neither
			// auto-creation nor an empty tag set is what the fault lands on: an
			// UntagResource with nothing to remove legitimately performs no write.
			warm := orgsRequest(t, ts, "DescribeOrganization", map[string]any{})
			warm.Body.Close() //nolint:errcheck
			if status, code := orgTagCall(t, ts, "TagResource", map[string]any{
				"ResourceId": orgTestAccount, "Tags": []map[string]any{orgTag("Owner", "x")},
			}); status != http.StatusOK {
				t.Fatalf("seed tag before arming: got %d (%s)", status, code)
			}
			armOrgFaultState(wrapped)

			bodies := map[string]map[string]any{
				"TagResource":         {"ResourceId": orgTestAccount, "Tags": []map[string]any{orgTag("Team", "y")}},
				"UntagResource":       {"ResourceId": orgTestAccount, "TagKeys": []string{"Owner"}},
				"ListTagsForResource": {"ResourceId": orgTestAccount},
			}
			for op, body := range bodies {
				if op == "ListTagsForResource" && c.name == "tag write fails" {
					continue // A listing performs no write, so no write can fail it.
				}
				status, _ := orgTagCall(t, ts, op, body)
				if status != http.StatusInternalServerError {
					t.Errorf("%s: expected 500 for a store fault, got %d", op, status)
				}
			}
		})
	}
}

// newOrgTagsServerOver starts an Organizations server over an arbitrary state
// manager, so a fault-injecting wrapper can sit under the HTTP surface.
func newOrgTagsServerOver(t *testing.T, state emulator.StateManager) *httptest.Server {
	t.Helper()
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
		t.Fatalf("initialize organizations plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// armOrgFaultState turns on whichever fault wrapper state is.
func armOrgFaultState(state emulator.StateManager) {
	switch s := state.(type) {
	case *errOrgState:
		s.armed = true
	case *corruptOrgState:
		s.armed = true
	}
}

// --- the authorization decision -------------------------------------------

// orgAuthzFixture is an Organizations state store plus an AuthController over the
// same store, which is what makes the tag-gated boundary testable: the tag has to
// travel from the Organizations namespace into the condition context.
type orgAuthzFixture struct {
	state  emulator.StateManager
	auth   *emulator.AuthController
	plugin *emulator.OrganizationsPlugin
	root   string
}

// newOrgAuthzFixture builds a user carrying one policy, an organization, and an
// AuthController reading both.
func newOrgAuthzFixture(t *testing.T, user string, doc emulator.PolicyDocument) *orgAuthzFixture {
	t.Helper()
	policyARN := "arn:aws:iam::123456789012:policy/OrgTagGate-" + user
	state := newAuthTestState(t, user, policyARN, doc)
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	p := emulator.NewOrganizationsPluginForTest(state, tc)
	root, err := p.LoadRootForTest(t.Context(), "123456789012")
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	return &orgAuthzFixture{
		state:  state,
		auth:   emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false)),
		plugin: p,
		root:   root.ID,
	}
}

// check runs one Organizations request through the authorization decision.
func (f *orgAuthzFixture) check(t *testing.T, user, operation string, body map[string]any) error {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/" + user)
	return f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service:   "organizations",
		Operation: operation,
		Path:      "/",
		Body:      raw,
	})
}

// orgAuthzDenied reports whether err is the denial an Organizations caller sees.
// Organizations speaks JSON-RPC, so the code carries the "Exception" suffix.
func orgAuthzDenied(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	var awsErr *emulator.AWSError
	if !errors.As(err, &awsErr) {
		t.Fatalf("expected an *AWSError, got %T: %v", err, err)
	}
	if awsErr.Code != "AccessDeniedException" {
		t.Errorf("expected AccessDeniedException, got %q", awsErr.Code)
	}
	if awsErr.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected 403, got %d", awsErr.HTTPStatus)
	}
	return true
}

// TestOrganizations_Authz_RequestTagGatesTagResource is the decisive test for the
// aws:RequestTag half of #578's point 9: a policy that allows tagging only when
// the request carries Owner=platform must allow that request and deny one
// carrying a different owner. Without the organizations arm of addRequestTags the
// condition key is never populated, so the Allow never matches and the *first*
// case fails — a boundary that denies everything looks like it works until
// someone legitimate is blocked by it.
func TestOrganizations_Authz_RequestTagGatesTagResource(t *testing.T) {
	doc := newABACPolicy("Allow", "organizations:TagResource", "*", "aws:RequestTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orgtagger", doc)

	body := func(owner string) map[string]any {
		return map[string]any{
			"ResourceId": f.root,
			"Tags":       []map[string]any{{"Key": "Owner", "Value": owner}},
		}
	}

	if err := f.check(t, "orgtagger", "TagResource", body("platform")); err != nil {
		t.Errorf("expected the matching request tag to be allowed: %v", err)
	}
	if err := f.check(t, "orgtagger", "TagResource", body("security")); !orgAuthzDenied(t, err) {
		t.Error("expected a request carrying the wrong Owner tag to be denied")
	}
	// No tag at all leaves the condition key absent, which also fails to match.
	if err := f.check(t, "orgtagger", "TagResource", map[string]any{"ResourceId": f.root}); !orgAuthzDenied(t, err) {
		t.Error("expected a request carrying no Owner tag to be denied")
	}
}

// TestOrganizations_Authz_ResourceTagGatesTheResourceNamed is the other half: a
// policy allowing an operation only when the resource already carries
// Owner=platform. This is the test that fails without the organizations arm of
// addResourceTags, and the one that proves the tags a TagResource call stored are
// the tags the next decision reads.
func TestOrganizations_Authz_ResourceTagGatesTheResourceNamed(t *testing.T) {
	doc := newABACPolicy("Allow", "organizations:UntagResource", "*", "aws:ResourceTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orguntagger", doc)
	ctx := t.Context()

	mine := "ou-" + f.root[2:] + "-11112222"
	theirs := "ou-" + f.root[2:] + "-33334444"
	if err := f.plugin.SaveTagsForTest(ctx, mine, []emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("tag mine: %v", err)
	}
	if err := f.plugin.SaveTagsForTest(ctx, theirs, []emulator.OrgTag{{Key: "Owner", Value: "security"}}); err != nil {
		t.Fatalf("tag theirs: %v", err)
	}

	body := func(id string) map[string]any {
		return map[string]any{"ResourceId": id, "TagKeys": []string{"Owner"}}
	}
	if err := f.check(t, "orguntagger", "UntagResource", body(mine)); err != nil {
		t.Errorf("expected the matching resource tag to be allowed: %v", err)
	}
	if err := f.check(t, "orguntagger", "UntagResource", body(theirs)); !orgAuthzDenied(t, err) {
		t.Error("expected a resource tagged with a different Owner to be denied")
	}
	// An untagged resource has no Owner at all, which cannot satisfy the condition.
	untagged := "ou-" + f.root[2:] + "-55556666"
	if err := f.check(t, "orguntagger", "UntagResource", body(untagged)); !orgAuthzDenied(t, err) {
		t.Error("expected an untagged resource to be denied")
	}
}

// TestOrganizations_Authz_ResourceTagsComeFromTheNamedResourceOnly asserts the
// decision reads the tags of one resource, not of every ID the body mentions.
// AttachPolicy names both a policy and a target; merging both tag sets would let
// a tag on the policy satisfy a condition written about the target, which is a
// false allow — the one direction a privilege boundary must never fail in.
func TestOrganizations_Authz_ResourceTagsComeFromTheNamedResourceOnly(t *testing.T) {
	doc := newABACPolicy("Allow", "organizations:AttachPolicy", "*", "aws:ResourceTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orgattacher", doc)
	ctx := t.Context()

	policyID := "p-authztest"
	targetID := "ou-" + f.root[2:] + "-77778888"
	// The policy carries the tag the condition wants; the target does not.
	if err := f.plugin.SaveTagsForTest(ctx, policyID, []emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("tag policy: %v", err)
	}
	if err := f.plugin.SaveTagsForTest(ctx, targetID, []emulator.OrgTag{{Key: "Owner", Value: "security"}}); err != nil {
		t.Fatalf("tag target: %v", err)
	}

	// PolicyId is the subject of AttachPolicy, so its tags are the ones read.
	if err := f.check(t, "orgattacher", "AttachPolicy", map[string]any{
		"PolicyId": policyID, "TargetId": targetID,
	}); err != nil {
		t.Errorf("expected the named policy's tag to decide the request: %v", err)
	}
	// Reversing which resource carries the tag must reverse the decision; if both
	// were merged, this would still be allowed.
	if err := f.plugin.SaveTagsForTest(ctx, policyID, []emulator.OrgTag{{Key: "Owner", Value: "security"}}); err != nil {
		t.Fatalf("retag policy: %v", err)
	}
	if err := f.plugin.SaveTagsForTest(ctx, targetID, []emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("retag target: %v", err)
	}
	if err := f.check(t, "orgattacher", "AttachPolicy", map[string]any{
		"PolicyId": policyID, "TargetId": targetID,
	}); !orgAuthzDenied(t, err) {
		t.Error("expected a tag on the target not to satisfy a condition about the policy")
	}
}

// TestOrganizations_Authz_ResourceScopedStatementNarrows covers buildResourceARN.
// A policy whose Resource element names one OU must not admit an operation
// against another: falling through to "*" would make every resource-scoped
// Organizations statement match everything, which is a silent grant rather than a
// visible failure.
func TestOrganizations_Authz_ResourceScopedStatementNarrows(t *testing.T) {
	f := newOrgAuthzFixture(t, "orgscoped", emulator.PolicyDocument{})
	ctx := t.Context()

	mine := "ou-" + f.root[2:] + "-aaaa1111"
	theirs := "ou-" + f.root[2:] + "-bbbb2222"
	mineArn := "arn:aws:organizations::123456789012:ou/o-scoped/" + mine
	theirsArn := "arn:aws:organizations::123456789012:ou/o-scoped/" + theirs
	for id, arn := range map[string]string{mine: mineArn, theirs: theirsArn} {
		if err := f.plugin.SaveOUForTest(ctx, "123456789012", emulator.OrgOrganizationalUnit{
			ID: id, Arn: arn, Name: id,
		}); err != nil {
			t.Fatalf("save OU %s: %v", id, err)
		}
	}

	// Rewrite the user's policy so its Resource names only one of the two OUs.
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"organizations:TagResource"},
			Resource: emulator.StringOrSlice{mineArn},
		}},
	}
	pol := emulator.IAMPolicy{
		PolicyName:       "testpolicy",
		PolicyID:         "ANPATEST",
		ARN:              "arn:aws:iam::123456789012:policy/OrgTagGate-orgscoped",
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         doc,
	}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", "policy:"+pol.ARN, raw); err != nil { //nolint:contextcheck
		t.Fatalf("store policy: %v", err)
	}

	body := func(id string) map[string]any {
		return map[string]any{"ResourceId": id, "Tags": []map[string]any{{"Key": "Owner", "Value": "platform"}}}
	}
	if err := f.check(t, "orgscoped", "TagResource", body(mine)); err != nil {
		t.Errorf("expected the OU the statement names to be allowed: %v", err)
	}
	if err := f.check(t, "orgscoped", "TagResource", body(theirs)); !orgAuthzDenied(t, err) {
		t.Error("expected an OU the statement does not name to be denied")
	}
}

// TestOrganizations_Authz_ARNsMatchWhatTheAPIReports pins that the ARN
// authorization builds is the ARN the API itself reports for each kind. A policy
// is written by pasting an ARN out of a Describe response, so an ARN assembled
// differently here would fail to match the resource it names — and the caller
// would have no way to see why.
func TestOrganizations_Authz_ARNsMatchWhatTheAPIReports(t *testing.T) {
	f := newOrgAuthzFixture(t, "orgarns", emulator.PolicyDocument{})
	ctx := t.Context()

	root, err := f.plugin.LoadRootForTest(ctx, "123456789012")
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	account, err := f.plugin.LoadAccountForTest(ctx, "123456789012")
	if err != nil || account == nil {
		t.Fatalf("load management account: %v", err)
	}
	ouID := "ou-" + root.ID[2:] + "-cccc3333"
	ouArn := "arn:aws:organizations::123456789012:ou/o-arns/" + ouID
	if err := f.plugin.SaveOUForTest(ctx, "123456789012", emulator.OrgOrganizationalUnit{
		ID: ouID, Arn: ouArn, Name: "prod",
	}); err != nil {
		t.Fatalf("save OU: %v", err)
	}
	policyID := "p-arntest1"
	policyArn := "arn:aws:organizations::123456789012:policy/o-arns/service_control_policy/" + policyID
	if err := f.plugin.SavePolicyForTest(ctx, "123456789012", emulator.OrgPolicy{
		PolicySummary: emulator.OrgPolicySummary{
			ID: policyID, Arn: policyArn, Name: "deny", Type: emulator.OrgPolicyTypeSCPForTest,
		},
		Content: `{"Version":"2012-10-17"}`,
	}); err != nil {
		t.Fatalf("save policy: %v", err)
	}

	cases := []struct {
		name string
		body map[string]any
		want string
	}{
		{"root", map[string]any{"ResourceId": root.ID}, root.Arn},
		{"account", map[string]any{"ResourceId": "123456789012"}, account.Arn},
		{"organizational unit", map[string]any{"ResourceId": ouID}, ouArn},
		{"policy", map[string]any{"ResourceId": policyID}, policyArn},
		{"AWS-managed policy", map[string]any{"PolicyId": emulator.OrgFullAWSAccessIDForTest},
			emulator.FullAWSAccessPolicyForTest().PolicySummary.Arn},
		// A resource the organization does not contain, and a request naming none at
		// all, both fall back to "*" rather than to the empty string: an empty
		// resource matches every statement, which would widen a scoped policy.
		{"absent OU", map[string]any{"ResourceId": "ou-" + root.ID[2:] + "-99999999"}, "*"},
		{"no resource named", map[string]any{}, "*"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := json.Marshal(c.body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			// The Resource element names exactly the ARN under test, so an Allow that
			// matches proves buildResourceARN produced it.
			doc := emulator.PolicyDocument{
				Version: "2012-10-17",
				Statement: []emulator.PolicyStatement{{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"organizations:TagResource"},
					Resource: emulator.StringOrSlice{c.want},
				}},
			}
			pol := emulator.IAMPolicy{
				PolicyName:       "arncheck",
				PolicyID:         "ANPAARN",
				ARN:              "arn:aws:iam::123456789012:policy/OrgTagGate-orgarns",
				Path:             "/",
				DefaultVersionID: "v1",
				IsAttachable:     true,
				Document:         doc,
			}
			polRaw, err := json.Marshal(pol)
			if err != nil {
				t.Fatalf("marshal policy: %v", err)
			}
			if err := f.state.Put(context.Background(), "iam", "policy:"+pol.ARN, polRaw); err != nil { //nolint:contextcheck
				t.Fatalf("store policy: %v", err)
			}
			reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/orgarns")
			err = f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
				Service: "organizations", Operation: "TagResource", Path: "/", Body: raw,
			})
			if err != nil {
				t.Errorf("expected the request to match a statement naming %s: %v", c.want, err)
			}
		})
	}
}

// TestOrganizations_Authz_UnreadableTagsDenyRatherThanAllow pins the direction
// the tag lookup fails in. An unreadable tag record leaves the condition key
// absent, so an Allow that requires it does not match and the caller is denied.
// The opposite — assuming the tag matched — would let a storage fault grant
// access, which is the one failure mode a privilege boundary cannot have.
func TestOrganizations_Authz_UnreadableTagsDenyRatherThanAllow(t *testing.T) {
	doc := newABACPolicy("Allow", "organizations:UntagResource", "*", "aws:ResourceTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orgfaulty", doc)
	ctx := t.Context()

	ouID := "ou-" + f.root[2:] + "-dddd4444"
	if err := f.plugin.SaveTagsForTest(ctx, ouID, []emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("tag OU: %v", err)
	}
	// Sanity: the tag decides the request while it is readable.
	if err := f.check(t, "orgfaulty", "UntagResource", map[string]any{
		"ResourceId": ouID, "TagKeys": []string{"Owner"},
	}); err != nil {
		t.Fatalf("expected the readable tag to allow the request: %v", err)
	}

	// Now make the tag record unreadable and re-decide over the same state.
	broken := &corruptOrgState{StateManager: f.state, prefix: "tags:", armed: true}
	auth := emulator.NewAuthController(broken, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/orgfaulty")
	raw, err := json.Marshal(map[string]any{"ResourceId": ouID, "TagKeys": []string{"Owner"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	err = auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service: "organizations", Operation: "UntagResource", Path: "/", Body: raw,
	})
	if !orgAuthzDenied(t, err) {
		t.Error("expected an unreadable tag record to deny rather than allow")
	}
}

// TestOrganizations_Authz_UnparseableBodyDeniesRatherThanAllows asserts a body
// authorization cannot decode names no resource and carries no request tags, so
// a tag-gated Allow does not match it. Authorization runs before the handler, so
// it sees the malformed body first: treating an undecodable body as "no
// conditions to check" would let a caller bypass an ABAC gate by sending
// garbage — and the handler's own 400 would never be reached to catch it.
func TestOrganizations_Authz_UnparseableBodyDeniesRatherThanAllows(t *testing.T) {
	doc := newABACPolicy("Allow", "organizations:TagResource", "*", "aws:RequestTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orggarbage", doc)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/orggarbage")
	err := f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service: "organizations", Operation: "TagResource", Path: "/",
		Body: []byte(`{"ResourceId":`),
	})
	if !orgAuthzDenied(t, err) {
		t.Error("expected an unparseable body to be denied by a tag-gated policy")
	}
}

// TestOrganizations_Authz_NonOrganizationsRequestsAreUnaffected asserts the new
// arms are scoped to the organizations service. A condition context leaking
// Organizations tags into another service's decision would change that service's
// answers, and every one of those decisions is already covered elsewhere.
func TestOrganizations_Authz_NonOrganizationsRequestsAreUnaffected(t *testing.T) {
	doc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:ResourceTag/Owner", "platform")
	f := newOrgAuthzFixture(t, "orgunrelated", doc)

	// An Organizations resource carrying the tag the S3 condition wants must not
	// satisfy it.
	if err := f.plugin.SaveTagsForTest(t.Context(), "my-bucket",
		[]emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("tag: %v", err)
	}
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/orgunrelated")
	err := f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service: "s3", Operation: "PutObject", Path: "/my-bucket/key",
	})
	if err == nil {
		t.Fatal("expected an Organizations tag not to satisfy an S3 resource-tag condition")
	}
	// S3 speaks its own XML dialect, so its denial is the bare code; asserting it
	// here also pins that the new arms did not change another protocol's error.
	var awsErr *emulator.AWSError
	if !errors.As(err, &awsErr) {
		t.Fatalf("expected an *AWSError, got %T: %v", err, err)
	}
	if awsErr.Code != "AccessDenied" {
		t.Errorf("expected AccessDenied for an S3 request, got %q", awsErr.Code)
	}
}
