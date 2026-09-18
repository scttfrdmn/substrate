package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// GetResources' eight request parameters: the four substrate discarded, the range it never
// checked, and the cursor it never validated (#1004, #1010).
//
// `API_GetResources` publishes eight request members. Substrate decoded four of them —
// `TagFilters`, `ResourceTypeFilters`, `ResourcesPerPage`, `PaginationToken` — and of those four,
// two were not honored as published either: `ResourcesPerPage` was clamped rather than
// range-checked, so `5000` returned every resource in the account in one page where AWS refuses
// the value outright; and `PaginationToken` discarded both the base64 and the integer error, so a
// token substrate never issued silently meant page one.
//
// `ResourceARNList` was the worst of the four, and not because it was unimplemented. The page
// publishes it as **mutually exclusive** with `ResourceTypeFilters`, with `TagFilters`, and with
// all three pagination members — three refusals, each stated in its own sentence. So substrate
// accepted three request shapes AWS refuses, and in every one of them answered the account-wide
// scan: a caller asking for the tags on five ARNs got a superset at HTTP 200 with nothing in the
// response to say the request served was not the request sent.
//
// **Substrate's reading where the page does not settle a question**, each recorded here and in
// `docs/services.md` rather than left to be inferred from the code:
//
//   - *A previously-tagged resource named in `ResourceARNList` is returned*, with the empty tag
//     set #938 established. The page scopes that form to the no-`TagFilters` case and says nothing
//     about `ResourceARNList`, but the operation's own scoping sentence is *"GetResources does not
//     return untagged resources"* — and a resource that ever held a tag is not untagged, which is
//     exactly why AWS publishes `"Tags": []` for it. Narrowing to named ARNs selects among the
//     eligible resources; it does not redefine which are eligible.
//   - *`ComplianceStatus` is omitted*, for the reason [taggingNoEffectiveTagPolicy] gives.
//   - *`ExcludeCompliantResources` is refused only when it is `true`*. The page's constraint is
//     that the member may be used "only if the `IncludeComplianceDetails` parameter is also set to
//     `true`", and an explicit `false` asks for nothing — AWS's own Sample Request sends the member
//     as `null` alongside a `true` `IncludeComplianceDetails`, so a caller sending the member with
//     a non-meaningful value is a shape AWS itself documents. Refusing `false` would refuse a
//     request that differs from silence in no observable way.
//   - *`PaginationToken` is emitted on every response*, `omitempty` removed. The evidence is the
//     operation's own Sample Response, which carries `"PaginationToken": ""` on a complete result,
//     and the prose *"repeat the query … until you receive a null value"* — a consumer cannot
//     receive a value from a member that is absent. One sample is not a citation, so this is
//     recorded as a reading.
//
// **The published constraints this file does not enforce**, stated so the new checks are not read
// as complete coverage: `ResourceTypeFilters`' per-item 0–256 length and its 100-item array bound,
// and `TagFilters`' 50-key and 20-values-per-key bounds. They belong to neither issue, and adding a
// bound without walking its citations is how an emulator starts refusing what AWS accepts.
//
// `PaginationTokenExpiredException`/400 stays unreachable for two reasons. The expiry it reports is
// wall-clock — fifteen minutes — which no substrate behavior may depend on (CLAUDE.md). And it is
// not the code for the defect above in any case: a token substrate never issued is *malformed*, not
// *expired*, and the distinction is one a consumer acts on, since an expired token means "start
// again from page one" while a malformed one means "your code composed this wrong".

// taggingResourcesPerPageMin, taggingResourcesPerPageMax and taggingResourcesPerPageDefault are the
// published bounds and default page size of GetResources' ResourcesPerPage.
//
// The bounds are stated in the member's own prose — *"You can specify a minimum of 1 and a maximum
// value of 100"* — rather than as Length Constraints, which the member does not carry because it is
// an Integer. The default is not published; 100 is the value substrate already used for an absent
// member and is kept so that this change refuses what AWS refuses without also repaging every
// caller that never sent the member.
const (
	taggingResourcesPerPageMin     = 1
	taggingResourcesPerPageMax     = 100
	taggingResourcesPerPageDefault = 100
)

// taggingTagsPerPageMin and taggingTagsPerPageMax are the published bounds of TagsPerPage.
//
// *"You can set TagsPerPage to a minimum of 100 items up to a maximum of 500 items."* Note the
// minimum is 100, not 1: the member counts tags rather than resources, so a small value could not
// return even one resource of ordinary size. That is also why a tag budget too small for the first
// resource cannot arise in practice — see [taggingTagBudget].
const (
	taggingTagsPerPageMin = 100
	taggingTagsPerPageMax = 500
)

// taggingPaginationTokenMaxLength is PaginationToken's published maximum length.
//
// "Minimum length of 0. Maximum length of 2048." The minimum is 0, so an empty token is the initial
// request rather than a refusal — which is what the member's own description says to send. The
// pattern is `[\s\S]*`, which admits every string including a newline, so there is no pattern
// refusal to make: whether a token is usable is decided by whether it decodes, not by its
// characters.
const taggingPaginationTokenMaxLength = 2048

// The published bounds of ResourceARNList: its array size, and the length of each ARN in it.
//
// "Array Members: Minimum number of 1 item. Maximum number of 100 items. Length Constraints:
// Minimum length of 1. Maximum length of 1011." The array minimum of 1 is why an empty
// `ResourceARNList: []` is refused rather than treated as absent — a caller that sent the member
// asked to filter by ARN, and filtering by none of them is not the account-wide scan.
const (
	taggingResourceARNListMinItems = 1
	taggingResourceARNListMaxItems = 100
	taggingResourceARNMinLength    = 1
	taggingResourceARNMaxLength    = 1011
)

// getResourcesInput is GetResources' request body: all eight published members.
//
// `ResourcesPerPage` and `TagsPerPage` are pointers because absent and zero are different requests
// for both. `Required: No` with a published minimum above zero means `encoding/json` leaving an
// omitted integer at its zero value is indistinguishable from an explicit `0` — and an explicit `0`
// is out of range for both members while an absent one is the default. Substrate's previous
// `if in.ResourcesPerPage <= 0 { in.ResourcesPerPage = 100 }` was one arm doing both jobs: supplying
// the default and swallowing an out-of-range value. Only the first was correct.
//
// The pointer cannot come from `req.Params` the way [iamValidateMaxItems] and [queryMaxRecords] read
// presence, because `parser.go` fills that map from the query string and form body and RGTA is a
// JSON-body / `X-Amz-Target` service. Only the *shape* of #868's fix transfers: named constants
// carrying the citation, a refusal rather than a clamp, and a message naming the range.
//
// The two compliance members are plain `bool`. Neither has a published minimum that an explicit
// `false` would violate, and the reading that makes `false` equivalent to absent is recorded in this
// file's preamble.
type getResourcesInput struct {
	ExcludeCompliantResources bool        `json:"ExcludeCompliantResources"`
	IncludeComplianceDetails  bool        `json:"IncludeComplianceDetails"`
	PaginationToken           string      `json:"PaginationToken"`
	ResourceARNList           []string    `json:"ResourceARNList"`
	ResourcesPerPage          *int        `json:"ResourcesPerPage"`
	ResourceTypeFilters       []string    `json:"ResourceTypeFilters"`
	TagFilters                []tagFilter `json:"TagFilters"`
	TagsPerPage               *int        `json:"TagsPerPage"`
}

// taggingComplianceDetails is the ComplianceDetails response member, replacing the `*struct{}`
// placeholder that could only ever render as absent or as `{}`.
//
// **`ComplianceStatus` is deliberately not a field**, which is a stronger statement than leaving it
// unset: it cannot be reported by accident later. All four published members are `Required: No`, so
// a document without it is still the published shape.
//
// The three key arrays are here because each is *derivable* rather than invented. Every one is
// defined against the effective tag policy by its own description — "keys defined in the effective
// policy", "defined as required in the `report_required_tag_for` block of the effective tag policy",
// "keys on the resource [that] are noncompliant with the effective tag policy" — and substrate
// models no organization with tag policies enabled, so no key can be a member of any of them. Empty
// is the answer, not a placeholder for one.
//
// The operation's own Sample Response emits three of the four, omitting `MissingTagKeys`, where the
// Response Syntax publishes four. Substrate follows the Response Syntax: a sample is one instance
// and the syntax is the shape.
type taggingComplianceDetails struct {
	KeysWithNoncompliantValues []string `json:"KeysWithNoncompliantValues"`
	MissingTagKeys             []string `json:"MissingTagKeys"`
	NoncompliantKeys           []string `json:"NoncompliantKeys"`
}

// taggingNoEffectiveTagPolicy renders the compliance details of a resource that no tag policy
// evaluates.
//
// Each array is `[]string{}` rather than nil so that it marshals as `[]` and not `null`, which is
// #938's rule: an empty collection is an observation a consumer can act on, and `null` is not.
//
// `ComplianceStatus` is omitted, and the Organizations tag-policies guide is why. Reporting `true`
// would be the tempting answer — substrate has no tag policies, so nothing can be noncompliant —
// but the guide states *"Untagged resources or tags that aren't defined in the tag policy aren't
// evaluated for compliance with the tag policy"*, and not evaluated is not compliant. A `true` here
// would claim an evaluation that did not happen, which is #1013's honest-empty rule read the way it
// is meant: substrate does not report a member it does not model.
//
// It follows that `ExcludeCompliantResources` excludes nothing, since no resource is evaluated as
// compliant. That is stated in `docs/services.md` rather than left for a caller to discover from an
// unexpectedly full response.
func taggingNoEffectiveTagPolicy() *taggingComplianceDetails {
	return &taggingComplianceDetails{
		KeysWithNoncompliantValues: []string{},
		MissingTagKeys:             []string{},
		NoncompliantKeys:           []string{},
	}
}

// taggingInvalidParameter reports a request GetResources will not serve.
//
// `InvalidParameterException` at HTTP 400 is the operation's own published code, and its gloss
// enumerates the conditions this file refuses among its six bullets: *"a provided string parameter
// is malformed"* and *"a provided parameter value is out of range"*.
//
// The mutual-exclusion refusals are a one-step reading rather than a transcription. Each of the
// three sentences promises an *"`Invalid Parameter` exception"* — with a space, and capitalised —
// which is not an entry in the operation's Errors section at all. `InvalidParameterException` is the
// only published code that could be meant, and the near-identical spelling is the argument. That
// gap is recorded rather than papered over, because it is the difference between quoting AWS and
// interpreting it.
func taggingInvalidParameter(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// taggingValidateGetResources refuses every GetResources request AWS publishes a refusal for, and
// reports the effective page size and starting offset for one it does not.
//
// **Order: per-member checks first, then the cross-member exclusions.** No ordering avoids every
// two-round-trip case — a request that is both out of range and mutually exclusive has two problems
// whichever is named first — so the criterion is the one the tree already uses at #991's plaintext
// guard and #983's policy document: name what is wrong with a single member before what is wrong
// with the request as a whole, because the first is fixable from the member's own documentation and
// the second requires the caller to decide which of two features it wanted.
//
// Within the per-member group the order follows the Request Syntax, which is alphabetical, so that
// the sequence is derived from the page rather than chosen.
func taggingValidateGetResources(in *getResourcesInput) (perPage, offset int, awsErr *AWSError) {
	if in.ExcludeCompliantResources && !in.IncludeComplianceDetails {
		return 0, 0, taggingInvalidParameter(
			"ExcludeCompliantResources can be used only if IncludeComplianceDetails is also true")
	}

	if len(in.PaginationToken) > taggingPaginationTokenMaxLength {
		return 0, 0, taggingInvalidParameter(fmt.Sprintf(
			"PaginationToken is %d characters, which exceeds the maximum of %d",
			len(in.PaginationToken), taggingPaginationTokenMaxLength))
	}
	offset, ok := decodeOffsetPaginationToken(in.PaginationToken)
	if !ok {
		return 0, 0, taggingInvalidParameter(
			"PaginationToken is not a token this API issued")
	}

	if awsErr := taggingValidateResourceARNList(in.ResourceARNList); awsErr != nil {
		return 0, 0, awsErr
	}

	perPage = taggingResourcesPerPageDefault
	if in.ResourcesPerPage != nil {
		perPage = *in.ResourcesPerPage
		if perPage < taggingResourcesPerPageMin || perPage > taggingResourcesPerPageMax {
			return 0, 0, taggingInvalidParameter(fmt.Sprintf(
				"ResourcesPerPage must be between %d and %d, got %d",
				taggingResourcesPerPageMin, taggingResourcesPerPageMax, perPage))
		}
	}

	if in.TagsPerPage != nil {
		if n := *in.TagsPerPage; n < taggingTagsPerPageMin || n > taggingTagsPerPageMax {
			return 0, 0, taggingInvalidParameter(fmt.Sprintf(
				"TagsPerPage must be between %d and %d, got %d",
				taggingTagsPerPageMin, taggingTagsPerPageMax, n))
		}
	}

	if awsErr := taggingValidateARNListExclusions(in); awsErr != nil {
		return 0, 0, awsErr
	}
	return perPage, offset, nil
}

// taggingValidateResourceARNList enforces ResourceARNList's published array and item bounds.
//
// A nil list is an absent member and is not checked; an empty one is present and violates the array
// minimum of 1, for the reason [taggingResourceARNListMinItems] records. The per-item bounds are
// walked before the array bound is reported so that a caller sending 200 ARNs of which one is empty
// hears about the array size first — the array bound is the one that cannot be satisfied by editing
// a single entry.
func taggingValidateResourceARNList(arns []string) *AWSError {
	if arns == nil {
		return nil
	}
	if len(arns) < taggingResourceARNListMinItems || len(arns) > taggingResourceARNListMaxItems {
		return taggingInvalidParameter(fmt.Sprintf(
			"ResourceARNList must contain between %d and %d items, got %d",
			taggingResourceARNListMinItems, taggingResourceARNListMaxItems, len(arns)))
	}
	for _, arn := range arns {
		if len(arn) < taggingResourceARNMinLength || len(arn) > taggingResourceARNMaxLength {
			return taggingInvalidParameter(fmt.Sprintf(
				"each ResourceARNList entry must be between %d and %d characters, got %d",
				taggingResourceARNMinLength, taggingResourceARNMaxLength, len(arn)))
		}
	}
	return nil
}

// taggingValidateARNListExclusions enforces the three mutual-exclusion sentences ResourceARNList
// publishes.
//
// All three are refusals substrate did not make, and the third is why this and #1004 are one change
// rather than two: it has to distinguish an absent `ResourcesPerPage` from a sent one, which is the
// pointer #1004 introduces. Written from `ResourceARNList`'s side because that is the member all
// three sentences are about — the two written on `ResourceTypeFilters` and `TagFilters` say the same
// thing from the other direction, and both misspell the member as `ResourceArnList`.
//
// The pagination members are reported together, naming the one that was sent, because the sentence
// covers them as a set: *"you can't specify both this parameter and any of the pagination parameters
// (ResourcesPerPage, TagsPerPage, PaginationToken)"*.
func taggingValidateARNListExclusions(in *getResourcesInput) *AWSError {
	if len(in.ResourceARNList) == 0 {
		return nil
	}
	if len(in.ResourceTypeFilters) > 0 {
		return taggingInvalidParameter(
			"ResourceARNList and ResourceTypeFilters cannot be specified in the same request")
	}
	if len(in.TagFilters) > 0 {
		return taggingInvalidParameter(
			"ResourceARNList and TagFilters cannot be specified in the same request")
	}
	var sent []string
	if in.ResourcesPerPage != nil {
		sent = append(sent, "ResourcesPerPage")
	}
	if in.TagsPerPage != nil {
		sent = append(sent, "TagsPerPage")
	}
	if in.PaginationToken != "" {
		sent = append(sent, "PaginationToken")
	}
	if len(sent) > 0 {
		return taggingInvalidParameter(
			"ResourceARNList cannot be specified with the pagination parameters " +
				"(ResourcesPerPage, TagsPerPage, PaginationToken); this request sent " +
				strings.Join(sent, ", "))
	}
	return nil
}

// taggingFilterByARNList narrows a scan to the resources named in ResourceARNList.
//
// An ARN that names nothing simply does not match, which is the published behavior verbatim: *"if a
// resource specified by this parameter doesn't exist, it doesn't generate an error; it simply isn't
// included in the response."* So there is no not-found refusal here, and none is published.
//
// The comparison is exact rather than a prefix or a case fold. `ResourceARNList` takes ARNs and an
// ARN identifies one resource; a filter that matched loosely would return resources the caller did
// not name, which is the defect this whole change is about in a smaller form.
//
// Previously-tagged resources are eligible, per this file's preamble.
func taggingFilterByARNList(all []resourceTagMapping, arns []string) []resourceTagMapping {
	wanted := make(map[string]struct{}, len(arns))
	for _, arn := range arns {
		wanted[arn] = struct{}{}
	}
	kept := all[:0]
	for _, rm := range all {
		if _, ok := wanted[rm.ResourceARN]; ok {
			kept = append(kept, rm)
		}
	}
	return kept
}

// taggingTagBudget reports how many of a page's resources fit within a TagsPerPage budget.
//
// Three published rules, and all three are in the count: *"a resource with no tags is counted as
// having one tag (one key and value pair)"*, so the cost of a resource is `max(1, len(tags))`;
// *"GetResources does not split a resource and its associated tags across pages"*, so a resource is
// taken whole or not at all; and the operation's own worked example — `TagsPerPage` 100 against 22
// resources of 10 tags each yielding pages of 10, 10 and 2 — which pins the comparison as inclusive,
// since ten resources of ten tags is exactly 100.
//
// **At least one resource is always taken**, which departs from a literal reading of *"a
// PaginationToken is returned in place of the affected resource and its tags"*. Taken literally, a
// first resource whose own tags exceed the whole budget would yield an empty page and a token
// pointing at the same resource — a caller looping to a null token would never terminate. The case
// cannot arise: the published minimum `TagsPerPage` is 100 and no service substrate models admits
// more than 50 tags on a resource, so the budget always covers the first resource. Progress is
// guaranteed anyway rather than left to depend on that, because a hang is a worse failure than a
// page one tag over budget, and the divergence is unobservable.
func taggingTagBudget(page []resourceTagMapping, budget int) int {
	used, kept := 0, 0
	for _, rm := range page {
		cost := len(rm.Tags)
		if cost == 0 {
			cost = 1
		}
		if kept > 0 && used+cost > budget {
			break
		}
		used += cost
		kept++
	}
	return kept
}
