package emulator

import (
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// What an AssumeRole session carries beyond its identity: the session tags and source identity the
// request passes, and — when the caller is itself a session — the transitive tags and source
// identity role chaining carries forward. The IAM User Guide's session-tag and source-identity pages
// are the reference for both.

// stsAssumeRoleSession is the resolved Tags, TransitiveTagKeys and SourceIdentity of one AssumeRole.
type stsAssumeRoleSession struct {
	// tags are the session's tags: the caller's transitive tags and the request's own.
	tags map[string]string
	// requestTags are only the request's own, which are what sts:TagSession is asked about.
	requestTags map[string]string
	// transitiveKeys are the keys the new session carries forward in turn.
	transitiveKeys []string
	// sourceIdentity is the request's or, when chained, the caller's.
	sourceIdentity string
}

// stsAssumeRoleSessionParams reads an AssumeRole's Tags, TransitiveTagKeys and SourceIdentity and
// combines them with what the calling session carries.
//
// A chained session inherits the caller's transitive tags, and a request tag that would override
// one is refused: "If you try to pass the Sun = 2 session tag, the operation fails" (the User
// Guide's chaining example); that message is substrate's own. A source identity "cannot be changed
// during the role session", so a different one is refused with AccessDenied, in substrate's own
// wording; the same one, or none, inherits the caller's.
func stsAssumeRoleSessionParams(principal *Principal, params map[string]string) (stsAssumeRoleSession, *AWSError) {
	var out stsAssumeRoleSession
	var tagList []stsTag
	for _, m := range iamMemberStructs(params, "Tags") {
		tagList = append(tagList, stsTag{m["Key"], m["Value"]})
	}
	requestTags, refusal := stsSessionTagMap(tagList, "ValidationError")
	if refusal != nil {
		return out, refusal
	}
	out.requestTags = requestTags
	requestTransitive := iamMemberList(params, "TransitiveTagKeys")
	// A transitive key must name a tag this request passes; one that names none is refused rather
	// than dropped. The message is substrate's own.
	for _, k := range requestTransitive {
		if _, ok := stsTagKeyFold(requestTags, k); !ok {
			return out, stsFederationError("InvalidParameterValue",
				fmt.Sprintf("TransitiveTagKeys names %s, which is not one of the session tags in Tags.", k),
				http.StatusBadRequest)
		}
	}

	inherited := map[string]string{}
	var inheritedKeys []string
	if principal != nil {
		for _, k := range principal.TransitiveTagKeys {
			if actual, ok := stsTagKeyFold(principal.SessionTags, k); ok {
				inherited[actual] = principal.SessionTags[actual]
				inheritedKeys = append(inheritedKeys, actual)
			}
		}
	}
	for k := range requestTags {
		for ik := range inherited {
			if strings.EqualFold(k, ik) {
				return out, stsFederationError("InvalidParameterValue",
					fmt.Sprintf("Session tag %s cannot override the transitive tag %s inherited from the calling session.", k, ik),
					http.StatusBadRequest)
			}
		}
	}
	if len(inherited)+len(requestTags) > 0 {
		out.tags = make(map[string]string, len(inherited)+len(requestTags))
		for k, v := range inherited {
			out.tags[k] = v
		}
		for k, v := range requestTags {
			out.tags[k] = v
		}
		// The 50-tag limit applies to the session's tags, inherited and passed together.
		if len(out.tags) > 50 {
			return out, stsFederationError("ValidationError",
				"Session tags must number at most 50, with keys of 1 to 128 characters and values of at most 256.",
				http.StatusBadRequest)
		}
		for _, k := range append(inheritedKeys, requestTransitive...) {
			if actual, ok := stsTagKeyFold(out.tags, k); ok && !slices.Contains(out.transitiveKeys, actual) {
				out.transitiveKeys = append(out.transitiveKeys, actual)
			}
		}
		sort.Strings(out.transitiveKeys)
	}

	si := params["SourceIdentity"]
	if si != "" && (!stsSourceIdentityPattern.MatchString(si) || strings.HasPrefix(strings.ToLower(si), "aws:")) {
		return out, stsFederationError("ValidationError",
			`Source Identity must match [a-zA-Z_0-9+=,.@-]{2,256} and not begin with "aws:"`, http.StatusBadRequest)
	}
	if principal != nil && principal.SourceIdentity != "" {
		if si != "" && si != principal.SourceIdentity {
			return out, stsFederationError("AccessDenied",
				"The source identity of a role session cannot be changed: the calling session's is "+principal.SourceIdentity+".",
				http.StatusForbidden)
		}
		si = principal.SourceIdentity
	}
	out.sourceIdentity = si
	return out, nil
}

// trustContext adds the session's tags and source identity to an AssumeRole's trust-policy
// context: aws:RequestTag/<key>, aws:TagKeys, sts:TransitiveTagKeys and sts:SourceIdentity.
func (s stsAssumeRoleSession) trustContext(condCtx map[string]string) map[string][]string {
	multi := map[string][]string{}
	if len(s.requestTags) > 0 {
		keys := make([]string, 0, len(s.requestTags))
		for k, v := range s.requestTags {
			condCtx["aws:RequestTag/"+k] = v
			keys = append(keys, k)
		}
		sort.Strings(keys)
		multi["aws:TagKeys"] = keys
	}
	if len(s.transitiveKeys) > 0 {
		multi["sts:TransitiveTagKeys"] = s.transitiveKeys
	}
	if s.sourceIdentity != "" {
		condCtx["sts:SourceIdentity"] = s.sourceIdentity
	}
	return multi
}

// extraActions are the permissions-only actions the trust policy must also allow:
// sts:TagSession for tags the request passes, and sts:SetSourceIdentity for a source identity,
// whether passed or carried from the calling session ("permissions for sts:SetSourceIdentity are
// required … in the role trust policy of the target role" when chaining).
func (s stsAssumeRoleSession) extraActions() []string {
	var out []string
	if len(s.requestTags) > 0 {
		out = append(out, "sts:TagSession")
	}
	if s.sourceIdentity != "" {
		out = append(out, "sts:SetSourceIdentity")
	}
	return out
}

// stsTagKeyFold returns the key in tags that equals key regardless of case — session tag keys are
// case-insensitive — and whether there is one.
func stsTagKeyFold(tags map[string]string, key string) (string, bool) {
	if _, ok := tags[key]; ok {
		return key, true
	}
	for k := range tags {
		if strings.EqualFold(k, key) {
			return k, true
		}
	}
	return "", false
}
