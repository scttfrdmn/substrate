package emulator

// What an omitted member means to a Cognito user-pool update, and where each default is published
// (#1089).
//
// `API_UpdateUserPool` and `API_UpdateUserPoolClient` carry the same Important box, word for word:
//
//	If you don't provide a value for an attribute, Amazon Cognito sets it to its default value.
//
// and the same recommendation above it — construct the request from the current configuration, which
// both pages point at DescribeUserPool / DescribeUserPoolClient to obtain. So an update is a full
// replacement of the members the operation publishes, and substrate's `if body.X != ""` guards were a
// merge: a caller following AWS's advice and omitting a member it did not wish to change saw the old
// value survive where AWS resets it. The divergence is invisible until something is omitted.
//
// Full replacement governs only the members an operation publishes. `Schema` is absent from
// `API_UpdateUserPool`'s Request Syntax, so `CognitoUserPool.SchemaAttributes` is *preserved* across
// an update rather than cleared — an operation cannot reset a member it does not accept. The same
// holds for ProviderName, Status, Arn and CreationDate, none of which is settable by either body.
//
// The helpers below exist so the create and the update resolve an absent member the same way. Two
// doors each deciding independently what a body means is how the merge got there, and a shared
// resolver is what stops it coming back.
//
// Cognito's other handlers are untouched. #671's rule is that a guard is a defect only where that
// operation's own page publishes full replacement, never by analogy with a sibling — and the sibling
// here would be an especially tempting one, since these two pages publish the sentence and the
// twenty-odd other `update*` handlers in the tree do not.

// cognitoDefaultExplicitAuthFlows returns the authentication flows an app client supports when a
// request names none.
//
// This is the one member in either operation with an explicitly published default, and the sentence is
// on `API_UpdateUserPoolClient` and `API_CreateUserPoolClient` alike: "If you don't specify a value
// for `ExplicitAuthFlows`, your app client supports `ALLOW_REFRESH_TOKEN_AUTH`,
// `ALLOW_USER_SRP_AUTH`, and `ALLOW_CUSTOM_AUTH`." A fresh slice is returned per call so a caller
// cannot mutate the default out from under the next one.
func cognitoDefaultExplicitAuthFlows() []string {
	return []string{"ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_CUSTOM_AUTH"}
}

// cognitoExplicitAuthFlows resolves a request's ExplicitAuthFlows to the value the client carries.
//
// A nil slice is an absent member and takes the published default. An explicitly empty array is a
// value the caller specified, and is stored as given — the published sentence conditions on "if you
// don't specify a value", and `[]` is one.
//
// What the caller then observes is the empty set, not `[]`: [CognitoUserPoolClient]'s field carries
// `omitempty`, so an empty set marshals to an absent member where AWS would render `[]`. That is the
// pre-existing state-struct-on-the-wire divergence (#756), not this change's, and it does not affect
// the distinction that matters here — an explicit `[]` does not acquire the three defaults.
func cognitoExplicitAuthFlows(flows []string) []string {
	if flows == nil {
		return cognitoDefaultExplicitAuthFlows()
	}
	return flows
}

// cognitoMfaConfiguration resolves a request's MfaConfiguration to the value the pool carries.
//
// Neither page publishes a default; both publish `Valid Values: OFF | ON | OPTIONAL`. OFF is
// substrate's reading, and it predates this change — [CognitoIDPPlugin.createUserPool] has always
// applied it. It is applied at the update too so that an omitted member cannot leave the pool
// reporting the empty string, which is a value the page does not publish (#1013).
func cognitoMfaConfiguration(mfa string) string {
	if mfa == "" {
		return "OFF"
	}
	return mfa
}
