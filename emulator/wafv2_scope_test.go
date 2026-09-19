package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scttfrdmn/substrate/emulator"
)

// The two halves of Scope that the one-code member-complaint table cannot carry (#1062).
//
// memberComplaintServices asserts one code per service, and Scope has two: an omitted member answers
// ValidationError and a present-but-unrecognized one answers WAFInvalidParameterException, which is
// #755's split and the reason wafv2_validate.go has two constructors. The table pins the first half at
// all eight required operations; this file pins the second, and pins the one operation that is exempt
// from the first.

// wafv2ScopeHost routes to the WAFv2 plugin.
const wafv2ScopeHost = "wafv2.us-east-1.amazonaws.com"

// TestWAFv2ScopeValueIsHeldToTheTwoPublishedValues asserts an unrecognized Scope answers
// WAFInvalidParameterException rather than the ValidationError an absent one answers.
//
// Every WAFv2 page publishes Valid Values: CLOUDFRONT | REGIONAL for the member, and GLOBAL is the
// plausible wrong guess — it is what CloudFront's own console calls the scope, and a caller who sends
// it would have been given a REGIONAL lookup silently before #1062, because the check was a
// zero-value default and a non-empty wrong value sailed past it.
//
// The value is asserted to appear in the message. WAFInvalidParameterException's published Field,
// Parameter and Reason members have no wire channel in substrate, so the message is the only place a
// caller can learn which value was refused — see [wafv2InvalidParameter].
func TestWAFv2ScopeValueIsHeldToTheTwoPublishedValues(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// One per operation that requires Scope, because the check is applied per handler rather than in
	// a shared decoder: a handler that forgot the call is exactly what this catches.
	for _, target := range []string{
		"CreateWebACL", "UpdateWebACL", "DeleteWebACL", "ListWebACLs",
		"GetIPSet", "UpdateIPSet", "DeleteIPSet", "ListIPSets",
	} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			// Name and Id are supplied so the refusal cannot be one of the identifier checks that
			// sit beside the Scope check in these handlers.
			body := []byte(`{"Scope":"GLOBAL","Name":"acl","Id":"` +
				"00000000-0000-0000-0000-000000000000" + `"}`)
			status, code, message := rawUnsignedCall(t, ts, wafv2ScopeHost,
				"AWSWAF_20190729."+target, "", body)

			assert.Equal(t, "WAFInvalidParameterException", code,
				"a present but unrecognized Scope is the invalid-value case, not the omitted one")
			assert.Equal(t, http.StatusBadRequest, status, "the page publishes the code at 400")
			assert.Contains(t, message, "GLOBAL", "the refused value is named")
			assert.Contains(t, message, "CLOUDFRONT, REGIONAL", "the published values are named")
		})
	}
}

// TestWAFv2GetWebACLDoesNotRequireScope asserts the one operation #1062 left alone.
//
// API_GetWebACL marks all four of ARN, Id, Name and Scope Required: No, because the ARN addresses a
// web ACL on its own. So the sweep that made Scope required at eight operations must not reach the
// ninth, and the two assertions here are what distinguish "the check was applied everywhere it
// belongs" from "the check was applied everywhere".
//
// An absent Scope still reaches the refusal that GetWebACL does have — none of its four identifiers
// was supplied, so the request addresses nothing — which is why the assertion is on the message
// rather than on the status: both refusals are ValidationError at 400, and only the message tells
// them apart.
func TestWAFv2GetWebACLDoesNotRequireScope(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, code, message := rawUnsignedCall(t, ts, wafv2ScopeHost,
		"AWSWAF_20190729.GetWebACL", "", []byte("{}"))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationError", code)
	assert.NotContains(t, message, "Scope is a required parameter",
		"Scope is Required: No on API_GetWebACL")
	assert.Contains(t, message, "the request identifies no web ACL",
		"the refusal is about the missing identifier, which is the one GetWebACL publishes")

	// A present but unrecognized Scope is still refused here, because Required: No governs whether
	// the member may be absent and not whether a supplied value may be anything.
	_, code, message = rawUnsignedCall(t, ts, wafv2ScopeHost, "AWSWAF_20190729.GetWebACL", "",
		[]byte(`{"ARN":"arn:aws:wafv2:us-east-1:123456789012:regional/webacl/acl/id","Scope":"GLOBAL"}`))
	assert.Equal(t, "WAFInvalidParameterException", code)
	assert.Contains(t, message, "GLOBAL")
}
