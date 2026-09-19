package emulator

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
)

// wafv2IPSetNamePattern is CreateIPSet's documented Name pattern, "^[\w\-]+$".
var wafv2IPSetNamePattern = regexp.MustCompile(`^[\w\-]+$`)

// wafv2IPSetNameMaxLen is CreateIPSet's documented maximum Name length. The
// documented minimum is 1, which is indistinguishable from the member being
// absent, so it is enforced by the required-member check rather than here.
const wafv2IPSetNameMaxLen = 128

// wafv2ValidateCreateIPSet refuses a CreateIPSet request that omits a required
// member or supplies an invalid value, and returns nil for one AWS would accept.
//
// Two error codes, because AWS documents two distinct failures and #755's open
// question was which one applies to each:
//
//   - An **omitted** required member answers ValidationError/400, from WAFv2's
//     Common Error Types: "The input doesn't meet the required format or
//     constraints. Check that all required parameters are included and that
//     values are valid." That sentence names the omitted-parameter case
//     explicitly, and ValidationError is not on CreateIPSet's own error list
//     because it is common to every action.
//   - A **present but invalid** value answers WAFInvalidParameterException/400,
//     which CreateIPSet's Errors section does list, whose first bullet is "You
//     specified a parameter name or value that isn't valid."
//
// All four of Addresses, IPAddressVersion, Name and Scope are Required: Yes.
// Substrate previously defaulted three of them — Scope to REGIONAL,
// IPAddressVersion to IPV4, and a nil Addresses to an empty slice — so a request
// AWS refuses created an IP set whose scope and address family the caller never
// asked for.
//
// The nil-Addresses default was the most consequential, because AWS makes absent
// and empty *distinct*: the page lists `"Addresses": []` among its valid example
// specifications while marking `"Addresses": [""]` INVALID. So an empty array is
// accepted here and an absent member is refused, and Addresses must not be
// normalized from nil to []string{} anywhere before this check runs — the same
// three-way distinction #824's change-set tags needed.
//
// Two things AWS's page describes but this function does not enforce, recorded
// rather than invented:
//
//   - Whether an address must match the declared IPAddressVersion. AWS states
//     only that addresses use CIDR notation and that /0 is unsupported; it does
//     not say an IPV4 set refuses an IPv6 CIDR. Refusing it would be substrate's
//     rule, so the version is validated as an enum and the addresses as CIDRs,
//     independently.
//   - WAFInvalidParameterException's three modeled members, Field ("the settings
//     where the invalid parameter was found"), Parameter ("the invalid parameter
//     that resulted in the exception") and Reason ("additional information about
//     the exception"). The API Reference describes them in prose and publishes no
//     enumeration of the values Field and Reason take, so emitting them would
//     mean inventing values — the thing #671 settles against. The three are
//     folded into the message instead, naming the member and the reason in AWS's
//     own vocabulary. AWSError also carries only a code, a message and a status,
//     so a modeled error's extra members have no channel to the wire today.
func wafv2ValidateCreateIPSet(name, scope, ipAddressVersion string, addresses []string) error {
	// Required members first: an omitted member and an invalid one answer
	// different codes, and a request missing several should not be reported as
	// having an invalid value for one of them.
	for _, m := range []struct {
		member  string
		omitted bool
	}{
		{"Name", name == ""},
		{"Scope", scope == ""},
		{"IPAddressVersion", ipAddressVersion == ""},
		{"Addresses", addresses == nil},
	} {
		if m.omitted {
			return wafv2MissingMember(m.member)
		}
	}

	if len(name) > wafv2IPSetNameMaxLen {
		return wafv2InvalidParameter("Name", name,
			fmt.Sprintf("must be at most %d characters", wafv2IPSetNameMaxLen))
	}
	if !wafv2IPSetNamePattern.MatchString(name) {
		return wafv2InvalidParameter("Name", name, `must match the pattern ^[\w\-]+$`)
	}
	if err := wafv2ValidateScopeValue(scope); err != nil {
		return err
	}
	if ipAddressVersion != "IPV4" && ipAddressVersion != "IPV6" {
		return wafv2InvalidParameter("IPAddressVersion", ipAddressVersion,
			"must be one of IPV4, IPV6")
	}
	for _, addr := range addresses {
		if err := wafv2ValidateIPSetAddress(addr); err != nil {
			return err
		}
	}
	return nil
}

// wafv2ValidateIPSetAddress refuses one entry of an IP set's Addresses array.
//
// AWS documents the whole constraint in one sentence — addresses are "specified
// using Classless Inter-Domain Routing (CIDR) notation" and WAF "supports all
// IPv4 and IPv6 CIDR ranges except for /0" — so parsing as a CIDR and rejecting a
// zero-length prefix is the complete rule for the value itself. The member's
// documented per-string constraints, a length of 1 to 50 and the pattern
// `.*\S.*`, are subsumed: neither an empty string nor a whitespace-only one nor
// anything over 50 characters parses as a CIDR. The empty string is the case AWS
// singles out, marking `"Addresses": [""]` INVALID.
func wafv2ValidateIPSetAddress(addr string) error {
	_, network, err := net.ParseCIDR(addr)
	if err != nil {
		return wafv2InvalidParameter("Addresses", addr,
			"must be an IPv4 or IPv6 address in CIDR notation")
	}
	if ones, _ := network.Mask.Size(); ones == 0 {
		return wafv2InvalidParameter("Addresses", addr,
			"a /0 CIDR range is not supported")
	}
	return nil
}

// wafv2ValidateScope refuses a request whose Scope is omitted or is not one of the two
// published values, and returns nil for one AWS would accept.
//
// Scope is Required: Yes on eight of the nine operations that read it —
// CreateWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs, GetIPSet, UpdateIPSet, DeleteIPSet
// and ListIPSets — and Required: No only on GetWebACL, which addresses a web ACL by ARN
// instead. Every one of those eight defaulted an absent Scope to REGIONAL before #1062, and
// **no WAFv2 page publishes any default for the member**: the value was substrate's
// invention, so a caller who omitted it while meaning CLOUDFRONT silently got a REGIONAL
// lookup and a NotFound, or worse, created a web ACL in the wrong scope. #1062's issue body
// named two of the eight; correcting only those two would have left the same defect at six
// sites, which is how #950's eleven deferred sites survived four releases into #1063.
//
// GetWebACL keeps its default and calls [wafv2ValidateScopeValue] instead — an optional
// member cannot be refused for being absent, but the lookup still needs a value to key on,
// so the REGIONAL fallback there is a recorded divergence rather than something this release
// can fix.
func wafv2ValidateScope(scope string) error {
	if scope == "" {
		return wafv2MissingMember("Scope")
	}
	return wafv2ValidateScopeValue(scope)
}

// wafv2ValidateScopeValue refuses a Scope that is present but is neither CLOUDFRONT nor
// REGIONAL, the two values every WAFv2 page publishes for the member.
//
// It is separate from [wafv2ValidateScope] because the omitted and invalid cases answer
// different codes — see [wafv2ValidateCreateIPSet] for #755's argument — and because
// GetWebACL needs the value check without the presence check.
func wafv2ValidateScopeValue(scope string) error {
	if scope != "CLOUDFRONT" && scope != "REGIONAL" {
		return wafv2InvalidParameter("Scope", scope, "must be one of CLOUDFRONT, REGIONAL")
	}
	return nil
}

// wafv2MissingMember builds the ValidationError an omitted required member answers.
//
// #755 settled the code for CreateIPSet and wrote the argument out above; #1063 found six
// further sites that had answered WAFInvalidParameterException for the same condition —
// createWebACL's Name, the ResourceArn check that associateWebACL, disassociateWebACL and
// getWebACLForResource each carry, and the Id check in loadWebACLByID and loadIPSetByID —
// so one plugin answered two codes for one class of caller error, which is the defect
// [wafv2ValidateCreateIPSet]'s split exists to prevent. They all call this now.
//
// The distinction is the one #755 drew and every page re-states verbatim:
// WAFInvalidParameterException is glossed "The operation failed because AWS WAF didn't
// recognize a parameter in the request", and all four of its published examples are about a
// value substrate *read* — a name or value that isn't valid, an unnestable nested statement,
// an unavailable DefaultAction type, a malformed ARN. An absent member is none of those, and
// the common list's ValidationError names the case outright: "Check that all required
// parameters are included."
//
// The member name goes in the message because ValidationError's published sentence does not
// say which parameter was missing and a caller omitting one of four needs to know.
func wafv2MissingMember(member string) *AWSError {
	return wafv2ValidationError(member + " is a required parameter")
}

// wafv2ValidationError builds the common list's ValidationError with detail appended to its
// published sentence.
//
// It exists for the one refusal that is about the request as a whole rather than about a
// named member — getWebACL, whose four identifiers are each individually Required: No but
// which cannot address a web ACL when none of them is supplied. [wafv2MissingMember] is the
// named-member case and the only other caller.
func wafv2ValidationError(detail string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "The input doesn't meet the required format or constraints: " + detail,
		HTTPStatus: http.StatusBadRequest,
	}
}

// wafv2InvalidParameter builds the WAFInvalidParameterException a present but
// invalid value answers. The member, the offending value and the reason go in the
// message because the exception's Field, Parameter and Reason members have no
// wire channel and no published enumeration — see wafv2ValidateCreateIPSet.
//
// The value is quoted with %q so an empty or whitespace-only one is visible in
// the message rather than vanishing into it — which matters because `[""]` is the
// invalid address AWS singles out.
func wafv2InvalidParameter(member, value, reason string) *AWSError {
	return &AWSError{
		Code: "WAFInvalidParameterException",
		Message: fmt.Sprintf("You specified a parameter name or value that isn't valid: %s=%q %s",
			member, value, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
