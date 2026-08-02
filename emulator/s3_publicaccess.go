package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
)

// S3 predefined group URIs. A grant to either one is what makes an ACL public:
// "Amazon S3 considers a bucket or object ACL public if it grants any permissions
// to members of the predefined AllUsers or AuthenticatedUsers groups" (S3 user
// guide, Blocking public access → The meaning of "public" → ACLs).
//
// AuthenticatedUsers is every AWS account, not every account in yours, which is
// why it counts as public despite the name.
const (
	s3GroupAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	s3GroupAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

// s3AccessDeniedResponse is the rejection Block Public Access returns for a
// request that would make a bucket or object public.
//
// Neither PutBucketAcl, PutObjectAcl nor PutBucketPolicy documents an Errors
// section covering this, so the code and message come from observed real-AWS
// behavior rather than from the API model: a blocked PutBucketPolicy surfaces
// through the CLI as "An error occurred (AccessDenied) when calling the
// PutBucketPolicy operation: Access Denied" — code AccessDenied, HTTP 403, and
// the message string "Access Denied" that S3 uses for every authorization
// failure. Substrate returns the same triple so a consumer matching on the code
// takes the branch it would take against AWS.
func s3AccessDeniedResponse() *AWSResponse {
	return s3ErrorResponse("AccessDenied", "Access Denied", http.StatusForbidden)
}

// s3LoadPublicAccessBlock returns a bucket's Block Public Access configuration,
// or nil when the bucket has none.
//
// A nil return must behave exactly as substrate did before enforcement existed:
// an unconfigured bucket accepts a public ACL and a public policy, which is both
// what real S3 does for a bucket outside an account-level or organization-level
// setting and what keeps the #446 tests — written against the record-only
// behavior — passing unchanged.
func (p *S3Plugin) s3LoadPublicAccessBlock(ctx context.Context, bucket string) (*S3PublicAccessBlockConfiguration, error) {
	raw, err := p.state.Get(ctx, s3Namespace, s3PublicAccessBlockKey(bucket))
	if err != nil {
		return nil, fmt.Errorf("get public access block: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	var cfg S3PublicAccessBlockConfiguration
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal public access block: %w", err)
	}
	return &cfg, nil
}

// s3BlocksPublicACLs reports whether a bucket's configuration rejects a public
// ACL, and s3BlocksPublicPolicy whether it rejects a public bucket policy.
//
// Both read the bucket's own configuration. Block Public Access has no
// per-object setting — "Amazon S3 doesn't support block public access settings on
// a per-object basis" — so PutObjectAcl consults the bucket that holds the
// object.
func (p *S3Plugin) s3BlocksPublicACLs(ctx context.Context, bucket string) (bool, error) {
	cfg, err := p.s3LoadPublicAccessBlock(ctx, bucket)
	if err != nil {
		return false, err
	}
	return cfg != nil && cfg.BlockPublicAcls, nil
}

// s3BlocksPublicPolicy reports whether a bucket's configuration rejects a public
// bucket policy. See [S3Plugin.s3BlocksPublicACLs].
func (p *S3Plugin) s3BlocksPublicPolicy(ctx context.Context, bucket string) (bool, error) {
	cfg, err := p.s3LoadPublicAccessBlock(ctx, bucket)
	if err != nil {
		return false, err
	}
	return cfg != nil && cfg.BlockPublicPolicy, nil
}

// s3PublicACLDenied returns the 403 to send when a bucket blocks public ACLs and
// the request carries one, or nil when the request may proceed.
//
// The ACL and the headers are both examined because they are two of the three ways
// a public grant arrives: acl is what PutBucketAcl/PutObjectAcl resolved from the
// XML body or the x-amz-acl canned header, and headers still needs reading for
// x-amz-grant-*, which substrate does not fold into the resolved ACL.
//
// The error return is reserved for a state-store failure, which the caller must
// propagate rather than report as an authorization decision.
func (p *S3Plugin) s3PublicACLDenied(ctx context.Context, bucket string, acl S3AccessControlList, headers map[string]string) (*AWSResponse, error) {
	blocked, err := p.s3BlocksPublicACLs(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !blocked {
		return nil, nil
	}
	if s3ACLIsPublic(acl) || s3GrantHeadersArePublic(headers) {
		return s3AccessDeniedResponse(), nil
	}
	return nil, nil
}

// s3ACLIsPublic reports whether an ACL grants any permission to a predefined
// public group.
//
// "Any permissions" is literal: READ, WRITE, READ_ACP, WRITE_ACP and
// FULL_CONTROL all count, and the permission itself is not inspected. Matching is
// on the grantee URI rather than on the Type attribute, because the URI is what
// identifies the group and a request may set Type to "Group" for a grantee that
// is not one of the two public ones.
func s3ACLIsPublic(acl S3AccessControlList) bool {
	for _, grant := range acl.Grants {
		switch grant.Grantee.URI {
		case s3GroupAllUsers, s3GroupAuthenticatedUsers:
			return true
		}
	}
	return false
}

// s3GrantHeaderNames are the x-amz-grant-* headers PutBucketAcl and PutObjectAcl
// accept, each naming grantees for one permission.
//
// Substrate does not otherwise model these headers — an ACL set through them is
// not stored, which is a separate gap. They are parsed here because a header
// naming a public group URI is a public ACL expressed the third documented way,
// and a Block Public Access check that ignored them would have a hole a consumer
// could drive a public grant straight through.
var s3GrantHeaderNames = []string{
	"X-Amz-Grant-Read",
	"X-Amz-Grant-Write",
	"X-Amz-Grant-Read-Acp",
	"X-Amz-Grant-Write-Acp",
	"X-Amz-Grant-Full-Control",
}

// s3GrantHeadersArePublic reports whether any x-amz-grant-* header names a
// predefined public group.
//
// The header value is a comma-separated list of grantees, each written as
// type=value — for example `uri="http://acs.amazonaws.com/groups/global/AllUsers"`.
// Values may be quoted, so quotes are trimmed before the comparison.
func s3GrantHeadersArePublic(headers map[string]string) bool {
	for _, name := range s3GrantHeaderNames {
		value := headers[name]
		if value == "" {
			continue
		}
		for _, grantee := range strings.Split(value, ",") {
			key, val, found := strings.Cut(strings.TrimSpace(grantee), "=")
			if !found || !strings.EqualFold(strings.TrimSpace(key), "uri") {
				continue
			}
			switch strings.Trim(strings.TrimSpace(val), `"`) {
			case s3GroupAllUsers, s3GroupAuthenticatedUsers:
				return true
			}
		}
	}
	return false
}

// s3PolicyQualifyingConditionKeys are the condition keys that can qualify an
// otherwise-public statement as non-public, from the S3 user guide's "The meaning
// of 'public'" → Bucket policies list.
//
// aws:userid is on the list only "outside the pattern AROLEID:*", which
// [s3ConditionValuePinsAccess] handles as part of the wildcard rule: a value
// ending in ":*" contains a wildcard and therefore does not pin anything.
//
// The guide's first bullet is "An AWS principal, user, role, or service principal
// (e.g. aws:PrincipalOrgID)", so the three keys that name a principal directly —
// aws:PrincipalOrgID, aws:PrincipalArn and aws:PrincipalAccount — qualify on the
// same footing as the explicitly-listed ones. Keys not named or exemplified by the
// guide are deliberately absent: a key substrate accepted that S3 does not would
// let a public policy through, which is the defect this check exists to close.
//
// Lookup is lowercased because IAM condition keys are case-insensitive.
var s3PolicyQualifyingConditionKeys = map[string]bool{
	"aws:sourceip":              true,
	"aws:sourcearn":             true,
	"aws:sourcevpc":             true,
	"aws:sourcevpce":            true,
	"aws:sourceowner":           true,
	"aws:sourceaccount":         true,
	"aws:userid":                true,
	"aws:principalorgid":        true,
	"aws:principalarn":          true,
	"aws:principalaccount":      true,
	"s3:dataaccesspointarn":     true,
	"s3:dataaccesspointaccount": true,
}

// s3PolicyIsPublic reports whether a bucket policy grants public access, by
// Block Public Access's own definition.
//
// The rule is inverted from the obvious one, and getting it backwards is the
// trap: S3 does **not** look for a wildcard principal. "When evaluating a bucket
// policy, Amazon S3 begins by assuming that the policy is public. It then
// evaluates the policy to determine whether it qualifies as non-public. To be
// considered non-public, a bucket policy must grant access only to fixed values
// (values that don't contain a wildcard or an AWS Identity and Access Management
// Policy Variable)" for a principal or one of the keys in
// [s3PolicyQualifyingConditionKeys].
//
// The consequence a wildcard-detecting implementation gets wrong is the guide's
// own worked example: `Principal: "*"` narrowed by
// `StringLike aws:SourceVpc: "vpc-*"` **is public**, because the narrowing value
// itself contains a wildcard. The same statement with
// `StringEquals aws:SourceVpc: "vpc-91237329"` is not.
//
// A policy that does not parse as a policy document is not public. PutBucketPolicy
// already rejects a non-JSON body with MalformedPolicy before this runs, and a body
// that is JSON but not a policy document should not acquire a second rejection
// reason here — substrate would be refusing something real S3 accepts or refuses
// for an entirely different reason.
//
// Substrate does not model the guide's unsupported-action clause (S3 treats a
// statement granting an action S3 does not support as potentially public). That
// requires an authoritative list of every supported s3: action, which would go
// stale silently.
func s3PolicyIsPublic(policyJSON []byte) bool {
	var doc PolicyDocument
	if err := json.Unmarshal(policyJSON, &doc); err != nil {
		return false
	}
	for i := range doc.Statement {
		if s3StatementIsPublic(&doc.Statement[i]) {
			return true
		}
	}
	return false
}

// s3StatementIsPublic reports whether one statement makes a policy public.
//
// A single surviving public statement makes the whole policy public regardless of
// what the others grant. That is the guide's worked example: a policy granting
// CloudTrail access, a named second account access, and the public access
// "qualifies as public because of the third statement", and S3 then disables the
// cross-account grant too.
func s3StatementIsPublic(stmt *PolicyStatement) bool {
	// Only an Allow can grant public access; a Deny narrows. Effect is compared
	// case-insensitively because IAM accepts "Allow" and "allow" alike.
	if !strings.EqualFold(stmt.Effect, "Allow") {
		return false
	}

	// A statement whose Principal names only fixed values is non-public on the
	// principal alone and needs no condition.
	if s3PrincipalIsFixed(stmt.Principal) {
		return false
	}

	// Otherwise a condition must pin one of the documented keys to a fixed value.
	for _, values := range stmt.Condition {
		for key, vals := range values {
			if !s3PolicyQualifyingConditionKeys[strings.ToLower(key)] {
				continue
			}
			if s3ConditionPinsAccess(key, vals) {
				return false
			}
		}
	}
	return true
}

// s3PrincipalIsFixed reports whether a statement's Principal names only fixed
// values.
//
// A nil Principal is treated as not fixed. A bucket policy is a resource policy
// and Principal is required in one; a statement without one is malformed, and
// assuming public for it is the direction that matches S3's assume-public start.
func s3PrincipalIsFixed(principal *PolicyPrincipal) bool {
	if principal == nil || principal.All {
		return false
	}
	values := make([]string, 0, len(principal.AWS)+len(principal.Service)+len(principal.Federated))
	values = append(values, principal.AWS...)
	values = append(values, principal.Service...)
	values = append(values, principal.Federated...)
	if len(values) == 0 {
		return false
	}
	for _, v := range values {
		if !s3ValueIsFixed(v) {
			return false
		}
	}
	return true
}

// s3ConditionPinsAccess reports whether a condition key's values pin access to
// fixed values.
//
// Every value must be fixed: a key set to both a fixed and a wildcard value still
// admits the wildcard, so it pins nothing.
//
// Two keys carry documented departures from the plain fixed-value rule, and both
// are handled here rather than in [s3ValueIsFixed] because they are properties of
// the key, not of the string: aws:SourceIp adds a breadth test that a syntactically
// fixed CIDR can still fail, and s3:DataAccessPointArn permits one wildcard that
// does not make the policy public.
func s3ConditionPinsAccess(key string, values []string) bool {
	if len(values) == 0 {
		return false
	}
	if strings.EqualFold(key, "s3:DataAccessPointArn") {
		return s3DataAccessPointARNsPinAccess(values)
	}
	for _, v := range values {
		if !s3ValueIsFixed(v) {
			return false
		}
	}
	if strings.EqualFold(key, "aws:SourceIp") {
		return s3SourceIPPinsAccess(values)
	}
	return true
}

// s3DataAccessPointARNsPinAccess reports whether every s3:DataAccessPointArn value
// pins access.
//
// This key has an explicit carve-out from the wildcard rule, for bucket policies
// only: "When used in a bucket policy, this value can contain a wildcard for the
// access point name without rendering the policy public, as long as the account ID
// is fixed. For example, allowing access to
// arn:aws:s3:us-west-2:123456789012:accesspoint/* would permit access to any access
// point associated with account 123456789012 in Region us-west-2, without rendering
// the bucket policy public." The same statement in an *access point* policy would
// render it public — substrate models bucket policies here, so the carve-out
// applies.
//
// So the wildcard is allowed in the resource segment and nowhere else: every ARN
// field up to and including the account ID must be fixed.
func s3DataAccessPointARNsPinAccess(values []string) bool {
	for _, v := range values {
		// arn:partition:service:region:account-id:resource — six fields, and the
		// resource is the only one permitted a wildcard.
		fields := strings.SplitN(v, ":", 6)
		if len(fields) != 6 {
			// Not an ARN shape at all; fall back to the plain fixed-value rule.
			if !s3ValueIsFixed(v) {
				return false
			}
			continue
		}
		for _, field := range fields[:5] {
			if strings.ContainsAny(field, "*?") || strings.Contains(field, "${") {
				return false
			}
		}
		if fields[4] == "" {
			// An empty account ID pins nothing, wildcard or not.
			return false
		}
	}
	return true
}

// s3ValueIsFixed reports whether a policy value is fixed: no wildcard and no IAM
// policy variable.
//
// The two wildcard characters IAM recognizes in a policy value are "*" and "?",
// and a policy variable is written "${...}" — a value carrying any of them is not
// fixed, per "values that don't contain a wildcard or an AWS Identity and Access
// Management Policy Variable".
func s3ValueIsFixed(value string) bool {
	if value == "" {
		return false
	}
	return !strings.ContainsAny(value, "*?") && !strings.Contains(value, "${")
}

// s3SourceIPPinsAccess reports whether every aws:SourceIp value is narrow enough
// to count as fixed.
//
// A broad range pins nothing even though it contains no wildcard: "Bucket policies
// that grant access conditioned on the aws:SourceIp condition key with very broad
// IP ranges (for example, 0.0.0.0/1) are evaluated as 'public'. This includes
// values broader than /8 for IPv4 and /32 for IPv6 (excluding RFC1918 private
// ranges)."
//
// The prefix limits are read as inclusive: /8 is not "broader than /8", so an IPv4
// /8 pins access and only /7 and above do not.
//
// The RFC1918 exclusion exists because a private range is never reachable from the
// internet, so its breadth cannot make a bucket public. Under the inclusive reading
// it cannot change an IPv4 answer — the broadest RFC1918 range is 10.0.0.0/8, which
// the /8 bound already admits — but it is load-bearing for IPv6, where the
// unique-local range is fc00::/7 and every prefix inside it is far broader than the
// /32 bound. It is written for both families rather than IPv6 only so the rule in
// the code is the rule in the guide.
//
// A value that is neither a prefix nor a bare address does not pin access. That is
// the assume-public direction, and real S3 rejects a malformed aws:SourceIp value
// outright.
func s3SourceIPPinsAccess(values []string) bool {
	for _, v := range values {
		if !s3SourceIPValuePinsAccess(v) {
			return false
		}
	}
	return true
}

// s3SourceIPValuePinsAccess reports whether one aws:SourceIp value is narrow
// enough to count as fixed. See [s3SourceIPPinsAccess].
func s3SourceIPValuePinsAccess(value string) bool {
	prefix, err := netip.ParsePrefix(value)
	if err != nil {
		// A bare address is a single host, the narrowest possible range.
		addr, addrErr := netip.ParseAddr(value)
		return addrErr == nil && addr.IsValid()
	}
	if s3PrefixIsPrivate(prefix) {
		return true
	}
	if prefix.Addr().Is4() {
		return prefix.Bits() >= 8
	}
	return prefix.Bits() >= 32
}

// s3RFC1918Prefixes are the private IPv4 ranges the breadth rule excludes.
var s3RFC1918Prefixes = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}

// s3PrefixIsPrivate reports whether a prefix lies entirely within an RFC1918
// private range.
//
// Containment is tested rather than equality, so 10.1.0.0/16 is private as well as
// 10.0.0.0/8. [netip.Addr.IsPrivate] covers the IPv6 unique-local range too, which
// is the same reasoning applied to the address family the /32 bound governs.
func s3PrefixIsPrivate(prefix netip.Prefix) bool {
	if prefix.Addr().Is6() {
		return prefix.Addr().IsPrivate()
	}
	for _, p := range s3RFC1918Prefixes {
		private, err := netip.ParsePrefix(p)
		if err != nil {
			continue
		}
		if private.Bits() <= prefix.Bits() && private.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}
