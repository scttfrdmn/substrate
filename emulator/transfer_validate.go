package emulator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
)

// What the Transfer Family pages publish about a request member, and how a request that breaks it is
// refused (#1197, #1198).
//
// Every refusal here is InvalidRequestException at HTTP 400, the code each operation's own page
// publishes for "the client submits a malformed request" — the same code transferInvalidBody answers
// for a body that will not parse. No Transfer page publishes ValidationException, and the Common
// Errors page's ValidationError is not listed on any of the ten routed operations, so neither is
// borrowed (#671).
//
// The cross-member rules the CreateServer page states in prose — FTP or FTPS needs EndpointType VPC
// and a non-SERVICE_MANAGED identity provider, FTPS needs a Certificate, AS2 needs VPC and the S3
// domain — are not enforced. Each member is checked against its own Valid Values, Length Constraints
// and Pattern; the combinations are recorded as sent, since the page publishes no code or message for
// a combination and substrate would have to invent both.

// transferUserNamePattern is API_CreateUser's UserName pattern, [\w][\w@.-]{2,99}.
var transferUserNamePattern = regexp.MustCompile(`^[\w][\w@.-]{2,99}$`)

// transferRolePattern is the Role pattern arn:.*role/\S+, which both CreateUser and LoggingRole use.
var transferRolePattern = regexp.MustCompile(`^arn:.*role/\S+$`)

// transferSecurityPolicyPattern is the SecurityPolicyName pattern.
var transferSecurityPolicyPattern = regexp.MustCompile(`^Transfer[A-Za-z0-9]*SecurityPolicy-[A-Za-z0-9-]+$`)

// transferBannerPattern is the login-banner pattern [\x09-\x0D\x20-\x7E]*.
var transferBannerPattern = regexp.MustCompile(`^[\x09-\x0D\x20-\x7E]*$`)

// transferLogDestinationPattern is the StructuredLogDestinations element pattern arn:\S+.
var transferLogDestinationPattern = regexp.MustCompile(`^arn:\S+$`)

// transferHomeDirectoryPattern is the HomeDirectory pattern (|/.*).
var transferHomeDirectoryPattern = regexp.MustCompile(`^(|/.*)$`)

// transferSSHPublicKeyPattern is API_CreateUser's SSHPublicKeyBody pattern.
var transferSSHPublicKeyPattern = regexp.MustCompile(`^\s*(ssh|ecdsa)-[a-z0-9-]+[ \t]+(([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{1,3})?(={0,3})?)(\s*|[ \t]+[\S \t]*\s*)$`)

// The Valid Values of the enumerated members the routed operations read.
var (
	transferDomains               = []string{"S3", "EFS"}
	transferEndpointTypes         = []string{"PUBLIC", "VPC", "VPC_ENDPOINT"}
	transferIdentityProviderTypes = []string{"SERVICE_MANAGED", "API_GATEWAY", "AWS_DIRECTORY_SERVICE", "AWS_LAMBDA"}
	transferIPAddressTypes        = []string{"IPV4", "DUALSTACK"}
	transferProtocols             = []string{"SFTP", "FTP", "FTPS", "AS2"}
	transferHomeDirectoryTypes    = []string{"PATH", "LOGICAL"}
)

// transferInvalidRequest is the published refusal for a member that breaks its own constraints.
func transferInvalidRequest(format string, args ...any) *AWSError {
	return &AWSError{Code: "InvalidRequestException", Message: fmt.Sprintf(format, args...), HTTPStatus: http.StatusBadRequest}
}

// transferNotFound is ResourceNotFoundException at 400, the status every Transfer page that publishes
// the code gives it (#1198). It was 404, which a retry wrapper reads as "not yet consistent".
func transferNotFound(message string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// transferIsNotFound reports whether err is the published not-found refusal, as opposed to a store
// fault that must be returned.
func transferIsNotFound(err error) bool {
	var awsErr *AWSError
	return errors.As(err, &awsErr) && awsErr.Code == "ResourceNotFoundException"
}

// transferCheckEnum refuses a non-empty value outside the member's published Valid Values.
func transferCheckEnum(member, value string, valid []string) error {
	if value == "" {
		return nil
	}
	for _, v := range valid {
		if value == v {
			return nil
		}
	}
	return transferInvalidRequest("%s %q is not one of %v", member, value, valid)
}

// transferCheckString refuses a non-empty value longer than maxLen or not matching pattern. A nil
// pattern checks length only.
func transferCheckString(member, value string, maxLen int, pattern *regexp.Regexp) error {
	if value == "" {
		return nil
	}
	if len(value) > maxLen {
		return transferInvalidRequest("%s is longer than %d characters", member, maxLen)
	}
	if pattern != nil && !pattern.MatchString(value) {
		return transferInvalidRequest("%s %q does not match the pattern %s", member, value, pattern.String())
	}
	return nil
}

// transferCheckProtocols applies Protocols' published 1–4 items and Valid Values.
func transferCheckProtocols(protocols []string) error {
	if len(protocols) < 1 || len(protocols) > 4 {
		return transferInvalidRequest("Protocols must name between 1 and 4 protocols")
	}
	for _, p := range protocols {
		if err := transferCheckEnum("Protocols", p, transferProtocols); err != nil {
			return err
		}
	}
	return nil
}

// transferCheckLogDestinations applies StructuredLogDestinations' published 0–1 items, each 20–1600
// characters matching arn:\S+.
func transferCheckLogDestinations(dests []string) error {
	if len(dests) > 1 {
		return transferInvalidRequest("StructuredLogDestinations names at most 1 log group")
	}
	for _, d := range dests {
		if len(d) < 20 {
			return transferInvalidRequest("StructuredLogDestinations entry %q is shorter than 20 characters", d)
		}
		if err := transferCheckString("StructuredLogDestinations", d, 1600, transferLogDestinationPattern); err != nil {
			return err
		}
	}
	return nil
}

// transferRawJSONKind reports whether raw is absent (or null), or else whether it is an object or an
// array, so a member the request carries as a nested structure is refused when it is anything else.
func transferRawJSONKind(member string, raw json.RawMessage, wantArray bool) (present bool, err error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false, nil
	}
	want, kind := byte('{'), "an object"
	if wantArray {
		want, kind = '[', "an array"
	}
	if trimmed[0] != want {
		return false, transferInvalidRequest("%s must be %s", member, kind)
	}
	return true, nil
}

// transferCheckUserProfile applies the published constraints of the four scalar user members CreateUser
// and UpdateUser share, so the two operations cannot drift apart: Role (20–2048 characters,
// arn:.*role/\S+), HomeDirectory (0–1024, (|/.*)), HomeDirectoryType (PATH | LOGICAL) and Policy
// (0–2048). An empty value is one the request did not send, and is left to the caller.
func transferCheckUserProfile(role, homeDirectory, homeDirectoryType, policy string) error {
	if role != "" && len(role) < 20 {
		return transferInvalidRequest("Role is shorter than 20 characters")
	}
	if err := transferCheckString("Role", role, 2048, transferRolePattern); err != nil {
		return err
	}
	if err := transferCheckString("HomeDirectory", homeDirectory, 1024, transferHomeDirectoryPattern); err != nil {
		return err
	}
	if err := transferCheckEnum("HomeDirectoryType", homeDirectoryType, transferHomeDirectoryTypes); err != nil {
		return err
	}
	return transferCheckString("Policy", policy, 2048, nil)
}

// transferCheckHomeDirectoryMappings reports whether raw carries HomeDirectoryMappings, refusing anything
// but an array of 1–50000 entries, the published Array Members of both CreateUser and UpdateUser.
func transferCheckHomeDirectoryMappings(raw json.RawMessage) (present bool, err error) {
	present, err = transferRawJSONKind("HomeDirectoryMappings", raw, true)
	if err != nil || !present {
		return present, err
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return false, transferInvalidRequest("HomeDirectoryMappings must be an array of objects")
	}
	if len(entries) < 1 || len(entries) > 50000 {
		return false, transferInvalidRequest("HomeDirectoryMappings must hold between 1 and 50000 entries")
	}
	return true, nil
}

// transferCheckTags applies a Tags member's published Array Members, 1–50 items. A nil slice is a
// request that sent no Tags; an empty one sent `[]`, which is below the published minimum.
func transferCheckTags(tags []TransferTag) error {
	if tags == nil {
		return nil
	}
	if len(tags) < 1 || len(tags) > 50 {
		return transferInvalidRequest("Tags must hold between 1 and 50 tags")
	}
	return nil
}

// transferDeref returns the string p points to, or "" for a member the request did not send.
func transferDeref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
