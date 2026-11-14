package emulator

import (
	"encoding/json"
	"fmt"
)

// How IAM serves a policy document back, and why it is the submitted text (#1452).
//
// # Verbatim
//
// IAM stores a policy as the text it was given and answers that text. Substrate used to keep only
// the parsed [PolicyDocument] and re-marshal it on every read, which normalized the document: a
// trust principal submitted as {"AWS":"arn:..."} came back as the bare string "arn:...", because
// [PolicyPrincipal.MarshalJSON] writes a lone AWS principal that way. A bare-string Principal is
// valid only as "*", so the document a caller read back was not one IAM would accept through
// UpdateAssumeRolePolicy, and code converging a trust policy by comparing GetRole's answer with
// the document it means to write saw a difference real IAM never reports.
//
// So every writer now keeps the submitted text beside the parsed form: CreateRole and
// UpdateAssumeRolePolicy on [IAMRole.AssumeRolePolicyDocumentText], CreatePolicy on
// [IAMPolicy.DocumentText], and Put{User,Role,Group}Policy as the stored value itself. The parsed
// form is still what policy evaluation, the STS trust gate and CloudFormation drift comparison
// read; only what a response renders changed. A record with no text — one written before #1452,
// a service-linked role, a bundled AWS managed policy — falls back to marshaling the parsed form.
//
// # Encoded where a page says so
//
// GetRole, GetRolePolicy, GetUserPolicy, GetGroupPolicy, GetPolicyVersion and
// GetAccountAuthorizationDetails each carry the note "Policies returned by this operation are
// URL-encoded compliant with RFC 3986", so those operations percent-encode the text with
// [iamEscapeRFC3986]. CreateRole's page says instead that "the response includes the same trust
// policy in JSON format", and ListRoles, CreateServiceLinkedRole, GetInstanceProfile and
// ListInstanceProfiles carry no such note, so they serve the same text unencoded. SDKs that decode
// automatically (botocore unquotes every policy-document member) read the same document either
// way; a client that decodes by hand must do so for the six operations above.

// iamDocEncoding is how a response renders a policy document.
type iamDocEncoding int

const (
	// iamDocPlain renders the document text as it is, XML-escaped.
	iamDocPlain iamDocEncoding = iota
	// iamDocRFC3986 percent-encodes the document text before XML-escaping it.
	iamDocRFC3986
)

// render returns text as the response element's content under e.
func (e iamDocEncoding) render(text string) string {
	if e == iamDocRFC3986 {
		return xmlEsc(iamEscapeRFC3986(text))
	}
	return xmlEsc(text)
}

// iamPolicyText returns the text a response serves for a policy document: text when the writer
// recorded it, otherwise doc marshaled — the fallback for records that predate #1452 and for
// documents substrate builds itself.
func iamPolicyText(text string, doc PolicyDocument) (string, error) {
	if text != "" {
		return text, nil
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal policy document: %w", err)
	}
	return string(raw), nil
}
