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
// # URL-encoded everywhere
//
// Every policy document IAM returns is the text percent-encoded per RFC 3986, in every operation
// and every shape: Role.AssumeRolePolicyDocument (CreateRole, GetRole, ListRoles,
// CreateServiceLinkedRole, and the roles nested in GetInstanceProfile, ListInstanceProfiles and
// GetAccountAuthorizationDetails), the inline Get{User,Group,Role}Policy documents, PolicyDetail and
// PolicyVersion.Document. The evidence is botocore (as bundled in AWS CLI v2.40.3): its
// json_decode_policies handler, registered as after-call.iam for every IAM operation, says "Any time
// an IAM operation returns a policy document it is a string that is json that has been urlencoded,
// i.e urlencode(json.dumps(policy_document))", and URL-decodes every member whose shape is
// policyDocumentType — which, in the IAM service model, is the shape of Role.AssumeRolePolicyDocument,
// RoleDetail.AssumeRolePolicyDocument, PolicyDetail.PolicyDocument, PolicyVersion.Document and each
// Get*PolicyResponse.PolicyDocument alike. Six API Reference pages (GetRole, GetRolePolicy,
// GetUserPolicy, GetGroupPolicy, GetPolicyVersion, GetAccountAuthorizationDetails) state the encoding
// in a note; the others are silent rather than contrary, and their sample responses render plain JSON
// exactly as the six annotated pages' samples do, so the samples are not evidence either way.
//
// Encoding only on the annotated pages, as #1452 first did, would have handed one document back two
// ways depending on which operation read it — a CreateRole response a caller could not compare with
// the GetRole answer for the same role.

// iamRenderPolicyDocument returns text as a response element's content: percent-encoded per RFC 3986
// with [iamEscapeRFC3986], then XML-escaped.
func iamRenderPolicyDocument(text string) string {
	return xmlEsc(iamEscapeRFC3986(text))
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
