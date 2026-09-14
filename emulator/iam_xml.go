package emulator

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// iamXMLNS is the XML namespace for all IAM responses.
const iamXMLNS = "https://iam.amazonaws.com/doc/2010-05-08/"

// iamXMLResponse builds a successful XML response wrapped in the standard
// IAM envelope: <{op}Response><{op}Result>{resultXML}</{op}Result>...</{op}Response>.
// If resultXML is empty the result element is omitted (void operations).
func iamXMLResponse(status int, op, resultXML string) (*AWSResponse, error) {
	var buf bytes.Buffer
	buf.WriteString("<")
	buf.WriteString(op)
	buf.WriteString(`Response xmlns="`)
	buf.WriteString(iamXMLNS)
	buf.WriteString(`">`)
	if resultXML != "" {
		buf.WriteString("<")
		buf.WriteString(op)
		buf.WriteString("Result>")
		buf.WriteString(resultXML)
		buf.WriteString("</")
		buf.WriteString(op)
		buf.WriteString("Result>")
	}
	buf.WriteString("<ResponseMetadata><RequestId>stub-request-id</RequestId></ResponseMetadata></")
	buf.WriteString(op)
	buf.WriteString("Response>")
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml"},
		Body:       buf.Bytes(),
	}, nil
}

// iamXMLEmptyResponse builds a successful XML response with no result element.
// Used for void operations such as DeleteUser, AttachUserPolicy, etc.
func iamXMLEmptyResponse(op string) *AWSResponse {
	body := `<` + op + `Response xmlns="` + iamXMLNS + `"><ResponseMetadata><RequestId>stub-request-id</RequestId></ResponseMetadata></` + op + `Response>`
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "text/xml"},
		Body:       []byte(body),
	}
}

// iamErrorResponse builds an IAM XML error response.
// The returned response matches the IAM ErrorResponse envelope format.
func iamErrorResponse(code, message string, status int) *AWSResponse {
	body := fmt.Sprintf(`<ErrorResponse xmlns="`+iamXMLNS+`"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>stub-request-id</RequestId></ErrorResponse>`,
		xmlEsc(code), xmlEsc(message))
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml"},
		Body:       []byte(body),
	}
}

// xmlEsc returns s with XML special characters escaped.
func xmlEsc(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// --- Per-resource XML builders -----------------------------------------------

// iamUserXMLFields returns XML element content for an IAMUser (no wrapper tag).
//
// PasswordLastUsed is rendered here, so it reaches both user shapes: AWS's `User`
// type says the member "is returned only in the GetUser and ListUsers operations",
// and `ListUsers`' sample response carries it (#807). It is omitted when nil, which
// AWS documents as meaning the user never signed in with a password — and that is
// always the case here, because substrate models no password operation
// (`ChangePassword`, `CreateLoginProfile` and `UpdateLoginProfile` all answer
// `InvalidAction`), so nothing assigns the field. Rendering it anyway costs nothing
// and is what a consumer seeding an [IAMUser] into state directly observes.
//
// The member therefore also cannot appear on `CreateUser`, which shares this
// builder and is the one operation returning `User` that AWS's sentence excludes: a
// user created a moment ago has never used a password, so AWS omits it there too.
func iamUserXMLFields(u *IAMUser) string {
	var b strings.Builder
	b.WriteString("<UserId>")
	b.WriteString(xmlEsc(u.UserID))
	b.WriteString("</UserId><UserName>")
	b.WriteString(xmlEsc(u.UserName))
	b.WriteString("</UserName><Arn>")
	b.WriteString(xmlEsc(u.ARN))
	b.WriteString("</Arn><Path>")
	b.WriteString(xmlEsc(u.Path))
	b.WriteString("</Path><CreateDate>")
	b.WriteString(u.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")
	if u.PasswordLastUsed != nil {
		b.WriteString("<PasswordLastUsed>")
		b.WriteString(u.PasswordLastUsed.UTC().Format("2006-01-02T15:04:05Z"))
		b.WriteString("</PasswordLastUsed>")
	}
	return b.String()
}

// iamPermissionsBoundaryXML renders an entity's permissions boundary for a
// single-entity shape, and nothing at all when it has none.
//
// Omitted when absent because `PermissionsBoundary` is `Required: No` on both `User`
// and `Role`, and rendering an empty element would report a boundary policy with an
// empty ARN where AWS reports no boundary at all.
//
// Called from the single-entity wrappers only, never from a listing builder, for the
// same reason [iamEntityTagsXML] is: AWS's listing operations exclude it by name.
// `ListRoles` and `ListUsers` both carry this note verbatim (#807):
//
//	IAM resource-listing operations return a subset of the available attributes for the
//	resource. This operation does not return the following attributes, even though they are
//	an attribute of the returned object: PermissionsBoundary, RoleLastUsed, Tags. To view
//	all of the information for a role, see GetRole.
//
// So a consumer that sets a boundary and reads it back through GetRole or GetUser sees
// it, and one that finds the entity through ListRoles or ListUsers does not, and must
// read the entity — which is what the note tells them to do. Substrate reported one on
// both shapes until this release, which is a divergence in the direction where an
// emulator is *more* generous than the service: a consumer could write an assertion
// against a list response that AWS never satisfies.
//
// The users [iamUserListXML] renders inside `GetGroup`, and the roles
// [iamRoleMembersXML] renders inside an instance profile, are the list shape too. AWS
// documents no boundary on either — `GetGroup`'s and `GetInstanceProfile`'s samples both
// carry a reduced entity — and neither operation is a way to read one entity, so the
// note's "see GetRole" instruction applies unchanged.
//
// The two member names are AWS's, which they were not until #852. The element used to carry
// `PolicyArn` and `PolicyName`, and *neither name exists on AWS's shape*: the
// `PermissionsBoundary` member of `User`, `Role`, `UserDetail` and `RoleDetail` is an
// [AttachedPermissionsBoundary], whose Contents section lists exactly `PermissionsBoundaryArn`
// and `PermissionsBoundaryType`. So an SDK decoded the element into an empty struct — a
// consumer reading `role.PermissionsBoundary.PermissionsBoundaryArn` got `""` for a boundary
// substrate had stored and was reporting. Nothing in the tree had ever emitted either AWS
// name, and the one test covering the element asserted only that the tag was present, which is
// the assertion a decoded struct can make and raw XML cannot be fooled by.
//
// [IAMAttachedPolicy.PolicyName] is still stored on the entity — it costs nothing and
// `ListAttachedRolePolicies` renders it, where AWS's `AttachedPolicy` shape *does* have it —
// but it no longer reaches the wire here, because the boundary's shape has no member to carry
// it. That is what makes #846's "source the PolicyName from the resolved policy" moot rather
// than deferred: there is nowhere for a better-sourced name to go.
//
// [AttachedPermissionsBoundary]: https://docs.aws.amazon.com/IAM/latest/APIReference/API_AttachedPermissionsBoundary.html
func iamPermissionsBoundaryXML(boundary *IAMAttachedPolicy) string {
	if boundary == nil {
		return ""
	}
	return "<PermissionsBoundary><PermissionsBoundaryType>" + iamPermissionsBoundaryTypePolicy +
		"</PermissionsBoundaryType><PermissionsBoundaryArn>" + xmlEsc(boundary.PolicyARN) +
		"</PermissionsBoundaryArn></PermissionsBoundary>"
}

// iamPermissionsBoundaryTypePolicy is the only value AWS's model admits for
// `PermissionsBoundaryType`.
//
// AWS's page contradicts itself about it. The prose says the type "can only have a value of
// `Policy`" while the same page's enumeration says "Valid Values: `PermissionsBoundaryPolicy`",
// and the CLI v2 reference — generated from the service model — lists `PermissionsBoundaryPolicy`
// as the only possible value. Under #671's binding rule, *only what the API model states*, the
// wire value is the enum's. The prose is recorded here rather than silently resolved, because a
// consumer who read the prose and asserts `Policy` needs to know which of the two substrate
// chose and why.
//
// A constant rather than a literal at the one call site, so that a future shape rendering a
// boundary — `GetAccountAuthorizationDetails`' `UserDetail` and `RoleDetail` (#848) — cannot
// pick the other spelling.
const iamPermissionsBoundaryTypePolicy = "PermissionsBoundaryPolicy"

// iamEntityTagsXML renders an entity's tags for a single-entity shape, and nothing at all
// when there are none.
//
// Two rules meet here, and both come from AWS.
//
// The member is *omitted* rather than rendered empty when an entity has no tags. `Tags` is
// documented `Required: No` on `User`, `Role`, `Policy` and `InstanceProfile`, and every
// reference sample for an untagged entity carries no `<Tags>` element —
// `CreateInstanceProfile`'s sample proves the contrast, rendering its *required* empty list
// as `<Roles/>` while carrying no `<Tags>` at all. So [iamTagListXML], which writes the
// wrapper unconditionally, is right for `ListUserTags`/`ListRoleTags`, where `Tags` is
// required, and is guarded here (#796).
//
// And it is called from the single-entity wrappers only, never from a listing builder,
// because AWS's list operations document the opposite of its single reads. `ListRoles`,
// `ListUsers`, `ListPolicies` and `ListInstanceProfiles` all carry the same note verbatim:
//
//	IAM resource-listing operations return a subset of the available attributes for the
//	resource. This operation does not return the following attributes, even though they are
//	an attribute of the returned object: PermissionsBoundary, RoleLastUsed, Tags. To view
//	all of the information for a role, see GetRole.
//
// A consumer that tags a role and reads it back through GetRole sees the tags; one that
// finds it through ListRoles does not, and must read the entity to see them — which is
// what the note tells them to do.
func iamEntityTagsXML(tags []IAMTag) string {
	if len(tags) == 0 {
		return ""
	}
	return iamTagListXML(tags)
}

// iamSingleUserXML wraps user fields in a <User> element.
func iamSingleUserXML(u *IAMUser) string {
	return "<User>" + iamUserXMLFields(u) + iamPermissionsBoundaryXML(u.PermissionsBoundary) +
		iamEntityTagsXML(u.Tags) + "</User>"
}

// iamUserListXML builds <Users> containing <member> elements.
func iamUserListXML(users []*IAMUser) string {
	var b strings.Builder
	b.WriteString("<Users>")
	for _, u := range users {
		b.WriteString("<member>")
		b.WriteString(iamUserXMLFields(u))
		b.WriteString("</member>")
	}
	b.WriteString("</Users>")
	return b.String()
}

// iamRoleXMLFields returns XML element content for an IAMRole (no wrapper tag).
//
// `RoleLastUsed` is *not* rendered here; it is single-entity-only, via
// [iamRoleLastUsedXML], for the reason that function documents.
func iamRoleXMLFields(r *IAMRole) string {
	var b strings.Builder
	b.WriteString("<RoleId>")
	b.WriteString(xmlEsc(r.RoleID))
	b.WriteString("</RoleId><RoleName>")
	b.WriteString(xmlEsc(r.RoleName))
	b.WriteString("</RoleName><Arn>")
	b.WriteString(xmlEsc(r.ARN))
	b.WriteString("</Arn><Path>")
	b.WriteString(xmlEsc(r.Path))
	b.WriteString("</Path><CreateDate>")
	b.WriteString(r.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")
	if r.MaxSessionDuration > 0 {
		fmt.Fprintf(&b, "<MaxSessionDuration>%d</MaxSessionDuration>", r.MaxSessionDuration)
	}
	if r.Description != "" {
		b.WriteString("<Description>")
		b.WriteString(xmlEsc(r.Description))
		b.WriteString("</Description>")
	}
	// The Role shape carries AssumeRolePolicyDocument, so a role's trust policy is
	// readable through GetRole/ListRoles — which is the only way a caller can
	// confirm what UpdateAssumeRolePolicy stored (#594). Emitted as JSON rather
	// than URL-encoded JSON, matching how GetRolePolicy already returns an inline
	// document: botocore's after-call.iam handler unquotes then json.loads every
	// policyDocumentType member, and unquoting plain JSON is a no-op.
	//
	// The document is *re-marshaled from the parsed form*, not returned verbatim,
	// because that is what substrate stores — the same normalization GetRolePolicy
	// and GetUserPolicy already apply to inline policies. So a semantically
	// equivalent form can come back: a Principal submitted as {"AWS":"1234"}
	// reports as "1234", since PolicyPrincipal marshals a lone AWS entry as the
	// bare string AWS also accepts. Real IAM stores the document as text and
	// returns the submitted bytes, so a consumer comparing byte-for-byte sees a
	// difference; one parsing the document does not.
	//
	// Omitted when there are no statements, because a role created without a trust
	// policy has a zero PolicyDocument, and marshaling that would report a
	// document ({"Version":"","Statement":null}) where AWS reports none. The
	// emptiness test is the same one the STS gate uses to decide the role is
	// unenforced.
	if len(r.AssumeRolePolicyDocument.Statement) > 0 {
		if doc, err := json.Marshal(r.AssumeRolePolicyDocument); err == nil {
			b.WriteString("<AssumeRolePolicyDocument>")
			b.WriteString(xmlEsc(string(doc)))
			b.WriteString("</AssumeRolePolicyDocument>")
		}
	}
	return b.String()
}

// iamRoleLastUsedXML renders a role's last use for a single-entity shape, and nothing at
// all when it has never been assumed.
//
// Single-entity-only, and AWS scopes this member by naming its operations rather than by
// leaving it to the listing note — from the `RoleLastUsed` data type (#816):
//
//	This data type is returned as a response element in the GetRole and
//	GetAccountAuthorizationDetails operations.
//
// so it is called from [iamSingleRoleXML] and never from [iamRoleListXML] or
// [iamRoleMembersXML]. `GetAccountAuthorizationDetails` is not among the operations
// substrate answers, so `GetRole` is the whole of the member's reach here; if it is
// implemented later it renders a role through this same wrapper and gains the member with
// it. `ListRoles` excludes it by name, in the very sentence that already excludes
// `PermissionsBoundary` and `Tags` — see [iamPermissionsBoundaryXML] for the note
// verbatim. `CreateRole` and `CreateServiceLinkedRole` share this wrapper and render
// nothing, because a role created a moment ago has not been assumed.
//
// Omitted entirely for a never-assumed role, and **that is substrate's choice rather than
// AWS's**. AWS's page settles neither half of the question: `LastUsedDate` is documented
// only as
//
//	This field is null if the role has not been used within the IAM tracking period.
//
// which is a statement about a *tracked* role falling out of the trailing 400 days, not
// about a role never assumed at all; it says nothing about `Region` in that case, and
// nothing about whether the structure itself is present. Both members are `Required: No`,
// so omitting the wrapper is admissible, and it is the answer #816 asked for: a consumer
// can then distinguish "not yet assumed" from "assumed", which a rendered wrapper holding
// a zero date and an empty region could not express without inventing a value AWS never
// publishes.
func iamRoleLastUsedXML(lastUsed *IAMRoleLastUsed) string {
	if lastUsed == nil {
		return ""
	}
	return "<RoleLastUsed><LastUsedDate>" +
		lastUsed.LastUsedDate.UTC().Format("2006-01-02T15:04:05Z") +
		"</LastUsedDate><Region>" + xmlEsc(lastUsed.Region) +
		"</Region></RoleLastUsed>"
}

// iamSingleRoleXML wraps role fields in a <Role> element.
func iamSingleRoleXML(r *IAMRole) string {
	return "<Role>" + iamRoleXMLFields(r) + iamRoleLastUsedXML(r.RoleLastUsed) +
		iamPermissionsBoundaryXML(r.PermissionsBoundary) +
		iamEntityTagsXML(r.Tags) + "</Role>"
}

// iamRoleListXML builds <Roles> containing <member> elements.
func iamRoleListXML(roles []*IAMRole) string {
	var b strings.Builder
	b.WriteString("<Roles>")
	for _, r := range roles {
		b.WriteString("<member>")
		b.WriteString(iamRoleXMLFields(r))
		b.WriteString("</member>")
	}
	b.WriteString("</Roles>")
	return b.String()
}

// iamRoleMembersXML builds a <Roles> element with embedded role members
// suitable for use inside an InstanceProfile response.
func iamRoleMembersXML(roles []IAMRole) string {
	var b strings.Builder
	b.WriteString("<Roles>")
	for i := range roles {
		b.WriteString("<member>")
		b.WriteString(iamRoleXMLFields(&roles[i]))
		b.WriteString("</member>")
	}
	b.WriteString("</Roles>")
	return b.String()
}

// iamGroupXMLFields returns XML element content for an IAMGroup.
func iamGroupXMLFields(g *IAMGroup) string {
	return "<GroupId>" + xmlEsc(g.GroupID) + "</GroupId><GroupName>" + xmlEsc(g.GroupName) +
		"</GroupName><Arn>" + xmlEsc(g.ARN) + "</Arn><Path>" + xmlEsc(g.Path) +
		"</Path><CreateDate>" + g.CreateDate.UTC().Format("2006-01-02T15:04:05Z") + "</CreateDate>"
}

// iamSingleGroupXML wraps group fields in a <Group> element.
func iamSingleGroupXML(g *IAMGroup) string {
	return "<Group>" + iamGroupXMLFields(g) + "</Group>"
}

// iamGroupListXML builds <Groups> containing <member> elements.
func iamGroupListXML(groups []*IAMGroup) string {
	var b strings.Builder
	b.WriteString("<Groups>")
	for _, g := range groups {
		b.WriteString("<member>")
		b.WriteString(iamGroupXMLFields(g))
		b.WriteString("</member>")
	}
	b.WriteString("</Groups>")
	return b.String()
}

// iamPolicyXMLFields returns XML element content for an IAMPolicy.
//
// `IsAttachable` is rendered here, so it reaches both policy shapes: AWS's `Policy` type
// documents it with no operation restriction, and `ListPolicies`' sample response renders
// it on every member (#807). Substrate has stored it since `CreatePolicy` and simply never
// reported it, so a consumer could not tell an attachable policy from an unattachable one.
//
// It is written unconditionally, not omitted when false. The member is `Required: No` on
// the type, but the value is a state a policy always has, and `false` is meaningful rather
// than absent — while an omitted boolean decodes to the same `false` in every SDK, so a
// consumer cannot distinguish "not attachable" from "not reported".
//
// `PermissionsBoundaryUsageCount` is documented on the same type with no carve-out either,
// and `ListPolicies`' sample renders it on every member, so it reaches both shapes from here
// too (#815). It arrives as an argument rather than off `p`, because substrate stores a
// boundary as an ARN on the entity and not as a back-reference on the policy: the count is
// derived from a single scan of the users and roles, hoisted to one per request by the
// caller — see [IAMPlugin.iamBoundaryUsageCounts] for why deriving it beats keeping a
// counter, and for the cost. Rendered unconditionally for the reason `IsAttachable` is: a
// policy no entity uses as a boundary has a count, and it is zero.
//
// `Description` is *not* rendered here; it is single-entity-only, via
// [iamPolicyDescriptionXML].
func iamPolicyXMLFields(p *IAMPolicy, boundaryUsage int) string {
	var b strings.Builder
	b.WriteString("<PolicyId>")
	b.WriteString(xmlEsc(p.PolicyID))
	b.WriteString("</PolicyId><PolicyName>")
	b.WriteString(xmlEsc(p.PolicyName))
	b.WriteString("</PolicyName><Arn>")
	b.WriteString(xmlEsc(p.ARN))
	b.WriteString("</Arn><Path>")
	b.WriteString(xmlEsc(p.Path))
	b.WriteString("</Path>")
	if p.DefaultVersionID != "" {
		b.WriteString("<DefaultVersionId>")
		b.WriteString(xmlEsc(p.DefaultVersionID))
		b.WriteString("</DefaultVersionId>")
	}
	fmt.Fprintf(&b, "<AttachmentCount>%d</AttachmentCount>", p.AttachmentCount)
	fmt.Fprintf(&b, "<PermissionsBoundaryUsageCount>%d</PermissionsBoundaryUsageCount>", boundaryUsage)
	b.WriteString("<IsAttachable>")
	b.WriteString(iamBoolXML(p.IsAttachable))
	b.WriteString("</IsAttachable><CreateDate>")
	b.WriteString(p.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")
	if !p.UpdateDate.IsZero() {
		b.WriteString("<UpdateDate>")
		b.WriteString(p.UpdateDate.UTC().Format("2006-01-02T15:04:05Z"))
		b.WriteString("</UpdateDate>")
	}
	return b.String()
}

// iamPolicyDescriptionXML renders a policy's description for a single-entity shape, and
// nothing at all when it has none.
//
// Single-entity-only, and here AWS says so about this member specifically rather than
// leaving it to the listing note — from the `Policy` data type (#807):
//
//	Description — A friendly description of the policy. This element is included in the
//	response to the GetPolicy operation. It is not included in the response to the
//	ListPolicies operation.
//
// which `ListPolicies`' sample confirms by rendering no `Description` on any member.
// Substrate has stored the value since `CreatePolicy` and never reported it, so a consumer
// setting a description could not read it back at all.
//
// Omitted when empty, since `Required: No` and an empty element would report a policy
// described as "" where AWS reports no description.
func iamPolicyDescriptionXML(description string) string {
	if description == "" {
		return ""
	}
	return "<Description>" + xmlEsc(description) + "</Description>"
}

// iamSinglePolicyXML wraps policy fields in a <Policy> element.
//
// boundaryUsage is this policy's `PermissionsBoundaryUsageCount`.
func iamSinglePolicyXML(p *IAMPolicy, boundaryUsage int) string {
	return "<Policy>" + iamPolicyXMLFields(p, boundaryUsage) + iamPolicyDescriptionXML(p.Description) +
		iamEntityTagsXML(p.Tags) + "</Policy>"
}

// iamPolicyListXML builds <Policies> containing <member> elements.
//
// boundaryUsage is `PermissionsBoundaryUsageCount` keyed by policy ARN, from one scan for the
// whole listing rather than one per member; a policy no entity uses as a boundary is absent
// from it and reports zero.
func iamPolicyListXML(policies []*IAMPolicy, boundaryUsage map[string]int) string {
	var b strings.Builder
	b.WriteString("<Policies>")
	for _, p := range policies {
		b.WriteString("<member>")
		b.WriteString(iamPolicyXMLFields(p, boundaryUsage[p.ARN]))
		b.WriteString("</member>")
	}
	b.WriteString("</Policies>")
	return b.String()
}

// iamAccessKeyXML returns a <AccessKey> element, optionally including the secret.
func iamAccessKeyXML(k *IAMAccessKey, includeSecret bool) string {
	var b strings.Builder
	b.WriteString("<AccessKey><UserName>")
	b.WriteString(xmlEsc(k.UserName))
	b.WriteString("</UserName><AccessKeyId>")
	b.WriteString(xmlEsc(k.AccessKeyID))
	b.WriteString("</AccessKeyId>")
	if includeSecret {
		b.WriteString("<SecretAccessKey>")
		b.WriteString(xmlEsc(k.SecretAccessKey))
		b.WriteString("</SecretAccessKey>")
	}
	b.WriteString("<Status>")
	b.WriteString(xmlEsc(k.Status))
	b.WriteString("</Status><CreateDate>")
	b.WriteString(k.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate></AccessKey>")
	return b.String()
}

// iamAccessKeyMetaListXML builds <AccessKeyMetadata> containing <member> elements
// (without SecretAccessKey).
func iamAccessKeyMetaListXML(keys []map[string]interface{}) string {
	var b strings.Builder
	b.WriteString("<AccessKeyMetadata>")
	for _, k := range keys {
		b.WriteString("<member>")
		if id, ok := k["AccessKeyId"].(string); ok {
			b.WriteString("<AccessKeyId>")
			b.WriteString(xmlEsc(id))
			b.WriteString("</AccessKeyId>")
		}
		if un, ok := k["UserName"].(string); ok {
			b.WriteString("<UserName>")
			b.WriteString(xmlEsc(un))
			b.WriteString("</UserName>")
		}
		if st, ok := k["Status"].(string); ok {
			b.WriteString("<Status>")
			b.WriteString(xmlEsc(st))
			b.WriteString("</Status>")
		}
		b.WriteString("</member>")
	}
	b.WriteString("</AccessKeyMetadata>")
	return b.String()
}

// iamAttachedPoliciesXML builds <AttachedPolicies> containing <member> elements.
func iamAttachedPoliciesXML(policies []IAMAttachedPolicy) string {
	var b strings.Builder
	b.WriteString("<AttachedPolicies>")
	for _, p := range policies {
		b.WriteString("<member><PolicyName>")
		b.WriteString(xmlEsc(p.PolicyName))
		b.WriteString("</PolicyName><PolicyArn>")
		b.WriteString(xmlEsc(p.PolicyARN))
		b.WriteString("</PolicyArn></member>")
	}
	b.WriteString("</AttachedPolicies>")
	return b.String()
}

// iamTagListXML builds a <Tags> list containing <member> elements.
func iamTagListXML(tags []IAMTag) string {
	var b strings.Builder
	b.WriteString("<Tags>")
	for _, t := range tags {
		b.WriteString("<member><Key>")
		b.WriteString(xmlEsc(t.Key))
		b.WriteString("</Key><Value>")
		b.WriteString(xmlEsc(t.Value))
		b.WriteString("</Value></member>")
	}
	b.WriteString("</Tags>")
	return b.String()
}

// iamStringListXML builds a named XML list element containing <member> text elements.
// Used for PolicyNames lists.
func iamStringListXML(wrapper string, items []string) string {
	var b strings.Builder
	b.WriteString("<")
	b.WriteString(wrapper)
	b.WriteString(">")
	for _, s := range items {
		b.WriteString("<member>")
		b.WriteString(xmlEsc(s))
		b.WriteString("</member>")
	}
	b.WriteString("</")
	b.WriteString(wrapper)
	b.WriteString(">")
	return b.String()
}

// iamInstanceProfileXMLFields returns XML fields for an IAMInstanceProfile.
func iamInstanceProfileXMLFields(p *IAMInstanceProfile) string {
	var b strings.Builder
	b.WriteString("<InstanceProfileId>")
	b.WriteString(xmlEsc(p.InstanceProfileID))
	b.WriteString("</InstanceProfileId><InstanceProfileName>")
	b.WriteString(xmlEsc(p.InstanceProfileName))
	b.WriteString("</InstanceProfileName><Arn>")
	b.WriteString(xmlEsc(p.ARN))
	b.WriteString("</Arn><Path>")
	b.WriteString(xmlEsc(p.Path))
	b.WriteString("</Path><CreateDate>")
	b.WriteString(p.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")
	b.WriteString(iamRoleMembersXML(p.Roles))
	return b.String()
}

// iamSingleInstanceProfileXML wraps instance profile fields in <InstanceProfile>.
func iamSingleInstanceProfileXML(p *IAMInstanceProfile) string {
	return "<InstanceProfile>" + iamInstanceProfileXMLFields(p) +
		iamEntityTagsXML(p.Tags) + "</InstanceProfile>"
}

// iamInstanceProfileListXML builds <InstanceProfiles> containing <member> elements.
func iamInstanceProfileListXML(profiles []IAMInstanceProfile) string {
	var b strings.Builder
	b.WriteString("<InstanceProfiles>")
	for i := range profiles {
		b.WriteString("<member>")
		b.WriteString(iamInstanceProfileXMLFields(&profiles[i]))
		b.WriteString("</member>")
	}
	b.WriteString("</InstanceProfiles>")
	return b.String()
}

// iamBoolXML returns "true" or "false" for use in an XML element.
func iamBoolXML(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
