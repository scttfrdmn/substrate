package substrate

import (
	"bytes"
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
	if u.PermissionsBoundary != nil {
		b.WriteString("<PermissionsBoundary><PolicyArn>")
		b.WriteString(xmlEsc(u.PermissionsBoundary.PolicyARN))
		b.WriteString("</PolicyArn><PolicyName>")
		b.WriteString(xmlEsc(u.PermissionsBoundary.PolicyName))
		b.WriteString("</PolicyName></PermissionsBoundary>")
	}
	return b.String()
}

// iamSingleUserXML wraps user fields in a <User> element.
func iamSingleUserXML(u *IAMUser) string {
	return "<User>" + iamUserXMLFields(u) + "</User>"
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
	if r.PermissionsBoundary != nil {
		b.WriteString("<PermissionsBoundary><PolicyArn>")
		b.WriteString(xmlEsc(r.PermissionsBoundary.PolicyARN))
		b.WriteString("</PolicyArn><PolicyName>")
		b.WriteString(xmlEsc(r.PermissionsBoundary.PolicyName))
		b.WriteString("</PolicyName></PermissionsBoundary>")
	}
	return b.String()
}

// iamSingleRoleXML wraps role fields in a <Role> element.
func iamSingleRoleXML(r *IAMRole) string {
	return "<Role>" + iamRoleXMLFields(r) + "</Role>"
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
func iamPolicyXMLFields(p *IAMPolicy) string {
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
	b.WriteString("<CreateDate>")
	b.WriteString(p.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")
	if !p.UpdateDate.IsZero() {
		b.WriteString("<UpdateDate>")
		b.WriteString(p.UpdateDate.UTC().Format("2006-01-02T15:04:05Z"))
		b.WriteString("</UpdateDate>")
	}
	return b.String()
}

// iamSinglePolicyXML wraps policy fields in a <Policy> element.
func iamSinglePolicyXML(p *IAMPolicy) string {
	return "<Policy>" + iamPolicyXMLFields(p) + "</Policy>"
}

// iamPolicyListXML builds <Policies> containing <member> elements.
func iamPolicyListXML(policies []*IAMPolicy) string {
	var b strings.Builder
	b.WriteString("<Policies>")
	for _, p := range policies {
		b.WriteString("<member>")
		b.WriteString(iamPolicyXMLFields(p))
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
	return "<InstanceProfile>" + iamInstanceProfileXMLFields(p) + "</InstanceProfile>"
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
