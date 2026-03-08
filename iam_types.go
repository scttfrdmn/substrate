package substrate

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// IAMEffectAllow is the Allow effect constant for IAM policy statements.
const IAMEffectAllow = "Allow"

// IAMEffectDeny is the Deny effect constant for IAM policy statements.
const IAMEffectDeny = "Deny"

// IAMUser represents an AWS IAM user entity.
type IAMUser struct {
	UserName         string     `json:"UserName"`
	UserID           string     `json:"UserId"`
	ARN              string     `json:"Arn"`
	Path             string     `json:"Path"`
	CreateDate       time.Time  `json:"CreateDate"`
	PasswordLastUsed *time.Time `json:"PasswordLastUsed,omitempty"`
	Tags             []IAMTag   `json:"Tags,omitempty"`
}

// IAMRole represents an AWS IAM role entity.
type IAMRole struct {
	RoleName                 string         `json:"RoleName"`
	RoleID                   string         `json:"RoleId"`
	ARN                      string         `json:"Arn"`
	Path                     string         `json:"Path"`
	CreateDate               time.Time      `json:"CreateDate"`
	Description              string         `json:"Description,omitempty"`
	MaxSessionDuration       int            `json:"MaxSessionDuration,omitempty"`
	AssumeRolePolicyDocument PolicyDocument `json:"AssumeRolePolicyDocument"`
	Tags                     []IAMTag       `json:"Tags,omitempty"`
}

// IAMGroup represents an AWS IAM group entity.
type IAMGroup struct {
	GroupName  string    `json:"GroupName"`
	GroupID    string    `json:"GroupId"`
	ARN        string    `json:"Arn"`
	Path       string    `json:"Path"`
	CreateDate time.Time `json:"CreateDate"`
}

// IAMPolicy represents an AWS IAM managed or customer-managed policy entity.
type IAMPolicy struct {
	PolicyName       string         `json:"PolicyName"`
	PolicyID         string         `json:"PolicyId"`
	ARN              string         `json:"Arn"`
	Path             string         `json:"Path"`
	Description      string         `json:"Description,omitempty"`
	DefaultVersionID string         `json:"DefaultVersionId"`
	AttachmentCount  int            `json:"AttachmentCount"`
	IsAttachable     bool           `json:"IsAttachable"`
	CreateDate       time.Time      `json:"CreateDate"`
	UpdateDate       time.Time      `json:"UpdateDate"`
	Document         PolicyDocument `json:"Document,omitempty"`
}

// IAMAccessKey represents an AWS IAM access key credential.
type IAMAccessKey struct {
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	Status          string    `json:"Status"`
	UserName        string    `json:"UserName"`
	CreateDate      time.Time `json:"CreateDate"`
}

// IAMTag represents an AWS resource tag key-value pair.
type IAMTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// IAMAttachedPolicy represents a managed policy attached to an IAM entity.
type IAMAttachedPolicy struct {
	PolicyName string `json:"PolicyName"`
	PolicyARN  string `json:"PolicyArn"`
}

// PolicyDocument represents a parsed IAM policy document.
type PolicyDocument struct {
	Version   string            `json:"Version"`
	Statement []PolicyStatement `json:"Statement"`
}

// PolicyStatement represents a single statement within an IAM policy document.
type PolicyStatement struct {
	Sid          string                              `json:"Sid,omitempty"`
	Effect       string                              `json:"Effect"`
	Principal    *PolicyPrincipal                    `json:"Principal,omitempty"`
	NotPrincipal *PolicyPrincipal                    `json:"NotPrincipal,omitempty"`
	Action       StringOrSlice                       `json:"Action,omitempty"`
	NotAction    StringOrSlice                       `json:"NotAction,omitempty"`
	Resource     StringOrSlice                       `json:"Resource,omitempty"`
	NotResource  StringOrSlice                       `json:"NotResource,omitempty"`
	Condition    map[string]map[string]StringOrSlice `json:"Condition,omitempty"`
}

// PolicyPrincipal handles the Principal field in policy statements.
// It can be "*" (wildcard), a plain ARN string, or a typed map such as
// {"AWS": [...], "Service": [...]}.
type PolicyPrincipal struct {
	All       bool
	AWS       []string
	Service   []string
	Federated []string
}

// UnmarshalJSON implements json.Unmarshaler for PolicyPrincipal.
func (p *PolicyPrincipal) UnmarshalJSON(data []byte) error {
	// Try string: "*" or a plain ARN.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s == "*" {
			p.All = true
		} else {
			p.AWS = []string{s}
		}
		return nil
	}

	// Try typed map: {"AWS": [...], "Service": [...], ...}.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("unmarshal PolicyPrincipal: %w", err)
	}
	for k, v := range m {
		var vals StringOrSlice
		if err := json.Unmarshal(v, &vals); err != nil {
			return fmt.Errorf("unmarshal PolicyPrincipal %s: %w", k, err)
		}
		switch k {
		case "AWS":
			p.AWS = vals
		case "Service":
			p.Service = vals
		case "Federated":
			p.Federated = vals
		}
	}
	return nil
}

// MarshalJSON implements json.Marshaler for PolicyPrincipal.
func (p *PolicyPrincipal) MarshalJSON() ([]byte, error) {
	if p.All {
		return json.Marshal("*")
	}
	if len(p.Service) == 0 && len(p.Federated) == 0 {
		if len(p.AWS) == 1 {
			return json.Marshal(p.AWS[0])
		}
		return json.Marshal(p.AWS)
	}
	m := make(map[string][]string)
	if len(p.AWS) > 0 {
		m["AWS"] = p.AWS
	}
	if len(p.Service) > 0 {
		m["Service"] = p.Service
	}
	if len(p.Federated) > 0 {
		m["Federated"] = p.Federated
	}
	return json.Marshal(m)
}

// StringOrSlice unmarshals either a JSON string or an array of strings.
// AWS policy fields Action, Resource, NotAction, and NotResource accept both forms.
type StringOrSlice []string

// UnmarshalJSON implements json.Unmarshaler for StringOrSlice.
func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = StringOrSlice{str}
		return nil
	}
	var arr []string
	if err := json.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("unmarshal StringOrSlice: %w", err)
	}
	*s = StringOrSlice(arr)
	return nil
}

// MarshalJSON implements json.Marshaler for StringOrSlice.
// Single-element slices are marshalled as a plain string for compactness.
func (s StringOrSlice) MarshalJSON() ([]byte, error) {
	if len(s) == 1 {
		return json.Marshal(s[0])
	}
	return json.Marshal([]string(s))
}

// iamUserARN returns the ARN for an IAM user.
func iamUserARN(accountID, path, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:user%s%s", accountID, normalisePath(path), name)
}

// iamRoleARN returns the ARN for an IAM role.
func iamRoleARN(accountID, path, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:role%s%s", accountID, normalisePath(path), name)
}

// iamGroupARN returns the ARN for an IAM group.
func iamGroupARN(accountID, path, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:group%s%s", accountID, normalisePath(path), name)
}

// iamPolicyARN returns the ARN for a customer-managed IAM policy.
func iamPolicyARN(accountID, path, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:policy%s%s", accountID, normalisePath(path), name)
}

// normalisePath ensures a path starts and ends with "/".
func normalisePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

// iamIDChars is the character set used in IAM entity IDs.
const iamIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// generateIAMID generates a 21-character IAM entity ID with the given prefix.
// AWS prefixes: AIDA (user), AROA (role), AGPA (group), ANPA (policy), AKIA (access key).
func generateIAMID(prefix string) string {
	const totalLen = 21
	remaining := totalLen - len(prefix)
	raw := make([]byte, remaining)
	if _, err := cryptorand.Read(raw); err != nil {
		panic(fmt.Sprintf("generateIAMID: crypto/rand read: %v", err))
	}
	out := make([]byte, remaining)
	for i, b := range raw {
		out[i] = iamIDChars[int(b)%len(iamIDChars)]
	}
	return prefix + string(out)
}
