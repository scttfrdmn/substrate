package emulator

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// IAMEffectAllow is the Allow effect constant for IAM policy statements.
const IAMEffectAllow = "Allow"

// IAMEffectDeny is the Deny effect constant for IAM policy statements.
const IAMEffectDeny = "Deny"

// IAMUser represents an AWS IAM user entity.
type IAMUser struct {
	UserName            string             `json:"UserName"`
	UserID              string             `json:"UserId"`
	ARN                 string             `json:"Arn"`
	Path                string             `json:"Path"`
	CreateDate          time.Time          `json:"CreateDate"`
	PasswordLastUsed    *time.Time         `json:"PasswordLastUsed,omitempty"`
	Tags                []IAMTag           `json:"Tags,omitempty"`
	PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`

	// EverTagged records that this user has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// IAMRole represents an AWS IAM role entity.
type IAMRole struct {
	RoleName                 string             `json:"RoleName"`
	RoleID                   string             `json:"RoleId"`
	ARN                      string             `json:"Arn"`
	Path                     string             `json:"Path"`
	CreateDate               time.Time          `json:"CreateDate"`
	Description              string             `json:"Description,omitempty"`
	MaxSessionDuration       int                `json:"MaxSessionDuration,omitempty"`
	AssumeRolePolicyDocument PolicyDocument     `json:"AssumeRolePolicyDocument"`
	Tags                     []IAMTag           `json:"Tags,omitempty"`
	PermissionsBoundary      *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`

	// RoleLastUsed records the last time the role was assumed, written by
	// [STSPlugin.assumeRole] and reported by GetRole (#816).
	//
	// A pointer, and omitted when nil, because that is how a role that has never been
	// assumed is represented: both members of AWS's `RoleLastUsed` are `Required: No`,
	// so there is no value that means "never". A record written before #816 reads back
	// with it nil, which is the same thing a never-assumed role is.
	RoleLastUsed *IAMRoleLastUsed `json:"RoleLastUsed,omitempty"`

	// EverTagged records that this role has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// IAMRoleLastUsed records when and where an IAM role was last assumed.
//
// It is AWS's `RoleLastUsed` structure, which that type's reference describes as
// containing "information about the last time that an IAM role was used. This includes
// the date and time and the Region in which the role was last used." Real IAM reports
// only the trailing 400 days of activity; substrate keeps whatever the run recorded,
// since a run is not 400 days long.
type IAMRoleLastUsed struct {
	// LastUsedDate is the simulated time at which the role was last assumed.
	LastUsedDate time.Time `json:"LastUsedDate"`

	// Region is the region of the AssumeRole request that last used the role — AWS
	// documents it as "the name of the AWS Region in which the role was last used", so
	// it is the request's region and not the emulator's configured one.
	Region string `json:"Region"`
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

	// Tags are the policy's resource tags, which AWS's `Policy` data type documents as
	// `Tags.member.N`, max 50, `Required: No`. A record written before #796 has none, which
	// reads back as an untagged policy — the same thing an untagged policy is.
	Tags []IAMTag `json:"Tags,omitempty"`
}

// IAMAccessKey represents an AWS IAM access key credential.
type IAMAccessKey struct {
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	Status          string    `json:"Status"`
	UserName        string    `json:"UserName"`
	CreateDate      time.Time `json:"CreateDate"`

	// AccountID is the account owning the user this key belongs to.
	//
	// Every other IAM record carries its account in the state key, but an access key
	// cannot: its ID is what *determines* the account, so [resolvePrincipal] looks the
	// record up before any account is known (#737). The account is a field here
	// instead. It is absent from the wire in both directions — AWS's CreateAccessKey
	// and ListAccessKeys responses publish no account member — so it is stored and
	// never rendered. A record written before #737 has it empty, and the reader falls
	// back to the account the request resolved to.
	AccountID string `json:"AccountId,omitempty"`

	// UserID is the `AIDA…` unique ID of the user this key belongs to.
	//
	// AWS publishes it as `aws:userid` for an IAM user, so [resolvePrincipal] needs it
	// on every signed request; copying it here when the key is created is what keeps
	// that lookup at the one Get the record itself costs. Like AccountID it is absent
	// from the wire in both directions — neither CreateAccessKey nor ListAccessKeys
	// publishes a user ID — so it is stored and never rendered. A record written
	// before #771 has it empty, and no `aws:userid` is published for that caller.
	UserID string `json:"UserId,omitempty"`
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
// AWS policy fields Action, Resource, NotAction, and NotResource accept both forms,
// as does a condition value list.
type StringOrSlice []string

// UnmarshalJSON implements json.Unmarshaler for StringOrSlice.
//
// Numbers and Booleans are read as their text. The IAM grammar says so of every value
// in a policy — "Values are enclosed in quotation marks. Quotation marks are optional
// for numeric and Boolean values" — and a document substrate refused for writing them
// unquoted is one real IAM accepts: `{"Bool": {"aws:SecureTransport": false}}` and
// `{"NumericLessThanEquals": {"s3:max-keys": 10}}` are both legal, and both answered
// MalformedPolicyDocument here before #714.
//
// A number keeps the spelling the document used, through [json.Number], rather than
// being routed through a float: 10 must not become "1e+01" on its way to a comparison,
// and a value too large for a float64 must not be silently rounded before
// [condParseNumber] ever sees it.
//
// The tolerance reaches Action and Resource too, since they share this type, and there
// a number is nonsense rather than shorthand. It is left in rather than scoped to
// condition values because the effect is bounded and lies in the safe direction: an
// `"Action": 5` becomes the action name "5", which matches no action, so the statement
// grants nothing instead of being rejected.
func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("unmarshal StringOrSlice: %w", err)
	}
	if list, ok := raw.([]any); ok {
		out := make(StringOrSlice, 0, len(list))
		for _, elem := range list {
			text, ok := jsonScalarText(elem)
			if !ok {
				return fmt.Errorf("unmarshal StringOrSlice: element %v is not a string, number or boolean", elem)
			}
			out = append(out, text)
		}
		*s = out
		return nil
	}
	text, ok := jsonScalarText(raw)
	if !ok {
		return fmt.Errorf("unmarshal StringOrSlice: %v is not a string, number or boolean", raw)
	}
	*s = StringOrSlice{text}
	return nil
}

// jsonScalarText renders a decoded JSON scalar as the text a policy comparison uses,
// reporting false for null, an object, or a nested array — none of which the grammar
// admits where a string, a number or a Boolean is allowed.
func jsonScalarText(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case bool:
		return strconv.FormatBool(v), true
	}
	return "", false
}

// MarshalJSON implements json.Marshaler for StringOrSlice.
// Single-element slices are marshaled as a plain string for compactness.
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

// iamInstanceProfileARN returns the ARN for an IAM instance profile.
//
// It joins the other four late, because until #770 the only place an instance-profile ARN
// was built was CreateInstanceProfile's own response, which interpolated the request's path
// directly and so skipped [normalisePath]. That was nearly harmless there — the handler
// defaults an absent Path to "/", and substrate does not validate the parameter, so only a
// path AWS would itself have rejected reached the interpolation. It is not harmless in an
// authorization resolver, which reads paths back out of records written by earlier versions:
// one missing slash is the difference between matching a statement scoped to
// `instance-profile/*` and matching nothing.
func iamInstanceProfileARN(accountID, path, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:instance-profile%s%s", accountID, normalisePath(path), name)
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

// IAMInstanceProfile represents an AWS IAM instance profile — a container
// that passes an IAM role to an EC2 instance.
type IAMInstanceProfile struct {
	// InstanceProfileName is the user-supplied name of the instance profile.
	InstanceProfileName string `json:"InstanceProfileName"`

	// InstanceProfileID is the AWS-generated unique identifier.
	InstanceProfileID string `json:"InstanceProfileId"`

	// ARN is the Amazon Resource Name for the instance profile.
	ARN string `json:"Arn"`

	// Path is the IAM path for the instance profile.
	Path string `json:"Path"`

	// Roles holds the IAM role attached to this profile (at most one per AWS rules).
	Roles []IAMRole `json:"Roles"`

	// CreateDate is when the instance profile was created.
	CreateDate time.Time `json:"CreateDate"`

	// Tags are the profile's resource tags, which AWS's `InstanceProfile` data type
	// documents as `Tags.member.N`, max 50, `Required: No`. A record written before #796 has
	// none, which reads back as an untagged profile — the same thing an untagged profile is.
	Tags []IAMTag `json:"Tags,omitempty"`
}
