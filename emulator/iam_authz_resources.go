package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// iamResourceRef names the resource an IAM operation is about: the resource type AWS
// publishes for the action, and where on the request the resource's name is carried.
type iamResourceRef struct {
	// Type is the resource type AWS publishes for this action, spelled the way the Service
	// Reference Information endpoint spells it.
	//
	// It is what picks the ARN minter, and it is also the citation: because it uses AWS's
	// own vocabulary, a test can ask [authzActionSupportsResourceType] whether the pairing
	// is one AWS documents, for every row, rather than a comment claiming it is.
	//
	// Empty for a row whose parameter carries a full ARN whose type varies —
	// SimulatePrincipalPolicy's PolicySourceArn may name a user, a role or a group — where
	// nothing has to be minted and no single type describes the row.
	Type string

	// NameParams are the request parameters that may carry the resource's name, tried in
	// order, first non-empty winning. A value that is already an ARN is used as it stands.
	//
	// It is a list rather than one name because a resource can be named two ways by the
	// same operation: CreatePolicy carries PolicyName, while every other policy operation
	// carries the finished PolicyArn.
	//
	// Empty for an operation that carries no name at all — see CallerIsResource.
	NameParams []string

	// CallerIsResource reports that an absent name means the calling user.
	//
	// AWS documents this per operation, not as a general rule: GetUser's UserName is
	// "optional. If it is not included, it defaults to the user making the request", the
	// three access-key operations say the same, and ChangePassword "changes the password of
	// the IAM user who is calling this operation" and carries no name at all. For those the
	// resource AWS decides against is the caller's own user, so answering
	// `arn:aws:iam::<acct>:*` instead would leave a statement scoped to
	// `user/${aws:username}` — the shape of every "let a user manage their own
	// credentials" policy, including the bundled IAMUserChangePassword — matching nothing.
	//
	// The name comes from [Principal.UserName], which is recorded when the credential is
	// minted (#745) rather than parsed out of the principal ARN, and which is empty for
	// every caller that is not an IAM user. A role has no password and no long-term keys
	// of its own, so leaving the resource unresolved for one is the honest answer rather
	// than a gap.
	CallerIsResource bool
}

// iamAuthzOperationResource maps an IAM operation onto the resource it names.
//
// This is the table #770 is about. Before it, [AuthController.buildResourceARN] answered
// `arn:aws:iam::<acct>:*` for every IAM operation — a literal `*` in the resource position
// that no statement naming a user, a role or a path can match, so a policy scoped to
// `arn:aws:iam::123456789012:user/alice` was inert and every "manage your own things"
// managed policy granted nothing.
//
// Membership is the claim, and it is checked rather than asserted: every row's Type is one
// AWS publishes for that action, and every action AWS publishes *no* resource types for is
// absent from this table. Both directions are covered by
// TestIAMAuthzOperationResource_EveryRowMatchesWhatAWSPublishes, reading the vendored
// Service Reference Information snapshot (#797) — so a row that drifts from AWS's own data
// fails the build rather than quietly deciding requests against the wrong resource.
//
// Where AWS lists several types for one action, the row names the one the action is
// *about*: AddUserToGroup lists only `group`, because the group is what a policy governs
// there even though the request also carries a user name; AttachUserPolicy lists only
// `user`, even though it carries a PolicyArn. Neither of those is substrate's choice — both
// are read off AWS's data.
//
// The three service-linked-role operations are deliberately absent. [iamAuthzResources]
// answers them first, through [iamAuthzSLRResourceARN], because their resource is derived
// from a service principal or a deletion-task ID rather than from a name on the wire
// (#747).
var iamAuthzOperationResource = map[string]iamResourceRef{
	// Users. Path is on the wire for the create and read from the record for the rest; see
	// [iamAuthzMintResourceARN].
	"CreateUser":                    {Type: "user", NameParams: []string{"UserName"}},
	"GetUser":                       {Type: "user", NameParams: []string{"UserName"}, CallerIsResource: true},
	"DeleteUser":                    {Type: "user", NameParams: []string{"UserName"}},
	"TagUser":                       {Type: "user", NameParams: []string{"UserName"}},
	"UntagUser":                     {Type: "user", NameParams: []string{"UserName"}},
	"ListUserTags":                  {Type: "user", NameParams: []string{"UserName"}},
	"AttachUserPolicy":              {Type: "user", NameParams: []string{"UserName"}},
	"DetachUserPolicy":              {Type: "user", NameParams: []string{"UserName"}},
	"ListAttachedUserPolicies":      {Type: "user", NameParams: []string{"UserName"}},
	"PutUserPolicy":                 {Type: "user", NameParams: []string{"UserName"}},
	"GetUserPolicy":                 {Type: "user", NameParams: []string{"UserName"}},
	"DeleteUserPolicy":              {Type: "user", NameParams: []string{"UserName"}},
	"ListUserPolicies":              {Type: "user", NameParams: []string{"UserName"}},
	"PutUserPermissionsBoundary":    {Type: "user", NameParams: []string{"UserName"}},
	"DeleteUserPermissionsBoundary": {Type: "user", NameParams: []string{"UserName"}},
	"ListGroupsForUser":             {Type: "user", NameParams: []string{"UserName"}},

	// The three access-key operations and ChangePassword name the caller's own user when
	// they carry no UserName, which is the common form for all four.
	"CreateAccessKey": {Type: "user", NameParams: []string{"UserName"}, CallerIsResource: true},
	"DeleteAccessKey": {Type: "user", NameParams: []string{"UserName"}, CallerIsResource: true},
	"ListAccessKeys":  {Type: "user", NameParams: []string{"UserName"}, CallerIsResource: true},
	// ChangePassword carries no name at all: OldPassword and NewPassword are its only
	// parameters. Substrate does not implement the operation, but the generic gate decides
	// every request whether a plugin handles it or not, and this row is what makes the
	// bundled IAMUserChangePassword grant what it says it grants (#770).
	"ChangePassword": {Type: "user", CallerIsResource: true},

	// Roles.
	"CreateRole":                    {Type: "role", NameParams: []string{"RoleName"}},
	"GetRole":                       {Type: "role", NameParams: []string{"RoleName"}},
	"DeleteRole":                    {Type: "role", NameParams: []string{"RoleName"}},
	"UpdateAssumeRolePolicy":        {Type: "role", NameParams: []string{"RoleName"}},
	"TagRole":                       {Type: "role", NameParams: []string{"RoleName"}},
	"UntagRole":                     {Type: "role", NameParams: []string{"RoleName"}},
	"ListRoleTags":                  {Type: "role", NameParams: []string{"RoleName"}},
	"AttachRolePolicy":              {Type: "role", NameParams: []string{"RoleName"}},
	"DetachRolePolicy":              {Type: "role", NameParams: []string{"RoleName"}},
	"ListAttachedRolePolicies":      {Type: "role", NameParams: []string{"RoleName"}},
	"PutRolePolicy":                 {Type: "role", NameParams: []string{"RoleName"}},
	"GetRolePolicy":                 {Type: "role", NameParams: []string{"RoleName"}},
	"DeleteRolePolicy":              {Type: "role", NameParams: []string{"RoleName"}},
	"ListRolePolicies":              {Type: "role", NameParams: []string{"RoleName"}},
	"PutRolePermissionsBoundary":    {Type: "role", NameParams: []string{"RoleName"}},
	"DeleteRolePermissionsBoundary": {Type: "role", NameParams: []string{"RoleName"}},

	// Groups. AddUserToGroup and RemoveUserFromGroup are here rather than under users
	// because that is where AWS puts them: both publish `group` and only `group`.
	"CreateGroup":               {Type: "group", NameParams: []string{"GroupName"}},
	"GetGroup":                  {Type: "group", NameParams: []string{"GroupName"}},
	"DeleteGroup":               {Type: "group", NameParams: []string{"GroupName"}},
	"AddUserToGroup":            {Type: "group", NameParams: []string{"GroupName"}},
	"RemoveUserFromGroup":       {Type: "group", NameParams: []string{"GroupName"}},
	"AttachGroupPolicy":         {Type: "group", NameParams: []string{"GroupName"}},
	"DetachGroupPolicy":         {Type: "group", NameParams: []string{"GroupName"}},
	"ListAttachedGroupPolicies": {Type: "group", NameParams: []string{"GroupName"}},
	"PutGroupPolicy":            {Type: "group", NameParams: []string{"GroupName"}},
	"GetGroupPolicy":            {Type: "group", NameParams: []string{"GroupName"}},
	"DeleteGroupPolicy":         {Type: "group", NameParams: []string{"GroupName"}},
	"ListGroupPolicies":         {Type: "group", NameParams: []string{"GroupName"}},

	// Policies. Only the create carries a name; the rest carry the finished ARN, which
	// costs no state read and no path lookup.
	"CreatePolicy":       {Type: "policy", NameParams: []string{"PolicyName"}},
	"GetPolicy":          {Type: "policy", NameParams: []string{"PolicyArn"}},
	"DeletePolicy":       {Type: "policy", NameParams: []string{"PolicyArn"}},
	"GetPolicyVersion":   {Type: "policy", NameParams: []string{"PolicyArn"}},
	"ListPolicyVersions": {Type: "policy", NameParams: []string{"PolicyArn"}},
	"TagPolicy":          {Type: "policy", NameParams: []string{"PolicyArn"}},
	"UntagPolicy":        {Type: "policy", NameParams: []string{"PolicyArn"}},
	"ListPolicyTags":     {Type: "policy", NameParams: []string{"PolicyArn"}},

	// Instance profiles. AddRoleToInstanceProfile carries a RoleName too, and AWS still
	// publishes only `instance-profile` for it — the profile is the resource a policy
	// governs, which matters because that operation is the classic privilege-escalation
	// step and had no plugin-side gate at all before #770.
	"CreateInstanceProfile":         {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"GetInstanceProfile":            {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"DeleteInstanceProfile":         {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"AddRoleToInstanceProfile":      {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"RemoveRoleFromInstanceProfile": {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"TagInstanceProfile":            {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"UntagInstanceProfile":          {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},
	"ListInstanceProfileTags":       {Type: "instance-profile", NameParams: []string{"InstanceProfileName"}},

	// PolicySourceArn is a finished ARN of whichever of the three entity types the caller
	// asked about, so there is nothing to mint and no one Type to name.
	"SimulatePrincipalPolicy": {NameParams: []string{"PolicySourceArn"}},
}

// iamAuthzRequestResource returns the resource ARN an IAM request is decided against.
//
// Both authorization doors call this, on the same [AWSRequest], which is what makes them
// agree by construction rather than by two sets of call sites happening to pass the same
// string. The plugin door used to pass a literal `"*"` at 48 sites and the generic gate
// answered `arn:aws:iam::<acct>:*` at one: two resources for one request, and a policy
// scoped to the account wildcard was therefore honored at one door and not the other even
// before #770's per-operation resource existed.
//
// The fallback is [iamAuthzAccountResourceARN] rather than a bare `"*"`, because a bare `*`
// in the *resource* position is what [resourceMatches] treats as matching every statement:
// harmless for an Allow and wrong for a Deny.
func iamAuthzRequestResource(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	if resources := iamAuthzResources(state, reqCtx, req); len(resources) == 1 {
		return resources[0].ARN
	}
	return iamAuthzAccountResourceARN(reqCtx.AccountID)
}

// iamAuthzAccountResourceARN is the resource an IAM request whose own resource substrate
// cannot resolve is decided against: every IAM resource in the request's account.
//
// It is one function rather than two string concatenations so that
// [AuthController.buildResourceARN]'s iam arm and [iamAuthzRequestResource]'s fallback
// cannot drift into two different wildcards — the failure this whole change is about,
// arrived at from the other side.
func iamAuthzAccountResourceARN(accountID string) string {
	return "arn:aws:iam::" + accountID + ":*"
}

// iamAuthzOperationResourceARN returns the ARN of the resource [iamAuthzOperationResource]
// says the operation names, or "" when the table does not cover it or the request does not
// name enough to resolve one.
//
// Returning "" rather than a guess is [iamAuthzSLRResourceARN]'s rule: leave the request on
// the general path rather than mint an ARN that only looks specific.
func iamAuthzOperationResourceARN(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	ref, ok := iamAuthzOperationResource[req.Operation]
	if !ok {
		return ""
	}

	name := ""
	for _, param := range ref.NameParams {
		if value := iamAuthzParam(req, param); value != "" {
			name = value
			break
		}
	}
	if name == "" {
		if !ref.CallerIsResource || reqCtx.Principal == nil {
			return ""
		}
		name = reqCtx.Principal.UserName
		if name == "" {
			return ""
		}
	}

	if strings.HasPrefix(name, "arn:") {
		// PolicyArn and PolicySourceArn arrive finished. Nothing to mint, no record to read
		// and no path to recover, so these rows cost nothing beyond the map lookup.
		return name
	}
	if ref.Type == "" {
		// A row with no Type expects an ARN and got a name. Nothing honest to build.
		return ""
	}
	return iamAuthzMintResourceARN(state, reqCtx, req, ref.Type, name)
}

// iamAuthzMintResourceARN builds the ARN for a named IAM entity, supplying the path the ARN
// needs but the request usually does not carry.
//
// An IAM ARN embeds the entity's path — AWS writes the placeholder as
// `${UserNameWithPath}` — and only the `Create*` operations put Path on the wire. So the
// path is read from the entity's own record, which costs one [StateManager.Get] per
// resource-naming IAM request at each door. That is the same read [iamAuthzRolePath] has
// always done for DeleteServiceLinkedRole, generalized; the alternative is an ARN with a
// `/` path that silently fails to match any statement written about a real path.
//
// When there is no record the request's own Path is used, which is right for a `Create*`
// and harmless otherwise: a name that names nothing has no path to be wrong about, and
// [normalisePath] turns the empty case into `/`. Falling back to the account wildcard
// instead would be the permissive direction for a Deny scoped to `user/*`, which is the
// one direction a privilege boundary must not drift in.
func iamAuthzMintResourceARN(state StateManager, reqCtx *RequestContext, req *AWSRequest,
	resourceType, name string) string {
	path := iamAuthzEntityPath(state, reqCtx.AccountID, resourceType, name)
	if path == "" {
		path = iamAuthzParam(req, "Path")
	}
	switch resourceType {
	case "user":
		return iamUserARN(reqCtx.AccountID, path, name)
	case "role":
		return iamRoleARN(reqCtx.AccountID, path, name)
	case "group":
		return iamGroupARN(reqCtx.AccountID, path, name)
	case "policy":
		return iamPolicyARN(reqCtx.AccountID, path, name)
	case "instance-profile":
		return iamInstanceProfileARN(reqCtx.AccountID, path, name)
	default:
		return ""
	}
}

// iamAuthzEntityPath returns the stored path of a named IAM entity, or "" when there is no
// such record and for a resource type whose records carry no path to read.
//
// A customer-managed policy is the second case: its record is keyed by the ARN the path is
// already part of, so there is nothing to look up by name — CreatePolicy's Path parameter
// is the only source, and it is on the wire.
//
// Like [iamAuthzRolePath], which now delegates here, it reads through the raw
// [StateManager] rather than through IAMPlugin: [AuthController] holds no plugin, and a
// decision must not depend on one being registered.
func iamAuthzEntityPath(state StateManager, accountID, resourceType, name string) string {
	if state == nil {
		return ""
	}
	kind := iamAuthzResourceStateKind(resourceType)
	if kind == "" {
		return ""
	}
	raw, err := state.Get(context.Background(), iamNamespace, iamEntityKey(accountID, kind, name))
	if err != nil || raw == nil {
		return ""
	}
	// Users, roles, groups and instance profiles all publish their path under the same
	// member, so one shape reads any of the four and none has to be unmarshalled in full.
	var stored struct {
		Path string `json:"Path"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return ""
	}
	return stored.Path
}

// iamAuthzResourceStateKind maps AWS's resource-type name onto the prefix substrate stores
// that entity's record under, or "" for a type whose path cannot be looked up by name.
//
// The two vocabularies agree for three of the four — `user`, `role` and `group` — and
// differ for the fourth, because AWS hyphenates `instance-profile` where
// [iamInstanceProfileKey] uses an underscore. Naming that difference in one place is what
// keeps the table above spelled AWS's way, which is what lets the citation test compare it
// against AWS's own data.
func iamAuthzResourceStateKind(resourceType string) string {
	switch resourceType {
	case "user", "role", "group":
		return resourceType
	case "instance-profile":
		return "instance_profile"
	default:
		return ""
	}
}
