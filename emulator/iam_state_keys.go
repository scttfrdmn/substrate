package emulator

// IAM state keys are account-scoped: every key reads `<kind>:<account>/<rest>`,
// which is the shape DynamoDB (`table:<account>/<name>`) and every other
// account-aware plugin already uses, with the account after the kind so a
// `List(iamNamespace, iam…Prefix(account))` is a single-shot per-account scan.
//
// Until #737 the account was absent, so an IAM entity belonged to the emulator
// rather than to an account: two accounts could not both hold a role called
// "deploy" (the second create answered EntityAlreadyExists), a listing returned
// every account's entities, and resolveIAMEntity attached one account's policies
// to another account's principal of the same name.
//
// The builders below are the only place these strings are spelled. They exist
// because ~90 call sites used to concatenate them inline, where a missed one is
// invisible: it still compiles, still reads and writes state, and simply addresses
// a key nothing else uses.

// iamStateKey returns the state key for a record of the given kind in one account.
//
// The separator between the account and the name is "/" rather than ":" so a key
// stays readable — `user:111122223333/alice` — and so the composed keys below can
// keep ":" for the boundary they already used before the account existed.
func iamStateKey(kind, accountID, name string) string {
	return kind + ":" + accountID + "/" + name
}

// iamStatePrefix returns the List prefix that reaches every record of the given
// kind in one account, and nothing outside it.
func iamStatePrefix(kind, accountID string) string {
	return kind + ":" + accountID + "/"
}

// iamUserKey returns the state key for an IAM user.
func iamUserKey(accountID, userName string) string {
	return iamStateKey("user", accountID, userName)
}

// iamUserPrefix returns the List prefix reaching every user in one account.
func iamUserPrefix(accountID string) string { return iamStatePrefix("user", accountID) }

// iamRoleKey returns the state key for an IAM role.
func iamRoleKey(accountID, roleName string) string {
	return iamStateKey("role", accountID, roleName)
}

// iamRolePrefix returns the List prefix reaching every role in one account.
func iamRolePrefix(accountID string) string { return iamStatePrefix("role", accountID) }

// iamGroupKey returns the state key for an IAM group.
func iamGroupKey(accountID, groupName string) string {
	return iamStateKey("group", accountID, groupName)
}

// iamGroupPrefix returns the List prefix reaching every group in one account.
func iamGroupPrefix(accountID string) string { return iamStatePrefix("group", accountID) }

// iamEntityKey returns the state key for a policy-holding entity of the given kind
// — "user", "role" or "group" — which is how the operations that take an entity
// type as a parameter (PutUserPolicy and its role and group siblings) address it.
func iamEntityKey(accountID, kind, name string) string {
	return iamStateKey(kind, accountID, name)
}

// iamInstanceProfileKey returns the state key for an instance profile.
func iamInstanceProfileKey(accountID, name string) string {
	return iamStateKey("instance_profile", accountID, name)
}

// iamInstanceProfilePrefix returns the List prefix reaching every instance profile
// in one account.
func iamInstanceProfilePrefix(accountID string) string {
	return iamStatePrefix("instance_profile", accountID)
}

// iamPolicyKey returns the state key for a customer-managed policy, taking the
// account from the ARN.
//
// The account is in the ARN already, so the key does carry it twice. That is
// deliberate: a caller holding only an attachment's ARN — every policy read in the
// authorization path is one — can address the record without knowing whose request
// it is, while ListPolicies still reaches exactly one account's policies with a
// single prefix. IAM does not let one account attach another's customer-managed
// policy, so the ARN's account is the owning account by construction.
//
// A bundled AWS-managed ARN yields "policy:aws/arn:aws:iam::aws:policy/…", a key
// nothing writes: the catalog is consulted before state everywhere, and a policy
// under "::aws:" is never stored.
func iamPolicyKey(arn string) string {
	return iamStateKey("policy", arnAccountID(arn), arn)
}

// iamPolicyPrefix returns the List prefix reaching every customer-managed policy in
// one account.
func iamPolicyPrefix(accountID string) string { return iamStatePrefix("policy", accountID) }

// iamAttachedPoliciesKey returns the state key holding the ARNs of the managed
// policies attached to one entity — "<kind>_policies:<account>/<name>".
func iamAttachedPoliciesKey(accountID, kind, name string) string {
	return iamStateKey(kind+"_policies", accountID, name)
}

// iamAttachedPoliciesPrefix returns the List prefix reaching every attachment list
// of the given entity kind in one account.
func iamAttachedPoliciesPrefix(accountID, kind string) string {
	return iamStatePrefix(kind+"_policies", accountID)
}

// iamInlinePolicyKey returns the state key holding one inline policy document.
//
// The policy name keeps the ":" boundary it had before the account was part of the
// key. Neither an entity name nor a policy name may contain ":" or "/", so the key
// is unambiguous either way.
func iamInlinePolicyKey(accountID, kind, entityName, policyName string) string {
	return iamStateKey(kind+"_inline", accountID, entityName+":"+policyName)
}

// iamInlinePolicyNamesKey returns the state key holding the names of one entity's
// inline policies.
func iamInlinePolicyNamesKey(accountID, kind, entityName string) string {
	return iamStateKey(kind+"_inline_names", accountID, entityName)
}

// iamUserAccessKeysKey returns the state key holding the IDs of one user's access
// keys.
func iamUserAccessKeysKey(accountID, userName string) string {
	return iamStateKey("user_accesskeys", accountID, userName)
}

// iamAccessKeyKey returns the state key for an access key record.
//
// This is the one IAM key with no account in it, and it cannot have one: an access
// key ID is what *determines* the account, so [resolvePrincipal] looks a key up
// before any account is known. The owning account is a field on the record
// ([IAMAccessKey.AccountID]) instead, which is also why #737 is a schema change
// here rather than a key rename. Access key IDs are unique across accounts, so
// nothing collides.
func iamAccessKeyKey(accessKeyID string) string {
	return "accesskey:" + accessKeyID
}

// iamGroupUsersKey returns the state key holding the user names in one group.
func iamGroupUsersKey(accountID, groupName string) string {
	return iamStateKey("group_users", accountID, groupName)
}

// iamUserGroupsKey returns the state key holding the group names one user belongs to.
func iamUserGroupsKey(accountID, userName string) string {
	return iamStateKey("user_groups", accountID, userName)
}

// iamSLRDeletionTaskKey returns the state key for a service-linked-role deletion
// task, whose ID is what GetServiceLinkedRoleDeletionStatus is given.
//
// The task ID contains "/" — AWS's format is
// `task/aws-service-role/<principal>/<role>/<uuid>` — which is harmless here: nothing
// prefix-scans within a task ID, and the account still delimits the scan that reaches
// one account's tasks.
func iamSLRDeletionTaskKey(accountID, taskID string) string {
	return iamStateKey("slr_deletion_task", accountID, taskID)
}

// iamSLRDeletionTaskPrefix returns the List prefix reaching every service-linked-role
// deletion task in one account.
func iamSLRDeletionTaskPrefix(accountID string) string {
	return iamStatePrefix("slr_deletion_task", accountID)
}

// iamGroupPoliciesKey returns the state key holding the managed policy ARNs
// attached to one group.
func iamGroupPoliciesKey(accountID, groupName string) string {
	return iamAttachedPoliciesKey(accountID, "group", groupName)
}
