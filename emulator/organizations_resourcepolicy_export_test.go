package emulator

// This file exports the resource-policy cluster for the external test package,
// following the per-cluster convention the other Organizations lanes use.

// Resource policy content bounds, exported so a test can pin each against its
// documented value. The maximum in particular is a boundary an off-by-one hides
// in a nominal run.
const (
	OrgMinResourcePolicyCharsForTest = orgMinResourcePolicyChars
	OrgMaxResourcePolicyCharsForTest = orgMaxResourcePolicyChars
)

// OrgKindResourcePolicyForTest is the kind resolveOrgTarget names for the
// organization's resource policy.
const OrgKindResourcePolicyForTest = orgKindResourcePolicy
