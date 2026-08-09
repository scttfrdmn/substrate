package emulator

// This file exports the policy lifecycle's internals for the external test
// package. It is separate from organizations_export_test.go so the policy cluster
// can add what it needs without contending for a shared file, and it is compiled
// only when running tests.

// IsOrgPolicyIDSyntaxForTest wraps isOrgPolicyIDSyntax for external tests.
func IsOrgPolicyIDSyntaxForTest(id string) bool { return isOrgPolicyIDSyntax(id) }

// IsOrgTargetIDSyntaxForTest wraps isOrgTargetIDSyntax for external tests.
func IsOrgTargetIDSyntaxForTest(id string) bool { return isOrgTargetIDSyntax(id) }

// OrgPolicyNumberLimitExceededForTest wraps orgPolicyNumberLimitExceeded for
// external tests, so the quota boundary can be pinned without creating
// orgMaxSCPsPerOrg policies.
func OrgPolicyNumberLimitExceededForTest(count int) bool { return orgPolicyNumberLimitExceeded(count) }

// OrgMaxPolicyNameCharsForTest is the PolicyName shape's maximum length.
const OrgMaxPolicyNameCharsForTest = orgMaxPolicyNameChars

// OrgMaxPolicyDescriptionCharsForTest is the PolicyDescription shape's maximum
// length.
const OrgMaxPolicyDescriptionCharsForTest = orgMaxPolicyDescriptionChars

// OrgPolicyIDSuffixLenForTest is the length of the generated part of a policy ID.
const OrgPolicyIDSuffixLenForTest = orgPolicyIDSuffixLen
