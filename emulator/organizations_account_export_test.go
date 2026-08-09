package emulator

// This file exports what the account-vending tests need. It is separate from
// organizations_export_test.go so the vending lane does not contend with the
// other operation clusters for one file.

// CreateAccountState values, exported so a test pins each against the model's
// CreateAccountState enum rather than against a string literal.
const (
	OrgCreateStateInProgressForTest = orgCreateStateInProgress
	OrgCreateStateSucceededForTest  = orgCreateStateSucceeded
	OrgCreateStateFailedForTest     = orgCreateStateFailed
)

// OrgCreatePendingPrefixForTest is the state-key prefix under which the terminal
// outcome of an in-flight CreateAccount request is recorded, so a fault test can
// scope a store failure to exactly that record.
const OrgCreatePendingPrefixForTest = "car_pending:"

// OrgOUNamesRootForTest wraps orgOUNamesRoot for external tests.
func OrgOUNamesRootForTest(ouID, rootID string) bool { return orgOUNamesRoot(ouID, rootID) }

// IsOrgParentIDForTest wraps isOrgParentID for external tests.
func IsOrgParentIDForTest(id string) bool { return isOrgParentID(id) }
