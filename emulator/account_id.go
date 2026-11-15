package emulator

// isAccountID reports whether s is a 12-digit AWS account ID.
//
// Every service that constrains an account-ID member publishes the same pattern for it —
// `^[0-9]{12}$` (ECR's registryId, QuickSight's AwsAccountId), `^\d{12}$` (Account and Organizations'
// AccountId) — so one check serves them all. Four copies of it had grown up, two as regular
// expressions and two as loops; this is the one they now call. It compares bytes rather than runes, so
// a non-ASCII digit is not a digit.
func isAccountID(s string) bool {
	if len(s) != 12 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
