package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestRoute53_RRSetPathAcceptsTrailingSlash pins that both spellings of the record-set
// path resolve: botocore sends ChangeResourceRecordSets to .../rrset/ (#1559).
func TestRoute53_RRSetPathAcceptsTrailingSlash(t *testing.T) {
	cases := []struct {
		method, path, wantOp string
	}{
		{"POST", "/2013-04-01/hostedzone/Z1/rrset", "ChangeResourceRecordSets"},
		{"POST", "/2013-04-01/hostedzone/Z1/rrset/", "ChangeResourceRecordSets"},
		{"GET", "/2013-04-01/hostedzone/Z1/rrset", "ListResourceRecordSets"},
		{"GET", "/2013-04-01/hostedzone/Z1/rrset/", "ListResourceRecordSets"},
	}
	for _, tc := range cases {
		op, zone := emulator.ParseRoute53OperationForTest(tc.method, tc.path)
		assert.Equal(t, tc.wantOp, op, tc.path)
		assert.Equal(t, "Z1", zone, tc.path)
	}
}
