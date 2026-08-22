package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DescribeSnapshotAttribute, ModifySnapshotAttribute and ResetSnapshotAttribute (#709).
//
// All three reached the dispatcher's default arm through v0.106.0, so every case here is a
// fail-before answering InvalidAction. None of the three pages publishes an operation-specific
// error, so each refusal below is substrate's reading of a prose rule; the test names say which
// rule, and the handler's comments say where it comes from.

// snapshotAttributeDoc is a DescribeSnapshotAttribute response.
//
// The two wrapper fields are pointers so the test can tell an *omitted* element from a
// present-but-empty one — the distinction #669 established and the whole reason the handler's
// own fields are pointers. A plain slice here would read both as an empty slice and the
// assertions below would pass against a handler that always emitted both elements.
type snapshotAttributeDoc struct {
	SnapshotID             string `xml:"snapshotId"`
	CreateVolumePermission *struct {
		Items []struct {
			UserID string `xml:"userId"`
			Group  string `xml:"group"`
		} `xml:"item"`
	} `xml:"createVolumePermission"`
	ProductCodes *struct {
		Items []struct {
			ProductCode string `xml:"productCode"`
			Type        string `xml:"type"`
		} `xml:"item"`
	} `xml:"productCodes"`
}

// ec2SnapshotAttribute sends DescribeSnapshotAttribute for one attribute and returns both the
// parsed document and the raw body.
//
// The raw body is returned because two of the assertions below are about an element that must
// *not* be there, and an absent element is invisible in a struct that does not declare it.
func ec2SnapshotAttribute(t *testing.T, ts *httptest.Server, snapshotID, attr string) (snapshotAttributeDoc, string) {
	t.Helper()
	body := ec2RawPost(t, ts, map[string]string{
		"Action":     "DescribeSnapshotAttribute",
		"SnapshotId": snapshotID,
		"Attribute":  attr,
	})
	var doc snapshotAttributeDoc
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body: %s", body)
	return doc, body
}

// ec2RawPost sends an EC2 request through [ec2Request] and returns the body, requiring HTTP
// 200.
//
// It goes through ec2Request rather than posting directly so the Host header — which is what
// resolves the region — is the one every other EC2 test sends.
func ec2RawPost(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)
	return string(raw)
}

// ec2ModifySnapshotAttribute sends ModifySnapshotAttribute and requires it to succeed.
func ec2ModifySnapshotAttribute(t *testing.T, ts *httptest.Server, params map[string]string) {
	t.Helper()
	full := map[string]string{"Action": "ModifySnapshotAttribute"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Return bool `xml:"return"`
	}
	ec2FleetXML(t, ts, full, &doc)
	assert.True(t, doc.Return, "return is documented as true on success")
}

// ec2VolumePermissions reduces the describe to the permissions it reports, in order.
func ec2VolumePermissions(t *testing.T, ts *httptest.Server, snapshotID string) []string {
	t.Helper()
	doc, body := ec2SnapshotAttribute(t, ts, snapshotID, "createVolumePermission")
	require.NotNil(t, doc.CreateVolumePermission, "the element must be present: %s", body)
	out := make([]string, 0, len(doc.CreateVolumePermission.Items))
	for _, item := range doc.CreateVolumePermission.Items {
		out = append(out, item.UserID+item.Group)
	}
	return out
}

// TestEC2_DescribeSnapshotAttribute_AnswersBothAttributes is the describe's fail-before, and
// pins the one thing a struct-only assertion cannot see: exactly one attribute element
// marshals.
//
// A fresh snapshot's createVolumePermission is present and empty, not omitted. AWS describes a
// reset snapshot as "a private snapshot that can only be used by the account that created it",
// which is what a newly created one is — so "no permissions" is the true answer, and an SDK
// reads a present-but-empty element as an empty list where it reads an omitted one as nil.
func TestEC2_DescribeSnapshotAttribute_AnswersBothAttributes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	perms, body := ec2SnapshotAttribute(t, ts, snapshotID, "createVolumePermission")
	assert.Equal(t, snapshotID, perms.SnapshotID)
	require.NotNil(t, perms.CreateVolumePermission, "present, so a caller reads an empty list")
	assert.Empty(t, perms.CreateVolumePermission.Items)
	assert.Nil(t, perms.ProductCodes, "an attribute the caller did not ask about")
	assert.NotContains(t, body, "productCodes",
		"a wrapper that is not a pointer would emit this element too")

	// productCodes is answered rather than refused, and the reason differs from
	// createVolumePermission's: nothing in substrate assigns a product code to a snapshot, so
	// "none" is a fact about every snapshot it can produce rather than an invented default.
	codes, body := ec2SnapshotAttribute(t, ts, snapshotID, "productCodes")
	assert.Equal(t, snapshotID, codes.SnapshotID)
	require.NotNil(t, codes.ProductCodes)
	assert.Empty(t, codes.ProductCodes.Items)
	assert.Nil(t, codes.CreateVolumePermission)
	assert.NotContains(t, body, "createVolumePermission")
}

// TestEC2_ModifySnapshotAttribute_BothWireForms pins that the structured and flat forms are the
// same request, because an SDK picks between them and a caller cannot tell which they got.
func TestEC2_ModifySnapshotAttribute_BothWireForms(t *testing.T) {
	t.Parallel()

	for name, params := range map[string]map[string]string{
		// The form AWS's own two examples send.
		"structured": {
			"CreateVolumePermission.Add.1.UserId": "123456789012",
		},
		// OperationType + UserId.N. The Attribute parameter is Required: No here, so its
		// absence must not be a refusal — the wire form carries the selection.
		"flat": {
			"OperationType": "add",
			"UserId.1":      "123456789012",
		},
		// The same, with the attribute named explicitly.
		"flat with Attribute": {
			"Attribute":     "createVolumePermission",
			"OperationType": "add",
			"UserId.1":      "123456789012",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

			full := map[string]string{"SnapshotId": snapshotID}
			for k, v := range params {
				full[k] = v
			}
			ec2ModifySnapshotAttribute(t, ts, full)
			assert.Equal(t, []string{"123456789012"}, ec2VolumePermissions(t, ts, snapshotID))
		})
	}
}

// TestEC2_ModifySnapshotAttribute_UserGroupIsTheWireMember guards the member name the CLI and
// the SDKs disguise: the wire member of the flat form is **UserGroup.N**, where the CLI spells
// the same thing --group-names and an SDK spells it GroupNames. A handler reading GroupNames.N
// would silently accept the request and share nothing.
func TestEC2_ModifySnapshotAttribute_UserGroupIsTheWireMember(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":    snapshotID,
		"OperationType": "add",
		"UserGroup.1":   "all",
	})
	assert.Equal(t, []string{"all"}, ec2VolumePermissions(t, ts, snapshotID))
}

// TestEC2_ModifySnapshotAttribute_AddsAndRemoves walks the lifecycle a sharing consumer walks,
// and pins the two idempotence choices AWS leaves unstated.
func TestEC2_ModifySnapshotAttribute_AddsAndRemoves(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                          snapshotID,
		"CreateVolumePermission.Add.1.UserId": "111122223333",
		"CreateVolumePermission.Add.2.UserId": "444455556666",
	})
	assert.Equal(t, []string{"111122223333", "444455556666"},
		ec2VolumePermissions(t, ts, snapshotID), "the list keeps request order")

	// Adding one already present changes nothing rather than duplicating.
	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                          snapshotID,
		"CreateVolumePermission.Add.1.UserId": "111122223333",
	})
	assert.Equal(t, []string{"111122223333", "444455556666"},
		ec2VolumePermissions(t, ts, snapshotID))

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                             snapshotID,
		"CreateVolumePermission.Remove.1.UserId": "111122223333",
	})
	assert.Equal(t, []string{"444455556666"}, ec2VolumePermissions(t, ts, snapshotID))

	// Removing one that is not there is not an error — AWS documents none, and a cleanup loop
	// that runs twice needs the second run to succeed.
	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                             snapshotID,
		"CreateVolumePermission.Remove.1.UserId": "111122223333",
	})
	assert.Equal(t, []string{"444455556666"}, ec2VolumePermissions(t, ts, snapshotID))

	// A request naming no modification at all is not a refusal: every parameter but SnapshotId
	// is optional and AWS publishes no error for the empty case.
	ec2ModifySnapshotAttribute(t, ts, map[string]string{"SnapshotId": snapshotID})
	assert.Equal(t, []string{"444455556666"}, ec2VolumePermissions(t, ts, snapshotID))
}

// TestEC2_ModifySnapshotAttribute_AWSExample2Succeeds is the test that keeps substrate from
// refusing a request AWS documents as valid.
//
// AWS: "You may add or remove specified AWS account IDs from a snapshot's list of create
// volume permissions, but you cannot do both in a single operation." Read as a blanket rule
// that sentence would refuse AWS's own Example 2, which adds the group "all" while removing
// the account 111122223333 in one request. The sentence is scoped to account IDs, so the
// refusal is too.
func TestEC2_ModifySnapshotAttribute_AWSExample2Succeeds(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                          snapshotID,
		"CreateVolumePermission.Add.1.UserId": "111122223333",
	})

	// AWS's Example 2, verbatim in shape.
	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                             snapshotID,
		"CreateVolumePermission.Add.1.Group":     "all",
		"CreateVolumePermission.Remove.1.UserId": "111122223333",
	})
	assert.Equal(t, []string{"all"}, ec2VolumePermissions(t, ts, snapshotID))
}

// TestEC2_ModifySnapshotAttribute_RefusesABadRequest is the refusal table.
//
// Every row is substrate's reading of a prose rule, since the operation publishes no error;
// the comment on each names the sentence.
func TestEC2_ModifySnapshotAttribute_RefusesABadRequest(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	tooMany := map[string]string{"SnapshotId": snapshotID}
	for i := 1; i <= 501; i++ {
		tooMany["CreateVolumePermission.Add."+strconv.Itoa(i)+".UserId"] =
			strconv.FormatInt(int64(100000000000+i), 10)
	}

	for name, tc := range map[string]struct {
		params   map[string]string
		wantCode string
	}{
		"no snapshot": {
			map[string]string{"CreateVolumePermission.Add.1.UserId": "123456789012"},
			"MissingParameter",
		},
		"malformed snapshot": {
			map[string]string{"SnapshotId": "snapshot-1"}, "InvalidSnapshotID.Malformed",
		},
		"absent snapshot": {
			map[string]string{"SnapshotId": "snap-0123456789abcdef0"}, "InvalidSnapshot.NotFound",
		},
		// "Only volume creation permissions can be modified."
		"productCodes": {
			map[string]string{"SnapshotId": snapshotID, "Attribute": "productCodes"},
			"InvalidParameterValue",
		},
		"unknown attribute": {
			map[string]string{"SnapshotId": snapshotID, "Attribute": "launchPermission"},
			"InvalidParameterValue",
		},
		// Group's only valid value is "all"; a group nothing can grant would be a permission
		// a caller reads back and cannot act on.
		"another group, structured": {
			map[string]string{
				"SnapshotId":                         snapshotID,
				"CreateVolumePermission.Add.1.Group": "everyone",
			},
			"InvalidParameterValue",
		},
		"another group, flat": {
			map[string]string{
				"SnapshotId": snapshotID, "OperationType": "add", "UserGroup.1": "everyone",
			},
			"InvalidParameterValue",
		},
		// The account IDs half of "you cannot do both in a single operation".
		"add and remove an account": {
			map[string]string{
				"SnapshotId":                             snapshotID,
				"CreateVolumePermission.Add.1.UserId":    "111122223333",
				"CreateVolumePermission.Remove.1.UserId": "444455556666",
			},
			"InvalidParameterValue",
		},
		// "You can make up to 500 modifications to a snapshot in a single operation."
		"five hundred and one modifications": {tooMany, "InvalidParameterValue"},
		"unknown OperationType": {
			map[string]string{
				"SnapshotId": snapshotID, "OperationType": "share", "UserId.1": "111122223333",
			},
			"InvalidParameterValue",
		},
		// A flat value with no OperationType names no list to join, so there is nothing to do
		// with it but refuse — accepting it would silently discard the permission.
		"flat values with no OperationType": {
			map[string]string{"SnapshotId": snapshotID, "UserId.1": "111122223333"},
			"MissingParameter",
		},
	} {
		t.Run(name, func(t *testing.T) {
			params := map[string]string{"Action": "ModifySnapshotAttribute"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
		})
	}

	// None of the above wrote anything: the snapshot is still private.
	assert.Empty(t, ec2VolumePermissions(t, ts, snapshotID))
}

// TestEC2_ModifySnapshotAttribute_EncryptedSnapshotCannotBeMadePublic pins the EBS user guide's
// rule — "You can share only unencrypted snapshots publicly" — and its scope: the refusal is
// about the group, so sharing an encrypted snapshot with a *named account* still works.
//
// The companion rule AWS states in the same breath, that a snapshot with a Marketplace product
// code cannot be made public, is unreachable rather than unmodeled: substrate assigns no
// product codes at all.
func TestEC2_ModifySnapshotAttribute_EncryptedSnapshotCannotBeMadePublic(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volID := ec2CreateSizedVolume(t, ts, 12, map[string]string{"Encrypted": "true"})
	snap := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volID})
	require.True(t, snap.Encrypted, "the snapshot inherits the volume's encryption")

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                             "ModifySnapshotAttribute",
		"SnapshotId":                         snap.SnapshotID,
		"CreateVolumePermission.Add.1.Group": "all",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Contains(t, strings.ToLower(message), "unencrypted")
	assert.Empty(t, ec2VolumePermissions(t, ts, snap.SnapshotID))

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                          snap.SnapshotID,
		"CreateVolumePermission.Add.1.UserId": "111122223333",
	})
	assert.Equal(t, []string{"111122223333"},
		ec2VolumePermissions(t, ts, snap.SnapshotID), "sharing with an account is not sharing publicly")
}

// TestEC2_ResetSnapshotAttribute_ClearsThePermissions is the reset's fail-before.
//
// Resetting clears the list rather than restoring a remembered default, because AWS describes
// the result as "making it a private snapshot that can only be used by the account that
// created it" — which is what a created snapshot already is.
func TestEC2_ResetSnapshotAttribute_ClearsThePermissions(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	ec2ModifySnapshotAttribute(t, ts, map[string]string{
		"SnapshotId":                          snapshotID,
		"CreateVolumePermission.Add.1.UserId": "111122223333",
		"CreateVolumePermission.Add.2.Group":  "all",
	})
	require.Len(t, ec2VolumePermissions(t, ts, snapshotID), 2)

	var doc struct {
		Return bool `xml:"return"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":     "ResetSnapshotAttribute",
		"SnapshotId": snapshotID,
		"Attribute":  "createVolumePermission",
	}, &doc)
	assert.True(t, doc.Return)

	// Present and empty, not omitted: the snapshot still exists and still has the attribute.
	after, body := ec2SnapshotAttribute(t, ts, snapshotID, "createVolumePermission")
	require.NotNil(t, after.CreateVolumePermission, "body: %s", body)
	assert.Empty(t, after.CreateVolumePermission.Items)

	// And the snapshot itself is untouched.
	described := ec2DescribeSnapshots(t, ts, map[string]string{"SnapshotId.1": snapshotID})
	require.Len(t, described, 1)
	assert.Equal(t, int64(12), described[0].VolumeSize)
}

// TestEC2_SnapshotAttribute_ValidatesTheAttributeBeforeTheSnapshot pins the ordering
// [EC2Plugin.describeInstanceAttribute] established: an attribute name the operation does not
// accept is a defect in the request that no retry fixes, while a snapshot ID naming nothing may
// be one the caller is still waiting on. Reporting the wrong one first sends a consumer's
// retry loop after a request that will never succeed.
func TestEC2_SnapshotAttribute_ValidatesTheAttributeBeforeTheSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, action := range []string{
		"DescribeSnapshotAttribute", "ModifySnapshotAttribute", "ResetSnapshotAttribute",
	} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
				"Action":     action,
				"Attribute":  "nonsense",
				"SnapshotId": "snap-0123456789abcdef0",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code,
				"the attribute name is refused before the snapshot is resolved")
		})
	}
}

// TestEC2_SnapshotAttribute_RequiresTheAttributeWhereAWSDoes pins the split between the three
// operations: Attribute is Required: Yes on the describe and the reset — "You can specify only
// one attribute at a time" — and Required: No on the modify, where the wire form carries the
// selection. A handler that required it everywhere would refuse the flat modify form; one that
// defaulted it everywhere would answer an attribute the caller never named.
func TestEC2_SnapshotAttribute_RequiresTheAttributeWhereAWSDoes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, snapshotID := ec2SnapshotOfSize(t, ts, 12)

	for _, action := range []string{"DescribeSnapshotAttribute", "ResetSnapshotAttribute"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
				"Action": action, "SnapshotId": snapshotID,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "MissingParameter", code)
		})
	}

	t.Run("ResetSnapshotAttribute refuses productCodes", func(t *testing.T) {
		t.Parallel()
		// "Only the attribute for permission to create volumes can be reset."
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "ResetSnapshotAttribute", "SnapshotId": snapshotID,
			"Attribute": "productCodes",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})

	t.Run("DescribeSnapshotAttribute requires a snapshot", func(t *testing.T) {
		t.Parallel()
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "DescribeSnapshotAttribute", "Attribute": "createVolumePermission",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "MissingParameter", code)
	})
}
