package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are #544's gate.
//
// CreateStack answers with a StackId — the stack's ARN — and every stack-scoped
// operation documents that identifier as an alternative to the name: "The name or
// the unique stack ID that's associated with the stack". Substrate looked the
// caller's string up verbatim, so the ARN it had just minted resolved to no stack.
//
// The delete is the case worth stating plainly, and it is why this is filed as a
// silent-success bug rather than a lookup gap. DeleteStack documents no not-found
// error, so the deployer treats an unresolvable name as a stack already gone and
// succeeds. Handed an ARN, it swept nothing, answered 200, and left the stack
// standing — and since v0.89.0 shipped #518's sweep, a by-name delete really does
// tear resources down, so the two paths disagreed about what a successful delete
// means.
//
// A caller could not tell: no error to catch, no status to poll. The next
// CreateStack answering AlreadyExists was the only symptom.

// cfnTestAccount and cfnTestRegion are the identity cfnRequest's signed
// Authorization header resolves to. The ARN assertions below are built from them
// rather than from a pasted string, so a change to either shows up as a failure
// here rather than as a test that no longer exercises the scope check.
const (
	cfnTestAccount = "123456789012"
	cfnTestRegion  = "us-east-1"
)

// cfnCreateStackARN creates a stack and returns the StackId it reported, which is
// the identifier a caller holds and the one these tests hand back.
func cfnCreateStackARN(t *testing.T, ts *cfnTestServer, name, template string) string {
	t.Helper()
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    name,
		"TemplateBody": template,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	arn := cfnXMLValue(t, body, "StackId")
	require.Contains(t, arn, ":stack/"+name+"/", "StackId was %q", arn)
	return arn
}

// TestCFNStackARN_DescribeByReturnedID is the smallest statement of the defect:
// the identifier substrate handed the caller is one substrate accepts.
func TestCFNStackARN_DescribeByReturnedID(t *testing.T) {
	ts := newCFNTestServer(t)
	arn := cfnCreateStackARN(t, ts, "by-id", cfnEmptyTemplate)

	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": arn})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackName>by-id</StackName>")
	assert.Contains(t, body, "<StackStatus>CREATE_COMPLETE</StackStatus>")

	// And the ARN is exactly the one the create reported, so a caller comparing the
	// two identifiers sees them agree.
	assert.Equal(t, arn, cfnXMLValue(t, body, "StackId"))
}

// TestCFNStackARN_DeleteByARNSweepsResources is the half a caller could not
// observe. Before the fix the delete answered 200, the bucket survived, the stack
// stayed CREATE_COMPLETE, and the next create by name answered AlreadyExists.
func TestCFNStackARN_DeleteByARNSweepsResources(t *testing.T) {
	ts := newCFNTestServer(t)
	arn := cfnCreateStackARN(t, ts, "swept-by-arn", cfnBucketTemplate)
	require.Equal(t, http.StatusOK, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
		"the stack's bucket exists before the delete")

	code, body := cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": arn})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	assert.Equal(t, http.StatusNotFound, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
		"a delete by ARN sweeps the stack's resources, as a delete by name does")

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "swept-by-arn"})
	require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	// The symptom the issue was filed from: the name is free again.
	code, body = cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "swept-by-arn",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "AlreadyExists")
}

// TestCFNStackARN_DeleteAbsentByARNSucceeds keeps the tolerance the resolver runs
// ahead of. A well-formed in-scope ARN for a stack that is genuinely gone is still
// a successful delete — DeleteStack documents no not-found error — and the fix must
// not turn that into a refusal.
func TestCFNStackARN_DeleteAbsentByARNSucceeds(t *testing.T) {
	ts := newCFNTestServer(t)
	arn := cfnCreateStackARN(t, ts, "twice-deleted", cfnEmptyTemplate)

	for _, pass := range []string{"first", "second"} {
		code, body := cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": arn})
		require.Equal(t, http.StatusOK, code, "%s delete: body was %s", pass, body)
	}
}

// TestCFNStackARN_StackScopedOperations covers the whole set rather than the two
// the report named. Fixing describe and delete alone would leave the same trap in
// GetTemplate, the change-set family and the drift pair — and those are exactly the
// operations a caller reaches holding a StackId rather than a name.
func TestCFNStackARN_StackScopedOperations(t *testing.T) {
	ts := newCFNTestServer(t)
	arn := cfnCreateStackARN(t, ts, "every-op", cfnEmptyTemplate)

	// A change set to address, so the change-set operations have a subject.
	code, body := cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName":     arn,
		"ChangeSetName": "cs-by-arn",
		"TemplateBody":  cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "CreateChangeSet by ARN: body was %s", body)

	cases := []struct {
		action  string
		params  map[string]string
		expects string
	}{
		{action: "DescribeStacks", expects: "<StackName>every-op</StackName>"},
		{action: "DescribeStackResources", expects: "<DescribeStackResourcesResult>"},
		{action: "GetTemplate", expects: "Resources"},
		{action: "ListChangeSets", expects: "<ChangeSetName>cs-by-arn</ChangeSetName>"},
		{
			action:  "DescribeChangeSet",
			params:  map[string]string{"ChangeSetName": "cs-by-arn"},
			expects: "<StackName>every-op</StackName>",
		},
		{action: "DetectStackDrift", expects: "<StackDriftDetectionId>"},
		{action: "DescribeStackResourceDrifts", expects: "<DescribeStackResourceDriftsResult>"},
		{
			action:  "UpdateStack",
			params:  map[string]string{"TemplateBody": cfnBucketTemplate},
			expects: ":stack/every-op/",
		},
		{
			action:  "ExecuteChangeSet",
			params:  map[string]string{"ChangeSetName": "cs-by-arn"},
			expects: "<ExecuteChangeSetResponse",
		},
		{
			action:  "DeleteChangeSet",
			params:  map[string]string{"ChangeSetName": "cs-by-arn"},
			expects: "<DeleteChangeSetResponse",
		},
	}
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			params := map[string]string{"StackName": arn}
			for k, v := range tc.params {
				params[k] = v
			}
			code, body := cfnAction(t, ts, tc.action, params)
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, tc.expects)
		})
	}
}

// TestCFNStackARN_OutOfScopeARNIsRefused is the check that keeps this fix from
// being worse than the bug it closes.
//
// Lifting the name out of an ARN without verifying the rest of it would let a
// caller reach — and DeleteStack tear down — a stack in another account, Region or
// partition, or bind a UUID to a name it was never derived from, simply by writing
// the ARN by hand. Each case below is a string a caller can compose; none of them
// is an identifier substrate would have minted for this caller.
func TestCFNStackARN_OutOfScopeARNIsRefused(t *testing.T) {
	ts := newCFNTestServer(t)
	arn := cfnCreateStackARN(t, ts, "in-scope", cfnBucketTemplate)
	// A second stack, so the name-swap case below names something that really
	// exists: the refusal must come from the UUID, not from the stack being absent.
	_ = cfnCreateStackARN(t, ts, "other", cfnEmptyTemplate)

	uuid := arn[strings.LastIndexByte(arn, '/')+1:]
	require.NotEmpty(t, uuid)

	cases := []struct {
		name string
		arn  string
	}{
		{
			name: "a foreign account",
			arn:  strings.Replace(arn, cfnTestAccount, "999999999999", 1),
		},
		{
			name: "a foreign Region",
			arn:  strings.Replace(arn, cfnTestRegion, "eu-west-1", 1),
		},
		{
			name: "a foreign partition",
			arn:  strings.Replace(arn, "arn:aws:", "arn:aws-cn:", 1),
		},
		{
			name: "a UUID belonging to another stack's name",
			arn:  "arn:aws:cloudformation:" + cfnTestRegion + ":" + cfnTestAccount + ":stack/other/" + uuid,
		},
		{
			name: "a UUID that matches nothing",
			arn: "arn:aws:cloudformation:" + cfnTestRegion + ":" + cfnTestAccount +
				":stack/in-scope/00000000-0000-0000-0000-000000000000",
		},
		{
			name: "no UUID at all",
			arn:  "arn:aws:cloudformation:" + cfnTestRegion + ":" + cfnTestAccount + ":stack/in-scope",
		},
		{
			name: "an empty name segment",
			arn:  "arn:aws:cloudformation:" + cfnTestRegion + ":" + cfnTestAccount + ":stack//" + uuid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, arn, tc.arn, "the case must differ from the real ARN")

			code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": tc.arn})
			require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

			// The delete is the case that matters: an out-of-scope ARN must be
			// refused outright, not absorbed by the absent-stack-is-success
			// tolerance, or the refusal would read to a caller as a completed
			// teardown.
			code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": tc.arn})
			require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
		})
	}

	// Nothing above touched the stack it was aimed at.
	assert.Equal(t, http.StatusOK, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
		"a refused ARN must not have swept the in-scope stack's resources")
	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "in-scope"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackStatus>CREATE_COMPLETE</StackStatus>")
}

// TestCFNStackARN_AnotherCallersARNIsRefused is the scope check's sharpest case,
// and the one that needs two callers to state.
//
// The cases in TestCFNStackARN_OutOfScopeARNIsRefused edit one field of a real ARN,
// which leaves the digest disagreeing with the rest — so a check that verified only
// the digest would refuse them all and look sufficient. This test hands over an ARN
// that is *internally consistent* and belongs to a different caller: substrate minted
// it, every field agrees, and it is still not this caller's to use. Only comparing
// the ARN against the one substrate would mint for **this** caller refuses it.
//
// It is not a hypothetical. One substrate process serves every account and Region a
// test suite uses, and a stack ARN is something a caller passes around; #517 is what
// happened the last time a stack's identity and its caller's were allowed to drift
// apart.
func TestCFNStackARN_AnotherCallersARNIsRefused(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	// Two callers on one server: unsigned resolves to 000000000000, an AKIA-signed
	// request to 123456789012.
	const unsigned = ""
	signed := signedAuthHeader("cloudformation", cfnTestRegion)

	create := func(auth, region, name string) string {
		t.Helper()
		code, body := cfnIdentityRequest(t, ts, "cloudformation", region, auth, map[string]string{
			"Action":       "CreateStack",
			"Version":      "2010-05-15",
			"StackName":    name,
			"TemplateBody": cfnEmptyTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		return cfnXMLValue(t, body, "StackId")
	}

	zeroARN := create(unsigned, cfnTestRegion, "zero-owned")
	testARN := create(signed, cfnTestRegion, "test-owned")
	westARN := create(signed, "eu-west-1", "west-owned")

	require.Contains(t, zeroARN, ":000000000000:stack/zero-owned/")
	require.Contains(t, testARN, ":"+cfnTestAccount+":stack/test-owned/")
	require.Contains(t, westARN, ":eu-west-1:")

	cases := []struct {
		name   string
		auth   string
		region string
		arn    string
	}{
		{
			name: "the unsigned caller's own ARN, offered by the signed caller",
			auth: signed, region: cfnTestRegion, arn: zeroARN,
		},
		{
			name: "the signed caller's own ARN, offered by the unsigned caller",
			auth: unsigned, region: cfnTestRegion, arn: testARN,
		},
		{
			name: "a us-east-1 ARN offered to the eu-west-1 endpoint",
			auth: signedAuthHeader("cloudformation", "eu-west-1"), region: "eu-west-1", arn: testARN,
		},
		{
			name: "an eu-west-1 ARN offered to the us-east-1 endpoint",
			auth: signed, region: cfnTestRegion, arn: westARN,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := cfnIdentityRequest(t, ts, "cloudformation", tc.region, tc.auth,
				map[string]string{
					"Action": "DescribeStacks", "Version": "2010-05-15", "StackName": tc.arn,
				})
			require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Contains(t, body, "<Code>ValidationError</Code>")

			code, body = cfnIdentityRequest(t, ts, "cloudformation", tc.region, tc.auth,
				map[string]string{
					"Action": "DeleteStack", "Version": "2010-05-15", "StackName": tc.arn,
				})
			require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Contains(t, body, "<Code>ValidationError</Code>")
		})
	}

	// Each owner still reaches its own stack by its own ARN, which is what keeps the
	// refusals above from being a blanket "no ARNs across identities".
	for _, own := range []struct {
		auth, region, arn, name string
	}{
		{unsigned, cfnTestRegion, zeroARN, "zero-owned"},
		{signed, cfnTestRegion, testARN, "test-owned"},
		{signedAuthHeader("cloudformation", "eu-west-1"), "eu-west-1", westARN, "west-owned"},
	} {
		code, body := cfnIdentityRequest(t, ts, "cloudformation", own.region, own.auth,
			map[string]string{
				"Action": "DescribeStacks", "Version": "2010-05-15", "StackName": own.arn,
			})
		require.Equal(t, http.StatusOK, code, "%s: body was %s", own.name, body)
		assert.Contains(t, body, "<StackName>"+own.name+"</StackName>")
	}
}

// TestCFNStackARN_BareNamesUnchanged is the regression half. A name is not an ARN,
// an absent name still answers ValidationError, and an absent name still deletes
// successfully — the resolver sits in front of all three and must pass them
// through untouched.
func TestCFNStackARN_BareNamesUnchanged(t *testing.T) {
	ts := newCFNTestServer(t)
	_ = cfnCreateStackARN(t, ts, "plain", cfnEmptyTemplate)

	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "plain"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackName>plain</StackName>")

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "absent"})
	require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "absent"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// An absent StackName is still absent: DescribeStacks with none reports every
	// stack, and the resolver must not turn "" into a refusal.
	code, body = cfnAction(t, ts, "DescribeStacks", nil)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackName>plain</StackName>")

	// DeleteStack with no StackName is still the missing-parameter case, not the
	// out-of-scope one.
	code, body = cfnAction(t, ts, "DeleteStack", nil)
	require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "MissingParameter", cfnErrorCode(t, body))
}
