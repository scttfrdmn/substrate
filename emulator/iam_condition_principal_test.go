package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// aws:PrincipalArn has a producer on every enforcement door (#714).
//
// It had one only in the simulator, which is a divergence in the direction that matters:
// a caller could simulate a policy conditioned on the key, watch it evaluate, and then
// have the same policy decided differently by the gate the simulation exists to predict.
//
// The tests below are in two halves. The first asserts the key is populated and readable
// by the operators written about it. The second is the regression #714's own absent-key
// rule created: AWS answers *true* for a negated operator over an absent key, so with no
// producer, `ArnNotEquals` over aws:PrincipalArn was satisfied by every caller — a Deny
// so written fired against the principal its wildcard exempted, and an Allow so written
// granted everyone.

// iamPrincipalARN is the caller every case here authorizes as, matching the ARN
// newAuthTestReqCtx is given below.
const iamPrincipalARN = "arn:aws:iam::123456789012:user/jill"

// iamPrincipalPolicy is a one-condition Allow for s3:GetObject over aws:PrincipalArn.
func iamPrincipalPolicy(operator, condValue string) *emulator.PolicyDocument {
	return iamClockPolicy(operator, "aws:PrincipalArn", condValue)
}

// TestAuthController_PrincipalArnHasAProducer reads the key with each family of operator
// AWS documents for it.
//
// The Null rows assert population without reference to the value; the rest assert the ARN
// written is the caller's own, which a Null row alone would not catch — a producer writing
// the *role*'s ARN, or a truncated one, would pass Null and fail these.
func TestAuthController_PrincipalArnHasAProducer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		operator  string
		condValue string
		allowed   bool
	}{
		{"the key exists", "Null", "false", true},
		{"the key is not absent", "Null", "true", false},
		{"ArnEquals on the caller's own ARN", "ArnEquals", iamPrincipalARN, true},
		{"ArnEquals on another ARN", "ArnEquals", "arn:aws:iam::123456789012:user/jack", false},
		{"ArnLike over the user path", "ArnLike", "arn:aws:iam::123456789012:user/*", true},
		{"ArnLike over the role path", "ArnLike", "arn:aws:iam::123456789012:role/*", false},
		// AWS: aws:PrincipalArn is "always present" and its value is a string, so the
		// String operators read it too — which is how a policy conditioned with
		// StringLike rather than ArnLike behaves the same way.
		{"StringLike over the user path", "StringLike", "arn:aws:iam::*:user/jill", true},
		{"StringEquals on the caller's own ARN", "StringEquals", iamPrincipalARN, true},
		// The negated form, read against a present key: it is the absence that used to
		// make these vacuous, so both directions are pinned.
		{"ArnNotEquals on another ARN", "ArnNotEquals", "arn:aws:iam::123456789012:user/jack", true},
		{"ArnNotEquals on the caller's own ARN", "ArnNotEquals", iamPrincipalARN, false},
		{"ArnNotEquals over a matching wildcard", "ArnNotEquals", "arn:aws:iam::123456789012:user/*", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := iamClockCheckAccess(t, iamPrincipalPolicy(tc.operator, tc.condValue), false)
			if tc.allowed {
				assert.NoError(t, err, "%s %s", tc.operator, tc.condValue)
				return
			}
			require.Error(t, err, "%s %s", tc.operator, tc.condValue)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDenied", awsErr.Code)
		})
	}
}

// TestAuthController_ArnNotEqualsDenyExemptsTheNamedPrincipal is the regression, written
// as the policy that expresses it: a broad Allow with a Deny that carves out one caller.
//
// This is the shape a guardrail takes — "deny this action to everyone who is not the
// break-glass principal" — and it inverted completely without a producer for the key it
// names. The second case is the control: the same Deny must still fire on a principal the
// wildcard does not cover, or the test would pass on a Deny that had simply gone inert.
func TestAuthController_ArnNotEqualsDenyExemptsTheNamedPrincipal(t *testing.T) {
	t.Parallel()

	denyUnless := func(pattern string) *emulator.PolicyDocument {
		return &emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{
				{
					Effect:   emulator.IAMEffectAllow,
					Action:   emulator.StringOrSlice{"s3:*"},
					Resource: emulator.StringOrSlice{"*"},
				},
				{
					Effect:   emulator.IAMEffectDeny,
					Action:   emulator.StringOrSlice{"s3:GetObject"},
					Resource: emulator.StringOrSlice{"*"},
					Condition: map[string]map[string]emulator.StringOrSlice{
						"ArnNotEquals": {"aws:PrincipalArn": emulator.StringOrSlice{pattern}},
					},
				},
			},
		}
	}

	t.Run("the exempted principal is allowed", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, iamClockCheckAccess(t, denyUnless("arn:aws:iam::*:user/jill"), false),
			"the wildcard names jill, so the Deny must not fire on her")
	})

	t.Run("everyone else is denied", func(t *testing.T) {
		t.Parallel()
		assert.Error(t, iamClockCheckAccess(t, denyUnless("arn:aws:iam::*:user/jack"), false),
			"the wildcard does not name jill, so the Deny must fire")
	})
}

// TestEC2_TagOnCreate_ReadsPrincipalArn covers the second door a tagged create passes.
//
// The tag-on-create gate builds its own condition context, so the key had to be written
// there too; a request authorized twice against two different answers for the same key
// would allow at one door and deny at the other. Both statements below carry the
// condition, so a missing producer fails whichever gate lost it.
func TestEC2_TagOnCreate_ReadsPrincipalArn(t *testing.T) {
	t.Parallel()

	principal := "arn:aws:iam::" + ec2AuthzAccount + ":user/tagger"
	onlyTagger := map[string]map[string]emulator.StringOrSlice{
		"ArnEquals": {"aws:PrincipalArn": emulator.StringOrSlice{principal}},
	}
	f := newEC2AuthzFixture(t, "tagger", emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", ec2AuthzAllFive(), onlyTagger),
			ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN}, onlyTagger),
		},
	})

	err := f.launch(t, map[string]string{
		"ImageId":                         ec2AuthzAMI,
		"SubnetId":                        ec2AuthzSubnet,
		"SecurityGroupId.1":               ec2AuthzSG,
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	})
	assert.NoError(t, err, "both gates condition on aws:PrincipalArn, so both must read it")
}

// TestIAMPlugin_ControlPlaneDoorReadsPrincipalArn covers the fourth door.
//
// The IAM plugin authorizes its own control-plane calls rather than going through
// [emulator.AuthController], and its context was empty by design — nothing there reads the
// request for tags. aws:PrincipalArn is the exception, because it is not read from the
// request at all: it is who signed it, and this door knows that as surely as the others.
// While it was missing, a condition on the key held vacuously here and failed at the main
// gate, so one policy got two answers depending on which service the call went to.
func TestIAMPlugin_ControlPlaneDoorReadsPrincipalArn(t *testing.T) {
	const danaARN = "arn:aws:iam::123456789012:user/dana"

	// The call under test creates a second user, which is as good as any: it is
	// authorized as iam:CreateUser and needs no prior state.
	call := func(t *testing.T, srv *emulator.Server, keyID, userName string) int {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"UserName": userName})
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateUser")
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(keyID, "iam"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		resp := w.Result()
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	cases := []struct {
		name       string
		condARN    string
		wantStatus int
	}{
		{"ArnEquals on the caller is allowed", danaARN, http.StatusOK},
		{"ArnEquals on another principal is refused",
			"arn:aws:iam::123456789012:user/eve", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTrustPolicyTestServer(t)
			require.Equal(t, http.StatusOK,
				trustIAMCall(t, srv, "CreateUser", map[string]any{"UserName": "dana"}).StatusCode)
			require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
				"UserName":   "dana",
				"PolicyName": "iam-as-dana",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Action":"iam:*","Resource":"*",` +
					`"Condition":{"ArnEquals":{"aws:PrincipalArn":"` + tc.condARN + `"}}}]}`,
			}).StatusCode)

			resp := trustIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": "dana"})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			var parsed struct {
				AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
			}
			require.NoError(t, xml.Unmarshal(body, &parsed))
			require.NotEmpty(t, parsed.AccessKeyID)

			assert.Equal(t, tc.wantStatus, call(t, srv, parsed.AccessKeyID, "made-by-dana"))
		})
	}
}

// TestSTSAssumeRole_TrustPolicyReadsPrincipalArn covers the trust-policy door.
//
// A trust policy is where AWS's own guidance puts a condition on aws:PrincipalArn — it is
// how a Principal naming a whole account is narrowed to particular callers — so the key
// being absent there made the narrowing either inert or inverted. The Deny case is the
// same exemption shape as the identity-policy test above, on the door where it is most
// often written.
func TestSTSAssumeRole_TrustPolicyReadsPrincipalArn(t *testing.T) {
	const aliceARN = "arn:aws:iam::123456789012:user/alice"

	cases := []struct {
		name        string
		trustPolicy string
		wantStatus  int
	}{
		{
			name: "ArnEquals on the caller is admitted",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",` +
				`"Condition":{"ArnEquals":{"aws:PrincipalArn":"` + aliceARN + `"}}}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "ArnEquals on another principal is refused",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",` +
				`"Condition":{"ArnEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/bob"}}}]}`,
			wantStatus: http.StatusForbidden,
		},
		{
			// The exemption shape: everyone in the account may assume the role except
			// the principals the wildcard does not name. Without a producer the Deny
			// fired on alice too, and the role was assumable by nobody.
			name: "a Deny with ArnNotEquals exempts the named principal",
			trustPolicy: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"},` +
				`{"Effect":"Deny","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",` +
				`"Condition":{"ArnNotEquals":{"aws:PrincipalArn":"arn:aws:iam::*:user/alice"}}}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "the same Deny fires on a principal it does not name",
			trustPolicy: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"},` +
				`{"Effect":"Deny","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",` +
				`"Condition":{"ArnNotEquals":{"aws:PrincipalArn":"arn:aws:iam::*:user/bob"}}}]}`,
			wantStatus: http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTrustPolicyTestServer(t)
			keyID := trustSetupCaller(t, srv, "alice")
			trustCreateRole(t, srv, "target", tc.trustPolicy)

			resp := trustAssumeRole(t, srv, keyID,
				"arn:aws:iam::123456789012:role/target", "sess", "")
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}
