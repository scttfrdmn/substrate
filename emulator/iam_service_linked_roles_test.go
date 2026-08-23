package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Service-linked roles were the largest hole in substrate's condition-key coverage: the
// three operations answered InvalidAction, so the eleven bundled managed-policy
// statements conditioned on iam:AWSServiceName could not be evaluated at all (#747).

func TestIAM_CreateServiceLinkedRole(t *testing.T) {
	tests := []struct {
		name         string
		serviceName  string
		customSuffix string
		description  string
		wantRoleName string
		wantPath     string
	}{
		{
			// The table's citation: AWSLambda_FullAccess scopes its Resource to this
			// exact ARN with no trailing wildcard, so a derived name would match nothing.
			name:         "a tabled principal gets the name AWS uses",
			serviceName:  "lambda.amazonaws.com",
			wantRoleName: "AWSServiceRoleForLambda",
			wantPath:     "/aws-service-role/lambda.amazonaws.com/",
		},
		{
			// AmazonElastiCacheFullAccess spells the internal capital, which no
			// mechanical derivation from "elasticache" produces.
			name:         "the table preserves AWS's own casing",
			serviceName:  "elasticache.amazonaws.com",
			wantRoleName: "AWSServiceRoleForElastiCache",
			wantPath:     "/aws-service-role/elasticache.amazonaws.com/",
		},
		{
			name:         "a custom suffix is appended after an underscore",
			serviceName:  "lambda.amazonaws.com",
			customSuffix: "deploy",
			wantRoleName: "AWSServiceRoleForLambda_deploy",
			wantPath:     "/aws-service-role/lambda.amazonaws.com/",
		},
		{
			// A principal with no citation still gets a stable, well-formed name.
			name:         "an untabled principal gets substrate's derived name",
			serviceName:  "someservice.amazonaws.com",
			wantRoleName: "AWSServiceRoleForSomeservice",
			wantPath:     "/aws-service-role/someservice.amazonaws.com/",
		},
		{
			name:         "a description is stored",
			serviceName:  "ssm.amazonaws.com",
			description:  "Provides access to AWS Resources managed by Amazon SSM",
			wantRoleName: "AWSServiceRoleForAmazonSSM",
			wantPath:     "/aws-service-role/ssm.amazonaws.com/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			body := map[string]string{"AWSServiceName": tt.serviceName}
			if tt.customSuffix != "" {
				body["CustomSuffix"] = tt.customSuffix
			}
			if tt.description != "" {
				body["Description"] = tt.description
			}
			resp := iamRequest(t, srv, "CreateServiceLinkedRole", body)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			decodeIAMXML(t, resp, &out)
			role, ok := out["Role"].(map[string]any)
			require.True(t, ok, "response should carry a Role: %#v", out)
			assert.Equal(t, tt.wantRoleName, role["RoleName"])
			assert.Equal(t, tt.wantPath, role["Path"])
			assert.Equal(t,
				"arn:aws:iam::123456789012:role"+tt.wantPath+tt.wantRoleName,
				role["Arn"])
			if tt.description != "" {
				assert.Equal(t, tt.description, role["Description"])
			}

			// The trust policy names the linked service and nothing else — "unless
			// defined otherwise, only that service can assume the roles" — so the role
			// is usable by the STS trust-policy gate without inventing permissions.
			doc, ok := role["AssumeRolePolicyDocument"].(string)
			require.True(t, ok, "the Role shape should carry the trust policy")
			var parsed emulator.PolicyDocument
			require.NoError(t, json.Unmarshal([]byte(doc), &parsed))
			require.Len(t, parsed.Statement, 1)
			assert.Equal(t, "Allow", parsed.Statement[0].Effect)
			assert.Equal(t, emulator.StringOrSlice{"sts:AssumeRole"}, parsed.Statement[0].Action)
			require.NotNil(t, parsed.Statement[0].Principal)
			assert.Equal(t, []string{tt.serviceName}, parsed.Statement[0].Principal.Service)

			// The role is an ordinary role to every other operation, which is the whole
			// point of writing it to the same state key.
			getResp := iamRequest(t, srv, "GetRole", map[string]string{"RoleName": tt.wantRoleName})
			assert.Equal(t, http.StatusOK, getResp.StatusCode)
		})
	}
}

func TestIAM_CreateServiceLinkedRole_Refusals(t *testing.T) {
	// CreateServiceLinkedRole publishes exactly four errors and EntityAlreadyExists is
	// not among them, so the duplicate refusal cannot be a copy of CreateRole's.
	tests := []struct {
		name     string
		body     map[string]string
		wantCode string
		wantHTTP int
	}{
		{
			name:     "AWSServiceName is required",
			body:     map[string]string{},
			wantCode: "ValidationError",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:     "a malformed service name is InvalidInput",
			body:     map[string]string{"AWSServiceName": "not a principal!"},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "a service name over 128 characters is InvalidInput",
			body: map[string]string{
				"AWSServiceName": strings.Repeat("a", 120) + ".amazonaws.com",
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "a malformed custom suffix is InvalidInput",
			body: map[string]string{
				"AWSServiceName": "lambda.amazonaws.com",
				"CustomSuffix":   "no spaces allowed",
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "a custom suffix over 64 characters is InvalidInput",
			body: map[string]string{
				"AWSServiceName": "lambda.amazonaws.com",
				"CustomSuffix":   strings.Repeat("s", 65),
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "a description over 1000 characters is InvalidInput",
			body: map[string]string{
				"AWSServiceName": "lambda.amazonaws.com",
				"Description":    strings.Repeat("d", 1001),
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			// A 64-character role-name cap applies everywhere in IAM, and a long
			// derived name plus a long suffix can pass both individual checks and
			// still exceed it.
			name: "a combined name over 64 characters is InvalidInput",
			body: map[string]string{
				"AWSServiceName": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.amazonaws.com",
				"CustomSuffix":   strings.Repeat("s", 40),
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			resp := iamRequest(t, srv, "CreateServiceLinkedRole", tt.body)
			require.Equal(t, tt.wantHTTP, resp.StatusCode)
			var out map[string]any
			decodeIAMXML(t, resp, &out)
			assert.Equal(t, tt.wantCode, out["__type"])
		})
	}
}

func TestIAM_CreateServiceLinkedRole_DuplicateIsInvalidInput(t *testing.T) {
	srv := newIAMTestServer(t)
	body := map[string]string{"AWSServiceName": "lambda.amazonaws.com", "CustomSuffix": "one"}
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole", body).StatusCode)

	resp := iamRequest(t, srv, "CreateServiceLinkedRole", body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"the duplicate refusal is InvalidInput 400, not EntityAlreadyExists 409: "+
			"CreateServiceLinkedRole does not publish EntityAlreadyExists at all")
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	assert.Equal(t, "InvalidInput", out["__type"])
	assert.Contains(t, out["message"], "has been taken in this account")

	// A different suffix is a different role, which is what CustomSuffix exists for.
	other := map[string]string{"AWSServiceName": "lambda.amazonaws.com", "CustomSuffix": "two"}
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole", other).StatusCode)
}

func TestIAM_DeleteRole_RefusesAServiceLinkedRole(t *testing.T) {
	// Without this guard DeleteServiceLinkedRole would be decorative: a caller could
	// delete the role through the ordinary path and never submit a deletion task. AWS
	// publishes UnmodifiableEntity at HTTP 400 here, not the 409 DeleteConflict uses.
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"}).StatusCode)

	resp := iamRequest(t, srv, "DeleteRole",
		map[string]string{"RoleName": "AWSServiceRoleForLambda"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	assert.Equal(t, "UnmodifiableEntity", out["__type"])
	assert.Contains(t, out["message"], "protected AWS resources")

	// The role is still there — a refusal must not half-delete.
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "GetRole",
		map[string]string{"RoleName": "AWSServiceRoleForLambda"}).StatusCode)

	// An ordinary role is unaffected, which is what keeps the guard from being a
	// regression in DeleteRole itself.
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateRole",
		map[string]string{"RoleName": "deploy"}).StatusCode)
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "DeleteRole",
		map[string]string{"RoleName": "deploy"}).StatusCode)
}

func TestIAM_DeleteServiceLinkedRole(t *testing.T) {
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"}).StatusCode)

	resp := iamRequest(t, srv, "DeleteServiceLinkedRole",
		map[string]string{"RoleName": "AWSServiceRoleForLambda"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	taskID, ok := out["DeletionTaskId"].(string)
	require.True(t, ok, "the submission returns a DeletionTaskId: %#v", out)

	// AWS documents the format exactly, and both halves matter: the service and the
	// role in the ID are what let a status poll's request resource resolve after the
	// role itself is gone.
	assert.True(t, strings.HasPrefix(taskID,
		"task/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda/"),
		"task ID should follow AWS's documented format, got %q", taskID)

	// The role is gone: nothing here is using it, so the deletion succeeds.
	getResp := iamRequest(t, srv, "GetRole", map[string]string{"RoleName": "AWSServiceRoleForLambda"})
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)

	statusResp := iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": taskID})
	require.Equal(t, http.StatusOK, statusResp.StatusCode)
	var status map[string]any
	decodeIAMXML(t, statusResp, &status)
	assert.Equal(t, "SUCCEEDED", status["Status"])
	assert.NotContains(t, status, "Reason", "a succeeded task reports no failure reason")
}

func TestIAM_DeleteServiceLinkedRole_Refusals(t *testing.T) {
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateRole",
		map[string]string{"RoleName": "ordinary"}).StatusCode)

	tests := []struct {
		name        string
		body        map[string]string
		wantHTTP    int
		wantCode    string
		wantMessage string
	}{
		{
			name:     "RoleName is required",
			body:     map[string]string{},
			wantHTTP: http.StatusBadRequest,
			wantCode: "ValidationError",
		},
		{
			name:     "an unknown role is NoSuchEntity",
			body:     map[string]string{"RoleName": "nosuchrole"},
			wantHTTP: http.StatusNotFound,
			wantCode: "NoSuchEntity",
		},
		{
			// AWS publishes no InvalidInput on this operation, so of the four codes it
			// does publish NoSuchEntity is the only one that fits — there is no
			// service-linked role by that name. The message says which.
			name:        "an ordinary role is NoSuchEntity, and says why",
			body:        map[string]string{"RoleName": "ordinary"},
			wantHTTP:    http.StatusNotFound,
			wantCode:    "NoSuchEntity",
			wantMessage: "is not a service-linked role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := iamRequest(t, srv, "DeleteServiceLinkedRole", tt.body)
			require.Equal(t, tt.wantHTTP, resp.StatusCode)
			var out map[string]any
			decodeIAMXML(t, resp, &out)
			assert.Equal(t, tt.wantCode, out["__type"])
			if tt.wantMessage != "" {
				assert.Contains(t, out["message"], tt.wantMessage)
			}
		})
	}

	// The ordinary role survived the refusal.
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "GetRole",
		map[string]string{"RoleName": "ordinary"}).StatusCode)
}

func TestIAM_GetServiceLinkedRoleDeletionStatus_Refusals(t *testing.T) {
	srv := newIAMTestServer(t)

	resp := iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus", map[string]string{})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	assert.Equal(t, "ValidationError", out["__type"])

	resp = iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": "task/aws-service-role/lambda.amazonaws.com/X/abc"})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	decodeIAMXML(t, resp, &out)
	assert.Equal(t, "NoSuchEntity", out["__type"])
}

func TestIAM_SeededSLRDeletionOutcome(t *testing.T) {
	// The failure AWS documents — "If you submit a deletion request for a
	// service-linked role whose linked service is still accessing a resource, then the
	// deletion task fails" — is unreachable in an emulator that runs no linked service.
	// Seeding it is the only way a consumer's FAILED branch is testable, and the role
	// staying in place is the observable difference between the two outcomes.
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)

	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"}).StatusCode)

	seedSLRDeletionStatus(t, srv, map[string]any{
		"roleName": "AWSServiceRoleForLambda",
		"status":   "FAILED",
		"reason":   "Cannot delete the role because it is still in use.",
		"roleUsageList": []map[string]any{{
			"Region":    "us-east-1",
			"Resources": []string{"arn:aws:lambda:us-east-1:123456789012:function:live"},
		}},
	}, http.StatusOK)

	resp := iamRequest(t, srv, "DeleteServiceLinkedRole",
		map[string]string{"RoleName": "AWSServiceRoleForLambda"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	taskID, ok := out["DeletionTaskId"].(string)
	require.True(t, ok)

	// A failed deletion leaves the role: a caller who polls to FAILED and then reads
	// the role must still find it.
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "GetRole",
		map[string]string{"RoleName": "AWSServiceRoleForLambda"}).StatusCode,
		"a FAILED deletion must not delete the role")

	statusResp := iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": taskID})
	require.Equal(t, http.StatusOK, statusResp.StatusCode)
	var status map[string]any
	decodeIAMXML(t, statusResp, &status)
	assert.Equal(t, "FAILED", status["Status"])
	reason, ok := status["Reason"].(map[string]any)
	require.True(t, ok, "a FAILED task reports a DeletionTaskFailureReasonType: %#v", status)
	assert.Equal(t, "Cannot delete the role because it is still in use.", reason["Reason"])
	usage, ok := reason["RoleUsageList"].([]any)
	require.True(t, ok, "the reason names the resources that must be deleted: %#v", reason)
	require.Len(t, usage, 1)
	first, ok := usage[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "us-east-1", first["Region"])
}

func TestIAM_SeededSLRDeletionInProgressReturnsTheSameTask(t *testing.T) {
	// "If the deletion task is not yet complete, the DeletionTaskId of the existing
	// task is returned" — which is also what keeps a caller's retry from accumulating
	// tasks. Only IN_PROGRESS and NOT_STARTED hold a resubmission back; a FAILED task
	// must not, or a caller who removed the blocking resources could never retry.
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"}).StatusCode)

	seedSLRDeletionStatus(t, srv, map[string]any{
		"roleName": "AWSServiceRoleForLambda", "status": "IN_PROGRESS",
	}, http.StatusOK)

	first := deleteSLRTaskID(t, srv, "AWSServiceRoleForLambda")
	second := deleteSLRTaskID(t, srv, "AWSServiceRoleForLambda")
	assert.Equal(t, first, second, "an incomplete task is returned again, not duplicated")

	status := iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": first})
	var out map[string]any
	decodeIAMXML(t, status, &out)
	assert.Equal(t, "IN_PROGRESS", out["Status"])
}

func TestIAM_SLRDeletionSeedWildcardAndClear(t *testing.T) {
	srv := newIAMTestServer(t)

	// A "*" seed applies to any role, which is what a test seeding one outcome for a
	// whole run wants.
	seedSLRDeletionStatus(t, srv, map[string]any{"status": "NOT_STARTED"}, http.StatusOK)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "events.amazonaws.com"}).StatusCode)
	taskID := deleteSLRTaskID(t, srv, "AWSServiceRoleForCloudWatchEvents")
	var out map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": taskID}), &out)
	assert.Equal(t, "NOT_STARTED", out["Status"])

	// A status outside AWS's four documented values is refused where it is written,
	// not reported later as a status no SDK models.
	seedSLRDeletionStatus(t, srv, map[string]any{"status": "PENDING"}, http.StatusBadRequest)
	seedSLRDeletionStatus(t, srv, map[string]any{}, http.StatusBadRequest)

	// Clearing restores the nominal path.
	clearReq := httptest.NewRequest(http.MethodDelete, "/v1/iam/slr-deletion-status", nil)
	clearW := httptest.NewRecorder()
	srv.ServeHTTP(clearW, clearReq)
	require.Equal(t, http.StatusOK, clearW.Code)

	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "elasticache.amazonaws.com"}).StatusCode)
	nominal := deleteSLRTaskID(t, srv, "AWSServiceRoleForElastiCache")
	decodeIAMXML(t, iamRequest(t, srv, "GetServiceLinkedRoleDeletionStatus",
		map[string]string{"DeletionTaskId": nominal}), &out)
	assert.Equal(t, "SUCCEEDED", out["Status"])
}

// seedSLRDeletionStatus POSTs a deletion-outcome seed and asserts the status code.
func seedSLRDeletionStatus(t *testing.T, srv *emulator.Server, body map[string]any, wantStatus int) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/v1/iam/slr-deletion-status", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, wantStatus, w.Code, "seed response body: %s", w.Body.String())
}

// deleteSLRTaskID submits a deletion and returns the task ID.
func deleteSLRTaskID(t *testing.T, srv *emulator.Server, roleName string) string {
	t.Helper()
	resp := iamRequest(t, srv, "DeleteServiceLinkedRole", map[string]string{"RoleName": roleName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	taskID, ok := out["DeletionTaskId"].(string)
	require.True(t, ok, "expected a DeletionTaskId: %#v", out)
	return taskID
}

func TestIAMSLRRoleName(t *testing.T) {
	// AWS's real names are not derivable — "spot" becomes AWSServiceRoleForEC2Spot,
	// which no transformation of the word produces — and the User Guide explicitly
	// warns against guessing. So the table holds exactly the six principals a bundled
	// statement names in a Resource, and the derivation is substrate's documented
	// convention for everything else.
	tests := []struct {
		serviceName  string
		customSuffix string
		want         string
	}{
		{serviceName: "lambda.amazonaws.com", want: "AWSServiceRoleForLambda"},
		{serviceName: "events.amazonaws.com", want: "AWSServiceRoleForCloudWatchEvents"},
		{serviceName: "ssm.amazonaws.com", want: "AWSServiceRoleForAmazonSSM"},
		{serviceName: "elasticache.amazonaws.com", want: "AWSServiceRoleForElastiCache"},
		{serviceName: "cognito-idp.amazonaws.com", want: "AWSServiceRoleForAmazonCognitoIdp"},
		{serviceName: "email.cognito-idp.amazonaws.com", want: "AWSServiceRoleForAmazonCognitoIdpEmail"},
		// Outside the table, so this is the derivation — and it is what makes
		// AWSBatchFullAccess's arn:aws:iam::*:role/*Batch* match, which is the one
		// bundled statement that scopes a Resource around an untabled principal.
		{serviceName: "batch.amazonaws.com", want: "AWSServiceRoleForBatch"},
		{
			serviceName:  "lambda.amazonaws.com",
			customSuffix: "prod",
			want:         "AWSServiceRoleForLambda_prod",
		},
	}
	for _, tt := range tests {
		t.Run(tt.serviceName+"/"+tt.customSuffix, func(t *testing.T) {
			assert.Equal(t, tt.want,
				emulator.IAMSLRRoleNameForTest(tt.serviceName, tt.customSuffix))
		})
	}
}

func TestIAMSLRDerivedRoleName(t *testing.T) {
	// Substrate's convention for a principal with no citation. Pinned because it is a
	// name a consumer will read back out of GetRole, so changing it is a wire change.
	tests := []struct{ serviceName, want string }{
		{"someservice.amazonaws.com", "AWSServiceRoleForSomeservice"},
		{"foo.application-autoscaling.amazonaws.com", "AWSServiceRoleForFooApplicationAutoscaling"},
		{"a-b-c.amazonaws.com", "AWSServiceRoleForABC"},
		{"noSuffix", "AWSServiceRoleForNoSuffix"},
	}
	for _, tt := range tests {
		t.Run(tt.serviceName, func(t *testing.T) {
			assert.Equal(t, tt.want, emulator.IAMSLRDerivedRoleNameForTest(tt.serviceName))
		})
	}
}

func TestIAMSLRServiceFromRoleName(t *testing.T) {
	tests := []struct{ roleName, want string }{
		{"AWSServiceRoleForLambda", "lambda.amazonaws.com"},
		{"AWSServiceRoleForLambda_prod", "lambda.amazonaws.com"},
		// The longest-match rule: one table value is a prefix of another only in
		// spirit here, but the "_" separator is what actually keeps them apart.
		{"AWSServiceRoleForAmazonCognitoIdp", "cognito-idp.amazonaws.com"},
		{"AWSServiceRoleForAmazonCognitoIdpEmail", "email.cognito-idp.amazonaws.com"},
		// Outside the table the key is absent rather than guessed.
		{"AWSServiceRoleForSomeservice", ""},
		{"ordinary", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.roleName, func(t *testing.T) {
			assert.Equal(t, tt.want, emulator.IAMSLRServiceFromRoleNameForTest(tt.roleName))
		})
	}
}

func TestIAMSLRRoleFromDeletionTaskID(t *testing.T) {
	service, role, ok := emulator.IAMSLRRoleFromDeletionTaskIDForTest(
		"task/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda/" +
			"ec720f7a-c0ba-4838-be33-f72e1873dd52")
	require.True(t, ok)
	assert.Equal(t, "lambda.amazonaws.com", service)
	assert.Equal(t, "AWSServiceRoleForLambda", role)

	for _, bad := range []string{
		"",
		"not-a-task",
		"task/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda",
		"task/aws-service-role//AWSServiceRoleForLambda/uuid",
		"task/aws-service-role/lambda.amazonaws.com//uuid",
	} {
		t.Run("rejects "+bad, func(t *testing.T) {
			_, _, ok := emulator.IAMSLRRoleFromDeletionTaskIDForTest(bad)
			assert.False(t, ok)
		})
	}
}

func TestIAMAuthzRequestContext_PublishesAWSServiceName(t *testing.T) {
	// The key is published where the Service Authorization Reference lists it and
	// nowhere else. Publishing one AWS does not would let a StringEquals succeed here
	// and fail there, which is the permissive direction.
	tests := []struct {
		name      string
		service   string
		operation string
		params    map[string]string
		want      string
	}{
		{
			name:      "CreateServiceLinkedRole publishes the parameter",
			service:   "iam",
			operation: "CreateServiceLinkedRole",
			params:    map[string]string{"AWSServiceName": "lambda.amazonaws.com"},
			want:      "lambda.amazonaws.com",
		},
		{
			name:      "DeleteServiceLinkedRole recovers it from the role name",
			service:   "iam",
			operation: "DeleteServiceLinkedRole",
			params:    map[string]string{"RoleName": "AWSServiceRoleForAmazonSSM"},
			want:      "ssm.amazonaws.com",
		},
		{
			name:      "a role outside the table leaves the key absent",
			service:   "iam",
			operation: "DeleteServiceLinkedRole",
			params:    map[string]string{"RoleName": "AWSServiceRoleForSomeservice"},
		},
		{
			name:      "the status operation does not publish it",
			service:   "iam",
			operation: "GetServiceLinkedRoleDeletionStatus",
			params: map[string]string{
				"DeletionTaskId": "task/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda/u",
			},
		},
		{
			name:      "an unrelated IAM operation publishes nothing",
			service:   "iam",
			operation: "ListRoles",
			params:    map[string]string{"AWSServiceName": "lambda.amazonaws.com"},
		},
		{
			name:      "a non-IAM service publishes nothing",
			service:   "ec2",
			operation: "CreateServiceLinkedRole",
			params:    map[string]string{"AWSServiceName": "lambda.amazonaws.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.IAMAuthzRequestContextForTest(&emulator.AWSRequest{
				Service: tt.service, Operation: tt.operation, Params: tt.params,
			})
			if tt.want == "" {
				assert.NotContains(t, got, "iam:AWSServiceName")
				return
			}
			assert.Equal(t, tt.want, got["iam:AWSServiceName"])
		})
	}
}

func TestIAMAuthzRequestContext_ReadsEitherWireForm(t *testing.T) {
	// A live SDK sends the query form, which populates Params; a caller speaking the
	// JSON protocol directly populates only Body. An authorization decision must not
	// depend on which the caller chose.
	body, err := json.Marshal(map[string]string{"AWSServiceName": "rds.amazonaws.com"})
	require.NoError(t, err)
	got := emulator.IAMAuthzRequestContextForTest(&emulator.AWSRequest{
		Service:   "iam",
		Operation: "CreateServiceLinkedRole",
		Params:    map[string]string{},
		Body:      body,
	})
	assert.Equal(t, "rds.amazonaws.com", got["iam:AWSServiceName"])
}

func TestIAMSLRResourceARN(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()
	role := emulator.IAMRole{
		RoleName: "AWSServiceRoleForLambda",
		ARN:      "arn:aws:iam::123456789012:role/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda",
		Path:     "/aws-service-role/lambda.amazonaws.com/",
	}
	raw, err := json.Marshal(role)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam",
		emulator.IAMRoleKeyForTest("123456789012", role.RoleName), raw))

	ordinary := emulator.IAMRole{RoleName: "deploy", Path: "/"}
	rawOrdinary, err := json.Marshal(ordinary)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam",
		emulator.IAMRoleKeyForTest("123456789012", "deploy"), rawOrdinary))

	reqCtx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	tests := []struct {
		name      string
		operation string
		params    map[string]string
		want      string
	}{
		{
			name:      "the create names the ARN it is about to mint",
			operation: "CreateServiceLinkedRole",
			params:    map[string]string{"AWSServiceName": "lambda.amazonaws.com"},
			want:      role.ARN,
		},
		{
			name:      "a custom suffix is part of the ARN",
			operation: "CreateServiceLinkedRole",
			params:    map[string]string{"AWSServiceName": "lambda.amazonaws.com", "CustomSuffix": "x"},
			want:      role.ARN + "_x",
		},
		{
			name:      "the delete reads the role's stored path",
			operation: "DeleteServiceLinkedRole",
			params:    map[string]string{"RoleName": "AWSServiceRoleForLambda"},
			want:      role.ARN,
		},
		{
			// Not the resource shape those statements are written about, so the
			// request stays on the general path rather than getting an ARN that only
			// looks specific.
			name:      "an ordinary role leaves the request on the general path",
			operation: "DeleteServiceLinkedRole",
			params:    map[string]string{"RoleName": "deploy"},
		},
		{
			name:      "an unknown role leaves the request on the general path",
			operation: "DeleteServiceLinkedRole",
			params:    map[string]string{"RoleName": "ghost"},
		},
		{
			// The task ID carries the service and the role, so this resolves with no
			// state read — which is what makes it work after the role is deleted.
			name:      "the status operation resolves from the task ID alone",
			operation: "GetServiceLinkedRoleDeletionStatus",
			params: map[string]string{
				"DeletionTaskId": "task/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda/u",
			},
			want: role.ARN,
		},
		{
			name:      "a malformed task ID leaves the request on the general path",
			operation: "GetServiceLinkedRoleDeletionStatus",
			params:    map[string]string{"DeletionTaskId": "garbage"},
		},
		{
			name:      "an unrelated operation leaves the request on the general path",
			operation: "ListRoles",
			params:    map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.IAMSLRResourceARNForTest(state, reqCtx, &emulator.AWSRequest{
				Service: "iam", Operation: tt.operation, Params: tt.params,
			})
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIAM_BundledPolicyGrantsCreateServiceLinkedRole(t *testing.T) {
	// The point of #747. AWSLambda_FullAccess scopes its Resource to an exact
	// service-linked-role ARN and conditions on iam:AWSServiceName. Before this both
	// halves failed: the request resource was a flat arn:aws:iam::<acct>:* that the
	// pattern could not match, and nothing published the condition key. And the two
	// doors have to agree — a 403 from the plugin after the gate allowed is the
	// one-ARN-two-answers class of #411.
	const account = "123456789012"
	const policyARN = "arn:aws:iam::aws:policy/AWSLambda_FullAccess"

	tests := []struct {
		name       string
		service    string
		wantDenied bool
	}{
		{
			name:    "the service the statement names is allowed",
			service: "lambda.amazonaws.com",
		},
		{
			// The condition is what stops the grant from being unconditional, and it
			// only bites once the key is published at all.
			name:       "another service is denied by the condition",
			service:    "rds.amazonaws.com",
			wantDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := emulator.NewMemoryStateManager()
			ctx := context.Background()

			userRaw, err := json.Marshal(emulator.IAMUser{
				UserName: "alice",
				ARN:      "arn:aws:iam::" + account + ":user/alice",
				Path:     "/",
			})
			require.NoError(t, err)
			require.NoError(t, state.Put(ctx, "iam",
				emulator.IAMUserKeyForTest(account, "alice"), userRaw))
			attachedRaw, err := json.Marshal([]string{policyARN})
			require.NoError(t, err)
			require.NoError(t, state.Put(ctx, "iam",
				emulator.IAMAttachedPoliciesKeyForTest(account, "user", "alice"), attachedRaw))

			logger := emulator.NewDefaultLogger(slog.LevelError, false)
			auth := emulator.NewAuthController(state, logger)
			plugin := &emulator.IAMPlugin{}
			require.NoError(t, plugin.Initialize(ctx,
				emulator.PluginConfig{State: state, Logger: logger}))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: account,
				Region:    "us-east-1",
				Principal: &emulator.Principal{
					ARN:      "arn:aws:iam::" + account + ":user/alice",
					Type:     "IAMUser",
					UserName: "alice",
				},
				Metadata: make(map[string]interface{}),
			}
			req := &emulator.AWSRequest{
				Service:   "iam",
				Operation: "CreateServiceLinkedRole",
				Params:    map[string]string{"AWSServiceName": tt.service},
			}

			gateErr := auth.CheckAccess(reqCtx, req)
			roleARN := "arn:aws:iam::" + account + ":role" +
				emulator.IAMSLRPathForTest(tt.service) +
				emulator.IAMSLRRoleNameForTest(tt.service, "")
			pluginErr := emulator.IAMAuthorizeWithForTest(plugin, reqCtx,
				"iam:CreateServiceLinkedRole", roleARN,
				map[string]string{"iam:AWSServiceName": tt.service})

			if tt.wantDenied {
				assert.Error(t, gateErr, "CheckAccess should deny")
				assert.Error(t, pluginErr, "IAMPlugin.authorizeWith should deny")
				return
			}
			assert.NoError(t, gateErr, "CheckAccess should allow")
			assert.NoError(t, pluginErr, "IAMPlugin.authorizeWith should allow")
		})
	}
}

func TestIAM_ServiceLinkedRoleEndToEndUnderTheBundledPolicy(t *testing.T) {
	// The whole chain through the server, which is what a consumer sees: a user
	// holding only AWSLambda_FullAccess creates Lambda's service-linked role and is
	// refused another service's. Before #747 the first was a 403 and the operation
	// itself answered InvalidAction.
	const account = "123456789012"
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	userRaw, err := json.Marshal(emulator.IAMUser{
		UserName: "alice", ARN: "arn:aws:iam::" + account + ":user/alice", Path: "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMUserKeyForTest(account, "alice"), userRaw))
	attachedRaw, err := json.Marshal([]string{"arn:aws:iam::aws:policy/AWSLambda_FullAccess"})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam",
		emulator.IAMAttachedPoliciesKeyForTest(account, "user", "alice"), attachedRaw))

	srv := newIAMTestServerWithState(t, state)
	plugin := &emulator.IAMPlugin{}
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	require.NoError(t, plugin.Initialize(ctx, emulator.PluginConfig{State: state, Logger: logger}))

	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: account,
		Region:    "us-east-1",
		Principal: &emulator.Principal{
			ARN: "arn:aws:iam::" + account + ":user/alice", Type: "IAMUser", UserName: "alice",
		},
		Metadata: make(map[string]interface{}),
	}

	allowed := emulator.IAMAuthorizeWithForTest(plugin, reqCtx, "iam:CreateServiceLinkedRole",
		"arn:aws:iam::"+account+":role/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda",
		map[string]string{"iam:AWSServiceName": "lambda.amazonaws.com"})
	require.NoError(t, allowed, "the bundled policy grants exactly this")

	// And the operation works, unauthenticated, which is the default path.
	resp := iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	role, ok := out["Role"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AWSServiceRoleForLambda", role["RoleName"])
}
