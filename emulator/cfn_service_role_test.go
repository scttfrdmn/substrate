package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A stack's resource calls were dispatched with no principal at all, and
// PluginRegistry.RouteRequest — which is what an in-process deploy routes through —
// never authorizes. So a template asking for a permission the deploying identity
// did not have deployed cleanly, which is the failure class #411 exists to catch:
// in spore-host/spawn#70 a worker launched with an instance profile granting no SQS
// permissions passed every composition test and then never drained a queue on real
// AWS.

// cfnRoleTestClient drives a TestServer over HTTP with the CloudFormation query
// protocol, signed as the given access key.
type cfnRoleTestClient struct {
	t         *testing.T
	baseURL   string
	accessKey string
}

// call issues a CloudFormation query request and returns the status and body.
func (c *cfnRoleTestClient) call(action string, params map[string]string) (int, string) {
	c.t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2010-05-15"}}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		c.baseURL+"/", strings.NewReader(form.Encode()))
	require.NoError(c.t, err)
	req.Host = "cloudformation.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.accessKey != "" {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.accessKey+
			"/20250101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=unverified")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(c.t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	require.NoError(c.t, resp.Body.Close())
	return resp.StatusCode, string(body)
}

// cfnRoleBucketTemplate is a template declaring one bucket, which is enough: the
// question under test is whose permissions the CreateBucket call is made with.
const cfnRoleBucketTemplate = `{
  "Resources": {
    "Data": {
      "Type": "AWS::S3::Bucket",
      "Properties": {"BucketName": "role-test-bucket"}
    }
  }
}`

// cfnSeedRole writes an IAM role and, when doc is non-empty, an inline policy
// granting it. Written straight to state rather than through the IAM API because
// what is under test is the deployment, not IAM's own request handling.
func cfnSeedRole(t *testing.T, state emulator.StateManager, roleName string, actions []string) {
	t.Helper()
	ctx := context.Background()

	roleRaw, err := json.Marshal(emulator.IAMRole{
		RoleName: roleName,
		RoleID:   "AROATEST" + roleName,
		ARN:      "arn:aws:iam::123456789012:role/" + roleName,
		Path:     "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "role:"+roleName, roleRaw))

	if len(actions) == 0 {
		return
	}
	policyARN := "arn:aws:iam::123456789012:policy/" + roleName + "Policy"
	arnsRaw, err := json.Marshal([]string{policyARN})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "role_policies:"+roleName, arnsRaw))

	polRaw, err := json.Marshal(emulator.IAMPolicy{
		PolicyName: roleName + "Policy",
		ARN:        policyARN,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice(actions),
				Resource: emulator.StringOrSlice{"*"},
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))
}

// cfnStackStatus reads a stack's status and RoleARN out of a DescribeStacks body.
func cfnStackStatus(t *testing.T, body string) (status, roleARN, reason string) {
	t.Helper()
	var parsed struct {
		Stacks []struct {
			StackStatus       string `xml:"StackStatus"`
			StackStatusReason string `xml:"StackStatusReason"`
			RoleARN           string `xml:"RoleARN"`
		} `xml:"DescribeStacksResult>Stacks>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed), "body: %s", body)
	require.Len(t, parsed.Stacks, 1, "body: %s", body)
	return parsed.Stacks[0].StackStatus, parsed.Stacks[0].RoleARN, parsed.Stacks[0].StackStatusReason
}

func TestCFN_ServiceRoleAuthorizesResourceCalls(t *testing.T) {
	// #411's acceptance and #562's, end to end over HTTP: a stack deployed under a
	// service role that cannot create the resource fails, and the same template
	// under a role that can succeeds. Nothing about the template differs — only the
	// permissions of the identity CloudFormation makes the calls with.
	tests := []struct {
		name       string
		roleName   string
		actions    []string
		wantStatus string
		wantReason string
	}{
		{
			// The point of the release. Before this, a role granting nothing
			// deployed the bucket without complaint.
			name:       "a role that cannot create the bucket rolls the stack back",
			roleName:   "narrow",
			actions:    []string{"s3:ListBucket"},
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "AccessDeniedException",
		},
		{
			name:       "a role that can create the bucket deploys",
			roleName:   "wide",
			actions:    []string{"s3:*"},
			wantStatus: "CREATE_COMPLETE",
		},
		{
			// A role that grants nothing at all is an implicit deny, which is what
			// a freshly created role with no policy attached is on AWS. This is the
			// "forgot to attach the policy" case, distinct from "the policy is too
			// narrow" above.
			name:       "a role with no policy at all rolls the stack back",
			roleName:   "empty",
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "AccessDeniedException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			cfnSeedRole(t, ts.StateManager(), tt.roleName, tt.actions)
			client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

			code, body := client.call("CreateStack", map[string]string{
				"StackName":    "rolestack",
				"TemplateBody": cfnRoleBucketTemplate,
				"RoleARN":      "arn:aws:iam::123456789012:role/" + tt.roleName,
			})
			// A rolled-back stack is still a successful CreateStack call: the API
			// returns its StackId before any rollback happens, and the outcome is
			// the stack's status.
			require.Equal(t, http.StatusOK, code, "body: %s", body)

			_, body = client.call("DescribeStacks", map[string]string{"StackName": "rolestack"})
			status, roleARN, reason := cfnStackStatus(t, body)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, "arn:aws:iam::123456789012:role/"+tt.roleName, roleARN,
				"the service role round-trips through DescribeStacks")
			if tt.wantReason != "" {
				assert.Contains(t, reason, tt.wantReason,
					"the denial must name itself, not just a status")
			}
		})
	}
}

func TestCFN_DeniedResourceNamesTheDenialInStackResources(t *testing.T) {
	// #562's reporting criterion, minus the StackEvent one that is blocked by #501.
	// A caller has to be able to learn *which* resource was refused and *why*, not
	// only that the stack rolled back — that is the difference between a test
	// failure a consumer can act on and one that sends them to real AWS to find out.
	ts := emulator.StartTestServer(t)
	cfnSeedRole(t, ts.StateManager(), "narrow", []string{"s3:ListBucket"})
	client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

	code, body := client.call("CreateStack", map[string]string{
		"StackName":    "reasonstack",
		"TemplateBody": cfnRoleBucketTemplate,
		"RoleARN":      "arn:aws:iam::123456789012:role/narrow",
	})
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	_, body = client.call("DescribeStackResources", map[string]string{"StackName": "reasonstack"})
	var parsed struct {
		Resources []struct {
			LogicalResourceID    string `xml:"LogicalResourceId"`
			ResourceStatus       string `xml:"ResourceStatus"`
			ResourceStatusReason string `xml:"ResourceStatusReason"`
		} `xml:"DescribeStackResourcesResult>StackResources>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed), "body: %s", body)
	require.NotEmpty(t, parsed.Resources, "body: %s", body)

	res := parsed.Resources[0]
	assert.Equal(t, "Data", res.LogicalResourceID)
	assert.Contains(t, res.ResourceStatusReason, "AccessDeniedException")
	assert.Contains(t, res.ResourceStatusReason, "s3:CreateBucket",
		"the reason names the action that was refused")
}

func TestCFN_RoleARNSemantics(t *testing.T) {
	// The documented lifetime rules, which are not the convenient ones: a service
	// role governs "all future operations on the stack", so an UpdateStack that
	// omits RoleARN keeps it, while a DeleteStack's applies to that one operation
	// and is not persisted.
	t.Run("a stack without a role reports none", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

		code, body := client.call("CreateStack", map[string]string{
			"StackName":    "noroles",
			"TemplateBody": cfnRoleBucketTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		_, body = client.call("DescribeStacks", map[string]string{"StackName": "noroles"})
		status, roleARN, _ := cfnStackStatus(t, body)
		// The non-breaking gate: no role and no resolvable caller means nothing is
		// enforced, exactly as before this existed.
		assert.Equal(t, "CREATE_COMPLETE", status)
		assert.Empty(t, roleARN, "a stack with no service role reports no RoleARN at all")
	})

	t.Run("an update that omits RoleARN keeps the stored one", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		cfnSeedRole(t, ts.StateManager(), "wide", []string{"s3:*"})
		client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

		code, body := client.call("CreateStack", map[string]string{
			"StackName":    "keeprole",
			"TemplateBody": cfnRoleBucketTemplate,
			"RoleARN":      "arn:aws:iam::123456789012:role/wide",
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		code, body = client.call("UpdateStack", map[string]string{
			"StackName":    "keeprole",
			"TemplateBody": cfnRoleBucketTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		_, body = client.call("DescribeStacks", map[string]string{"StackName": "keeprole"})
		_, roleARN, _ := cfnStackStatus(t, body)
		assert.Equal(t, "arn:aws:iam::123456789012:role/wide", roleARN,
			"an omitted RoleARN uses the role previously associated with the stack")
	})

	t.Run("an update that supplies RoleARN replaces the stored one", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		cfnSeedRole(t, ts.StateManager(), "wide", []string{"s3:*"})
		cfnSeedRole(t, ts.StateManager(), "wider", []string{"s3:*", "sqs:*"})
		client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

		code, body := client.call("CreateStack", map[string]string{
			"StackName":    "swaprole",
			"TemplateBody": cfnRoleBucketTemplate,
			"RoleARN":      "arn:aws:iam::123456789012:role/wide",
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		code, body = client.call("UpdateStack", map[string]string{
			"StackName":    "swaprole",
			"TemplateBody": cfnRoleBucketTemplate,
			"RoleARN":      "arn:aws:iam::123456789012:role/wider",
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		_, body = client.call("DescribeStacks", map[string]string{"StackName": "swaprole"})
		_, roleARN, _ := cfnStackStatus(t, body)
		assert.Equal(t, "arn:aws:iam::123456789012:role/wider", roleARN)
	})

	t.Run("a delete's RoleARN applies to that operation only", func(t *testing.T) {
		// The asymmetry worth testing: CreateStack and UpdateStack document "always
		// uses this role for all future operations", and DeleteStack does not. A
		// delete refused by its override must leave the stack's own role intact, or
		// a retried delete would silently run as a different identity than the
		// first attempt.
		ts := emulator.StartTestServer(t)
		cfnSeedRole(t, ts.StateManager(), "wide", []string{"s3:*"})
		cfnSeedRole(t, ts.StateManager(), "cannotdelete", []string{"s3:CreateBucket"})
		client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

		code, body := client.call("CreateStack", map[string]string{
			"StackName":    "delrole",
			"TemplateBody": cfnRoleBucketTemplate,
			"RoleARN":      "arn:aws:iam::123456789012:role/wide",
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		// DeleteStack answers 200 even when the sweep fails: the outcome is the
		// stack's status, which the caller polls for.
		code, body = client.call("DeleteStack", map[string]string{
			"StackName": "delrole",
			"RoleARN":   "arn:aws:iam::123456789012:role/cannotdelete",
		})
		require.Equal(t, http.StatusOK, code, "body: %s", body)

		_, body = client.call("DescribeStacks", map[string]string{"StackName": "delrole"})
		status, roleARN, _ := cfnStackStatus(t, body)
		assert.Equal(t, "DELETE_FAILED", status,
			"the override's role cannot delete the bucket, so the sweep fails")
		assert.Equal(t, "arn:aws:iam::123456789012:role/wide", roleARN,
			"a delete's RoleARN is not persisted onto the stack")
	})

	t.Run("a RoleARN shorter than the model allows is refused", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

		code, body := client.call("CreateStack", map[string]string{
			"StackName":    "badrole",
			"TemplateBody": cfnRoleBucketTemplate,
			"RoleARN":      "arn:aws:iam::1:r",
		})
		assert.Equal(t, http.StatusBadRequest, code, "body: %s", body)
		assert.Contains(t, body, "ValidationError")
	})
}

func TestCFN_DeleteRunsAsTheStacksRole(t *testing.T) {
	// A resource created by a role is deleted by that role, not by whoever happens
	// to ask for the delete. Without this a stack deployed under a narrow role could
	// be swept by an unauthorized delete, which would make the enforcement decorative:
	// the permissions that gated creation would not gate teardown.
	ts := emulator.StartTestServer(t)
	cfnSeedRole(t, ts.StateManager(), "createonly", []string{"s3:CreateBucket"})
	client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

	code, body := client.call("CreateStack", map[string]string{
		"StackName":    "sweepstack",
		"TemplateBody": cfnRoleBucketTemplate,
		"RoleARN":      "arn:aws:iam::123456789012:role/createonly",
	})
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	_, body = client.call("DescribeStacks", map[string]string{"StackName": "sweepstack"})
	status, _, reason := cfnStackStatus(t, body)
	require.Equal(t, "CREATE_COMPLETE", status, "reason: %s", reason)

	// The same role cannot delete a bucket, so the sweep fails and says so.
	code, body = client.call("DeleteStack", map[string]string{"StackName": "sweepstack"})
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	_, body = client.call("DescribeStacks", map[string]string{"StackName": "sweepstack"})
	status, _, _ = cfnStackStatus(t, body)
	assert.Equal(t, "DELETE_FAILED", status,
		"the delete is attributed to the stack's role, which cannot delete")
}

// cfnSeedUserWithKey writes an IAM user, an access key for it, and a policy
// allowing actions on every resource, returning the access key ID to sign with.
func cfnSeedUserWithKey(t *testing.T, state emulator.StateManager, user, keyID string, actions []string) string {
	t.Helper()
	ctx := context.Background()

	userRaw, err := json.Marshal(emulator.IAMUser{
		UserName: user,
		UserID:   "AIDA" + user,
		ARN:      "arn:aws:iam::123456789012:user/" + user,
		Path:     "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "user:"+user, userRaw))

	policyARN := "arn:aws:iam::123456789012:policy/" + user + "Policy"
	arnsRaw, err := json.Marshal([]string{policyARN})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "user_policies:"+user, arnsRaw))

	polRaw, err := json.Marshal(emulator.IAMPolicy{
		PolicyName: user + "Policy",
		ARN:        policyARN,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice(actions),
				Resource: emulator.StringOrSlice{"*"},
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))

	keyRaw, err := json.Marshal(emulator.IAMAccessKey{
		AccessKeyID: keyID,
		UserName:    user,
		Status:      "Active",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "accesskey:"+keyID, keyRaw))
	return keyID
}

func TestCFN_ServiceRoleWinsOverTheCreatingPrincipal(t *testing.T) {
	// The one case where the resolution *order* is observable, and therefore the
	// only thing that measures it: both a service role and a creating principal
	// exist, and they disagree. A service role is what CloudFormation "always uses
	// for all future operations on the stack", so it wins — which means a caller
	// cannot widen a narrow deploy role by holding a broader policy of their own,
	// and cannot be blamed for a role's refusal either.
	tests := []struct {
		name        string
		roleActions []string
		userActions []string
		wantStatus  string
	}{
		{
			// If the creator won, this would deploy — the caller can create buckets.
			// It must not: the stack's role cannot.
			name:        "a narrow role refuses what the broad creator could do",
			roleActions: []string{"s3:ListBucket"},
			userActions: []string{"cloudformation:*", "s3:*"},
			wantStatus:  "ROLLBACK_COMPLETE",
		},
		{
			// The converse, which pins the order rather than merely "the role is
			// consulted": if the creator won, this would roll back.
			name:        "a broad role deploys what the narrow creator could not",
			roleActions: []string{"s3:*"},
			userActions: []string{"cloudformation:*"},
			wantStatus:  "CREATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			state := ts.StateManager()
			cfnSeedRole(t, state, "deployrole", tt.roleActions)
			keyID := cfnSeedUserWithKey(t, state, "caller", "AKIACALLER0000000001", tt.userActions)
			client := &cfnRoleTestClient{t: t, baseURL: ts.URL, accessKey: keyID}

			code, body := client.call("CreateStack", map[string]string{
				"StackName":    "orderstack",
				"TemplateBody": cfnRoleBucketTemplate,
				"RoleARN":      "arn:aws:iam::123456789012:role/deployrole",
			})
			require.Equal(t, http.StatusOK, code, "body: %s", body)

			_, body = client.call("DescribeStacks", map[string]string{"StackName": "orderstack"})
			status, roleARN, reason := cfnStackStatus(t, body)
			assert.Equal(t, tt.wantStatus, status, "reason: %s", reason)
			assert.Equal(t, "arn:aws:iam::123456789012:role/deployrole", roleARN)
		})
	}
}

func TestCFN_CreatorPrincipalDeploysWhenThereIsNoRole(t *testing.T) {
	// CloudFormation's no-service-role case: absent a role it uses "a temporary
	// session that's generated from your user credentials", so a stack created by a
	// caller whose policy does not allow the resource must fail — otherwise
	// CreateStack would be a way to launder a permission the caller does not have.
	ts := emulator.StartTestServer(t)
	state := ts.StateManager()
	ctx := context.Background()

	// A user with an access key, and a policy that allows CloudFormation but not S3.
	userRaw, err := json.Marshal(emulator.IAMUser{
		UserName: "deployer",
		UserID:   "AIDATEST",
		ARN:      "arn:aws:iam::123456789012:user/deployer",
		Path:     "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "user:deployer", userRaw))

	policyARN := "arn:aws:iam::123456789012:policy/CFNOnly"
	arnsRaw, err := json.Marshal([]string{policyARN})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "user_policies:deployer", arnsRaw))
	polRaw, err := json.Marshal(emulator.IAMPolicy{
		PolicyName: "CFNOnly",
		ARN:        policyARN,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"cloudformation:*"},
				Resource: emulator.StringOrSlice{"*"},
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))

	const keyID = "AKIADEPLOYER00000001"
	keyRaw, err := json.Marshal(emulator.IAMAccessKey{
		AccessKeyID: keyID,
		UserName:    "deployer",
		Status:      "Active",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "accesskey:"+keyID, keyRaw))

	client := &cfnRoleTestClient{t: t, baseURL: ts.URL, accessKey: keyID}

	code, body := client.call("CreateStack", map[string]string{
		"StackName":    "creatorstack",
		"TemplateBody": cfnRoleBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	_, body = client.call("DescribeStacks", map[string]string{"StackName": "creatorstack"})
	status, roleARN, reason := cfnStackStatus(t, body)
	assert.Equal(t, "ROLLBACK_COMPLETE", status)
	assert.Empty(t, roleARN, "the stack has no service role; the creator is not one")
	assert.Contains(t, reason, "AccessDeniedException")
}
