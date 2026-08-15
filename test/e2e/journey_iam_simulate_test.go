package e2e_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// The point of the v0.100.0 milestone, driven through the real SDK: a tool asking
// whether it may act, before acting.
//
// This level is the one that matters for these two operations. A unit test calls the
// handler directly and so never learns whether the operation is routed at all — that
// was #636 — and it hand-builds a JSON body with real arrays, while every parameter
// SimulatePrincipalPolicy takes is a query-protocol `.member.N` list that arrives as
// a flat map of strings (#639). Both failure modes are invisible below this line.

// TestJourney_IAMSimulatePrincipalPolicy asserts all three decisions in a single
// response, from three different policy sources.
//
// The three-way distinction is the feature. A handler that returned one uniform
// answer — or that collapsed explicitDeny into implicitDeny — would satisfy a test
// that checked each action separately, so they are checked together: one call, three
// actions, three different decisions, each traced to the policy that produced it.
func TestJourney_IAMSimulatePrincipalPolicy(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response.
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	const (
		userName  = "jill"
		groupName = "devs"
		bucketARN = "arn:aws:s3:::my-test-bucket/object"
		// The group's grant. Bundled, so the simulation reads a real AWS document.
		managedARN  = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
		managedName = "AmazonS3ReadOnlyAccess"
		inlineName  = "nodelete"
	)
	userARN := "arn:aws:iam::" + journeyAccountID + ":user/" + userName

	if _, err := client.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(userName),
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if _, err := client.CreateGroup(ctx, &iam.CreateGroupInput{
		GroupName: aws.String(groupName),
	}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if _, err := client.AddUserToGroup(ctx, &iam.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	}); err != nil {
		t.Fatalf("AddUserToGroup: %v", err)
	}
	// The allow arrives through the *group*, which is why groups had to become real
	// before the simulator could be right: AWS includes a user's group policies in the
	// simulation, and a simulation missing them is wrong rather than partial.
	if _, err := client.AttachGroupPolicy(ctx, &iam.AttachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: aws.String(managedARN),
	}); err != nil {
		t.Fatalf("AttachGroupPolicy: %v", err)
	}
	// The deny is inline on the user, and it is explicit — the distinction the API
	// exists to report.
	if _, err := client.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:   aws.String(userName),
		PolicyName: aws.String(inlineName),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Sid":"NoDelete",` +
			`"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}]}`),
	}); err != nil {
		t.Fatalf("PutUserPolicy: %v", err)
	}

	out, err := client.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String(userARN),
		// Three actions in one call: allowed by the group's managed policy, denied
		// explicitly by the inline policy, and granted by nothing at all.
		ActionNames:  []string{"s3:GetObject", "s3:DeleteObject", "s3:PutObject"},
		ResourceArns: []string{bucketARN},
	})
	if err != nil {
		t.Fatalf("SimulatePrincipalPolicy: %v", err)
	}
	if len(out.EvaluationResults) != 3 {
		t.Fatalf("got %d evaluation results, want 3 (one per action)", len(out.EvaluationResults))
	}

	byAction := make(map[string]iamtypes.EvaluationResult, len(out.EvaluationResults))
	for _, result := range out.EvaluationResults {
		byAction[aws.ToString(result.EvalActionName)] = result
	}

	want := map[string]iamtypes.PolicyEvaluationDecisionType{
		"s3:GetObject":    iamtypes.PolicyEvaluationDecisionTypeAllowed,
		"s3:DeleteObject": iamtypes.PolicyEvaluationDecisionTypeExplicitDeny,
		"s3:PutObject":    iamtypes.PolicyEvaluationDecisionTypeImplicitDeny,
	}
	for action, wantDecision := range want {
		result, ok := byAction[action]
		if !ok {
			t.Fatalf("no evaluation result for %s; got %v", action, byAction)
		}
		if result.EvalDecision != wantDecision {
			t.Errorf("%s: EvalDecision = %q, want %q", action, result.EvalDecision, wantDecision)
		}
		if aws.ToString(result.EvalResourceName) != bucketARN {
			t.Errorf("%s: EvalResourceName = %q, want %q",
				action, aws.ToString(result.EvalResourceName), bucketARN)
		}
	}

	// MatchedStatements names *which* policy decided, which is the whole output of a
	// simulation: a decision with no source tells a caller nothing it can act on.
	if got := matchedSourceIDs(byAction["s3:GetObject"]); !containsString(got, managedName) {
		t.Errorf("s3:GetObject matched %v, want the group's %s among them", got, managedName)
	}
	if got := matchedSourceIDs(byAction["s3:DeleteObject"]); !containsString(got, inlineName) {
		t.Errorf("s3:DeleteObject matched %v, want the inline %s among them", got, inlineName)
	}
	// Nothing matched the ungranted action — an implicit deny is the absence of a
	// statement, so a source here would mean the emulator invented one.
	if got := matchedSourceIDs(byAction["s3:PutObject"]); len(got) != 0 {
		t.Errorf("s3:PutObject matched %v, want nothing for an implicit deny", got)
	}

	// Removing the group takes the allow with it: the grant was never on the user.
	if _, err := client.RemoveUserFromGroup(ctx, &iam.RemoveUserFromGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	}); err != nil {
		t.Fatalf("RemoveUserFromGroup: %v", err)
	}
	after, err := client.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String(userARN),
		ActionNames:     []string{"s3:GetObject"},
		ResourceArns:    []string{bucketARN},
	})
	if err != nil {
		t.Fatalf("SimulatePrincipalPolicy after removal: %v", err)
	}
	if len(after.EvaluationResults) != 1 {
		t.Fatalf("got %d results after removal, want 1", len(after.EvaluationResults))
	}
	if got := after.EvaluationResults[0].EvalDecision; got != iamtypes.PolicyEvaluationDecisionTypeImplicitDeny {
		t.Errorf("s3:GetObject after leaving the group = %q, want implicitDeny", got)
	}
}

// TestJourney_IAMSimulatePrincipalPolicy_UnknownPrincipal pins the error the
// principal form has and the custom form does not.
func TestJourney_IAMSimulatePrincipalPolicy_UnknownPrincipal(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	_, err = client.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String("arn:aws:iam::" + journeyAccountID + ":user/ghost"),
		ActionNames:     []string{"s3:GetObject"},
	})
	if err == nil {
		t.Fatal("SimulatePrincipalPolicy on an unknown user succeeded, want NoSuchEntity")
	}
	// Asserting through the SDK's typed error is what proves the wire shape is right:
	// a plain 404 with the wrong code does not deserialize into this type.
	var notFound *iamtypes.NoSuchEntityException
	if !errors.As(err, &notFound) {
		t.Fatalf("SimulatePrincipalPolicy error = %v, want *NoSuchEntityException", err)
	}
}

// TestJourney_IAMSimulateCustomPolicy evaluates policies that were never stored.
//
// The custom form resolves no entity, so it takes documents inline and declares no
// NoSuchEntity at all. It is also the form a tool uses to check a policy it is about
// to write, which is the case that cannot be tested any other way.
func TestJourney_IAMSimulateCustomPolicy(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	out, err := client.SimulateCustomPolicy(ctx, &iam.SimulateCustomPolicyInput{
		PolicyInputList: []string{
			`{"Version":"2012-10-17","Statement":[{"Sid":"Read","Effect":"Allow",` +
				`"Action":"s3:GetObject","Resource":"*"}]}`,
		},
		ActionNames: []string{"s3:GetObject", "s3:PutObject"},
	})
	if err != nil {
		t.Fatalf("SimulateCustomPolicy: %v", err)
	}
	if len(out.EvaluationResults) != 2 {
		t.Fatalf("got %d results, want 2", len(out.EvaluationResults))
	}

	byAction := make(map[string]iamtypes.EvaluationResult, len(out.EvaluationResults))
	for _, result := range out.EvaluationResults {
		byAction[aws.ToString(result.EvalActionName)] = result
		// ResourceArns was not supplied, so it defaults to "*" per the reference.
		if got := aws.ToString(result.EvalResourceName); got != "*" {
			t.Errorf("%s: EvalResourceName = %q, want the default *",
				aws.ToString(result.EvalActionName), got)
		}
	}
	if got := byAction["s3:GetObject"].EvalDecision; got != iamtypes.PolicyEvaluationDecisionTypeAllowed {
		t.Errorf("s3:GetObject = %q, want allowed", got)
	}
	if got := byAction["s3:PutObject"].EvalDecision; got != iamtypes.PolicyEvaluationDecisionTypeImplicitDeny {
		t.Errorf("s3:PutObject = %q, want implicitDeny", got)
	}

	// A boundary caps what the identity policy reaches. The allow above stays an
	// allow only while the boundary permits it, and the result is an *implicit* deny:
	// a boundary limits reachability rather than denying an action by name.
	bounded, err := client.SimulateCustomPolicy(ctx, &iam.SimulateCustomPolicyInput{
		PolicyInputList: []string{
			`{"Version":"2012-10-17","Statement":[{"Sid":"Read","Effect":"Allow",` +
				`"Action":"s3:GetObject","Resource":"*"}]}`,
		},
		PermissionsBoundaryPolicyInputList: []string{
			`{"Version":"2012-10-17","Statement":[{"Sid":"OnlyList","Effect":"Allow",` +
				`"Action":"s3:ListBucket","Resource":"*"}]}`,
		},
		ActionNames: []string{"s3:GetObject"},
	})
	if err != nil {
		t.Fatalf("SimulateCustomPolicy with a boundary: %v", err)
	}
	if len(bounded.EvaluationResults) != 1 {
		t.Fatalf("got %d results, want 1", len(bounded.EvaluationResults))
	}
	result := bounded.EvaluationResults[0]
	if result.EvalDecision != iamtypes.PolicyEvaluationDecisionTypeImplicitDeny {
		t.Errorf("s3:GetObject under a boundary that omits it = %q, want implicitDeny",
			result.EvalDecision)
	}
	if result.PermissionsBoundaryDecisionDetail == nil {
		t.Fatal("PermissionsBoundaryDecisionDetail is absent; a caller cannot tell the " +
			"boundary was what refused")
	}
	if result.PermissionsBoundaryDecisionDetail.AllowedByPermissionsBoundary {
		t.Error("AllowedByPermissionsBoundary = true for an action the boundary omits")
	}
}

// TestJourney_IAMSimulateCustomPolicy_MissingContextValues asserts that a
// conditional grant does not read as a clean refusal.
//
// Without this, a policy allowing an action only when a condition key matches looks
// identical to one that never granted it — so a consumer's preflight would report
// "you cannot" where the truth is "not with what you told me".
func TestJourney_IAMSimulateCustomPolicy_MissingContextValues(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	const conditional = `{"Version":"2012-10-17","Statement":[{"Sid":"TaggedOnly",` +
		`"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":` +
		`{"StringEquals":{"aws:RequestTag/env":"prod"}}}]}`

	out, err := client.SimulateCustomPolicy(ctx, &iam.SimulateCustomPolicyInput{
		PolicyInputList: []string{conditional},
		ActionNames:     []string{"s3:GetObject"},
	})
	if err != nil {
		t.Fatalf("SimulateCustomPolicy: %v", err)
	}
	if len(out.EvaluationResults) != 1 {
		t.Fatalf("got %d results, want 1", len(out.EvaluationResults))
	}
	if got := out.EvaluationResults[0].MissingContextValues; !containsString(got, "aws:RequestTag/env") {
		t.Errorf("MissingContextValues = %v, want aws:RequestTag/env among them", got)
	}

	// Supplied, the same simulation allows — and reports nothing missing.
	supplied, err := client.SimulateCustomPolicy(ctx, &iam.SimulateCustomPolicyInput{
		PolicyInputList: []string{conditional},
		ActionNames:     []string{"s3:GetObject"},
		ContextEntries: []iamtypes.ContextEntry{{
			ContextKeyName:   aws.String("aws:RequestTag/env"),
			ContextKeyType:   iamtypes.ContextKeyTypeEnumString,
			ContextKeyValues: []string{"prod"},
		}},
	})
	if err != nil {
		t.Fatalf("SimulateCustomPolicy with a context entry: %v", err)
	}
	if len(supplied.EvaluationResults) != 1 {
		t.Fatalf("got %d results, want 1", len(supplied.EvaluationResults))
	}
	if got := supplied.EvaluationResults[0].EvalDecision; got != iamtypes.PolicyEvaluationDecisionTypeAllowed {
		t.Errorf("s3:GetObject with the condition satisfied = %q, want allowed", got)
	}
	if got := supplied.EvaluationResults[0].MissingContextValues; len(got) != 0 {
		t.Errorf("MissingContextValues = %v, want empty once the key is supplied", got)
	}
}

// matchedSourceIDs returns the SourcePolicyId of every statement a result matched.
func matchedSourceIDs(result iamtypes.EvaluationResult) []string {
	ids := make([]string, 0, len(result.MatchedStatements))
	for _, statement := range result.MatchedStatements {
		ids = append(ids, aws.ToString(statement.SourcePolicyId))
	}
	return ids
}

// containsString reports whether values holds want.
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
