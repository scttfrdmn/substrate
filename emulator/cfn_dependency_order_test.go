package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The deploy order is the dependency order (#1420). Each test below names the relationship it
// orders by, and asserts the recorded operation sequence: the resources exist either way, so the
// order they were created in is observable only through the event log.

// TestCFN_DeployOrderFollowsDependencies covers every edge kind the DependsOn page names — Ref,
// Fn::GetAtt, Fn::Sub in both forms, and DependsOn as a string and as a list — each written so
// the dependency contradicts typePriority. Under the old (priority, logical ID) sort every one of
// these deployed in the wrong order.
func TestCFN_DeployOrderFollowsDependencies(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
		want []string
	}{
		{
			// A role is priority 1 and a queue 3, so the role went first and its policy's
			// GetAtt found no queue.
			name: "Fn::GetAtt",
			tmpl: `{"Resources":{
				"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"order-getatt"}},
				"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"order-getatt-role",
					"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]},
					"Description":{"Fn::GetAtt":["Queue","Arn"]}}}}}`,
			want: []string{"CreateQueue", "CreateRole"},
		},
		{
			name: "Ref",
			tmpl: `{"Resources":{
				"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"order-ref"}},
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"order-ref-bucket",
					"Tags":[{"Key":"queue","Value":{"Ref":"Queue"}}]}}}}`,
			want: []string{"CreateQueue", "CreateBucket"},
		},
		{
			name: "Fn::Sub string form",
			tmpl: `{"Resources":{
				"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"order-sub"}},
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{
					"BucketName":{"Fn::Sub":"order-sub-${Topic.TopicName}"}}}}}`,
			want: []string{"CreateTopic", "CreateBucket"},
		},
		{
			name: "Fn::Sub list form",
			tmpl: `{"Resources":{
				"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"order-sublist"}},
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{
					"BucketName":{"Fn::Sub":["order-${N}",{"N":{"Fn::GetAtt":["Topic","TopicName"]}}]}}}}}`,
			want: []string{"CreateTopic", "CreateBucket"},
		},
		{
			name: "DependsOn as a string",
			tmpl: `{"Resources":{
				"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"order-dependson"}},
				"Bucket":{"Type":"AWS::S3::Bucket","DependsOn":"Topic",
					"Properties":{"BucketName":"order-dependson-bucket"}}}}`,
			want: []string{"CreateTopic", "CreateBucket"},
		},
		{
			name: "DependsOn as a list",
			tmpl: `{"Resources":{
				"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"order-list"}},
				"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"order-list"}},
				"Role":{"Type":"AWS::IAM::Role","DependsOn":["Topic","Queue"],
					"Properties":{"RoleName":"order-list-role",
					"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`,
			want: []string{"CreateQueue", "CreateTopic", "CreateRole"},
		},
		{
			// Independent resources keep the tie-break order exactly, so a template whose
			// references already agreed with the priorities deploys as it did.
			name: "independent resources keep the tie-break order",
			tmpl: `{"Resources":{
				"B":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"order-tie-b"}},
				"A":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"order-tie-a"}},
				"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"order-tie-role",
					"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`,
			want: []string{"CreateRole", "CreateQueue", "CreateQueue"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			result, err := d.Deploy(context.Background(), tc.tmpl, "ordered", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, result)
			assert.Equal(t, tc.want, cfnCreateOperations(sweepOperations(t, store, "ordered")))
		})
	}
}

// cfnCreateOperations drops the follow-up configuration calls a deploy makes after a create — an S3
// bucket's PutBucketTagging, an SQS queue's GetQueueAttributes — so the assertion is on the creates.
func cfnCreateOperations(ops []string) []string {
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		switch op {
		case "CreateQueue", "CreateRole", "CreateTopic", "CreateBucket", "DeleteQueue", "DeleteRole",
			"DeleteTopic", "DeleteBucket":
			out = append(out, op)
		}
	}
	return out
}

// TestCFN_DeleteOrderIsTheReverseOfTheDependencyOrder pins the other half of the DependsOn page's
// rule: "Resource A is deleted before resource B." The template deploys a topic before a role
// that depends on it, against their priorities, so a sweep in reverse priority order would delete
// the topic first.
func TestCFN_DeleteOrderIsTheReverseOfTheDependencyOrder(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()
	const tmpl = `{"Resources":{
		"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"reverse-topic"}},
		"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"reverse-role",
			"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]},
			"Description":{"Ref":"Topic"}}}}}`
	_, err := d.Deploy(ctx, tmpl, "reverse", nil)
	require.NoError(t, err)
	created := sweepOperations(t, store, "reverse")
	require.Equal(t, []string{"CreateTopic", "CreateRole"}, cfnCreateOperations(created))

	require.NoError(t, d.DeleteStack(ctx, "reverse"))
	deleted := sweepOperations(t, store, "reverse")[len(created):]
	assert.Equal(t, []string{"DeleteRole", "DeleteTopic"}, cfnCreateOperations(deleted),
		"the dependent goes first")
}

// TestCFN_IAMPolicyGetAttResolvesAfterItsTarget is #1153's remaining criterion and #1420's third:
// an AWS::IAM::Policy whose PolicyDocument names a queue by Fn::GetAtt stores the queue's ARN.
//
// AWS::IAM::Policy was the one type at priority 0, so it deployed before every other resource and
// the GetAtt found nothing — the stored Resource was the literal "Data.Arn". Read back through
// GetRolePolicy over the registry, the way a caller would, with no Fn:: key anywhere.
func TestCFN_IAMPolicyGetAttResolvesAfterItsTarget(t *testing.T) {
	const tmpl = `{"Resources":{
		"Data":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-doc-getatt"}},
		"Reader":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"queue-reader",
			"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}},
		"ReadData":{"Type":"AWS::IAM::Policy","Properties":{
			"PolicyName":"read-the-queue","Roles":[{"Ref":"Reader"}],
			"PolicyDocument":{"Version":"2012-10-17","Statement":[{
				"Effect":"Allow","Action":"sqs:ReceiveMessage",
				"Resource":{"Fn::GetAtt":["Data","Arn"]}}]}}}}}`
	deployer, registry := sfnDefinitionDeployer(t)
	result, err := deployer.Deploy(context.Background(), tmpl, "cfn-doc-getatt", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	// Read back through GetRolePolicy: the type is an inline policy on its role (#1562).
	body := cfnDocRoute(t, registry, &emulator.AWSRequest{
		Service: "iam", Operation: "GetRolePolicy",
		Body: []byte(`{"RoleName":"queue-reader","PolicyName":"read-the-queue"}`),
	})
	var out struct {
		Document string `xml:"GetRolePolicyResult>PolicyDocument"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), string(body))
	doc, err := url.QueryUnescape(out.Document)
	require.NoError(t, err)
	var parsed struct {
		Statement []struct {
			Resource string `json:"Resource"`
		} `json:"Statement"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(doc), &parsed), "document does not parse: %s", doc)
	require.Len(t, parsed.Statement, 1, doc)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:cfn-doc-getatt", parsed.Statement[0].Resource,
		"the statement names the queue's ARN")
	requireNoIntrinsic(t, doc)
}

// TestCFN_UnorderableTemplatesAreRefused pins the two request-level refusals and their published
// wording (see ErrCFNInvalidDependency for the sources). Neither deploys anything.
func TestCFN_UnorderableTemplatesAreRefused(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
		want string
	}{
		{
			name: "a DependsOn the template does not declare",
			tmpl: `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","DependsOn":["Ghost","Absent"],
				"Properties":{"BucketName":"refused-unresolved"}}}}`,
			want: "Template format error: Unresolved resource dependencies [Absent, Ghost] in the " +
				"Resources block of the template",
		},
		{
			name: "two resources naming each other",
			tmpl: `{"Resources":{
				"A":{"Type":"AWS::SNS::Topic","DependsOn":"B","Properties":{"TopicName":"refused-a"}},
				"B":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":{"Fn::GetAtt":["A","TopicName"]}}}}}`,
			want: "Circular dependency between resources: [A, B]",
		},
		{
			name: "a resource depending on itself",
			tmpl: `{"Resources":{"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"refused-self",
				"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]},
				"Description":{"Fn::GetAtt":["Role","Arn"]}}}}}`,
			want: "Circular dependency between resources: [Role]",
		},
		{
			// A resource downstream of a cycle cannot be ordered either, and is named, as the
			// reports quoted on ErrCFNInvalidDependency name such resources.
			name: "a dependent of a cycle",
			tmpl: `{"Resources":{
				"A":{"Type":"AWS::SNS::Topic","DependsOn":"B","Properties":{"TopicName":"refused-ca"}},
				"B":{"Type":"AWS::SNS::Topic","DependsOn":"A","Properties":{"TopicName":"refused-cb"}},
				"C":{"Type":"AWS::SQS::Queue","DependsOn":"A","Properties":{"QueueName":"refused-cc"}},
				"Free":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"refused-free"}}}}`,
			want: "Circular dependency between resources: [A, B, C]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			_, err := d.Deploy(context.Background(), tc.tmpl, "refused", nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, emulator.ErrCFNInvalidDependency)
			assert.Equal(t, tc.want, err.Error())
			assert.Empty(t, sweepOperations(t, store, "refused"), "nothing deployed")
			stacks, err := d.ListStacks(context.Background())
			require.NoError(t, err)
			assert.Empty(t, stacks, "and no stack record was written")
		})
	}
}

// TestCFN_ConditionedOutResourceAndDependencies pins the two sides of a resource skipped by a false
// Condition. A DependsOn naming it is unresolved and refuses the template, which is what user
// reports of the refusal trace it to. A Ref to it inside the untaken branch of an Fn::If is no edge
// and deploys, which the same reports say is accepted.
func TestCFN_ConditionedOutResourceAndDependencies(t *testing.T) {
	const conditions = `"Conditions":{"Never":{"Fn::Equals":["a","b"]}},`
	t.Run("a DependsOn naming it is refused", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		_, err := d.Deploy(context.Background(), `{`+conditions+`"Resources":{
			"Skipped":{"Type":"AWS::SNS::Topic","Condition":"Never","Properties":{"TopicName":"skipped"}},
			"Bucket":{"Type":"AWS::S3::Bucket","DependsOn":"Skipped",
				"Properties":{"BucketName":"conditioned-bucket"}}}}`, "conditioned", nil)
		require.ErrorIs(t, err, emulator.ErrCFNInvalidDependency)
		assert.Equal(t, "Template format error: Unresolved resource dependencies [Skipped] in the "+
			"Resources block of the template", err.Error())
	})
	t.Run("a Ref in an untaken Fn::If branch deploys", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(context.Background(), `{`+conditions+`"Resources":{
			"Skipped":{"Type":"AWS::SNS::Topic","Condition":"Never","Properties":{"TopicName":"skipped"}},
			"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"conditioned-bucket",
				"Tags":[{"Key":"topic","Value":{"Fn::If":["Never",{"Ref":"Skipped"},"none"]}}]}}}}`,
			"conditioned", nil)
		require.NoError(t, err)
		requireNoResourceErrors(t, result)
		require.Len(t, result.Resources, 1)
		assert.Equal(t, "Bucket", result.Resources[0].LogicalID)
	})
}

// TestCFN_SubVariablesAndIntrinsicAttributes pins two reference forms the dependency walk reads
// precisely (#1420 review). A `${Name}` that is a key of a two-argument Fn::Sub's variable map is
// the variable, not the resource of the same name, so it creates no edge — and here no false cycle.
// An Fn::GetAtt whose attribute name is itself an intrinsic still depends on its resource.
func TestCFN_SubVariablesAndIntrinsicAttributes(t *testing.T) {
	t.Run("a Fn::Sub variable is not a reference", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(context.Background(), `{"Resources":{
			"Topic":{"Type":"AWS::SNS::Topic","Properties":{
				"TopicName":{"Fn::Sub":["subvar-${Bucket}",{"Bucket":"literal"}]}}},
			"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"subvar-bucket",
				"Tags":[{"Key":"t","Value":{"Ref":"Topic"}}]}}}}`, "subvars", nil)
		require.NoError(t, err, "the Topic's ${Bucket} names its variable, so there is no cycle")
		requireNoResourceErrors(t, result)
	})
	t.Run("an intrinsic attribute name still orders", func(t *testing.T) {
		d, _, _, store := newSweepDeployer(t)
		result, err := d.Deploy(context.Background(), `{
			"Parameters":{"Attr":{"Type":"String","Default":"Arn"}},
			"Resources":{
				"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"intrinsic-attr"}},
				"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"intrinsic-attr-role",
					"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]},
					"Description":{"Fn::GetAtt":["Queue",{"Ref":"Attr"}]}}}}}`, "intrinsic-attr", nil)
		require.NoError(t, err)
		requireNoResourceErrors(t, result)
		assert.Equal(t, []string{"CreateQueue", "CreateRole"},
			cfnCreateOperations(sweepOperations(t, store, "intrinsic-attr")),
			"the role follows the queue its attribute names, though the role's priority is lower")
	})
}

// TestCFNPlugin_UnorderableTemplateIsAValidationError pins the wire shape of both refusals, on
// every operation that takes a template: CreateStack, UpdateStack and CreateChangeSet each answer
// ValidationError at 400 with AWS's message verbatim, and none changes the stack.
func TestCFNPlugin_UnorderableTemplateIsAValidationError(t *testing.T) {
	const cyclic = `{"Resources":{
		"A":{"Type":"AWS::S3::Bucket","DependsOn":"B","Properties":{"BucketName":"wire-cycle-a"}},
		"B":{"Type":"AWS::S3::Bucket","DependsOn":"A","Properties":{"BucketName":"wire-cycle-b"}}}}`
	const unresolved = `{"Resources":{"A":{"Type":"AWS::S3::Bucket","DependsOn":"Nope",
		"Properties":{"BucketName":"wire-unresolved"}}}}`
	cfnWireMessage := func(t *testing.T, body string) string {
		t.Helper()
		var doc struct {
			Message string `xml:"Error>Message"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
		return doc.Message
	}

	ts := newCFNTestServer(t)
	for _, tc := range []struct {
		tmpl, want string
	}{
		{cyclic, "Circular dependency between resources: [A, B]"},
		{unresolved, "Template format error: Unresolved resource dependencies [Nope] in the Resources block of the template"},
	} {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{
			"StackName": "wire-refused", "TemplateBody": tc.tmpl})
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
		assert.Equal(t, tc.want, cfnWireMessage(t, body))
	}
	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wire-refused"})
	assert.Equal(t, http.StatusBadRequest, code, "a refused CreateStack leaves no stack: %s", body)

	code, body = cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "wire-kept", "TemplateBody": cfnBucketTemplate})
	require.Equal(t, http.StatusOK, code, body)
	code, body = cfnAction(t, ts, "UpdateStack", map[string]string{
		"StackName": "wire-kept", "TemplateBody": cyclic})
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Equal(t, "Circular dependency between resources: [A, B]", cfnWireMessage(t, body),
		"the message carries no deployer context")
	code, body = cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName": "wire-kept", "ChangeSetName": "cs", "TemplateBody": unresolved})
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wire-kept"})
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<StackStatus>CREATE_COMPLETE</StackStatus>",
		"the refused update left the stack as it was")
	assert.Equal(t, http.StatusOK, cfnHeadBucket(t, ts, "cfn-wire-bucket"))
}
