package emulator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #843: an AWS::ApiGateway::Method's physical ID was its HTTP verb, so every
// method in a stack reported the same identifier as every other method with the
// same verb — and because a PhysicalResourceId lookup scans every stack in the
// account, as every GET method in every stack.
//
// The tests here assert the identifier and the two things changing it put at
// risk: stack deletion, which built the method's DELETE path out of the physical
// ID as though it were still the verb, and redeploy stability, which is the whole
// reason the generated suffix is derived rather than random.
//
// AWS's warrant for the shape is the Template Reference for the type: Ref
// "returns the method ID, such as mysta-metho-01234b567890example". Its REST API
// publishes no method identifier at all — API_Method has no id member and
// API_PutMethod returns none — which is why the fix is entirely CloudFormation-side
// and the plugin is untouched.

// apigwTwoMethodsTemplate declares one verb twice, on two different resources of
// one API. That is the collision #843 reports and the reason the logical ID alone
// cannot be the discriminator either: both methods are GET, and only the pair
// (resource, verb) tells them apart in API Gateway's own state.
const apigwTwoMethodsTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyAPI": {
			"Type": "AWS::ApiGateway::RestApi",
			"Properties": {"Name": "two-methods-api"}
		},
		"ItemsResource": {
			"Type": "AWS::ApiGateway::Resource",
			"Properties": {
				"RestApiId": {"Ref": "MyAPI"},
				"ParentId": {"Fn::GetAtt": ["MyAPI", "RootResourceId"]},
				"PathPart": "items"
			},
			"DependsOn": "MyAPI"
		},
		"UsersResource": {
			"Type": "AWS::ApiGateway::Resource",
			"Properties": {
				"RestApiId": {"Ref": "MyAPI"},
				"ParentId": {"Fn::GetAtt": ["MyAPI", "RootResourceId"]},
				"PathPart": "users"
			},
			"DependsOn": "MyAPI"
		},
		"GetItems": {
			"Type": "AWS::ApiGateway::Method",
			"Properties": {
				"RestApiId": {"Ref": "MyAPI"},
				"ResourceId": {"Ref": "ItemsResource"},
				"HttpMethod": "GET",
				"AuthorizationType": "NONE"
			},
			"DependsOn": "ItemsResource"
		},
		"GetUsers": {
			"Type": "AWS::ApiGateway::Method",
			"Properties": {
				"RestApiId": {"Ref": "MyAPI"},
				"ResourceId": {"Ref": "UsersResource"},
				"HttpMethod": "GET",
				"AuthorizationType": "NONE"
			},
			"DependsOn": "UsersResource"
		}
	},
	"Outputs": {
		"ItemsMethodRef": {"Value": {"Ref": "GetItems"}},
		"UsersMethodRef": {"Value": {"Ref": "GetUsers"}}
	}
}`

// apigwMethodStatus reads a method back through the API Gateway plugin and
// returns its HTTP status, which is how a deletion is proved rather than assumed:
// DeleteStack reports no error for a resource whose delete request was never
// built.
func apigwMethodStatus(
	t *testing.T, d *emulator.StackDeployer, apiID, resourceID, verb, streamID string,
) int {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service:   "apigateway",
		Operation: "GET",
		Path:      "/restapis/" + apiID + "/resources/" + resourceID + "/methods/" + verb,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}, streamID)
	if err != nil {
		var awsErr *emulator.AWSError
		require.ErrorAs(t, err, &awsErr, "unexpected non-AWS error reading the method back")
		return awsErr.HTTPStatus
	}
	require.NotNil(t, resp)
	return resp.StatusCode
}

// TestCFNMethodID_TwoMethodsWithOneVerbReportDistinctPhysicalIDs is #843's own
// reproducer. Both methods are GET, so before the fix both reported "GET".
func TestCFNMethodID_TwoMethodsWithOneVerbReportDistinctPhysicalIDs(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), apigwTwoMethodsTemplate, "two-methods", nil)
	require.NoError(t, err)
	for _, res := range result.Resources {
		require.Empty(t, res.Error, "%s must deploy cleanly", res.LogicalID)
	}

	items := resourceByLogicalID(t, result, "GetItems").PhysicalID
	users := resourceByLogicalID(t, result, "GetUsers").PhysicalID

	assert.NotEqual(t, "GET", items, "a method's physical ID must not be its HTTP verb")
	assert.NotEqual(t, "GET", users, "a method's physical ID must not be its HTTP verb")
	assert.NotEqual(t, items, users,
		"two methods with the same verb on different resources must be distinguishable")

	// The shape is AWS's documented one, {stack}-{logical}-{suffix}, lowercased —
	// the example the Template Reference publishes, mysta-metho-01234b567890example,
	// is lowercase and its segments are truncated.
	for id, physical := range map[string]string{"GetItems": items, "GetUsers": users} {
		assert.Equal(t, strings.ToLower(physical), physical,
			"%s: the documented example is lowercase", id)
		assert.True(t, strings.HasPrefix(physical, "two-m"),
			"%s: %q must lead with the stack-name segment", id, physical)
		assert.Contains(t, physical, "-", "%s: %q must carry the generated shape", id, physical)
	}
}

// TestCFNMethodID_RefResolvesToTheMethodID asserts the intrinsic follows the
// physical ID rather than the verb, which is what AWS documents Ref to return for
// this type. Before the fix both outputs read "GET".
func TestCFNMethodID_RefResolvesToTheMethodID(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), apigwTwoMethodsTemplate, "method-refs", nil)
	require.NoError(t, err)

	assert.Equal(t, resourceByLogicalID(t, result, "GetItems").PhysicalID,
		result.Outputs["ItemsMethodRef"])
	assert.Equal(t, resourceByLogicalID(t, result, "GetUsers").PhysicalID,
		result.Outputs["UsersMethodRef"])
	assert.NotEqual(t, "GET", result.Outputs["ItemsMethodRef"])
}

// TestCFNMethodID_AnUpdateDoesNotMoveAMethodID is the determinism guarantee
// cfnNameSuffix exists for. UpdateStack in substrate is a re-Deploy of the whole
// template, so a random suffix would report a different PhysicalResourceId on
// every update for a resource nothing changed.
func TestCFNMethodID_AnUpdateDoesNotMoveAMethodID(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	before, err := d.Deploy(ctx, apigwTwoMethodsTemplate, "stable-methods", nil)
	require.NoError(t, err)
	after, err := d.UpdateStack(ctx, apigwTwoMethodsTemplate, "stable-methods", nil,
		emulator.CFNDeployOptions{})
	require.NoError(t, err)

	for _, id := range []string{"GetItems", "GetUsers"} {
		assert.Equal(t, resourceByLogicalID(t, before, id).PhysicalID,
			resourceByLogicalID(t, after, id).PhysicalID,
			"%s: an unchanged update must not move a method ID", id)
	}
}

// TestCFNMethodID_TwoStacksReportDifferentMethodIDs is the other half of the
// determinism property: stable within a stack, distinct between stacks, because
// the suffix hashes the stack name along with the account, Region and logical ID.
func TestCFNMethodID_TwoStacksReportDifferentMethodIDs(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	a, err := d.Deploy(ctx, apigwTwoMethodsTemplate, "methods-a", nil)
	require.NoError(t, err)
	b, err := d.Deploy(ctx, apigwTwoMethodsTemplate, "methods-b", nil)
	require.NoError(t, err)

	assert.NotEqual(t, resourceByLogicalID(t, a, "GetItems").PhysicalID,
		resourceByLogicalID(t, b, "GetItems").PhysicalID,
		"one logical ID in two stacks must report two method IDs")
}

// TestCFNMethodID_TheStackStillDeletes is the load-bearing test. The deleter built
// the method's DELETE path from the physical ID **as the verb**, so generating a
// method ID without changing the deleter in the same commit would have left every
// method behind — with DeleteStack reporting no error, because a DELETE of
// /methods/{stack}-{logical}-{suffix} is a well-formed request for a method that
// does not exist.
//
// The methods are read back through the API Gateway plugin afterwards, since a
// clean DeleteStack result cannot distinguish "deleted" from "asked to delete
// something else".
func TestCFNMethodID_TheStackStillDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, apigwTwoMethodsTemplate, "delete-methods", nil)
	require.NoError(t, err)

	apiID := resourceByLogicalID(t, result, "MyAPI").PhysicalID
	itemsRes := resourceByLogicalID(t, result, "ItemsResource").PhysicalID
	usersRes := resourceByLogicalID(t, result, "UsersResource").PhysicalID

	require.Equal(t, 200, apigwMethodStatus(t, d, apiID, itemsRes, "GET", "delete-methods"),
		"the method must exist before the stack is deleted")
	require.Equal(t, 200, apigwMethodStatus(t, d, apiID, usersRes, "GET", "delete-methods"))

	require.NoError(t, d.DeleteStack(ctx, "delete-methods"))

	assert.Equal(t, 404, apigwMethodStatus(t, d, apiID, itemsRes, "GET", "delete-methods"),
		"deleting the stack must delete the method, not a method named after its physical ID")
	assert.Equal(t, 404, apigwMethodStatus(t, d, apiID, usersRes, "GET", "delete-methods"))
}

// TestCFNMethodID_TheCreatedMethodIsStillAddressedByItsVerb guards the direction
// the fix must not break: the physical ID changed, but API Gateway's own state is
// keyed by (account, Region, API, resource, verb) and always was. The plugin was
// never wrong here, which is why #843's two criteria asking it to mint an
// identifier were reversed rather than implemented.
func TestCFNMethodID_TheCreatedMethodIsStillAddressedByItsVerb(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), apigwTwoMethodsTemplate, "verb-keyed", nil)
	require.NoError(t, err)

	apiID := resourceByLogicalID(t, result, "MyAPI").PhysicalID
	itemsRes := resourceByLogicalID(t, result, "ItemsResource").PhysicalID
	methodID := resourceByLogicalID(t, result, "GetItems").PhysicalID

	assert.Equal(t, 200, apigwMethodStatus(t, d, apiID, itemsRes, "GET", "verb-keyed"),
		"the verb still addresses the method in API Gateway's own state")
	assert.Equal(t, 404, apigwMethodStatus(t, d, apiID, itemsRes, methodID, "verb-keyed"),
		"the generated method ID is a CloudFormation identifier, not an API Gateway one")
}
