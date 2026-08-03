package emulator_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The templates the rollback tests deploy.
//
// Every one of them fails at a *real* refusal rather than an injected one, and the
// refusal is always the same: "no" is shorter than S3's three-character minimum, so
// CreateBucket answers InvalidBucketName. A rollback test whose failure came from a
// missing plugin would be testing ServiceNotAvailable, which is substrate's own
// condition rather than one a caller can hit against AWS.
const (
	// cfnRollbackTemplate is #520's own reproduction: a resource that is refused and
	// a resource beside it that deploys and must therefore be swept. The queue sorts
	// after the bucket by type priority, so it is created *after* the failure — which
	// is what makes "the trailing queue is gone" the assertion the issue asks for.
	cfnRollbackTemplate = `{"Resources":{` +
		`"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}},` +
		`"Kept":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"rollback-kept"}},` +
		`"Trailing":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-trailing"}}}}`

	// cfnRollbackCleanTemplate is the same shape with nothing refused, for the tests
	// that need a stack to succeed first.
	cfnRollbackCleanTemplate = `{"Resources":{` +
		`"Kept":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"rollback-kept"}},` +
		`"Trailing":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-trailing"}}}}`
)

// queueExists reports whether a queue of this name is still there, which is the
// observable #520 asks for by name ("get-queue-url --queue-name still-here → absent").
func queueExists(t *testing.T, d *emulator.StackDeployer, name string) bool {
	t.Helper()
	resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "sqs", Operation: "GetQueueUrl", Path: "/",
		Headers: map[string]string{}, Params: map[string]string{"QueueName": name},
	}, "probe")
	return resp != nil && resp.StatusCode == http.StatusOK
}

// TestCFN_AFailedResourceRollsTheStackBack is #520's own reproduction, and it is the
// release's headline: before it, a stack containing a resource that was refused still
// reported CREATE_COMPLETE, so a consumer's failure-handling path could not be tested
// at all — substrate never produced the failure it exists to reproduce.
func TestCFN_AFailedResourceRollsTheStackBack(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, cfnRollbackTemplate, "rolled", nil)
	require.NoError(t, err,
		"a rolled-back create is a stack status, not a failed call: real CreateStack "+
			"has returned its StackId before the rollback happens")

	assert.Equal(t, "ROLLBACK_COMPLETE", result.Status,
		"ROLLBACK is CreateStack's default and nothing asked for another")
	assert.Contains(t, result.StatusReason, "InvalidBucketName",
		"the stack's reason names the resource that failed and why")
	assert.Contains(t, result.StatusReason, "Refused")
	assert.Empty(t, result.Outputs, "a rolled-back stack publishes no outputs")

	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "rollback-kept"),
		"the bucket that deployed is swept: rollback removes what the create made")
	assert.False(t, queueExists(t, d, "rollback-trailing"),
		"and so is the resource deployed after the failure")

	// The stack stays, reporting ROLLBACK_COMPLETE. That is what makes the failure
	// impossible to miss — it blocks a create of the same name until it is deleted.
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "ROLLBACK_COMPLETE", stacks[0].Status)
	assert.Contains(t, stacks[0].StatusReason, "InvalidBucketName")
}

// TestCFN_RollbackSweepsInReverseOrder pins that the rollback reuses #518's sweep
// rather than deleting in whatever order the resources happen to be listed.
//
// The type priorities encode real dependencies, so a rollback in any other order is
// one that happens to work on templates whose resources are independent. Asserting the
// recorded operation sequence is the only way that is observable.
func TestCFN_RollbackSweepsInReverseOrder(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()

	// A role (priority 1), a bucket (2) and a queue (3) all deploy; a second bucket
	// with an invalid name is refused, which triggers the rollback.
	tmpl := `{"Resources":{` +
		`"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"rollback-role",` +
		`"AssumeRolePolicyDocument":{"Statement":[]}}},` +
		`"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"rollback-ordered"}},` +
		`"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}},` +
		`"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-ordered-q"}}}}`

	result, err := d.Deploy(ctx, tmpl, "revorder", nil)
	require.NoError(t, err)
	require.Equal(t, "ROLLBACK_COMPLETE", result.Status)

	ops := sweepOperations(t, store, "revorder")

	// The creates come first, in ascending priority. The refused bucket's CreateBucket
	// is recorded too — the request was made and refused — so the creates are located
	// by finding where the deletes begin rather than by counting.
	var deletes []string
	for _, op := range ops {
		switch op {
		case "DeleteRole", "DeleteBucket", "DeleteQueue":
			deletes = append(deletes, op)
		}
	}
	assert.Equal(t, []string{"DeleteQueue", "DeleteBucket", "DeleteRole"}, deletes,
		"descending priority, the exact inverse of the deploy order")
}

// TestCFN_RollbackDoesNotDeleteWhatWasNeverCreated pins that the sweep skips the
// resource that failed.
//
// Its create was refused, so there is nothing on the other side to delete, and the
// delete request would be built from the same rejected identifier — an invalid bucket
// name yields an equally invalid DeleteBucket, whose refusal is not a not-found and
// would turn a clean rollback into ROLLBACK_FAILED.
func TestCFN_RollbackDoesNotDeleteWhatWasNeverCreated(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, cfnRollbackTemplate, "notcreated", nil)
	require.NoError(t, err)
	require.Equal(t, "ROLLBACK_COMPLETE", result.Status,
		"the refused resource must not have wedged the rollback")

	byID := map[string]emulator.CFNResourceDeletion{}
	for _, del := range result.ResourceDeletions {
		byID[del.LogicalID] = del
	}
	assert.NotContains(t, byID, "Refused",
		"the resource that failed is not swept; its create never happened")
	assert.Contains(t, byID, "Kept")
	assert.Contains(t, byID, "Trailing")
}

// TestCFN_OnFailureDeleteRemovesTheStack pins the third option: nothing is left, not
// even the record.
func TestCFN_OnFailureDeleteRemovesTheStack(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.DeployWithOptions(ctx, cfnRollbackTemplate, "deleted", nil,
		emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDelete})
	require.NoError(t, err)

	assert.Equal(t, "DELETE_COMPLETE", result.Status,
		"the result is the only place this outcome is readable, since the record is gone")
	assert.Contains(t, result.StatusReason, "InvalidBucketName")

	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "rollback-kept"))
	assert.False(t, queueExists(t, d, "rollback-trailing"))

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks, "OnFailure DELETE leaves no stack behind")
}

// TestCFN_ARollbackThatCannotDeleteReportsRollbackFailed pins the honest status for a
// rollback that did not finish.
//
// Claiming ROLLBACK_COMPLETE here would assert a cleanliness that does not hold, and
// the resource still standing is exactly what the caller needs to know about. The
// failure is real rather than injected: an object written into the bucket behind the
// stack's back makes DeleteBucket answer BucketNotEmpty, which is what S3 does.
func TestCFN_ARollbackThatCannotDeleteReportsRollbackFailed(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	// First a clean deploy, so the bucket exists to be written into.
	_, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "wedgedback", nil)
	require.NoError(t, err)

	resp, err := d.DispatchForTest(ctx, &emulator.AWSRequest{
		Service: "s3", Operation: "PUT", Path: "/rollback-kept/left-behind.txt",
		Body: []byte("not the stack's"), Headers: map[string]string{},
		Params: map[string]string{},
	}, "wedgedback")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Now redeploy with a resource that will be refused. The bucket's declaration is
	// unchanged, so its already-exists refusal is not counted as a failure, and the
	// rollback then cannot delete it.
	result, err := d.Deploy(ctx, `{"Resources":{`+
		`"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}},`+
		`"Kept":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"rollback-kept"}},`+
		`"Trailing":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-trailing"}}}}`,
		"wedgedback", nil)
	require.NoError(t, err)

	assert.Equal(t, "ROLLBACK_FAILED", result.Status)
	assert.Contains(t, result.StatusReason, "BucketNotEmpty",
		"the reason is the plugin's own refusal, not a paraphrase")
	assert.Contains(t, result.StatusReason, "Kept",
		"and it names the resource still standing")

	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"),
		"which is still there, and is why the rollback failed")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "ROLLBACK_FAILED", stacks[0].Status,
		"the stack stays so the caller can see what is holding it")
}

// TestCFN_RetainDoesNotSurviveACreateRollback pins RetainExceptOnCreate's whole
// reason for existing, and the one place it differs from Retain.
//
// "Except for the stack operation that initially created the resource": a
// RetainExceptOnCreate resource is kept by a DeleteStack sweep and deleted by the
// rollback of the create that made it. Plain Retain is kept by both. Asserting the two
// side by side is what proves the policy resolver reads the *operation* rather than
// just the policy name.
func TestCFN_RetainDoesNotSurviveACreateRollback(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, `{"Resources":{`+
		`"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}},`+
		`"Retained":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain",`+
		`"Properties":{"BucketName":"rollback-retained"}},`+
		`"ExceptOnCreate":{"Type":"AWS::S3::Bucket","DeletionPolicy":"RetainExceptOnCreate",`+
		`"Properties":{"BucketName":"rollback-except"}}}}`, "retainroll", nil)
	require.NoError(t, err)
	require.Equal(t, "ROLLBACK_COMPLETE", result.Status)

	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-retained"),
		"Retain survives a create rollback")
	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "rollback-except"),
		"RetainExceptOnCreate does not: this is the create that made it")
}

// TestCFN_AnUnchangedRedeployIsNotAFailedCreate pins the guard that keeps rollback
// from eating the resources it was asked to keep.
//
// StackDeployer.UpdateStack is a re-Deploy of the whole template, so a resource the
// update leaves alone is created a *second* time and its plugin refuses the duplicate.
// Real CloudFormation issues no create call at all for an unchanged resource, so there
// is no real refusal here to model — and reading it as a failure would make every
// update of an unchanged template roll back. This is the defect the rollback work
// introduced and this test is what pins it shut.
func TestCFN_AnUnchangedRedeployIsNotAFailedCreate(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	first, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "redeployed", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", first.Status)

	second, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "redeployed", nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", second.Status,
		"redeploying an unchanged template is not a failed create")
	for _, r := range second.Resources {
		assert.Empty(t, r.Error, "%s: an already-exists refusal of an unchanged "+
			"resource is not this deployment's failure", r.LogicalID)
	}
	assert.Empty(t, second.ResourceDeletions, "and nothing is swept")

	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"),
		"the resources the caller asked to keep are still there")
	assert.True(t, queueExists(t, d, "rollback-trailing"))
}

// TestCFN_AForeignNameCollisionIsStillAFailure is the other half of the guard, and
// the reason it compares declarations rather than merely matching logical IDs.
//
// A template edited to point a slot at a name another stack already owns is a real
// failure that CloudFormation reports. Clearing every already-exists refusal for a
// stack that has been deployed before would hide it — so the refusal is cleared only
// when the declaration is byte-identical to the one that succeeded.
func TestCFN_AForeignNameCollisionIsStillAFailure(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	// Another stack owns rollback-foreign.
	_, err := d.Deploy(ctx, `{"Resources":{"Owned":{"Type":"AWS::S3::Bucket",`+
		`"Properties":{"BucketName":"rollback-foreign"}}}}`, "owner", nil)
	require.NoError(t, err)

	// This stack deploys cleanly at its own name first, so it has a previous record.
	_, err = d.Deploy(ctx, `{"Resources":{"Slot":{"Type":"AWS::S3::Bucket",`+
		`"Properties":{"BucketName":"rollback-own"}}}}`, "renamer", nil)
	require.NoError(t, err)

	// Then the same slot is repointed at the name the other stack holds.
	result, err := d.DeployWithOptions(ctx, `{"Resources":{"Slot":{"Type":"AWS::S3::Bucket",`+
		`"Properties":{"BucketName":"rollback-foreign"}}}}`, "renamer", nil,
		emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_FAILED", result.Status,
		"a rename into a name another stack owns is a real failure")
	assert.Contains(t, result.StatusReason, "BucketAlreadyExists")

	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-foreign"),
		"and the other stack's bucket is untouched")
}

// TestCFN_ARepeatedForeignCollisionIsStillAFailure pins that "this stack already
// deployed it" is read from the previous record's *outcome*, not merely from the
// previous record naming the logical ID.
//
// A stack left standing at CREATE_FAILED (DO_NOTHING, or a rollback that could not
// finish) holds a record listing the resource it failed to create. Deploying it again
// unchanged draws the same refusal from the same owner. Nothing changed and nothing was
// fixed, so the second attempt must fail exactly as the first did — reading the record
// as "I own this name" would turn a permanent collision into a success on the second
// try, and the caller would be told a bucket is theirs while another stack holds it.
func TestCFN_ARepeatedForeignCollisionIsStillAFailure(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	const squat = `{"Resources":{"Slot":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"rollback-repeat"}}}}`

	_, err := d.Deploy(ctx, `{"Resources":{"Owned":{"Type":"AWS::S3::Bucket",`+
		`"Properties":{"BucketName":"rollback-repeat"}}}}`, "owner", nil)
	require.NoError(t, err)

	// DO_NOTHING so the failed record survives to be read by the second attempt.
	opts := emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing}
	first, err := d.DeployWithOptions(ctx, squat, "squatter", nil, opts)
	require.NoError(t, err)
	require.Equal(t, "CREATE_FAILED", first.Status)

	second, err := d.DeployWithOptions(ctx, squat, "squatter", nil, opts)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_FAILED", second.Status,
		"the resource failed the first time, so this stack never owned the name; "+
			"an unchanged retry against an unchanged owner fails the same way")
	assert.Contains(t, second.StatusReason, "BucketAlreadyExists")
}

// TestCFN_AFailedUpdateRollsBackToThePreviousTemplate pins UPDATE_ROLLBACK_COMPLETE,
// and the approximation behind it.
//
// UpdateStack is a re-Deploy, so the previous template is the only description of the
// previous state substrate holds; the rollback re-deploys it. That converges on the
// previous template's *declared* state, which is not identical to CloudFormation's
// per-resource restore — see StackDeployer.rollbackFailedUpdate. What is asserted here
// is the part that does hold: the status, and that the resource the failed update
// declared is not left behind.
func TestCFN_AFailedUpdateRollsBackToThePreviousTemplate(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "updated", nil)
	require.NoError(t, err)

	// The update keeps both resources and adds one that will be refused.
	result, err := d.UpdateStack(ctx, `{"Resources":{`+
		`"Kept":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"rollback-kept"}},`+
		`"Trailing":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-trailing"}},`+
		`"Added":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}}}`,
		"updated", nil)
	require.NoError(t, err, "a rolled-back update is a status, not a failed call")

	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
	assert.Contains(t, result.StatusReason, "InvalidBucketName")
	assert.Contains(t, result.StatusReason, "Added")

	// The resources the previous template declared are still there: an update
	// rollback converges *onto* that template rather than deleting what it declares.
	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"),
		"an update rollback is not a create rollback; it does not sweep")
	assert.True(t, queueExists(t, d, "rollback-trailing"))

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", stacks[0].Status)
}

// TestCFN_AnUnchangedUpdateStillReportsUpdateComplete is the regression this release
// came closest to shipping: with rollback added and no redeploy guard, an update that
// changed nothing would have reported UPDATE_ROLLBACK_COMPLETE and deleted nothing —
// or worse, taken the create-rollback path and deleted everything.
func TestCFN_AnUnchangedUpdateStillReportsUpdateComplete(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "noop", nil)
	require.NoError(t, err)

	result, err := d.UpdateStack(ctx, cfnRollbackCleanTemplate, "noop", nil)
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", result.Status,
		"an update that changes nothing succeeds")
	assert.Empty(t, result.StatusReason)

	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"))
	assert.True(t, queueExists(t, d, "rollback-trailing"))
}

// TestCFNDeployOptionsAreValidated pins that an unrecognized OnFailure is refused
// rather than defaulted.
//
// Every alternative reading of an unrecognized value is a wrong one: treating it as
// ROLLBACK deletes resources the caller may have meant to keep, and treating it as
// DO_NOTHING keeps resources they may have meant to remove. A typo'd "ROLBACK" that
// silently deletes is the failure this rejection exists to prevent.
func TestCFNDeployOptionsAreValidated(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	tests := []struct {
		name      string
		onFailure string
		wantErr   bool
	}{
		{name: "empty is the default", onFailure: "", wantErr: false},
		{name: "ROLLBACK", onFailure: emulator.CFNOnFailureRollback, wantErr: false},
		{name: "DO_NOTHING", onFailure: emulator.CFNOnFailureDoNothing, wantErr: false},
		{name: "DELETE", onFailure: emulator.CFNOnFailureDelete, wantErr: false},
		{name: "a typo is not a default", onFailure: "ROLBACK", wantErr: true},
		{name: "the wrong case is not the value", onFailure: "rollback", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := d.DeployWithOptions(context.Background(), cfnRollbackCleanTemplate,
				"validated-"+tc.name, nil,
				emulator.CFNDeployOptions{OnFailure: tc.onFailure})
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, emulator.ErrCFNInvalidOnFailure)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestCFN_AnotherAccountsStackIsNotAPreviousDeployment pins the scope check behind the
// redeploy guard.
//
// Export names are scoped per account and Region, and so is stack identity: a record
// under the same *name* in another account is a different stack, and its resources are
// not the ones being redeployed. Reading it as the previous deployment would clear a
// genuine collision — the deployer would decide "I already own this, so the refusal is
// not a failure" on the strength of a record belonging to someone else, which is the
// one direction the guard must never err in.
//
// The collision is real rather than contrived: substrate's S3 bucket namespace is
// global, as AWS's is, so the second account's create is refused exactly as it would be.
func TestCFN_AnotherAccountsStackIsNotAPreviousDeployment(t *testing.T) {
	east, _, _, _, as := newSweepDeployerShared(t)
	ctx := context.Background()

	const tmpl = `{"Resources":{"Slot":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"rollback-scoped"}}}}`

	first, err := east.Deploy(ctx, tmpl, "scoped", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", first.Status)

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "another account in the same region", accountID: "999988887777", region: "us-east-1"},
		{name: "another region in the same account", accountID: "123456789012", region: "eu-west-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			other := as(tc.accountID, tc.region)
			result, err := other.DeployWithOptions(ctx, tmpl, "scoped", nil,
				emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing})
			require.NoError(t, err)
			assert.Equal(t, "CREATE_FAILED", result.Status,
				"a stack of the same name in another scope is not this stack's previous "+
					"deployment, so the collision stands")
			assert.Contains(t, result.StatusReason, "BucketAlreadyExists")
		})
	}
}
