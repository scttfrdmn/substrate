package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestCFN_DeployerErrorsCarryTheirClassification asserts that every
// StackDeployer failure the wire plugin has to classify wraps a sentinel, and
// that the message it carries is byte-for-byte what it was before the sentinels
// existed.
//
// Both halves matter. The sentinel is what lets cfnMapDeployerError use
// errors.Is; the unchanged message is what makes the change safe to land on its
// own, since anything reading a substrate log sees exactly what it saw before.
func TestCFN_DeployerErrorsCarryTheirClassification(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	const tmpl = `{"Resources":{"B":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"cls-bucket"}}}}`
	_, err := d.Deploy(ctx, tmpl, "cls-stack", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		call    func() error
		class   error
		message string
	}{
		{
			name: "CreateChangeSet on an absent stack",
			call: func() error {
				_, err := d.CreateChangeSet(ctx, "no-such-stack", "cs", tmpl, nil)
				return err
			},
			class:   emulator.ErrCFNStackNotFound,
			message: `cfn CreateChangeSet: stack "no-such-stack" not found`,
		},
		{
			name: "DescribeChangeSet on an absent change set",
			call: func() error {
				_, err := d.DescribeChangeSet(ctx, "cls-stack", "no-such-cs")
				return err
			},
			class:   emulator.ErrCFNChangeSetNotFound,
			message: `cfn DescribeChangeSet: change set "no-such-cs" not found`,
		},
		{
			name: "DetectStackDrift on an absent stack",
			call: func() error {
				_, err := d.DetectStackDrift(ctx, "no-such-stack")
				return err
			},
			class:   emulator.ErrCFNStackNotFound,
			message: `cfn DetectStackDrift: stack "no-such-stack" not found`,
		},
		{
			name: "StartStackDriftDetection on an absent stack",
			call: func() error {
				_, err := d.StartStackDriftDetection(ctx, "no-such-stack")
				return err
			},
			class:   emulator.ErrCFNStackNotFound,
			message: `cfn StartStackDriftDetection: stack "no-such-stack" not found`,
		},
		{
			name: "DescribeStackDriftDetectionStatus on an absent detection",
			call: func() error {
				_, err := d.DescribeStackDriftDetectionStatus(ctx, "no-such-detection")
				return err
			},
			class:   emulator.ErrCFNDriftDetectionNotFound,
			message: `cfn DescribeStackDriftDetectionStatus: detection "no-such-detection" not found`,
		},
		{
			name: "Deploy with an unparseable template",
			call: func() error {
				_, err := d.Deploy(ctx, "{ not a template", "bad-stack", nil)
				return err
			},
			class: emulator.ErrCFNTemplateInvalid,
		},
		{
			name: "CreateChangeSet with an unparseable new template",
			call: func() error {
				_, err := d.CreateChangeSet(ctx, "cls-stack", "cs", "{ not a template", nil)
				return err
			},
			class: emulator.ErrCFNTemplateInvalid,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.class)
			if tc.message != "" {
				assert.Equal(t, tc.message, err.Error(),
					"the message must be unchanged: a consumer's logs already carry it")
			}
		})
	}
}

// TestCFN_TemplateParseFailureKeepsItsCause asserts that classifying a parse
// failure did not swallow the underlying decoder error.
//
// cfnErrf builds the message with fmt.Errorf, so a %w in the format still wraps.
// Losing that would leave a caller with "parse template" and no statement of what
// about the template was wrong.
func TestCFN_TemplateParseFailureKeepsItsCause(t *testing.T) {
	d := newTestDeployer(t)
	_, err := d.Deploy(context.Background(), "{ not a template", "bad-stack", nil)
	require.Error(t, err)

	assert.ErrorIs(t, err, emulator.ErrCFNTemplateInvalid)
	assert.Contains(t, err.Error(), "parse template: ")
	assert.NotErrorIs(t, err, emulator.ErrCFNStackNotFound)

	// The decoder's own error must still be reachable, not merely quoted in the
	// message. A caller that wants to know *what* was malformed reads the
	// SyntaxError's offset; a classification that replaced the cause rather than
	// sitting above it would leave only prose.
	var syntaxErr *json.SyntaxError
	require.ErrorAs(t, err, &syntaxErr,
		"the decoder's error must survive underneath the classification")
	assert.Positive(t, syntaxErr.Offset)
}

// TestCFN_ResourceDeployFailureFromATemplate provokes the deploy-resource path
// through a real template rather than by building the error, so the wrapping at
// the call site is asserted too.
//
// A YAML mapping with a non-string key decodes fine and then cannot be
// JSON-marshaled into a plugin request, which is a resource that failed to
// deploy for a reason the caller did cause but that no pre-flight catches — the
// one shape reaching Deploy's error return.
func TestCFN_ResourceDeployFailureFromATemplate(t *testing.T) {
	d := newTestDeployer(t)

	const tmpl = "Resources:\n" +
		"  Table:\n" +
		"    Type: AWS::DynamoDB::Table\n" +
		"    Properties:\n" +
		"      TableName: unmarshalable\n" +
		"      KeySchema:\n" +
		"        - 1: pk\n"

	_, err := d.Deploy(context.Background(), tmpl, "resource-fail", nil)
	require.Error(t, err)

	assert.ErrorIs(t, err, emulator.ErrCFNResourceDeployFailed)
	assert.NotErrorIs(t, err, emulator.ErrCFNTemplateInvalid,
		"the template parsed; it was the resource that could not be deployed")
	assert.NotErrorIs(t, err, emulator.ErrCFNStackNotFound)

	var deployErr *emulator.CFNResourceDeployError
	require.ErrorAs(t, err, &deployErr)
	assert.Equal(t, "Table", deployErr.LogicalID,
		"the failing resource must be named without re-parsing the message")
	assert.Equal(t, "deploy resource Table: marshal dynamodb body: "+
		"json: unsupported type: map[interface {}]interface {}", err.Error())

	awsErr := emulator.CFNMapDeployerErrorForTest(err)
	require.NotNil(t, awsErr)
	assert.Equal(t, "InternalFailure", awsErr.Code)
	assert.Equal(t, http.StatusInternalServerError, awsErr.HTTPStatus)
}

// TestCFN_ResourceDeployFailureNamesTheResource pins the one classification a
// substring match could not make.
//
// "deploy resource %s: %w" wraps whatever the plugin returned, and a plugin's own
// message may well contain "not found" — an instance whose AMI does not resolve,
// say. Under the old string match that reported ValidationError at 400, as though
// the caller had named an absent stack. It is substrate failing to build
// something the caller correctly asked for, which is a 500.
func TestCFN_ResourceDeployFailureNamesTheResource(t *testing.T) {
	inner := errors.New("security group sg-missing not found")
	err := fmt.Errorf("wrapped: %w", &emulator.CFNResourceDeployError{
		LogicalID: "Worker",
		Err:       inner,
	})

	assert.ErrorIs(t, err, emulator.ErrCFNResourceDeployFailed)
	assert.NotErrorIs(t, err, emulator.ErrCFNStackNotFound,
		"a resource failure whose cause says 'not found' is not a missing stack")
	assert.ErrorIs(t, err, inner, "the plugin's own reason must still be reachable")

	var deployErr *emulator.CFNResourceDeployError
	require.ErrorAs(t, err, &deployErr)
	assert.Equal(t, "Worker", deployErr.LogicalID)
	assert.Contains(t, err.Error(), "deploy resource Worker: ")

	awsErr := emulator.CFNMapDeployerErrorForTest(err)
	require.NotNil(t, awsErr)
	assert.Equal(t, "InternalFailure", awsErr.Code)
	assert.Equal(t, http.StatusInternalServerError, awsErr.HTTPStatus)
}

// TestCFN_ErrorCodeDoesNotDependOnMessageWording is the regression test for
// #502's actual complaint.
//
// Each case carries a message that shares no word with the strings the old switch
// matched on, so a mapping that still read the text would fall through to
// InternalFailure at 500. Rewording a deployer message is now a copy-edit rather
// than a silent change to every consumer's error handling.
func TestCFN_ErrorCodeDoesNotDependOnMessageWording(t *testing.T) {
	tests := []struct {
		name       string
		class      error
		message    string
		wantCode   string
		wantStatus int
		wantPrefix string
	}{
		{
			name:       "a reworded stack lookup is still a ValidationError",
			class:      emulator.ErrCFNStackNotFound,
			message:    `cfn CreateChangeSet: no stack named "x" exists`,
			wantCode:   "ValidationError",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "a reworded change-set lookup is still a ValidationError",
			class:      emulator.ErrCFNChangeSetNotFound,
			message:    `cfn DescribeChangeSet: no such change set "y"`,
			wantCode:   "ValidationError",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "a reworded drift lookup is still a ValidationError",
			class:      emulator.ErrCFNDriftDetectionNotFound,
			message:    `cfn DescribeStackDriftDetectionStatus: unknown detection "z"`,
			wantCode:   "ValidationError",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "a reworded decode failure is still a template format error",
			class:      emulator.ErrCFNTemplateInvalid,
			message:    "the body could not be decoded as JSON or YAML",
			wantCode:   "ValidationError",
			wantStatus: http.StatusBadRequest,
			wantPrefix: "Template format error: ",
		},
		{
			name:       "a resource failure is a 500 whatever it says",
			class:      emulator.ErrCFNResourceDeployFailed,
			message:    "deploy resource Worker: bucket not found",
			wantCode:   "InternalFailure",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "a state-manager refusal is a 500",
			class:      emulator.ErrCFNStateRequired,
			message:    "cfn CreateChangeSet: state manager required",
			wantCode:   "InternalFailure",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := emulator.CFNClassifiedErrorForTest(tc.class, tc.message)
			require.ErrorIs(t, err, tc.class)
			assert.Equal(t, tc.message, err.Error())

			awsErr := emulator.CFNMapDeployerErrorForTest(err)
			require.NotNil(t, awsErr)
			assert.Equal(t, tc.wantCode, awsErr.Code)
			assert.Equal(t, tc.wantStatus, awsErr.HTTPStatus)
			assert.Equal(t, tc.wantPrefix+tc.message, awsErr.Message)
		})
	}
}

// TestCFN_MapDeployerErrorClassifiesUnknownFailuresAsInternal pins the two edges
// of the mapping: nil in, nil out, and an unclassified error as a 500 rather than
// a 400.
//
// An unclassified error is one no deployer path builds, so treating it as the
// caller's fault would be a guess. Reporting it as substrate's failure is the
// honest answer and the one a consumer can act on — a 400 tells them to fix their
// request, which would be wrong.
func TestCFN_MapDeployerErrorClassifiesUnknownFailuresAsInternal(t *testing.T) {
	assert.Nil(t, emulator.CFNMapDeployerErrorForTest(nil))

	awsErr := emulator.CFNMapDeployerErrorForTest(errors.New("stack not found somewhere"))
	require.NotNil(t, awsErr)
	assert.Equal(t, "InternalFailure", awsErr.Code,
		"an unwrapped error that merely reads like a lookup failure is not one")
	assert.Equal(t, http.StatusInternalServerError, awsErr.HTTPStatus)
}

// TestCFNPlugin_ClassifiedErrorsReachTheWire asserts the classification survives
// the whole path a consumer uses, not just the mapping function.
//
// The handlers pre-flight most not-found cases themselves, so this asserts the
// path that has no pre-flight: an unparseable body handed to CreateStack, which
// reaches the deployer and comes back as a ValidationError at 400.
func TestCFNPlugin_ClassifiedErrorsReachTheWire(t *testing.T) {
	ts := newCFNTestServer(t)

	status, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "bad-body",
		"TemplateBody": "{ not a template",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Contains(t, body, "Template format error")
}
