package emulator_test

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file covers #716: the answer a plugin gives for an operation it does not
// implement.
//
// Every plugin used to write that answer itself, in eight different shapes, and each
// one named the plugin's own Go type — "SSMPlugin: unknown operation \"Foo\"". No AWS
// endpoint emits a Go type name, so a consumer's error branch was reading a substrate
// implementation detail. Worse, forty-four of the fifty-nine sites answered
// InvalidAction, which is the Query protocol's code, on a service whose protocol is
// JSON; AWS publishes UnknownOperationException at HTTP 404 for those.
//
// Both halves are pinned here. The table asserts the exact code, status and message
// for one service per protocol arm, and the sweep asserts across *every* registered
// plugin that no answer names a Go type — which is the assertion that stays true as
// plugins are added, since a new plugin joins the sweep for free.

// unknownActionRequest builds a request for an operation no AWS service publishes.
//
// It fills every field a plugin's dispatcher might read — Operation for a JSON-RPC or
// Query service, HTTPMethod and Path for a REST one, and Action for a Query service
// parsed from form parameters — so one request reaches the default arm whatever the
// plugin routes on. No X-Amz-Target header is set, because each JSON-RPC plugin trims
// its own service prefix from it and a foreign prefix would leave a plugin dispatching
// on a value this function did not choose.
func unknownActionRequest(service string) *emulator.AWSRequest {
	const op = "SubstrateNoSuchOperation"
	return &emulator.AWSRequest{
		Service:    service,
		Operation:  op,
		HTTPMethod: http.MethodPost,
		Path:       "/substrate-no-such-path",
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       []byte("{}"),
		Params:     map[string]string{"Action": op},
	}
}

// unknownActionRegistry builds the same plugin set the server builds, so the sweep
// covers every service a consumer can reach rather than a hand-kept list.
func unknownActionRegistry(t *testing.T) (*emulator.PluginRegistry, *emulator.RequestContext) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	require.NoError(t, emulator.RegisterDefaultPlugins(
		context.Background(),
		registry,
		emulator.NewMemoryStateManager(),
		emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
		emulator.NewDefaultLogger(slog.LevelError, false),
		nil,
		nil,
	))
	return registry, &emulator.RequestContext{
		RequestID: "req-unknown-action",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Metadata:  make(map[string]interface{}),
	}
}

// TestUnknownAction_AnswerComesFromTheProtocol pins the code, status and message for
// one service per protocol arm.
//
// The two JSON families answer UnknownOperationException at 404, which is what AWS's
// Common Errors pages publish — checked on DynamoDB's (JSON-RPC) and Lambda's
// (REST-JSON), which agree on both. The Query, "ec2" and REST-XML families answer
// InvalidAction at 400, which AWS no longer publishes anywhere: the InvalidAction
// entry has been removed from EC2's, IAM's and SNS's Common Errors pages, and no
// replacement was put in its place. Substrate keeps it because it is the code the
// Query protocol has always used and an SDK caller's error branch is written against
// it, so that row is substrate's choice rather than a citation.
//
// The message wording is substrate's in both arms — AWS publishes a description, not
// a wire message — but naming the action is the point of it, so each row asserts the
// name reaches the caller.
func TestUnknownAction_AnswerComesFromTheProtocol(t *testing.T) {
	registry, reqCtx := unknownActionRegistry(t)

	tests := []struct {
		name       string
		service    string
		protocol   string
		wantCode   string
		wantStatus int
		// wantMessage is the whole message, so a change to the wording is visible here
		// rather than only in whatever a consumer happens to match on.
		wantMessage string
	}{
		{
			name: "ec2 is its own protocol", service: "ec2", protocol: "ec2",
			wantCode: "InvalidAction", wantStatus: http.StatusBadRequest,
			wantMessage: "The action SubstrateNoSuchOperation is not valid for this endpoint.",
		},
		{
			name: "sns is query", service: "sns", protocol: "query",
			wantCode: "InvalidAction", wantStatus: http.StatusBadRequest,
			wantMessage: "The action SubstrateNoSuchOperation is not valid for this endpoint.",
		},
		{
			name: "cloudformation is query", service: "cloudformation", protocol: "query",
			wantCode: "InvalidAction", wantStatus: http.StatusBadRequest,
			wantMessage: "The action SubstrateNoSuchOperation is not valid for this endpoint.",
		},
		{
			// Route 53 is REST-XML and resolves by verb and path, so it exercises both the
			// XML arm and the route form of the message in one row.
			name: "route53 is rest-xml and routes by path", service: "route53", protocol: "rest-xml",
			wantCode: "InvalidAction", wantStatus: http.StatusBadRequest,
			wantMessage: "The action POST /substrate-no-such-path is not valid for this endpoint.",
		},
		{
			name: "dynamodb is json", service: "dynamodb", protocol: "json",
			wantCode: "UnknownOperationException", wantStatus: http.StatusNotFound,
			wantMessage: "The action SubstrateNoSuchOperation is not recognized.",
		},
		{
			name: "ssm is json", service: "ssm", protocol: "json",
			wantCode: "UnknownOperationException", wantStatus: http.StatusNotFound,
			wantMessage: "The action SubstrateNoSuchOperation is not recognized.",
		},
		{
			name: "lambda is rest-json and routes by path", service: "lambda", protocol: "rest-json",
			wantCode: "UnknownOperationException", wantStatus: http.StatusNotFound,
			wantMessage: "The action POST /substrate-no-such-path is not recognized.",
		},
		{
			name: "account is rest-json and routes by path", service: "account", protocol: "rest-json",
			wantCode: "UnknownOperationException", wantStatus: http.StatusNotFound,
			wantMessage: "The action POST /substrate-no-such-path is not recognized.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := registry.RouteRequest(reqCtx, unknownActionRequest(tt.service))
			require.Nil(t, resp, "a refusal carries no response")
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, tt.wantCode, awsErr.Code, "%s is %s", tt.service, tt.protocol)
			assert.Equal(t, tt.wantStatus, awsErr.HTTPStatus, "%s is %s", tt.service, tt.protocol)
			assert.Equal(t, tt.wantMessage, awsErr.Message)
		})
	}
}

// TestUnknownAction_IAMKeepsItsResponseShape pins the one plugin that answers a
// refusal as a 4xx *AWSResponse with a nil error rather than as an *AWSError.
//
// Both conventions are in use across the tree and #516 turns on that, so #716 changed
// only where IAM's code, message and status come from — not the shape it returns them
// in. Without this case the sweep below would read IAM's answer as a success and
// assert nothing about it.
func TestUnknownAction_IAMKeepsItsResponseShape(t *testing.T) {
	registry, reqCtx := unknownActionRegistry(t)

	resp, err := registry.RouteRequest(reqCtx, unknownActionRequest("iam"))
	require.NoError(t, err, "IAM answers a refusal as a response, not an error")
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "IAM is a query service")
	body := string(resp.Body)
	assert.Contains(t, body, "InvalidAction", "body: %s", body)
	assert.Contains(t, body, "SubstrateNoSuchOperation", "body: %s", body)
}

// TestUnknownAction_NoServiceNamesAGoType is the tripwire, and it runs over every
// registered plugin.
//
// "Plugin" is the substring to look for because every plugin's Go type is named for
// it — SSMPlugin, EC2Plugin, S3Plugin — and all eight of the old wordings put that
// type at the front of the message. No AWS error message contains the word, so a hit
// here means a refusal is leaking a substrate internal again.
//
// A plugin that answers something other than a refusal is not a failure of this test:
// a few resolve a bogus path to a real operation and answer a normal error for it, and
// S3 reads any unrecognized path as a bucket request. What matters is that whatever
// they answer, no consumer reads a Go type out of it.
func TestUnknownAction_NoServiceNamesAGoType(t *testing.T) {
	registry, reqCtx := unknownActionRegistry(t)

	names := registry.Names()
	require.Greater(t, len(names), 50, "the sweep is only meaningful over the whole plugin set")

	for _, service := range names {
		t.Run(service, func(t *testing.T) {
			resp, err := registry.RouteRequest(reqCtx, unknownActionRequest(service))

			if err != nil {
				assert.NotContains(t, err.Error(), "Plugin",
					"%s names a Go type in its refusal", service)
				var awsErr *emulator.AWSError
				if assert.ErrorAs(t, err, &awsErr) {
					assert.NotEmpty(t, awsErr.Code, "%s refuses with an empty code", service)
				}
			}
			if resp != nil {
				assert.NotContains(t, string(resp.Body), "Plugin",
					"%s names a Go type in its response body", service)
			}
		})
	}
}

// TestUnknownAction_BlankActionIsNamed pins the normalization for a dispatcher that
// reached its default arm with no operation name at all.
//
// This is reachable rather than theoretical: a JSON-RPC service dispatches on an
// X-Amz-Target header a caller can simply omit, and a REST plugin's path resolver
// answers "" for a path it does not recognize. Without the guard the message read
// "The action  is not recognized." — two spaces where the name belongs, which tells a
// consumer nothing.
func TestUnknownAction_BlankActionIsNamed(t *testing.T) {
	registry, reqCtx := unknownActionRegistry(t)

	req := unknownActionRequest("dynamodb")
	req.Operation = ""
	req.Params = map[string]string{}

	_, err := registry.RouteRequest(reqCtx, req)
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "The action (empty) is not recognized.", awsErr.Message)
	assert.NotContains(t, awsErr.Message, "  ", "a blank name leaves no doubled space")
	assert.False(t, strings.Contains(awsErr.Message, "action  is"),
		"the name is normalized rather than omitted")
}
