package emulator_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The derivation under test is #501's answer to a question it filed open: whether
// stack events are read out of the event store or recorded separately. Neither, as
// it turns out. A recorded [emulator.Event] carries no LogicalResourceId — the
// store builds one from the request context and operation and never copies the
// per-resource metadata across — and every StackEvent needs one. The stack record
// has it, so the events are derived from the record, which also means an
// in-process [emulator.Client] caller (whose event store is disabled) gets events
// at all. TestCFNEvents_DerivesWithoutAnEventStore is the guard on that.

// cfnEventsAt is the instant the fixtures below use, so a test asserting a
// timestamp reads the same value the fixture wrote rather than a literal.
var cfnEventsAt = time.Date(2026, 3, 15, 20, 54, 30, 0, time.UTC)

// cfnEventsStackID is a stack ARN of the shape cfnStackID builds. The derivation
// takes it as an argument rather than building it, because only the wire boundary
// knows the requesting caller's partition.
const cfnEventsStackID = "arn:aws:cloudformation:us-east-1:123456789012:stack/demo/" +
	"11111111-2222-3333-4444-555555555555"

// cfnCleanStack is a two-resource stack that deployed cleanly. Resources are in
// deployment order, which is what cfnDeriveStackEvents reverses.
func cfnCleanStack() emulator.CFNStackState {
	return emulator.CFNStackState{
		StackName: "demo",
		Status:    "CREATE_COMPLETE",
		CreatedAt: cfnEventsAt,
		UpdatedAt: cfnEventsAt,
		Resources: []emulator.DeployedResource{
			{LogicalID: "Role", Type: "AWS::IAM::Role", PhysicalID: "demo-role"},
			{LogicalID: "Bucket", Type: "AWS::S3::Bucket", PhysicalID: "demo-bucket"},
		},
	}
}

// TestCFNEvents_DeriveShape asserts the whole event list a stack record produces,
// for the record shapes the wire cannot easily reach.
//
// Asserted as a full ordered list rather than a set of spot checks, because
// ordering *is* the contract: DescribeStackEvents documents "reverse chronological
// order", and a deploy's events share one instant (see the derivation's doc
// comment), so nothing but position can order them.
func TestCFNEvents_DeriveShape(t *testing.T) {
	type want struct {
		logicalID string
		status    string
		reason    string
	}
	tests := []struct {
		name  string
		stack emulator.CFNStackState
		want  []want
	}{
		{
			// Newest first: the stack's terminal event, then the resources in the
			// reverse of the order they deployed in, then the stack's opening event.
			// Real CloudFormation shows the last-deployed resource nearest the top.
			name:  "a clean create brackets its resources newest-first",
			stack: cfnCleanStack(),
			want: []want{
				{"demo", "CREATE_COMPLETE", ""},
				{"Bucket", "CREATE_COMPLETE", ""},
				{"Role", "CREATE_COMPLETE", ""},
				{"demo", "CREATE_IN_PROGRESS", "User Initiated"},
			},
		},
		{
			// #562's criterion in its purest form: the resource's recorded Error
			// becomes the event's ResourceStatusReason, so a caller polling events
			// learns which resource was refused and why. A stack left CREATE_FAILED
			// rather than rolled back is what OnFailure DO_NOTHING asks for.
			name: "a failed resource carries its error as the reason",
			stack: emulator.CFNStackState{
				StackName:    "demo",
				Status:       "CREATE_FAILED",
				StatusReason: "resource Bucket failed",
				CreatedAt:    cfnEventsAt,
				UpdatedAt:    cfnEventsAt,
				Resources: []emulator.DeployedResource{
					{LogicalID: "Role", Type: "AWS::IAM::Role", PhysicalID: "demo-role"},
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket",
						Error: "AccessDenied: not authorized to perform: s3:CreateBucket"},
				},
			},
			want: []want{
				{"demo", "CREATE_FAILED", "resource Bucket failed"},
				{"Bucket", "CREATE_FAILED",
					"AccessDenied: not authorized to perform: s3:CreateBucket"},
				{"Role", "CREATE_COMPLETE", ""},
				{"demo", "CREATE_IN_PROGRESS", "User Initiated"},
			},
		},
		{
			// The sweep's record overlays the resources it removed. Reporting
			// CREATE_COMPLETE for Role here would claim a resource the rollback
			// deleted — the failed one keeps its own reason, because the rollback
			// never created it to delete.
			name: "a rollback reports the sweep's outcome, not CREATE_COMPLETE",
			stack: emulator.CFNStackState{
				StackName:    "demo",
				Status:       "ROLLBACK_COMPLETE",
				StatusReason: "AccessDenied on Bucket",
				CreatedAt:    cfnEventsAt,
				UpdatedAt:    cfnEventsAt,
				Resources: []emulator.DeployedResource{
					{LogicalID: "Role", Type: "AWS::IAM::Role", PhysicalID: "demo-role"},
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket",
						Error: "AccessDenied on s3:CreateBucket"},
				},
				ResourceDeletions: []emulator.CFNResourceDeletion{
					{LogicalID: "Role", Type: "AWS::IAM::Role", Status: "DELETE_COMPLETE"},
				},
			},
			want: []want{
				{"demo", "ROLLBACK_COMPLETE", "AccessDenied on Bucket"},
				{"Bucket", "CREATE_FAILED", "AccessDenied on s3:CreateBucket"},
				{"Role", "DELETE_COMPLETE", ""},
				// A rollback is the tail of the CreateStack the caller made, so the
				// opening event is still CREATE_IN_PROGRESS.
				{"demo", "CREATE_IN_PROGRESS", "User Initiated"},
			},
		},
		{
			// A sweep that could not remove a resource says which and why. This is
			// the state that wedges a stack, so it is the one a consumer most needs
			// to be able to read out of events.
			name: "a failed sweep names the resource holding the stack",
			stack: emulator.CFNStackState{
				StackName:    "demo",
				Status:       "DELETE_FAILED",
				StatusReason: "resource Bucket could not be deleted",
				CreatedAt:    cfnEventsAt,
				UpdatedAt:    cfnEventsAt,
				Resources: []emulator.DeployedResource{
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket", PhysicalID: "demo-bucket"},
				},
				ResourceDeletions: []emulator.CFNResourceDeletion{
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket",
						Status: "DELETE_FAILED", Reason: "BucketNotEmpty"},
				},
			},
			want: []want{
				{"demo", "DELETE_FAILED", "resource Bucket could not be deleted"},
				{"Bucket", "DELETE_FAILED", "BucketNotEmpty"},
				{"demo", "DELETE_IN_PROGRESS", "User Initiated"},
			},
		},
		{
			// An update's opening event is UPDATE_IN_PROGRESS, and dated UpdatedAt:
			// CreatedAt belongs to the create the record no longer describes, so
			// using it would date this operation before it happened.
			name: "an update opens with UPDATE_IN_PROGRESS at UpdatedAt",
			stack: emulator.CFNStackState{
				StackName: "demo",
				Status:    "UPDATE_COMPLETE",
				CreatedAt: cfnEventsAt.Add(-time.Hour),
				UpdatedAt: cfnEventsAt,
				Resources: []emulator.DeployedResource{
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket", PhysicalID: "demo-bucket"},
				},
			},
			want: []want{
				{"demo", "UPDATE_COMPLETE", ""},
				{"Bucket", "CREATE_COMPLETE", ""},
				{"demo", "UPDATE_IN_PROGRESS", "User Initiated"},
			},
		},
		{
			// A record restored from a snapshot written before Status existed gets
			// no invented opening event. Guessing one would be the fabrication the
			// whole model exists to avoid.
			name: "a record with no status yields only its resources",
			stack: emulator.CFNStackState{
				StackName: "demo",
				UpdatedAt: cfnEventsAt,
				Resources: []emulator.DeployedResource{
					{LogicalID: "Bucket", Type: "AWS::S3::Bucket", PhysicalID: "demo-bucket"},
				},
			},
			want: []want{{"Bucket", "CREATE_COMPLETE", ""}},
		},
		{
			// A stack with no resources still reports its own two events: the
			// deploy did start and did finish, both recorded.
			name: "an empty stack still brackets itself",
			stack: emulator.CFNStackState{
				StackName: "demo",
				Status:    "CREATE_COMPLETE",
				CreatedAt: cfnEventsAt,
				UpdatedAt: cfnEventsAt,
			},
			want: []want{
				{"demo", "CREATE_COMPLETE", ""},
				{"demo", "CREATE_IN_PROGRESS", "User Initiated"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := emulator.CFNDeriveStackEventsForTest(tt.stack, cfnEventsStackID)
			require.Len(t, events, len(tt.want))
			for i, w := range tt.want {
				assert.Equal(t, w.logicalID, events[i].LogicalResourceID, "event %d logical ID", i)
				assert.Equal(t, w.status, events[i].ResourceStatus, "event %d status", i)
				assert.Equal(t, w.reason, events[i].ResourceStatusReason, "event %d reason", i)
			}
		})
	}
}

// TestCFNEvents_MemberShapes asserts the members each kind of event carries.
//
// The StackEvent shape requires only EventId, StackId, StackName and Timestamp,
// which is what lets the two kinds differ: a stack-level event's
// PhysicalResourceId is the stack ARN and its type is AWS::CloudFormation::Stack,
// while a resource event's are the resource's own.
func TestCFNEvents_MemberShapes(t *testing.T) {
	events := emulator.CFNDeriveStackEventsForTest(cfnCleanStack(), cfnEventsStackID)
	require.Len(t, events, 4)

	wantTime := cfnEventsAt.Format(time.RFC3339)
	for i, e := range events {
		assert.Equal(t, cfnEventsStackID, e.StackID, "event %d", i)
		assert.Equal(t, "demo", e.StackName, "event %d", i)
		assert.Equal(t, wantTime, e.Timestamp, "event %d", i)
		assert.NotEmpty(t, e.EventID, "event %d: EventId is a required member", i)
	}

	stackEvent := events[0]
	assert.Equal(t, "AWS::CloudFormation::Stack", stackEvent.ResourceType)
	assert.Equal(t, "demo", stackEvent.LogicalResourceID,
		"a stack's own event names the stack as its logical resource")
	assert.Equal(t, cfnEventsStackID, stackEvent.PhysicalResourceID,
		"and the stack ARN as its physical one")

	resourceEvent := events[1]
	assert.Equal(t, "AWS::S3::Bucket", resourceEvent.ResourceType)
	assert.Equal(t, "Bucket", resourceEvent.LogicalResourceID)
	assert.Equal(t, "demo-bucket", resourceEvent.PhysicalResourceID)
}

// TestCFNEvents_EventIDsAreDeterministicAndUnique is the replay criterion.
//
// #501 asks that a replayed stack report identical events, so a random UUID is
// disqualified however well it satisfies the API's uniqueness requirement. The
// resource form is the one AWS's own sample response returns —
// "MyEC2Instance-CREATE_COMPLETE-2016-03-15T20:54:30.174Z" — which is both
// deterministic and observed rather than invented.
func TestCFNEvents_EventIDsAreDeterministicAndUnique(t *testing.T) {
	first := emulator.CFNDeriveStackEventsForTest(cfnCleanStack(), cfnEventsStackID)
	second := emulator.CFNDeriveStackEventsForTest(cfnCleanStack(), cfnEventsStackID)
	require.Equal(t, first, second, "the same record must derive byte-identical events")

	seen := map[string]bool{}
	for _, e := range first {
		assert.False(t, seen[e.EventID], "EventId %q repeated", e.EventID)
		seen[e.EventID] = true
	}

	assert.Equal(t, "Bucket-CREATE_COMPLETE-"+cfnEventsAt.Format(time.RFC3339), first[1].EventID,
		"a resource EventId follows AWS's observed logicalID-status-timestamp form")

	// Two stacks are not allowed to collide: the stack-level ID is derived from the
	// stack ARN, which carries the name, region and account.
	other := cfnCleanStack()
	other.StackName = "other"
	otherEvents := emulator.CFNDeriveStackEventsForTest(other, cfnEventsStackID+"-other")
	assert.NotEqual(t, first[0].EventID, otherEvents[0].EventID)
}

// TestCFNEvents_Paginate round-trips a NextToken.
//
// DescribeStackEvents takes no MaxResults, so the page size is substrate's, and the
// only way a consumer's token loop is testable is if a large enough stack actually
// truncates. The page boundary is built from the exported constant rather than a
// second copy of the number.
func TestCFNEvents_Paginate(t *testing.T) {
	// One resource past a full page, so exactly one event spills: the stack's
	// bracketing pair plus the resources is pageSize+1 events.
	stack := emulator.CFNStackState{
		StackName: "demo",
		Status:    "CREATE_COMPLETE",
		CreatedAt: cfnEventsAt,
		UpdatedAt: cfnEventsAt,
	}
	for i := range emulator.CFNStackEventsPageSizeForTest - 1 {
		stack.Resources = append(stack.Resources, emulator.DeployedResource{
			LogicalID:  fmt.Sprintf("R%03d", i),
			Type:       "AWS::S3::Bucket",
			PhysicalID: fmt.Sprintf("bucket-%03d", i),
		})
	}
	events := emulator.CFNDeriveStackEventsForTest(stack, cfnEventsStackID)
	require.Len(t, events, emulator.CFNStackEventsPageSizeForTest+1)

	page, token := emulator.CFNPaginateEventsForTest(events, "")
	assert.Len(t, page, emulator.CFNStackEventsPageSizeForTest)
	require.NotEmpty(t, token, "a truncated page must hand back a NextToken")
	assert.Equal(t, events[:emulator.CFNStackEventsPageSizeForTest], page)

	rest, nextToken := emulator.CFNPaginateEventsForTest(events, token)
	assert.Empty(t, nextToken, "the last page carries no token")
	require.Len(t, rest, 1)
	assert.Equal(t, events[len(events)-1], rest[0],
		"the second page resumes where the first stopped, losing nothing")

	// A complete list is one page and no token — the case a substrate-sized stack
	// almost always hits, and the one a consumer's loop must terminate on.
	page, token = emulator.CFNPaginateEventsForTest(events[:3], "")
	assert.Len(t, page, 3)
	assert.Empty(t, token)

	// A token substrate did not mint restarts from the beginning rather than
	// erroring: DescribeStackEvents documents no service-specific errors, so there
	// is no code to return, and dropping the whole list would be worse.
	page, _ = emulator.CFNPaginateEventsForTest(events, "not-base64-at-all")
	assert.Equal(t, events[0], page[0])
}

// TestCFNEvents_DerivesWithoutAnEventStore is the guard on #501's design decision.
//
// The issue's own preferred route was to derive events from the recorded event
// stream. That route is unavailable for two measured reasons, and this test pins
// the second: an in-process caller's plugin has no event store — the CFN plugin
// substitutes a disabled one when none is passed — so a derivation that read the
// log would report an empty list here while the stack plainly deployed. If this
// test starts failing, the derivation has been pointed at the event store.
//
// The first reason has no test because it is a property of the [emulator.Event]
// type: it carries no LogicalResourceId, and every StackEvent needs one.
func TestCFNEvents_DerivesWithoutAnEventStore(t *testing.T) {
	ctx := context.Background()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(cfnEventsAt)

	s3p := &emulator.S3Plugin{}
	require.NoError(t, s3p.Initialize(ctx, emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	registry := emulator.NewPluginRegistry()
	registry.Register(s3p)

	// No "event_store" option, which is the whole point: this is the configuration
	// an emulator.Client caller's plugin runs in.
	cfnp := &emulator.CloudFormationPlugin{}
	require.NoError(t, cfnp.Initialize(ctx, emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(cfnp)

	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: cfnEventsAt,
	}
	create := &emulator.AWSRequest{
		Service:   "cloudformation",
		Operation: "CreateStack",
		Params: map[string]string{
			"StackName": "storeless",
			"TemplateBody": `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket",` +
				`"Properties":{"BucketName":"storeless-bucket"}}}}`,
		},
		Headers: map[string]string{},
	}
	_, err := cfnp.HandleRequest(reqCtx, create)
	require.NoError(t, err)

	resp, err := cfnp.HandleRequest(reqCtx, &emulator.AWSRequest{
		Service:   "cloudformation",
		Operation: "DescribeStackEvents",
		Params:    map[string]string{"StackName": "storeless"},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parsed struct {
		Events []struct {
			LogicalResourceID string `xml:"LogicalResourceId"`
			ResourceStatus    string `xml:"ResourceStatus"`
		} `xml:"DescribeStackEventsResult>StackEvents>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &parsed), "body was %s", resp.Body)
	require.Len(t, parsed.Events, 3, "body was %s", resp.Body)
	assert.Equal(t, "storeless", parsed.Events[0].LogicalResourceID)
	assert.Equal(t, "CREATE_COMPLETE", parsed.Events[0].ResourceStatus)
	assert.Equal(t, "Bucket", parsed.Events[1].LogicalResourceID,
		"the resource event names the logical resource, which the event log cannot")
	assert.Equal(t, "CREATE_COMPLETE", parsed.Events[1].ResourceStatus)
}
