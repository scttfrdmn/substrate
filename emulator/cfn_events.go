package emulator

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// cfnStackEvent is one member of a DescribeStackEvents response.
//
// The XML member names are the StackEvent shape's. EventId, StackId, StackName and
// Timestamp are the shape's only required members; the resource fields are all
// documented "Required: No", which is what lets a stack-level event carry no
// physical ID and a resource event carry no reason.
type cfnStackEvent struct {
	EventID              string `xml:"EventId"`
	StackID              string `xml:"StackId"`
	StackName            string `xml:"StackName"`
	LogicalResourceID    string `xml:"LogicalResourceId,omitempty"`
	PhysicalResourceID   string `xml:"PhysicalResourceId,omitempty"`
	ResourceType         string `xml:"ResourceType,omitempty"`
	Timestamp            string `xml:"Timestamp"`
	ResourceStatus       string `xml:"ResourceStatus,omitempty"`
	ResourceStatusReason string `xml:"ResourceStatusReason,omitempty"`
}

// cfnStackEventResourceType is the ResourceType a stack's own events carry, as
// AWS's own DescribeStackEvents sample response shows.
const cfnStackEventResourceType = "AWS::CloudFormation::Stack"

// cfnStackEventsPageSize is how many events one page carries.
//
// DescribeStackEvents takes no MaxResults parameter — only StackName and
// NextToken — so the page size is the service's to choose, and 100 is the size
// real CloudFormation is observed to return (the CLI's --max-items is applied
// client-side over the same token loop). Paging at all is what makes a consumer's
// NextToken loop testable; a substrate stack small enough to fit in one page
// simply gets no token, which is also what AWS does.
const cfnStackEventsPageSize = 100

// cfnResourceStatus derives one resource's status and reason from the stack
// record.
//
// Nothing here is invented: DeployedResource.Error is set when a resource failed
// and empty when it succeeded, and a rollback's sweep records per-resource
// deletion outcomes of its own. It is shared by DescribeStackResources and
// DescribeStackEvents so the two views cannot disagree about whether a resource
// failed — they are two renderings of one fact, not two derivations.
func cfnResourceStatus(s CFNStackState, r DeployedResource) (status, reason string) {
	status, reason = "CREATE_COMPLETE", ""
	if r.Error != "" {
		status, reason = "CREATE_FAILED", r.Error
	}
	// A rollback swept the resources the failed create had created, so reporting
	// CREATE_COMPLETE for one would claim a resource that is gone. The sweep's own
	// record is what says which, and why.
	if del := cfnDeletionFor(s.ResourceDeletions, r.LogicalID); del != nil && r.Error == "" {
		status = del.Status
		reason = del.Reason
	}
	return status, reason
}

// cfnDeriveStackEvents renders a stack's recorded state as the events
// DescribeStackEvents reports, newest first.
//
// This operation was refused outright until #501, and the refusal's reasoning is
// why the model looks like this. Substrate carries no per-resource event log, so
// synthesizing a plausible CREATE_IN_PROGRESS/CREATE_COMPLETE pair per resource
// would invent observations a real stack never produced — the opposite of the
// provenance rule in docs/fidelity.md. Three decisions keep that rule intact:
//
// Derived from the stack record, not the event store. Every StackEvent needs a
// LogicalResourceId, and a recorded [Event] has none: [EventStore.RecordRequest]
// builds an event from the request context and operation only, and never copies
// reqCtx.Metadata onto it, so the log knows "s3:CreateBucket at T" but not which
// logical resource that was. The record knows. The store is also disabled for
// [Client] callers, who would otherwise see no events at all.
//
// Terminal resource states only. A deploy is synchronous, so no caller ever
// observed a resource mid-create and substrate holds no record that one existed.
// The stack's own bracketing events are different: the record's Status names the
// operation that ran, and CreatedAt/UpdatedAt say when, so an opening
// *_IN_PROGRESS is a fact rather than a guess.
//
// No invented timestamps. Timestamps come from the record's CreatedAt/UpdatedAt,
// which are [TimeController]-sourced. A synchronous deploy's events therefore
// share one instant. Spreading them apart is not available: [TimeController.Now]
// advances with wall time, so any interval substrate manufactured would make a
// consumer's assertions wall-clock dependent — the same finding sqs_control.go
// already records, and its reason for counting observations rather than measuring
// durations. Ordering is carried by position instead: stack.Resources is in
// deployment order, so reversing it puts the last-deployed resource first, which is
// what real CloudFormation shows.
//
// Only the most recent operation is derivable. An update overwrites the record's
// Status and UpdatedAt, so the create that preceded it left nothing behind to
// report.
func cfnDeriveStackEvents(s CFNStackState, stackID string) []cfnStackEvent {
	// Built oldest-first, because that is the order the record describes, then
	// reversed at the end.
	events := make([]cfnStackEvent, 0, len(s.Resources)+2)

	// The opening event. Its status names the operation the terminal status
	// reports on, and "User Initiated" is the reason real CloudFormation puts on a
	// caller-initiated stack's first event — every substrate stack is one.
	opening, openingAt := cfnStackOpeningEvent(s)
	if opening != "" {
		events = append(events, cfnStackLevelEvent(s, stackID, opening, "User Initiated", openingAt))
	}

	for _, r := range s.Resources {
		status, reason := cfnResourceStatus(s, r)
		events = append(events, cfnStackEvent{
			EventID:              cfnResourceEventID(r.LogicalID, status, cfnTime(s.UpdatedAt)),
			StackID:              stackID,
			StackName:            s.StackName,
			LogicalResourceID:    r.LogicalID,
			PhysicalResourceID:   r.PhysicalID,
			ResourceType:         r.Type,
			Timestamp:            cfnTime(s.UpdatedAt),
			ResourceStatus:       status,
			ResourceStatusReason: reason,
		})
	}

	if s.Status != "" {
		events = append(events, cfnStackLevelEvent(s, stackID, s.Status, s.StatusReason, s.UpdatedAt))
	}

	// Newest first, as the API documents. Reversing the record's order *is* the whole
	// ordering, with no timestamp sort behind it, because the list is already
	// non-decreasing in time when built: the opening event is at CreatedAt (or at
	// UpdatedAt for a later operation) and everything after it is at UpdatedAt, which
	// is always tc.Now() at or after CreatedAt. A sort was written here first and
	// found to be unreachable — it could never reorder anything — so it is gone
	// rather than left as a branch no test can reach.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events
}

// cfnStackLevelEvent builds one event about the stack itself.
//
// AWS's sample response is the shape being matched: the stack's LogicalResourceId
// is its name, its PhysicalResourceId is its ARN, and its EventId is UUID-shaped
// rather than the resource events' composite form.
func cfnStackLevelEvent(s CFNStackState, stackID, status, reason string, at time.Time) cfnStackEvent {
	return cfnStackEvent{
		EventID:              cfnDeterministicUUID(stackID, status, cfnTime(at)),
		StackID:              stackID,
		StackName:            s.StackName,
		LogicalResourceID:    s.StackName,
		PhysicalResourceID:   stackID,
		ResourceType:         cfnStackEventResourceType,
		Timestamp:            cfnTime(at),
		ResourceStatus:       status,
		ResourceStatusReason: reason,
	}
}

// cfnStackOpeningEvent derives the *_IN_PROGRESS status a stack's first event
// carries, and when it happened, from the status the record ended on.
//
// The terminal status names which operation ran, so this is read off the record
// rather than tracked separately. A create's opening event is at CreatedAt; any
// later operation's is at UpdatedAt, because CreatedAt belongs to the create that
// the record no longer describes. An unrecognized or empty status yields no
// opening event rather than a guessed one.
func cfnStackOpeningEvent(s CFNStackState) (status string, at time.Time) {
	switch {
	case strings.HasPrefix(s.Status, "CREATE_"), strings.HasPrefix(s.Status, "ROLLBACK_"):
		// A rollback is the tail of a create that failed, so the operation the
		// caller started was still CreateStack.
		return "CREATE_IN_PROGRESS", s.CreatedAt
	case strings.HasPrefix(s.Status, "UPDATE_"):
		return "UPDATE_IN_PROGRESS", s.UpdatedAt
	case strings.HasPrefix(s.Status, "DELETE_"):
		return "DELETE_IN_PROGRESS", s.UpdatedAt
	}
	return "", s.UpdatedAt
}

// cfnResourceEventID builds a resource event's EventId.
//
// The format is the one AWS's own DescribeStackEvents sample returns —
// "MyEC2Instance-CREATE_COMPLETE-2016-03-15T20:54:30.174Z" — which is already
// deterministic, so a replay of the same deploy reports byte-identical event IDs.
// A random UUID would satisfy the API's uniqueness requirement and break that.
func cfnResourceEventID(logicalID, status, timestamp string) string {
	return logicalID + "-" + status + "-" + timestamp
}

// cfnPaginateEvents applies a NextToken cursor to events and returns the page
// plus the token for the next one, empty when the page is the last.
//
// The token is a base64-encoded offset, following the base64-marker shape
// paginateIAMKeys established. An offset rather than an event ID because the
// derivation is a pure function of the record: the same call produces the same
// list in the same order, so position is stable, and an unparseable or
// out-of-range token starts from the beginning rather than erroring —
// DescribeStackEvents documents no service-specific errors.
func cfnPaginateEvents(events []cfnStackEvent, token string) (page []cfnStackEvent, nextToken string) {
	start := 0
	if token != "" {
		if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
			if offset, convErr := strconv.Atoi(string(decoded)); convErr == nil && offset > 0 && offset < len(events) {
				start = offset
			}
		}
	}
	end := start + cfnStackEventsPageSize
	if end < len(events) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(events)
	}
	return events[start:end], nextToken
}
