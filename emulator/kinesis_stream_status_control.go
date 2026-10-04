package emulator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// A stream's StreamStatus as a seeded progression (#1119).
//
// API_CreateStream: "Upon receiving a CreateStream request, Kinesis Data Streams immediately returns
// and sets the stream status to CREATING. After the stream is created, Kinesis Data Streams sets the
// stream status to ACTIVE." API_UpdateShardCount, API_MergeShards and API_SplitShard say the same of
// UPDATING. Substrate completes each of those in the request that starts it, so a stream read ACTIVE
// from birth and after every reshard, and a consumer's wait-for-ACTIVE loop — the loop this part of
// the API exists to require — exited on its first poll without testing anything.
//
// The transition is now a [progression] (emulator/progression.go), counted in observations and
// seeded through POST /v1/kinesis/stream-status. A seed says how many observations of a stream
// report its transient status before it reports ACTIVE. The default is zero observations, so an
// unseeded stream reads exactly as it did.
//
// # Which transient status
//
// The seed does not name the status; the stream's most recent transition does, as the published
// operations define it: CREATING after CreateStream, UPDATING after UpdateShardCount, MergeShards or
// SplitShard. The record carries it as TransitionStatus, written only by a reshard. A stream whose
// field is empty has had no reshard, so its most recent transition is its creation and it reports
// CREATING. That keeps a CreateStream's stored record byte-identical to the record before #1119.
//
// # What observes and what peeks
//
// DescribeStream and DescribeStreamSummary are the reads the pages tell a caller to poll, so each
// spends one observation. UpdateShardCount, MergeShards and SplitShard each check the status first
// — every one of their pages publishes ResourceInUseException for a stream that is not ACTIVE, and
// MergeShards' says so of CREATING, UPDATING and DELETING by name — and that check peeks, spending
// nothing, so a refused reshard does not use up the polls a test seeded.
//
// DELETING is not modeled: DeleteStream removes the record in the request, so a stream being
// deleted answers ResourceNotFoundException at once, which is what a delete that has completed
// answers. Observing a DELETING window would need a record that outlives its delete.

// kinesisStreamCtrlNamespace is the [StateManager] namespace the stream-status seeds and their
// per-stream observation counters live in.
const kinesisStreamCtrlNamespace = "kinesis-stream-status-ctrl"

// The two transient statuses API_StreamDescription publishes for a transition substrate models.
const (
	kinesisStatusCreating = "CREATING"
	kinesisStatusUpdating = "UPDATING"
	kinesisStatusActive   = "ACTIVE"
)

// kinesisStreamStatusSeed is the body of POST /v1/kinesis/stream-status.
type kinesisStreamStatusSeed struct {
	// StreamName targets one stream by name. A name is unique only within an account and Region, so
	// a name-keyed seed governs that name in every account and Region; StreamARN is exact.
	StreamName string `json:"streamName,omitempty"`
	// StreamARN targets one stream exactly, and wins over a name-keyed seed for that stream.
	StreamARN string `json:"streamARN,omitempty"`
	// PendingObservations is how many observations of a governed stream report its transient status
	// — CREATING or UPDATING, whichever its most recent transition was — before it reports ACTIVE.
	PendingObservations int `json:"pendingObservations"`
}

// progressionID returns the seed's key: the ARN when given, otherwise the name, otherwise "*".
func (s kinesisStreamStatusSeed) progressionID() string {
	if s.StreamARN != "" {
		return s.StreamARN
	}
	return s.StreamName
}

// progressionObservations returns the countdown length.
func (s kinesisStreamStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a seed naming a stream two inconsistent ways, or an ARN that is not a
// Kinesis stream ARN. The statuses are not the seed's to name (see the file comment), so there is
// no enumeration to check.
func (s kinesisStreamStatusSeed) validateProgression() error {
	if s.StreamARN == "" {
		return nil
	}
	_, name, found := strings.Cut(s.StreamARN, ":stream/")
	if !strings.HasPrefix(s.StreamARN, "arn:") || !strings.Contains(s.StreamARN, ":kinesis:") || !found || name == "" {
		return fmt.Errorf("streamARN %q is not a Kinesis stream ARN", s.StreamARN)
	}
	if s.StreamName != "" && s.StreamName != name {
		return fmt.Errorf("streamName %q does not match streamARN %q", s.StreamName, s.StreamARN)
	}
	return nil
}

// kinesisStreamProgressions is the stream-status countdown. It is configuration, not state; see
// [progression].
var kinesisStreamProgressions = newProgression[kinesisStreamStatusSeed](kinesisStreamCtrlNamespace, "stream", "pendingObservations")

// kinesisTransientStatus is the status a stream's most recent transition reports while a seed holds
// it: UPDATING after a reshard, CREATING otherwise.
func kinesisTransientStatus(stream KinesisStream) string {
	if stream.TransitionStatus != "" {
		return stream.TransitionStatus
	}
	return kinesisStatusCreating
}

// resolveStreamSeed returns the seed governing one stream: its ARN first, then its name, then "*".
// The helper's resolve tries one ID and then the wildcard, so the ARN lookup is accepted only when
// the seed it finds is the ARN's own.
func (p *KinesisPlugin) resolveStreamSeed(ctx context.Context, arn, name string) (*kinesisStreamStatusSeed, error) {
	seed, err := kinesisStreamProgressions.resolve(ctx, p.state, arn)
	if err != nil {
		return nil, err
	}
	if seed != nil && seed.progressionID() == arn {
		return seed, nil
	}
	return kinesisStreamProgressions.resolve(ctx, p.state, name)
}

// streamStatus returns the StreamStatus one read of stream reports. When observe is true the read
// spends an observation (DescribeStream, DescribeStreamSummary); otherwise it peeks (a reshard's
// precondition). An unseeded stream reports its record's own status.
//
// The counter is keyed by the stream's ARN, which is unique, even when the governing seed is keyed
// by name or "*" — so one seed spent by two streams counts each separately (#582).
func (p *KinesisPlugin) streamStatus(ctx context.Context, stream KinesisStream, observe bool) (string, error) {
	arn := stream.StreamArn
	if arn == "" {
		return "", errKinesisNoStreamARN
	}
	if observe {
		p.seedMu.Lock()
		defer p.seedMu.Unlock()
	}
	seed, err := p.resolveStreamSeed(ctx, arn, stream.StreamName)
	if err != nil || seed == nil {
		return stream.StreamStatus, err
	}
	seen, err := kinesisStreamProgressions.observations(ctx, p.state, arn)
	if err != nil {
		return "", err
	}
	status, terminal := countdownState(seen, seed.PendingObservations, kinesisTransientStatus(stream), stream.StreamStatus, "", "")
	if observe && !terminal {
		if err := kinesisStreamProgressions.advance(ctx, p.state, arn, seen); err != nil {
			return "", err
		}
	}
	return status, nil
}

// requireStreamActive refuses a reshard of a stream whose status is not ACTIVE, with the
// ResourceInUseException/400 API_UpdateShardCount, API_MergeShards and API_SplitShard each publish.
// It peeks, so a refused reshard spends no observation. The message is substrate's wording; the
// pages publish the code and its gloss, not a message.
func (p *KinesisPlugin) requireStreamActive(ctx context.Context, stream KinesisStream, target kinesisStreamTarget) error {
	status, err := p.streamStatus(ctx, stream, false)
	if err != nil {
		return err
	}
	if status != kinesisStatusActive {
		return &AWSError{
			Code: "ResourceInUseException",
			Message: fmt.Sprintf("Stream %s under account %s not ACTIVE, instead in state %s.",
				target.Name, target.AccountID, status),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// startStreamTransition records that a reshard has begun one: the record reports UPDATING while a
// seed holds it, and the stream's countdown restarts, so one seed covers every reshard in a test.
func (p *KinesisPlugin) startStreamTransition(ctx context.Context, stream *KinesisStream) error {
	stream.TransitionStatus = kinesisStatusUpdating
	return kinesisStreamProgressions.reset(ctx, p.state, stream.StreamArn)
}

// handleKinesisSeedStreamStatus handles POST /v1/kinesis/stream-status.
func (s *Server) handleKinesisSeedStreamStatus(w http.ResponseWriter, r *http.Request) {
	kinesisStreamProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleKinesisClearStreamStatus handles DELETE /v1/kinesis/stream-status. With ?stream=… (a name
// or an ARN, as the seed was keyed) it removes that seed and its countdown; without it, every seed
// and counter.
func (s *Server) handleKinesisClearStreamStatus(w http.ResponseWriter, r *http.Request) {
	kinesisStreamProgressions.serveClear(w, r, s.state, s.logger)
}

// errKinesisNoStreamARN guards [KinesisPlugin.streamStatus] against a record with no ARN, which
// createStream never writes; it exists so a malformed record is an error rather than a counter keyed
// by "".
var errKinesisNoStreamARN = errors.New("kinesis stream record carries no StreamArn")
