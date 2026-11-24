package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// A consumer's ConsumerStatus as a seeded progression (#1613).
//
// API_RegisterStreamConsumer registers a consumer in CREATING, and a consumer "can't read data while
// in the CREATING or DELETING states", so a caller polls DescribeStreamConsumer until ACTIVE.
// Substrate registers it in the request, so the record holds ACTIVE; a seed posted to
// POST /v1/kinesis/consumer-status says how many observations of a consumer report CREATING first.
// Unseeded, a consumer is ACTIVE from registration.
//
// DescribeStreamConsumer and ListStreamConsumers observe, each spending one observation per
// consumer it reports. RegisterStreamConsumer's own response and its count of CREATING consumers
// peek. The counter is keyed by consumer ARN, which carries the registration time, so a consumer
// registered again under the same name later starts a fresh countdown.

// kinesisConsumerCtrlNamespace is the [StateManager] namespace the consumer-status seeds and their
// per-consumer observation counters live in.
const kinesisConsumerCtrlNamespace = "kinesis-consumer-status-ctrl"

// kinesisConsumerStatusSeed is the body of POST /v1/kinesis/consumer-status.
type kinesisConsumerStatusSeed struct {
	// ConsumerName targets every consumer of that name, on any stream. ConsumerARN is exact.
	ConsumerName string `json:"consumerName,omitempty"`
	// ConsumerARN targets one consumer exactly, and wins over a name-keyed seed for it.
	ConsumerARN string `json:"consumerARN,omitempty"`
	// PendingObservations is how many observations of a governed consumer report CREATING before
	// it reports ACTIVE.
	PendingObservations int `json:"pendingObservations"`
}

// progressionID returns the seed's key: the ARN when given, otherwise the name, otherwise "*".
func (s kinesisConsumerStatusSeed) progressionID() string {
	if s.ConsumerARN != "" {
		return s.ConsumerARN
	}
	return s.ConsumerName
}

// progressionObservations returns the countdown length.
func (s kinesisConsumerStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a consumer ARN that does not parse, or a name that disagrees with it.
func (s kinesisConsumerStatusSeed) validateProgression() error {
	if s.ConsumerARN == "" {
		return nil
	}
	_, name, refErr := kinesisParseConsumerARN(s.ConsumerARN)
	if refErr != nil {
		return fmt.Errorf("consumerARN %q is not a Kinesis consumer ARN", s.ConsumerARN)
	}
	if s.ConsumerName != "" && s.ConsumerName != name {
		return fmt.Errorf("consumerName %q does not match consumerARN %q", s.ConsumerName, s.ConsumerARN)
	}
	return nil
}

// kinesisConsumerProgressions is the consumer-status countdown. It is configuration, not state;
// see [progression].
var kinesisConsumerProgressions = newProgression[kinesisConsumerStatusSeed](kinesisConsumerCtrlNamespace, "consumer", "pendingObservations")

// consumerStatus returns the ConsumerStatus one read of consumer reports, spending an observation
// when observe is true. The seed is resolved ARN first, then name, then "*".
func (p *KinesisPlugin) consumerStatus(ctx context.Context, consumer KinesisConsumer, observe bool) (string, error) {
	if observe {
		p.seedMu.Lock()
		defer p.seedMu.Unlock()
	}
	arn := consumer.ConsumerARN
	seed, err := kinesisConsumerProgressions.resolve(ctx, p.state, arn)
	if err != nil {
		return "", err
	}
	if seed == nil || seed.progressionID() != arn {
		if seed, err = kinesisConsumerProgressions.resolve(ctx, p.state, consumer.ConsumerName); err != nil {
			return "", err
		}
	}
	if seed == nil {
		return consumer.ConsumerStatus, nil
	}
	seen, err := kinesisConsumerProgressions.observations(ctx, p.state, arn)
	if err != nil {
		return "", err
	}
	status, terminal := countdownState(seen, seed.PendingObservations, kinesisStatusCreating, consumer.ConsumerStatus, "", "")
	if observe && !terminal {
		if err := kinesisConsumerProgressions.advance(ctx, p.state, arn, seen); err != nil {
			return "", err
		}
	}
	return status, nil
}

// handleKinesisSeedConsumerStatus handles POST /v1/kinesis/consumer-status.
func (s *Server) handleKinesisSeedConsumerStatus(w http.ResponseWriter, r *http.Request) {
	kinesisConsumerProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleKinesisClearConsumerStatus handles DELETE /v1/kinesis/consumer-status. With ?consumer=… (a
// name or an ARN, as the seed was keyed) it removes that seed and its countdown; without it, every
// seed and counter.
func (s *Server) handleKinesisClearConsumerStatus(w http.ResponseWriter, r *http.Request) {
	kinesisConsumerProgressions.serveClear(w, r, s.state, s.logger)
}
