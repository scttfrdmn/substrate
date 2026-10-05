package emulator

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// A seedable IAM role propagation window for Lambda (#1274, item 5).
//
// IAM is eventually consistent: a role is not assumable by Lambda for a short while after
// CreateRole returns, and a CreateFunction naming it in that window is refused with
// InvalidParameterValueException and the sentence "The role defined for the function cannot be
// assumed by Lambda." A deployer that creates its own roles meets this on nearly every first
// deploy, and has to retry that error while failing fast on the others. That behavior, its code
// and its sentence are observed on a real account (#1274); no API page publishes the sentence,
// though API_CreateFunction publishes InvalidParameterValueException at 400.
//
// Substrate's roles are assumable the moment they exist, so the retry is never exercised. The
// window is now a [progression] (emulator/progression.go), counted in attempts and seeded through
// POST /v1/lambda/role-propagation. A seed names a role ARN, or none for every role ("*"), and
// says how many attempts naming that role are refused before it propagates. The default is zero
// attempts, so an unseeded role is assumable at once, as before.
//
// # What counts as an attempt
//
// A CreateFunction naming the role, and an UpdateFunctionConfiguration that names it as the new
// Role. Each spends one observation of that role's own counter, refused or not, and the refusal is
// answered while the attempt's index is below the seed's count. So refusedAttempts 2 refuses the
// first two attempts and admits the third. Attempts are counted per role, so one wildcard seed
// gives every role its own window (#582's per-resource rule).
//
// The window counts from the seed rather than from CreateRole. Substrate's IAM and Lambda plugins
// do not share a clock of role creation, and counting from the seed is what a test controls: post
// the seed, create the role, retry. A seed's POST restarts the counters it governs.
//
// # What is checked first
//
// The refusal comes after the body parses and the function name is checked, and after
// CreateFunction's own conflict check, so a request refused for another reason spends no attempt.

// lambdaRolePropagationNamespace is the [StateManager] namespace the role-propagation seeds and their
// per-role attempt counters live in.
const lambdaRolePropagationNamespace = "lambda-role-propagation-ctrl"

// lambdaRoleNotAssumableMessage is the sentence a real account answers for a role Lambda cannot yet
// assume, quoted from #1274.
const lambdaRoleNotAssumableMessage = "The role defined for the function cannot be assumed by Lambda."

// lambdaRolePropagationSeed is the body of POST /v1/lambda/role-propagation.
type lambdaRolePropagationSeed struct {
	// RoleArn targets one role. Empty (or "*") governs every role.
	RoleArn string `json:"roleArn,omitempty"`
	// RefusedAttempts is how many attempts naming a governed role are refused before it is
	// assumable.
	RefusedAttempts int `json:"refusedAttempts"`
}

// progressionID returns the seed's key: the role ARN, or "" for every role.
func (s lambdaRolePropagationSeed) progressionID() string { return s.RoleArn }

// progressionObservations returns the number of refused attempts.
func (s lambdaRolePropagationSeed) progressionObservations() int { return s.RefusedAttempts }

// validateProgression refuses a RoleArn that is not an IAM role ARN. There are no states to check:
// the window has one transient answer, the refusal, and one settled one, admission.
func (s lambdaRolePropagationSeed) validateProgression() error {
	if s.RoleArn == "" || s.RoleArn == "*" {
		return nil
	}
	if !strings.HasPrefix(s.RoleArn, "arn:") || !strings.Contains(s.RoleArn, ":iam::") || !strings.Contains(s.RoleArn, ":role/") {
		return fmt.Errorf("roleArn %q is not an IAM role ARN", s.RoleArn)
	}
	return nil
}

// lambdaRolePropagations is the role-propagation countdown. It is configuration, not state; see
// [progression].
var lambdaRolePropagations = newProgression[lambdaRolePropagationSeed](lambdaRolePropagationNamespace, "roleArn", "refusedAttempts")

// checkRolePropagated spends one attempt of role's propagation window and refuses the attempt while
// the window is open. An empty role names nothing to assume, so it spends nothing.
func (p *LambdaPlugin) checkRolePropagated(ctx context.Context, role string) error {
	if role == "" {
		return nil
	}
	seed, seen, err := lambdaRolePropagations.observe(ctx, p.state, &p.seedMu, role)
	if err != nil {
		return fmt.Errorf("lambda role propagation: %w", err)
	}
	if seed != nil && seen < seed.RefusedAttempts {
		return &AWSError{Code: "InvalidParameterValueException", Message: lambdaRoleNotAssumableMessage, HTTPStatus: http.StatusBadRequest}
	}
	return nil
}

// handleLambdaSeedRolePropagation handles POST /v1/lambda/role-propagation.
func (s *Server) handleLambdaSeedRolePropagation(w http.ResponseWriter, r *http.Request) {
	lambdaRolePropagations.serveSeed(w, r, s.state, s.logger)
}

// handleLambdaClearRolePropagation handles DELETE /v1/lambda/role-propagation. With ?roleArn=… it
// removes that seed and its counter; without it, every seed and counter.
func (s *Server) handleLambdaClearRolePropagation(w http.ResponseWriter, r *http.Request) {
	lambdaRolePropagations.serveClear(w, r, s.state, s.logger)
}
