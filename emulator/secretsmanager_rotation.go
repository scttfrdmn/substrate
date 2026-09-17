package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// RotateSecret's configuration, its response and its refusal — #952.
//
// Substrate read exactly one of RotateSecret's seven request parameters, omitted a response member AWS
// publishes and all four of its samples return, and refused nothing. The result was worse than a thin
// response: the handler set RotationEnabled = true unconditionally, so DescribeSecret reported rotation
// configured for a secret that had no rotation function — the state AWS refuses to create. A consumer's
// nominal path was substrate's nominal path and its error path was unreachable.
//
// Three things follow from AWS's own text, and the boundary CLAUDE.md draws decides which of them
// substrate models:
//
//   - **The response's VersionId is derivable, not minted.** All four samples say the same sentence:
//     "The ClientRequestToken field becomes the VersionId of the new version created during the
//     rotation." So the value comes from the request, which is what makes it reproducible on replay
//     rather than a second entry in #856's inventory. Substrate echoes it.
//   - **The configuration is recorded intent.** RotationLambdaARN and RotationRules are stored on the
//     secret and reported by DescribeSecret. Nothing invokes the rotation function — running a Lambda is
//     the workload-internal half that is out of scope — but which function and which schedule a caller
//     configured is observable through an API call, so it belongs here.
//   - **Two of the three published InvalidRequestException causes are decidable and are refused.** The
//     first, "the secret is scheduled for deletion", became decidable when #953 modeled the recovery
//     window. The second, "you tried to enable rotation on a secret that doesn't already have a Lambda
//     function ARN configured and you didn't include such an ARN as a parameter in this call", is
//     decidable from the record and the request. The third, "the secret is managed by another service",
//     needs OwningService, which substrate models nothing of, so it is not refused — recorded here
//     rather than left as a silent gap.
//
// Deliberately not enforced, and recorded rather than skipped: the published length constraints on
// ClientRequestToken (32 to 64), ExternalSecretRotationRoleArn (20 to 2048) and RotationLambdaARN (0 to
// 2048), and the patterns on Duration ([0-9]+h) and ScheduleExpression. Substrate does not validate
// parameter lengths or patterns as a rule, and adding it at this one operation would answer
// InvalidParameterException for a token a consumer's test chose while every sibling operation accepted
// it. #952 asked for none of it; the class is worth its own issue rather than one operation's exception.
//
// ExternalSecretRotationRoleArn and ExternalSecretRotationMetadata are decoded by nothing here. They
// belong to the managed-external-secret integration substrate models no part of — there is no Type
// member, no OwningService and no partner — so recording them would put values in state that no
// operation could ever report.

// smRotationNoFunction reports RotateSecret's second published InvalidRequestException cause.
//
// The message names which cause applies because one code covers three unrelated conditions, following
// [smSecretScheduledForDeletion]. It is the refusal that makes substrate's old nominal path — enabling
// rotation on a secret with no rotation function — reachable as the error AWS answers.
func smRotationNoFunction(id string) *AWSError {
	return &AWSError{
		Code: "InvalidRequestException",
		Message: fmt.Sprintf(
			"the secret %q has no rotation function configured and this call supplied no RotationLambdaARN",
			id),
		HTTPStatus: http.StatusBadRequest,
	}
}

// smRotationRulesInput is RotateSecret's RotationRules parameter, decoded.
//
// Separate from [SMRotationRules] only in that AutomaticallyAfterDays is a pointer, so a request that
// sends 0 — outside AWS's published range of 1 to 1000 — is refused rather than read as an omission.
type smRotationRulesInput struct {
	AutomaticallyAfterDays *int64 `json:"AutomaticallyAfterDays"`
	Duration               string `json:"Duration"`
	ScheduleExpression     string `json:"ScheduleExpression"`
}

const (
	// smMinAutomaticallyAfterDays is the smallest rotation interval RotationRules accepts, from
	// AutomaticallyAfterDays' own "Valid Range: Minimum value of 1. Maximum value of 1000".
	smMinAutomaticallyAfterDays = 1

	// smMaxAutomaticallyAfterDays is the largest, from the same sentence.
	smMaxAutomaticallyAfterDays = 1000
)

// smValidateRotationRules turns a decoded RotationRules into the shape stored on the secret, refusing
// the two conditions AWS states for it.
//
// The mutual exclusion is AWS's, stated in AutomaticallyAfterDays' own description: "in RotateSecret,
// you can set the rotation schedule in RotationRules with AutomaticallyAfterDays or ScheduleExpression,
// but not both." The *code* is substrate's reading — AWS publishes no code for the violation, and
// InvalidParameterException ("the parameter name or value is invalid") is the one it publishes on this
// operation that fits. Refusing on presence follows #953's DeleteSecret precedent for the same reason:
// the alternative silently picks one of two schedules the caller asked for.
func smValidateRotationRules(in *smRotationRulesInput) (*SMRotationRules, *AWSError) {
	if in == nil {
		return nil, nil
	}
	if in.AutomaticallyAfterDays != nil && in.ScheduleExpression != "" {
		return nil, &AWSError{
			Code:       "InvalidParameterException",
			Message:    "RotationRules accepts AutomaticallyAfterDays or ScheduleExpression, but not both",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	rules := &SMRotationRules{Duration: in.Duration, ScheduleExpression: in.ScheduleExpression}
	if in.AutomaticallyAfterDays != nil {
		days := *in.AutomaticallyAfterDays
		if days < smMinAutomaticallyAfterDays || days > smMaxAutomaticallyAfterDays {
			return nil, &AWSError{
				Code: "InvalidParameterException",
				Message: fmt.Sprintf("AutomaticallyAfterDays must be from %d to %d, not %d",
					smMinAutomaticallyAfterDays, smMaxAutomaticallyAfterDays, days),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		rules.AutomaticallyAfterDays = days
	}
	return rules, nil
}

func (p *SecretsManagerPlugin) rotateSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// RotateImmediately is a pointer so its documented default of true is applied on absence rather
	// than inherited from Go's zero value, which is the opposite of AWS's.
	var input struct {
		SecretID           string                `json:"SecretId"`
		ClientRequestToken string                `json:"ClientRequestToken"`
		RotationLambdaARN  string                `json:"RotationLambdaARN"`
		RotationRules      *smRotationRulesInput `json:"RotationRules"`
		RotateImmediately  *bool                 `json:"RotateImmediately"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	// **Substrate's reading**, and the one decision here AWS publishes no code for. ClientRequestToken
	// is "Required: No" for an SDK caller because "the CLI or SDK generates a random UUID for you", but
	// the page is explicit about the caller substrate actually serves: "if you generate a raw HTTP
	// request to the Secrets Manager service endpoint, then you must generate a ClientRequestToken and
	// include it in the request." Substrate *is* that endpoint. The two alternatives are worse: minting
	// one puts a nondeterministic value in a response body, which is #856's whole defect class, and
	// echoing an empty one sends a VersionId of "" where AWS publishes a minimum length of 32 — a value
	// AWS would never answer.
	if input.ClientRequestToken == "" {
		return nil, &AWSError{
			Code: "InvalidParameterException",
			Message: "ClientRequestToken is required: a raw HTTP request to the Secrets Manager endpoint " +
				"must generate one, and it becomes the VersionId of the new version",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	rules, rulesErr := smValidateRotationRules(input.RotationRules)
	if rulesErr != nil {
		return nil, rulesErr
	}

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	// The causes are checked in the order AWS lists them, so a secret that is both scheduled for
	// deletion and unconfigured reports the condition a caller has to resolve first.
	if !secret.DeletionDate.IsZero() {
		return nil, smSecretScheduledForDeletion(input.SecretID, secret.DeletionDate)
	}

	// "If you include the configuration parameters, the operation sets the values for the secret and then
	// immediately starts a rotation. If you don't include the configuration parameters, the operation
	// starts a rotation with the values already stored in the secret." So the function this rotation
	// would run is the request's if it named one and the record's otherwise — and if neither has one
	// there is nothing to rotate with, which is exactly the cause. Note RotationLambdaARN's published
	// minimum length of 0: an explicit "" is not naming an ARN.
	//
	// A secret using AWS's *managed rotation* legitimately has no ARN — "for secrets that use managed
	// rotation, omit this field" — but managed rotation is an OwningService secret, and substrate models
	// no OwningService, so no secret here can be one. That is why the refusal is unconditional.
	lambdaARN := input.RotationLambdaARN
	if lambdaARN == "" {
		lambdaARN = secret.RotationLambdaARN
	}
	if lambdaARN == "" {
		return nil, smRotationNoFunction(input.SecretID)
	}

	// Everything above returns before this point, so a refused rotation writes nothing — the half of the
	// defect that a status-only assertion misses, since the old handler saved RotationEnabled = true on
	// the way to its 200.
	secret.RotationEnabled = true
	secret.RotationLambdaARN = lambdaARN
	if rules != nil {
		secret.RotationRules = rules
	}
	secret.RotateImmediately = input.RotateImmediately == nil || *input.RotateImmediately
	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm rotateSecret saveSecret: %w", err)
	}

	// "The ClientRequestToken field becomes the VersionId of the new version created during the
	// rotation." No version *payload* is written for it: the rotation function is what would produce the
	// new value, and substrate does not run one, so writing a version would be inventing a secret value.
	// What the caller gets is the handle AWS gives them, traceable to the token they sent.
	return smJSONResponse(http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": input.ClientRequestToken,
	})
}
