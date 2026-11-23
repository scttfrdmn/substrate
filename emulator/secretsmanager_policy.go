package emulator

// Secrets Manager resource-based policies: PutResourcePolicy, GetResourcePolicy,
// DeleteResourcePolicy and ValidateResourcePolicy (#1623).
//
// All four were unrouted until #1623, so a consumer attaching a policy got
// UnknownOperationException, and Terraform's aws_secretsmanager_secret, which calls
// GetResourcePolicy on every refresh, could not read a secret back (#1607).
//
// # Stored and validated, not enforced
//
// The policy is stored on the secret record ([SecretState.ResourcePolicy]) as the text the caller
// submitted, and GetResourcePolicy answers that text. It does **not** take part in authorization:
// GetSecretValue and every other operation is decided by the caller's identity policies alone, as it
// was before. Evaluating resource policies is a cross-service change — S3, KMS, SQS and Secrets
// Manager all store one — and is tracked as #1636. docs/services.md states the same limit.
//
// # Validation
//
// A document is refused with MalformedPolicyDocumentException ("The resource policy has syntax
// errors", 400, on PutResourcePolicy's and ValidateResourcePolicy's pages) when it is not JSON, is
// JSON but not an object (`null` included), carries no Statement or an empty one, or carries a
// Statement that does not decode as one statement or a list of them. The Statement rule is the IAM
// grammar's ("Grammar of the IAM JSON policy language"): `<statement_block>` is the one block of
// `policy` not marked optional, and it is `[ <statement>, <statement>, ... ]`, which the grammar's
// conventions say takes one or more values. Nothing stricter is checked, for the reason
// [kmsValidateKeyPolicy] gives for key policies: the set of documents AWS accepts is not published
// beyond that grammar, and a guessed refusal fails a document AWS would have stored.
//
// # Broad access
//
// PutResourcePolicy's BlockPublicPolicy "block[s] resource-based policies that allow broad access to
// the secret, for example those that use a wildcard for the principal", answering
// PublicPolicyException. ValidateResourcePolicy runs the same check and reports it as a failed
// validation rather than a refusal. AWS decides breadth with Zelkova, whose rules are not published;
// substrate decides it with the definition S3 Block Public Access publishes (see [s3PolicyIsPublic]
// and [policyStatementIsPublic]): an Allow statement is broad unless its principal names only fixed
// values, or a condition pins one of the principal- or source-narrowing global keys to fixed values
// under a positive operator — StringNotEquals, an ...IfExists form or Null narrows nothing (see
// [policyNarrowingConditionOperators]). S3's own s3:DataAccessPoint* keys do not qualify here. A Deny statement is never broad, so the
// common "deny everyone except this role" policy passes. That definition is substrate's reading for
// this service.
//
// ValidateResourcePolicy's third published check, that "the policy does not lock out a caller", is
// not modeled: it needs the evaluation #1636 adds.
//
// # Policy names
//
// "You can only attach the default policy; if you specify any other name, the operation returns an
// InvalidParameterException" — on PutResourcePolicy and DeleteResourcePolicy. GetResourcePolicy's
// page is different: "If you specify a name other than default and no policy with that name exists,
// the operation returns a ResourceNotFoundException." Both are followed as written.
//
// # Secrets pending deletion
//
// Every one of the four pages lists "The secret is scheduled for deletion." as the first cause of
// InvalidRequestException. PutResourcePolicy and DeleteResourcePolicy change the secret and refuse it
// through [smSecretScheduledForDeletion], with the operations #956 covered. GetResourcePolicy and
// ValidateResourcePolicy read and do not refuse, as DescribeSecret does not; which reads AWS refuses
// for a scheduled secret is not stated per operation, so that is substrate's reading.
//
// Error messages in this file are substrate's own except PublicPolicyException's, which is the
// page's gloss of the code.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"unicode/utf8"
)

// smDefaultPolicyName is the only resource-policy name Secrets Manager accepts.
const smDefaultPolicyName = "default"

// smMaxResourcePolicyChars is ResourcePolicy's published maximum length.
const smMaxResourcePolicyChars = 20480

// smBroadAccessCheck is the CheckName ValidateResourcePolicy reports for a policy granting broad
// access. AWS publishes the member but no value for it, so the name is substrate's own.
const smBroadAccessCheck = "BroadAccessCheck"

// smPolicyNamePattern is PolicyName's published pattern, `[0-9A-Za-z\-\_]+`, with its 1-64 length.
var smPolicyNamePattern = regexp.MustCompile(`^[0-9A-Za-z\-_]{1,64}$`)

// smInvalidPolicyParameter reports a resource-policy parameter outside its published constraint.
func smInvalidPolicyParameter(format string, args ...any) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    fmt.Sprintf(format, args...),
		HTTPStatus: http.StatusBadRequest,
	}
}

// smMalformedPolicy reports a resource policy that does not parse as a policy document.
func smMalformedPolicy(reason string) *AWSError {
	return &AWSError{
		Code:       "MalformedPolicyDocumentException",
		Message:    "the resource policy is not a valid policy document: " + reason,
		HTTPStatus: http.StatusBadRequest,
	}
}

// smPublicPolicy reports a policy BlockPublicPolicy refuses. The message is PutResourcePolicy's own
// gloss of PublicPolicyException.
func smPublicPolicy() *AWSError {
	return &AWSError{
		Code:       "PublicPolicyException",
		Message:    "The BlockPublicPolicy parameter is set to true, and the resource policy did not prevent broad access to the secret.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// smCheckPolicyName validates an optional PolicyName and reports whether it names the default
// policy. A malformed name is always InvalidParameterException; a well-formed name other than
// default is left to the caller, because the operations answer it differently.
func smCheckPolicyName(name string) (isDefault bool, err *AWSError) {
	if name == "" {
		return true, nil
	}
	if !smPolicyNamePattern.MatchString(name) {
		return false, smInvalidPolicyParameter("PolicyName %q must be 1 to 64 letters, digits, hyphens or underscores", name)
	}
	return name == smDefaultPolicyName, nil
}

// smParseResourcePolicy checks ResourcePolicy's length and syntax and returns the parsed document.
func smParseResourcePolicy(policy string) (PolicyDocument, *AWSError) {
	if policy == "" {
		return PolicyDocument{}, smInvalidPolicyParameter("ResourcePolicy is required")
	}
	if n := utf8.RuneCountInString(policy); n > smMaxResourcePolicyChars {
		return PolicyDocument{}, smInvalidPolicyParameter("ResourcePolicy is %d characters; the maximum is %d", n, smMaxResourcePolicyChars)
	}
	if !json.Valid([]byte(policy)) {
		return PolicyDocument{}, smMalformedPolicy("it is not valid JSON")
	}
	var object map[string]json.RawMessage
	// JSON null decodes into a nil map without error, so the nil check is what refuses it.
	if err := json.Unmarshal([]byte(policy), &object); err != nil || object == nil {
		return PolicyDocument{}, smMalformedPolicy("its top-level value is not a JSON object")
	}
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return PolicyDocument{}, smMalformedPolicy("its Statement is not a statement or a list of statements")
	}
	if len(doc.Statement) == 0 {
		return PolicyDocument{}, smMalformedPolicy("it has no Statement, which the IAM policy grammar requires")
	}
	return doc, nil
}

// smPolicyGrantsBroadAccess reports whether a parsed policy grants broad access; see this file's
// preamble.
func smPolicyGrantsBroadAccess(doc PolicyDocument) bool {
	for i := range doc.Statement {
		if policyStatementIsPublic(&doc.Statement[i], policyGlobalQualifyingConditionKeys) {
			return true
		}
	}
	return false
}

// smPolicyTarget loads the secret a resource-policy request names, refusing an absent one. The error
// is an [*AWSError] for a refusal and a wrapped store error otherwise.
func (p *SecretsManagerPlugin) smPolicyTarget(ctx *RequestContext, secretID string) (*SecretState, error) {
	target, idErr := smResolveSecretID(secretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	secret, err := p.loadSecret(context.Background(), target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(secretID)
	}
	return secret, nil
}

func (p *SecretsManagerPlugin) putResourcePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		BlockPublicPolicy bool   `json:"BlockPublicPolicy"`
		PolicyName        string `json:"PolicyName"`
		ResourcePolicy    string `json:"ResourcePolicy"`
		SecretID          string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, smInvalidBody()
	}
	isDefault, nameErr := smCheckPolicyName(input.PolicyName)
	if nameErr != nil {
		return nil, nameErr
	}
	if !isDefault {
		return nil, smInvalidPolicyParameter("PolicyName %q is not valid; %s is the only policy a secret can have", input.PolicyName, smDefaultPolicyName)
	}
	doc, policyErr := smParseResourcePolicy(input.ResourcePolicy)
	if policyErr != nil {
		return nil, policyErr
	}
	// The secret is resolved before the broad-access check, so an absent secret is
	// ResourceNotFoundException and a scheduled one InvalidRequestException whatever the policy
	// grants: both describe the request's target, which a caller has to fix first.
	secret, err := p.smPolicyTarget(ctx, input.SecretID)
	if err != nil {
		return nil, err
	}
	if !secret.DeletionDate.IsZero() {
		return nil, smSecretScheduledForDeletion(input.SecretID, secret.DeletionDate)
	}
	if input.BlockPublicPolicy && smPolicyGrantsBroadAccess(doc) {
		return nil, smPublicPolicy()
	}
	// "If there is already a policy attached, the command replaces it with the new policy."
	secret.ResourcePolicy = input.ResourcePolicy
	if err := p.saveSecret(context.Background(), secret); err != nil {
		return nil, fmt.Errorf("sm putResourcePolicy saveSecret: %w", err)
	}
	return smJSONResponse(http.StatusOK, map[string]any{
		"ARN": secret.ARN, "Name": secret.Name, "PolicyName": smDefaultPolicyName,
	})
}

// getResourcePolicy answers the stored policy text. A secret with no policy answers ARN, Name and
// PolicyName with no ResourcePolicy member: the request names the default policy, which is not an
// error to ask for, and a member with nothing to carry is omitted.
func (p *SecretsManagerPlugin) getResourcePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PolicyName string `json:"PolicyName"`
		SecretID   string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, smInvalidBody()
	}
	isDefault, nameErr := smCheckPolicyName(input.PolicyName)
	if nameErr != nil {
		return nil, nameErr
	}
	secret, err := p.smPolicyTarget(ctx, input.SecretID)
	if err != nil {
		return nil, err
	}
	if !isDefault {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("the secret %q has no resource policy named %q", input.SecretID, input.PolicyName),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	out := map[string]any{"ARN": secret.ARN, "Name": secret.Name, "PolicyName": smDefaultPolicyName}
	if secret.ResourcePolicy != "" {
		out["ResourcePolicy"] = secret.ResourcePolicy
	}
	return smJSONResponse(http.StatusOK, out)
}

// deleteResourcePolicy removes the stored policy. A secret with no policy succeeds: the operation's
// outcome, a secret with no policy attached, already holds, and the page publishes no code for it.
func (p *SecretsManagerPlugin) deleteResourcePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PolicyName string `json:"PolicyName"`
		SecretID   string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, smInvalidBody()
	}
	isDefault, nameErr := smCheckPolicyName(input.PolicyName)
	if nameErr != nil {
		return nil, nameErr
	}
	if !isDefault {
		return nil, smInvalidPolicyParameter("PolicyName %q is not valid; %s is the only policy a secret can have", input.PolicyName, smDefaultPolicyName)
	}
	secret, err := p.smPolicyTarget(ctx, input.SecretID)
	if err != nil {
		return nil, err
	}
	if !secret.DeletionDate.IsZero() {
		return nil, smSecretScheduledForDeletion(input.SecretID, secret.DeletionDate)
	}
	if secret.ResourcePolicy != "" {
		secret.ResourcePolicy = ""
		if err := p.saveSecret(context.Background(), secret); err != nil {
			return nil, fmt.Errorf("sm deleteResourcePolicy saveSecret: %w", err)
		}
	}
	return smJSONResponse(http.StatusOK, map[string]any{
		"ARN": secret.ARN, "Name": secret.Name, "PolicyName": smDefaultPolicyName,
	})
}

// validateResourcePolicy answers whether a policy passes the checks substrate models. SecretId is
// optional; when it is given, the secret must exist.
//
// A passing policy answers `"ValidationErrors": []`, the shape the AWS CLI reference's example output
// shows, rather than omitting the member.
func (p *SecretsManagerPlugin) validateResourcePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourcePolicy string `json:"ResourcePolicy"`
		SecretID       string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, smInvalidBody()
	}
	doc, policyErr := smParseResourcePolicy(input.ResourcePolicy)
	if policyErr != nil {
		return nil, policyErr
	}
	if input.SecretID != "" {
		if _, err := p.smPolicyTarget(ctx, input.SecretID); err != nil {
			return nil, err
		}
	}

	type validationError struct {
		CheckName    string `json:"CheckName"`
		ErrorMessage string `json:"ErrorMessage"`
	}
	errs := []validationError{}
	if smPolicyGrantsBroadAccess(doc) {
		errs = append(errs, validationError{
			CheckName:    smBroadAccessCheck,
			ErrorMessage: "the policy allows broad access to the secret: an Allow statement's principal is not limited to fixed values",
		})
	}
	return smJSONResponse(http.StatusOK, map[string]any{
		"PolicyValidationPassed": len(errs) == 0,
		"ValidationErrors":       errs,
	})
}
