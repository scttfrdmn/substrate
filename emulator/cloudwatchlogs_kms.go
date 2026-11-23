package emulator

import (
	"context"
	"encoding/json"
	"net/http"
)

// A log group's KMS key (#1519).
//
// CreateLogGroup takes `kmsKeyId`, and DescribeLogGroups reports it back on the LogGroup. Substrate
// decoded neither, so a log group Terraform created with `kms_key_id` read back without one, and
// the plan after every apply showed the key being added again. AssociateKmsKey and
// DisassociateKmsKey are how the provider changes the key in place; both were unrouted.
//
// The key is recorded intent: substrate encrypts nothing, so it does not resolve the key against
// KMS, and a group's events read back the same whichever key it names. Each operation's newer
// `resourceIdentifier` form (a log group ARN, or a query-results scope) is not modeled; a request
// naming no logGroupName is refused as one missing it is.

// associateKmsKey records kmsKeyId on a log group, replacing any key it had.
func (p *CloudWatchLogsPlugin) associateKmsKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName string `json:"logGroupName"`
		KmsKeyID     string `json:"kmsKeyId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.KmsKeyID == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "kmsKeyId is required", HTTPStatus: http.StatusBadRequest}
	}
	return p.setLogGroupKmsKey(ctx, body.LogGroupName, body.KmsKeyID, "associateKmsKey")
}

// disassociateKmsKey clears a log group's KMS key.
func (p *CloudWatchLogsPlugin) disassociateKmsKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName string `json:"logGroupName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	return p.setLogGroupKmsKey(ctx, body.LogGroupName, "", "disassociateKmsKey")
}

// setLogGroupKmsKey loads a group, sets its key (empty clears it) and stores it. An absent group
// is the ResourceNotFoundException [CloudWatchLogsPlugin.loadLogGroup] answers.
func (p *CloudWatchLogsPlugin) setLogGroupKmsKey(ctx *RequestContext, name, keyID, op string) (*AWSResponse, error) {
	goCtx := context.Background()
	stateKey := cwLogGroupKey(ctx.AccountID, ctx.Region, name)
	lg, err := p.loadLogGroup(goCtx, stateKey, name, op)
	if err != nil {
		return nil, err
	}
	lg.KmsKeyID = keyID
	if putErr := p.storeLogGroup(goCtx, stateKey, lg, op); putErr != nil {
		return nil, putErr
	}
	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}
