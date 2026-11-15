package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ServiceQuotasPlugin emulates the AWS Service Quotas API.
// It handles ListServices, ListServiceQuotas, ListAWSDefaultServiceQuotas,
// GetServiceQuota, GetAWSDefaultServiceQuota, RequestServiceQuotaIncrease,
// ListRequestedServiceQuotaChangeHistory, and GetRequestedServiceQuotaChange.
// The built-in quota table provides representative default values; increases
// are stored in state as PENDING requests. The Service Quotas API is free.
type ServiceQuotasPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "servicequotas".
func (p *ServiceQuotasPlugin) Name() string { return servicequotasNamespace }

// Initialize sets up the ServiceQuotasPlugin with the provided configuration.
func (p *ServiceQuotasPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ServiceQuotasPlugin.
func (p *ServiceQuotasPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Service Quotas JSON-protocol request.
//
// The three quota-increase operations take the caller's account, because a quota
// increase belongs to the account that filed it. The quota reads take the caller's
// context only to mint each quota's QuotaArn, which names the caller's region and, for
// an applied value, account: the quota table itself is per-service and identical for
// every caller, since substrate models no per-account value and nothing can set one.
func (p *ServiceQuotasPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "ListServices":
		return p.listServices(req)
	case "ListServiceQuotas":
		return p.listServiceQuotas(reqCtx, req)
	case "ListAWSDefaultServiceQuotas":
		return p.listAWSDefaultServiceQuotas(reqCtx, req)
	case "GetServiceQuota":
		return p.getServiceQuota(reqCtx, req)
	case "GetAWSDefaultServiceQuota":
		return p.getAWSDefaultServiceQuota(reqCtx, req)
	case "RequestServiceQuotaIncrease":
		return p.requestServiceQuotaIncrease(reqCtx.IDs, sqAccountID(reqCtx), req)
	// The second name is not one AWS has; it is the name this handler shipped
	// under, kept as an alias so a fixture that drives the plugin by a hand-built
	// X-Amz-Target keeps working. See the handler's doc comment (#636).
	case "ListRequestedServiceQuotaChangeHistory", "ListRequestedServiceQuotaChangesByService":
		return p.listRequestedServiceQuotaChangeHistory(sqAccountID(reqCtx), req)
	case "GetRequestedServiceQuotaChange":
		return p.getRequestedServiceQuotaChange(sqAccountID(reqCtx), req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// sqAccountID returns the account a quota-increase record belongs to.
//
// A nil context, or one carrying no account, falls back to the account substrate
// attributes an unconfigured request to (#624). That is not a guess about the
// caller: it is what every request off the wire resolves to, so a plugin driven
// directly by a unit test files its increase where an HTTP caller would rather
// than under "".
func sqAccountID(reqCtx *RequestContext) string {
	if reqCtx == nil || reqCtx.AccountID == "" {
		return defaultAccountID
	}
	return reqCtx.AccountID
}

// --- State helpers -----------------------------------------------------------

func (p *ServiceQuotasPlugin) increaseKey(accountID, id string) string {
	return "quota_increase:" + accountID + "/" + id
}

func (p *ServiceQuotasPlugin) increaseIDsKey(accountID string) string {
	return "quota_increase_ids:" + accountID
}

func (p *ServiceQuotasPlugin) loadIncreaseIDs(ctx context.Context, accountID string) ([]string, error) {
	data, err := p.state.Get(ctx, servicequotasNamespace, p.increaseIDsKey(accountID))
	if err != nil {
		return nil, fmt.Errorf("servicequotas loadIncreaseIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("servicequotas loadIncreaseIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *ServiceQuotasPlugin) saveIncreaseIDs(ctx context.Context, accountID string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("servicequotas saveIncreaseIDs marshal: %w", err)
	}
	return p.state.Put(ctx, servicequotasNamespace, p.increaseIDsKey(accountID), data)
}

func (p *ServiceQuotasPlugin) loadIncrease(ctx context.Context, accountID, id string) (*QuotaIncrease, error) {
	data, err := p.state.Get(ctx, servicequotasNamespace, p.increaseKey(accountID, id))
	if err != nil {
		return nil, fmt.Errorf("servicequotas loadIncrease: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var qi QuotaIncrease
	if err := json.Unmarshal(data, &qi); err != nil {
		return nil, fmt.Errorf("servicequotas loadIncrease unmarshal: %w", err)
	}
	return &qi, nil
}

func (p *ServiceQuotasPlugin) saveIncrease(ctx context.Context, accountID string, qi *QuotaIncrease) error {
	data, err := json.Marshal(qi)
	if err != nil {
		return fmt.Errorf("servicequotas saveIncrease marshal: %w", err)
	}
	return p.state.Put(ctx, servicequotasNamespace, p.increaseKey(accountID, qi.ID), data)
}

// --- Operations --------------------------------------------------------------

// listServices answers ListServices, one page at a time.
func (p *ServiceQuotasPlugin) listServices(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults *int   `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if err := sqUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	pageSize, offset, awsErr := sqPageInput(input.MaxResults, input.NextToken)
	if awsErr != nil {
		return nil, awsErr
	}
	// defaultServiceList is a fixed slice, so its order is stable across pages.
	page, next := pageByOffsetToken(defaultServiceList, offset, pageSize)
	return sqJSONResponse(http.StatusOK, sqWithNextToken(map[string]interface{}{"Services": page}, next))
}

// sqListInput is the request shape ListServiceQuotas and ListAWSDefaultServiceQuotas share.
// QuotaCode and QuotaAppliedAtLevel are ListServiceQuotas members only; the default listing
// does not model them, so it never reads them.
type sqListInput struct {
	ServiceCode         string `json:"ServiceCode"`
	QuotaCode           string `json:"QuotaCode"`
	QuotaAppliedAtLevel string `json:"QuotaAppliedAtLevel"`
	MaxResults          *int   `json:"MaxResults"`
	NextToken           string `json:"NextToken"`
}

// listServiceQuotas answers ListServiceQuotas: the applied values of a service's quotas.
//
// QuotaCode narrows the listing to one quota, and a code the service does not publish
// narrows it to none, which is a filter answering with nothing rather than a refusal.
// QuotaAppliedAtLevel is validated against its three values. Every quota substrate
// publishes is applied at the account level, so ACCOUNT (the default) and ALL list them all
// and RESOURCE lists none.
func (p *ServiceQuotasPlugin) listServiceQuotas(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.listQuotas(reqCtx, req, false)
}

// listAWSDefaultServiceQuotas answers ListAWSDefaultServiceQuotas: "the default values for
// the quotas for the specified AWS service. A default value does not reflect any quota
// increases." Its pagination is ListServiceQuotas'.
func (p *ServiceQuotasPlugin) listAWSDefaultServiceQuotas(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.listQuotas(reqCtx, req, true)
}

// listQuotas is the body of both quota listings; awsDefault selects which value is read.
func (p *ServiceQuotasPlugin) listQuotas(reqCtx *RequestContext, req *AWSRequest, awsDefault bool) (*AWSResponse, error) {
	var input sqListInput
	if err := sqUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.ServiceCode == "" {
		return nil, sqIllegalArgument("ServiceCode is required")
	}
	pageSize, offset, awsErr := sqPageInput(input.MaxResults, input.NextToken)
	if awsErr != nil {
		return nil, awsErr
	}

	// An unrecognized service is NoSuchResourceException, which the model declares
	// for this operation. Answering 200 with an empty list instead claims the
	// service exists and has no quotas — a different, and false, statement.
	quotas, ok := defaultServiceQuotas[input.ServiceCode]
	if !ok {
		return nil, sqNoSuchService(input.ServiceCode)
	}

	selected := quotas
	if !awsDefault {
		switch input.QuotaAppliedAtLevel {
		case "", "ACCOUNT", "ALL":
		case "RESOURCE":
			selected = nil
		default:
			return nil, sqIllegalArgument(fmt.Sprintf(
				"QuotaAppliedAtLevel %q is not one of ACCOUNT, RESOURCE or ALL", input.QuotaAppliedAtLevel))
		}
		if input.QuotaCode != "" {
			var one []ServiceQuota
			for _, q := range selected {
				if q.QuotaCode == input.QuotaCode {
					one = append(one, q)
				}
			}
			selected = one
		}
	}

	// The table rows are a fixed slice, so the offset a token carries names the same row on
	// every call.
	page, next := pageByOffsetToken(selected, offset, pageSize)
	accountID, region := sqCaller(reqCtx)
	rendered := make([]ServiceQuota, 0, len(page))
	for _, q := range page {
		rendered = append(rendered, sqRenderQuota(q, accountID, region, awsDefault))
	}
	return sqJSONResponse(http.StatusOK, sqWithNextToken(map[string]interface{}{"Quotas": rendered}, next))
}

// getServiceQuota answers GetServiceQuota: one quota's applied value.
func (p *ServiceQuotasPlugin) getServiceQuota(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.getQuota(reqCtx, req, false)
}

// getAWSDefaultServiceQuota answers GetAWSDefaultServiceQuota: "the default value for the
// specified quota. The default value does not reflect any quota increases."
//
// For nearly every quota the two are the same number, because nothing in substrate raises a
// quota. The exception is L-1216C47A, whose applied value substrate has long published as 32
// while AWS's new-account default is 5; see [ec2StandardOnDemandAppliedValue].
func (p *ServiceQuotasPlugin) getAWSDefaultServiceQuota(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.getQuota(reqCtx, req, true)
}

// getQuota is the body of both single-quota reads; awsDefault selects which value is read.
func (p *ServiceQuotasPlugin) getQuota(reqCtx *RequestContext, req *AWSRequest, awsDefault bool) (*AWSResponse, error) {
	var input struct {
		ServiceCode string `json:"ServiceCode"`
		QuotaCode   string `json:"QuotaCode"`
	}
	if err := sqUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.ServiceCode == "" || input.QuotaCode == "" {
		return nil, sqIllegalArgument("ServiceCode and QuotaCode are required")
	}

	quota, awsErr := p.findQuota(input.ServiceCode, input.QuotaCode)
	if awsErr != nil {
		return nil, awsErr
	}
	accountID, region := sqCaller(reqCtx)
	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"Quota": sqRenderQuota(*quota, accountID, region, awsDefault),
	})
}

// sqRenderQuota returns q as the wire carries it to the caller.
//
// The applied value's ARN names the caller's account and the default value's names none —
// "arn:aws:servicequotas:us-east-2::ec2/L-1216C47A" is what the Service Quotas CLI
// reference's get-aws-default-service-quota example shows, against
// "arn:aws:servicequotas:us-east-2:123456789012:ec2/L-1216C47A" for get-service-quota. A
// default belongs to no account; that is what makes it a default. The format is the quota
// resource type's, arn:${Partition}:servicequotas:${Region}:${Account}:${ServiceCode}/${QuotaCode}.
func sqRenderQuota(q ServiceQuota, accountID, region string, awsDefault bool) ServiceQuota {
	out := q
	if awsDefault {
		if q.defaultValue != nil {
			out.Value = *q.defaultValue
		}
		accountID = ""
	}
	out.QuotaArn = "arn:" + cfnPartition(region) + ":servicequotas:" + region + ":" + accountID + ":" +
		q.ServiceCode + "/" + q.QuotaCode
	return out
}

// sqCaller returns the account and region a quota's ARN is minted under. A nil or empty
// context falls back to the account and region an unconfigured request resolves to, as
// [sqAccountID] does.
func sqCaller(reqCtx *RequestContext) (accountID, region string) {
	accountID, region = sqAccountID(reqCtx), defaultRegion
	if reqCtx != nil && reqCtx.Region != "" {
		region = reqCtx.Region
	}
	return accountID, region
}

// sqMaxResultsLimit is the upper bound the reference gives every paginated Service Quotas
// listing: "Valid Range: Minimum value of 1. Maximum value of 100."
const sqMaxResultsLimit = 100

// sqPageInput validates a listing's MaxResults and NextToken and returns the page size and
// starting offset.
//
// MaxResults "defaults to a value appropriate to the operation"; substrate's is the upper
// bound, so an unpaginated caller sees every quota a service publishes in one page. A value
// outside 1–100 is IllegalArgumentException, the code the reference declares for invalid
// input. A token substrate did not issue is InvalidPaginationTokenException, which the
// reference declares for both quota listings and for ListServices — never page one.
func sqPageInput(maxResults *int, nextToken string) (pageSize, offset int, refusal *AWSError) {
	pageSize = sqMaxResultsLimit
	if maxResults != nil {
		if *maxResults < 1 || *maxResults > sqMaxResultsLimit {
			return 0, 0, sqIllegalArgument(fmt.Sprintf(
				"MaxResults %d is outside the valid range of 1 to %d", *maxResults, sqMaxResultsLimit))
		}
		pageSize = *maxResults
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return 0, 0, &AWSError{
			Code:       "InvalidPaginationTokenException",
			Message:    "The NextToken is not one this listing issued.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return pageSize, offset, nil
}

// sqWithNextToken adds NextToken to a listing's body when another page exists. On the last
// page it is absent, which a client reads as null: "repeat this until the NextToken response
// element comes back as null".
func sqWithNextToken(body map[string]interface{}, next string) map[string]interface{} {
	if next != "" {
		body["NextToken"] = next
	}
	return body
}

func (p *ServiceQuotasPlugin) requestServiceQuotaIncrease(mint *IDMint, accountID string, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode  string  `json:"ServiceCode"`
		QuotaCode    string  `json:"QuotaCode"`
		DesiredValue float64 `json:"DesiredValue"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sqInvalidBody()
	}
	if input.ServiceCode == "" || input.QuotaCode == "" {
		return nil, sqIllegalArgument("ServiceCode and QuotaCode are required")
	}

	id := generateSQSMessageID(mint) // reuse UUID generator

	qi := &QuotaIncrease{
		ID:           id,
		ServiceCode:  input.ServiceCode,
		QuotaCode:    input.QuotaCode,
		DesiredValue: input.DesiredValue,
		Status:       "PENDING",
		Created:      float64(p.tc.Now().Unix()),
	}

	ctx := context.Background()
	if err := p.saveIncrease(ctx, accountID, qi); err != nil {
		return nil, fmt.Errorf("servicequotas requestServiceQuotaIncrease saveIncrease: %w", err)
	}

	ids, err := p.loadIncreaseIDs(ctx, accountID)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)
	if err := p.saveIncreaseIDs(ctx, accountID, ids); err != nil {
		return nil, fmt.Errorf("servicequotas requestServiceQuotaIncrease saveIncreaseIDs: %w", err)
	}

	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"RequestedQuota": qi,
	})
}

// listRequestedServiceQuotaChangeHistory reports the caller's quota-increase
// requests, optionally filtered by service and status.
//
// This shipped as "ListRequestedServiceQuotaChangesByService", a name the Service
// Quotas API does not have: the 2019-06-24 model declares
// ListRequestedServiceQuotaChangeHistory (this operation) and
// ListRequestedServiceQuotaChangeHistoryByQuota, and nothing resembling
// "…ChangesByService". So the handler was reachable only by a hand-built
// X-Amz-Target — every SDK and CLI call got InvalidAction — which is why the
// plugin's own unit tests could not see it (#636). Both names now route here; the
// invented one is kept because a fixture may already drive it directly.
//
// The Status filter is honored rather than ignored: a caller asking for
// CASE_OPENED that was handed a PENDING record back would read an outcome the
// service never reports.
func (p *ServiceQuotasPlugin) listRequestedServiceQuotaChangeHistory(accountID string, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode string `json:"ServiceCode"`
		Status      string `json:"Status"`
	}
	// This is the one site in the file that decoded the body by hand rather than through [sqUnmarshal],
	// which is why it was the one site still discarding the error (#1007). The helper's own length check
	// replaces the one that used to be here.
	if refusal := sqUnmarshal(req.Body, &input); refusal != nil {
		return nil, refusal
	}

	ctx := context.Background()

	ids, err := p.loadIncreaseIDs(ctx, accountID)
	if err != nil {
		return nil, err
	}

	var results []QuotaIncrease
	for _, id := range ids {
		qi, err := p.loadIncrease(ctx, accountID, id)
		if err != nil || qi == nil {
			continue
		}
		if input.ServiceCode != "" && qi.ServiceCode != input.ServiceCode {
			continue
		}
		if input.Status != "" && qi.Status != input.Status {
			continue
		}
		results = append(results, *qi)
	}
	if results == nil {
		results = []QuotaIncrease{}
	}

	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"RequestedQuotas": results,
	})
}

func (p *ServiceQuotasPlugin) getRequestedServiceQuotaChange(accountID string, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		RequestID string `json:"RequestId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, sqInvalidBody()
	}

	qi, err := p.loadIncrease(context.Background(), accountID, input.RequestID)
	if err != nil {
		return nil, err
	}
	if qi == nil {
		return nil, &AWSError{
			Code:       "NoSuchResourceException",
			Message:    "No quota increase request found: " + input.RequestID,
			HTTPStatus: http.StatusBadRequest,
		}
	}

	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"RequestedQuota": qi,
	})
}

// --- Helpers -----------------------------------------------------------------

// findQuota returns the quota matching serviceCode and quotaCode from the
// built-in table.
//
// The refusal distinguishes an unknown service from an unknown quota code on a
// known service. Both are NoSuchResourceException — that is the only code the
// model declares — so the message is the only place the two can be told apart,
// and a caller that cannot tell them apart goes looking for a wrong quota code
// when the service name is what was wrong.
func (p *ServiceQuotasPlugin) findQuota(serviceCode, quotaCode string) (*ServiceQuota, *AWSError) {
	quotas, ok := defaultServiceQuotas[serviceCode]
	if !ok {
		return nil, sqNoSuchService(serviceCode)
	}
	for i := range quotas {
		if quotas[i].QuotaCode == quotaCode {
			return &quotas[i], nil
		}
	}
	return nil, &AWSError{
		Code: "NoSuchResourceException",
		Message: fmt.Sprintf("No quota code %q for service %q. Use ListServiceQuotas to find the quota codes "+
			"a service publishes.", quotaCode, serviceCode),
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqNoSuchService is the refusal for a service code that is not in the built-in
// table. It names the service so a caller does not read it as a bad quota code.
func sqNoSuchService(serviceCode string) *AWSError {
	return &AWSError{
		Code: "NoSuchResourceException",
		Message: fmt.Sprintf("No such service %q. Use ListServices to find the service codes substrate emulates "+
			"quotas for.", serviceCode),
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqIllegalArgument is the refusal for a missing or malformed request member.
// IllegalArgumentException is the code the model declares for it on every
// Service Quotas operation.
func sqIllegalArgument(message string) *AWSError {
	return &AWSError{
		Code:       "IllegalArgumentException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqUnmarshal decodes a request body, treating an absent body as an empty one so
// the required-member check below reports the missing member rather than a
// serialization failure.
func sqUnmarshal(body []byte, out interface{}) *AWSError {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return sqInvalidBody()
	}
	return nil
}

// sqInvalidBody reports that a request body would not decode.
//
// #950 found this file contradicting itself: five sites answered a code that appears
// on none of the four Service Quotas pages, while [sqIllegalArgument] below already
// documented IllegalArgumentException as "the code the model declares for it on every
// Service Quotas operation". It does — ListServiceQuotas, GetServiceQuota,
// RequestServiceQuotaIncrease and GetRequestedServiceQuotaChangeHistory all publish
// IllegalArgumentException at HTTP 400, glossed "Invalid input was provided." So a
// body that will not parse is one, and nothing is left answering the other.
//
// The message was the worse half. All five sites passed err.Error() through **bare**,
// with no prefix at all, so a caller was handed encoding/json's own text — which Go
// struct field failed to unmarshal — by an endpoint whose whole purpose is to look
// like AWS.
func sqInvalidBody() *AWSError {
	return sqIllegalArgument("the request body is not valid JSON")
}

// sqJSONResponse creates a JSON AWSResponse with the given status and body.
func sqJSONResponse(status int, body interface{}) (*AWSResponse, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("servicequotas marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       b,
	}, nil
}
