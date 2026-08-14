package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ServiceQuotasPlugin emulates the AWS Service Quotas API.
// It handles ListServices, ListServiceQuotas, GetServiceQuota,
// GetAWSDefaultServiceQuota, RequestServiceQuotaIncrease,
// ListRequestedServiceQuotaChangesByService, and GetRequestedServiceQuotaChange.
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
func (p *ServiceQuotasPlugin) HandleRequest(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "ListServices":
		return p.listServices(req)
	case "ListServiceQuotas":
		return p.listServiceQuotas(req)
	case "GetServiceQuota":
		return p.getServiceQuota(req)
	case "GetAWSDefaultServiceQuota":
		return p.getAWSDefaultServiceQuota(req)
	case "RequestServiceQuotaIncrease":
		return p.requestServiceQuotaIncrease(req)
	case "ListRequestedServiceQuotaChangesByService":
		return p.listRequestedServiceQuotaChangesByService(req)
	case "GetRequestedServiceQuotaChange":
		return p.getRequestedServiceQuotaChange(req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("ServiceQuotasPlugin: unsupported operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
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

func (p *ServiceQuotasPlugin) listServices(_ *AWSRequest) (*AWSResponse, error) {
	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"Services": defaultServiceList,
	})
}

func (p *ServiceQuotasPlugin) listServiceQuotas(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode string `json:"ServiceCode"`
	}
	if err := sqUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.ServiceCode == "" {
		return nil, sqIllegalArgument("ServiceCode is required")
	}

	// An unrecognized service is NoSuchResourceException, which the model declares
	// for this operation. Answering 200 with an empty list instead claims the
	// service exists and has no quotas — a different, and false, statement.
	quotas, ok := defaultServiceQuotas[input.ServiceCode]
	if !ok {
		return nil, sqNoSuchService(input.ServiceCode)
	}

	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"Quotas": quotas,
	})
}

func (p *ServiceQuotasPlugin) getServiceQuota(req *AWSRequest) (*AWSResponse, error) {
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
	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"Quota": quota,
	})
}

func (p *ServiceQuotasPlugin) getAWSDefaultServiceQuota(req *AWSRequest) (*AWSResponse, error) {
	return p.getServiceQuota(req)
}

func (p *ServiceQuotasPlugin) requestServiceQuotaIncrease(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode  string  `json:"ServiceCode"`
		QuotaCode    string  `json:"QuotaCode"`
		DesiredValue float64 `json:"DesiredValue"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.ServiceCode == "" || input.QuotaCode == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "ServiceCode and QuotaCode are required", HTTPStatus: http.StatusBadRequest}
	}

	// Use a placeholder account ID since RequestContext is not passed here.
	// The account ID will be pulled from req.AccountID if available in future.
	accountID := "000000000000"

	id := generateSQSMessageID() // reuse UUID generator

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

func (p *ServiceQuotasPlugin) listRequestedServiceQuotaChangesByService(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode string `json:"ServiceCode"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	accountID := "000000000000"
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
		if input.ServiceCode == "" || qi.ServiceCode == input.ServiceCode {
			results = append(results, *qi)
		}
	}
	if results == nil {
		results = []QuotaIncrease{}
	}

	return sqJSONResponse(http.StatusOK, map[string]interface{}{
		"RequestedQuotas": results,
	})
}

func (p *ServiceQuotasPlugin) getRequestedServiceQuotaChange(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		RequestID string `json:"RequestId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	accountID := "000000000000"
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
		return &AWSError{
			Code:       "SerializationException",
			Message:    err.Error(),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
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
