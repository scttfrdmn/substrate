package substrate_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	substrate "github.com/scttfrdmn/substrate"
)

// makeServiceQuotasRequest issues a Service Quotas JSON-target request.
func makeServiceQuotasRequest(t *testing.T, srv *substrate.TestServer, op string, body map[string]interface{}) *http.Response {
	t.Helper()
	var bodyStr string
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		bodyStr = string(b)
	} else {
		bodyStr = "{}"
	}

	req, err := http.NewRequest(http.MethodPost, srv.URL,
		strings.NewReader(bodyStr))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = "servicequotas.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "ServiceQuotas."+op)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestServiceQuotas_ListServices(t *testing.T) {
	srv := substrate.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "ListServices", nil)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListServices: got status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	services, ok := result["Services"].([]interface{})
	if !ok || len(services) == 0 {
		t.Fatalf("expected non-empty Services list, got %v", result["Services"])
	}

	// Verify lambda is in the list.
	found := false
	for _, svc := range services {
		m := svc.(map[string]interface{})
		if m["ServiceCode"] == "lambda" {
			found = true
			break
		}
	}
	if !found {
		t.Error("lambda not found in ListServices result")
	}
}

func TestServiceQuotas_ListServiceQuotas(t *testing.T) {
	srv := substrate.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "ListServiceQuotas", map[string]interface{}{
		"ServiceCode": "lambda",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListServiceQuotas: got status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	quotas, ok := result["Quotas"].([]interface{})
	if !ok || len(quotas) == 0 {
		t.Fatalf("expected non-empty Quotas for lambda, got %v", result["Quotas"])
	}

	// Verify the concurrent executions quota.
	found := false
	for _, q := range quotas {
		m := q.(map[string]interface{})
		if m["QuotaCode"] == "L-B99A9384" {
			found = true
			v, _ := m["Value"].(float64)
			if v != 1000 {
				t.Errorf("expected Value=1000, got %v", v)
			}
		}
	}
	if !found {
		t.Error("concurrent executions quota L-B99A9384 not found")
	}
}

func TestServiceQuotas_GetServiceQuota(t *testing.T) {
	srv := substrate.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "GetServiceQuota", map[string]interface{}{
		"ServiceCode": "s3",
		"QuotaCode":   "L-DC2B2D3D",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetServiceQuota: got status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	quota, ok := result["Quota"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected Quota field, got %v", result)
	}
	if quota["QuotaName"] != "Buckets" {
		t.Errorf("expected QuotaName=Buckets, got %v", quota["QuotaName"])
	}
}

func TestServiceQuotas_GetServiceQuota_NotFound(t *testing.T) {
	srv := substrate.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "GetServiceQuota", map[string]interface{}{
		"ServiceCode": "s3",
		"QuotaCode":   "NONEXISTENT",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode == http.StatusOK {
		t.Fatal("expected error for nonexistent quota, got 200")
	}
}

func TestServiceQuotas_RequestAndGetIncrease(t *testing.T) {
	srv := substrate.StartTestServer(t)

	// Request an increase.
	resp := makeServiceQuotasRequest(t, srv, "RequestServiceQuotaIncrease", map[string]interface{}{
		"ServiceCode":  "lambda",
		"QuotaCode":    "L-B99A9384",
		"DesiredValue": 2000.0,
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RequestServiceQuotaIncrease: got status %d", resp.StatusCode)
	}

	var createResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	requested, ok := createResult["RequestedQuota"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected RequestedQuota field, got %v", createResult)
	}
	reqID, _ := requested["Id"].(string)
	if reqID == "" {
		t.Fatal("expected non-empty Id in RequestedQuota")
	}
	if requested["Status"] != "PENDING" {
		t.Errorf("expected Status=PENDING, got %v", requested["Status"])
	}

	// GetRequestedServiceQuotaChange.
	resp2 := makeServiceQuotasRequest(t, srv, "GetRequestedServiceQuotaChange", map[string]interface{}{
		"RequestId": reqID,
	})
	defer resp2.Body.Close() //nolint:errcheck

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GetRequestedServiceQuotaChange: got status %d", resp2.StatusCode)
	}

	var getResult map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&getResult); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	qi, _ := getResult["RequestedQuota"].(map[string]interface{})
	if qi["Id"] != reqID {
		t.Errorf("expected Id=%s, got %v", reqID, qi["Id"])
	}

	// ListRequestedServiceQuotaChangesByService.
	resp3 := makeServiceQuotasRequest(t, srv, "ListRequestedServiceQuotaChangesByService", map[string]interface{}{
		"ServiceCode": "lambda",
	})
	defer resp3.Body.Close() //nolint:errcheck

	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("ListRequestedServiceQuotaChangesByService: got status %d", resp3.StatusCode)
	}

	var listResult map[string]interface{}
	if err := json.NewDecoder(resp3.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	items, _ := listResult["RequestedQuotas"].([]interface{})
	if len(items) != 1 {
		t.Errorf("expected 1 requested quota, got %d", len(items))
	}
}

func TestServiceQuotas_GetAWSDefaultServiceQuota(t *testing.T) {
	srv := substrate.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "GetAWSDefaultServiceQuota", map[string]interface{}{
		"ServiceCode": "dynamodb",
		"QuotaCode":   "L-F98FE922",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetAWSDefaultServiceQuota: got status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	quota, _ := result["Quota"].(map[string]interface{})
	if quota["QuotaName"] != "Table count" {
		t.Errorf("expected QuotaName=Table count, got %v", quota["QuotaName"])
	}
}
