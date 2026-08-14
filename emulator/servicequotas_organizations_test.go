package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file covers #620: `organizations` was in neither Service Quotas seed table,
// so a consumer could vend accounts with CreateAccount but could not read the
// ceiling those calls run into. It also covers the two refusals that made the gap
// hard to diagnose — an unknown service answering 200 with an empty list, and an
// unknown service being indistinguishable from an unknown quota code.

// sqQuota reads one quota through GetServiceQuota and returns it decoded.
func sqQuota(t *testing.T, srv *emulator.TestServer, serviceCode, quotaCode string) map[string]interface{} {
	t.Helper()
	resp := makeServiceQuotasRequest(t, srv, "GetServiceQuota", map[string]interface{}{
		"ServiceCode": serviceCode,
		"QuotaCode":   quotaCode,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		body, _ := json.Marshal(sqDecode(t, resp))
		t.Fatalf("GetServiceQuota(%s, %s): status %d: %s", serviceCode, quotaCode, resp.StatusCode, body)
	}
	quota, ok := sqDecode(t, resp)["Quota"].(map[string]interface{})
	if !ok {
		t.Fatalf("GetServiceQuota(%s, %s) carried no Quota", serviceCode, quotaCode)
	}
	return quota
}

// sqDecode decodes a Service Quotas response body into a map.
func sqDecode(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

// sqRefusal issues a request expected to be refused and returns the error code and
// message. The __type member carries the code, prefixed with the protocol's
// namespace on some paths, so only the suffix is compared.
func sqRefusal(t *testing.T, srv *emulator.TestServer, op string, body map[string]interface{}) (int, string, string) {
	t.Helper()
	resp := makeServiceQuotasRequest(t, srv, op, body)
	defer resp.Body.Close() //nolint:errcheck
	decoded := sqDecode(t, resp)
	code, _ := decoded["__type"].(string)
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	message, _ := decoded["message"].(string)
	if message == "" {
		message, _ = decoded["Message"].(string)
	}
	return resp.StatusCode, code, message
}

// TestServiceQuotas_OrganizationsQuotasAreReadable is #620's repro verbatim:
// L-E619E033 read through GetServiceQuota. It is the call that answered
// NoSuchResourceException before the service was seeded.
func TestServiceQuotas_OrganizationsQuotasAreReadable(t *testing.T) {
	srv := emulator.StartTestServer(t)

	tests := []struct {
		quotaCode  string
		name       string
		value      float64
		adjustable bool
	}{
		{"L-E619E033", "Maximum number of accounts", 10, true},
		{"L-29A0C5DF", "Service control policies in an organization", 10000, false},
		{"L-0F0F51F4", "Organizational units in an organization", 2000, false},
	}
	for _, tt := range tests {
		t.Run(tt.quotaCode, func(t *testing.T) {
			quota := sqQuota(t, srv, "organizations", tt.quotaCode)
			if got := quota["QuotaName"]; got != tt.name {
				t.Errorf("QuotaName = %v, want %q", got, tt.name)
			}
			if got, _ := quota["Value"].(float64); got != tt.value {
				t.Errorf("Value = %v, want %v", got, tt.value)
			}
			if got, _ := quota["Adjustable"].(bool); got != tt.adjustable {
				t.Errorf("Adjustable = %v, want %v", got, tt.adjustable)
			}
			// Organizations is a global service, so none of its quotas are
			// per-region. A consumer that reads GlobalQuota to decide whether to
			// enumerate regions would enumerate them for nothing.
			if got, _ := quota["GlobalQuota"].(bool); !got {
				t.Error("GlobalQuota = false, want true for a global service")
			}
			if got := quota["ServiceName"]; got != "AWS Organizations" {
				t.Errorf("ServiceName = %v, want \"AWS Organizations\"", got)
			}
		})
	}
}

// TestServiceQuotas_OrganizationsIsDiscoverable pins the discovery path a consumer
// actually walks: ListServices to find the code, ListServiceQuotas to find the
// quota codes, GetServiceQuota to read one. A service missing from any step is
// unreachable through the step before it.
func TestServiceQuotas_OrganizationsIsDiscoverable(t *testing.T) {
	srv := emulator.StartTestServer(t)

	listed := makeServiceQuotasRequest(t, srv, "ListServices", nil)
	defer listed.Body.Close() //nolint:errcheck
	services, _ := sqDecode(t, listed)["Services"].([]interface{})
	found := 0
	for _, svc := range services {
		entry, _ := svc.(map[string]interface{})
		if entry["ServiceCode"] == "organizations" {
			found++
			if entry["ServiceName"] != "AWS Organizations" {
				t.Errorf("ListServices names organizations %v", entry["ServiceName"])
			}
		}
	}
	if found != 1 {
		t.Fatalf("ListServices reported organizations %d times, want exactly 1", found)
	}

	quotasResp := makeServiceQuotasRequest(t, srv, "ListServiceQuotas", map[string]interface{}{
		"ServiceCode": "organizations",
	})
	defer quotasResp.Body.Close() //nolint:errcheck
	if quotasResp.StatusCode != http.StatusOK {
		t.Fatalf("ListServiceQuotas(organizations): status %d", quotasResp.StatusCode)
	}
	quotas, _ := sqDecode(t, quotasResp)["Quotas"].([]interface{})
	codes := make(map[string]bool, len(quotas))
	for _, q := range quotas {
		entry, _ := q.(map[string]interface{})
		code, _ := entry["QuotaCode"].(string)
		codes[code] = true
	}
	for _, want := range []string{"L-E619E033", "L-29A0C5DF", "L-0F0F51F4"} {
		if !codes[want] {
			t.Errorf("ListServiceQuotas(organizations) omitted %s; got %v", want, codes)
		}
	}
	if len(quotas) != 3 {
		t.Errorf("ListServiceQuotas(organizations) returned %d quotas, want 3", len(quotas))
	}
}

// TestServiceQuotas_OrganizationsValuesMatchTheEnforcedLimits is the property that
// makes the seeded values worth having: the ceiling a consumer reads and the
// ceiling the Organizations plugin refuses at are the same number. A quota table
// that disagrees with its own emulator is worse than a missing one, because a test
// written against the published value would then fail for the wrong reason.
func TestServiceQuotas_OrganizationsValuesMatchTheEnforcedLimits(t *testing.T) {
	quotas, ok := emulator.ServiceQuotaRowsForTest("organizations")
	if !ok {
		t.Fatal("organizations is not in defaultServiceQuotas")
	}

	want := map[string]float64{
		"L-E619E033": emulator.OrgMaxAccountsForTest,
		"L-29A0C5DF": emulator.OrgMaxSCPsPerOrgForTest,
		"L-0F0F51F4": emulator.OrgMaxOUsPerOrgForTest,
	}
	for _, quota := range quotas {
		expected, known := want[quota.QuotaCode]
		if !known {
			t.Errorf("unexpected quota %s in the organizations table", quota.QuotaCode)
			continue
		}
		if quota.Value != expected {
			t.Errorf("%s (%s) publishes %v but the plugin enforces %v",
				quota.QuotaCode, quota.QuotaName, quota.Value, expected)
		}
		delete(want, quota.QuotaCode)
	}
	for code := range want {
		t.Errorf("the organizations table is missing %s", code)
	}
}

// TestServiceQuotas_TheTwoTablesAgree closes the drift the plan named: the two seed
// tables are maintained by hand, ListServices reads only defaultServiceList and
// ListServiceQuotas only defaultServiceQuotas, so a service added to one and not
// the other is either undiscoverable or discoverable with no quotas. Neither is
// visible from any single response, which is why this asserts on the tables.
func TestServiceQuotas_TheTwoTablesAgree(t *testing.T) {
	listed := emulator.ServiceListForTest()
	if got := emulator.ServiceListLenForTest(); got != len(listed) {
		t.Errorf("defaultServiceList has %d entries but %d distinct service codes — a duplicate",
			got, len(listed))
	}

	for _, code := range emulator.ServiceQuotaCodesForTest() {
		name, ok := listed[code]
		if !ok {
			t.Errorf("%q has quotas but is absent from defaultServiceList, so ListServices cannot find it", code)
			continue
		}
		// Every row of a service's quotas repeats the service's name, so a
		// mismatch means the same service answers to two names depending on
		// which operation asked.
		quotas, _ := emulator.ServiceQuotaRowsForTest(code)
		for _, quota := range quotas {
			if quota.ServiceName != name {
				t.Errorf("%q is %q in defaultServiceList but %q on quota %s",
					code, name, quota.ServiceName, quota.QuotaCode)
			}
			if quota.ServiceCode != code {
				t.Errorf("quota %s is keyed under %q but carries ServiceCode %q",
					quota.QuotaCode, code, quota.ServiceCode)
			}
		}
		delete(listed, code)
	}
	for code := range listed {
		t.Errorf("%q is in defaultServiceList but has no quotas, so ListServiceQuotas refuses it", code)
	}
}

// TestServiceQuotas_UnknownServiceIsARefusal is the behavior change #620's
// "Related" note asks for. An empty Quotas list says the service exists and
// publishes no quotas; only NoSuchResourceException says it does not exist, and
// that is the error the model declares for ListServiceQuotas.
func TestServiceQuotas_UnknownServiceIsARefusal(t *testing.T) {
	srv := emulator.StartTestServer(t)

	for _, op := range []string{"ListServiceQuotas", "GetServiceQuota", "GetAWSDefaultServiceQuota"} {
		t.Run(op, func(t *testing.T) {
			status, code, message := sqRefusal(t, srv, op, map[string]interface{}{
				"ServiceCode": "nosuchsvc",
				"QuotaCode":   "L-E619E033",
			})
			if status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", status)
			}
			if code != "NoSuchResourceException" {
				t.Errorf("code = %q, want NoSuchResourceException", code)
			}
			// The message has to name the service, since that is the input that
			// was wrong. Naming only the quota code sends a caller to audit a
			// code that was fine.
			if !strings.Contains(message, `"nosuchsvc"`) {
				t.Errorf("message does not name the service: %q", message)
			}
			if !strings.Contains(message, "ListServices") {
				t.Errorf("message does not point at ListServices: %q", message)
			}
		})
	}
}

// TestServiceQuotas_UnknownQuotaCodeSaysSo is the other half of the same defect:
// a known service with an unknown quota code must not read as an unknown service.
// Both are NoSuchResourceException — the only code the model declares — so the
// message is the only place they differ.
func TestServiceQuotas_UnknownQuotaCodeSaysSo(t *testing.T) {
	srv := emulator.StartTestServer(t)

	status, code, message := sqRefusal(t, srv, "GetServiceQuota", map[string]interface{}{
		"ServiceCode": "organizations",
		"QuotaCode":   "L-NOTAQUOTA",
	})
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if code != "NoSuchResourceException" {
		t.Errorf("code = %q, want NoSuchResourceException", code)
	}
	if !strings.Contains(message, `"L-NOTAQUOTA"`) {
		t.Errorf("message does not name the quota code: %q", message)
	}
	if !strings.Contains(message, "ListServiceQuotas") {
		t.Errorf("message does not point at ListServiceQuotas: %q", message)
	}
	// The distinguishing assertion: this must not read as "no such service".
	if strings.Contains(message, "No such service") {
		t.Errorf("a known service with a bad quota code reported an unknown service: %q", message)
	}
}

// TestServiceQuotas_RequiredMembersAreEnforced pins the model's required members.
// Both operations declare ServiceCode required (GetServiceQuota also QuotaCode),
// and IllegalArgumentException is the error the model declares for bad input —
// answering NoSuchResourceException for an absent member would tell a caller the
// resource is missing when the request never named one.
func TestServiceQuotas_RequiredMembersAreEnforced(t *testing.T) {
	srv := emulator.StartTestServer(t)

	tests := []struct {
		name string
		op   string
		body map[string]interface{}
	}{
		{"ListServiceQuotas without a service", "ListServiceQuotas", map[string]interface{}{}},
		{"ListServiceQuotas with no body at all", "ListServiceQuotas", nil},
		{"GetServiceQuota without a service", "GetServiceQuota", map[string]interface{}{"QuotaCode": "L-E619E033"}},
		{"GetServiceQuota without a quota code", "GetServiceQuota", map[string]interface{}{"ServiceCode": "organizations"}},
		{"GetAWSDefaultServiceQuota without a quota code", "GetAWSDefaultServiceQuota", map[string]interface{}{"ServiceCode": "organizations"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, code, _ := sqRefusal(t, srv, tt.op, tt.body)
			if status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", status)
			}
			if code != "IllegalArgumentException" {
				t.Errorf("code = %q, want IllegalArgumentException", code)
			}
		})
	}
}

// TestServiceQuotas_BodyDecodeFailures covers the decode path with bodies the
// shared helper cannot produce, since it substitutes "{}" for an absent one.
//
// A body that is not JSON is a serialization failure. A body that is *absent*
// is not: it carries no members, so the useful answer is the missing required
// member rather than a decode error about the empty string — which is why the
// zero-length case is handled before the unmarshal rather than by it.
func TestServiceQuotas_BodyDecodeFailures(t *testing.T) {
	srv := emulator.StartTestServer(t)

	tests := []struct {
		name string
		op   string
		body string
		code string
	}{
		{"unparseable on list", "ListServiceQuotas", "{not json", "SerializationException"},
		{"unparseable on get", "GetServiceQuota", "{not json", "SerializationException"},
		{"empty on list", "ListServiceQuotas", "", "IllegalArgumentException"},
		{"empty on get", "GetServiceQuota", "", "IllegalArgumentException"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Host = "servicequotas.us-east-1.amazonaws.com"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "ServiceQuotas."+tt.op)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			code, _ := sqDecode(t, resp)["__type"].(string)
			if i := strings.LastIndex(code, "#"); i >= 0 {
				code = code[i+1:]
			}
			if code != tt.code {
				t.Errorf("code = %q, want %q", code, tt.code)
			}
		})
	}
}

// TestServiceQuotas_OrganizationsIncreaseIsRequestable is the rest of #620's story:
// the accounts quota is the adjustable one, so a consumer that reads 10 and wants
// more files an increase against it. The request is what a tool does next, and it
// has to reach a PENDING record rather than a refusal.
func TestServiceQuotas_OrganizationsIncreaseIsRequestable(t *testing.T) {
	srv := emulator.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, srv, "RequestServiceQuotaIncrease", map[string]interface{}{
		"ServiceCode":  "organizations",
		"QuotaCode":    "L-E619E033",
		"DesiredValue": 50.0,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RequestServiceQuotaIncrease: status %d", resp.StatusCode)
	}
	requested, ok := sqDecode(t, resp)["RequestedQuota"].(map[string]interface{})
	if !ok {
		t.Fatal("RequestServiceQuotaIncrease carried no RequestedQuota")
	}
	if requested["Status"] != "PENDING" {
		t.Errorf("Status = %v, want PENDING", requested["Status"])
	}
	if requested["QuotaCode"] != "L-E619E033" {
		t.Errorf("QuotaCode = %v, want L-E619E033", requested["QuotaCode"])
	}
	if got, _ := requested["DesiredValue"].(float64); got != 50 {
		t.Errorf("DesiredValue = %v, want 50", got)
	}
	// The increase does not move the published value. Service Quotas records the
	// request; nothing is granted until it is approved, and a consumer that read
	// the quota back expecting 50 would be reading a state AWS never reaches
	// synchronously.
	quota := sqQuota(t, srv, "organizations", "L-E619E033")
	if got, _ := quota["Value"].(float64); got != 10 {
		t.Errorf("after a pending increase the quota reads %v, want the unchanged 10", got)
	}
}
