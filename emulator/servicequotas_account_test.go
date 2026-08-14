package emulator_test

import (
	"net/http"
	"slices"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file is #624: a quota increase belongs to the account that filed it.
//
// ServiceQuotasPlugin.HandleRequest discarded its *RequestContext and filed every
// increase under the literal 000000000000, so two accounts sharing one emulator
// shared one pile of requests. The existing Service Quotas tests could not see it
// — they are all one caller, so "everyone's requests" and "my requests" are the
// same set.

// sqTarget is the Service Quotas JSON-target endpoint.
//
// The signing name is servicequotasv20190624, not the plugin name: aws-sdk-go-v2
// puts the versioned form in the credential scope, and parser.go carries an alias
// for exactly that. Signing as "servicequotas" here would test a request no SDK
// sends — the same class of gap #610 was.
var sqTarget = signedRequestTarget{
	host:        "servicequotas.us-east-1.amazonaws.com",
	target:      "ServiceQuotas",
	signingName: "servicequotasv20190624",
}

// sqSecondAccount is a second caller, unrelated to the built-in test account.
const sqSecondAccount = "444455556666"

// TestServiceQuotas_IncreasesAreFiledUnderTheCaller is #624's repro.
//
// Two accounts each file one increase, and each must see only its own. Before the
// fix both saw both, and a consumer reading
// ListRequestedServiceQuotaChangeHistory to decide whether it had already asked for
// a raise would find another account's request and skip filing its own.
func TestServiceQuotas_IncreasesAreFiledUnderTheCaller(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t, sqSecondAccount)

	type requestedQuota struct {
		ID           string  `json:"Id"`
		ServiceCode  string  `json:"ServiceCode"`
		QuotaCode    string  `json:"QuotaCode"`
		DesiredValue float64 `json:"DesiredValue"`
		Status       string  `json:"Status"`
	}

	// The two accounts ask for different values, so a leak shows up as a wrong
	// DesiredValue rather than only as a count.
	filed := map[string]requestedQuota{}
	for account, want := range map[string]float64{
		orgManagementAccount: 50,
		sqSecondAccount:      75,
	} {
		var out struct {
			RequestedQuota requestedQuota `json:"RequestedQuota"`
		}
		status, code := decodeAWSResponse(t, signedRequest(t, ts, sqTarget, account,
			"RequestServiceQuotaIncrease", map[string]any{
				"ServiceCode":  "organizations",
				"QuotaCode":    "L-E619E033",
				"DesiredValue": want,
			}), &out)
		if status != http.StatusOK {
			t.Fatalf("RequestServiceQuotaIncrease as %s: %d %s", account, status, code)
		}
		if out.RequestedQuota.Status != "PENDING" {
			t.Errorf("as %s: Status = %q, want PENDING", account, out.RequestedQuota.Status)
		}
		filed[account] = out.RequestedQuota
	}

	for account, own := range filed {
		var listed struct {
			RequestedQuotas []requestedQuota `json:"RequestedQuotas"`
		}
		status, code := decodeAWSResponse(t, signedRequest(t, ts, sqTarget, account,
			"ListRequestedServiceQuotaChangeHistory", map[string]any{
				"ServiceCode": "organizations",
			}), &listed)
		if status != http.StatusOK {
			t.Fatalf("ListRequestedServiceQuotaChangeHistory as %s: %d %s", account, status, code)
		}
		if len(listed.RequestedQuotas) != 1 {
			t.Fatalf("as %s: %d requests listed, want 1 — the other account's request leaked",
				account, len(listed.RequestedQuotas))
		}
		if got := listed.RequestedQuotas[0]; got != own {
			t.Errorf("as %s: listed %+v, want its own %+v", account, got, own)
		}
	}

	// GetRequestedServiceQuotaChange is keyed the same way, so one account cannot
	// read the other's request by ID. NoSuchResourceException rather than the
	// record is the answer that matters: an ID is guessable, and a lookup that
	// ignored the caller would hand it over.
	status, code := decodeAWSResponse(t, signedRequest(t, ts, sqTarget, sqSecondAccount,
		"GetRequestedServiceQuotaChange", map[string]any{
			"RequestId": filed[orgManagementAccount].ID,
		}), nil)
	if status != http.StatusBadRequest || code != "NoSuchResourceException" {
		t.Errorf("reading another account's request by ID: %d %q, want 400/NoSuchResourceException",
			status, code)
	}

	// And its own is readable, so the refusal above is about the caller rather
	// than the operation being broken.
	var got struct {
		RequestedQuota requestedQuota `json:"RequestedQuota"`
	}
	status, code = decodeAWSResponse(t, signedRequest(t, ts, sqTarget, sqSecondAccount,
		"GetRequestedServiceQuotaChange", map[string]any{
			"RequestId": filed[sqSecondAccount].ID,
		}), &got)
	if status != http.StatusOK {
		t.Fatalf("GetRequestedServiceQuotaChange for its own request: %d %s", status, code)
	}
	if got.RequestedQuota != filed[sqSecondAccount] {
		t.Errorf("read back %+v, want %+v", got.RequestedQuota, filed[sqSecondAccount])
	}
}

// TestServiceQuotas_UnattributedCallerKeepsThePlaceholder pins the fallback.
//
// An unsigned request carries no account, and substrate's parser answers
// 000000000000 for it — the same literal the plugin used to hardcode. So a test
// that files an increase without signing still finds it, which is what keeps the
// existing Service Quotas fixtures working: #624 changes who a *signed* request is
// attributed to, not whether an unattributed one has somewhere to live.
func TestServiceQuotas_UnattributedCallerKeepsThePlaceholder(t *testing.T) {
	ts := emulator.StartTestServer(t)

	resp := makeServiceQuotasRequest(t, ts, "RequestServiceQuotaIncrease", map[string]interface{}{
		"ServiceCode":  "organizations",
		"QuotaCode":    "L-E619E033",
		"DesiredValue": float64(50),
	})
	if status, code := decodeAWSResponse(t, resp, nil); status != http.StatusOK {
		t.Fatalf("RequestServiceQuotaIncrease unsigned: %d %s", status, code)
	}

	var listed struct {
		RequestedQuotas []struct {
			QuotaCode string `json:"QuotaCode"`
		} `json:"RequestedQuotas"`
	}
	// Deliberately the legacy name, which is the one this handler shipped under and
	// which no SDK can send (#636). It is kept as an alias precisely so a fixture
	// like this one keeps working, so something has to hold it.
	status, code := decodeAWSResponse(t,
		makeServiceQuotasRequest(t, ts, "ListRequestedServiceQuotaChangesByService", map[string]interface{}{
			"ServiceCode": "organizations",
		}), &listed)
	if status != http.StatusOK {
		t.Fatalf("ListRequestedServiceQuotaChangesByService unsigned: %d %s", status, code)
	}
	if len(listed.RequestedQuotas) != 1 || listed.RequestedQuotas[0].QuotaCode != "L-E619E033" {
		t.Errorf("an unsigned caller listed %+v, want the one request it just filed", listed.RequestedQuotas)
	}
}

// TestServiceQuotas_ChangeHistoryFilters is #636: the operation an SDK can reach,
// and the filters it accepts.
//
// The handler shipped as ListRequestedServiceQuotaChangesByService, which is not an
// operation the Service Quotas API has — the 2019-06-24 model declares
// ListRequestedServiceQuotaChangeHistory and …ChangeHistoryByQuota. So every real
// SDK call answered InvalidAction while the unit tests, which name the operation
// themselves, stayed green. The two names must reach the same handler, and the
// Status filter has to narrow: a caller asking for DENIED that is handed a PENDING
// record reads an outcome the service never reported.
func TestServiceQuotas_ChangeHistoryFilters(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, svc := range []string{"organizations", "lambda"} {
		resp := makeServiceQuotasRequest(t, ts, "RequestServiceQuotaIncrease", map[string]interface{}{
			"ServiceCode":  svc,
			"QuotaCode":    map[string]string{"organizations": "L-E619E033", "lambda": "L-B99A9384"}[svc],
			"DesiredValue": float64(50),
		})
		if status, code := decodeAWSResponse(t, resp, nil); status != http.StatusOK {
			t.Fatalf("RequestServiceQuotaIncrease(%s): %d %s", svc, status, code)
		}
	}

	list := func(t *testing.T, operation string, input map[string]interface{}) []string {
		t.Helper()
		var out struct {
			RequestedQuotas []struct {
				ServiceCode string `json:"ServiceCode"`
			} `json:"RequestedQuotas"`
		}
		status, code := decodeAWSResponse(t, makeServiceQuotasRequest(t, ts, operation, input), &out)
		if status != http.StatusOK {
			t.Fatalf("%s%v: %d %s", operation, input, status, code)
		}
		codes := make([]string, 0, len(out.RequestedQuotas))
		for _, q := range out.RequestedQuotas {
			codes = append(codes, q.ServiceCode)
		}
		slices.Sort(codes)
		return codes
	}

	tests := []struct {
		name  string
		input map[string]interface{}
		want  []string
	}{
		{"no filter lists both", map[string]interface{}{}, []string{"lambda", "organizations"}},
		{"service narrows", map[string]interface{}{"ServiceCode": "lambda"}, []string{"lambda"}},
		{"status matches", map[string]interface{}{"Status": "PENDING"}, []string{"lambda", "organizations"}},
		{"status excludes", map[string]interface{}{"Status": "DENIED"}, []string{}},
		{
			"service and status together",
			map[string]interface{}{"ServiceCode": "organizations", "Status": "PENDING"},
			[]string{"organizations"},
		},
		{
			"a service with no request is empty, not everything",
			map[string]interface{}{"ServiceCode": "s3"},
			[]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := list(t, "ListRequestedServiceQuotaChangeHistory", tt.input)
			if !slices.Equal(got, tt.want) {
				t.Errorf("listed %v, want %v", got, tt.want)
			}
			// The legacy name is an alias, not a second implementation: whatever the
			// real operation answers, it answers.
			if legacy := list(t, "ListRequestedServiceQuotaChangesByService", tt.input); !slices.Equal(legacy, got) {
				t.Errorf("the legacy name listed %v, the real one %v", legacy, got)
			}
		})
	}
}
