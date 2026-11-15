package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// sqWireQuota is the ServiceQuota members the EC2 vCPU tests assert on.
type sqWireQuota struct {
	ServiceCode string  `json:"ServiceCode"`
	ServiceName string  `json:"ServiceName"`
	QuotaArn    string  `json:"QuotaArn"`
	QuotaCode   string  `json:"QuotaCode"`
	QuotaName   string  `json:"QuotaName"`
	Value       float64 `json:"Value"`
	Unit        string  `json:"Unit"`
	Adjustable  bool    `json:"Adjustable"`
	GlobalQuota bool    `json:"GlobalQuota"`
	UsageMetric *struct {
		MetricNamespace               string            `json:"MetricNamespace"`
		MetricName                    string            `json:"MetricName"`
		MetricDimensions              map[string]string `json:"MetricDimensions"`
		MetricStatisticRecommendation string            `json:"MetricStatisticRecommendation"`
	} `json:"UsageMetric"`
}

// sqCall sends one Service Quotas request and decodes the reply into out, returning the
// status code and the raw body.
func sqCall(t *testing.T, srv *emulator.TestServer, op string, body map[string]interface{}, out interface{}) (int, string) {
	t.Helper()
	resp := makeServiceQuotasRequest(t, srv, op, body)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if out != nil && resp.StatusCode == http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, out), "%s: %s", op, raw)
	}
	return resp.StatusCode, string(raw)
}

// TestServiceQuotas_EC2VCPUQuotas pins every EC2 vCPU quota substrate publishes to the
// On-Demand and Spot tables on AWS's instance-type quotas page: its code, its name, its
// default, and the AWS/Usage metric it names (#1498).
func TestServiceQuotas_EC2VCPUQuotas(t *testing.T) {
	t.Parallel()
	srv := emulator.StartTestServer(t)

	cases := []struct {
		code, name, class string
		applied, def      float64
	}{
		{"L-1216C47A", "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances", "Standard/OnDemand", 32, 5},
		{"L-74FC7D96", "Running On-Demand F instances", "F/OnDemand", 0, 0},
		{"L-DB2E81BA", "Running On-Demand G and VT instances", "G/OnDemand", 0, 0},
		{"L-1945791B", "Running On-Demand Inf instances", "Inf/OnDemand", 0, 0},
		{"L-417A185B", "Running On-Demand P instances", "P/OnDemand", 0, 0},
		{"L-7295265B", "Running On-Demand X instances", "X/OnDemand", 0, 0},
		{"L-34B43A08", "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests", "Standard/Spot", 5, 5},
		{"L-88CF9481", "All F Spot Instance Requests", "F/Spot", 0, 0},
		{"L-3819A6DF", "All G and VT Spot Instance Requests", "G/Spot", 0, 0},
		{"L-B5D1601B", "All Inf Spot Instance Requests", "Inf/Spot", 0, 0},
		{"L-7212CCBC", "All P Spot Instance Requests", "P/Spot", 0, 0},
		{"L-E3A00192", "All X Spot Instance Requests", "X/Spot", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			for _, op := range []struct {
				name  string
				value float64
				arn   string
			}{
				{"GetServiceQuota", tc.applied, "arn:aws:servicequotas:us-east-1:123456789012:ec2/" + tc.code},
				// A default belongs to no account, so its ARN names none.
				{"GetAWSDefaultServiceQuota", tc.def, "arn:aws:servicequotas:us-east-1::ec2/" + tc.code},
			} {
				var out struct {
					Quota sqWireQuota `json:"Quota"`
				}
				status, raw := sqCall(t, srv, op.name, map[string]interface{}{
					"ServiceCode": "ec2", "QuotaCode": tc.code,
				}, &out)
				require.Equal(t, http.StatusOK, status, "%s: %s", op.name, raw)
				q := out.Quota
				assert.Equal(t, tc.name, q.QuotaName, op.name)
				assert.Equal(t, op.value, q.Value, op.name)
				assert.Equal(t, op.arn, q.QuotaArn, op.name)
				assert.Equal(t, "Amazon Elastic Compute Cloud (Amazon EC2)", q.ServiceName, op.name)
				assert.Equal(t, "None", q.Unit, op.name)
				assert.True(t, q.Adjustable, op.name)
				assert.False(t, q.GlobalQuota, op.name)
				require.NotNil(t, q.UsageMetric, op.name)
				assert.Equal(t, "AWS/Usage", q.UsageMetric.MetricNamespace)
				assert.Equal(t, "ResourceCount", q.UsageMetric.MetricName)
				assert.Equal(t, "Maximum", q.UsageMetric.MetricStatisticRecommendation)
				assert.Equal(t, map[string]string{
					"Service": "EC2", "Type": "Resource", "Resource": "vCPU", "Class": tc.class,
				}, q.UsageMetric.MetricDimensions)
			}
		})
	}
}

// TestServiceQuotas_ListAWSDefaultServiceQuotasPaginates walks the EC2 default listing a
// page at a time, which is the operation #1498 found answering UnknownOperationException.
func TestServiceQuotas_ListAWSDefaultServiceQuotasPaginates(t *testing.T) {
	t.Parallel()
	srv := emulator.StartTestServer(t)

	var codes []string
	token := ""
	pages := 0
	for {
		body := map[string]interface{}{"ServiceCode": "ec2", "MaxResults": 5}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			Quotas    []sqWireQuota `json:"Quotas"`
			NextToken *string       `json:"NextToken"`
		}
		status, raw := sqCall(t, srv, "ListAWSDefaultServiceQuotas", body, &out)
		require.Equal(t, http.StatusOK, status, raw)
		pages++
		require.LessOrEqual(t, len(out.Quotas), 5)
		for _, q := range out.Quotas {
			codes = append(codes, q.QuotaCode)
			assert.True(t, strings.HasPrefix(q.QuotaArn, "arn:aws:servicequotas:us-east-1::ec2/"), q.QuotaArn)
			if q.QuotaCode == "L-1216C47A" {
				assert.Equal(t, float64(5), q.Value, "the default listing reports AWS's default, not the applied 32")
			}
		}
		if out.NextToken == nil {
			break
		}
		token = *out.NextToken
	}
	assert.Equal(t, 3, pages, "12 quotas at 5 a page is three pages, and the last carries no token")
	assert.Len(t, codes, 12)
	assert.Contains(t, codes, "L-34B43A08")
}

// TestServiceQuotas_ListingInputs covers the listing members and their refusals.
func TestServiceQuotas_ListingInputs(t *testing.T) {
	t.Parallel()
	srv := emulator.StartTestServer(t)

	cases := []struct {
		name     string
		op       string
		body     map[string]interface{}
		wantCode string // empty for 200
		wantLen  int
	}{
		{"QuotaCode narrows to one", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "QuotaCode": "L-34B43A08"}, "", 1},
		{"an unpublished QuotaCode narrows to none", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "QuotaCode": "L-NOTAQUOTA"}, "", 0},
		{"ACCOUNT lists every account-level quota", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "QuotaAppliedAtLevel": "ACCOUNT"}, "", 12},
		{"RESOURCE lists none", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "QuotaAppliedAtLevel": "RESOURCE"}, "", 0},
		{"an unknown level is refused", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "QuotaAppliedAtLevel": "REGION"}, "IllegalArgumentException", 0},
		{"MaxResults below 1", "ListServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "MaxResults": 0}, "IllegalArgumentException", 0},
		{"MaxResults above 100", "ListAWSDefaultServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "MaxResults": 101}, "IllegalArgumentException", 0},
		{"a token substrate did not issue", "ListAWSDefaultServiceQuotas",
			map[string]interface{}{"ServiceCode": "ec2", "NextToken": "bm90LWEtbnVtYmVy"}, "InvalidPaginationTokenException", 0},
		{"an unknown service", "ListAWSDefaultServiceQuotas",
			map[string]interface{}{"ServiceCode": "nosuchsvc"}, "NoSuchResourceException", 0},
		{"no service", "ListAWSDefaultServiceQuotas",
			map[string]interface{}{}, "IllegalArgumentException", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out struct {
				Quotas []sqWireQuota `json:"Quotas"`
			}
			status, raw := sqCall(t, srv, tc.op, tc.body, &out)
			if tc.wantCode == "" {
				require.Equal(t, http.StatusOK, status, raw)
				assert.Len(t, out.Quotas, tc.wantLen)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, tc.wantCode)
		})
	}
}

// TestServiceQuotas_ListServicesPaginates checks ListServices honors MaxResults and
// NextToken, and that walking it reports every service exactly once.
func TestServiceQuotas_ListServicesPaginates(t *testing.T) {
	t.Parallel()
	srv := emulator.StartTestServer(t)

	seen := map[string]bool{}
	token := ""
	for range 10 {
		body := map[string]interface{}{"MaxResults": 4}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			Services []struct {
				ServiceCode string `json:"ServiceCode"`
			} `json:"Services"`
			NextToken *string `json:"NextToken"`
		}
		status, raw := sqCall(t, srv, "ListServices", body, &out)
		require.Equal(t, http.StatusOK, status, raw)
		require.LessOrEqual(t, len(out.Services), 4)
		for _, s := range out.Services {
			assert.False(t, seen[s.ServiceCode], "%s reported twice", s.ServiceCode)
			seen[s.ServiceCode] = true
		}
		if out.NextToken == nil {
			break
		}
		token = *out.NextToken
	}
	assert.Len(t, seen, emulator.ServiceListLenForTest())
	assert.True(t, seen["ec2"])
}

// TestServiceQuotas_EC2VCPUQuotaClassOf pins the instance-type-to-class rule: first letter,
// with the prefixes that first-letter matching would misfile tested first.
func TestServiceQuotas_EC2VCPUQuotaClassOf(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"m5.large": "Standard", "c5.xlarge": "Standard", "r5.2xlarge": "Standard", "t3.micro": "Standard",
		"a1.medium": "Standard", "d3.xlarge": "Standard", "h1.2xlarge": "Standard", "i4i.large": "Standard",
		"z1d.large": "Standard", "M5.LARGE": "Standard",
		"inf1.xlarge": "Inf", "inf2.xlarge": "Inf",
		"g5.xlarge": "G", "g4dn.xlarge": "G", "vt1.3xlarge": "G",
		"p3.2xlarge": "P", "p4d.24xlarge": "P",
		"f1.2xlarge": "F", "x2idn.16xlarge": "X",
		// Classes with a quota substrate does not publish, and families with no vCPU quota.
		"trn1.2xlarge": "", "dl1.24xlarge": "", "hpc7g.4xlarge": "", "u-6tb1.metal": "", "mac2.metal": "",
		"q9.large": "", "": "",
	}
	for instanceType, want := range cases {
		assert.Equal(t, want, emulator.EC2VCPUQuotaClassOfForTest(instanceType), instanceType)
	}
}
