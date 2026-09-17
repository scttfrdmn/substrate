package emulator_test

import (
	"net/http"
	"strings"
	"testing"
)

// MaxRecords is refused outside the published range, not honored or rewritten (#913).
//
// Both the RDS and the ElastiCache Query families publish the same three facts on the
// parameter — "Default: 100" and a minimum of 20 with a maximum of 100 — and substrate
// honored any positive integer while silently rewriting 0, -1 and a non-integer to 100.
// The two halves fail differently and both are asserted here:
//
//   - An honored MaxRecords=5 produces a five-record page AWS would refuse, so a consumer
//     that works against substrate fails against AWS. That is the direction of divergence
//     that matters, and it is the one an emulator cannot leave in place.
//   - A rewritten MaxRecords=0 is invisible: a caller asking for a small page and receiving
//     a hundred records sees the same well-formed shape as a caller whose listing is short.
//
// **The finding this fix records rather than hides**: every pagination test in the tree
// paged at MaxRecords=2 before #913 — eleven request sites across eight tests in
// query_marker_pagination_test.go, rds_plugin_test.go and elasticache_plugin_test.go — which is
// a page size real RDS and ElastiCache refuse. Those tests passed only because substrate was
// permissive, which is evidence of the divergence rather than noise, so they now page at the
// documented minimum of twenty and create twenty-odd records to get a second page (see
// markerTestPageSize).
//
// The refusal is asserted against an *empty* listing at every operation, which is what
// makes it a parameter check rather than a data-dependent one: nothing has been created, so
// a handler that read state first would answer an empty 200 instead.

// maxRecordsRefusalCases are the values no describe accepts, with why each one matters.
var maxRecordsRefusalCases = []struct {
	name  string
	value string
}{
	{name: "one below the minimum", value: "19"},
	{name: "the page size every test used to ask for", value: "2"},
	{name: "one above the maximum", value: "101"},
	{name: "ten times the maximum", value: "1000"},
	{name: "zero", value: "0"},
	{name: "negative", value: "-1"},
	{name: "not an integer", value: "abc"},
	{name: "an integer with trailing text", value: "20records"},
}

// assertMaxRecordsRefused requires a 400 whose body reports InvalidParameterValue and names
// the documented range, following the message convention parseSimulateRequest and
// parseS3ListBucketsParams already set: a refusal that does not say what the bound is
// leaves the caller to guess it.
func assertMaxRecordsRefused(t *testing.T, status int, body string) {
	t.Helper()
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400; body: %s", status, body)
	}
	if !strings.Contains(body, "InvalidParameterValue") {
		t.Errorf("body does not report InvalidParameterValue: %s", body)
	}
	if !strings.Contains(body, "between 20 and 100") {
		t.Errorf("the message does not name the documented range: %s", body)
	}
}

func TestRDSDescribeDBInstancesRefusesAMaxRecordsOutsideTheDocumentedRange(t *testing.T) {
	ts := newRDSTestServer(t)
	for _, tt := range maxRecordsRefusalCases {
		t.Run(tt.name, func(t *testing.T) {
			resp := rdsRequest(t, ts, map[string]string{
				"Action":     "DescribeDBInstances",
				"MaxRecords": tt.value,
			})
			assertMaxRecordsRefused(t, resp.StatusCode, rdsBody(t, resp))
		})
	}
}

func TestRDSDescribeDBClustersRefusesAMaxRecordsOutsideTheDocumentedRange(t *testing.T) {
	ts := newRDSTestServer(t)
	for _, tt := range maxRecordsRefusalCases {
		t.Run(tt.name, func(t *testing.T) {
			resp := rdsRequest(t, ts, map[string]string{
				"Action":     "DescribeDBClusters",
				"MaxRecords": tt.value,
			})
			assertMaxRecordsRefused(t, resp.StatusCode, rdsBody(t, resp))
		})
	}
}

func TestElastiCacheDescribeCacheClustersRefusesAMaxRecordsOutsideTheDocumentedRange(t *testing.T) {
	ts := newElastiCacheTestServer(t)
	for _, tt := range maxRecordsRefusalCases {
		t.Run(tt.name, func(t *testing.T) {
			resp := ecRequest(t, ts, map[string]string{
				"Action":     "DescribeCacheClusters",
				"MaxRecords": tt.value,
			})
			assertMaxRecordsRefused(t, resp.StatusCode, ecBody(t, resp))
		})
	}
}

// TestQueryMaxRecordsAcceptsTheEndsOfTheDocumentedRange pins the bounds as inclusive: the
// range AWS publishes is "Minimum 20, maximum 100", so both ends are values the caller may
// ask for and a refusal at either would be substrate inventing a narrower contract.
func TestQueryMaxRecordsAcceptsTheEndsOfTheDocumentedRange(t *testing.T) {
	ts := newRDSTestServer(t)
	for _, value := range []string{"20", "100"} {
		t.Run("MaxRecords="+value, func(t *testing.T) {
			resp := rdsRequest(t, ts, map[string]string{
				"Action":     "DescribeDBInstances",
				"MaxRecords": value,
			})
			if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
			}
		})
	}
}

// TestRDSDescribeDBInstancesAbsentMaxRecordsPagesAtTheDocumentedDefault asserts the
// default survives the refusal: an absent MaxRecords is not "unusable", it is the case
// AWS publishes a default for.
//
// It creates 101 instances because that is what distinguishes a default of 100 from a
// default of "everything" — the exact number is the published one, so asserting a page of
// twenty-odd would leave the value unpinned.
func TestRDSDescribeDBInstancesAbsentMaxRecordsPagesAtTheDocumentedDefault(t *testing.T) {
	ts := newRDSTestServer(t)
	ids := markerTestIDs("mr-db-", 101)
	for _, id := range ids {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": id,
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "mysql",
		})
		if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBInstance %s status %d, body: %s", id, resp.StatusCode, body)
		}
	}

	resp := rdsRequest(t, ts, map[string]string{"Action": "DescribeDBInstances"})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", resp.StatusCode, body)
	}
	page := rdsInstancePage(t, body)
	if len(page.IDs) != 100 {
		t.Errorf("page 1 carries %d records, want the published default of 100", len(page.IDs))
	}
	if page.Marker == "" {
		t.Error("page 1 of 101 records carries no Marker, so the 101st is unreachable")
	}
}
