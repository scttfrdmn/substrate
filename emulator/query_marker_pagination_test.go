package emulator_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// The RDS and ElastiCache Marker cursor (#887).
//
// Every assertion here is made over the wire against a resource created through the
// service's own create call, because the property under test is one a caller can only
// observe by paging: that the union of the pages is the listing, with nothing skipped
// and nothing repeated, even when the set changes between two requests. A test that
// read state directly, or that asserted only "page one has a Marker", cannot see the
// defect — the previous pagination test asserted exactly that and passed against an
// offset cursor that dropped a record (recorded on #887).

// markerPage is the part of an RDS or ElastiCache describe response this file asserts
// on: the record identifiers and the next Marker.
//
// The identifiers of all three operations are decoded through one type because the
// element names are the only difference between them, and a page whose records land in
// the wrong wrapper would otherwise read as an empty page rather than as a failure.
type markerPage struct {
	IDs    []string
	Marker string
}

// rdsInstancePage decodes a DescribeDBInstances response.
func rdsInstancePage(t *testing.T, body string) markerPage {
	t.Helper()
	var doc struct {
		Result struct {
			IDs    []string `xml:"DBInstances>DBInstance>DBInstanceIdentifier"`
			Marker string   `xml:"Marker"`
		} `xml:"DescribeDBInstancesResult"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode DescribeDBInstances response: %v\nbody: %s", err, body)
	}
	return markerPage{IDs: doc.Result.IDs, Marker: doc.Result.Marker}
}

// rdsClusterPage decodes a DescribeDBClusters response.
func rdsClusterPage(t *testing.T, body string) markerPage {
	t.Helper()
	var doc struct {
		Result struct {
			IDs    []string `xml:"DBClusters>DBCluster>DBClusterIdentifier"`
			Marker string   `xml:"Marker"`
		} `xml:"DescribeDBClustersResult"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode DescribeDBClusters response: %v\nbody: %s", err, body)
	}
	return markerPage{IDs: doc.Result.IDs, Marker: doc.Result.Marker}
}

// ecClusterPage decodes a DescribeCacheClusters response.
func ecClusterPage(t *testing.T, body string) markerPage {
	t.Helper()
	var doc struct {
		Result struct {
			IDs    []string `xml:"CacheClusters>CacheCluster>CacheClusterId"`
			Marker string   `xml:"Marker"`
		} `xml:"DescribeCacheClustersResult"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode DescribeCacheClusters response: %v\nbody: %s", err, body)
	}
	return markerPage{IDs: doc.Result.IDs, Marker: doc.Result.Marker}
}

// assertNoDuplicates reports every identifier that appears in more than one page, and
// every expected identifier no page reported.
func assertNoDuplicates(t *testing.T, want []string, got []string) {
	t.Helper()
	seen := make(map[string]int, len(got))
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		switch seen[id] {
		case 1:
		case 0:
			t.Errorf("%q was never reported across the pages; got %v", id, got)
		default:
			t.Errorf("%q was reported %d times across the pages; got %v", id, seen[id], got)
		}
	}
	for id, n := range seen {
		if n > 0 && !slices.Contains(want, id) {
			t.Errorf("%q was reported but was not expected; got %v", id, got)
		}
	}
}

func TestRDSDescribeDBInstancesMarkerNamesTheLastRecordOfThePage(t *testing.T) {
	ts := newRDSTestServer(t)
	for i := range 3 {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": fmt.Sprintf("mk-db-%d", i),
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "mysql",
		})
		if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBInstance %d status %d, body: %s", i, resp.StatusCode, body)
		}
	}

	resp := rdsRequest(t, ts, map[string]string{
		"Action":     "DescribeDBInstances",
		"MaxRecords": "2",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("page 1 status %d, body: %s", resp.StatusCode, body)
	}
	page := rdsInstancePage(t, body)

	if got, want := page.IDs, []string{"mk-db-0", "mk-db-1"}; !slices.Equal(got, want) {
		t.Errorf("page 1 = %v, want %v", got, want)
	}
	// The Marker is the last record of the page, not a count of the records before it.
	// A caller cannot rely on the encoding, but substrate's own contract has to be
	// pinned somewhere: an offset here would page correctly until the set changed.
	if want := base64.StdEncoding.EncodeToString([]byte("mk-db-1")); page.Marker != want {
		t.Errorf("page 1 Marker = %q, want %q", page.Marker, want)
	}

	resp = rdsRequest(t, ts, map[string]string{
		"Action":     "DescribeDBInstances",
		"MaxRecords": "2",
		"Marker":     page.Marker,
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("page 2 status %d, body: %s", resp.StatusCode, body)
	}
	page2 := rdsInstancePage(t, body)
	if got, want := page2.IDs, []string{"mk-db-2"}; !slices.Equal(got, want) {
		t.Errorf("page 2 = %v, want %v", got, want)
	}
	if page2.Marker != "" {
		t.Errorf("the last page carries Marker %q, want none", page2.Marker)
	}
}

// TestRDSDescribeDBInstancesMarkerSurvivesADeletionBetweenPages is the defect #887
// records: an offset Marker skips a record when the listing shrinks behind the cursor.
//
// Five instances are paged two at a time and the first is deleted after page one. The
// offset cursor answered page two as `items[2:]` of a now-four-record listing, so
// mk-db-2 was never reported — the caller's loop terminated having seen four of the
// five records with no indication anything was missing.
func TestRDSDescribeDBInstancesMarkerSurvivesADeletionBetweenPages(t *testing.T) {
	ts := newRDSTestServer(t)
	for i := range 5 {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": fmt.Sprintf("mk-db-%d", i),
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "mysql",
		})
		if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBInstance %d status %d, body: %s", i, resp.StatusCode, body)
		}
	}

	var (
		got     []string
		marker  string
		deleted bool
	)
	for page := 1; page <= 5; page++ {
		params := map[string]string{
			"Action":     "DescribeDBInstances",
			"MaxRecords": "2",
		}
		if marker != "" {
			params["Marker"] = marker
		}
		resp := rdsRequest(t, ts, params)
		body := rdsBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d status %d, body: %s", page, resp.StatusCode, body)
		}
		decoded := rdsInstancePage(t, body)
		got = append(got, decoded.IDs...)

		if !deleted {
			// mk-db-0 has already been reported and sorts before the marker, so
			// removing it must not move any later record.
			resp = rdsRequest(t, ts, map[string]string{
				"Action":               "DeleteDBInstance",
				"DBInstanceIdentifier": "mk-db-0",
			})
			if b := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
				t.Fatalf("DeleteDBInstance status %d, body: %s", resp.StatusCode, b)
			}
			deleted = true
		}

		marker = decoded.Marker
		if marker == "" {
			break
		}
	}

	if marker != "" {
		t.Fatalf("paging did not terminate; last Marker %q, got %v", marker, got)
	}
	assertNoDuplicates(t, []string{"mk-db-0", "mk-db-1", "mk-db-2", "mk-db-3", "mk-db-4"}, got)
}

// TestRDSDescribeDBInstancesFullLastPageCarriesNoMarker pins the truncation rule: the
// Marker is emitted when a further record exists, not when a page fills up. Four
// records paged two at a time is the case that distinguishes them.
func TestRDSDescribeDBInstancesFullLastPageCarriesNoMarker(t *testing.T) {
	ts := newRDSTestServer(t)
	for i := range 4 {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": fmt.Sprintf("mk-db-%d", i),
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "mysql",
		})
		if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBInstance %d status %d, body: %s", i, resp.StatusCode, body)
		}
	}

	resp := rdsRequest(t, ts, map[string]string{
		"Action":     "DescribeDBInstances",
		"MaxRecords": "2",
	})
	first := rdsInstancePage(t, rdsBody(t, resp))
	if first.Marker == "" {
		t.Fatal("page 1 of 4 records carries no Marker")
	}

	resp = rdsRequest(t, ts, map[string]string{
		"Action":     "DescribeDBInstances",
		"MaxRecords": "2",
		"Marker":     first.Marker,
	})
	second := rdsInstancePage(t, rdsBody(t, resp))
	if len(second.IDs) != 2 {
		t.Errorf("page 2 = %v, want two records", second.IDs)
	}
	if second.Marker != "" {
		t.Errorf("a full last page carries Marker %q, want none — it costs the caller a round trip to an empty page", second.Marker)
	}
}

func TestRDSDescribeMarkerItDidNotIssueIsRefused(t *testing.T) {
	ts := newRDSTestServer(t)

	tests := []struct {
		name   string
		action string
		marker string
	}{
		{name: "instances offset marker", action: "DescribeDBInstances", marker: "2"},
		{name: "instances not base64", action: "DescribeDBInstances", marker: "mk-db-1!"},
		{name: "clusters offset marker", action: "DescribeDBClusters", marker: "2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := rdsRequest(t, ts, map[string]string{
				"Action": tt.action,
				"Marker": tt.marker,
			})
			body := rdsBody(t, resp)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d, want 400; body: %s", resp.StatusCode, body)
			}
			if !strings.Contains(body, "InvalidParameterValue") {
				t.Errorf("body does not report InvalidParameterValue: %s", body)
			}
		})
	}
}

func TestRDSDescribeDBClustersMarkerPagesEveryClusterOnce(t *testing.T) {
	ts := newRDSTestServer(t)
	want := []string{"mk-cl-0", "mk-cl-1", "mk-cl-2", "mk-cl-3", "mk-cl-4"}
	for _, id := range want {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":              "CreateDBCluster",
			"DBClusterIdentifier": id,
			"Engine":              "aurora-mysql",
			"MasterUsername":      "admin",
		})
		if body := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBCluster %s status %d, body: %s", id, resp.StatusCode, body)
		}
	}

	var got []string
	marker := ""
	for page := 1; page <= 6; page++ {
		params := map[string]string{"Action": "DescribeDBClusters", "MaxRecords": "2"}
		if marker != "" {
			params["Marker"] = marker
		}
		resp := rdsRequest(t, ts, params)
		body := rdsBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d status %d, body: %s", page, resp.StatusCode, body)
		}
		decoded := rdsClusterPage(t, body)
		got = append(got, decoded.IDs...)
		marker = decoded.Marker
		if marker == "" {
			break
		}
	}
	if marker != "" {
		t.Fatalf("paging did not terminate; got %v", got)
	}
	assertNoDuplicates(t, want, got)
}

func TestElastiCacheDescribeCacheClustersMarkerSurvivesADeletionBetweenPages(t *testing.T) {
	ts := newElastiCacheTestServer(t)
	want := []string{"mk-ec-0", "mk-ec-1", "mk-ec-2", "mk-ec-3", "mk-ec-4"}
	for _, id := range want {
		resp := ecRequest(t, ts, map[string]string{
			"Action":         "CreateCacheCluster",
			"CacheClusterId": id,
			"CacheNodeType":  "cache.t3.micro",
			"Engine":         "redis",
			"NumCacheNodes":  "1",
		})
		if body := ecBody(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateCacheCluster %s status %d, body: %s", id, resp.StatusCode, body)
		}
	}

	var got []string
	marker := ""
	deleted := false
	for page := 1; page <= 6; page++ {
		params := map[string]string{"Action": "DescribeCacheClusters", "MaxRecords": "2"}
		if marker != "" {
			params["Marker"] = marker
		}
		resp := ecRequest(t, ts, params)
		body := ecBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d status %d, body: %s", page, resp.StatusCode, body)
		}
		decoded := ecClusterPage(t, body)
		got = append(got, decoded.IDs...)

		if !deleted {
			resp = ecRequest(t, ts, map[string]string{
				"Action":         "DeleteCacheCluster",
				"CacheClusterId": "mk-ec-0",
			})
			if b := ecBody(t, resp); resp.StatusCode != http.StatusOK {
				t.Fatalf("DeleteCacheCluster status %d, body: %s", resp.StatusCode, b)
			}
			deleted = true
		}

		marker = decoded.Marker
		if marker == "" {
			break
		}
	}
	if marker != "" {
		t.Fatalf("paging did not terminate; got %v", got)
	}
	assertNoDuplicates(t, want, got)
}

func TestElastiCacheDescribeCacheClustersRefusesAMarkerItDidNotIssue(t *testing.T) {
	ts := newElastiCacheTestServer(t)
	resp := ecRequest(t, ts, map[string]string{
		"Action":     "DescribeCacheClusters",
		"MaxRecords": "2",
		"Marker":     "2",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400; body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "InvalidParameterValue") {
		t.Errorf("body does not report InvalidParameterValue: %s", body)
	}
}
