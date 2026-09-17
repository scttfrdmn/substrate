package emulator_test

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// The six describes that published Marker and MaxRecords and implemented neither (#916).
//
// #887 fixed the cursor *basis* at the three operations that had one. These six had none at
// all: each returned its whole listing, emitted no Marker, and ignored a MaxRecords the caller
// sent. That is the worst of the three possible states — implemented, absent-and-refused, or
// silently ignored — because a consumer's paging loop is dead code that never runs until it
// runs against real AWS, and a Marker obtained elsewhere is answered with the full listing
// again rather than refused.
//
// Every assertion here is made over the wire against records created through the service's own
// create call, for the reason #887 recorded: the property under test is one only a paging
// caller can observe, and a test that read state directly cannot see it.

// sixDescribeOp is one of the six operations, with everything that differs between them.
//
// The six are exercised through one table because the *contract* is one contract — they share
// parseQueryMarker, queryMaxRecords and queryMarkerPage — so a divergence between them is a
// defect by definition, and six hand-written tests would be six chances to assert it
// differently.
type sixDescribeOp struct {
	name string
	// newServer, request and body are the service's own test harness; the two families have
	// separate ones and neither is generic over the other.
	newServer func(*testing.T) *httptest.Server
	request   func(*testing.T, *httptest.Server, map[string]string) *http.Response
	body      func(*testing.T, *http.Response) string
	// setup runs once before the records are created, for an operation whose create needs a
	// resource of another kind to exist first.
	setup func(*testing.T, *httptest.Server)
	// create is the action that mints one record, and createParams renders the rest of its
	// request for the identifier id.
	create       string
	createParams func(id string) map[string]string
	// describe is the paginated action, idElement the response member carrying a record's
	// identifier, and idPrefix the prefix the test's identifiers are minted under.
	describe  string
	idElement string
	idPrefix  string
	// notFoundCode and notFoundStatus are the fault a single-record filter answers for a record
	// that does not exist. notFoundCode is "" at the five operations that publish such a fault
	// but do not yet answer it, which is #1020 rather than #916.
	notFoundCode   string
	notFoundStatus int
	// filterParam is the single-resource filter parameter the operation publishes.
	filterParam string
}

// sixDescribeOps is the set #916 names, with the identifier each operation's marker is keyed by.
//
// The identifier is always the trailing segment of the state key, which is what queryMarkerPage
// compares the cursor against — a marker keyed by anything else would not be well defined over
// the listing order, which is the trap #887 fixed at the first three operations.
var sixDescribeOps = []sixDescribeOp{
	{
		name:      "RDS DescribeDBSnapshots",
		newServer: newRDSTestServer,
		request:   rdsRequest,
		body:      rdsBody,
		setup: func(t *testing.T, ts *httptest.Server) {
			t.Helper()
			resp := rdsRequest(t, ts, map[string]string{
				"Action":               "CreateDBInstance",
				"DBInstanceIdentifier": "pg-source",
				"DBInstanceClass":      "db.t3.micro",
				"Engine":               "mysql",
			})
			if b := rdsBody(t, resp); resp.StatusCode != http.StatusOK {
				t.Fatalf("CreateDBInstance status %d, body: %s", resp.StatusCode, b)
			}
		},
		create: "CreateDBSnapshot",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":               "CreateDBSnapshot",
				"DBSnapshotIdentifier": id,
				"DBInstanceIdentifier": "pg-source",
			}
		},
		describe:    "DescribeDBSnapshots",
		idElement:   "DBSnapshotIdentifier",
		idPrefix:    "pg-snap-",
		filterParam: "DBSnapshotIdentifier",
	},
	{
		name:      "RDS DescribeDBSubnetGroups",
		newServer: newRDSTestServer,
		request:   rdsRequest,
		body:      rdsBody,
		create:    "CreateDBSubnetGroup",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":                   "CreateDBSubnetGroup",
				"DBSubnetGroupName":        id,
				"DBSubnetGroupDescription": "paging",
			}
		},
		describe:    "DescribeDBSubnetGroups",
		idElement:   "DBSubnetGroupName",
		idPrefix:    "pg-dbsn-",
		filterParam: "DBSubnetGroupName",
	},
	{
		name:      "RDS DescribeDBParameterGroups",
		newServer: newRDSTestServer,
		request:   rdsRequest,
		body:      rdsBody,
		create:    "CreateDBParameterGroup",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":                 "CreateDBParameterGroup",
				"DBParameterGroupName":   id,
				"DBParameterGroupFamily": "mysql8.0",
				"Description":            "paging",
			}
		},
		describe:    "DescribeDBParameterGroups",
		idElement:   "DBParameterGroupName",
		idPrefix:    "pg-dbpg-",
		filterParam: "DBParameterGroupName",
	},
	{
		name:      "ElastiCache DescribeReplicationGroups",
		newServer: newElastiCacheTestServer,
		request:   ecRequest,
		body:      ecBody,
		create:    "CreateReplicationGroup",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":                      "CreateReplicationGroup",
				"ReplicationGroupId":          id,
				"ReplicationGroupDescription": "paging",
				"CacheNodeType":               "cache.t3.micro",
				"Engine":                      "redis",
			}
		},
		describe:       "DescribeReplicationGroups",
		idElement:      "ReplicationGroupId",
		idPrefix:       "pg-repl-",
		notFoundCode:   "ReplicationGroupNotFoundFault",
		notFoundStatus: http.StatusNotFound,
		filterParam:    "ReplicationGroupId",
	},
	{
		name:      "ElastiCache DescribeCacheSubnetGroups",
		newServer: newElastiCacheTestServer,
		request:   ecRequest,
		body:      ecBody,
		create:    "CreateCacheSubnetGroup",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":                      "CreateCacheSubnetGroup",
				"CacheSubnetGroupName":        id,
				"CacheSubnetGroupDescription": "paging",
			}
		},
		describe:    "DescribeCacheSubnetGroups",
		idElement:   "CacheSubnetGroupName",
		idPrefix:    "pg-ecsn-",
		filterParam: "CacheSubnetGroupName",
	},
	{
		name:      "ElastiCache DescribeCacheParameterGroups",
		newServer: newElastiCacheTestServer,
		request:   ecRequest,
		body:      ecBody,
		create:    "CreateCacheParameterGroup",
		createParams: func(id string) map[string]string {
			return map[string]string{
				"Action":                    "CreateCacheParameterGroup",
				"CacheParameterGroupName":   id,
				"CacheParameterGroupFamily": "redis7",
				"Description":               "paging",
			}
		},
		describe:    "DescribeCacheParameterGroups",
		idElement:   "CacheParameterGroupName",
		idPrefix:    "pg-ecpg-",
		filterParam: "CacheParameterGroupName",
	},
}

// queryMarkerXMLPage decodes the record identifiers and the next Marker out of any of the six
// response bodies, by element name and nesting depth rather than through a per-operation struct.
//
// Depth is what makes one decoder safe for six shapes. All six nest their records as
// Response > Result > Wrapper > Item > Identifier, so an identifier sits at depth five and the
// Result's own Marker at depth three. Matching on the name alone would be a weaker assertion
// than the per-operation structs it replaces: a record rendered into the wrong wrapper, or a
// Marker rendered inside a record, would still be picked up and the page would read as correct.
// Six near-identical structs would instead be six sets of tags free to drift apart, which is the
// divergence this file exists to rule out.
func queryMarkerXMLPage(t *testing.T, body, idElement string) markerPage {
	t.Helper()
	const (
		identifierDepth = 5
		markerDepth     = 3
	)
	dec := xml.NewDecoder(strings.NewReader(body))
	var (
		out        markerPage
		depth      int
		takeID     bool
		takeMarker bool
	)
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode %s response: %v\nbody: %s", idElement, err, body)
		}
		switch el := tok.(type) {
		case xml.StartElement:
			depth++
			takeID = depth == identifierDepth && el.Name.Local == idElement
			takeMarker = depth == markerDepth && el.Name.Local == "Marker"
		case xml.CharData:
			switch {
			case takeID:
				out.IDs = append(out.IDs, string(el))
				takeID = false
			case takeMarker:
				out.Marker = string(el)
				takeMarker = false
			}
		case xml.EndElement:
			depth--
			takeID, takeMarker = false, false
		}
	}
	return out
}

// createSixDescribeRecords creates n records for op and returns their identifiers in the
// lexicographic order the describe reports them in.
func createSixDescribeRecords(t *testing.T, op sixDescribeOp, ts *httptest.Server, n int) []string {
	t.Helper()
	if op.setup != nil {
		op.setup(t, ts)
	}
	ids := markerTestIDs(op.idPrefix, n)
	for _, id := range ids {
		resp := op.request(t, ts, op.createParams(id))
		if b := op.body(t, resp); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s status %d, body: %s", op.create, id, resp.StatusCode, b)
		}
	}
	return ids
}

// TestSixDescribesPageASingleFullListing is the "smaller than the page size" half of #916's
// fourth criterion: a listing that fits in one page reports every record and carries no Marker.
//
// It is the case a caller cannot distinguish from the old behavior by looking at one response,
// which is why the two halves are separate tests: before #916 the whole listing came back with
// no Marker too, and only the second test can tell the difference.
func TestSixDescribesPageASingleFullListing(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			want := createSixDescribeRecords(t, op, ts, 5)

			resp := op.request(t, ts, map[string]string{
				"Action":     op.describe,
				"MaxRecords": markerTestPageSize,
			})
			body := op.body(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, body: %s", resp.StatusCode, body)
			}
			page := queryMarkerXMLPage(t, body, op.idElement)
			if !slices.Equal(page.IDs, want) {
				t.Errorf("page = %v, want %v", page.IDs, want)
			}
			if page.Marker != "" {
				t.Errorf("a listing inside one page carries Marker %q, want none", page.Marker)
			}
		})
	}
}

// TestSixDescribesPageEveryRecordExactlyOnce is the "larger than the page size" half: every
// record is reported once across the pages, none repeated and none omitted.
//
// Twenty-one records at the documented minimum of twenty is the smallest listing that pages at
// all, because #913 made twenty the smallest page AWS accepts. It also pins the Marker's value
// as the last record of the page rather than a count of the records before it — the distinction
// #887 exists for, and the one an offset cursor passes until the set changes.
func TestSixDescribesPageEveryRecordExactlyOnce(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			want := createSixDescribeRecords(t, op, ts, 21)

			resp := op.request(t, ts, map[string]string{
				"Action":     op.describe,
				"MaxRecords": markerTestPageSize,
			})
			body := op.body(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("page 1 status %d, body: %s", resp.StatusCode, body)
			}
			first := queryMarkerXMLPage(t, body, op.idElement)
			if !slices.Equal(first.IDs, want[:20]) {
				t.Errorf("page 1 = %v, want %v", first.IDs, want[:20])
			}
			if wantMarker := base64.StdEncoding.EncodeToString([]byte(want[19])); first.Marker != wantMarker {
				t.Errorf("page 1 Marker = %q, want %q — the last record of the page, not an offset", first.Marker, wantMarker)
			}

			resp = op.request(t, ts, map[string]string{
				"Action":     op.describe,
				"MaxRecords": markerTestPageSize,
				"Marker":     first.Marker,
			})
			body = op.body(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("page 2 status %d, body: %s", resp.StatusCode, body)
			}
			second := queryMarkerXMLPage(t, body, op.idElement)
			if !slices.Equal(second.IDs, want[20:]) {
				t.Errorf("page 2 = %v, want %v", second.IDs, want[20:])
			}
			if second.Marker != "" {
				t.Errorf("the last page carries Marker %q, want none", second.Marker)
			}
			assertNoDuplicates(t, want, append(first.IDs, second.IDs...))
		})
	}
}

// TestSixDescribesFullLastPageCarriesNoMarker pins the truncation rule at all six: the Marker is
// emitted when a further record exists, not when a page fills up, so a listing that is an exact
// multiple of the page size costs no round trip to an empty page.
func TestSixDescribesFullLastPageCarriesNoMarker(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			createSixDescribeRecords(t, op, ts, 40)

			resp := op.request(t, ts, map[string]string{
				"Action":     op.describe,
				"MaxRecords": markerTestPageSize,
			})
			first := queryMarkerXMLPage(t, op.body(t, resp), op.idElement)
			if first.Marker == "" {
				t.Fatalf("page 1 of forty records carries no Marker")
			}

			resp = op.request(t, ts, map[string]string{
				"Action":     op.describe,
				"MaxRecords": markerTestPageSize,
				"Marker":     first.Marker,
			})
			second := queryMarkerXMLPage(t, op.body(t, resp), op.idElement)
			if len(second.IDs) != 20 {
				t.Errorf("page 2 carries %d records, want twenty", len(second.IDs))
			}
			if second.Marker != "" {
				t.Errorf("a full last page carries Marker %q, want none", second.Marker)
			}
		})
	}
}

// TestSixDescribesRefuseAMarkerTheyDidNotIssue asserts the refusal at all six, and asserts it
// against an *empty* listing so it is a parameter check rather than a data-dependent one.
//
// The values are the two forms a token substrate never issued actually takes: an offset left
// over from a recording made before the cursor changed basis, and a string that is not base64
// at all. The code is InvalidParameterValue / 400 at every one of the six — published on
// API_DescribeReplicationGroups and API_DescribeCacheParameterGroups, and substrate's reading
// at the other four, whose pages publish only their NotFound faults.
func TestSixDescribesRefuseAMarkerTheyDidNotIssue(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			for _, marker := range []string{"2", "not base64!"} {
				t.Run(marker, func(t *testing.T) {
					resp := op.request(t, ts, map[string]string{
						"Action": op.describe,
						"Marker": marker,
					})
					body := op.body(t, resp)
					if resp.StatusCode != http.StatusBadRequest {
						t.Fatalf("status %d, want 400; body: %s", resp.StatusCode, body)
					}
					if !strings.Contains(body, "InvalidParameterValue") {
						t.Errorf("body does not report InvalidParameterValue: %s", body)
					}
				})
			}
		})
	}
}

// TestSixDescribesRefuseAMaxRecordsOutsideTheDocumentedRange asserts that the six inherit #913's
// range check rather than the coercion that preceded it, so the nine operations sharing the
// helper cannot disagree about what a page size is.
func TestSixDescribesRefuseAMaxRecordsOutsideTheDocumentedRange(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			for _, tt := range maxRecordsRefusalCases {
				t.Run(tt.name, func(t *testing.T) {
					resp := op.request(t, ts, map[string]string{
						"Action":     op.describe,
						"MaxRecords": tt.value,
					})
					assertMaxRecordsRefused(t, resp.StatusCode, op.body(t, resp))
				})
			}
		})
	}
}

// TestSixDescribesFilterPastPageOneStillAnswersTheRecord is #916's fifth criterion, and it is
// the one assertion here that could have gone wrong in implementation rather than in absence.
//
// Each of the six publishes a single-resource filter parameter. Filtering for a record that
// sorts past the first page must answer that record, not an empty page and not the NotFound
// fault the filter's absence would justify. It holds because the filter runs *inside*
// queryMarkerPage's record callback, so a non-matching record answers ok=false and consumes no
// page slot — the filtered listing is one record long however deep into the unfiltered listing
// it sits. Asserting it is what keeps a later refactor from moving the filter outside the
// callback, where a filtered request for the twenty-fifth record would answer NotFound for a
// record that exists.
func TestSixDescribesFilterPastPageOneStillAnswersTheRecord(t *testing.T) {
	for _, op := range sixDescribeOps {
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			want := createSixDescribeRecords(t, op, ts, 25)
			target := want[24]

			resp := op.request(t, ts, map[string]string{
				"Action":       op.describe,
				"MaxRecords":   markerTestPageSize,
				op.filterParam: target,
			})
			body := op.body(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("filtering for %s answered status %d; body: %s", target, resp.StatusCode, body)
			}
			page := queryMarkerXMLPage(t, body, op.idElement)
			if !slices.Equal(page.IDs, []string{target}) {
				t.Errorf("filtered page = %v, want exactly %v", page.IDs, []string{target})
			}
			if page.Marker != "" {
				t.Errorf("a one-record filtered page carries Marker %q, want none", page.Marker)
			}
		})
	}
}

// TestSixDescribesNotFoundSurvivesPagination guards whichever of the six answer a NotFound fault:
// the fault must still fire for a resource that does not exist, over a listing long enough to
// page, which is the pairing that would break if the filter and the fault were ordered wrongly
// against each other.
//
// It is driven off the table rather than written against DescribeReplicationGroups, the only one
// of the six that answers a fault today, so that the five #1020 covers come under the same guard
// as they gain one rather than needing a test written per operation. Those five publish a fault
// their handler does not answer — a filtered request for a record that does not exist gets an
// empty 200 — which is a defect about a request's result rather than about how a listing is
// paged, so it is #1020 rather than part of #916, following the reasoning #887 used for keeping
// MaxRecords out of its own diff.
func TestSixDescribesNotFoundSurvivesPagination(t *testing.T) {
	var guarded int
	for _, op := range sixDescribeOps {
		if op.notFoundCode == "" {
			continue
		}
		guarded++
		t.Run(op.name, func(t *testing.T) {
			ts := op.newServer(t)
			createSixDescribeRecords(t, op, ts, 25)

			resp := op.request(t, ts, map[string]string{
				"Action":       op.describe,
				"MaxRecords":   markerTestPageSize,
				op.filterParam: op.idPrefix + "absent",
			})
			body := op.body(t, resp)
			if resp.StatusCode != op.notFoundStatus {
				t.Fatalf("status %d, want %d; body: %s", resp.StatusCode, op.notFoundStatus, body)
			}
			if !strings.Contains(body, op.notFoundCode) {
				t.Errorf("body does not report %s: %s", op.notFoundCode, body)
			}
		})
	}
	if guarded == 0 {
		t.Fatal("no operation in the table answers a NotFound fault; the guard has nothing to assert")
	}
}
