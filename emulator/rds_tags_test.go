package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// RDS tagging across the four resource types its ARNs address (part of #835).
//
// rdsResolveARN recognized `db:` and `snapshot:` only, while RDS's own CreateDBCluster and
// CreateDBSubnetGroup hand callers `cluster:` and `subgrp:` ARNs — so substrate reported two ARN
// shapes that its own tag operations then refused, and the Resource Groups Tagging API could not
// reach either resource at all.
//
// Every resource here is created through its owning operation and every tag read back through a
// tag operation, never out of the state store. That is #765's rule, and here it is load-bearing
// twice over: a helper writing the record directly would write the key the assertion has to
// trust, and it would write a record of whichever shape the *test* chose rather than the shape
// CreateDBCluster stores — which is the shape the merge path used to truncate.

// rdsTagTarget is the wire detail of an RDS query-protocol request: the Host the parser routes
// on and the SigV4 signing name a real SDK sends.
const (
	rdsTagHost        = "rds.us-east-1.amazonaws.com"
	rdsTagSigningName = "rds"
	rdsTagRegion      = "us-east-1"
)

// rdsTagServer starts a server callable as [taggingTestAccount], with both RDS and the tagging
// API registered — the point of most of these tests is that the two agree.
func rdsTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// rdsQuery posts one RDS query-protocol action as the given account and returns the status, the
// body, and the error code a refusal carries.
//
// The error code is returned separately for the reason [decodeAWSResponse] gives for the JSON
// protocol: a refusal and a success are different documents, and a test asserting on a refusal
// cares which code it got rather than what the message says.
func rdsQuery(t *testing.T, ts *emulator.TestServer, account string, params map[string]string) (int, string, string) {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	if !ok {
		t.Fatalf("no credential registered for account %s", account)
	}

	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	encoded := form.Encode()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(encoded))
	if err != nil {
		t.Fatalf("build %s request: %v", params["Action"], err)
	}
	req.Host = rdsTagHost
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(
		http.MethodPost, "/", rdsTagHost, rdsTagSigningName, rdsTagRegion, sigV4TestDateTime,
		[]byte(encoded), creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s as %s: %v", params["Action"], account, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s body: %v", params["Action"], err)
	}

	var errDoc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Code    string   `xml:"Error>Code"`
	}
	if unmarshalErr := xml.Unmarshal(raw, &errDoc); unmarshalErr == nil && errDoc.Code != "" {
		return resp.StatusCode, string(raw), errDoc.Code
	}
	return resp.StatusCode, string(raw), ""
}

// rdsQueryOK posts an action and fails the test unless it answers 200.
func rdsQueryOK(t *testing.T, ts *emulator.TestServer, params map[string]string) string {
	t.Helper()
	status, body, errCode := rdsQuery(t, ts, taggingTestAccount, params)
	if errCode != "" || status != http.StatusOK {
		t.Fatalf("%s: status %d, error %q, body %s", params["Action"], status, errCode, body)
	}
	return body
}

// rdsXMLTag is one member of a TagList as RDS renders it.
type rdsXMLTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// rdsTagList returns the resource's tags as RDS's own ListTagsForResource reports them, in
// document order.
//
// Order is preserved rather than collapsed into a map because it is one of the things under
// test: the list was built by ranging a Go map, so two identical calls answered differently.
func rdsTagList(t *testing.T, ts *emulator.TestServer, arn string) []rdsXMLTag {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": arn,
	})
	var doc struct {
		Tags []rdsXMLTag `xml:"ListTagsForResourceResult>TagList>Tag"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode ListTagsForResource %s: %v (body %s)", arn, err, body)
	}
	return doc.Tags
}

// rdsTags reduces [rdsTagList] to a map, for an assertion that is about a value rather than an
// order.
func rdsTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, tag := range rdsTagList(t, ts, arn) {
		out[tag.Key] = tag.Value
	}
	return out
}

// createRDSCluster creates an Aurora DB cluster and returns the ARN RDS reports for it.
//
// The ARN comes out of the response rather than being composed here, because a composed ARN
// would let the test agree with a resolver that disagreed with what CreateDBCluster hands a
// caller — which is precisely the divergence being fixed.
func createRDSCluster(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":              "CreateDBCluster",
		"DBClusterIdentifier": id,
		"Engine":              "aurora-postgresql",
		"MasterUsername":      "admin",
	})
	var doc struct {
		ARN string `xml:"CreateDBClusterResult>DBCluster>DBClusterArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode CreateDBCluster: %v (body %s)", err, body)
	}
	if doc.ARN == "" {
		t.Fatalf("CreateDBCluster reported no DBClusterArn: %s", body)
	}
	return doc.ARN
}

// createRDSSubnetGroup creates a DB subnet group and returns the ARN RDS reports for it.
func createRDSSubnetGroup(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":                   "CreateDBSubnetGroup",
		"DBSubnetGroupName":        name,
		"DBSubnetGroupDescription": "subnets for " + name,
		"VpcId":                    "vpc-0123456789abcdef0",
	})
	var doc struct {
		ARN string `xml:"CreateDBSubnetGroupResult>DBSubnetGroup>DBSubnetGroupArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode CreateDBSubnetGroup: %v (body %s)", err, body)
	}
	if doc.ARN == "" {
		t.Fatalf("CreateDBSubnetGroup reported no DBSubnetGroupArn: %s", body)
	}
	return doc.ARN
}

// createRDSInstance creates a DB instance and returns the ARN RDS reports for it.
func createRDSInstance(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": id,
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "postgres",
		"MasterUsername":       "admin",
	})
	var doc struct {
		ARN string `xml:"CreateDBInstanceResult>DBInstance>DBInstanceArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode CreateDBInstance: %v (body %s)", err, body)
	}
	if doc.ARN == "" {
		t.Fatalf("CreateDBInstance reported no DBInstanceArn: %s", body)
	}
	return doc.ARN
}

// TestRDSTagging_EveryARNRDSMintsIsReadableThroughItsOwnTagCalls is #765's criterion applied to
// the four resource types RDS builds ARNs for.
//
// Both directions are asserted for each: a removal aimed at a resource the resolver could not
// key is the more damaging half, and it is the half that used to answer InvalidParameterValue
// for an ARN RDS itself had just reported.
func TestRDSTagging_EveryARNRDSMintsIsReadableThroughItsOwnTagCalls(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		create func(t *testing.T, ts *emulator.TestServer) string
	}{{
		name:   "db instance",
		create: func(t *testing.T, ts *emulator.TestServer) string { return createRDSInstance(t, ts, "orders-db") },
	}, {
		name:   "db cluster",
		create: func(t *testing.T, ts *emulator.TestServer) string { return createRDSCluster(t, ts, "orders-cluster") },
	}, {
		name: "db subnet group",
		create: func(t *testing.T, ts *emulator.TestServer) string {
			return createRDSSubnetGroup(t, ts, "orders-subnets")
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := rdsTagServer(t)
			arn := tc.create(t, ts)

			rdsQueryOK(t, ts, map[string]string{
				"Action":              "AddTagsToResource",
				"ResourceName":        arn,
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "test",
			})
			if got := rdsTags(t, ts, arn)["env"]; got != "test" {
				t.Errorf("ListTagsForResource env = %q, want test", got)
			}

			rdsQueryOK(t, ts, map[string]string{
				"Action":           "RemoveTagsFromResource",
				"ResourceName":     arn,
				"TagKeys.member.1": "env",
			})
			if _, still := rdsTags(t, ts, arn)["env"]; still {
				t.Errorf("RemoveTagsFromResource left env in place on %s", arn)
			}
		})
	}
}

// TestRDSTagging_ATaggedClusterKeepsItsOwnMembers is the truncation assertion, and the one that
// fails against the arm this PR replaces.
//
// mergeResourceTags' rds arm decoded [emulator.RDSDBInstance] whatever the key named and stored
// the result back, so tagging a cluster through the tagging API replaced the cluster record with
// an instance-shaped husk: Endpoint, ReaderEndpoint, MasterUsername and Port are members
// RDSDBInstance does not carry under those names, so DescribeDBClusters reported them empty
// afterwards. Nothing refused, and nothing about the tag looked wrong — which is why this is
// asserted on the *other* members rather than on the tag.
func TestRDSTagging_ATaggedClusterKeepsItsOwnMembers(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)
	arn := createRDSCluster(t, ts, "billing-cluster")

	before := describeRDSCluster(t, ts, "billing-cluster")
	if before.Endpoint == "" || before.ReaderEndpoint == "" || before.MasterUsername == "" || before.Port == 0 {
		t.Fatalf("CreateDBCluster stored an incomplete record, so this test cannot detect a truncation: %+v", before)
	}

	if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
		t.Fatalf("TagResources %s: %+v", arn, failures)
	}

	after := describeRDSCluster(t, ts, "billing-cluster")
	if after != before {
		t.Errorf("after TagResources the cluster reads back as\n\t%+v\nwant it unchanged at\n\t%+v", after, before)
	}
	if got := rdsTags(t, ts, arn)["env"]; got != "test" {
		t.Errorf("the tag did not land: env = %q, want test", got)
	}
}

// TestRDSTagging_ATaggedSubnetGroupKeepsItsOwnMembers is the same assertion for the other shape
// the old arm would have flattened.
func TestRDSTagging_ATaggedSubnetGroupKeepsItsOwnMembers(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)
	arn := createRDSSubnetGroup(t, ts, "billing-subnets")

	before := describeRDSSubnetGroup(t, ts, "billing-subnets")
	if before.VpcID == "" || before.Description == "" || before.Status == "" {
		t.Fatalf("CreateDBSubnetGroup stored an incomplete record: %+v", before)
	}

	if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
		t.Fatalf("TagResources %s: %+v", arn, failures)
	}

	if after := describeRDSSubnetGroup(t, ts, "billing-subnets"); after != before {
		t.Errorf("after TagResources the subnet group reads back as\n\t%+v\nwant\n\t%+v", after, before)
	}
	if got := rdsTags(t, ts, arn)["env"]; got != "test" {
		t.Errorf("the tag did not land: env = %q, want test", got)
	}
}

// TestRDSTagging_ClusterAndSubnetGroupAreReachableFromTheTaggingAPI is the cross-API half: a tag
// written through the Resource Groups Tagging API is readable through RDS's own call, and vice
// versa, for the two types the tagging API's rds arm could not key.
func TestRDSTagging_ClusterAndSubnetGroupAreReachableFromTheTaggingAPI(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		create func(t *testing.T, ts *emulator.TestServer) string
	}{{
		name:   "db cluster",
		create: func(t *testing.T, ts *emulator.TestServer) string { return createRDSCluster(t, ts, "reach-cluster") },
	}, {
		name: "db subnet group",
		create: func(t *testing.T, ts *emulator.TestServer) string {
			return createRDSSubnetGroup(t, ts, "reach-subnets")
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := rdsTagServer(t)
			arn := tc.create(t, ts)

			if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
				t.Fatalf("TagResources %s: %+v", arn, failures)
			}
			if got := rdsTags(t, ts, arn)["env"]; got != "test" {
				t.Errorf("RDS ListTagsForResource env = %q, want test", got)
			}

			// The other direction: a tag RDS wrote is one the tagging API reports.
			rdsQueryOK(t, ts, map[string]string{
				"Action":              "AddTagsToResource",
				"ResourceName":        arn,
				"Tags.member.1.Key":   "team",
				"Tags.member.1.Value": "platform",
			})
			if got := getResourcesTags(t, ts, arn)["team"]; got != "platform" {
				t.Errorf("GetResources team = %q, want platform", got)
			}

			if failures := tagResourcesFailures(t, ts, "UntagResources", arn); len(failures) != 0 {
				t.Fatalf("UntagResources %s: %+v", arn, failures)
			}
			if _, still := rdsTags(t, ts, arn)["env"]; still {
				t.Errorf("UntagResources left env in place on %s", arn)
			}
		})
	}
}

// TestRDSTagging_GetResourcesReportsEveryRDSType asserts the two new scanners run, since a
// resolver arm alone makes a resource taggable by name while leaving it invisible to a caller
// discovering resources — the two halves of #835's criterion for each row.
func TestRDSTagging_GetResourcesReportsEveryRDSType(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)

	instance := createRDSInstance(t, ts, "listed-db")
	cluster := createRDSCluster(t, ts, "listed-cluster")
	subnets := createRDSSubnetGroup(t, ts, "listed-subnets")

	for _, arn := range []string{instance, cluster, subnets} {
		rdsQueryOK(t, ts, map[string]string{
			"Action":              "AddTagsToResource",
			"ResourceName":        arn,
			"Tags.member.1.Key":   "env",
			"Tags.member.1.Value": "test",
		})
	}

	reported := getResourcesARNs(t, ts, "rds")
	for _, arn := range []string{instance, cluster, subnets} {
		if !slices.Contains(reported, arn) {
			t.Errorf("GetResources omitted %s; reported %v", arn, reported)
		}
	}
}

// TestRDSTagging_AMissingResourceNamesItsOwnKind pins the per-kind 404.
//
// Every kind answered DBInstanceNotFound, which tells a caller waiting on a cluster or a
// snapshot that it asked about the wrong sort of thing — and a consumer branching on the code to
// decide whether to keep polling branches wrong. AddTagsToResource and ListTagsForResource
// publish a separate 404 per kind; three of these four are on those lists verbatim and the
// subnet group's is recorded in rds_tags.go as substrate's reading.
func TestRDSTagging_AMissingResourceNamesItsOwnKind(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		arn      string
		wantCode string
	}{{
		name:     "db instance",
		arn:      "arn:aws:rds:us-east-1:" + taggingTestAccount + ":db:no-such-db",
		wantCode: "DBInstanceNotFound",
	}, {
		name:     "db cluster",
		arn:      "arn:aws:rds:us-east-1:" + taggingTestAccount + ":cluster:no-such-cluster",
		wantCode: "DBClusterNotFoundFault",
	}, {
		name:     "db snapshot",
		arn:      "arn:aws:rds:us-east-1:" + taggingTestAccount + ":snapshot:no-such-snapshot",
		wantCode: "DBSnapshotNotFound",
	}, {
		name:     "db subnet group",
		arn:      "arn:aws:rds:us-east-1:" + taggingTestAccount + ":subgrp:no-such-group",
		wantCode: "DBSubnetGroupNotFoundFault",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := rdsTagServer(t)

			// All three operations, because the read side and the write side resolved and
			// reported through separate code paths and so could disagree about the code.
			ops := []map[string]string{
				{"Action": "ListTagsForResource", "ResourceName": tc.arn},
				{"Action": "AddTagsToResource", "ResourceName": tc.arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "test"},
				{"Action": "RemoveTagsFromResource", "ResourceName": tc.arn, "TagKeys.member.1": "env"},
			}
			for _, params := range ops {
				status, body, errCode := rdsQuery(t, ts, taggingTestAccount, params)
				if errCode != tc.wantCode {
					t.Errorf("%s: error code %q, want %s (body %s)", params["Action"], errCode, tc.wantCode, body)
				}
				if status != http.StatusNotFound {
					t.Errorf("%s: status %d, want 404", params["Action"], status)
				}
			}
		})
	}
}

// TestRDSTagging_AnUnsupportedOrMalformedARNIsRefused keeps the new arms from having widened the
// resolver into accepting keys nothing is stored at.
//
// The two cluster look-alikes matter most: `cluster-pg` and `cluster-snapshot` are their own
// resource-type segments in AWS's ARN table, substrate stores neither, and a resolver matching
// them as a prefix of "cluster" would have keyed a DB cluster from an ARN naming something else.
func TestRDSTagging_AnUnsupportedOrMalformedARNIsRefused(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		arn  string
		why  string
	}{{
		name: "parameter group",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":pg:my-params",
		why:  "substrate stores a parameter group, but no ARN reaches its tags and it stores none",
	}, {
		name: "cluster parameter group",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":cluster-pg:my-cluster-params",
		why:  "a distinct resource type, not a cluster; a prefix match would key one",
	}, {
		name: "cluster snapshot",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":cluster-snapshot:my-snap",
		why:  "likewise a distinct resource type substrate does not store",
	}, {
		name: "event subscription",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":es:my-subscription",
		why:  "in AWS's ARN table, not in substrate's state",
	}, {
		name: "no resource name",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":db:",
		why:  "an empty identifier keys the account/Region scope itself",
	}, {
		name: "identifier carrying a path segment",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":db:orders/replica",
		why:  "an RDS identifier holds no /, so the key would carry an extra segment",
	}, {
		name: "too few segments",
		arn:  "arn:aws:rds:us-east-1:" + taggingTestAccount + ":db",
		why:  "no resource segment at all",
	}, {
		name: "not an RDS arn",
		arn:  "arn:aws:ec2:us-east-1:" + taggingTestAccount + ":db:orders",
		why:  "the service segment is checked, so another service's ARN cannot key here",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := rdsTagServer(t)

			status, body, errCode := rdsQuery(t, ts, taggingTestAccount, map[string]string{
				"Action":       "ListTagsForResource",
				"ResourceName": tc.arn,
			})
			if errCode != "InvalidParameterValue" {
				t.Errorf("%s: error code %q, want InvalidParameterValue (%s) (body %s)", tc.arn, errCode, tc.why, body)
			}
			if status != http.StatusBadRequest {
				t.Errorf("%s: status %d, want 400", tc.arn, status)
			}
		})
	}
}

// TestRDSTagging_AnAutomatedSnapshotIdentifierSurvivesTheARN covers the one resource type whose
// identifier legitimately contains a colon.
//
// AWS's ARN table gives an automated snapshot as `snapshot:rds:{name}`, and the extra segment
// belongs to the identifier — an automated snapshot really is named
// "rds:mydb-2019-07-22-07-23". So the snapshot arm must not apply the plain-identifier check the
// other three do, and this asserts that the resolver keeps the whole remainder rather than
// truncating at the second colon.
func TestRDSTagging_AnAutomatedSnapshotIdentifierSurvivesTheARN(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)

	createRDSInstance(t, ts, "auto-src")
	rdsQueryOK(t, ts, map[string]string{
		"Action":               "CreateDBSnapshot",
		"DBSnapshotIdentifier": "rds:auto-src-2026-01-01-00-00",
		"DBInstanceIdentifier": "auto-src",
	})

	arn := "arn:aws:rds:us-east-1:" + taggingTestAccount + ":snapshot:rds:auto-src-2026-01-01-00-00"
	rdsQueryOK(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "test",
	})
	if got := rdsTags(t, ts, arn)["env"]; got != "test" {
		t.Errorf("automated-snapshot ARN did not address its snapshot: env = %q, want test", got)
	}

	// And a manual snapshot of the same trailing name is a different resource, so the automated
	// form cannot be being resolved by dropping its prefix.
	manual := "arn:aws:rds:us-east-1:" + taggingTestAccount + ":snapshot:auto-src-2026-01-01-00-00"
	_, _, errCode := rdsQuery(t, ts, taggingTestAccount, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": manual,
	})
	if errCode != "DBSnapshotNotFound" {
		t.Errorf("manual-form ARN resolved to the automated snapshot: error code %q, want DBSnapshotNotFound", errCode)
	}
}

// TestRDSTagging_TagListIsReportedInKeyOrder pins the response order.
//
// TagList was built by ranging a Go map, so two identical calls could report the same tags in
// different orders and a caller asserting on the body could not replay a recorded run. AWS
// documents no order for TagList — its own sample response renders "owner" before "environment"
// — so sorted-by-key is substrate's reading, taken for the reason sortTagsByKey records.
func TestRDSTagging_TagListIsReportedInKeyOrder(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)
	arn := createRDSCluster(t, ts, "ordered-cluster")

	rdsQueryOK(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        arn,
		"Tags.member.1.Key":   "zebra",
		"Tags.member.1.Value": "3",
		"Tags.member.2.Key":   "apple",
		"Tags.member.2.Value": "1",
		"Tags.member.3.Key":   "mango",
		"Tags.member.3.Value": "2",
	})

	want := []string{"apple", "mango", "zebra"}
	// Twice, because one call cannot distinguish a sorted order from a map order that happened
	// to come out sorted.
	for i := range 2 {
		var got []string
		for _, tag := range rdsTagList(t, ts, arn) {
			got = append(got, tag.Key)
		}
		if !slices.Equal(got, want) {
			t.Errorf("call %d: TagList keys = %v, want %v", i+1, got, want)
		}
	}
}

// TestRDSTagging_AForeignAccountARNDoesNotTagTheCallersCluster is #826 applied to the two arms
// this PR adds, since a new arm building its key from the request context rather than the ARN is
// how that defect keeps returning.
//
// rdsResolveARN takes no *RequestContext at all, so the invariant is structural — but the
// consequence a consumer observes is what this asserts: the caller's own cluster does not change.
func TestRDSTagging_AForeignAccountARNDoesNotTagTheCallersCluster(t *testing.T) {
	t.Parallel()
	ts := rdsTagServer(t)

	own := createRDSCluster(t, ts, "shared-name")
	foreign := strings.Replace(own, taggingTestAccount, taggingForeignAccount, 1)
	if foreign == own {
		t.Fatalf("the foreign ARN is the same as the caller's: %s", own)
	}

	rdsQueryOK(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        own,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "test",
	})

	// A removal aimed at the wrong account is the more damaging direction, so it is asserted.
	_, _, errCode := rdsQuery(t, ts, taggingTestAccount, map[string]string{
		"Action":           "RemoveTagsFromResource",
		"ResourceName":     foreign,
		"TagKeys.member.1": "env",
	})
	if errCode != "DBClusterNotFoundFault" {
		t.Errorf("RemoveTagsFromResource against %s: error code %q, want DBClusterNotFoundFault", foreign, errCode)
	}
	if got := rdsTags(t, ts, own)["env"]; got != "test" {
		t.Errorf("after a foreign-account removal, own cluster env = %q, want it untouched at test", got)
	}
}

// --- read-back helpers -----------------------------------------------------

// rdsClusterView is the subset of a DB cluster this file asserts survives a tag write. It holds
// only members [emulator.RDSDBInstance] does not carry under the same name, since those are the
// ones a decode through the wrong struct dropped.
type rdsClusterView struct {
	Endpoint       string `xml:"Endpoint"`
	ReaderEndpoint string `xml:"ReaderEndpoint"`
	MasterUsername string `xml:"MasterUsername"`
	Port           int    `xml:"Port"`
}

// describeRDSCluster reads a cluster back through DescribeDBClusters.
func describeRDSCluster(t *testing.T, ts *emulator.TestServer, id string) rdsClusterView {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":              "DescribeDBClusters",
		"DBClusterIdentifier": id,
	})
	var doc struct {
		Clusters []rdsClusterView `xml:"DescribeDBClustersResult>DBClusters>DBCluster"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode DescribeDBClusters: %v (body %s)", err, body)
	}
	if len(doc.Clusters) != 1 {
		t.Fatalf("DescribeDBClusters %s returned %d clusters, want 1 (body %s)", id, len(doc.Clusters), body)
	}
	return doc.Clusters[0]
}

// rdsSubnetGroupView is the subset of a DB subnet group asserted to survive a tag write.
type rdsSubnetGroupView struct {
	Description string `xml:"DBSubnetGroupDescription"`
	Status      string `xml:"SubnetGroupStatus"`
	VpcID       string `xml:"VpcId"`
}

// describeRDSSubnetGroup reads a subnet group back through DescribeDBSubnetGroups.
func describeRDSSubnetGroup(t *testing.T, ts *emulator.TestServer, name string) rdsSubnetGroupView {
	t.Helper()
	body := rdsQueryOK(t, ts, map[string]string{
		"Action":            "DescribeDBSubnetGroups",
		"DBSubnetGroupName": name,
	})
	var doc struct {
		Groups []rdsSubnetGroupView `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups>DBSubnetGroup"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode DescribeDBSubnetGroups: %v (body %s)", err, body)
	}
	if len(doc.Groups) != 1 {
		t.Fatalf("DescribeDBSubnetGroups %s returned %d groups, want 1 (body %s)", name, len(doc.Groups), body)
	}
	return doc.Groups[0]
}

// getResourcesMappings runs GetResources with an optional resource-type filter and returns the
// mappings it reports.
func getResourcesMappings(t *testing.T, ts *emulator.TestServer, typeFilter string) []struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
} {
	t.Helper()
	body := map[string]any{}
	if typeFilter != "" {
		body["ResourceTypeFilters"] = []string{typeFilter}
	}
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
			Tags        []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
			} `json:"Tags"`
		} `json:"ResourceTagMappingList"`
	}
	resp := signedRequest(t, ts, taggingTarget, taggingTestAccount, "GetResources", body)
	if status, errCode := decodeAWSResponse(t, resp, &out); errCode != "" || status != http.StatusOK {
		t.Fatalf("GetResources: status %d, error %q", status, errCode)
	}
	return out.ResourceTagMappingList
}

// getResourcesARNs returns just the ARNs GetResources reports for a resource-type filter.
func getResourcesARNs(t *testing.T, ts *emulator.TestServer, typeFilter string) []string {
	t.Helper()
	var out []string
	for _, m := range getResourcesMappings(t, ts, typeFilter) {
		out = append(out, m.ResourceARN)
	}
	return out
}

// getResourcesTags returns the tags GetResources reports for one ARN.
func getResourcesTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	for _, m := range getResourcesMappings(t, ts, "") {
		if m.ResourceARN != arn {
			continue
		}
		tags := map[string]string{}
		for _, tag := range m.Tags {
			tags[tag.Key] = tag.Value
		}
		return tags
	}
	t.Fatalf("GetResources did not report %s", arn)
	return nil
}
