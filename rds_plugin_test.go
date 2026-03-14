package substrate_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newRDSTestServer builds a minimal Server with the RDS plugin registered.
func newRDSTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.RDSPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize rds plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// rdsRequest sends an RDS query-protocol request.
func rdsRequest(t *testing.T, ts *httptest.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build rds request: %v", err)
	}
	req.Host = "rds.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do rds request: %v", err)
	}
	return resp
}

// rdsBody reads and closes the response body, returning it as a string.
func rdsBody(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read rds response body: %v", err)
	}
	return string(b)
}

func TestRDSPlugin_CreateDescribeDeleteDBInstance(t *testing.T) {
	ts := newRDSTestServer(t)

	// CreateDBInstance
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": "mydb",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "mysql",
		"MasterUsername":       "admin",
		"AllocatedStorage":     "20",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBInstance status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "mydb") {
		t.Error("CreateDBInstance response missing instance ID")
	}
	if !strings.Contains(body, "available") {
		t.Error("CreateDBInstance response missing available status")
	}
	// MySQL default port should be present.
	if !strings.Contains(body, "3306") {
		t.Error("CreateDBInstance response missing default MySQL port 3306")
	}

	// DescribeDBInstances — all
	resp = rdsRequest(t, ts, map[string]string{"Action": "DescribeDBInstances"})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBInstances status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "mydb") {
		t.Error("DescribeDBInstances response missing instance")
	}

	// DescribeDBInstances — filter by ID
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBInstances",
		"DBInstanceIdentifier": "mydb",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBInstances filter status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "mydb") {
		t.Error("DescribeDBInstances filter response missing instance")
	}

	// DescribeDBInstances — not found
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBInstances",
		"DBInstanceIdentifier": "nonexistent",
	})
	rdsBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeDBInstances nonexistent should return non-200")
	}

	// StopDBInstance
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "StopDBInstance",
		"DBInstanceIdentifier": "mydb",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StopDBInstance status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "stopped") {
		t.Error("StopDBInstance response missing stopped status")
	}

	// StartDBInstance
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "StartDBInstance",
		"DBInstanceIdentifier": "mydb",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StartDBInstance status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "available") {
		t.Error("StartDBInstance response missing available status")
	}

	// RebootDBInstance
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "RebootDBInstance",
		"DBInstanceIdentifier": "mydb",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RebootDBInstance status %d", resp.StatusCode)
	}

	// ModifyDBInstance
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "ModifyDBInstance",
		"DBInstanceIdentifier": "mydb",
		"DBInstanceClass":      "db.t3.medium",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyDBInstance status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "db.t3.medium") {
		t.Error("ModifyDBInstance response missing updated class")
	}

	// CreateDBSnapshot
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBSnapshot",
		"DBSnapshotIdentifier": "mydb-snap",
		"DBInstanceIdentifier": "mydb",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBSnapshot status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "mydb-snap") {
		t.Error("CreateDBSnapshot response missing snapshot ID")
	}

	// DescribeDBSnapshots
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBSnapshots",
		"DBInstanceIdentifier": "mydb",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSnapshots status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "mydb-snap") {
		t.Error("DescribeDBSnapshots response missing snapshot")
	}

	// DeleteDBSnapshot
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBSnapshot",
		"DBSnapshotIdentifier": "mydb-snap",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBSnapshot status %d", resp.StatusCode)
	}

	// DeleteDBInstance
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBInstance",
		"DBInstanceIdentifier": "mydb",
		"SkipFinalSnapshot":    "true",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBInstance status %d", resp.StatusCode)
	}

	// Verify gone
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBInstances",
		"DBInstanceIdentifier": "mydb",
	})
	rdsBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeDBInstances after delete should return non-200")
	}
}

func TestRDSPlugin_SubnetGroupAndParameterGroup(t *testing.T) {
	ts := newRDSTestServer(t)

	// CreateDBSubnetGroup
	resp := rdsRequest(t, ts, map[string]string{
		"Action":                   "CreateDBSubnetGroup",
		"DBSubnetGroupName":        "my-sg",
		"DBSubnetGroupDescription": "test subnet group",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBSubnetGroup status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-sg") {
		t.Error("CreateDBSubnetGroup response missing name")
	}

	// DescribeDBSubnetGroups
	resp = rdsRequest(t, ts, map[string]string{"Action": "DescribeDBSubnetGroups"})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSubnetGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-sg") {
		t.Error("DescribeDBSubnetGroups response missing subnet group")
	}

	// DeleteDBSubnetGroup
	resp = rdsRequest(t, ts, map[string]string{
		"Action":            "DeleteDBSubnetGroup",
		"DBSubnetGroupName": "my-sg",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBSubnetGroup status %d", resp.StatusCode)
	}

	// CreateDBParameterGroup
	resp = rdsRequest(t, ts, map[string]string{
		"Action":                 "CreateDBParameterGroup",
		"DBParameterGroupName":   "my-pg",
		"DBParameterGroupFamily": "mysql8.0",
		"Description":            "test param group",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBParameterGroup status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-pg") {
		t.Error("CreateDBParameterGroup response missing name")
	}

	// DescribeDBParameterGroups — filter
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBParameterGroups",
		"DBParameterGroupName": "my-pg",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBParameterGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-pg") {
		t.Error("DescribeDBParameterGroups response missing param group")
	}

	// DeleteDBParameterGroup
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBParameterGroup",
		"DBParameterGroupName": "my-pg",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBParameterGroup status %d", resp.StatusCode)
	}
}

func TestRDSPlugin_Tagging(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create an instance first.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": "tagdb",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "postgres",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBInstance status %d, body: %s", resp.StatusCode, body)
	}

	arn := "arn:aws:rds:us-east-1:000000000000:db:tagdb"

	// AddTagsToResource
	resp = rdsRequest(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        arn,
		"Tags.member.1.Key":   "Env",
		"Tags.member.1.Value": "test",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToResource status %d, body: %s", resp.StatusCode, body)
	}

	// ListTagsForResource
	resp = rdsRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": arn,
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "Env") {
		t.Error("ListTagsForResource response missing tag key")
	}

	// RemoveTagsFromResource
	resp = rdsRequest(t, ts, map[string]string{
		"Action":           "RemoveTagsFromResource",
		"ResourceName":     arn,
		"TagKeys.member.1": "Env",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveTagsFromResource status %d, body: %s", resp.StatusCode, body)
	}
}

func TestRDSPlugin_DefaultPorts(t *testing.T) {
	ts := newRDSTestServer(t)

	tests := []struct {
		engine   string
		wantPort string
	}{
		{"mysql", "3306"},
		{"postgres", "5432"},
		{"oracle-se2", "1521"},
		{"sqlserver-se", "1433"},
	}

	for _, tt := range tests {
		t.Run(tt.engine, func(t *testing.T) {
			resp := rdsRequest(t, ts, map[string]string{
				"Action":               "CreateDBInstance",
				"DBInstanceIdentifier": "port-test-" + strings.ReplaceAll(tt.engine, "-", ""),
				"DBInstanceClass":      "db.t3.micro",
				"Engine":               tt.engine,
			})
			body := rdsBody(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("CreateDBInstance %s status %d, body: %s", tt.engine, resp.StatusCode, body)
			}
			if !strings.Contains(body, tt.wantPort) {
				t.Errorf("CreateDBInstance %s: response missing port %s in body: %s", tt.engine, tt.wantPort, body)
			}
		})
	}
}

func TestRDSPlugin_UnknownAction(t *testing.T) {
	ts := newRDSTestServer(t)

	resp := rdsRequest(t, ts, map[string]string{"Action": "NoSuchRDSAction"})
	rdsBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("unknown action should return non-200")
	}
}

func TestRDSPlugin_Pagination(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create multiple instances.
	for i := 0; i < 3; i++ {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": fmt.Sprintf("pg-db-%d", i),
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "mysql",
		})
		rdsBody(t, resp)
	}

	// Describe with MaxRecords=2 to force pagination.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":     "DescribeDBInstances",
		"MaxRecords": "2",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("paginated DescribeDBInstances status %d, body: %s", resp.StatusCode, body)
	}
}

func TestRDSPlugin_ErrorPaths(t *testing.T) {
	ts := newRDSTestServer(t)

	// ModifyDBInstance — not found.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "ModifyDBInstance",
		"DBInstanceIdentifier": "no-such-db",
		"DBInstanceClass":      "db.t3.medium",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("ModifyDBInstance on missing instance should fail")
	}

	// DeleteDBInstance — not found.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBInstance",
		"DBInstanceIdentifier": "no-such-db",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("DeleteDBInstance on missing instance should fail")
	}

	// CreateDBSnapshot — source instance not found.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBSnapshot",
		"DBSnapshotIdentifier": "snap",
		"DBInstanceIdentifier": "no-such-db",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("CreateDBSnapshot on missing instance should fail")
	}

	// DeleteDBSnapshot — not found.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBSnapshot",
		"DBSnapshotIdentifier": "no-such-snap",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("DeleteDBSnapshot on missing snapshot should fail")
	}

	// CreateDBInstance — missing identifier.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":          "CreateDBInstance",
		"DBInstanceClass": "db.t3.micro",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("CreateDBInstance without identifier should fail")
	}

	// StopDBInstance — not found.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "StopDBInstance",
		"DBInstanceIdentifier": "no-such-db",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("StopDBInstance on missing instance should fail")
	}
}

func TestRDSPlugin_DescribeSnapshotsNoFilter(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create instance.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": "snap-db",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "mysql",
	})
	rdsBody(t, resp)

	// Create snapshot.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBSnapshot",
		"DBSnapshotIdentifier": "snap1",
		"DBInstanceIdentifier": "snap-db",
	})
	rdsBody(t, resp)

	// DescribeDBSnapshots — no filter returns all.
	resp = rdsRequest(t, ts, map[string]string{"Action": "DescribeDBSnapshots"})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSnapshots status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "snap1") {
		t.Error("DescribeDBSnapshots should return snapshot")
	}
}

func TestRDSPlugin_DescribeSubnetGroupFilter(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create two subnet groups.
	for _, name := range []string{"sg-alpha", "sg-beta"} {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":                   "CreateDBSubnetGroup",
			"DBSubnetGroupName":        name,
			"DBSubnetGroupDescription": name + " description",
		})
		rdsBody(t, resp)
	}

	// DescribeDBSubnetGroups with filter.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":            "DescribeDBSubnetGroups",
		"DBSubnetGroupName": "sg-alpha",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSubnetGroups filter status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "sg-alpha") {
		t.Error("response should contain sg-alpha")
	}
	if strings.Contains(body, "sg-beta") {
		t.Error("response should not contain sg-beta")
	}
}

// TestRDSPlugin_ModifyAllParams covers AllocatedStorage and MultiAZ branches in modifyDBInstance.
func TestRDSPlugin_ModifyAllParams(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create instance.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": "mod-all",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "mysql",
		"MasterUsername":       "admin",
	})
	rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBInstance status %d", resp.StatusCode)
	}

	// ModifyDBInstance with all modifiable parameters.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "ModifyDBInstance",
		"DBInstanceIdentifier": "mod-all",
		"DBInstanceClass":      "db.t3.medium",
		"EngineVersion":        "8.0.32",
		"AllocatedStorage":     "100",
		"MultiAZ":              "true",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyDBInstance status %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "db.t3.medium") {
		t.Error("modified class should appear in response")
	}

	// Describe to confirm persistence.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBInstances",
		"DBInstanceIdentifier": "mod-all",
	})
	body = rdsBody(t, resp)
	if !strings.Contains(body, "db.t3.medium") {
		t.Error("describe should reflect modified class")
	}
}

// TestRDSPlugin_DescribeSnapshotsByInstance covers the filterInst branch in describeDBSnapshots.
func TestRDSPlugin_DescribeSnapshotsByInstance(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create two instances and their snapshots.
	for _, id := range []string{"inst-a", "inst-b"} {
		resp := rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBInstance",
			"DBInstanceIdentifier": id,
			"DBInstanceClass":      "db.t3.micro",
			"Engine":               "postgres",
			"MasterUsername":       "admin",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBInstance %s status %d", id, resp.StatusCode)
		}
		resp = rdsRequest(t, ts, map[string]string{
			"Action":               "CreateDBSnapshot",
			"DBSnapshotIdentifier": "snap-" + id,
			"DBInstanceIdentifier": id,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateDBSnapshot for %s status %d", id, resp.StatusCode)
		}
	}

	// Filter by instance.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBSnapshots",
		"DBInstanceIdentifier": "inst-a",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSnapshots status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "snap-inst-a") {
		t.Error("should contain inst-a snapshot")
	}
	if strings.Contains(body, "snap-inst-b") {
		t.Error("should not contain inst-b snapshot")
	}

	// Filter by snapshot ID.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DescribeDBSnapshots",
		"DBSnapshotIdentifier": "snap-inst-b",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSnapshots by snap-id status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "snap-inst-b") {
		t.Error("should contain snap-inst-b")
	}
	if strings.Contains(body, "snap-inst-a") {
		t.Error("should not contain snap-inst-a")
	}
}

// TestRDSPlugin_DeleteSubnetAndParamGroups covers deleteDBSubnetGroup / deleteDBParameterGroup success paths.
func TestRDSPlugin_DeleteSubnetAndParamGroups(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create and delete a subnet group.
	resp := rdsRequest(t, ts, map[string]string{
		"Action":                   "CreateDBSubnetGroup",
		"DBSubnetGroupName":        "del-sg",
		"DBSubnetGroupDescription": "to delete",
		"VpcId":                    "vpc-000",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBSubnetGroup status %d", resp.StatusCode)
	}
	resp = rdsRequest(t, ts, map[string]string{
		"Action":            "DeleteDBSubnetGroup",
		"DBSubnetGroupName": "del-sg",
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBSubnetGroup status %d: %s", resp.StatusCode, body)
	}
	// Confirm gone (returns empty list, not error).
	resp = rdsRequest(t, ts, map[string]string{
		"Action":            "DescribeDBSubnetGroups",
		"DBSubnetGroupName": "del-sg",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDBSubnetGroups after delete status %d", resp.StatusCode)
	}
	if strings.Contains(body, "del-sg") {
		t.Error("DescribeDBSubnetGroups should not return deleted group")
	}

	// Create and delete a parameter group.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":                 "CreateDBParameterGroup",
		"DBParameterGroupName":   "del-pg",
		"DBParameterGroupFamily": "mysql8.0",
		"Description":            "to delete",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBParameterGroup status %d", resp.StatusCode)
	}
	resp = rdsRequest(t, ts, map[string]string{
		"Action":               "DeleteDBParameterGroup",
		"DBParameterGroupName": "del-pg",
	})
	body = rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDBParameterGroup status %d: %s", resp.StatusCode, body)
	}
}

// TestRDSPlugin_SnapshotTagging exercises the rdsResolveARN "snapshot" branch
// via ListTagsForResource / AddTagsToResource / RemoveTagsFromResource.
func TestRDSPlugin_SnapshotTagging(t *testing.T) {
	ts := newRDSTestServer(t)

	// Create instance + snapshot.
	rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": "snap-tag-inst",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "mysql",
		"MasterUsername":       "admin",
	})
	resp := rdsRequest(t, ts, map[string]string{
		"Action":               "CreateDBSnapshot",
		"DBSnapshotIdentifier": "snap-tag",
		"DBInstanceIdentifier": "snap-tag-inst",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDBSnapshot status %d", resp.StatusCode)
	}

	snapARN := "arn:aws:rds:us-east-1:000000000000:snapshot:snap-tag"

	// AddTagsToResource on snapshot.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        snapARN,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "test",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToResource on snapshot status %d", resp.StatusCode)
	}

	// ListTagsForResource on snapshot.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": snapARN,
	})
	body := rdsBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource snapshot status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "env") {
		t.Error("snapshot tags should include env key")
	}

	// RemoveTagsFromResource on snapshot.
	resp = rdsRequest(t, ts, map[string]string{
		"Action":           "RemoveTagsFromResource",
		"ResourceName":     snapARN,
		"TagKeys.member.1": "env",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveTagsFromResource snapshot status %d", resp.StatusCode)
	}
}

// TestRDSPlugin_InvalidARN covers the invalid-ARN path in loadTagsByARN.
func TestRDSPlugin_InvalidARN(t *testing.T) {
	ts := newRDSTestServer(t)

	resp := rdsRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": "not-an-arn",
	})
	rdsBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("ListTagsForResource with invalid ARN should return error")
	}
}

// TestRDSPlugin_MissingParams covers missing-parameter error branches.
func TestRDSPlugin_MissingParams(t *testing.T) {
	ts := newRDSTestServer(t)

	cases := []map[string]string{
		{"Action": "ModifyDBInstance"},
		{"Action": "DeleteDBInstance"},
		{"Action": "StartDBInstance"},
		{"Action": "StopDBInstance"},
		{"Action": "RebootDBInstance"},
		{"Action": "CreateDBSnapshot", "DBSnapshotIdentifier": "s"},
		{"Action": "CreateDBSnapshot", "DBInstanceIdentifier": "i"},
		{"Action": "DeleteDBSnapshot"},
		{"Action": "CreateDBSubnetGroup"},
		{"Action": "DeleteDBSubnetGroup"},
		// DescribeDBSubnetGroups returns empty 200 for unknown names — skip that case.
		{"Action": "CreateDBParameterGroup"},
		{"Action": "DeleteDBParameterGroup"},
		// DescribeDBParameterGroups returns empty 200 for unknown names — skip that case.
		{"Action": "ListTagsForResource"},
		{"Action": "AddTagsToResource"},
		{"Action": "RemoveTagsFromResource"},
	}
	for _, params := range cases {
		resp := rdsRequest(t, ts, params)
		rdsBody(t, resp)
		if resp.StatusCode == http.StatusOK {
			t.Errorf("action %s with missing/invalid params should not return 200", params["Action"])
		}
	}
}
