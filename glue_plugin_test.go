package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newGlueTestServer builds a minimal server with the Glue plugin registered.
func newGlueTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.GluePlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize glue plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// glueRequest sends a Glue JSON-target request and returns the response.
func glueRequest(t *testing.T, ts *httptest.Server, op string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal glue request body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build glue request: %v", err)
	}
	req.Host = "glue.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSGlue."+op)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do glue request %s: %v", op, err)
	}
	return resp
}

// glueBody reads and closes the response body.
func glueBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read glue response body: %v", err)
	}
	return b
}

func TestGluePlugin_DatabaseCRUD(t *testing.T) {
	ts := newGlueTestServer(t)

	// CreateDatabase.
	resp := glueRequest(t, ts, "CreateDatabase", map[string]interface{}{
		"DatabaseInput": map[string]interface{}{
			"Name":        "mydb",
			"Description": "test database",
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: expected 200, got %d: %s", resp.StatusCode, glueBody(t, resp))
	}
	_ = glueBody(t, resp)

	// GetDatabase.
	resp2 := glueRequest(t, ts, "GetDatabase", map[string]interface{}{"Name": "mydb"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GetDatabase: expected 200, got %d", resp2.StatusCode)
	}
	var getDB struct {
		Database struct {
			Name        string `json:"Name"`
			Description string `json:"Description"`
			Arn         string `json:"Arn"`
		} `json:"Database"`
	}
	if err := json.Unmarshal(glueBody(t, resp2), &getDB); err != nil {
		t.Fatalf("decode GetDatabase: %v", err)
	}
	if getDB.Database.Name != "mydb" {
		t.Errorf("expected Name=mydb, got %s", getDB.Database.Name)
	}
	if getDB.Database.Arn == "" {
		t.Error("Arn empty")
	}

	// GetDatabases.
	resp3 := glueRequest(t, ts, "GetDatabases", map[string]interface{}{})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GetDatabases: expected 200, got %d", resp3.StatusCode)
	}
	var listDB struct {
		DatabaseList []struct {
			Name string `json:"Name"`
		} `json:"DatabaseList"`
	}
	if err := json.Unmarshal(glueBody(t, resp3), &listDB); err != nil {
		t.Fatalf("decode GetDatabases: %v", err)
	}
	if len(listDB.DatabaseList) == 0 {
		t.Fatal("expected at least 1 database")
	}

	// UpdateDatabase.
	resp4 := glueRequest(t, ts, "UpdateDatabase", map[string]interface{}{
		"Name": "mydb",
		"DatabaseInput": map[string]interface{}{
			"Description": "updated description",
		},
	})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("UpdateDatabase: expected 200, got %d", resp4.StatusCode)
	}
	_ = glueBody(t, resp4)

	// DeleteDatabase.
	resp5 := glueRequest(t, ts, "DeleteDatabase", map[string]interface{}{"Name": "mydb"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDatabase: expected 200, got %d", resp5.StatusCode)
	}
	_ = glueBody(t, resp5)

	// Confirm gone.
	resp6 := glueRequest(t, ts, "GetDatabase", map[string]interface{}{"Name": "mydb"})
	if resp6.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp6.StatusCode)
	}
	_ = glueBody(t, resp6)
}

func TestGluePlugin_TableCRUD(t *testing.T) {
	ts := newGlueTestServer(t)

	// Create a database.
	resp := glueRequest(t, ts, "CreateDatabase", map[string]interface{}{
		"DatabaseInput": map[string]interface{}{"Name": "testdb"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: %d", resp.StatusCode)
	}
	_ = glueBody(t, resp)

	// CreateTable.
	resp2 := glueRequest(t, ts, "CreateTable", map[string]interface{}{
		"DatabaseName": "testdb",
		"TableInput": map[string]interface{}{
			"Name":        "mytable",
			"Description": "test table",
			"TableType":   "EXTERNAL_TABLE",
		},
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable: expected 200, got %d: %s", resp2.StatusCode, glueBody(t, resp2))
	}
	_ = glueBody(t, resp2)

	// GetTable.
	resp3 := glueRequest(t, ts, "GetTable", map[string]interface{}{"DatabaseName": "testdb", "Name": "mytable"})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GetTable: expected 200, got %d", resp3.StatusCode)
	}
	var tbl struct {
		Table struct {
			Name         string `json:"Name"`
			DatabaseName string `json:"DatabaseName"`
			Arn          string `json:"Arn"`
		} `json:"Table"`
	}
	if err := json.Unmarshal(glueBody(t, resp3), &tbl); err != nil {
		t.Fatalf("decode GetTable: %v", err)
	}
	if tbl.Table.Name != "mytable" {
		t.Errorf("expected Name=mytable, got %s", tbl.Table.Name)
	}

	// GetTables.
	resp4 := glueRequest(t, ts, "GetTables", map[string]interface{}{"DatabaseName": "testdb"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("GetTables: expected 200, got %d", resp4.StatusCode)
	}
	var tableList struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	if err := json.Unmarshal(glueBody(t, resp4), &tableList); err != nil {
		t.Fatalf("decode GetTables: %v", err)
	}
	if len(tableList.TableList) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tableList.TableList))
	}

	// DeleteTable.
	resp5 := glueRequest(t, ts, "DeleteTable", map[string]interface{}{"DatabaseName": "testdb", "Name": "mytable"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("DeleteTable: expected 200, got %d", resp5.StatusCode)
	}
	_ = glueBody(t, resp5)
}

func TestGluePlugin_CrawlerCRUD(t *testing.T) {
	ts := newGlueTestServer(t)

	// Create a database first.
	resp := glueRequest(t, ts, "CreateDatabase", map[string]interface{}{
		"DatabaseInput": map[string]interface{}{"Name": "crawlerdb"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: %d", resp.StatusCode)
	}
	_ = glueBody(t, resp)

	// CreateCrawler.
	resp2 := glueRequest(t, ts, "CreateCrawler", map[string]interface{}{
		"Name":         "my-crawler",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
		"DatabaseName": "crawlerdb",
		"Description":  "test crawler",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateCrawler: expected 200, got %d: %s", resp2.StatusCode, glueBody(t, resp2))
	}
	_ = glueBody(t, resp2)

	// GetCrawler.
	resp3 := glueRequest(t, ts, "GetCrawler", map[string]interface{}{"Name": "my-crawler"})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GetCrawler: expected 200, got %d", resp3.StatusCode)
	}
	var crawlerResp struct {
		Crawler struct {
			Name  string `json:"Name"`
			State string `json:"State"`
			Arn   string `json:"Arn"`
		} `json:"Crawler"`
	}
	if err := json.Unmarshal(glueBody(t, resp3), &crawlerResp); err != nil {
		t.Fatalf("decode GetCrawler: %v", err)
	}
	if crawlerResp.Crawler.State != "READY" {
		t.Errorf("expected READY, got %s", crawlerResp.Crawler.State)
	}

	// StartCrawler — deterministic no-op.
	resp4 := glueRequest(t, ts, "StartCrawler", map[string]interface{}{"Name": "my-crawler"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("StartCrawler: expected 200, got %d", resp4.StatusCode)
	}
	_ = glueBody(t, resp4)

	// StopCrawler — deterministic no-op.
	resp5 := glueRequest(t, ts, "StopCrawler", map[string]interface{}{"Name": "my-crawler"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("StopCrawler: expected 200, got %d", resp5.StatusCode)
	}
	_ = glueBody(t, resp5)

	// DeleteCrawler.
	resp6 := glueRequest(t, ts, "DeleteCrawler", map[string]interface{}{"Name": "my-crawler"})
	if resp6.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCrawler: expected 200, got %d", resp6.StatusCode)
	}
	_ = glueBody(t, resp6)
}

func TestGluePlugin_JobAndJobRun(t *testing.T) {
	ts := newGlueTestServer(t)

	// CreateJob.
	resp := glueRequest(t, ts, "CreateJob", map[string]interface{}{
		"Name":        "my-etl-job",
		"Role":        "arn:aws:iam::123456789012:role/GlueRole",
		"Description": "ETL job",
		"Command": map[string]interface{}{
			"Name":           "glueetl",
			"ScriptLocation": "s3://my-bucket/script.py",
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateJob: expected 200, got %d: %s", resp.StatusCode, glueBody(t, resp))
	}
	var createJobResp struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(glueBody(t, resp), &createJobResp); err != nil {
		t.Fatalf("decode CreateJob: %v", err)
	}
	if createJobResp.Name != "my-etl-job" {
		t.Errorf("expected Name=my-etl-job, got %s", createJobResp.Name)
	}

	// GetJob.
	resp2 := glueRequest(t, ts, "GetJob", map[string]interface{}{"JobName": "my-etl-job"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GetJob: expected 200, got %d", resp2.StatusCode)
	}
	var jobResp struct {
		Job struct {
			Name string `json:"Name"`
			Arn  string `json:"Arn"`
		} `json:"Job"`
	}
	if err := json.Unmarshal(glueBody(t, resp2), &jobResp); err != nil {
		t.Fatalf("decode GetJob: %v", err)
	}
	if jobResp.Job.Arn == "" {
		t.Error("Job Arn empty")
	}

	// StartJobRun.
	resp3 := glueRequest(t, ts, "StartJobRun", map[string]interface{}{"JobName": "my-etl-job"})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("StartJobRun: expected 200, got %d: %s", resp3.StatusCode, glueBody(t, resp3))
	}
	var runResp struct {
		JobRunID string `json:"JobRunId"`
	}
	if err := json.Unmarshal(glueBody(t, resp3), &runResp); err != nil {
		t.Fatalf("decode StartJobRun: %v", err)
	}
	if runResp.JobRunID == "" {
		t.Fatal("JobRunID empty")
	}

	// GetJobRun — should be SUCCEEDED.
	resp4 := glueRequest(t, ts, "GetJobRun", map[string]interface{}{
		"JobName": "my-etl-job",
		"RunId":   runResp.JobRunID,
	})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("GetJobRun: expected 200, got %d", resp4.StatusCode)
	}
	var jobRunResp struct {
		JobRun struct {
			JobRunState string `json:"JobRunState"`
			ID          string `json:"Id"`
		} `json:"JobRun"`
	}
	if err := json.Unmarshal(glueBody(t, resp4), &jobRunResp); err != nil {
		t.Fatalf("decode GetJobRun: %v", err)
	}
	if jobRunResp.JobRun.JobRunState != "SUCCEEDED" {
		t.Errorf("expected SUCCEEDED, got %s", jobRunResp.JobRun.JobRunState)
	}

	// GetJobRuns.
	resp5 := glueRequest(t, ts, "GetJobRuns", map[string]interface{}{"JobName": "my-etl-job"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("GetJobRuns: expected 200, got %d", resp5.StatusCode)
	}
	var runsResp struct {
		JobRuns []struct {
			ID string `json:"Id"`
		} `json:"JobRuns"`
	}
	if err := json.Unmarshal(glueBody(t, resp5), &runsResp); err != nil {
		t.Fatalf("decode GetJobRuns: %v", err)
	}
	if len(runsResp.JobRuns) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runsResp.JobRuns))
	}

	// DeleteJob.
	resp6 := glueRequest(t, ts, "DeleteJob", map[string]interface{}{"JobName": "my-etl-job"})
	if resp6.StatusCode != http.StatusOK {
		t.Fatalf("DeleteJob: expected 200, got %d", resp6.StatusCode)
	}
	_ = glueBody(t, resp6)
}

func TestGluePlugin_Connection(t *testing.T) {
	ts := newGlueTestServer(t)

	// CreateConnection.
	resp := glueRequest(t, ts, "CreateConnection", map[string]interface{}{
		"ConnectionInput": map[string]interface{}{
			"Name":           "my-jdbc",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]interface{}{
				"JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
			},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateConnection: expected 200, got %d: %s", resp.StatusCode, glueBody(t, resp))
	}
	_ = glueBody(t, resp)

	// GetConnection.
	resp2 := glueRequest(t, ts, "GetConnection", map[string]interface{}{"Name": "my-jdbc"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GetConnection: expected 200, got %d", resp2.StatusCode)
	}
	var connResp struct {
		Connection struct {
			Name           string `json:"Name"`
			ConnectionType string `json:"ConnectionType"`
		} `json:"Connection"`
	}
	if err := json.Unmarshal(glueBody(t, resp2), &connResp); err != nil {
		t.Fatalf("decode GetConnection: %v", err)
	}
	if connResp.Connection.ConnectionType != "JDBC" {
		t.Errorf("expected JDBC, got %s", connResp.Connection.ConnectionType)
	}

	// GetConnections.
	resp3 := glueRequest(t, ts, "GetConnections", map[string]interface{}{})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GetConnections: expected 200, got %d", resp3.StatusCode)
	}
	var connList struct {
		ConnectionList []struct {
			Name string `json:"Name"`
		} `json:"ConnectionList"`
	}
	if err := json.Unmarshal(glueBody(t, resp3), &connList); err != nil {
		t.Fatalf("decode GetConnections: %v", err)
	}
	if len(connList.ConnectionList) != 1 {
		t.Fatalf("expected 1 connection, got %d", len(connList.ConnectionList))
	}

	// DeleteConnection.
	resp4 := glueRequest(t, ts, "DeleteConnection", map[string]interface{}{"ConnectionName": "my-jdbc"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("DeleteConnection: expected 200, got %d", resp4.StatusCode)
	}
	_ = glueBody(t, resp4)
}

func TestGluePlugin_UpdateOperations(t *testing.T) {
	ts := newGlueTestServer(t)

	// Create database.
	r := glueRequest(t, ts, "CreateDatabase", map[string]interface{}{
		"DatabaseInput": map[string]interface{}{"Name": "upd-db"},
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: expected 200, got %d: %s", r.StatusCode, glueBody(t, r))
	}
	_ = glueBody(t, r)

	// UpdateDatabase.
	r2 := glueRequest(t, ts, "UpdateDatabase", map[string]interface{}{
		"Name": "upd-db",
		"DatabaseInput": map[string]interface{}{
			"Description": "updated",
		},
	})
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("UpdateDatabase: expected 200, got %d: %s", r2.StatusCode, glueBody(t, r2))
	}
	_ = glueBody(t, r2)

	// Create table.
	r3 := glueRequest(t, ts, "CreateTable", map[string]interface{}{
		"DatabaseName": "upd-db",
		"TableInput":   map[string]interface{}{"Name": "upd-table"},
	})
	if r3.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable: %d", r3.StatusCode)
	}
	_ = glueBody(t, r3)

	// UpdateTable.
	r4 := glueRequest(t, ts, "UpdateTable", map[string]interface{}{
		"DatabaseName": "upd-db",
		"TableInput":   map[string]interface{}{"Name": "upd-table", "Description": "updated"},
	})
	if r4.StatusCode != http.StatusOK {
		t.Fatalf("UpdateTable: expected 200, got %d: %s", r4.StatusCode, glueBody(t, r4))
	}
	_ = glueBody(t, r4)

	// Create job.
	r5 := glueRequest(t, ts, "CreateJob", map[string]interface{}{
		"Name": "upd-job",
		"Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]interface{}{
			"Name": "glueetl",
		},
	})
	if r5.StatusCode != http.StatusOK {
		t.Fatalf("CreateJob: %d", r5.StatusCode)
	}
	_ = glueBody(t, r5)

	// UpdateJob.
	r6 := glueRequest(t, ts, "UpdateJob", map[string]interface{}{
		"JobName": "upd-job",
		"JobUpdate": map[string]interface{}{
			"Description": "updated",
		},
	})
	if r6.StatusCode != http.StatusOK {
		t.Fatalf("UpdateJob: expected 200, got %d: %s", r6.StatusCode, glueBody(t, r6))
	}
	_ = glueBody(t, r6)

	// GetJobs.
	r7 := glueRequest(t, ts, "GetJobs", map[string]interface{}{})
	if r7.StatusCode != http.StatusOK {
		t.Fatalf("GetJobs: expected 200, got %d", r7.StatusCode)
	}
	var jobList struct {
		Jobs []struct {
			Name string `json:"Name"`
		} `json:"Jobs"`
	}
	if err := json.Unmarshal(glueBody(t, r7), &jobList); err != nil {
		t.Fatalf("decode GetJobs: %v", err)
	}
	if len(jobList.Jobs) == 0 {
		t.Fatal("expected at least 1 job")
	}

	// Create crawler.
	r8 := glueRequest(t, ts, "CreateCrawler", map[string]interface{}{
		"Name":         "upd-crawler",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
		"DatabaseName": "upd-db",
	})
	if r8.StatusCode != http.StatusOK {
		t.Fatalf("CreateCrawler: %d", r8.StatusCode)
	}
	_ = glueBody(t, r8)

	// GetCrawlers.
	r9 := glueRequest(t, ts, "GetCrawlers", map[string]interface{}{})
	if r9.StatusCode != http.StatusOK {
		t.Fatalf("GetCrawlers: expected 200, got %d", r9.StatusCode)
	}
	var crawlerList struct {
		Crawlers []struct {
			Name string `json:"Name"`
		} `json:"Crawlers"`
	}
	if err := json.Unmarshal(glueBody(t, r9), &crawlerList); err != nil {
		t.Fatalf("decode GetCrawlers: %v", err)
	}
	if len(crawlerList.Crawlers) == 0 {
		t.Fatal("expected at least 1 crawler")
	}

	// UpdateCrawler.
	r10 := glueRequest(t, ts, "UpdateCrawler", map[string]interface{}{
		"Name":        "upd-crawler",
		"Description": "updated",
	})
	if r10.StatusCode != http.StatusOK {
		t.Fatalf("UpdateCrawler: expected 200, got %d: %s", r10.StatusCode, glueBody(t, r10))
	}
	_ = glueBody(t, r10)

	// UpdateConnection.
	cr := glueRequest(t, ts, "CreateConnection", map[string]interface{}{
		"ConnectionInput": map[string]interface{}{
			"Name":           "upd-conn",
			"ConnectionType": "JDBC",
		},
	})
	if cr.StatusCode != http.StatusOK {
		t.Fatalf("CreateConnection: %d", cr.StatusCode)
	}
	_ = glueBody(t, cr)
	r11 := glueRequest(t, ts, "UpdateConnection", map[string]interface{}{
		"Name": "upd-conn",
		"ConnectionInput": map[string]interface{}{
			"Name":           "upd-conn",
			"ConnectionType": "S3",
		},
	})
	if r11.StatusCode != http.StatusOK {
		t.Fatalf("UpdateConnection: expected 200, got %d: %s", r11.StatusCode, glueBody(t, r11))
	}
	_ = glueBody(t, r11)
}

func TestGluePlugin_TaggingOperations(t *testing.T) {
	ts := newGlueTestServer(t)

	// Create a job to tag.
	r := glueRequest(t, ts, "CreateJob", map[string]interface{}{
		"Name": "tag-job",
		"Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]interface{}{
			"Name": "glueetl",
		},
		"Tags": map[string]string{"Env": "test"},
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("CreateJob: %d: %s", r.StatusCode, glueBody(t, r))
	}
	_ = glueBody(t, r)
	jobARN := "arn:aws:glue:us-east-1:000000000000:job/tag-job"

	// GetTags.
	r2 := glueRequest(t, ts, "GetTags", map[string]interface{}{
		"ResourceArn": jobARN,
	})
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("GetTags: expected 200, got %d: %s", r2.StatusCode, glueBody(t, r2))
	}
	var tags struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(glueBody(t, r2), &tags); err != nil {
		t.Fatalf("decode GetTags: %v", err)
	}
	if tags.Tags["Env"] != "test" {
		t.Errorf("expected Env=test, got %v", tags.Tags)
	}

	// TagResource.
	r3 := glueRequest(t, ts, "TagResource", map[string]interface{}{
		"ResourceArn": jobARN,
		"TagsToAdd":   map[string]string{"Owner": "alice"},
	})
	if r3.StatusCode != http.StatusOK {
		t.Fatalf("TagResource: expected 200, got %d: %s", r3.StatusCode, glueBody(t, r3))
	}
	_ = glueBody(t, r3)

	// Verify tag added.
	r4 := glueRequest(t, ts, "GetTags", map[string]interface{}{"ResourceArn": jobARN})
	var tags2 struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(glueBody(t, r4), &tags2); err != nil {
		t.Fatalf("decode GetTags2: %v", err)
	}
	if tags2.Tags["Owner"] != "alice" {
		t.Errorf("expected Owner=alice, got %v", tags2.Tags)
	}

	// UntagResource.
	r5 := glueRequest(t, ts, "UntagResource", map[string]interface{}{
		"ResourceArn":  jobARN,
		"TagsToRemove": []string{"Env"},
	})
	if r5.StatusCode != http.StatusOK {
		t.Fatalf("UntagResource: expected 200, got %d: %s", r5.StatusCode, glueBody(t, r5))
	}
	_ = glueBody(t, r5)

	// Verify tag removed.
	r6 := glueRequest(t, ts, "GetTags", map[string]interface{}{"ResourceArn": jobARN})
	var tags3 struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(glueBody(t, r6), &tags3); err != nil {
		t.Fatalf("decode GetTags3: %v", err)
	}
	if _, ok := tags3.Tags["Env"]; ok {
		t.Error("Env tag should have been removed")
	}
}
