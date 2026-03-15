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

// newEFSTestServer builds a minimal server with the EFS plugin registered.
func newEFSTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.EFSPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize efs plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// efsRequest sends an EFS REST/JSON request and returns the response.
func efsRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal efs request body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build efs request: %v", err)
	}
	req.Host = "elasticfilesystem.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do efs request: %v", err)
	}
	return resp
}

// efsBody reads and closes the response body.
func efsBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read efs response body: %v", err)
	}
	return b
}

func TestEFSPlugin_CreateDescribeDeleteFileSystem(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create a file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken":   "my-token",
		"PerformanceMode": "generalPurpose",
		"Tags":            []map[string]string{{"Key": "Name", "Value": "my-fs"}},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateFileSystem: expected 201, got %d: %s", resp.StatusCode, efsBody(t, resp))
	}
	var created struct {
		FileSystemID   string `json:"FileSystemId"`
		FileSystemArn  string `json:"FileSystemArn"`
		LifeCycleState string `json:"LifeCycleState"`
		Name           string `json:"Name"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.FileSystemID == "" {
		t.Fatal("FileSystemId empty")
	}
	if created.LifeCycleState != "available" {
		t.Errorf("expected available, got %s", created.LifeCycleState)
	}
	if created.Name != "my-fs" {
		t.Errorf("expected Name=my-fs, got %s", created.Name)
	}
	if created.FileSystemArn == "" {
		t.Error("FileSystemArn empty")
	}
	fsID := created.FileSystemID

	// DescribeFileSystems — list all.
	resp2 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/file-systems", nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("DescribeFileSystems list: expected 200, got %d", resp2.StatusCode)
	}
	var list struct {
		FileSystems []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.FileSystems) == 0 {
		t.Fatal("expected at least 1 filesystem")
	}

	// DescribeFileSystems — by path ID.
	resp3 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/file-systems/"+fsID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeFileSystems by ID: expected 200, got %d", resp3.StatusCode)
	}
	var single struct {
		FileSystems []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}
	if err := json.Unmarshal(efsBody(t, resp3), &single); err != nil {
		t.Fatalf("decode single: %v", err)
	}
	if len(single.FileSystems) != 1 || single.FileSystems[0].FileSystemID != fsID {
		t.Errorf("expected fsID %s in response", fsID)
	}

	// DeleteFileSystem.
	resp4 := efsRequest(t, ts, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
	if resp4.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteFileSystem: expected 204, got %d", resp4.StatusCode)
	}
	_ = efsBody(t, resp4)

	// Confirm gone.
	resp5 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/file-systems/"+fsID, nil)
	if resp5.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp5.StatusCode)
	}
	_ = efsBody(t, resp5)
}

func TestEFSPlugin_AccessPoint(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create a file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "ap-test",
	})
	var fs struct {
		FileSystemID string `json:"FileSystemId"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &fs); err != nil {
		t.Fatalf("decode fs: %v", err)
	}

	// Create access point.
	resp2 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/access-points", map[string]interface{}{
		"FileSystemId": fs.FileSystemID,
		"Tags":         []map[string]string{{"Key": "Name", "Value": "my-ap"}},
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateAccessPoint: expected 200, got %d: %s", resp2.StatusCode, efsBody(t, resp2))
	}
	var ap struct {
		AccessPointID  string `json:"AccessPointId"`
		AccessPointArn string `json:"AccessPointArn"`
		FileSystemID   string `json:"FileSystemId"`
		LifeCycleState string `json:"LifeCycleState"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &ap); err != nil {
		t.Fatalf("decode ap: %v", err)
	}
	if ap.AccessPointID == "" {
		t.Fatal("AccessPointId empty")
	}
	if ap.FileSystemID != fs.FileSystemID {
		t.Errorf("FileSystemId mismatch: got %s", ap.FileSystemID)
	}
	if ap.LifeCycleState != "available" {
		t.Errorf("expected available, got %s", ap.LifeCycleState)
	}

	// DescribeAccessPoints — filter by FS ID.
	resp3 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/access-points?FileSystemId="+fs.FileSystemID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeAccessPoints: expected 200, got %d", resp3.StatusCode)
	}
	var apList struct {
		AccessPoints []struct {
			AccessPointID string `json:"AccessPointId"`
		} `json:"AccessPoints"`
	}
	if err := json.Unmarshal(efsBody(t, resp3), &apList); err != nil {
		t.Fatalf("decode apList: %v", err)
	}
	if len(apList.AccessPoints) != 1 {
		t.Fatalf("expected 1 access point, got %d", len(apList.AccessPoints))
	}

	// DeleteAccessPoint.
	resp4 := efsRequest(t, ts, http.MethodDelete, "/2015-02-01/access-points/"+ap.AccessPointID, nil)
	if resp4.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteAccessPoint: expected 204, got %d", resp4.StatusCode)
	}
	_ = efsBody(t, resp4)
}

func TestEFSPlugin_MountTarget(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "mt-test",
	})
	var fs struct {
		FileSystemID         string `json:"FileSystemId"`
		NumberOfMountTargets int    `json:"NumberOfMountTargets"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &fs); err != nil {
		t.Fatalf("decode fs: %v", err)
	}

	// Create mount target.
	resp2 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/mount-targets", map[string]interface{}{
		"FileSystemId": fs.FileSystemID,
		"SubnetId":     "subnet-abc123",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateMountTarget: expected 200, got %d: %s", resp2.StatusCode, efsBody(t, resp2))
	}
	var mt struct {
		MountTargetID  string `json:"MountTargetId"`
		FileSystemID   string `json:"FileSystemId"`
		SubnetID       string `json:"SubnetId"`
		LifeCycleState string `json:"LifeCycleState"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &mt); err != nil {
		t.Fatalf("decode mt: %v", err)
	}
	if mt.MountTargetID == "" {
		t.Fatal("MountTargetId empty")
	}
	if mt.LifeCycleState != "available" {
		t.Errorf("expected available, got %s", mt.LifeCycleState)
	}

	// DescribeMountTargets by FileSystemId.
	resp3 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/mount-targets?FileSystemId="+fs.FileSystemID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeMountTargets: expected 200, got %d", resp3.StatusCode)
	}
	var mtList struct {
		MountTargets []struct {
			MountTargetID string `json:"MountTargetId"`
		} `json:"MountTargets"`
	}
	if err := json.Unmarshal(efsBody(t, resp3), &mtList); err != nil {
		t.Fatalf("decode mtList: %v", err)
	}
	if len(mtList.MountTargets) != 1 {
		t.Fatalf("expected 1 mount target, got %d", len(mtList.MountTargets))
	}

	// Verify mount target count incremented on FS.
	resp4 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/file-systems/"+fs.FileSystemID, nil)
	var fsUpdated struct {
		FileSystems []struct {
			NumberOfMountTargets int `json:"NumberOfMountTargets"`
		} `json:"FileSystems"`
	}
	if err := json.Unmarshal(efsBody(t, resp4), &fsUpdated); err != nil {
		t.Fatalf("decode fsUpdated: %v", err)
	}
	if len(fsUpdated.FileSystems) == 1 && fsUpdated.FileSystems[0].NumberOfMountTargets != 1 {
		t.Errorf("expected NumberOfMountTargets=1, got %d", fsUpdated.FileSystems[0].NumberOfMountTargets)
	}

	// DeleteMountTarget.
	resp5 := efsRequest(t, ts, http.MethodDelete, "/2015-02-01/mount-targets/"+mt.MountTargetID, nil)
	if resp5.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteMountTarget: expected 204, got %d", resp5.StatusCode)
	}
	_ = efsBody(t, resp5)
}

func TestEFSPlugin_TagResourceAndList(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "tag-test",
	})
	var fs struct {
		FileSystemID string `json:"FileSystemId"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &fs); err != nil {
		t.Fatalf("decode fs: %v", err)
	}

	// TagResource.
	resp2 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/resource-tags/"+fs.FileSystemID, map[string]interface{}{
		"Tags": []map[string]string{
			{"Key": "Env", "Value": "test"},
			{"Key": "Owner", "Value": "alice"},
		},
	})
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("TagResource: expected 204, got %d: %s", resp2.StatusCode, efsBody(t, resp2))
	}
	_ = efsBody(t, resp2)

	// ListTagsForResource.
	resp3 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/resource-tags/"+fs.FileSystemID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource: expected 200, got %d", resp3.StatusCode)
	}
	var tagList struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(efsBody(t, resp3), &tagList); err != nil {
		t.Fatalf("decode tags: %v", err)
	}
	if len(tagList.Tags) < 2 {
		t.Fatalf("expected >=2 tags, got %d", len(tagList.Tags))
	}

	// UntagResource — remove Owner.
	resp4 := efsRequest(t, ts, http.MethodDelete, "/2015-02-01/resource-tags/"+fs.FileSystemID+"?tagKeys=Owner", nil)
	if resp4.StatusCode != http.StatusNoContent {
		t.Fatalf("UntagResource: expected 204, got %d: %s", resp4.StatusCode, efsBody(t, resp4))
	}
	_ = efsBody(t, resp4)

	// Verify Owner tag removed.
	resp5 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/resource-tags/"+fs.FileSystemID, nil)
	var tagList2 struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(efsBody(t, resp5), &tagList2); err != nil {
		t.Fatalf("decode tags2: %v", err)
	}
	for _, tag := range tagList2.Tags {
		if tag.Key == "Owner" {
			t.Error("Owner tag should have been removed")
		}
	}
}

func TestEFSPlugin_AccessPointTagging(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "aptag-test",
	})
	var fs struct {
		FileSystemID string `json:"FileSystemId"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &fs); err != nil {
		t.Fatalf("decode fs: %v", err)
	}

	// Create access point.
	resp2 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/access-points", map[string]interface{}{
		"FileSystemId": fs.FileSystemID,
	})
	var ap struct {
		AccessPointID string `json:"AccessPointId"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &ap); err != nil {
		t.Fatalf("decode ap: %v", err)
	}

	// TagResource on access point.
	resp3 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/resource-tags/"+ap.AccessPointID, map[string]interface{}{
		"Tags": []map[string]string{{"Key": "Team", "Value": "infra"}},
	})
	if resp3.StatusCode != http.StatusNoContent {
		t.Fatalf("TagResource (AP): expected 204, got %d: %s", resp3.StatusCode, efsBody(t, resp3))
	}
	_ = efsBody(t, resp3)

	// ListTagsForResource on access point.
	resp4 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/resource-tags/"+ap.AccessPointID, nil)
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource (AP): expected 200, got %d: %s", resp4.StatusCode, efsBody(t, resp4))
	}
	var tagList struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(efsBody(t, resp4), &tagList); err != nil {
		t.Fatalf("decode tags: %v", err)
	}
	found := false
	for _, tag := range tagList.Tags {
		if tag.Key == "Team" {
			found = true
		}
	}
	if !found {
		t.Error("expected Team tag")
	}

	// DescribeAccessPoints by path ID.
	resp5 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/access-points/"+ap.AccessPointID, nil)
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("DescribeAccessPoints by ID: expected 200, got %d", resp5.StatusCode)
	}
	var apSingle struct {
		AccessPoints []struct {
			AccessPointID string `json:"AccessPointId"`
		} `json:"AccessPoints"`
	}
	if err := json.Unmarshal(efsBody(t, resp5), &apSingle); err != nil {
		t.Fatalf("decode apSingle: %v", err)
	}
	if len(apSingle.AccessPoints) != 1 {
		t.Fatalf("expected 1 AP, got %d", len(apSingle.AccessPoints))
	}
}

func TestEFSPlugin_DescribeMountTargetsByID(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create FS.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "mt-id-test",
	})
	var fs struct {
		FileSystemID string `json:"FileSystemId"`
	}
	_ = json.Unmarshal(efsBody(t, resp), &fs)

	// Create mount target.
	resp2 := efsRequest(t, ts, http.MethodPost, "/2015-02-01/mount-targets", map[string]interface{}{
		"FileSystemId": fs.FileSystemID,
		"SubnetId":     "subnet-xyz789",
	})
	var mt struct {
		MountTargetID string `json:"MountTargetId"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &mt); err != nil {
		t.Fatalf("decode mt: %v", err)
	}

	// DescribeMountTargets by MountTargetId.
	resp3 := efsRequest(t, ts, http.MethodGet, "/2015-02-01/mount-targets?MountTargetId="+mt.MountTargetID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeMountTargets by ID: expected 200, got %d", resp3.StatusCode)
	}
	var mtList struct {
		MountTargets []struct {
			MountTargetID string `json:"MountTargetId"`
		} `json:"MountTargets"`
	}
	if err := json.Unmarshal(efsBody(t, resp3), &mtList); err != nil {
		t.Fatalf("decode mtList: %v", err)
	}
	if len(mtList.MountTargets) != 1 {
		t.Fatalf("expected 1 MT, got %d", len(mtList.MountTargets))
	}
}

func TestEFSPlugin_UpdateFileSystem(t *testing.T) {
	ts := newEFSTestServer(t)

	// Create a file system.
	resp := efsRequest(t, ts, http.MethodPost, "/2015-02-01/file-systems", map[string]interface{}{
		"CreationToken": "upd-test",
	})
	var fs struct {
		FileSystemID string `json:"FileSystemId"`
	}
	if err := json.Unmarshal(efsBody(t, resp), &fs); err != nil {
		t.Fatalf("decode fs: %v", err)
	}

	// UpdateFileSystem — change throughput mode.
	resp2 := efsRequest(t, ts, http.MethodPut, "/2015-02-01/file-systems/"+fs.FileSystemID, map[string]interface{}{
		"ThroughputMode": "provisioned",
	})
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("UpdateFileSystem: expected 202, got %d: %s", resp2.StatusCode, efsBody(t, resp2))
	}
	var updated struct {
		ThroughputMode string `json:"ThroughputMode"`
	}
	if err := json.Unmarshal(efsBody(t, resp2), &updated); err != nil {
		t.Fatalf("decode updated: %v", err)
	}
	if updated.ThroughputMode != "provisioned" {
		t.Errorf("expected ThroughputMode=provisioned, got %s", updated.ThroughputMode)
	}
}
