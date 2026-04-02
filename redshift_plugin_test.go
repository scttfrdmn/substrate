package substrate_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupRedshiftPlugin(t *testing.T) (*substrate.RedshiftPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.RedshiftPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("RedshiftPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-redshift-1",
	}
}

func redshiftRequest(t *testing.T, action string, params map[string]string) *substrate.AWSRequest {
	t.Helper()
	if params == nil {
		params = map[string]string{}
	}
	return &substrate.AWSRequest{
		Service:   "redshift",
		Operation: action,
		Path:      "/",
		Headers:   map[string]string{},
		Body:      nil,
		Params:    params,
	}
}

func TestRedshiftPlugin_ClusterCRUD(t *testing.T) {
	p, ctx := setupRedshiftPlugin(t)

	// CreateCluster.
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
		"ClusterIdentifier":  "my-cluster",
		"NodeType":           "dc2.large",
		"MasterUsername":     "admin",
		"MasterUserPassword": "Password123!",
		"DBName":             "mydb",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	// Parse XML response.
	var createResp struct {
		XMLName xml.Name `xml:"CreateClusterResult"`
		Cluster struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
			ClusterStatus     string `xml:"ClusterStatus"`
			NodeType          string `xml:"NodeType"`
			Endpoint          struct {
				Address string `xml:"Address"`
				Port    int    `xml:"Port"`
			} `xml:"Endpoint"`
		} `xml:"Cluster>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &createResp); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResp.Cluster.ClusterIdentifier != "my-cluster" {
		t.Errorf("want ClusterIdentifier=my-cluster, got %q", createResp.Cluster.ClusterIdentifier)
	}
	if createResp.Cluster.ClusterStatus != "available" {
		t.Errorf("want ClusterStatus=available, got %q", createResp.Cluster.ClusterStatus)
	}
	if createResp.Cluster.Endpoint.Port != 5439 {
		t.Errorf("want Port=5439, got %d", createResp.Cluster.Endpoint.Port)
	}
	if createResp.Cluster.Endpoint.Address == "" {
		t.Error("want non-empty endpoint address")
	}

	// Duplicate create.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
		"ClusterIdentifier": "my-cluster",
		"NodeType":          "dc2.large",
		"MasterUsername":    "admin",
	}))
	if err == nil {
		t.Fatal("want error for duplicate cluster, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ClusterAlreadyExistsFault" {
		t.Errorf("want ClusterAlreadyExistsFault, got %v", err)
	}

	// DescribeClusters.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusters", map[string]string{
		"ClusterIdentifier": "my-cluster",
	}))
	if err != nil {
		t.Fatalf("DescribeClusters: %v", err)
	}
	var descResp struct {
		XMLName  xml.Name `xml:"DescribeClustersResult"`
		Clusters []struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
		} `xml:"Clusters>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &descResp); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if len(descResp.Clusters) != 1 {
		t.Fatalf("want 1 cluster, got %d", len(descResp.Clusters))
	}

	// ModifyCluster.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "ModifyCluster", map[string]string{
		"ClusterIdentifier": "my-cluster",
		"NodeType":          "ra3.xlplus",
	}))
	if err != nil {
		t.Fatalf("ModifyCluster: %v", err)
	}
	var modResp struct {
		XMLName xml.Name `xml:"ModifyClusterResult"`
		Cluster struct {
			NodeType string `xml:"NodeType"`
		} `xml:"Cluster>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &modResp); err != nil {
		t.Fatalf("unmarshal modify: %v", err)
	}
	if modResp.Cluster.NodeType != "ra3.xlplus" {
		t.Errorf("want NodeType=ra3.xlplus, got %q", modResp.Cluster.NodeType)
	}

	// DeleteCluster.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "DeleteCluster", map[string]string{
		"ClusterIdentifier": "my-cluster",
	}))
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	// DescribeClusters after delete — should return not found.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusters", map[string]string{
		"ClusterIdentifier": "my-cluster",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ClusterNotFoundFault" {
		t.Errorf("want ClusterNotFoundFault, got %v", err)
	}
}

func TestRedshiftPlugin_DescribeClusters_Filter(t *testing.T) {
	p, ctx := setupRedshiftPlugin(t)

	for _, id := range []string{"cluster-a", "cluster-b"} {
		_, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
			"ClusterIdentifier": id,
			"NodeType":          "dc2.large",
			"MasterUsername":    "admin",
		}))
		if err != nil {
			t.Fatalf("CreateCluster %s: %v", id, err)
		}
	}

	// Describe without filter — should return 2.
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusters", nil))
	if err != nil {
		t.Fatalf("DescribeClusters (all): %v", err)
	}
	var allResp struct {
		XMLName  xml.Name `xml:"DescribeClustersResult"`
		Clusters []struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
		} `xml:"Clusters>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &allResp); err != nil {
		t.Fatalf("unmarshal all: %v", err)
	}
	if len(allResp.Clusters) != 2 {
		t.Errorf("want 2 clusters, got %d", len(allResp.Clusters))
	}

	// Describe with filter — should return 1.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusters", map[string]string{
		"ClusterIdentifier": "cluster-a",
	}))
	if err != nil {
		t.Fatalf("DescribeClusters (filtered): %v", err)
	}
	var filteredResp struct {
		XMLName  xml.Name `xml:"DescribeClustersResult"`
		Clusters []struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
		} `xml:"Clusters>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &filteredResp); err != nil {
		t.Fatalf("unmarshal filtered: %v", err)
	}
	if len(filteredResp.Clusters) != 1 {
		t.Errorf("want 1 cluster with filter, got %d", len(filteredResp.Clusters))
	}
	if len(filteredResp.Clusters) > 0 && filteredResp.Clusters[0].ClusterIdentifier != "cluster-a" {
		t.Errorf("want cluster-a, got %q", filteredResp.Clusters[0].ClusterIdentifier)
	}
}

func TestRedshiftPlugin_ParameterGroup_SubnetGroup(t *testing.T) {
	p, ctx := setupRedshiftPlugin(t)

	// CreateClusterParameterGroup.
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterParameterGroup", map[string]string{
		"ParameterGroupName":   "my-pg",
		"ParameterGroupFamily": "redshift-1.0",
		"Description":          "Test parameter group",
	}))
	if err != nil {
		t.Fatalf("CreateClusterParameterGroup: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	// DescribeClusterParameterGroups.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusterParameterGroups", nil))
	if err != nil {
		t.Fatalf("DescribeClusterParameterGroups: %v", err)
	}
	var pgResp struct {
		XMLName         xml.Name `xml:"DescribeClusterParameterGroupsResult"`
		ParameterGroups []struct {
			ParameterGroupName string `xml:"ParameterGroupName"`
		} `xml:"ParameterGroups>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &pgResp); err != nil {
		t.Fatalf("unmarshal param groups: %v", err)
	}
	if len(pgResp.ParameterGroups) != 1 {
		t.Errorf("want 1 parameter group, got %d", len(pgResp.ParameterGroups))
	}
	if len(pgResp.ParameterGroups) > 0 && pgResp.ParameterGroups[0].ParameterGroupName != "my-pg" {
		t.Errorf("want my-pg, got %q", pgResp.ParameterGroups[0].ParameterGroupName)
	}

	// CreateClusterSubnetGroup.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterSubnetGroup", map[string]string{
		"ClusterSubnetGroupName": "my-sg",
		"Description":            "Test subnet group",
		"VpcId":                  "vpc-12345678",
	}))
	if err != nil {
		t.Fatalf("CreateClusterSubnetGroup: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	// DescribeClusterSubnetGroups.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusterSubnetGroups", nil))
	if err != nil {
		t.Fatalf("DescribeClusterSubnetGroups: %v", err)
	}
	var sgResp struct {
		XMLName      xml.Name `xml:"DescribeClusterSubnetGroupsResult"`
		SubnetGroups []struct {
			ClusterSubnetGroupName string `xml:"ClusterSubnetGroupName"`
		} `xml:"ClusterSubnetGroups>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &sgResp); err != nil {
		t.Fatalf("unmarshal subnet groups: %v", err)
	}
	if len(sgResp.SubnetGroups) != 1 {
		t.Errorf("want 1 subnet group, got %d", len(sgResp.SubnetGroups))
	}
	if len(sgResp.SubnetGroups) > 0 && sgResp.SubnetGroups[0].ClusterSubnetGroupName != "my-sg" {
		t.Errorf("want my-sg, got %q", sgResp.SubnetGroups[0].ClusterSubnetGroupName)
	}
}

func TestRedshiftPlugin_CreateDescribeSnapshot(t *testing.T) {
	p, ctx := setupRedshiftPlugin(t)

	// Create cluster first.
	_, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
		"ClusterIdentifier": "snap-cluster",
		"NodeType":          "dc2.large",
		"MasterUsername":    "admin",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// CreateClusterSnapshot.
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterSnapshot", map[string]string{
		"ClusterIdentifier":  "snap-cluster",
		"SnapshotIdentifier": "my-snapshot",
	}))
	if err != nil {
		t.Fatalf("CreateClusterSnapshot: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	var snapResp struct {
		XMLName  xml.Name `xml:"CreateClusterSnapshotResult"`
		Snapshot struct {
			SnapshotIdentifier string `xml:"SnapshotIdentifier"`
			ClusterIdentifier  string `xml:"ClusterIdentifier"`
			Status             string `xml:"Status"`
		} `xml:"Snapshot"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &snapResp); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snapResp.Snapshot.SnapshotIdentifier != "my-snapshot" {
		t.Errorf("want SnapshotIdentifier=my-snapshot, got %q", snapResp.Snapshot.SnapshotIdentifier)
	}
	if snapResp.Snapshot.Status != "available" {
		t.Errorf("want Status=available, got %q", snapResp.Snapshot.Status)
	}

	// DescribeClusterSnapshots.
	resp, err = p.HandleRequest(ctx, redshiftRequest(t, "DescribeClusterSnapshots", nil))
	if err != nil {
		t.Fatalf("DescribeClusterSnapshots: %v", err)
	}
	var listResp struct {
		XMLName   xml.Name `xml:"DescribeClusterSnapshotsResult"`
		Snapshots []struct {
			SnapshotIdentifier string `xml:"SnapshotIdentifier"`
		} `xml:"Snapshots>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &listResp); err != nil {
		t.Fatalf("unmarshal list snapshots: %v", err)
	}
	if len(listResp.Snapshots) != 1 {
		t.Errorf("want 1 snapshot, got %d", len(listResp.Snapshots))
	}
}

func TestRedshiftPlugin_Errors(t *testing.T) {
	p, ctx := setupRedshiftPlugin(t)

	// ModifyCluster not found.
	_, err := p.HandleRequest(ctx, redshiftRequest(t, "ModifyCluster", map[string]string{
		"ClusterIdentifier": "nonexistent",
		"NodeType":          "ra3.xlplus",
	}))
	if err == nil {
		t.Fatal("want error for nonexistent cluster")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ClusterNotFoundFault" {
		t.Errorf("want ClusterNotFoundFault, got %v", err)
	}

	// DeleteCluster not found.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "DeleteCluster", map[string]string{
		"ClusterIdentifier": "nonexistent",
	}))
	if err == nil {
		t.Fatal("want error for delete nonexistent cluster")
	}

	// CreateClusterParameterGroup missing name.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterParameterGroup", nil))
	if err == nil {
		t.Fatal("want error for missing ParameterGroupName")
	}

	// CreateClusterSubnetGroup missing name.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterSubnetGroup", nil))
	if err == nil {
		t.Fatal("want error for missing ClusterSubnetGroupName")
	}

	// CreateClusterSnapshot missing cluster.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "CreateClusterSnapshot", map[string]string{
		"ClusterIdentifier":  "nonexistent",
		"SnapshotIdentifier": "snap-1",
	}))
	if err == nil {
		t.Fatal("want error for snapshot on nonexistent cluster")
	}

	// Unsupported operation.
	_, err = p.HandleRequest(ctx, redshiftRequest(t, "UnknownAction", nil))
	if err == nil {
		t.Fatal("want error for unknown action")
	}

	// ModifyCluster with NumberOfNodes update.
	_, createErr := p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
		"ClusterIdentifier": "mod-cluster",
		"NodeType":          "dc2.large",
		"MasterUsername":    "admin",
		"NumberOfNodes":     "2",
	}))
	if createErr != nil {
		t.Fatalf("CreateCluster: %v", createErr)
	}
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "ModifyCluster", map[string]string{
		"ClusterIdentifier": "mod-cluster",
		"NumberOfNodes":     "4",
	}))
	if err != nil {
		t.Fatalf("ModifyCluster NumberOfNodes: %v", err)
	}
	var modResp struct {
		XMLName xml.Name `xml:"ModifyClusterResult"`
		Cluster struct {
			NumberOfNodes int `xml:"NumberOfNodes"`
		} `xml:"Cluster>member"`
	}
	if err := xml.Unmarshal(stripXMLHeader(resp.Body), &modResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if modResp.Cluster.NumberOfNodes != 4 {
		t.Errorf("want NumberOfNodes=4, got %d", modResp.Cluster.NumberOfNodes)
	}
}

// stripXMLHeader removes the XML header declaration from the beginning of a byte slice
// to allow xml.Unmarshal to work on Redshift responses that include the header.
func stripXMLHeader(body []byte) []byte {
	const header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	if len(body) >= len(header) && string(body[:len(header)]) == header {
		return body[len(header):]
	}
	return body
}
