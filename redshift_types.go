package substrate

import "time"

// redshiftNamespace is the state namespace for AWS Redshift resources.
const redshiftNamespace = "redshift"

// redshiftXMLNS is the XML namespace for Redshift API responses.
const redshiftXMLNS = "http://redshift.amazonaws.com/doc/2012-12-01/"

// RedshiftCluster represents an AWS Redshift cluster.
type RedshiftCluster struct {
	// ClusterIdentifier is the unique identifier for the cluster.
	ClusterIdentifier string `json:"clusterIdentifier"`
	// ClusterStatus is the current status of the cluster (available).
	ClusterStatus string `json:"clusterStatus"`
	// NodeType is the node type for the cluster (e.g., dc2.large).
	NodeType string `json:"nodeType"`
	// MasterUsername is the master database user name.
	MasterUsername string `json:"masterUsername"`
	// DBName is the name of the initial database.
	DBName string `json:"dbName"`
	// EndpointAddress is the DNS address of the cluster endpoint.
	EndpointAddress string `json:"endpointAddress"`
	// EndpointPort is the port number for the cluster endpoint.
	EndpointPort int `json:"endpointPort"`
	// ClusterCreateTime is when the cluster was created.
	ClusterCreateTime time.Time `json:"clusterCreateTime"`
	// NumberOfNodes is the number of compute nodes in the cluster.
	NumberOfNodes int `json:"numberOfNodes"`
	// VpcID is the VPC identifier in which the cluster is running.
	VpcID string `json:"vpcId,omitempty"`
	// AvailabilityZone is the name of the availability zone.
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	// ClusterArn is the Amazon Resource Name (ARN) for the cluster.
	ClusterArn string `json:"clusterArn"`
	// AccountID is the AWS account that owns this cluster.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the cluster exists.
	Region string `json:"region"`
}

// RedshiftClusterParameterGroup represents an AWS Redshift cluster parameter group.
type RedshiftClusterParameterGroup struct {
	// ParameterGroupName is the name of the cluster parameter group.
	ParameterGroupName string `json:"parameterGroupName"`
	// ParameterGroupFamily is the parameter group family (e.g., redshift-1.0).
	ParameterGroupFamily string `json:"parameterGroupFamily"`
	// Description is the description of the parameter group.
	Description string `json:"description,omitempty"`
	// AccountID is the AWS account that owns this parameter group.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the parameter group exists.
	Region string `json:"region"`
}

// RedshiftClusterSubnetGroup represents an AWS Redshift cluster subnet group.
type RedshiftClusterSubnetGroup struct {
	// ClusterSubnetGroupName is the name of the cluster subnet group.
	ClusterSubnetGroupName string `json:"clusterSubnetGroupName"`
	// Description is the description of the subnet group.
	Description string `json:"description,omitempty"`
	// VpcID is the VPC identifier for the subnet group.
	VpcID string `json:"vpcId,omitempty"`
	// AccountID is the AWS account that owns this subnet group.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the subnet group exists.
	Region string `json:"region"`
}

// RedshiftSnapshot represents an AWS Redshift cluster snapshot.
type RedshiftSnapshot struct {
	// SnapshotIdentifier is the unique identifier of the snapshot.
	SnapshotIdentifier string `json:"snapshotIdentifier"`
	// ClusterIdentifier is the identifier of the source cluster.
	ClusterIdentifier string `json:"clusterIdentifier"`
	// SnapshotType is the type of snapshot (manual).
	SnapshotType string `json:"snapshotType"`
	// Status is the status of the snapshot (available).
	Status string `json:"status"`
	// SnapshotCreateTime is when the snapshot was created.
	SnapshotCreateTime time.Time `json:"snapshotCreateTime"`
	// AccountID is the AWS account that owns this snapshot.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the snapshot exists.
	Region string `json:"region"`
}

// State key helpers.

func redshiftClusterKey(acct, region, id string) string {
	return "cluster:" + acct + "/" + region + "/" + id
}

func redshiftClusterIDsKey(acct, region string) string {
	return "cluster_ids:" + acct + "/" + region
}

func redshiftParamGroupKey(acct, region, name string) string {
	return "paramgroup:" + acct + "/" + region + "/" + name
}

func redshiftParamGroupNamesKey(acct, region string) string {
	return "paramgroup_names:" + acct + "/" + region
}

func redshiftSubnetGroupKey(acct, region, name string) string {
	return "subnetgroup:" + acct + "/" + region + "/" + name
}

func redshiftSubnetGroupNamesKey(acct, region string) string {
	return "subnetgroup_names:" + acct + "/" + region
}

func redshiftSnapshotKey(acct, region, id string) string {
	return "snapshot:" + acct + "/" + region + "/" + id
}

func redshiftSnapshotIDsKey(acct, region string) string {
	return "snapshot_ids:" + acct + "/" + region
}
