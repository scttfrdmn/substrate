package substrate

import "time"

// rdsNamespace is the state namespace used by the RDS plugin.
const rdsNamespace = "rds"

// rdsXMLNS is the XML namespace used in RDS API responses.
const rdsXMLNS = "https://rds.amazonaws.com/doc/2014-10-31/"

// RDSDBInstance represents an Amazon RDS DB instance.
type RDSDBInstance struct {
	// DBInstanceIdentifier is the unique identifier for the DB instance.
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	// DBInstanceClass is the compute/memory class of the DB instance.
	DBInstanceClass string `json:"DBInstanceClass"`
	// Engine is the database engine (e.g. "mysql", "postgres").
	Engine string `json:"Engine"`
	// EngineVersion is the version of the database engine.
	EngineVersion string `json:"EngineVersion"`
	// DBInstanceStatus is the current status of the DB instance.
	DBInstanceStatus string `json:"DBInstanceStatus"`
	// MasterUsername is the master user name for the DB instance.
	MasterUsername string `json:"MasterUsername"`
	// AllocatedStorage is the amount of storage in gibibytes.
	AllocatedStorage int `json:"AllocatedStorage"`
	// DBInstanceArn is the Amazon Resource Name for the DB instance.
	DBInstanceArn string `json:"DBInstanceArn"`
	// Endpoint holds the connection endpoint details.
	Endpoint RDSEndpoint `json:"Endpoint"`
	// MultiAZ indicates whether the instance is a Multi-AZ deployment.
	MultiAZ bool `json:"MultiAZ"`
	// DBSubnetGroupName is the subnet group associated with the instance.
	DBSubnetGroupName string `json:"DBSubnetGroupName"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the instance.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the instance resides.
	Region string `json:"Region"`
	// CreatedAt is the time the instance was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// RDSEndpoint holds the endpoint address and port for a DB instance.
type RDSEndpoint struct {
	// Address is the DNS hostname of the endpoint.
	Address string `json:"Address"`
	// Port is the port number on which the database accepts connections.
	Port int `json:"Port"`
}

// RDSDBSnapshot represents a snapshot of a DB instance.
type RDSDBSnapshot struct {
	// DBSnapshotIdentifier is the unique name of the DB snapshot.
	DBSnapshotIdentifier string `json:"DBSnapshotIdentifier"`
	// DBInstanceIdentifier is the source DB instance identifier.
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	// SnapshotType is the type of snapshot (e.g. "manual").
	SnapshotType string `json:"SnapshotType"`
	// Status is the current status of the snapshot.
	Status string `json:"Status"`
	// Engine is the database engine of the source instance.
	Engine string `json:"Engine"`
	// AllocatedStorage is the allocated storage of the source instance in GiB.
	AllocatedStorage int `json:"AllocatedStorage"`
	// DBSnapshotArn is the Amazon Resource Name of the snapshot.
	DBSnapshotArn string `json:"DBSnapshotArn"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the snapshot.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the snapshot resides.
	Region string `json:"Region"`
	// CreatedAt is the time the snapshot was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// RDSDBSubnetGroup represents an RDS DB subnet group.
type RDSDBSubnetGroup struct {
	// DBSubnetGroupName is the name of the subnet group.
	DBSubnetGroupName string `json:"DBSubnetGroupName"`
	// DBSubnetGroupDescription is the description of the subnet group.
	DBSubnetGroupDescription string `json:"DBSubnetGroupDescription"`
	// SubnetGroupStatus is the current status of the subnet group.
	SubnetGroupStatus string `json:"SubnetGroupStatus"`
	// VpcID is the ID of the VPC for the subnet group.
	VpcID string `json:"VpcId"`
	// DBSubnetGroupArn is the Amazon Resource Name of the subnet group.
	DBSubnetGroupArn string `json:"DBSubnetGroupArn"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the subnet group.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the subnet group resides.
	Region string `json:"Region"`
}

// RDSDBParameterGroup represents an RDS DB parameter group.
type RDSDBParameterGroup struct {
	// DBParameterGroupName is the name of the parameter group.
	DBParameterGroupName string `json:"DBParameterGroupName"`
	// DBParameterGroupFamily is the parameter group family (e.g. "mysql8.0").
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	// Description is a description of the parameter group.
	Description string `json:"Description"`
	// DBParameterGroupArn is the Amazon Resource Name of the parameter group.
	DBParameterGroupArn string `json:"DBParameterGroupArn"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the parameter group.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the parameter group resides.
	Region string `json:"Region"`
}

// rdsDefaultPort returns the default port for the given database engine.
func rdsDefaultPort(engine string) int {
	switch engine {
	case "postgres", "aurora-postgresql":
		return 5432
	case "oracle-se2", "oracle-ee":
		return 1521
	case "sqlserver-se", "sqlserver-ee", "sqlserver-ex", "sqlserver-web":
		return 1433
	default:
		// mysql, mariadb, aurora, aurora-mysql
		return 3306
	}
}
