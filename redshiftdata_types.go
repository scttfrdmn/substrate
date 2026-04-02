package substrate

import "time"

// redshiftDataNamespace is the state namespace for AWS Redshift Data API resources.
const redshiftDataNamespace = "redshift-data"

// RedshiftDataStatement represents an executed SQL statement in the Redshift Data API.
type RedshiftDataStatement struct {
	// ID is the unique statement identifier.
	ID string `json:"Id"`
	// Status is the execution status (always FINISHED in the emulator).
	Status string `json:"Status"`
	// QueryString is the SQL that was submitted.
	QueryString string `json:"QueryString"`
	// WorkgroupName is the Redshift Serverless workgroup name.
	WorkgroupName string `json:"WorkgroupName,omitempty"`
	// ClusterIdentifier is the provisioned cluster name (alternative to workgroup).
	ClusterIdentifier string `json:"ClusterIdentifier,omitempty"`
	// Database is the database name.
	Database string `json:"Database,omitempty"`
	// CreatedAt is when the statement was submitted.
	CreatedAt time.Time `json:"CreatedAt"`
	// AccountID is the AWS account that executed this statement.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the statement was executed.
	Region string `json:"Region"`
}

// RedshiftDataColumnMetadata describes a result column in the Redshift Data API format.
type RedshiftDataColumnMetadata struct {
	// Name is the column name.
	Name string `json:"name"`
	// TypeName is the SQL type name (e.g. "varchar", "int8").
	TypeName string `json:"typeName"`
	// Label is the column label (defaults to Name).
	Label string `json:"label,omitempty"`
}

// RedshiftDataField is a single field value in a result row.
// Exactly one of the value fields should be set.
type RedshiftDataField struct {
	// StringValue holds a string-typed field value.
	StringValue *string `json:"stringValue,omitempty"`
	// LongValue holds an integer-typed field value.
	LongValue *int64 `json:"longValue,omitempty"`
	// DoubleValue holds a floating-point field value.
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	// BooleanValue holds a boolean-typed field value.
	BooleanValue *bool `json:"booleanValue,omitempty"`
	// IsNull indicates a NULL value.
	IsNull *bool `json:"isNull,omitempty"`
}

// State key helpers.

func redshiftDataStatementKey(acct, region, id string) string {
	return "statement:" + acct + "/" + region + "/" + id
}
