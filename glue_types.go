package substrate

import "time"

// glueNamespace is the state namespace used by GluePlugin.
const glueNamespace = "glue"

// GlueDatabase represents an AWS Glue Data Catalog database.
type GlueDatabase struct {
	// Name is the name of the database.
	Name string `json:"Name"`

	// Description is an optional description of the database.
	Description string `json:"Description,omitempty"`

	// LocationURI is the location of the database (e.g. an S3 path).
	LocationURI string `json:"LocationUri,omitempty"`

	// Parameters is a map of key-value pairs associated with the database.
	Parameters map[string]string `json:"Parameters,omitempty"`

	// Arn is the ARN of the database.
	Arn string `json:"Arn"`

	// Tags is the map of tags for the database.
	Tags map[string]string `json:"Tags"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this database resides.
	Region string `json:"Region"`

	// CreatedAt is the time the database was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// GlueTable represents an AWS Glue Data Catalog table.
type GlueTable struct {
	// Name is the name of the table.
	Name string `json:"Name"`

	// DatabaseName is the name of the database containing this table.
	DatabaseName string `json:"DatabaseName"`

	// Description is an optional description of the table.
	Description string `json:"Description,omitempty"`

	// TableType is the type of the table (e.g. "EXTERNAL_TABLE").
	TableType string `json:"TableType,omitempty"`

	// StorageDescriptor describes the physical storage of the table.
	StorageDescriptor *GlueStorageDescriptor `json:"StorageDescriptor,omitempty"`

	// Arn is the ARN of the table.
	Arn string `json:"Arn"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this table resides.
	Region string `json:"Region"`

	// CreatedAt is the time the table was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// GlueStorageDescriptor describes the physical storage of a Glue table.
type GlueStorageDescriptor struct {
	// Location is the physical URI of the data (e.g. an S3 path).
	Location string `json:"Location,omitempty"`

	// InputFormat is the input format class for the table.
	InputFormat string `json:"InputFormat,omitempty"`

	// OutputFormat is the output format class for the table.
	OutputFormat string `json:"OutputFormat,omitempty"`
}

// GlueConnection represents an AWS Glue connection.
type GlueConnection struct {
	// Name is the name of the connection.
	Name string `json:"Name"`

	// Description is an optional description of the connection.
	Description string `json:"Description,omitempty"`

	// ConnectionType is the type of the connection (e.g. "JDBC", "S3").
	ConnectionType string `json:"ConnectionType"`

	// ConnectionProperties is a map of connection properties.
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`

	// Arn is the ARN of the connection.
	Arn string `json:"Arn"`

	// Tags is the map of tags for the connection.
	Tags map[string]string `json:"Tags"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this connection resides.
	Region string `json:"Region"`

	// CreatedAt is the time the connection was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// GlueCrawler represents an AWS Glue crawler.
type GlueCrawler struct {
	// Name is the name of the crawler.
	Name string `json:"Name"`

	// Role is the IAM role ARN for the crawler.
	Role string `json:"Role"`

	// DatabaseName is the Glue database where crawled data is written.
	DatabaseName string `json:"DatabaseName"`

	// Description is an optional description of the crawler.
	Description string `json:"Description,omitempty"`

	// State is the current state of the crawler.
	State string `json:"State"`

	// Targets specifies the data stores to crawl.
	Targets map[string]interface{} `json:"Targets,omitempty"`

	// Arn is the ARN of the crawler.
	Arn string `json:"Arn"`

	// Tags is the map of tags for the crawler.
	Tags map[string]string `json:"Tags"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this crawler resides.
	Region string `json:"Region"`

	// CreatedAt is the time the crawler was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// GlueJob represents an AWS Glue ETL job.
type GlueJob struct {
	// Name is the name of the job.
	Name string `json:"Name"`

	// Role is the IAM role ARN for the job.
	Role string `json:"Role"`

	// Description is an optional description of the job.
	Description string `json:"Description,omitempty"`

	// Command describes the job command (script location, Python version, etc.).
	Command GlueJobCommand `json:"Command"`

	// Arn is the ARN of the job.
	Arn string `json:"Arn"`

	// Tags is the map of tags for the job.
	Tags map[string]string `json:"Tags"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this job resides.
	Region string `json:"Region"`

	// CreatedAt is the time the job was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// GlueJobCommand describes the script command for a Glue job.
type GlueJobCommand struct {
	// Name is the name of the job command (e.g. "glueetl", "pythonshell").
	Name string `json:"Name"`

	// ScriptLocation is the S3 path of the script to execute.
	ScriptLocation string `json:"ScriptLocation,omitempty"`

	// PythonVersion is the Python version (e.g. "3").
	PythonVersion string `json:"PythonVersion,omitempty"`
}

// GlueJobRun represents a single execution of a Glue job.
type GlueJobRun struct {
	// ID is the job run identifier.
	ID string `json:"Id"`

	// JobName is the name of the job that was run.
	JobName string `json:"JobName"`

	// JobRunState is the state of the job run.
	JobRunState string `json:"JobRunState"`

	// StartedOn is the time the job run started.
	StartedOn time.Time `json:"StartedOn"`

	// CompletedOn is the time the job run completed.
	CompletedOn time.Time `json:"CompletedOn"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this job run was executed.
	Region string `json:"Region"`
}
