package substrate

import "fmt"

// timestreamNamespace is the state namespace for all Timestream resources.
const timestreamNamespace = "timestream"

// timestreamCtrlNamespace is the state namespace for Timestream HTTP control-plane seeds.
const timestreamCtrlNamespace = "timestream-ctrl"

// TimestreamDatabase represents an Amazon Timestream database.
type TimestreamDatabase struct {
	// DatabaseName is the name of the database.
	DatabaseName string `json:"DatabaseName"`
	// Arn is the Amazon Resource Name for the database.
	Arn string `json:"Arn"`
	// TableCount is the number of tables in the database.
	TableCount int64 `json:"TableCount"`
	// CreationTime is the ISO-8601 timestamp when the database was created.
	CreationTime string `json:"CreationTime"`
	// LastUpdatedTime is the ISO-8601 timestamp of the last modification.
	LastUpdatedTime string `json:"LastUpdatedTime"`
}

// TimestreamTable represents an Amazon Timestream table.
type TimestreamTable struct {
	// DatabaseName is the name of the parent database.
	DatabaseName string `json:"DatabaseName"`
	// TableName is the name of the table.
	TableName string `json:"TableName"`
	// Arn is the Amazon Resource Name for the table.
	Arn string `json:"Arn"`
	// TableStatus is the current status (always "ACTIVE" in the emulator).
	TableStatus string `json:"TableStatus"`
	// CreationTime is the ISO-8601 timestamp when the table was created.
	CreationTime string `json:"CreationTime"`
	// LastUpdatedTime is the ISO-8601 timestamp of the last modification.
	LastUpdatedTime string `json:"LastUpdatedTime"`
	// RetentionProperties holds data-retention settings.
	RetentionProperties TimestreamRetentionProperties `json:"RetentionProperties"`
}

// TimestreamRetentionProperties holds data-retention settings for a Timestream table.
type TimestreamRetentionProperties struct {
	// MemoryStoreRetentionPeriodInHours is the in-memory retention period in hours.
	MemoryStoreRetentionPeriodInHours int64 `json:"MemoryStoreRetentionPeriodInHours"`
	// MagneticStoreRetentionPeriodInDays is the magnetic-store retention period in days.
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays"`
}

// TimestreamQueryResult holds a pre-seeded Timestream query result for testing.
type TimestreamQueryResult struct {
	// Rows contains the result rows.
	Rows []TimestreamRow `json:"Rows"`
	// ColumnInfo describes the result columns.
	ColumnInfo []TimestreamColumnInfo `json:"ColumnInfo"`
}

// TimestreamRow holds one row of Timestream query data.
type TimestreamRow struct {
	// Data is the list of column values for this row.
	Data []TimestreamDatum `json:"Data"`
}

// TimestreamDatum holds a single scalar value in a Timestream query result.
type TimestreamDatum struct {
	// ScalarValue is the string representation of the datum.
	ScalarValue string `json:"ScalarValue,omitempty"`
}

// TimestreamColumnInfo describes a column returned by a Timestream query.
type TimestreamColumnInfo struct {
	// Name is the column name.
	Name string `json:"Name"`
	// Type holds the scalar type information for the column.
	Type TimestreamColumnInfoType `json:"Type"`
}

// TimestreamColumnInfoType holds the scalar type string for a Timestream column.
type TimestreamColumnInfoType struct {
	// ScalarType is the Timestream scalar type (e.g. "VARCHAR", "BIGINT").
	ScalarType string `json:"ScalarType,omitempty"`
}

// State key helpers.

func timestreamDBKey(acct, region, name string) string {
	return fmt.Sprintf("db:%s/%s/%s", acct, region, name)
}

func timestreamDBNamesKey(acct, region string) string {
	return fmt.Sprintf("db_names:%s/%s", acct, region)
}

func timestreamTableKey(acct, region, dbName, tableName string) string {
	return fmt.Sprintf("table:%s/%s/%s/%s", acct, region, dbName, tableName)
}

func timestreamTableNamesKey(acct, region, dbName string) string {
	return fmt.Sprintf("table_names:%s/%s/%s", acct, region, dbName)
}

func timestreamCtrlResultKey(qs string) string { return "result:" + qs }
