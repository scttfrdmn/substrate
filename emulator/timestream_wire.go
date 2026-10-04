package emulator

// The wire is a different thing from the state, and the two types below keep them apart.
//
// TimestreamDatabase and TimestreamTable (timestream_types.go) are persisted state records, and every
// operation that answers one handed it straight to the caller: CreateDatabase, DescribeDatabase and
// ListDatabases under `Database`/`Databases`, CreateTable, DescribeTable and ListTables under
// `Table`/`Tables`. Both declared CreationTime and LastUpdatedTime as strings and wrote RFC3339, a
// whole second at a time (#1207).
//
// # Why Timestream's dates are EpochSeconds
//
// Timestream Write speaks awsJson1_0, where a Timestamp is published as epoch seconds with fractional
// precision. API_Database and API_Table type both members as Timestamp ("calculated from the Unix
// epoch time"), and an awsJson timestamp deserializer expects a number, so the RFC3339 strings made
// every database and table response undecodable in a typed SDK. EpochSeconds (epochseconds.go)
// renders three decimals, the precision AWS's samples carry.
//
// # Why, unlike CodeDeploy, the record was retyped
//
// CodeDeploy's records already held time.Time, so #1327 converted on projection and the stored bytes
// did not change. Timestream's held a whole-second string, which cannot carry the fractional seconds
// #1207 requires for two writes in one second to be orderable. So the two fields are time.Time now.
// That is a stored-shape change, with the fallback #1207 asks for: a record written before it holds
// an RFC3339 string, which time.Time decodes as-is
// (TestTimestreamWire_ARecordWrittenBeforeTheFixStillDecodes).
//
// A run recorded before the fix and replayed after it will not reproduce its state hash for a create
// made at a sub-second instant, because the record now keeps the fraction the old one dropped. Its
// responses differ too, since they were strings and are numbers now. Neither is avoidable: a fraction
// that was never stored cannot be reproduced.

// timestreamDatabaseOut is a Database element of a Timestream Write response.
//
// Five of API_Database's six members, the ones the record models. KmsKeyId is not modeled, so it is
// absent rather than present and empty (#1013's rule).
type timestreamDatabaseOut struct {
	Arn             string       `json:"Arn"`
	CreationTime    EpochSeconds `json:"CreationTime"`
	DatabaseName    string       `json:"DatabaseName"`
	LastUpdatedTime EpochSeconds `json:"LastUpdatedTime"`
	TableCount      int64        `json:"TableCount"`
}

// timestreamDatabaseToWire projects a persisted database onto the published shape.
func timestreamDatabaseToWire(db TimestreamDatabase) timestreamDatabaseOut {
	return timestreamDatabaseOut{
		Arn:             db.Arn,
		CreationTime:    EpochSeconds(db.CreationTime),
		DatabaseName:    db.DatabaseName,
		LastUpdatedTime: EpochSeconds(db.LastUpdatedTime),
		TableCount:      db.TableCount,
	}
}

// timestreamTableOut is a Table element of a Timestream Write response.
//
// Seven of API_Table's nine members, the ones the record models. MagneticStoreWriteProperties and
// Schema are not modeled, so they are absent.
type timestreamTableOut struct {
	Arn                 string                        `json:"Arn"`
	CreationTime        EpochSeconds                  `json:"CreationTime"`
	DatabaseName        string                        `json:"DatabaseName"`
	LastUpdatedTime     EpochSeconds                  `json:"LastUpdatedTime"`
	RetentionProperties TimestreamRetentionProperties `json:"RetentionProperties"`
	TableName           string                        `json:"TableName"`
	TableStatus         string                        `json:"TableStatus"`
}

// timestreamTableToWire projects a persisted table onto the published shape.
func timestreamTableToWire(tbl TimestreamTable) timestreamTableOut {
	return timestreamTableOut{
		Arn:                 tbl.Arn,
		CreationTime:        EpochSeconds(tbl.CreationTime),
		DatabaseName:        tbl.DatabaseName,
		LastUpdatedTime:     EpochSeconds(tbl.LastUpdatedTime),
		RetentionProperties: tbl.RetentionProperties,
		TableName:           tbl.TableName,
		TableStatus:         tbl.TableStatus,
	}
}
