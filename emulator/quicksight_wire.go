package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// QuickSightDataSource (quicksight_plugin.go) is a persisted state record, and describeDataSource
// handed it straight to the caller under `DataSource`. Two of its fields are substrate's own,
// AccountID and Region, and API_DataSource publishes neither (#756). QuickSightDataSet needs no type
// here: no response renders it — CreateDataSet answers a map of four published members, and
// DescribeIngestion reads the record only to confirm the dataset exists.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back. Projecting leaves the
// stored bytes unchanged, so a run recorded before this replays identically.

// quicksightDataSourceOut is the DataSource element of DescribeDataSource's response.
//
// Five of API_DataSource's members — the ones the record models. The rest are absent rather than
// present and empty (#1013's rule, #1199's gap).
type quicksightDataSourceOut struct {
	Arn          string `json:"Arn"`
	DataSourceID string `json:"DataSourceId"`
	Name         string `json:"Name"`
	Status       string `json:"Status"`
	Type         string `json:"Type"`
	// ErrorInfo is DataSourceErrorInfo, reported only for a seeded CREATION_FAILED (#1155).
	ErrorInfo *quicksightErrorInfoOut `json:"ErrorInfo,omitempty"`
}

// quicksightDataSourceToWire projects a persisted data source onto the published shape.
func quicksightDataSourceToWire(ds QuickSightDataSource) quicksightDataSourceOut {
	return quicksightDataSourceOut{
		Arn:          ds.Arn,
		DataSourceID: ds.DataSourceID,
		Name:         ds.Name,
		Status:       ds.Status,
		Type:         ds.Type,
	}
}
