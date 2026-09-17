package emulator

// The wire is a different thing from the state, and the type below exists to keep
// them apart.
//
// DynamoDBTable (dynamodb_types.go) is a persisted state record. Twelve of its
// fourteen fields happen to be genuine TableDescription members, which is what made
// it look safe to hand straight to a caller — and it was handed straight to one, at
// four sites, so the two fields that are substrate's own went out on the wire with
// the rest. `CreateTable` with a tag answered `"Tags":{"env":"test"}` inside its
// TableDescription and `DescribeTable` answered `"TTLAttribute":"expiresAt"`, neither
// of which API_TableDescription publishes; tags on a table are readable through
// ListTagsOfResource and the TTL through DescribeTimeToLive, and those two are where
// AWS puts them (#1013).
//
// So the description gets its own type, tagged from the model, projected from the
// state. This is the pattern #529 established for API Gateway v1 in
// apigateway_wire.go, and it is here for the same reason: a state record grows fields
// for substrate's own bookkeeping, and a projection is what stops the next one from
// reaching a response. #938 needs to add exactly such a field.
//
// Do not "fix" a leak here by adding `json:"-"` to a state field. That works for the
// one field and leaves the next one to be remembered rather than prevented, and it
// changes the format of every recorded run, because MemoryStateManager snapshots
// these bytes and a replay reads them back.
//
// The nested types are reused by value rather than re-projected: every member of
// DynamoDBKeySchemaElement, DynamoDBAttributeDefinition, DynamoDBBillingModeSummary,
// DynamoDBProvisionedThroughputDesc, the two index descriptions, DynamoDBProjection
// and DynamoDBStreamSpecification is a member of the AWS shape it mirrors, so none of
// them carries anything to strip.

// tableDescriptionOut is the TableDescription element of the DynamoDB table
// responses: CreateTable, UpdateTable, DeleteTable and — under the name Table —
// DescribeTable.
//
// Member names and optionality follow API_TableDescription. Members substrate does
// not model are simply absent from the type rather than being present and empty, so
// this reports nothing AWS would not: ArchivalSummary, DeletionProtectionEnabled,
// GlobalTableSettingsReplicationMode, GlobalTableVersion, GlobalTableWitnesses,
// LatestStreamLabel, MultiRegionConsistency, OnDemandThroughput, Replicas,
// RestoreSummary, SSEDescription, TableClassSummary, TableId, VectorIndexes and
// WarmThroughput.
type tableDescriptionOut struct {
	TableName              string                             `json:"TableName"`
	TableARN               string                             `json:"TableArn"`
	TableStatus            string                             `json:"TableStatus"`
	CreationDateTime       float64                            `json:"CreationDateTime"`
	KeySchema              []DynamoDBKeySchemaElement         `json:"KeySchema"`
	AttributeDefinitions   []DynamoDBAttributeDefinition      `json:"AttributeDefinitions"`
	BillingModeSummary     DynamoDBBillingModeSummary         `json:"BillingModeSummary"`
	ProvisionedThroughput  DynamoDBProvisionedThroughputDesc  `json:"ProvisionedThroughput"`
	GlobalSecondaryIndexes []DynamoDBGlobalSecondaryIndexDesc `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []DynamoDBLocalSecondaryIndexDesc  `json:"LocalSecondaryIndexes,omitempty"`
	StreamSpecification    *DynamoDBStreamSpecification       `json:"StreamSpecification,omitempty"`
	LatestStreamARN        string                             `json:"LatestStreamArn,omitempty"`
	TableSizeBytes         int64                              `json:"TableSizeBytes"`
	ItemCount              int64                              `json:"ItemCount"`
}

// dynamodbTableDescriptionWire projects a stored table onto the wire.
//
// Every field is copied explicitly rather than by conversion, because the point of
// the type is that it has fewer fields than the record: a conversion would stop
// compiling the moment the record gains one, which is the opposite of what is wanted
// here — a new state field must be inert, not a build failure to be worked around.
func dynamodbTableDescriptionWire(tbl DynamoDBTable) tableDescriptionOut {
	return tableDescriptionOut{
		TableName:              tbl.TableName,
		TableARN:               tbl.TableARN,
		TableStatus:            tbl.TableStatus,
		CreationDateTime:       tbl.CreationDateTime,
		KeySchema:              tbl.KeySchema,
		AttributeDefinitions:   tbl.AttributeDefinitions,
		BillingModeSummary:     tbl.BillingModeSummary,
		ProvisionedThroughput:  tbl.ProvisionedThroughput,
		GlobalSecondaryIndexes: tbl.GlobalSecondaryIndexes,
		LocalSecondaryIndexes:  tbl.LocalSecondaryIndexes,
		StreamSpecification:    tbl.StreamSpecification,
		LatestStreamARN:        tbl.LatestStreamARN,
		TableSizeBytes:         tbl.TableSizeBytes,
		ItemCount:              tbl.ItemCount,
	}
}
