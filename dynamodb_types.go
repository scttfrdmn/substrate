package substrate

// dynamodbNamespace is the state namespace used by DynamoDBPlugin.
const dynamodbNamespace = "dynamodb"

// AttributeValue mirrors the DynamoDB wire protocol attribute value shape.
// Each field corresponds to a DynamoDB type: S (string), N (number stored as
// string), B (binary), SS/NS/BS (string/number/binary sets), M (map), L (list),
// NULL (null), BOOL (boolean).
type AttributeValue struct {
	// S is a string value. Using *string so that empty strings ("") are
	// preserved in JSON serialisation instead of being dropped by omitempty.
	S *string `json:"S,omitempty"`

	// N is a number value, stored as a decimal string.
	N string `json:"N,omitempty"`

	// B is a binary value.
	B []byte `json:"B,omitempty"`

	// SS is a string set.
	SS []string `json:"SS,omitempty"`

	// NS is a number set.
	NS []string `json:"NS,omitempty"`

	// BS is a binary set.
	BS [][]byte `json:"BS,omitempty"`

	// M is a map of attribute values.
	M map[string]*AttributeValue `json:"M,omitempty"`

	// L is a list of attribute values.
	L []*AttributeValue `json:"L,omitempty"`

	// NULL indicates a null value when true.
	NULL *bool `json:"NULL,omitempty"`

	// BOOL is a boolean value.
	BOOL *bool `json:"BOOL,omitempty"`
}

// DynamoDBTable represents an emulated DynamoDB table stored in state.
type DynamoDBTable struct {
	// TableName is the name of the table.
	TableName string `json:"TableName"`

	// TableARN is the Amazon Resource Name of the table.
	TableARN string `json:"TableARN"`

	// TableStatus is the current table status (always "ACTIVE" in this emulator).
	TableStatus string `json:"TableStatus"`

	// CreationDateTime is the Unix epoch time (float64) when the table was created.
	CreationDateTime float64 `json:"CreationDateTime"`

	// KeySchema specifies the primary key attributes.
	KeySchema []DynamoDBKeySchemaElement `json:"KeySchema"`

	// AttributeDefinitions describes the key attribute types.
	AttributeDefinitions []DynamoDBAttributeDefinition `json:"AttributeDefinitions"`

	// BillingModeSummary summarizes the billing mode.
	BillingModeSummary DynamoDBBillingModeSummary `json:"BillingModeSummary"`

	// ProvisionedThroughput holds read/write capacity units.
	ProvisionedThroughput DynamoDBProvisionedThroughputDesc `json:"ProvisionedThroughput"`

	// GlobalSecondaryIndexes lists any GSIs on the table.
	GlobalSecondaryIndexes []DynamoDBGlobalSecondaryIndexDesc `json:"GlobalSecondaryIndexes,omitempty"`

	// LocalSecondaryIndexes lists any LSIs on the table.
	LocalSecondaryIndexes []DynamoDBLocalSecondaryIndexDesc `json:"LocalSecondaryIndexes,omitempty"`

	// StreamSpecification holds the Streams configuration.
	StreamSpecification *DynamoDBStreamSpecification `json:"StreamSpecification,omitempty"`

	// LatestStreamARN is the ARN of the latest stream, if enabled.
	LatestStreamARN string `json:"LatestStreamARN,omitempty"`

	// TableSizeBytes is estimated as ItemCount×100.
	TableSizeBytes int64 `json:"TableSizeBytes"`

	// ItemCount is the number of items in the table.
	ItemCount int64 `json:"ItemCount"`

	// TTLAttribute is the name of the attribute used for TTL, if enabled.
	TTLAttribute string `json:"TTLAttribute,omitempty"`

	// Tags holds optional user-defined key-value tags on the table.
	Tags map[string]string `json:"Tags,omitempty"`
}

// DynamoDBKeySchemaElement specifies a single attribute that forms the primary key.
type DynamoDBKeySchemaElement struct {
	// AttributeName is the name of the key attribute.
	AttributeName string `json:"AttributeName"`

	// KeyType is either "HASH" (partition key) or "RANGE" (sort key).
	KeyType string `json:"KeyType"`
}

// DynamoDBAttributeDefinition describes a single attribute used in a key schema.
type DynamoDBAttributeDefinition struct {
	// AttributeName is the name of the attribute.
	AttributeName string `json:"AttributeName"`

	// AttributeType is the attribute type: "S" (string), "N" (number), or "B" (binary).
	AttributeType string `json:"AttributeType"`
}

// DynamoDBProvisionedThroughputDesc describes provisioned throughput settings.
type DynamoDBProvisionedThroughputDesc struct {
	// ReadCapacityUnits is the provisioned read capacity.
	ReadCapacityUnits int64 `json:"ReadCapacityUnits"`

	// WriteCapacityUnits is the provisioned write capacity.
	WriteCapacityUnits int64 `json:"WriteCapacityUnits"`

	// NumberOfDecreasesToday is the number of capacity decreases in the current day.
	NumberOfDecreasesToday int64 `json:"NumberOfDecreasesToday"`

	// LastIncreaseDateTime is the Unix epoch time of the last capacity increase.
	LastIncreaseDateTime float64 `json:"LastIncreaseDateTime,omitempty"`

	// LastDecreaseDateTime is the Unix epoch time of the last capacity decrease.
	LastDecreaseDateTime float64 `json:"LastDecreaseDateTime,omitempty"`
}

// DynamoDBBillingModeSummary summarizes the billing mode for a table.
type DynamoDBBillingModeSummary struct {
	// BillingMode is either "PROVISIONED" or "PAY_PER_REQUEST".
	BillingMode string `json:"BillingMode"`
}

// DynamoDBGlobalSecondaryIndexDesc describes a global secondary index on a table.
type DynamoDBGlobalSecondaryIndexDesc struct {
	// IndexName is the name of the GSI.
	IndexName string `json:"IndexName"`

	// KeySchema specifies the GSI key attributes.
	KeySchema []DynamoDBKeySchemaElement `json:"KeySchema"`

	// Projection describes which attributes are projected into the index.
	Projection DynamoDBProjection `json:"Projection"`

	// IndexStatus is the GSI status (always "ACTIVE" in this emulator).
	IndexStatus string `json:"IndexStatus"`

	// ProvisionedThroughput holds read/write capacity for this GSI.
	ProvisionedThroughput DynamoDBProvisionedThroughputDesc `json:"ProvisionedThroughput"`

	// IndexARN is the Amazon Resource Name of the GSI.
	IndexARN string `json:"IndexARN,omitempty"`
}

// DynamoDBLocalSecondaryIndexDesc describes a local secondary index on a table.
type DynamoDBLocalSecondaryIndexDesc struct {
	// IndexName is the name of the LSI.
	IndexName string `json:"IndexName"`

	// KeySchema specifies the LSI key attributes.
	KeySchema []DynamoDBKeySchemaElement `json:"KeySchema"`

	// Projection describes which attributes are projected into the index.
	Projection DynamoDBProjection `json:"Projection"`

	// IndexARN is the Amazon Resource Name of the LSI.
	IndexARN string `json:"IndexARN,omitempty"`
}

// DynamoDBProjection specifies which attributes are projected into an index.
type DynamoDBProjection struct {
	// ProjectionType is one of "ALL", "KEYS_ONLY", or "INCLUDE".
	ProjectionType string `json:"ProjectionType"`

	// NonKeyAttributes lists attribute names to include when ProjectionType is "INCLUDE".
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}

// DynamoDBStreamSpecification describes a DynamoDB Streams configuration.
type DynamoDBStreamSpecification struct {
	// StreamEnabled indicates whether Streams is enabled.
	StreamEnabled bool `json:"StreamEnabled"`

	// StreamViewType is the view type: "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES", or "KEYS_ONLY".
	StreamViewType string `json:"StreamViewType,omitempty"`
}

// dynamodbItemKey encodes a DynamoDB item's primary key into a single state-key component.
// For hash-only tables, it returns pkVal. For hash+range tables, it returns "pkVal#skVal".
func dynamodbItemKey(pkVal, skVal string) string {
	if skVal == "" {
		return pkVal
	}
	return pkVal + "#" + skVal
}

// dynamodbTableARN constructs the ARN for a DynamoDB table.
func dynamodbTableARN(region, accountID, tableName string) string {
	return "arn:aws:dynamodb:" + region + ":" + accountID + ":table/" + tableName
}
