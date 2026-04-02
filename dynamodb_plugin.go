package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DynamoDBPlugin emulates the Amazon DynamoDB JSON-protocol API.
// It handles CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable,
// PutItem, GetItem, DeleteItem, UpdateItem, BatchGetItem, BatchWriteItem,
// Query, Scan, UpdateTimeToLive, DescribeTimeToLive, and Streams stubs.
type DynamoDBPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "dynamodb".
func (p *DynamoDBPlugin) Name() string { return "dynamodb" }

// Initialize sets up the DynamoDBPlugin with the provided configuration.
func (p *DynamoDBPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for DynamoDBPlugin.
func (p *DynamoDBPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a DynamoDB JSON-protocol request to the appropriate handler.
func (p *DynamoDBPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	// Table lifecycle.
	case "CreateTable":
		return p.createTable(ctx, req)
	case "DeleteTable":
		return p.deleteTable(ctx, req)
	case "DescribeTable":
		return p.describeTable(ctx, req)
	case "ListTables":
		return p.listTables(ctx, req)
	case "UpdateTable":
		return p.updateTable(ctx, req)
	// Item CRUD.
	case "PutItem":
		return p.putItem(ctx, req)
	case "GetItem":
		return p.getItem(ctx, req)
	case "DeleteItem":
		return p.deleteItem(ctx, req)
	case "UpdateItem":
		return p.updateItem(ctx, req)
	// Batch operations.
	case "BatchGetItem":
		return p.batchGetItem(ctx, req)
	case "BatchWriteItem":
		return p.batchWriteItem(ctx, req)
	// Query and Scan.
	case "Query":
		return p.query(ctx, req)
	case "Scan":
		return p.scan(ctx, req)
	// TTL.
	case "UpdateTimeToLive":
		return p.updateTimeToLive(ctx, req)
	case "DescribeTimeToLive":
		return p.describeTimeToLive(ctx, req)
	// Streams.
	case "ListStreams":
		return p.listStreams(ctx, req)
	case "DescribeStream":
		return p.describeStream(ctx, req)
	case "GetShardIterator":
		return p.getShardIterator(ctx, req)
	case "GetRecords":
		return p.getRecords(ctx, req)
	// Transactions.
	case "TransactGetItems":
		return p.transactGetItems(ctx, req)
	case "TransactWriteItems":
		return p.transactWriteItems(ctx, req)
	// PartiQL.
	case "ExecuteStatement":
		return p.executeStatement(ctx, req)
	case "BatchExecuteStatement":
		return p.batchExecuteStatement(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "UnknownOperationException",
			Message:    fmt.Sprintf("DynamoDBPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State helpers -----------------------------------------------------------

func (p *DynamoDBPlugin) tableStateKey(accountID, tableName string) string {
	return "table:" + accountID + "/" + tableName
}

func (p *DynamoDBPlugin) tableNamesKey(accountID string) string {
	return "table_names:" + accountID
}

func (p *DynamoDBPlugin) itemStateKey(accountID, tableName, itemKey string) string {
	return "item:" + accountID + "/" + tableName + "/" + itemKey
}

func (p *DynamoDBPlugin) itemKeysStateKey(accountID, tableName string) string {
	return "item_keys:" + accountID + "/" + tableName
}

func (p *DynamoDBPlugin) loadTable(ctx context.Context, accountID, tableName string) (*DynamoDBTable, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.tableStateKey(accountID, tableName))
	if err != nil {
		return nil, fmt.Errorf("dynamodb loadTable state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var tbl DynamoDBTable
	if err := json.Unmarshal(data, &tbl); err != nil {
		return nil, fmt.Errorf("dynamodb loadTable unmarshal: %w", err)
	}
	return &tbl, nil
}

func (p *DynamoDBPlugin) saveTable(ctx context.Context, accountID string, tbl *DynamoDBTable) error {
	data, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("dynamodb saveTable marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.tableStateKey(accountID, tbl.TableName), data)
}

func (p *DynamoDBPlugin) loadTableNames(ctx context.Context, accountID string) ([]string, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.tableNamesKey(accountID))
	if err != nil {
		return nil, fmt.Errorf("dynamodb loadTableNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("dynamodb loadTableNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *DynamoDBPlugin) saveTableNames(ctx context.Context, accountID string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("dynamodb saveTableNames marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.tableNamesKey(accountID), data)
}

func (p *DynamoDBPlugin) loadItemKeys(ctx context.Context, accountID, tableName string) ([]string, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.itemKeysStateKey(accountID, tableName))
	if err != nil {
		return nil, fmt.Errorf("dynamodb loadItemKeys: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("dynamodb loadItemKeys unmarshal: %w", err)
	}
	return keys, nil
}

func (p *DynamoDBPlugin) saveItemKeys(ctx context.Context, accountID, tableName string, keys []string) error {
	data, err := json.Marshal(keys)
	if err != nil {
		return fmt.Errorf("dynamodb saveItemKeys marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.itemKeysStateKey(accountID, tableName), data)
}

func (p *DynamoDBPlugin) loadItem(ctx context.Context, accountID, tableName, itemKey string) (map[string]*AttributeValue, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.itemStateKey(accountID, tableName, itemKey))
	if err != nil {
		return nil, fmt.Errorf("dynamodb loadItem: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var item map[string]*AttributeValue
	if err := json.Unmarshal(data, &item); err != nil {
		return nil, fmt.Errorf("dynamodb loadItem unmarshal: %w", err)
	}
	return item, nil
}

func (p *DynamoDBPlugin) saveItem(ctx context.Context, accountID, tableName, itemKey string, item map[string]*AttributeValue) error {
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("dynamodb saveItem marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.itemStateKey(accountID, tableName, itemKey), data)
}

func (p *DynamoDBPlugin) deleteItemByKey(ctx context.Context, accountID, tableName, itemKey string) error {
	return p.state.Delete(ctx, dynamodbNamespace, p.itemStateKey(accountID, tableName, itemKey))
}

// extractPrimaryKey extracts the PK and SK values from an item given the table's key schema.
// Returns (pkAttr, pkVal, skAttr, skVal, error).
func extractPrimaryKey(item map[string]*AttributeValue, keySchema []DynamoDBKeySchemaElement) (string, string, string, string, error) {
	var pkAttr, skAttr string
	for _, ks := range keySchema {
		switch ks.KeyType {
		case "HASH":
			pkAttr = ks.AttributeName
		case "RANGE":
			skAttr = ks.AttributeName
		}
	}
	if pkAttr == "" {
		return "", "", "", "", &AWSError{Code: "ValidationException", Message: "Table has no HASH key", HTTPStatus: http.StatusBadRequest}
	}
	pkAV, ok := item[pkAttr]
	if !ok || pkAV == nil {
		return "", "", "", "", &AWSError{Code: "ValidationException", Message: "One or more parameter values were invalid: Missing the key " + pkAttr + " in the item", HTTPStatus: http.StatusBadRequest}
	}
	pkVal := avToString(pkAV)
	var skVal string
	if skAttr != "" {
		skAV, skOk := item[skAttr]
		if !skOk || skAV == nil {
			return "", "", "", "", &AWSError{Code: "ValidationException", Message: "One or more parameter values were invalid: Missing the key " + skAttr + " in the item", HTTPStatus: http.StatusBadRequest}
		}
		skVal = avToString(skAV)
	}
	return pkAttr, pkVal, skAttr, skVal, nil
}

// avS safely dereferences an AttributeValue's S pointer, returning "" if nil.
func avS(av *AttributeValue) string {
	if av == nil || av.S == nil {
		return ""
	}
	return *av.S
}

// avToString returns a string representation of an AttributeValue for key usage.
func avToString(av *AttributeValue) string {
	if av == nil {
		return ""
	}
	if av.S != nil {
		return *av.S
	}
	if av.N != "" {
		return av.N
	}
	return ""
}

// --- Table lifecycle ---------------------------------------------------------

func (p *DynamoDBPlugin) createTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName              string                             `json:"TableName"`
		KeySchema              []DynamoDBKeySchemaElement         `json:"KeySchema"`
		AttributeDefinitions   []DynamoDBAttributeDefinition      `json:"AttributeDefinitions"`
		BillingMode            string                             `json:"BillingMode"`
		ProvisionedThroughput  *DynamoDBProvisionedThroughputDesc `json:"ProvisionedThroughput"`
		GlobalSecondaryIndexes []struct {
			IndexName             string                             `json:"IndexName"`
			KeySchema             []DynamoDBKeySchemaElement         `json:"KeySchema"`
			Projection            DynamoDBProjection                 `json:"Projection"`
			ProvisionedThroughput *DynamoDBProvisionedThroughputDesc `json:"ProvisionedThroughput"`
		} `json:"GlobalSecondaryIndexes"`
		LocalSecondaryIndexes []struct {
			IndexName  string                     `json:"IndexName"`
			KeySchema  []DynamoDBKeySchemaElement `json:"KeySchema"`
			Projection DynamoDBProjection         `json:"Projection"`
		} `json:"LocalSecondaryIndexes"`
		StreamSpecification *DynamoDBStreamSpecification `json:"StreamSpecification"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: "Failed to parse request: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "TableName is required", HTTPStatus: http.StatusBadRequest}
	}

	existing, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceInUseException", Message: "Table already exists: " + input.TableName, HTTPStatus: http.StatusConflict}
	}

	billingMode := input.BillingMode
	if billingMode == "" {
		billingMode = "PROVISIONED"
	}
	var pt DynamoDBProvisionedThroughputDesc
	if input.ProvisionedThroughput != nil {
		pt = *input.ProvisionedThroughput
	}

	now := p.tc.Now()
	tableARN := dynamodbTableARN(ctx.Region, ctx.AccountID, input.TableName)

	// Build GSIs.
	gsis := make([]DynamoDBGlobalSecondaryIndexDesc, 0, len(input.GlobalSecondaryIndexes))
	for _, g := range input.GlobalSecondaryIndexes {
		gpt := pt
		if g.ProvisionedThroughput != nil {
			gpt = *g.ProvisionedThroughput
		}
		gsis = append(gsis, DynamoDBGlobalSecondaryIndexDesc{
			IndexName:             g.IndexName,
			KeySchema:             g.KeySchema,
			Projection:            g.Projection,
			IndexStatus:           "ACTIVE",
			ProvisionedThroughput: gpt,
			IndexARN:              tableARN + "/index/" + g.IndexName,
		})
	}

	// Build LSIs.
	lsis := make([]DynamoDBLocalSecondaryIndexDesc, 0, len(input.LocalSecondaryIndexes))
	for _, l := range input.LocalSecondaryIndexes {
		lsis = append(lsis, DynamoDBLocalSecondaryIndexDesc{
			IndexName:  l.IndexName,
			KeySchema:  l.KeySchema,
			Projection: l.Projection,
			IndexARN:   tableARN + "/index/" + l.IndexName,
		})
	}

	tbl := &DynamoDBTable{
		TableName:              input.TableName,
		TableARN:               tableARN,
		TableStatus:            "ACTIVE",
		CreationDateTime:       float64(now.UnixNano()) / 1e9,
		KeySchema:              input.KeySchema,
		AttributeDefinitions:   input.AttributeDefinitions,
		BillingModeSummary:     DynamoDBBillingModeSummary{BillingMode: billingMode},
		ProvisionedThroughput:  pt,
		GlobalSecondaryIndexes: gsis,
		LocalSecondaryIndexes:  lsis,
		StreamSpecification:    input.StreamSpecification,
		TableSizeBytes:         0,
		ItemCount:              0,
	}

	// Generate stream ARN if streams enabled.
	if input.StreamSpecification != nil && input.StreamSpecification.StreamEnabled {
		tbl.LatestStreamARN = tableARN + "/stream/" + now.UTC().Format("2006-01-02T15:04:05.999")
	}

	if err := p.saveTable(context.Background(), ctx.AccountID, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb createTable saveTable: %w", err)
	}

	names, err := p.loadTableNames(context.Background(), ctx.AccountID)
	if err != nil {
		return nil, err
	}
	names = append(names, input.TableName)
	if err := p.saveTableNames(context.Background(), ctx.AccountID, names); err != nil {
		return nil, fmt.Errorf("dynamodb createTable saveTableNames: %w", err)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": tbl,
	})
}

func (p *DynamoDBPlugin) deleteTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	// Delete all items.
	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	for _, ik := range itemKeys {
		_ = p.deleteItemByKey(context.Background(), ctx.AccountID, input.TableName, ik)
	}
	_ = p.state.Delete(context.Background(), dynamodbNamespace, p.itemKeysStateKey(ctx.AccountID, input.TableName))

	// Delete table.
	_ = p.state.Delete(context.Background(), dynamodbNamespace, p.tableStateKey(ctx.AccountID, input.TableName))

	// Remove from names.
	names, err := p.loadTableNames(context.Background(), ctx.AccountID)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != input.TableName {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveTableNames(context.Background(), ctx.AccountID, newNames); err != nil {
		return nil, fmt.Errorf("dynamodb deleteTable saveTableNames: %w", err)
	}

	tbl.TableStatus = "DELETING"
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": tbl,
	})
}

func (p *DynamoDBPlugin) describeTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	// Refresh item count.
	itemKeys, _ := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
	tbl.ItemCount = int64(len(itemKeys))
	tbl.TableSizeBytes = tbl.ItemCount * 100

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Table": tbl,
	})
}

func (p *DynamoDBPlugin) listTables(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
		Limit                   int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	limit := input.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	names, err := p.loadTableNames(context.Background(), ctx.AccountID)
	if err != nil {
		return nil, err
	}

	// Apply ExclusiveStartTableName.
	start := 0
	if input.ExclusiveStartTableName != "" {
		for i, n := range names {
			if n == input.ExclusiveStartTableName {
				start = i + 1
				break
			}
		}
	}
	names = names[start:]

	var lastEvaluated string
	if len(names) > limit {
		lastEvaluated = names[limit-1]
		names = names[:limit]
	}

	result := map[string]interface{}{
		"TableNames": names,
	}
	if lastEvaluated != "" {
		result["LastEvaluatedTableName"] = lastEvaluated
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

func (p *DynamoDBPlugin) updateTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName             string                             `json:"TableName"`
		BillingMode           string                             `json:"BillingMode"`
		ProvisionedThroughput *DynamoDBProvisionedThroughputDesc `json:"ProvisionedThroughput"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	if input.BillingMode != "" {
		tbl.BillingModeSummary.BillingMode = input.BillingMode
	}
	if input.ProvisionedThroughput != nil {
		tbl.ProvisionedThroughput = *input.ProvisionedThroughput
	}

	if err := p.saveTable(context.Background(), ctx.AccountID, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb updateTable: %w", err)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": tbl,
	})
}

// --- Item CRUD ---------------------------------------------------------------

func (p *DynamoDBPlugin) putItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                 string                     `json:"TableName"`
		Item                      map[string]*AttributeValue `json:"Item"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
		ReturnValues              string                     `json:"ReturnValues"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	_, pkVal, _, skVal, err := extractPrimaryKey(input.Item, tbl.KeySchema)
	if err != nil {
		return nil, err
	}
	ik := dynamodbItemKey(pkVal, skVal)

	// Load old item for condition checking and ReturnValues.
	oldItem, err := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
	if err != nil {
		return nil, err
	}

	// Evaluate condition expression.
	if input.ConditionExpression != "" {
		ok, evalErr := evalDynamoCondition(input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, oldItem)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			return nil, &AWSError{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: http.StatusBadRequest}
		}
	}

	// Save new item.
	if err := p.saveItem(context.Background(), ctx.AccountID, input.TableName, ik, input.Item); err != nil {
		return nil, fmt.Errorf("dynamodb putItem saveItem: %w", err)
	}

	// Update item keys list if new.
	if oldItem == nil {
		itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
		if err != nil {
			return nil, err
		}
		itemKeys = append(itemKeys, ik)
		if err := p.saveItemKeys(context.Background(), ctx.AccountID, input.TableName, itemKeys); err != nil {
			return nil, fmt.Errorf("dynamodb putItem saveItemKeys: %w", err)
		}
	}

	// Append stream record if streams enabled.
	eventName := "MODIFY"
	if oldItem == nil {
		eventName = "INSERT"
	}
	p.appendStreamRecord(context.Background(), ctx.AccountID, input.TableName, eventName, oldItem, input.Item)

	result := map[string]interface{}{}
	if input.ReturnValues == "ALL_OLD" && oldItem != nil {
		result["Attributes"] = oldItem
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

func (p *DynamoDBPlugin) getItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                string                     `json:"TableName"`
		Key                      map[string]*AttributeValue `json:"Key"`
		ProjectionExpression     string                     `json:"ProjectionExpression"`
		ExpressionAttributeNames map[string]string          `json:"ExpressionAttributeNames"`
		ConsistentRead           bool                       `json:"ConsistentRead"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	_, pkVal, _, skVal, err := extractPrimaryKey(input.Key, tbl.KeySchema)
	if err != nil {
		return nil, err
	}
	ik := dynamodbItemKey(pkVal, skVal)

	item, err := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{}
	if item != nil {
		projected := applyProjection(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		result["Item"] = projected
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

func (p *DynamoDBPlugin) deleteItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                 string                     `json:"TableName"`
		Key                       map[string]*AttributeValue `json:"Key"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
		ReturnValues              string                     `json:"ReturnValues"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	_, pkVal, _, skVal, err := extractPrimaryKey(input.Key, tbl.KeySchema)
	if err != nil {
		return nil, err
	}
	ik := dynamodbItemKey(pkVal, skVal)

	oldItem, err := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
	if err != nil {
		return nil, err
	}

	// Evaluate condition expression.
	if input.ConditionExpression != "" {
		ok, evalErr := evalDynamoCondition(input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, oldItem)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			return nil, &AWSError{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: http.StatusBadRequest}
		}
	}

	if oldItem != nil {
		if err := p.deleteItemByKey(context.Background(), ctx.AccountID, input.TableName, ik); err != nil {
			return nil, fmt.Errorf("dynamodb deleteItem: %w", err)
		}
		// Remove from item keys.
		itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
		if err != nil {
			return nil, err
		}
		newKeys := make([]string, 0, len(itemKeys))
		for _, k := range itemKeys {
			if k != ik {
				newKeys = append(newKeys, k)
			}
		}
		if err := p.saveItemKeys(context.Background(), ctx.AccountID, input.TableName, newKeys); err != nil {
			return nil, fmt.Errorf("dynamodb deleteItem saveItemKeys: %w", err)
		}
		// Append stream record.
		p.appendStreamRecord(context.Background(), ctx.AccountID, input.TableName, "REMOVE", oldItem, nil)
	}

	result := map[string]interface{}{}
	if input.ReturnValues == "ALL_OLD" && oldItem != nil {
		result["Attributes"] = oldItem
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

func (p *DynamoDBPlugin) updateItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                 string                     `json:"TableName"`
		Key                       map[string]*AttributeValue `json:"Key"`
		UpdateExpression          string                     `json:"UpdateExpression"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
		ReturnValues              string                     `json:"ReturnValues"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	_, pkVal, _, skVal, err := extractPrimaryKey(input.Key, tbl.KeySchema)
	if err != nil {
		return nil, err
	}
	ik := dynamodbItemKey(pkVal, skVal)

	item, err := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
	if err != nil {
		return nil, err
	}

	isNew := item == nil
	if isNew {
		// Create item from key.
		item = make(map[string]*AttributeValue)
		for k, v := range input.Key {
			item[k] = v
		}
	}

	oldItem := copyItem(item)

	// Evaluate condition expression against current state.
	if input.ConditionExpression != "" {
		ok, evalErr := evalDynamoCondition(input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, oldItem)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			return nil, &AWSError{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: http.StatusBadRequest}
		}
	}

	// Apply update expression.
	if input.UpdateExpression != "" {
		if err := applyUpdateExpression(item, input.UpdateExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues); err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if err := p.saveItem(context.Background(), ctx.AccountID, input.TableName, ik, item); err != nil {
		return nil, fmt.Errorf("dynamodb updateItem saveItem: %w", err)
	}

	if isNew {
		itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
		if err != nil {
			return nil, err
		}
		itemKeys = append(itemKeys, ik)
		if err := p.saveItemKeys(context.Background(), ctx.AccountID, input.TableName, itemKeys); err != nil {
			return nil, fmt.Errorf("dynamodb updateItem saveItemKeys: %w", err)
		}
	}

	// Append stream record.
	updateEventName := "MODIFY"
	if isNew {
		updateEventName = "INSERT"
	}
	p.appendStreamRecord(context.Background(), ctx.AccountID, input.TableName, updateEventName, oldItem, item)

	result := map[string]interface{}{}
	switch input.ReturnValues {
	case "ALL_NEW":
		result["Attributes"] = item
	case "ALL_OLD":
		if !isNew {
			result["Attributes"] = oldItem
		}
	case "UPDATED_NEW":
		result["Attributes"] = item
	case "UPDATED_OLD":
		if !isNew {
			result["Attributes"] = oldItem
		}
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

// --- Batch operations --------------------------------------------------------

func (p *DynamoDBPlugin) batchGetItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		RequestItems map[string]struct {
			Keys                     []map[string]*AttributeValue `json:"Keys"`
			ProjectionExpression     string                       `json:"ProjectionExpression"`
			ExpressionAttributeNames map[string]string            `json:"ExpressionAttributeNames"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	responses := make(map[string][]map[string]*AttributeValue)

	for tableName, tableReq := range input.RequestItems {
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, tableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			continue
		}
		items := make([]map[string]*AttributeValue, 0)
		for _, key := range tableReq.Keys {
			_, pkVal, _, skVal, err := extractPrimaryKey(key, tbl.KeySchema)
			if err != nil {
				continue
			}
			ik := dynamodbItemKey(pkVal, skVal)
			item, err := p.loadItem(context.Background(), ctx.AccountID, tableName, ik)
			if err != nil || item == nil {
				continue
			}
			projected := applyProjection(item, tableReq.ProjectionExpression, tableReq.ExpressionAttributeNames)
			items = append(items, projected)
		}
		responses[tableName] = items
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Responses":       responses,
		"UnprocessedKeys": map[string]interface{}{},
	})
}

func (p *DynamoDBPlugin) batchWriteItem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		RequestItems map[string][]struct {
			PutRequest struct {
				Item map[string]*AttributeValue `json:"Item"`
			} `json:"PutRequest"`
			DeleteRequest struct {
				Key map[string]*AttributeValue `json:"Key"`
			} `json:"DeleteRequest"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	for tableName, requests := range input.RequestItems {
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, tableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + tableName, HTTPStatus: http.StatusBadRequest}
		}

		for _, writeReq := range requests {
			if writeReq.PutRequest.Item != nil {
				_, pkVal, _, skVal, err := extractPrimaryKey(writeReq.PutRequest.Item, tbl.KeySchema)
				if err != nil {
					return nil, err
				}
				ik := dynamodbItemKey(pkVal, skVal)
				existing, _ := p.loadItem(context.Background(), ctx.AccountID, tableName, ik)
				if err := p.saveItem(context.Background(), ctx.AccountID, tableName, ik, writeReq.PutRequest.Item); err != nil {
					return nil, fmt.Errorf("dynamodb batchWriteItem put: %w", err)
				}
				if existing == nil {
					itemKeys, _ := p.loadItemKeys(context.Background(), ctx.AccountID, tableName)
					itemKeys = append(itemKeys, ik)
					_ = p.saveItemKeys(context.Background(), ctx.AccountID, tableName, itemKeys)
				}
			} else if writeReq.DeleteRequest.Key != nil {
				_, pkVal, _, skVal, err := extractPrimaryKey(writeReq.DeleteRequest.Key, tbl.KeySchema)
				if err != nil {
					return nil, err
				}
				ik := dynamodbItemKey(pkVal, skVal)
				existing, _ := p.loadItem(context.Background(), ctx.AccountID, tableName, ik)
				if existing != nil {
					_ = p.deleteItemByKey(context.Background(), ctx.AccountID, tableName, ik)
					itemKeys, _ := p.loadItemKeys(context.Background(), ctx.AccountID, tableName)
					newKeys := make([]string, 0, len(itemKeys))
					for _, k := range itemKeys {
						if k != ik {
							newKeys = append(newKeys, k)
						}
					}
					_ = p.saveItemKeys(context.Background(), ctx.AccountID, tableName, newKeys)
				}
			}
		}
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"UnprocessedItems": map[string]interface{}{},
	})
}

// --- Transact operations -----------------------------------------------------

// transactGetItems implements DynamoDB TransactGetItems.
// It returns an array of item responses in the same order as the request.
func (p *DynamoDBPlugin) transactGetItems(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TransactItems []struct {
			Get *struct {
				TableName                string                     `json:"TableName"`
				Key                      map[string]*AttributeValue `json:"Key"`
				ProjectionExpression     string                     `json:"ProjectionExpression"`
				ExpressionAttributeNames map[string]string          `json:"ExpressionAttributeNames"`
			} `json:"Get"`
		} `json:"TransactItems"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	responses := make([]map[string]interface{}, 0, len(input.TransactItems))
	for _, ti := range input.TransactItems {
		if ti.Get == nil {
			responses = append(responses, map[string]interface{}{})
			continue
		}
		g := ti.Get
		tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, g.TableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + g.TableName, HTTPStatus: http.StatusBadRequest}
		}
		_, pkVal, _, skVal, err := extractPrimaryKey(g.Key, tbl.KeySchema)
		if err != nil {
			return nil, err
		}
		item, err := p.loadItem(context.Background(), reqCtx.AccountID, g.TableName, dynamodbItemKey(pkVal, skVal))
		if err != nil {
			return nil, err
		}
		entry := map[string]interface{}{}
		if item != nil {
			entry["Item"] = applyProjection(item, g.ProjectionExpression, g.ExpressionAttributeNames)
		}
		responses = append(responses, entry)
	}
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{"Responses": responses})
}

// transactWriteItems implements DynamoDB TransactWriteItems.
// All condition checks are evaluated first; if any fail the entire transaction is
// cancelled and TransactionCanceledException is returned with CancellationReasons.
// If all conditions pass, all mutations are applied atomically.
func (p *DynamoDBPlugin) transactWriteItems(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TransactItems []struct {
			Put *struct {
				TableName                 string                     `json:"TableName"`
				Item                      map[string]*AttributeValue `json:"Item"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Put"`
			Update *struct {
				TableName                 string                     `json:"TableName"`
				Key                       map[string]*AttributeValue `json:"Key"`
				UpdateExpression          string                     `json:"UpdateExpression"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Update"`
			Delete *struct {
				TableName                 string                     `json:"TableName"`
				Key                       map[string]*AttributeValue `json:"Key"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Delete"`
			ConditionCheck *struct {
				TableName                 string                     `json:"TableName"`
				Key                       map[string]*AttributeValue `json:"Key"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"ConditionCheck"`
		} `json:"TransactItems"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Phase 1: evaluate all condition expressions.
	type cancellationReason struct {
		Code    string `json:"Code"`
		Message string `json:"Message,omitempty"`
	}
	reasons := make([]cancellationReason, len(input.TransactItems))
	cancelled := false

	for i, ti := range input.TransactItems {
		var tableName string
		var key map[string]*AttributeValue
		var condExpr string
		var names map[string]string
		var vals map[string]*AttributeValue

		switch {
		case ti.Put != nil:
			tableName = ti.Put.TableName
			key = ti.Put.Item
			condExpr = ti.Put.ConditionExpression
			names = ti.Put.ExpressionAttributeNames
			vals = ti.Put.ExpressionAttributeValues
		case ti.Update != nil:
			tableName = ti.Update.TableName
			key = ti.Update.Key
			condExpr = ti.Update.ConditionExpression
			names = ti.Update.ExpressionAttributeNames
			vals = ti.Update.ExpressionAttributeValues
		case ti.Delete != nil:
			tableName = ti.Delete.TableName
			key = ti.Delete.Key
			condExpr = ti.Delete.ConditionExpression
			names = ti.Delete.ExpressionAttributeNames
			vals = ti.Delete.ExpressionAttributeValues
		case ti.ConditionCheck != nil:
			tableName = ti.ConditionCheck.TableName
			key = ti.ConditionCheck.Key
			condExpr = ti.ConditionCheck.ConditionExpression
			names = ti.ConditionCheck.ExpressionAttributeNames
			vals = ti.ConditionCheck.ExpressionAttributeValues
		}

		reasons[i] = cancellationReason{Code: "None"}

		if condExpr == "" {
			continue
		}

		tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, tableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + tableName, HTTPStatus: http.StatusBadRequest}
		}

		_, pkVal, _, skVal, keyErr := extractPrimaryKey(key, tbl.KeySchema)
		if keyErr != nil {
			return nil, keyErr
		}
		currentItem, loadErr := p.loadItem(context.Background(), reqCtx.AccountID, tableName, dynamodbItemKey(pkVal, skVal))
		if loadErr != nil {
			return nil, loadErr
		}

		ok, evalErr := evalDynamoCondition(condExpr, names, vals, currentItem)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			reasons[i] = cancellationReason{Code: "ConditionalCheckFailed", Message: "The conditional request failed"}
			cancelled = true
		}
	}

	if cancelled {
		body, _ := json.Marshal(map[string]interface{}{
			"__type":              "TransactionCanceledException",
			"message":             "Transaction cancelled, please refer cancellation reasons for specific reasons",
			"CancellationReasons": reasons,
		})
		return &AWSResponse{
			StatusCode: http.StatusBadRequest,
			Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
			Body:       body,
		}, nil
	}

	// Phase 2: apply all mutations.
	for _, ti := range input.TransactItems {
		switch {
		case ti.Put != nil:
			put := ti.Put
			tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, put.TableName)
			if err != nil {
				return nil, err
			}
			if tbl == nil {
				return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + put.TableName, HTTPStatus: http.StatusBadRequest}
			}
			_, pkVal, _, skVal, err := extractPrimaryKey(put.Item, tbl.KeySchema)
			if err != nil {
				return nil, err
			}
			ik := dynamodbItemKey(pkVal, skVal)
			old, _ := p.loadItem(context.Background(), reqCtx.AccountID, put.TableName, ik)
			if err := p.saveItem(context.Background(), reqCtx.AccountID, put.TableName, ik, put.Item); err != nil {
				return nil, fmt.Errorf("transactWriteItems put saveItem: %w", err)
			}
			if old == nil {
				keys, _ := p.loadItemKeys(context.Background(), reqCtx.AccountID, put.TableName)
				_ = p.saveItemKeys(context.Background(), reqCtx.AccountID, put.TableName, append(keys, ik))
			}
			eventName := "MODIFY"
			if old == nil {
				eventName = "INSERT"
			}
			p.appendStreamRecord(context.Background(), reqCtx.AccountID, put.TableName, eventName, old, put.Item)

		case ti.Update != nil:
			upd := ti.Update
			tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, upd.TableName)
			if err != nil {
				return nil, err
			}
			if tbl == nil {
				return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + upd.TableName, HTTPStatus: http.StatusBadRequest}
			}
			_, pkVal, _, skVal, err := extractPrimaryKey(upd.Key, tbl.KeySchema)
			if err != nil {
				return nil, err
			}
			ik := dynamodbItemKey(pkVal, skVal)
			item, _ := p.loadItem(context.Background(), reqCtx.AccountID, upd.TableName, ik)
			isNew := item == nil
			if isNew {
				item = make(map[string]*AttributeValue)
				for k, v := range upd.Key {
					item[k] = v
				}
			}
			old := copyItem(item)
			if upd.UpdateExpression != "" {
				if err := applyUpdateExpression(item, upd.UpdateExpression, upd.ExpressionAttributeNames, upd.ExpressionAttributeValues); err != nil {
					return nil, &AWSError{Code: "ValidationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
				}
			}
			if err := p.saveItem(context.Background(), reqCtx.AccountID, upd.TableName, ik, item); err != nil {
				return nil, fmt.Errorf("transactWriteItems update saveItem: %w", err)
			}
			if isNew {
				keys, _ := p.loadItemKeys(context.Background(), reqCtx.AccountID, upd.TableName)
				_ = p.saveItemKeys(context.Background(), reqCtx.AccountID, upd.TableName, append(keys, ik))
			}
			p.appendStreamRecord(context.Background(), reqCtx.AccountID, upd.TableName, "MODIFY", old, item)

		case ti.Delete != nil:
			del := ti.Delete
			tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, del.TableName)
			if err != nil {
				return nil, err
			}
			if tbl == nil {
				return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + del.TableName, HTTPStatus: http.StatusBadRequest}
			}
			_, pkVal, _, skVal, err := extractPrimaryKey(del.Key, tbl.KeySchema)
			if err != nil {
				return nil, err
			}
			ik := dynamodbItemKey(pkVal, skVal)
			old, _ := p.loadItem(context.Background(), reqCtx.AccountID, del.TableName, ik)
			if old != nil {
				if err := p.deleteItemByKey(context.Background(), reqCtx.AccountID, del.TableName, ik); err != nil {
					return nil, fmt.Errorf("transactWriteItems delete: %w", err)
				}
				keys, _ := p.loadItemKeys(context.Background(), reqCtx.AccountID, del.TableName)
				newKeys := make([]string, 0, len(keys))
				for _, k := range keys {
					if k != ik {
						newKeys = append(newKeys, k)
					}
				}
				_ = p.saveItemKeys(context.Background(), reqCtx.AccountID, del.TableName, newKeys)
				p.appendStreamRecord(context.Background(), reqCtx.AccountID, del.TableName, "REMOVE", old, nil)
			}
		}
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{})
}

// --- Query and Scan ----------------------------------------------------------

func (p *DynamoDBPlugin) scan(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                 string                     `json:"TableName"`
		FilterExpression          string                     `json:"FilterExpression"`
		ProjectionExpression      string                     `json:"ProjectionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
		Limit                     int                        `json:"Limit"`
		ExclusiveStartKey         map[string]*AttributeValue `json:"ExclusiveStartKey"`
		IndexName                 string                     `json:"IndexName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}

	// Find starting position for pagination.
	startIdx := 0
	if input.ExclusiveStartKey != nil {
		_, pkVal, _, skVal, _ := extractPrimaryKey(input.ExclusiveStartKey, tbl.KeySchema)
		startKey := dynamodbItemKey(pkVal, skVal)
		for i, k := range itemKeys {
			if k == startKey {
				startIdx = i + 1
				break
			}
		}
	}

	var items []map[string]*AttributeValue
	var lastEvaluatedKey map[string]*AttributeValue
	scannedCount := 0

	for i := startIdx; i < len(itemKeys); i++ {
		item, err := p.loadItem(context.Background(), ctx.AccountID, input.TableName, itemKeys[i])
		if err != nil || item == nil {
			continue
		}
		scannedCount++

		// Apply filter.
		if input.FilterExpression != "" {
			match, evalErr := evalDynamoCondition(input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, item)
			if evalErr != nil {
				return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
			}
			if !match {
				continue
			}
		}

		projected := applyProjection(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		items = append(items, projected)

		if input.Limit > 0 && len(items) >= input.Limit {
			// Set LastEvaluatedKey if there are more items.
			if i+1 < len(itemKeys) {
				lastEvaluatedKey = extractKeyFromItem(item, tbl.KeySchema)
			}
			break
		}
	}

	if items == nil {
		items = []map[string]*AttributeValue{}
	}

	result := map[string]interface{}{
		"Items":        items,
		"Count":        len(items),
		"ScannedCount": scannedCount,
	}
	if lastEvaluatedKey != nil {
		result["LastEvaluatedKey"] = lastEvaluatedKey
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

func (p *DynamoDBPlugin) query(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                 string                     `json:"TableName"`
		IndexName                 string                     `json:"IndexName"`
		KeyConditionExpression    string                     `json:"KeyConditionExpression"`
		FilterExpression          string                     `json:"FilterExpression"`
		ProjectionExpression      string                     `json:"ProjectionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
		ScanIndexForward          *bool                      `json:"ScanIndexForward"`
		Limit                     int                        `json:"Limit"`
		ExclusiveStartKey         map[string]*AttributeValue `json:"ExclusiveStartKey"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	// Determine the key schema to use (table or index).
	keySchema := tbl.KeySchema
	if input.IndexName != "" {
		keySchema = findIndexKeySchema(tbl, input.IndexName)
		if keySchema == nil {
			return nil, &AWSError{Code: "ValidationException", Message: "The table does not have the specified index: " + input.IndexName, HTTPStatus: http.StatusBadRequest}
		}
	}

	// Parse KeyConditionExpression.
	pkAttr, pkVal, skAttr, skCondOp, skLow, skHigh, err := parseKeyCondition(input.KeyConditionExpression, keySchema, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	if err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Load all item keys.
	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}

	// Filter items matching PK (and SK condition if present).
	var matchingKeys []string
	for _, ik := range itemKeys {
		item, loadErr := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
		if loadErr != nil || item == nil {
			continue
		}

		// Check PK match.
		pkAV := item[pkAttr]
		if pkAV == nil || avToString(pkAV) != pkVal {
			continue
		}

		// Check SK condition.
		if skAttr != "" && skCondOp != "" {
			skAV := item[skAttr]
			if skAV == nil {
				continue
			}
			if !evalSKCondition(avToString(skAV), skCondOp, skLow, skHigh) {
				continue
			}
		}

		matchingKeys = append(matchingKeys, ik)
	}

	// Sort by SK (ScanIndexForward).
	// Use the schema's sort key for ordering even when there is no SK condition.
	sortKey := skAttr
	if sortKey == "" {
		for _, ks := range keySchema {
			if ks.KeyType == "RANGE" {
				sortKey = ks.AttributeName
				break
			}
		}
	}
	forward := input.ScanIndexForward == nil || *input.ScanIndexForward
	if sortKey != "" {
		sort.Slice(matchingKeys, func(i, j int) bool {
			itemI, _ := p.loadItem(context.Background(), ctx.AccountID, input.TableName, matchingKeys[i])
			itemJ, _ := p.loadItem(context.Background(), ctx.AccountID, input.TableName, matchingKeys[j])
			if itemI == nil || itemJ == nil {
				return false
			}
			skI := avToString(itemI[sortKey])
			skJ := avToString(itemJ[sortKey])
			if forward {
				return skI < skJ
			}
			return skI > skJ
		})
	}

	// Apply pagination start.
	startIdx := 0
	if input.ExclusiveStartKey != nil {
		_, startPK, _, startSK, _ := extractPrimaryKey(input.ExclusiveStartKey, tbl.KeySchema)
		startKey := dynamodbItemKey(startPK, startSK)
		for i, k := range matchingKeys {
			if k == startKey {
				startIdx = i + 1
				break
			}
		}
	}
	matchingKeys = matchingKeys[startIdx:]

	var items []map[string]*AttributeValue
	var lastEvaluatedKey map[string]*AttributeValue
	scannedCount := 0

	for i, ik := range matchingKeys {
		item, loadErr := p.loadItem(context.Background(), ctx.AccountID, input.TableName, ik)
		if loadErr != nil || item == nil {
			continue
		}
		scannedCount++

		// Apply filter expression.
		if input.FilterExpression != "" {
			match, evalErr := evalDynamoCondition(input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, item)
			if evalErr != nil {
				return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
			}
			if !match {
				continue
			}
		}

		projected := applyProjection(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		items = append(items, projected)

		if input.Limit > 0 && len(items) >= input.Limit {
			if i+1 < len(matchingKeys) {
				lastEvaluatedKey = extractKeyFromItem(item, tbl.KeySchema)
			}
			break
		}
	}

	if items == nil {
		items = []map[string]*AttributeValue{}
	}

	result := map[string]interface{}{
		"Items":        items,
		"Count":        len(items),
		"ScannedCount": scannedCount,
	}
	if lastEvaluatedKey != nil {
		result["LastEvaluatedKey"] = lastEvaluatedKey
	}
	return dynamodbJSONResponse(http.StatusOK, result)
}

// --- TTL -------------------------------------------------------------------

func (p *DynamoDBPlugin) updateTimeToLive(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName               string `json:"TableName"`
		TimeToLiveSpecification struct {
			Enabled       bool   `json:"Enabled"`
			AttributeName string `json:"AttributeName"`
		} `json:"TimeToLiveSpecification"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	if input.TimeToLiveSpecification.Enabled {
		tbl.TTLAttribute = input.TimeToLiveSpecification.AttributeName
	} else {
		tbl.TTLAttribute = ""
	}

	if err := p.saveTable(context.Background(), ctx.AccountID, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb updateTimeToLive: %w", err)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TimeToLiveSpecification": map[string]interface{}{
			"Enabled":       input.TimeToLiveSpecification.Enabled,
			"AttributeName": input.TimeToLiveSpecification.AttributeName,
		},
	})
}

func (p *DynamoDBPlugin) describeTimeToLive(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	status := "DISABLED"
	attrName := ""
	if tbl.TTLAttribute != "" {
		status = "ENABLED"
		attrName = tbl.TTLAttribute
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TimeToLiveDescription": map[string]interface{}{
			"TimeToLiveStatus": status,
			"AttributeName":    attrName,
		},
	})
}

// --- Streams -----------------------------------------------------------------

// dynamodbMaxStreamRecords is the maximum records kept in a stream ring buffer.
const dynamodbMaxStreamRecords = 1000

// DynamoDBStreamRecord is a single change-data-capture record appended to a
// table's stream ring buffer by putItem, updateItem, and deleteItem.
type DynamoDBStreamRecord struct {
	// EventID is a unique identifier for this stream event.
	EventID string `json:"eventID"`

	// EventName is INSERT, MODIFY, or REMOVE.
	EventName string `json:"eventName"`

	// TableName is the table that generated this record.
	TableName string `json:"tableName"`

	// Sequence is a monotonically increasing counter within the ring buffer.
	Sequence int64 `json:"sequence"`

	// OldImage is the item before the change (nil for INSERT).
	OldImage map[string]*AttributeValue `json:"oldImage,omitempty"`

	// NewImage is the item after the change (nil for REMOVE).
	NewImage map[string]*AttributeValue `json:"newImage,omitempty"`

	// ApproxTimestamp is the Unix millisecond timestamp of the event.
	ApproxTimestamp int64 `json:"approximateCreationDateTime"`
}

// DynamoDBStreamCursor is the parsed body of a base64-encoded shard iterator.
type DynamoDBStreamCursor struct {
	// TableName identifies which table's ring buffer to read.
	TableName string `json:"tableName"`

	// AccountID is the owning account.
	AccountID string `json:"accountId"`

	// Sequence is the position in the ring buffer (next to read).
	Sequence int64 `json:"sequence"`

	// IterType is the iterator type (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER).
	IterType string `json:"iteratorType"`
}

func (p *DynamoDBPlugin) streamRecordsKey(accountID, tableName string) string {
	return "stream_records:" + accountID + "/" + tableName
}

// appendStreamRecord adds a CDC record to a table's stream ring buffer if
// streams are enabled for that table. Errors are logged and suppressed so
// that main item operations never fail due to stream problems.
func (p *DynamoDBPlugin) appendStreamRecord(
	ctx context.Context,
	accountID, tableName, eventName string,
	oldImage, newImage map[string]*AttributeValue,
) {
	tbl, err := p.loadTable(ctx, accountID, tableName)
	if err != nil || tbl == nil || tbl.LatestStreamARN == "" {
		return // streams not enabled
	}

	rk := p.streamRecordsKey(accountID, tableName)
	data, _ := p.state.Get(ctx, dynamodbNamespace, rk)
	var records []DynamoDBStreamRecord
	if data != nil {
		_ = json.Unmarshal(data, &records)
	}

	var seq int64
	if len(records) > 0 {
		seq = records[len(records)-1].Sequence + 1
	}

	records = append(records, DynamoDBStreamRecord{
		EventID:         fmt.Sprintf("%s-%d", tableName, seq),
		EventName:       eventName,
		TableName:       tableName,
		Sequence:        seq,
		OldImage:        oldImage,
		NewImage:        newImage,
		ApproxTimestamp: time.Now().UnixMilli(),
	})

	if len(records) > dynamodbMaxStreamRecords {
		records = records[len(records)-dynamodbMaxStreamRecords:]
	}

	if b, marshalErr := json.Marshal(records); marshalErr == nil {
		_ = p.state.Put(ctx, dynamodbNamespace, rk, b)
	}
}

func (p *DynamoDBPlugin) listStreams(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	var streams []map[string]string

	if input.TableName != "" {
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, input.TableName)
		if err != nil {
			return nil, err
		}
		if tbl != nil && tbl.LatestStreamARN != "" {
			streams = []map[string]string{
				{
					"StreamArn":   tbl.LatestStreamARN,
					"TableName":   tbl.TableName,
					"StreamLabel": tbl.LatestStreamARN[strings.LastIndex(tbl.LatestStreamARN, "/")+1:],
				},
			}
		}
	} else {
		names, _ := p.loadTableNames(context.Background(), ctx.AccountID)
		for _, name := range names {
			tbl, err := p.loadTable(context.Background(), ctx.AccountID, name)
			if err != nil || tbl == nil || tbl.LatestStreamARN == "" {
				continue
			}
			streams = append(streams, map[string]string{
				"StreamArn":   tbl.LatestStreamARN,
				"TableName":   tbl.TableName,
				"StreamLabel": tbl.LatestStreamARN[strings.LastIndex(tbl.LatestStreamARN, "/")+1:],
			})
		}
	}

	if streams == nil {
		streams = []map[string]string{}
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Streams": streams,
	})
}

func (p *DynamoDBPlugin) describeStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StreamArn string `json:"StreamArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Find the table for this stream ARN.
	tableName := dynamodbTableNameFromStreamARN(input.StreamArn)

	shardID := "shardId-00000000000000000000-00000001"
	shard := map[string]interface{}{
		"ShardId": shardID,
		"SequenceNumberRange": map[string]interface{}{
			"StartingSequenceNumber": "0",
		},
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamDescription": map[string]interface{}{
			"StreamArn":    input.StreamArn,
			"StreamStatus": "ENABLED",
			"TableName":    tableName,
			"Shards":       []interface{}{shard},
		},
	})
}

func (p *DynamoDBPlugin) getShardIterator(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		StreamArn         string `json:"StreamArn"`
		ShardID           string `json:"ShardId"`
		ShardIteratorType string `json:"ShardIteratorType"`
		SequenceNumber    string `json:"SequenceNumber"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tableName := dynamodbTableNameFromStreamARN(input.StreamArn)

	var startSeq int64
	switch input.ShardIteratorType {
	case "LATEST":
		// Start after the last record.
		rk := p.streamRecordsKey(ctx.AccountID, tableName)
		data, _ := p.state.Get(context.Background(), dynamodbNamespace, rk)
		var records []DynamoDBStreamRecord
		if data != nil {
			_ = json.Unmarshal(data, &records)
		}
		if len(records) > 0 {
			startSeq = records[len(records)-1].Sequence + 1
		}
	case "AT_SEQUENCE_NUMBER":
		n, _ := strconv.ParseInt(input.SequenceNumber, 10, 64)
		startSeq = n
	case "AFTER_SEQUENCE_NUMBER":
		n, _ := strconv.ParseInt(input.SequenceNumber, 10, 64)
		startSeq = n + 1
	default: // TRIM_HORIZON
		startSeq = 0
	}

	cursor := DynamoDBStreamCursor{
		TableName: tableName,
		AccountID: ctx.AccountID,
		Sequence:  startSeq,
		IterType:  input.ShardIteratorType,
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("marshal stream cursor: %w", err)
	}
	iterToken := base64.StdEncoding.EncodeToString(b)

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"ShardIterator": iterToken,
	})
}

func (p *DynamoDBPlugin) getRecords(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ShardIterator string `json:"ShardIterator"`
		Limit         int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	if input.ShardIterator == "" {
		return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
			"Records": []interface{}{},
		})
	}

	b, err := base64.StdEncoding.DecodeString(input.ShardIterator)
	if err != nil {
		// Handle legacy stub iterators gracefully.
		return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
			"Records": []interface{}{},
		})
	}

	var cursor DynamoDBStreamCursor
	if err := json.Unmarshal(b, &cursor); err != nil {
		return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
			"Records": []interface{}{},
		})
	}

	rk := p.streamRecordsKey(cursor.AccountID, cursor.TableName)
	data, _ := p.state.Get(context.Background(), dynamodbNamespace, rk)
	var allRecords []DynamoDBStreamRecord
	if data != nil {
		_ = json.Unmarshal(data, &allRecords)
	}

	// Filter records from cursor.Sequence onwards.
	var matchedRecords []DynamoDBStreamRecord
	for _, r := range allRecords {
		if r.Sequence >= cursor.Sequence {
			matchedRecords = append(matchedRecords, r)
		}
	}

	limit := 1000
	if input.Limit > 0 && input.Limit < limit {
		limit = input.Limit
	}
	if len(matchedRecords) > limit {
		matchedRecords = matchedRecords[:limit]
	}

	// Advance cursor.
	var nextSeq int64
	if len(matchedRecords) > 0 {
		nextSeq = matchedRecords[len(matchedRecords)-1].Sequence + 1
	} else {
		nextSeq = cursor.Sequence
	}
	cursor.Sequence = nextSeq
	nextCursorBytes, _ := json.Marshal(cursor)
	nextIterator := base64.StdEncoding.EncodeToString(nextCursorBytes)

	// Convert to DynamoDB Streams wire format.
	wireRecords := make([]map[string]interface{}, 0, len(matchedRecords))
	for _, r := range matchedRecords {
		rec := map[string]interface{}{
			"eventID":                     r.EventID,
			"eventName":                   r.EventName,
			"eventSource":                 "aws:dynamodb",
			"eventVersion":                "1.1",
			"approximateCreationDateTime": float64(r.ApproxTimestamp) / 1000.0,
			"dynamodb": map[string]interface{}{
				"TableName":      r.TableName,
				"SequenceNumber": strconv.FormatInt(r.Sequence, 10),
				"SizeBytes":      64,
				"StreamViewType": "NEW_AND_OLD_IMAGES",
			},
		}
		dynamoRecord := rec["dynamodb"].(map[string]interface{})
		if r.NewImage != nil {
			dynamoRecord["NewImage"] = r.NewImage
		}
		if r.OldImage != nil {
			dynamoRecord["OldImage"] = r.OldImage
		}
		wireRecords = append(wireRecords, rec)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Records":           wireRecords,
		"NextShardIterator": nextIterator,
	})
}

// dynamodbTableNameFromStreamARN extracts the table name from a DynamoDB stream ARN.
// ARN format: arn:aws:dynamodb:{region}:{account}:table/{name}/stream/{label}.
func dynamodbTableNameFromStreamARN(arn string) string {
	// Find "table/" prefix.
	const tablePrefix = "table/"
	idx := strings.Index(arn, tablePrefix)
	if idx < 0 {
		return arn
	}
	rest := arn[idx+len(tablePrefix):]
	// Find the next "/" to get just the table name.
	if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
		return rest[:slashIdx]
	}
	return rest
}

// --- Expression helpers ------------------------------------------------------

// evalDynamoCondition evaluates a DynamoDB condition expression against an item.
// Returns true if the item matches the expression, false otherwise.
// item may be nil (e.g., for attribute_not_exists checks).
func evalDynamoCondition(expr string, names map[string]string, values map[string]*AttributeValue, item map[string]*AttributeValue) (bool, error) {
	if expr == "" {
		return true, nil
	}
	tokens, err := tokenizeDynamo(expr)
	if err != nil {
		return false, err
	}
	ev := &condEvaluator{
		tokens: tokens,
		names:  names,
		values: values,
		item:   item,
	}
	result, err := ev.evalOr()
	if err != nil {
		return false, err
	}
	if ev.pos < len(ev.tokens) {
		return false, fmt.Errorf("unexpected token: %q", ev.tokens[ev.pos].value)
	}
	return result, nil
}

// dynamoToken holds a single lexical token from a DynamoDB expression.
type dynamoToken struct {
	typ   dynamoTokenType
	value string
}

// dynamoTokenType classifies lexical tokens in DynamoDB expressions.
type dynamoTokenType int

const (
	dynTokEOF dynamoTokenType = iota
	dynTokIdent
	dynTokExprName  // #name
	dynTokExprValue // :val
	dynTokEQ        // =
	dynTokNE        // <>
	dynTokLT        // <
	dynTokLE        // <=
	dynTokGT        // >
	dynTokGE        // >=
	dynTokLParen    // (
	dynTokRParen    // )
	dynTokComma     // ,
	dynTokDot       // .
	dynTokLBracket  // [
	dynTokRBracket  // ]
	dynTokNumber    // numeric literal
)

func tokenizeDynamo(expr string) ([]dynamoToken, error) {
	var tokens []dynamoToken
	i := 0
	for i < len(expr) {
		ch := expr[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i++
			continue
		}
		switch {
		case ch == '#':
			j := i + 1
			for j < len(expr) && (isAlphaNum(expr[j]) || expr[j] == '_') {
				j++
			}
			tokens = append(tokens, dynamoToken{dynTokExprName, expr[i:j]})
			i = j
		case ch == ':':
			j := i + 1
			for j < len(expr) && (isAlphaNum(expr[j]) || expr[j] == '_') {
				j++
			}
			tokens = append(tokens, dynamoToken{dynTokExprValue, expr[i:j]})
			i = j
		case ch == '=':
			tokens = append(tokens, dynamoToken{dynTokEQ, "="})
			i++
		case ch == '<':
			if i+1 < len(expr) && expr[i+1] == '>' {
				tokens = append(tokens, dynamoToken{dynTokNE, "<>"})
				i += 2
			} else if i+1 < len(expr) && expr[i+1] == '=' {
				tokens = append(tokens, dynamoToken{dynTokLE, "<="})
				i += 2
			} else {
				tokens = append(tokens, dynamoToken{dynTokLT, "<"})
				i++
			}
		case ch == '>':
			if i+1 < len(expr) && expr[i+1] == '=' {
				tokens = append(tokens, dynamoToken{dynTokGE, ">="})
				i += 2
			} else {
				tokens = append(tokens, dynamoToken{dynTokGT, ">"})
				i++
			}
		case ch == '(':
			tokens = append(tokens, dynamoToken{dynTokLParen, "("})
			i++
		case ch == ')':
			tokens = append(tokens, dynamoToken{dynTokRParen, ")"})
			i++
		case ch == ',':
			tokens = append(tokens, dynamoToken{dynTokComma, ","})
			i++
		case ch == '.':
			tokens = append(tokens, dynamoToken{dynTokDot, "."})
			i++
		case ch == '[':
			tokens = append(tokens, dynamoToken{dynTokLBracket, "["})
			i++
		case ch == ']':
			tokens = append(tokens, dynamoToken{dynTokRBracket, "]"})
			i++
		case ch >= '0' && ch <= '9':
			j := i
			for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
				j++
			}
			tokens = append(tokens, dynamoToken{dynTokNumber, expr[i:j]})
			i = j
		case isAlpha(ch) || ch == '_':
			j := i
			for j < len(expr) && (isAlphaNum(expr[j]) || expr[j] == '_') {
				j++
			}
			tokens = append(tokens, dynamoToken{dynTokIdent, expr[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q in expression", ch)
		}
	}
	return tokens, nil
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isAlphaNum(ch byte) bool {
	return isAlpha(ch) || (ch >= '0' && ch <= '9')
}

// condEvaluator evaluates DynamoDB condition expressions using recursive descent.
type condEvaluator struct {
	tokens []dynamoToken
	pos    int
	names  map[string]string
	values map[string]*AttributeValue
	item   map[string]*AttributeValue
}

func (e *condEvaluator) peek() dynamoToken {
	if e.pos >= len(e.tokens) {
		return dynamoToken{dynTokEOF, ""}
	}
	return e.tokens[e.pos]
}

func (e *condEvaluator) next() dynamoToken {
	tok := e.peek()
	if e.pos < len(e.tokens) {
		e.pos++
	}
	return tok
}

func (e *condEvaluator) isKeyword(tok dynamoToken, kw string) bool {
	return tok.typ == dynTokIdent && strings.EqualFold(tok.value, kw)
}

// evalOr evaluates OR expressions.
func (e *condEvaluator) evalOr() (bool, error) {
	left, err := e.evalAnd()
	if err != nil {
		return false, err
	}
	for e.isKeyword(e.peek(), "OR") {
		e.next()
		right, err := e.evalAnd()
		if err != nil {
			return false, err
		}
		left = left || right
	}
	return left, nil
}

// evalAnd evaluates AND expressions.
func (e *condEvaluator) evalAnd() (bool, error) {
	left, err := e.evalNot()
	if err != nil {
		return false, err
	}
	for e.isKeyword(e.peek(), "AND") {
		e.next()
		right, err := e.evalNot()
		if err != nil {
			return false, err
		}
		left = left && right
	}
	return left, nil
}

// evalNot evaluates NOT expressions.
func (e *condEvaluator) evalNot() (bool, error) {
	if e.isKeyword(e.peek(), "NOT") {
		e.next()
		val, err := e.evalPrimary()
		if err != nil {
			return false, err
		}
		return !val, nil
	}
	return e.evalPrimary()
}

// evalPrimary evaluates a single comparison, function call, or parenthesized expression.
func (e *condEvaluator) evalPrimary() (bool, error) {
	tok := e.peek()

	// Parenthesized expression.
	if tok.typ == dynTokLParen {
		e.next()
		val, err := e.evalOr()
		if err != nil {
			return false, err
		}
		if e.peek().typ != dynTokRParen {
			return false, fmt.Errorf("expected ')'")
		}
		e.next()
		return val, nil
	}

	// Function call: attribute_exists, attribute_not_exists, begins_with, contains, attribute_type, size
	if tok.typ == dynTokIdent {
		fn := strings.ToLower(tok.value)
		switch fn {
		case "attribute_exists":
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after attribute_exists")
			}
			e.next()
			path, err := e.evalPath()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after attribute_exists argument")
			}
			e.next()
			attr := getAttrByPath(e.item, path)
			return attr != nil, nil

		case "attribute_not_exists":
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after attribute_not_exists")
			}
			e.next()
			path, err := e.evalPath()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after attribute_not_exists argument")
			}
			e.next()
			attr := getAttrByPath(e.item, path)
			return attr == nil, nil

		case "begins_with":
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after begins_with")
			}
			e.next()
			av1, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokComma {
				return false, fmt.Errorf("expected ',' in begins_with")
			}
			e.next()
			av2, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after begins_with arguments")
			}
			e.next()
			if av1 == nil || av2 == nil {
				return false, nil
			}
			return strings.HasPrefix(avS(av1), avS(av2)), nil

		case "contains":
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after contains")
			}
			e.next()
			av1, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokComma {
				return false, fmt.Errorf("expected ',' in contains")
			}
			e.next()
			av2, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after contains arguments")
			}
			e.next()
			if av1 == nil || av2 == nil {
				return false, nil
			}
			if av1.S != nil {
				return strings.Contains(*av1.S, avS(av2)), nil
			}
			// Check list contains.
			for _, elem := range av1.L {
				if elem != nil && elem.S != nil && av2.S != nil && *elem.S == *av2.S {
					return true, nil
				}
			}
			return false, nil

		case "attribute_type":
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after attribute_type")
			}
			e.next()
			av1, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokComma {
				return false, fmt.Errorf("expected ',' in attribute_type")
			}
			e.next()
			av2, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after attribute_type arguments")
			}
			e.next()
			if av1 == nil || av2 == nil {
				return false, nil
			}
			return av2.S != nil && avTypeName(av1) == *av2.S, nil

		case "size":
			// size(path) op value — evaluated as comparison operand below
			// fall through to operand evaluation
		}
	}

	// Regular comparison: operand op operand
	left, err := e.evalOperand()
	if err != nil {
		return false, err
	}

	tok = e.peek()
	switch tok.typ {
	case dynTokEQ:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) == 0, nil
	case dynTokNE:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) != 0, nil
	case dynTokLT:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) < 0, nil
	case dynTokLE:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) <= 0, nil
	case dynTokGT:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) > 0, nil
	case dynTokGE:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return compareAV(left, right) >= 0, nil
	default:
		// Check BETWEEN and IN keywords.
		if e.isKeyword(tok, "BETWEEN") {
			e.next()
			lo, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if !e.isKeyword(e.peek(), "AND") {
				return false, fmt.Errorf("expected AND in BETWEEN expression")
			}
			e.next()
			hi, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			return compareAV(left, lo) >= 0 && compareAV(left, hi) <= 0, nil
		}
		if e.isKeyword(tok, "IN") {
			e.next()
			if e.peek().typ != dynTokLParen {
				return false, fmt.Errorf("expected '(' after IN")
			}
			e.next()
			for {
				val, err := e.evalOperand()
				if err != nil {
					return false, err
				}
				if compareAV(left, val) == 0 {
					// Skip remaining values.
					for e.peek().typ == dynTokComma {
						e.next()
						_, _ = e.evalOperand()
					}
					if e.peek().typ == dynTokRParen {
						e.next()
					}
					return true, nil
				}
				if e.peek().typ == dynTokComma {
					e.next()
					continue
				}
				break
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after IN list")
			}
			e.next()
			return false, nil
		}
		// No operator — treat as bool (e.g., function result).
		if left != nil && left.BOOL != nil {
			return *left.BOOL, nil
		}
		return left != nil, nil
	}
}

// evalPath reads a dotted attribute path (e.g., "a.b.c" or "#n") and returns it as a string.
func (e *condEvaluator) evalPath() (string, error) {
	tok := e.peek()
	var parts []string

	switch tok.typ {
	case dynTokIdent:
		e.next()
		parts = append(parts, tok.value)
	case dynTokExprName:
		e.next()
		name := tok.value
		if e.names != nil {
			if resolved, ok := e.names[name]; ok {
				name = resolved
			}
		}
		parts = append(parts, name)
	default:
		return "", fmt.Errorf("expected attribute path, got %q", tok.value)
	}

	// Handle dotted sub-paths.
	for e.peek().typ == dynTokDot {
		e.next()
		sub := e.peek()
		switch sub.typ {
		case dynTokIdent:
			e.next()
			parts = append(parts, sub.value)
		case dynTokExprName:
			e.next()
			name := sub.value
			if e.names != nil {
				if resolved, ok := e.names[name]; ok {
					name = resolved
				}
			}
			parts = append(parts, name)
		default:
			return "", fmt.Errorf("expected attribute name after '.', got %q", sub.value)
		}
	}

	return strings.Join(parts, "."), nil
}

// evalOperand evaluates a single value operand: #name, :val, attribute path, or size() function.
func (e *condEvaluator) evalOperand() (*AttributeValue, error) {
	tok := e.peek()

	if tok.typ == dynTokExprValue {
		e.next()
		if e.values != nil {
			if av, ok := e.values[tok.value]; ok {
				return av, nil
			}
		}
		return nil, fmt.Errorf("expression attribute value %q not defined", tok.value)
	}

	// size() function returns a numeric AttributeValue.
	if tok.typ == dynTokIdent && strings.ToLower(tok.value) == "size" {
		e.next()
		if e.peek().typ != dynTokLParen {
			return nil, fmt.Errorf("expected '(' after size")
		}
		e.next()
		path, err := e.evalPath()
		if err != nil {
			return nil, err
		}
		if e.peek().typ != dynTokRParen {
			return nil, fmt.Errorf("expected ')' after size argument")
		}
		e.next()
		attr := getAttrByPath(e.item, path)
		if attr == nil {
			return nil, nil
		}
		sz := avSize(attr)
		return &AttributeValue{N: strconv.Itoa(sz)}, nil
	}

	// Attribute path (#name or identifier).
	if tok.typ == dynTokExprName || tok.typ == dynTokIdent {
		path, err := e.evalPath()
		if err != nil {
			return nil, err
		}
		return getAttrByPath(e.item, path), nil
	}

	return nil, fmt.Errorf("expected operand, got %q", tok.value)
}

// getAttrByPath retrieves a nested attribute from an item using a dot-separated path.
func getAttrByPath(item map[string]*AttributeValue, path string) *AttributeValue {
	if item == nil || path == "" {
		return nil
	}
	parts := strings.SplitN(path, ".", 2)
	av, ok := item[parts[0]]
	if !ok || av == nil {
		return nil
	}
	if len(parts) == 1 {
		return av
	}
	if av.M != nil {
		return getAttrByPath(av.M, parts[1])
	}
	return nil
}

// compareAV compares two AttributeValues. Returns -1, 0, or 1.
func compareAV(a, b *AttributeValue) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	// Compare numbers numerically.
	if a.N != "" && b.N != "" {
		an, aerr := strconv.ParseFloat(a.N, 64)
		bn, berr := strconv.ParseFloat(b.N, 64)
		if aerr == nil && berr == nil {
			if an < bn {
				return -1
			}
			if an > bn {
				return 1
			}
			return 0
		}
	}
	// Compare strings.
	aStr := avToString(a)
	bStr := avToString(b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

// avTypeName returns the DynamoDB type name for an AttributeValue.
func avTypeName(av *AttributeValue) string {
	if av == nil {
		return ""
	}
	switch {
	case av.S != nil:
		return "S"
	case av.N != "":
		return "N"
	case len(av.B) > 0:
		return "B"
	case len(av.SS) > 0:
		return "SS"
	case len(av.NS) > 0:
		return "NS"
	case len(av.BS) > 0:
		return "BS"
	case av.M != nil:
		return "M"
	case av.L != nil:
		return "L"
	case av.BOOL != nil:
		return "BOOL"
	case av.NULL != nil:
		return "NULL"
	}
	return ""
}

// avSize returns the "size" of an AttributeValue for DynamoDB size() function.
func avSize(av *AttributeValue) int {
	if av == nil {
		return 0
	}
	switch {
	case av.S != nil:
		return len(*av.S)
	case av.N != "":
		return len(av.N)
	case len(av.B) > 0:
		return len(av.B)
	case len(av.SS) > 0:
		return len(av.SS)
	case len(av.NS) > 0:
		return len(av.NS)
	case len(av.BS) > 0:
		return len(av.BS)
	case av.M != nil:
		return len(av.M)
	case av.L != nil:
		return len(av.L)
	}
	return 0
}

// applyProjection returns the item with only the projected attributes.
// If projectionExpr is empty, the full item is returned.
func applyProjection(item map[string]*AttributeValue, projectionExpr string, names map[string]string) map[string]*AttributeValue {
	if projectionExpr == "" {
		return item
	}
	result := make(map[string]*AttributeValue)
	parts := strings.Split(projectionExpr, ",")
	for _, part := range parts {
		attr := strings.TrimSpace(part)
		// Resolve expression attribute names.
		if strings.HasPrefix(attr, "#") && names != nil {
			if resolved, ok := names[attr]; ok {
				attr = resolved
			}
		}
		if av, ok := item[attr]; ok {
			result[attr] = av
		}
	}
	return result
}

// extractKeyFromItem extracts the primary key attributes from an item.
func extractKeyFromItem(item map[string]*AttributeValue, keySchema []DynamoDBKeySchemaElement) map[string]*AttributeValue {
	key := make(map[string]*AttributeValue)
	for _, ks := range keySchema {
		if av, ok := item[ks.AttributeName]; ok {
			key[ks.AttributeName] = av
		}
	}
	return key
}

// copyItem makes a shallow copy of an item map.
func copyItem(item map[string]*AttributeValue) map[string]*AttributeValue {
	if item == nil {
		return nil
	}
	dst := make(map[string]*AttributeValue, len(item))
	for k, v := range item {
		dst[k] = v
	}
	return dst
}

// findIndexKeySchema returns the key schema for the named GSI or LSI.
func findIndexKeySchema(tbl *DynamoDBTable, indexName string) []DynamoDBKeySchemaElement {
	for _, gsi := range tbl.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			return gsi.KeySchema
		}
	}
	for _, lsi := range tbl.LocalSecondaryIndexes {
		if lsi.IndexName == indexName {
			return lsi.KeySchema
		}
	}
	return nil
}

// parseKeyCondition parses a DynamoDB KeyConditionExpression.
// Returns: pkAttr, pkVal, skAttr, skCondOp, skLow, skHigh, error.
// skCondOp is one of: "=", "<", "<=", ">", ">=", "BETWEEN", "begins_with".
func parseKeyCondition(expr string, keySchema []DynamoDBKeySchemaElement, names map[string]string, values map[string]*AttributeValue) (pkAttr, pkVal, skAttr, skCondOp, skLow, skHigh string, err error) {
	if expr == "" {
		return "", "", "", "", "", "", fmt.Errorf("KeyConditionExpression is required")
	}

	// Determine PK and SK attribute names from schema.
	for _, ks := range keySchema {
		switch ks.KeyType {
		case "HASH":
			pkAttr = ks.AttributeName
		case "RANGE":
			skAttr = ks.AttributeName
		}
	}

	// Tokenize.
	tokens, tokErr := tokenizeDynamo(expr)
	if tokErr != nil {
		return "", "", "", "", "", "", tokErr
	}

	// Simple pattern matching for common key conditions.
	// Form 1: pk = :val
	// Form 2: pk = :val AND sk = :val2
	// Form 3: pk = :val AND sk BETWEEN :lo AND :hi
	// Form 4: pk = :val AND begins_with(sk, :prefix)
	// Form 5: pk = :val AND sk [< <= > >=] :val2

	resolveToken := func(tok dynamoToken) string {
		switch tok.typ {
		case dynTokExprName:
			if names != nil {
				if resolved, ok := names[tok.value]; ok {
					return resolved
				}
			}
			return tok.value[1:] // strip #
		case dynTokIdent:
			return tok.value
		}
		return tok.value
	}

	resolveValue := func(tok dynamoToken) (string, error) {
		if tok.typ != dynTokExprValue {
			return "", fmt.Errorf("expected expression value, got %q", tok.value)
		}
		if values != nil {
			if av, ok := values[tok.value]; ok {
				return avToString(av), nil
			}
		}
		return "", fmt.Errorf("expression value %q not defined", tok.value)
	}

	pos := 0
	nextTok := func() dynamoToken {
		if pos < len(tokens) {
			t := tokens[pos]
			pos++
			return t
		}
		return dynamoToken{dynTokEOF, ""}
	}
	peekTok := func() dynamoToken {
		if pos < len(tokens) {
			return tokens[pos]
		}
		return dynamoToken{dynTokEOF, ""}
	}

	// Parse PK condition: attr = :val
	tok1 := nextTok()
	pkAttrParsed := resolveToken(tok1)
	if pkAttrParsed != pkAttr {
		// Maybe SK is first — swap.
		_ = pkAttrParsed
	}

	eqTok := nextTok()
	if eqTok.typ != dynTokEQ {
		return "", "", "", "", "", "", fmt.Errorf("expected '=' in key condition, got %q", eqTok.value)
	}

	valTok := nextTok()
	pkValParsed, valErr := resolveValue(valTok)
	if valErr != nil {
		return "", "", "", "", "", "", valErr
	}

	// If first attr is actually the SK, try to find PK by looking ahead.
	// For now assume PK comes first.
	pkVal = pkValParsed

	// No AND — done.
	if peekTok().typ == dynTokEOF {
		return pkAttr, pkVal, "", "", "", "", nil
	}

	// AND keyword.
	andTok := nextTok()
	if !strings.EqualFold(andTok.value, "AND") {
		return "", "", "", "", "", "", fmt.Errorf("expected AND in key condition, got %q", andTok.value)
	}

	// SK condition.
	skTok := nextTok()

	// Check for begins_with(sk, :val)
	if strings.ToLower(skTok.value) == "begins_with" && skTok.typ == dynTokIdent {
		// begins_with(skAttr, :prefix)
		if peekTok().typ != dynTokLParen {
			return "", "", "", "", "", "", fmt.Errorf("expected '(' after begins_with")
		}
		nextTok()     // consume (
		_ = nextTok() // skAttr inside parens
		if peekTok().typ != dynTokComma {
			return "", "", "", "", "", "", fmt.Errorf("expected ',' in begins_with")
		}
		nextTok() // consume ,
		prefixTok := nextTok()
		prefix, prefixErr := resolveValue(prefixTok)
		if prefixErr != nil {
			return "", "", "", "", "", "", prefixErr
		}
		if peekTok().typ != dynTokRParen {
			return "", "", "", "", "", "", fmt.Errorf("expected ')' in begins_with")
		}
		nextTok() // consume )
		return pkAttr, pkVal, skAttr, "begins_with", prefix, "", nil
	}

	// sk op :val or sk BETWEEN :lo AND :hi
	skAttrParsed := resolveToken(skTok)
	_ = skAttrParsed // we use skAttr from schema

	opTok := nextTok()
	if strings.EqualFold(opTok.value, "BETWEEN") {
		loTok := nextTok()
		lo, loErr := resolveValue(loTok)
		if loErr != nil {
			return "", "", "", "", "", "", loErr
		}
		andTok2 := nextTok()
		if !strings.EqualFold(andTok2.value, "AND") {
			return "", "", "", "", "", "", fmt.Errorf("expected AND in BETWEEN, got %q", andTok2.value)
		}
		hiTok := nextTok()
		hi, hiErr := resolveValue(hiTok)
		if hiErr != nil {
			return "", "", "", "", "", "", hiErr
		}
		return pkAttr, pkVal, skAttr, "BETWEEN", lo, hi, nil
	}

	// Regular comparison.
	var condOp string
	switch opTok.typ {
	case dynTokEQ:
		condOp = "="
	case dynTokLT:
		condOp = "<"
	case dynTokLE:
		condOp = "<="
	case dynTokGT:
		condOp = ">"
	case dynTokGE:
		condOp = ">="
	default:
		return "", "", "", "", "", "", fmt.Errorf("unsupported SK condition operator: %q", opTok.value)
	}

	valTok2 := nextTok()
	skValParsed, valErr2 := resolveValue(valTok2)
	if valErr2 != nil {
		return "", "", "", "", "", "", valErr2
	}

	return pkAttr, pkVal, skAttr, condOp, skValParsed, "", nil
}

// evalSKCondition evaluates a sort key condition.
func evalSKCondition(skVal, op, low, high string) bool {
	switch op {
	case "=":
		return skVal == low
	case "<":
		return skVal < low
	case "<=":
		return skVal <= low
	case ">":
		return skVal > low
	case ">=":
		return skVal >= low
	case "BETWEEN":
		return skVal >= low && skVal <= high
	case "begins_with":
		return strings.HasPrefix(skVal, low)
	}
	return false
}

// --- UpdateExpression parser -------------------------------------------------

// applyUpdateExpression parses and applies a DynamoDB UpdateExpression to an item.
func applyUpdateExpression(item map[string]*AttributeValue, expr string, names map[string]string, values map[string]*AttributeValue) error {
	// Split expression into SET/REMOVE/ADD/DELETE clauses.
	// Clauses start with SET, REMOVE, ADD, or DELETE keyword.
	upper := strings.ToUpper(expr)
	type clause struct {
		keyword string
		body    string
	}
	var clauses []clause

	// Find clause boundaries.
	keywords := []string{"SET", "REMOVE", "ADD", "DELETE"}
	type pos struct {
		start int
		kw    string
	}
	var positions []pos
	for _, kw := range keywords {
		// Find all occurrences of kw as a word boundary.
		// A clause keyword must be preceded by start-of-string or whitespace,
		// and followed by end-of-string or whitespace. This prevents false
		// matches like "REMOVE" inside expression values such as ":remove".
		idx := 0
		for {
			i := strings.Index(upper[idx:], kw)
			if i < 0 {
				break
			}
			absI := idx + i
			// Check strict whitespace word boundary.
			before := absI == 0 || upper[absI-1] == ' ' || upper[absI-1] == '\t'
			after := absI+len(kw) >= len(upper) || upper[absI+len(kw)] == ' ' || upper[absI+len(kw)] == '\t'
			if before && after {
				positions = append(positions, pos{absI, kw})
			}
			idx = absI + 1
		}
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i].start < positions[j].start })

	for i, p := range positions {
		var body string
		if i+1 < len(positions) {
			body = expr[p.start+len(p.kw) : positions[i+1].start]
		} else {
			body = expr[p.start+len(p.kw):]
		}
		clauses = append(clauses, clause{keyword: p.kw, body: strings.TrimSpace(body)})
	}

	for _, cl := range clauses {
		switch cl.keyword {
		case "SET":
			if err := applySetClause(item, cl.body, names, values); err != nil {
				return err
			}
		case "REMOVE":
			if err := applyRemoveClause(item, cl.body, names); err != nil {
				return err
			}
		case "ADD":
			if err := applyAddClause(item, cl.body, names, values); err != nil {
				return err
			}
		case "DELETE":
			if err := applyDeleteClause(item, cl.body, names, values); err != nil {
				return err
			}
		}
	}
	return nil
}

// applySetClause applies a SET clause to an item.
func applySetClause(item map[string]*AttributeValue, body string, names map[string]string, values map[string]*AttributeValue) error {
	actions := splitUpdateActions(body)
	for _, action := range actions {
		action = strings.TrimSpace(action)
		if action == "" {
			continue
		}

		// Find the = sign.
		eqIdx := strings.Index(action, "=")
		if eqIdx < 0 {
			return fmt.Errorf("SET action missing '=': %q", action)
		}
		lhs := strings.TrimSpace(action[:eqIdx])
		rhs := strings.TrimSpace(action[eqIdx+1:])

		attrName := resolveUpdateName(lhs, names)

		// Parse RHS: could be :val, path + :val, path - :val.
		if plus := strings.Index(rhs, "+"); plus > 0 {
			// Check it's not inside a function.
			left := strings.TrimSpace(rhs[:plus])
			right := strings.TrimSpace(rhs[plus+1:])
			leftVal := resolveUpdateValue(left, item, names, values)
			rightVal := resolveUpdateValue(right, item, names, values)
			item[attrName] = addAV(leftVal, rightVal)
		} else if minus := strings.LastIndex(rhs, "-"); minus > 0 && !strings.HasPrefix(strings.TrimSpace(rhs[minus:]), "->") {
			left := strings.TrimSpace(rhs[:minus])
			right := strings.TrimSpace(rhs[minus+1:])
			leftVal := resolveUpdateValue(left, item, names, values)
			rightVal := resolveUpdateValue(right, item, names, values)
			item[attrName] = subtractAV(leftVal, rightVal)
		} else {
			item[attrName] = resolveUpdateValue(rhs, item, names, values)
		}
	}
	return nil
}

// applyRemoveClause applies a REMOVE clause to an item.
func applyRemoveClause(item map[string]*AttributeValue, body string, names map[string]string) error {
	actions := splitUpdateActions(body)
	for _, action := range actions {
		attrName := resolveUpdateName(strings.TrimSpace(action), names)
		delete(item, attrName)
	}
	return nil
}

// applyAddClause applies an ADD clause to an item.
func applyAddClause(item map[string]*AttributeValue, body string, names map[string]string, values map[string]*AttributeValue) error {
	actions := splitUpdateActions(body)
	for _, action := range actions {
		action = strings.TrimSpace(action)
		parts := strings.Fields(action)
		if len(parts) < 2 {
			return fmt.Errorf("ADD action requires path and value: %q", action)
		}
		attrName := resolveUpdateName(parts[0], names)
		valKey := parts[1]
		addVal := resolveUpdateValue(valKey, item, names, values)
		existing := item[attrName]
		item[attrName] = addAV(existing, addVal)
	}
	return nil
}

// applyDeleteClause applies a DELETE clause to an item (set subtraction).
func applyDeleteClause(item map[string]*AttributeValue, body string, names map[string]string, values map[string]*AttributeValue) error {
	actions := splitUpdateActions(body)
	for _, action := range actions {
		action = strings.TrimSpace(action)
		parts := strings.Fields(action)
		if len(parts) < 2 {
			return fmt.Errorf("DELETE action requires path and value: %q", action)
		}
		attrName := resolveUpdateName(parts[0], names)
		valKey := parts[1]
		delVal := resolveUpdateValue(valKey, item, names, values)
		existing := item[attrName]
		if existing == nil {
			continue
		}
		item[attrName] = subtractSetAV(existing, delVal)
	}
	return nil
}

// splitUpdateActions splits a clause body by commas, respecting parentheses.
func splitUpdateActions(body string) []string {
	var actions []string
	depth := 0
	start := 0
	for i, ch := range body {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				actions = append(actions, body[start:i])
				start = i + 1
			}
		}
	}
	actions = append(actions, body[start:])
	return actions
}

// resolveUpdateName resolves an attribute name, handling #name references.
func resolveUpdateName(s string, names map[string]string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "#") && names != nil {
		if resolved, ok := names[s]; ok {
			return resolved
		}
	}
	return s
}

// resolveUpdateValue resolves a value in an update expression (either :val or attribute path).
func resolveUpdateValue(s string, item map[string]*AttributeValue, names map[string]string, values map[string]*AttributeValue) *AttributeValue {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, ":") {
		if values != nil {
			if av, ok := values[s]; ok {
				return av
			}
		}
		return nil
	}
	// Attribute reference.
	attrName := resolveUpdateName(s, names)
	return item[attrName]
}

// addAV adds two AttributeValues (numeric addition or set union).
func addAV(a, b *AttributeValue) *AttributeValue {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	// Numeric addition.
	if a.N != "" && b.N != "" {
		an, _ := strconv.ParseFloat(a.N, 64)
		bn, _ := strconv.ParseFloat(b.N, 64)
		result := an + bn
		return &AttributeValue{N: strconv.FormatFloat(result, 'f', -1, 64)}
	}
	// String set union.
	if len(a.SS) > 0 || len(b.SS) > 0 {
		merged := append(append([]string{}, a.SS...), b.SS...)
		seen := make(map[string]bool)
		unique := make([]string, 0, len(merged))
		for _, s := range merged {
			if !seen[s] {
				seen[s] = true
				unique = append(unique, s)
			}
		}
		sort.Strings(unique)
		return &AttributeValue{SS: unique}
	}
	return b
}

// subtractAV subtracts two AttributeValues (numeric subtraction).
func subtractAV(a, b *AttributeValue) *AttributeValue {
	if a == nil {
		return &AttributeValue{N: "0"}
	}
	if b == nil {
		return a
	}
	if a.N != "" && b.N != "" {
		an, _ := strconv.ParseFloat(a.N, 64)
		bn, _ := strconv.ParseFloat(b.N, 64)
		result := an - bn
		return &AttributeValue{N: strconv.FormatFloat(result, 'f', -1, 64)}
	}
	return a
}

// subtractSetAV removes elements of b from the set a.
func subtractSetAV(a, b *AttributeValue) *AttributeValue {
	if a == nil || b == nil {
		return a
	}
	if len(a.SS) > 0 && len(b.SS) > 0 {
		remove := make(map[string]bool, len(b.SS))
		for _, s := range b.SS {
			remove[s] = true
		}
		result := make([]string, 0, len(a.SS))
		for _, s := range a.SS {
			if !remove[s] {
				result = append(result, s)
			}
		}
		return &AttributeValue{SS: result}
	}
	return a
}

// --- Response helper ---------------------------------------------------------

// dynamodbJSONResponse encodes v as JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.0.
func dynamodbJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("dynamodbJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       body,
	}, nil
}

// --- PartiQL -----------------------------------------------------------------

// ExecuteStatementOutput is the JSON response for ExecuteStatement.
type ExecuteStatementOutput struct {
	// Items contains the result items for SELECT statements.
	Items []map[string]*AttributeValue `json:"Items"`

	// NextToken is the pagination token for large result sets.
	NextToken string `json:"NextToken,omitempty"`
}

// executeStatement handles ExecuteStatement (single PartiQL statement).
func (p *DynamoDBPlugin) executeStatement(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Statement  string            `json:"Statement"`
		Parameters []*AttributeValue `json:"Parameters"`
		NextToken  string            `json:"NextToken"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	items, err := p.executePartiQLStatement(ctx, input.Statement, input.Parameters)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []map[string]*AttributeValue{}
	}
	return dynamodbJSONResponse(http.StatusOK, ExecuteStatementOutput{Items: items})
}

// batchExecuteStatement handles BatchExecuteStatement (multiple PartiQL statements).
func (p *DynamoDBPlugin) batchExecuteStatement(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Statements []struct {
			Statement  string            `json:"Statement"`
			Parameters []*AttributeValue `json:"Parameters"`
		} `json:"Statements"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	type batchResponse struct {
		TableName string                     `json:"TableName,omitempty"`
		Item      map[string]*AttributeValue `json:"Item,omitempty"`
		Error     *map[string]interface{}    `json:"Error,omitempty"`
	}

	responses := make([]batchResponse, 0, len(input.Statements))
	for _, stmt := range input.Statements {
		items, err := p.executePartiQLStatement(ctx, stmt.Statement, stmt.Parameters)
		if err != nil {
			awsErr, ok := err.(*AWSError)
			if ok {
				errMap := map[string]interface{}{"Code": awsErr.Code, "Message": awsErr.Message}
				responses = append(responses, batchResponse{Error: &errMap})
			} else {
				errMap := map[string]interface{}{"Code": "InternalServerError", "Message": err.Error()}
				responses = append(responses, batchResponse{Error: &errMap})
			}
			continue
		}
		if len(items) > 0 {
			responses = append(responses, batchResponse{Item: items[0]})
		} else {
			responses = append(responses, batchResponse{})
		}
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Responses": responses,
	})
}

// executePartiQLStatement executes a single PartiQL statement and returns the
// result items. Supports SELECT, INSERT, UPDATE, and DELETE.
func (p *DynamoDBPlugin) executePartiQLStatement(
	ctx *RequestContext,
	stmt string,
	params []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	verb, tableName, whereClause, setClause := tokenizePartiQL(stmt)
	if tableName == "" {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Unable to parse statement: " + stmt,
			HTTPStatus: http.StatusBadRequest,
		}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, tableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "Table not found: " + tableName,
			HTTPStatus: http.StatusBadRequest,
		}
	}

	switch strings.ToUpper(verb) {
	case "SELECT":
		return p.partiQLSelect(ctx, tbl, whereClause, params)
	case "INSERT":
		return p.partiQLInsert(ctx, tbl, setClause, params)
	case "UPDATE":
		return p.partiQLUpdate(ctx, tbl, whereClause, setClause, params)
	case "DELETE":
		return p.partiQLDelete(ctx, tbl, whereClause, params)
	default:
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Unsupported PartiQL verb: " + verb,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// tokenizePartiQL extracts the verb, table name, WHERE clause, and SET clause
// from a simple PartiQL statement. Only basic patterns are supported.
func tokenizePartiQL(stmt string) (verb, tableName, whereClause, setClause string) {
	stmt = strings.TrimSpace(stmt)
	upper := strings.ToUpper(stmt)

	if strings.HasPrefix(upper, "SELECT") {
		// SELECT * FROM "table" [WHERE ...]
		fromIdx := strings.Index(upper, " FROM ")
		if fromIdx < 0 {
			return "SELECT", "", "", ""
		}
		rest := strings.TrimSpace(stmt[fromIdx+6:])
		tableName = extractPartiQLTableName(rest)
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "SELECT", tableName, whereClause, ""
	}

	if strings.HasPrefix(upper, "INSERT") {
		// INSERT INTO "table" VALUE {...}
		intoIdx := strings.Index(upper, " INTO ")
		if intoIdx < 0 {
			return "INSERT", "", "", ""
		}
		rest := strings.TrimSpace(stmt[intoIdx+6:])
		tableName = extractPartiQLTableName(rest)
		valueClause := extractPartiQLClause(rest, "VALUE")
		return "INSERT", tableName, "", valueClause
	}

	if strings.HasPrefix(upper, "UPDATE") {
		// UPDATE "table" SET ... WHERE ...
		rest := strings.TrimSpace(stmt[6:])
		tableName = extractPartiQLTableName(rest)
		setClause = extractPartiQLClause(rest, "SET")
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "UPDATE", tableName, whereClause, setClause
	}

	if strings.HasPrefix(upper, "DELETE") {
		// DELETE FROM "table" WHERE ...
		fromIdx := strings.Index(upper, " FROM ")
		if fromIdx < 0 {
			return "DELETE", "", "", ""
		}
		rest := strings.TrimSpace(stmt[fromIdx+6:])
		tableName = extractPartiQLTableName(rest)
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "DELETE", tableName, whereClause, ""
	}

	return "", "", "", ""
}

// extractPartiQLTableName returns the table name from the leading part of a
// PartiQL statement fragment. Table names may be quoted or unquoted.
func extractPartiQLTableName(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return ""
	}
	if s[0] == '"' {
		end := strings.Index(s[1:], `"`)
		if end < 0 {
			return s[1:]
		}
		return s[1 : end+1]
	}
	// Unquoted: read until space.
	if idx := strings.IndexAny(s, " \t"); idx >= 0 {
		return s[:idx]
	}
	return s
}

// extractPartiQLClause returns the text following keyword in a statement.
func extractPartiQLClause(stmt, keyword string) string {
	upper := strings.ToUpper(stmt)
	prefix := " " + keyword + " "
	idx := strings.Index(upper, prefix)
	if idx < 0 {
		prefix = keyword + " "
		if strings.HasPrefix(upper, prefix) {
			idx = -len(keyword)
		} else {
			return ""
		}
	}
	return strings.TrimSpace(stmt[idx+len(prefix):])
}

func (p *DynamoDBPlugin) partiQLSelect(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, tbl.TableName)
	if err != nil {
		return nil, err
	}

	var results []map[string]*AttributeValue
	for _, k := range keys {
		item, err := p.loadItem(context.Background(), ctx.AccountID, tbl.TableName, k)
		if err != nil || item == nil {
			continue
		}
		if whereClause != "" {
			ok, _ := evalDynamoCondition(whereClause, nil, nil, item)
			if !ok {
				continue
			}
		}
		results = append(results, item)
	}
	return results, nil
}

func (p *DynamoDBPlugin) partiQLInsert(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	valueClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	// valueClause should be a JSON object like {"pk": {"S": "val"}, ...}
	var item map[string]*AttributeValue
	if err := json.Unmarshal([]byte(valueClause), &item); err != nil {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Failed to parse VALUE clause as item: " + err.Error(),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	_, pkVal, _, skVal, err := extractPrimaryKey(item, tbl.KeySchema)
	if err != nil {
		return nil, err
	}
	ik := dynamodbItemKey(pkVal, skVal)

	if err := p.saveItem(context.Background(), ctx.AccountID, tbl.TableName, ik, item); err != nil {
		return nil, fmt.Errorf("partiQL insert saveItem: %w", err)
	}
	keys, _ := p.loadItemKeys(context.Background(), ctx.AccountID, tbl.TableName)
	keys = append(keys, ik)
	_ = p.saveItemKeys(context.Background(), ctx.AccountID, tbl.TableName, keys)
	p.appendStreamRecord(context.Background(), ctx.AccountID, tbl.TableName, "INSERT", nil, item)
	return nil, nil
}

func (p *DynamoDBPlugin) partiQLUpdate(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause, setClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, tbl.TableName)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		item, err := p.loadItem(context.Background(), ctx.AccountID, tbl.TableName, k)
		if err != nil || item == nil {
			continue
		}
		if whereClause != "" {
			ok, _ := evalDynamoCondition(whereClause, nil, nil, item)
			if !ok {
				continue
			}
		}
		oldItem := copyItem(item)
		if setClause != "" {
			// Convert SET clause to UpdateExpression format.
			updateExpr := "SET " + setClause
			if applyErr := applyUpdateExpression(item, updateExpr, nil, nil); applyErr != nil {
				return nil, &AWSError{
					Code:       "ValidationException",
					Message:    "Failed to apply SET clause: " + applyErr.Error(),
					HTTPStatus: http.StatusBadRequest,
				}
			}
		}
		if saveErr := p.saveItem(context.Background(), ctx.AccountID, tbl.TableName, k, item); saveErr != nil {
			return nil, fmt.Errorf("partiQL update saveItem: %w", saveErr)
		}
		p.appendStreamRecord(context.Background(), ctx.AccountID, tbl.TableName, "MODIFY", oldItem, item)
	}
	return nil, nil
}

func (p *DynamoDBPlugin) partiQLDelete(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, tbl.TableName)
	if err != nil {
		return nil, err
	}

	remaining := keys[:0]
	for _, k := range keys {
		item, err := p.loadItem(context.Background(), ctx.AccountID, tbl.TableName, k)
		if err != nil || item == nil {
			remaining = append(remaining, k)
			continue
		}
		if whereClause != "" {
			ok, _ := evalDynamoCondition(whereClause, nil, nil, item)
			if !ok {
				remaining = append(remaining, k)
				continue
			}
		}
		_ = p.deleteItemByKey(context.Background(), ctx.AccountID, tbl.TableName, k)
		p.appendStreamRecord(context.Background(), ctx.AccountID, tbl.TableName, "REMOVE", item, nil)
	}
	_ = p.saveItemKeys(context.Background(), ctx.AccountID, tbl.TableName, remaining)
	return nil, nil
}
