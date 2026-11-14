package emulator

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
	locks  dynamodbLocks // serializes writes per item and per table (#1449)
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
	// Resource tagging.
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsOfResource":
		return p.listTagsOfResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- State helpers -----------------------------------------------------------

func (p *DynamoDBPlugin) tableStateKey(accountID, region, tableName string) string {
	return "table:" + accountID + "/" + region + "/" + tableName
}

func (p *DynamoDBPlugin) tableNamesKey(accountID, region string) string {
	return "table_names:" + accountID + "/" + region
}

func (p *DynamoDBPlugin) itemStateKey(accountID, region, tableName, itemKey string) string {
	return "item:" + accountID + "/" + region + "/" + tableName + "/" + itemKey
}

func (p *DynamoDBPlugin) itemKeysStateKey(accountID, region, tableName string) string {
	return "item_keys:" + accountID + "/" + region + "/" + tableName
}

func (p *DynamoDBPlugin) loadTable(ctx context.Context, accountID, region, tableName string) (*DynamoDBTable, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.tableStateKey(accountID, region, tableName))
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

func (p *DynamoDBPlugin) saveTable(ctx context.Context, accountID, region string, tbl *DynamoDBTable) error {
	data, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("dynamodb saveTable marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.tableStateKey(accountID, region, tbl.TableName), data)
}

func (p *DynamoDBPlugin) loadTableNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.tableNamesKey(accountID, region))
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

func (p *DynamoDBPlugin) saveTableNames(ctx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("dynamodb saveTableNames marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.tableNamesKey(accountID, region), data)
}

func (p *DynamoDBPlugin) loadItemKeys(ctx context.Context, accountID, region, tableName string) ([]string, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.itemKeysStateKey(accountID, region, tableName))
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

func (p *DynamoDBPlugin) saveItemKeys(ctx context.Context, accountID, region, tableName string, keys []string) error {
	data, err := json.Marshal(keys)
	if err != nil {
		return fmt.Errorf("dynamodb saveItemKeys marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.itemKeysStateKey(accountID, region, tableName), data)
}

// addItemKey records ik in a table's item_keys list, under the table's stripe so two
// concurrent creates of distinct items both land (#1449). A key already listed is not
// added twice.
func (p *DynamoDBPlugin) addItemKey(ctx context.Context, accountID, region, tableName, ik string) error {
	unlock := p.locks.lockTable(accountID, region, tableName)
	defer unlock()
	keys, err := p.loadItemKeys(ctx, accountID, region, tableName)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if k == ik {
			return nil
		}
	}
	return p.saveItemKeys(ctx, accountID, region, tableName, append(keys, ik))
}

// removeItemKey drops ik from a table's item_keys list, under the table's stripe.
func (p *DynamoDBPlugin) removeItemKey(ctx context.Context, accountID, region, tableName, ik string) error {
	unlock := p.locks.lockTable(accountID, region, tableName)
	defer unlock()
	keys, err := p.loadItemKeys(ctx, accountID, region, tableName)
	if err != nil {
		return err
	}
	kept := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != ik {
			kept = append(kept, k)
		}
	}
	return p.saveItemKeys(ctx, accountID, region, tableName, kept)
}

func (p *DynamoDBPlugin) loadItem(ctx context.Context, accountID, region, tableName, itemKey string) (map[string]*AttributeValue, error) {
	data, err := p.state.Get(ctx, dynamodbNamespace, p.itemStateKey(accountID, region, tableName, itemKey))
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

func (p *DynamoDBPlugin) saveItem(ctx context.Context, accountID, region, tableName, itemKey string, item map[string]*AttributeValue) error {
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("dynamodb saveItem marshal: %w", err)
	}
	return p.state.Put(ctx, dynamodbNamespace, p.itemStateKey(accountID, region, tableName, itemKey), data)
}

func (p *DynamoDBPlugin) deleteItemByKey(ctx context.Context, accountID, region, tableName, itemKey string) error {
	return p.state.Delete(ctx, dynamodbNamespace, p.itemStateKey(accountID, region, tableName, itemKey))
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
	pkVal := dynamodbKeyComponent(pkAV)
	var skVal string
	if skAttr != "" {
		skAV, skOk := item[skAttr]
		if !skOk || skAV == nil {
			return "", "", "", "", &AWSError{Code: "ValidationException", Message: "One or more parameter values were invalid: Missing the key " + skAttr + " in the item", HTTPStatus: http.StatusBadRequest}
		}
		skVal = dynamodbKeyComponent(skAV)
	}
	return pkAttr, pkVal, skAttr, skVal, nil
}

// dynamodbItemKeyFor validates key against the table's key schema and declared key
// types and returns the item's encoded state-key component.
func dynamodbItemKeyFor(tbl *DynamoDBTable, key map[string]*AttributeValue) (string, error) {
	_, pkVal, _, skVal, err := extractPrimaryKey(key, tbl.KeySchema)
	if err != nil {
		return "", err
	}
	if err := dynamodbValidateKeyTypes(tbl, key); err != nil {
		return "", err
	}
	return dynamodbItemKey(pkVal, skVal), nil
}

// dynamodbNewItemFromKey returns the item an UpdateItem creates when no item exists:
// just the key attributes, deep-copied.
func dynamodbNewItemFromKey(key map[string]*AttributeValue) map[string]*AttributeValue {
	item := make(map[string]*AttributeValue, len(key))
	for k, v := range key {
		item[k] = deepCopyAV(v)
	}
	return item
}

// dynamodbProjectAttrs returns the named top-level attributes of item that exist. It
// is how UpdateItem's UPDATED_OLD and UPDATED_NEW report "only the updated attributes".
func dynamodbProjectAttrs(item map[string]*AttributeValue, names []string) map[string]*AttributeValue {
	out := make(map[string]*AttributeValue, len(names))
	for _, n := range names {
		if v, ok := item[n]; ok {
			out[n] = v
		}
	}
	return out
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
		Tags                []DynamoDBTag                `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: "Failed to parse request: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "TableName is required", HTTPStatus: http.StatusBadRequest}
	}

	existing, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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

	// Store any tags supplied at table creation time so they can be read back
	// via ListTagsOfResource.
	if len(input.Tags) > 0 {
		tbl.Tags = make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			tbl.Tags[t.Key] = t.Value
		}
	}

	// Generate stream ARN if streams enabled.
	if input.StreamSpecification != nil && input.StreamSpecification.StreamEnabled {
		tbl.LatestStreamARN = tableARN + "/stream/" + now.UTC().Format("2006-01-02T15:04:05.999")
	}

	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb createTable saveTable: %w", err)
	}

	names, err := p.loadTableNames(context.Background(), ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, input.TableName)
	if err := p.saveTableNames(context.Background(), ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("dynamodb createTable saveTableNames: %w", err)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": dynamodbTableDescriptionWire(*tbl),
	})
}

// loadTableForTagging resolves a DynamoDB resource ARN to its table, returning a
// ResourceNotFoundException if the ARN is malformed or the table does not exist.
func (p *DynamoDBPlugin) loadTableForTagging(ctx *RequestContext, resourceARN string) (*DynamoDBTable, error) {
	tableName := dynamodbTableNameFromARN(resourceARN)
	if tableName == "" {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Invalid ResourceArn: " + resourceARN, HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, tableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + tableName, HTTPStatus: http.StatusBadRequest}
	}
	return tbl, nil
}

func (p *DynamoDBPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string        `json:"ResourceArn"`
		Tags        []DynamoDBTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.loadTableForTagging(ctx, input.ResourceArn)
	if err != nil {
		return nil, err
	}
	tbl.EverTagged = taggingEverTagged(tbl.EverTagged, len(tbl.Tags), len(input.Tags))
	if tbl.Tags == nil {
		tbl.Tags = make(map[string]string, len(input.Tags))
	}
	for _, t := range input.Tags {
		tbl.Tags[t.Key] = t.Value
	}
	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb tagResource saveTable: %w", err)
	}
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *DynamoDBPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.loadTableForTagging(ctx, input.ResourceArn)
	if err != nil {
		return nil, err
	}
	tbl.EverTagged = taggingEverTagged(tbl.EverTagged, len(tbl.Tags), 0)
	for _, k := range input.TagKeys {
		delete(tbl.Tags, k)
	}
	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb untagResource saveTable: %w", err)
	}
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *DynamoDBPlugin) listTagsOfResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.loadTableForTagging(ctx, input.ResourceArn)
	if err != nil {
		return nil, err
	}
	// Emit tags sorted by key for deterministic output.
	keys := make([]string, 0, len(tbl.Tags))
	for k := range tbl.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tags := make([]DynamoDBTag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, DynamoDBTag{Key: k, Value: tbl.Tags[k]})
	}
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Tags":      tags,
		"NextToken": nil,
	})
}

func (p *DynamoDBPlugin) deleteTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	// Delete all items.
	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	for _, ik := range itemKeys {
		_ = p.deleteItemByKey(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
	}
	_ = p.state.Delete(context.Background(), dynamodbNamespace, p.itemKeysStateKey(ctx.AccountID, ctx.Region, input.TableName))

	// Delete table.
	_ = p.state.Delete(context.Background(), dynamodbNamespace, p.tableStateKey(ctx.AccountID, ctx.Region, input.TableName))

	// Remove from names.
	names, err := p.loadTableNames(context.Background(), ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != input.TableName {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveTableNames(context.Background(), ctx.AccountID, ctx.Region, newNames); err != nil {
		return nil, fmt.Errorf("dynamodb deleteTable saveTableNames: %w", err)
	}

	tbl.TableStatus = "DELETING"
	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": dynamodbTableDescriptionWire(*tbl),
	})
}

func (p *DynamoDBPlugin) describeTable(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	// Refresh item count.
	itemKeys, _ := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	tbl.ItemCount = int64(len(itemKeys))
	tbl.TableSizeBytes = tbl.ItemCount * 100

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"Table": dynamodbTableDescriptionWire(*tbl),
	})
}

func (p *DynamoDBPlugin) listTables(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
		Limit                   int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ddbInvalidBody()
		}
	}

	limit := input.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	names, err := p.loadTableNames(context.Background(), ctx.AccountID, ctx.Region)
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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

	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb updateTable: %w", err)
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"TableDescription": dynamodbTableDescriptionWire(*tbl),
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	ik, err := dynamodbItemKeyFor(tbl, input.Item)
	if err != nil {
		return nil, err
	}

	// Hold the item's stripe across load → condition → save (#1449).
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, input.TableName, ik)
	defer unlock()

	// Load old item for condition checking and ReturnValues.
	oldItem, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
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
	if err := p.saveItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik, input.Item); err != nil {
		return nil, fmt.Errorf("dynamodb putItem saveItem: %w", err)
	}

	// Update item keys list if new.
	if oldItem == nil {
		if err := p.addItemKey(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik); err != nil {
			return nil, fmt.Errorf("dynamodb putItem saveItemKeys: %w", err)
		}
	}

	// Append stream record if streams enabled.
	eventName := "MODIFY"
	if oldItem == nil {
		eventName = "INSERT"
	}
	if err := p.appendStreamRecord(context.Background(), ctx.AccountID, ctx.Region, input.TableName, eventName, oldItem, input.Item); err != nil {
		return nil, fmt.Errorf("dynamodb putItem appendStreamRecord: %w", err)
	}

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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	ik, err := dynamodbItemKeyFor(tbl, input.Key)
	if err != nil {
		return nil, err
	}

	item, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	ik, err := dynamodbItemKeyFor(tbl, input.Key)
	if err != nil {
		return nil, err
	}

	// Hold the item's stripe across load → condition → delete (#1449).
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, input.TableName, ik)
	defer unlock()

	oldItem, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
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
		if err := p.deleteItemByKey(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik); err != nil {
			return nil, fmt.Errorf("dynamodb deleteItem: %w", err)
		}
		// Remove from item keys.
		if err := p.removeItemKey(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik); err != nil {
			return nil, fmt.Errorf("dynamodb deleteItem saveItemKeys: %w", err)
		}
		// Append stream record.
		if err := p.appendStreamRecord(context.Background(), ctx.AccountID, ctx.Region, input.TableName, "REMOVE", oldItem, nil); err != nil {
			return nil, fmt.Errorf("dynamodb deleteItem appendStreamRecord: %w", err)
		}
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	ik, err := dynamodbItemKeyFor(tbl, input.Key)
	if err != nil {
		return nil, err
	}

	// Hold the item's stripe across load → condition → save, so concurrent
	// conditional updates of one item serialize and exactly one of N writers whose
	// conditions are mutually exclusive wins (#1449).
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, input.TableName, ik)
	defer unlock()

	// current is the stored item, nil when there is none. The condition is evaluated
	// against it — not against an item seeded from the key — so attribute_exists(pk)
	// is false and attribute_not_exists(pk) true for an item that does not exist yet,
	// as on DynamoDB (#1448, #1451).
	current, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
	if err != nil {
		return nil, err
	}

	if input.ConditionExpression != "" {
		ok, evalErr := evalDynamoCondition(input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, current)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			return nil, &AWSError{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: http.StatusBadRequest}
		}
	}

	// The item the update is applied to: a deep copy of the stored item, or the key
	// attributes when the update creates it.
	isNew := current == nil
	var item map[string]*AttributeValue
	if isNew {
		item = dynamodbNewItemFromKey(input.Key)
	} else {
		item = copyItem(current)
	}

	var touched []string
	if input.UpdateExpression != "" {
		touched, err = applyUpdateExpression(item, input.UpdateExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
		if err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if err := p.saveItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik, item); err != nil {
		return nil, fmt.Errorf("dynamodb updateItem saveItem: %w", err)
	}

	if isNew {
		if err := p.addItemKey(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik); err != nil {
			return nil, fmt.Errorf("dynamodb updateItem saveItemKeys: %w", err)
		}
	}

	// Append stream record: an INSERT has no old image.
	updateEventName := "MODIFY"
	if isNew {
		updateEventName = "INSERT"
	}
	if err := p.appendStreamRecord(context.Background(), ctx.AccountID, ctx.Region, input.TableName, updateEventName, current, item); err != nil {
		return nil, fmt.Errorf("dynamodb updateItem appendStreamRecord: %w", err)
	}

	// ReturnValues. An item that did not exist has no "old" attributes, so ALL_OLD and
	// UPDATED_OLD return nothing for it.
	result := map[string]interface{}{}
	switch input.ReturnValues {
	case "ALL_NEW":
		result["Attributes"] = item
	case "ALL_OLD":
		if !isNew {
			result["Attributes"] = current
		}
	case "UPDATED_NEW":
		if attrs := dynamodbProjectAttrs(item, touched); len(attrs) > 0 {
			result["Attributes"] = attrs
		}
	case "UPDATED_OLD":
		if attrs := dynamodbProjectAttrs(current, touched); len(attrs) > 0 {
			result["Attributes"] = attrs
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
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, tableName)
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
			item, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
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
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, tableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + tableName, HTTPStatus: http.StatusBadRequest}
		}

		for _, writeReq := range requests {
			var key, newItem map[string]*AttributeValue
			switch {
			case writeReq.PutRequest.Item != nil:
				key, newItem = writeReq.PutRequest.Item, writeReq.PutRequest.Item
			case writeReq.DeleteRequest.Key != nil:
				key = writeReq.DeleteRequest.Key
			default:
				continue
			}
			ik, err := dynamodbItemKeyFor(tbl, key)
			if err != nil {
				return nil, err
			}
			if err := p.batchWriteOne(ctx, tableName, ik, newItem); err != nil {
				return nil, fmt.Errorf("dynamodb batchWriteItem: %w", err)
			}
		}
	}

	return dynamodbJSONResponse(http.StatusOK, map[string]interface{}{
		"UnprocessedItems": map[string]interface{}{},
	})
}

// batchWriteOne applies one BatchWriteItem request — a put when newItem is non-nil,
// otherwise a delete — under the item's stripe.
func (p *DynamoDBPlugin) batchWriteOne(ctx *RequestContext, tableName, ik string, newItem map[string]*AttributeValue) error {
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tableName, ik)
	defer unlock()
	old, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
	if err != nil {
		return err
	}
	return p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik, old, newItem)
}

// commitItem replaces the stored item old with item — or deletes it when item is nil —
// and keeps the table's item_keys list and stream in step. The caller holds the item's
// stripe and has already loaded old.
func (p *DynamoDBPlugin) commitItem(ctx context.Context, accountID, region, tableName, ik string, old, item map[string]*AttributeValue) error {
	if item == nil {
		if old == nil {
			return nil
		}
		if err := p.deleteItemByKey(ctx, accountID, region, tableName, ik); err != nil {
			return err
		}
		if err := p.removeItemKey(ctx, accountID, region, tableName, ik); err != nil {
			return err
		}
		return p.appendStreamRecord(ctx, accountID, region, tableName, "REMOVE", old, nil)
	}
	if err := p.saveItem(ctx, accountID, region, tableName, ik, item); err != nil {
		return err
	}
	eventName := "MODIFY"
	if old == nil {
		eventName = "INSERT"
		if err := p.addItemKey(ctx, accountID, region, tableName, ik); err != nil {
			return err
		}
	}
	return p.appendStreamRecord(ctx, accountID, region, tableName, eventName, old, item)
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
		tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, reqCtx.Region, g.TableName)
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
		item, err := p.loadItem(context.Background(), reqCtx.AccountID, reqCtx.Region, g.TableName, dynamodbItemKey(pkVal, skVal))
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
// canceled and TransactionCanceledException is returned with CancellationReasons.
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

	// Resolve every action's table and item first, so all touched items can be locked
	// before anything is read (#1449). Stripes are taken in ascending order, each once,
	// so two transactions over overlapping items cannot deadlock.
	type txOp struct {
		kind      string // Put, Update, Delete, ConditionCheck, or "" for an empty entry
		tableName string
		ik        string
		key       map[string]*AttributeValue
		putItem   map[string]*AttributeValue
		update    string
		cond      string
		names     map[string]string
		vals      map[string]*AttributeValue
	}
	ops := make([]txOp, len(input.TransactItems))
	var stripes []int
	for i, ti := range input.TransactItems {
		var op txOp
		switch {
		case ti.Put != nil:
			op = txOp{kind: "Put", tableName: ti.Put.TableName, key: ti.Put.Item, putItem: ti.Put.Item,
				cond: ti.Put.ConditionExpression, names: ti.Put.ExpressionAttributeNames, vals: ti.Put.ExpressionAttributeValues}
		case ti.Update != nil:
			op = txOp{kind: "Update", tableName: ti.Update.TableName, key: ti.Update.Key, update: ti.Update.UpdateExpression,
				cond: ti.Update.ConditionExpression, names: ti.Update.ExpressionAttributeNames, vals: ti.Update.ExpressionAttributeValues}
		case ti.Delete != nil:
			op = txOp{kind: "Delete", tableName: ti.Delete.TableName, key: ti.Delete.Key,
				cond: ti.Delete.ConditionExpression, names: ti.Delete.ExpressionAttributeNames, vals: ti.Delete.ExpressionAttributeValues}
		case ti.ConditionCheck != nil:
			op = txOp{kind: "ConditionCheck", tableName: ti.ConditionCheck.TableName, key: ti.ConditionCheck.Key,
				cond: ti.ConditionCheck.ConditionExpression, names: ti.ConditionCheck.ExpressionAttributeNames, vals: ti.ConditionCheck.ExpressionAttributeValues}
		default:
			ops[i] = op
			continue
		}
		tbl, err := p.loadTable(context.Background(), reqCtx.AccountID, reqCtx.Region, op.tableName)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + op.tableName, HTTPStatus: http.StatusBadRequest}
		}
		ik, err := dynamodbItemKeyFor(tbl, op.key)
		if err != nil {
			return nil, err
		}
		op.ik = ik
		ops[i] = op
		stripes = append(stripes, p.locks.itemStripe(reqCtx.AccountID, reqCtx.Region, op.tableName, ik))
	}
	unlock := p.locks.lockItemStripes(stripes)
	defer unlock()

	// Phase 1: load every item and evaluate every condition against the stored item —
	// nil when there is none, never an item seeded from the key (#1448, #1451).
	type cancellationReason struct {
		Code    string `json:"Code"`
		Message string `json:"Message,omitempty"`
	}
	reasons := make([]cancellationReason, len(ops))
	canceled := false
	olds := make([]map[string]*AttributeValue, len(ops))
	for i, op := range ops {
		reasons[i] = cancellationReason{Code: "None"}
		if op.kind == "" {
			continue
		}
		current, err := p.loadItem(context.Background(), reqCtx.AccountID, reqCtx.Region, op.tableName, op.ik)
		if err != nil {
			return nil, err
		}
		olds[i] = current
		if op.cond == "" {
			continue
		}
		ok, evalErr := evalDynamoCondition(op.cond, op.names, op.vals, current)
		if evalErr != nil {
			return nil, &AWSError{Code: "ValidationException", Message: evalErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		if !ok {
			reasons[i] = cancellationReason{Code: "ConditionalCheckFailed", Message: "The conditional request failed"}
			canceled = true
		}
	}

	if canceled {
		body, err := json.Marshal(map[string]interface{}{
			"__type":              "TransactionCanceledException",
			"message":             "Transaction canceled, please refer cancellation reasons for specific reasons",
			"CancellationReasons": reasons,
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb transactWriteItems marshal: %w", err)
		}
		return &AWSResponse{
			StatusCode: http.StatusBadRequest,
			Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
			Body:       body,
		}, nil
	}

	// Phase 2: compute every resulting item before writing any, so an update
	// expression that cannot be applied fails the transaction with nothing written.
	news := make([]map[string]*AttributeValue, len(ops))
	for i, op := range ops {
		switch op.kind {
		case "Put":
			news[i] = op.putItem
		case "Update":
			var item map[string]*AttributeValue
			if olds[i] == nil {
				item = dynamodbNewItemFromKey(op.key)
			} else {
				item = copyItem(olds[i])
			}
			if op.update != "" {
				if _, err := applyUpdateExpression(item, op.update, op.names, op.vals); err != nil {
					return nil, &AWSError{Code: "ValidationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
				}
			}
			news[i] = item
		}
	}

	// Phase 3: apply all mutations.
	for i, op := range ops {
		switch op.kind {
		case "Put", "Update", "Delete":
			if err := p.commitItem(context.Background(), reqCtx.AccountID, reqCtx.Region, op.tableName, op.ik, olds[i], news[i]); err != nil {
				return nil, fmt.Errorf("dynamodb transactWriteItems %s: %w", op.kind, err)
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Table not found: " + input.TableName, HTTPStatus: http.StatusBadRequest}
	}

	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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
		item, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, itemKeys[i])
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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
	itemKeys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
	if err != nil {
		return nil, err
	}

	// Filter items matching PK (and SK condition if present).
	var matchingKeys []string
	for _, ik := range itemKeys {
		item, loadErr := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
		if loadErr != nil || item == nil {
			continue
		}

		// Check PK match.
		if !avEqual(item[pkAttr], pkVal) {
			continue
		}

		// Check SK condition.
		if skAttr != "" && skCondOp != "" {
			skAV := item[skAttr]
			if skAV == nil {
				continue
			}
			if !evalSKCondition(skAV, skCondOp, skLow, skHigh) {
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
		sort.SliceStable(matchingKeys, func(i, j int) bool {
			itemI, _ := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, matchingKeys[i])
			itemJ, _ := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, matchingKeys[j])
			if itemI == nil || itemJ == nil {
				return false
			}
			// Typed order: a Number sort key sorts numerically and a Binary one by
			// unsigned bytes (#1454).
			c, ok := avOrder(itemI[sortKey], itemJ[sortKey])
			if !ok {
				c = strings.Compare(dynamodbKeyComponent(itemI[sortKey]), dynamodbKeyComponent(itemJ[sortKey]))
			}
			if forward {
				return c < 0
			}
			return c > 0
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
		item, loadErr := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, input.TableName, ik)
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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

	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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

	// Region is the Region whose copy of the table the iterator reads. A table name is unique
	// only within a Region (#943), so an iterator that carried only the account could be
	// redeemed against another Region's ring buffer.
	Region string `json:"region"`

	// Sequence is the position in the ring buffer (next to read).
	Sequence int64 `json:"sequence"`

	// IterType is the iterator type (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER).
	IterType string `json:"iteratorType"`
}

func (p *DynamoDBPlugin) streamRecordsKey(accountID, region, tableName string) string {
	return "stream_records:" + accountID + "/" + region + "/" + tableName
}

// appendStreamRecord adds a CDC record to a table's stream ring buffer if
// streams are enabled for that table. A table without a stream is a no-op. A failed write of
// the record is returned: the item has already been written, so the caller reports the
// operation failed rather than leaving a stream that silently misses a change (#1192).
func (p *DynamoDBPlugin) appendStreamRecord(
	ctx context.Context,
	accountID, region, tableName, eventName string,
	oldImage, newImage map[string]*AttributeValue,
) error {
	tbl, err := p.loadTable(ctx, accountID, region, tableName)
	if err != nil {
		return fmt.Errorf("dynamodb appendStreamRecord %s: %w", tableName, err)
	}
	if tbl == nil || tbl.LatestStreamARN == "" {
		return nil // streams not enabled
	}

	// The ring is a read-modify-write shared by every item of the table; hold the
	// table's stripe so concurrent writes to distinct items keep every record (#1449).
	unlock := p.locks.lockTable(accountID, region, tableName)
	defer unlock()

	rk := p.streamRecordsKey(accountID, region, tableName)
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
		ApproxTimestamp: p.tc.Now().UnixMilli(),
	})

	if len(records) > dynamodbMaxStreamRecords {
		records = records[len(records)-dynamodbMaxStreamRecords:]
	}

	b, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("dynamodb appendStreamRecord %s marshal: %w", tableName, err)
	}
	if err := p.state.Put(ctx, dynamodbNamespace, rk, b); err != nil {
		return fmt.Errorf("dynamodb appendStreamRecord %s state.Put: %w", tableName, err)
	}
	return nil
}

func (p *DynamoDBPlugin) listStreams(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ddbInvalidBody()
		}
	}

	var streams []map[string]string

	if input.TableName != "" {
		tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, input.TableName)
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
		names, _ := p.loadTableNames(context.Background(), ctx.AccountID, ctx.Region)
		for _, name := range names {
			tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, name)
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
		rk := p.streamRecordsKey(ctx.AccountID, ctx.Region, tableName)
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
		Region:    ctx.Region,
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
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ddbInvalidBody()
		}
	}

	// ShardIterator is Required: Yes on API_streams_GetRecords. Answering 200 with an empty list
	// told a consumer's poll loop "this shard has no records yet" for a request that named no
	// shard, which is the one answer a poll loop cannot recover from — it keeps polling. See
	// [dynamodbStreamsValidationError] for why the code is not the plugin's ValidationException.
	if input.ShardIterator == "" {
		return nil, dynamodbStreamsValidationError("ShardIterator is a required parameter")
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

	rk := p.streamRecordsKey(cursor.AccountID, cursor.Region, cursor.TableName)
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
	nextCursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("dynamodb getRecords marshal: %w", err)
	}
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
	dynTokPlus      // +
	dynTokMinus     // -
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
		case ch == '+':
			tokens = append(tokens, dynamoToken{dynTokPlus, "+"})
			i++
		case ch == '-':
			tokens = append(tokens, dynamoToken{dynTokMinus, "-"})
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

	// Function call: attribute_exists, attribute_not_exists, begins_with, contains,
	// attribute_type. size() is an operand and is handled by evalOperand.
	if tok.typ == dynTokIdent && e.pos+1 < len(e.tokens) && e.tokens[e.pos+1].typ == dynTokLParen {
		switch fn := strings.ToLower(tok.value); fn {
		case "attribute_exists", "attribute_not_exists":
			e.next()
			e.next()
			path, err := e.evalPath()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after %s argument", fn)
			}
			e.next()
			exists := getAttrByPath(e.item, path) != nil
			if fn == "attribute_exists" {
				return exists, nil
			}
			return !exists, nil

		case "begins_with", "contains", "attribute_type":
			e.next()
			e.next()
			first, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokComma {
				return false, fmt.Errorf("expected ',' in %s", fn)
			}
			e.next()
			second, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if e.peek().typ != dynTokRParen {
				return false, fmt.Errorf("expected ')' after %s arguments", fn)
			}
			e.next()
			switch fn {
			case "begins_with":
				return avBeginsWith(first, second), nil
			case "contains":
				return avContains(first, second), nil
			default:
				return first != nil && second != nil && second.S != nil && avTypeName(first) == *second.S, nil
			}
		}
	}

	// Regular comparison: operand op operand
	left, err := e.evalOperand()
	if err != nil {
		return false, err
	}

	tok = e.peek()
	switch tok.typ {
	case dynTokEQ, dynTokNE, dynTokLT, dynTokLE, dynTokGT, dynTokGE:
		e.next()
		right, err := e.evalOperand()
		if err != nil {
			return false, err
		}
		return avCompareOp(tok.typ, left, right), nil
	}

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
		cLo, okLo := avOrder(left, lo)
		cHi, okHi := avOrder(left, hi)
		return okLo && okHi && cLo >= 0 && cHi <= 0, nil
	}

	if e.isKeyword(tok, "IN") {
		e.next()
		if e.peek().typ != dynTokLParen {
			return false, fmt.Errorf("expected '(' after IN")
		}
		e.next()
		matched := false
		for {
			val, err := e.evalOperand()
			if err != nil {
				return false, err
			}
			if avEqual(left, val) {
				matched = true
			}
			if e.peek().typ != dynTokComma {
				break
			}
			e.next()
		}
		if e.peek().typ != dynTokRParen {
			return false, fmt.Errorf("expected ')' after IN list")
		}
		e.next()
		return matched, nil
	}

	// No operator — treat as bool (e.g., function result).
	if left != nil && left.BOOL != nil {
		return *left.BOOL, nil
	}
	return left != nil, nil
}

// evalPath reads a document path (e.g. "a.b[0].c" or "#n") at the cursor.
func (e *condEvaluator) evalPath() ([]ddbPathSeg, error) {
	path, pos, err := parseDDBPath(e.tokens, e.pos, e.names)
	if err != nil {
		return nil, err
	}
	e.pos = pos
	return path, nil
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
	case av.B != nil:
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

// copyItem makes a deep copy of an item map, so an update applied to the copy —
// including to a nested map or list — never reaches the original.
func copyItem(item map[string]*AttributeValue) map[string]*AttributeValue {
	if item == nil {
		return nil
	}
	dst := make(map[string]*AttributeValue, len(item))
	for k, v := range item {
		dst[k] = deepCopyAV(v)
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
func parseKeyCondition(expr string, keySchema []DynamoDBKeySchemaElement, names map[string]string, values map[string]*AttributeValue) (pkAttr string, pkVal *AttributeValue, skAttr, skCondOp string, skLow, skHigh *AttributeValue, err error) {
	if expr == "" {
		return "", nil, "", "", nil, nil, fmt.Errorf("KeyConditionExpression is required")
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
		return "", nil, "", "", nil, nil, tokErr
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

	resolveValue := func(tok dynamoToken) (*AttributeValue, error) {
		if tok.typ != dynTokExprValue {
			return nil, fmt.Errorf("expected expression value, got %q", tok.value)
		}
		if values != nil {
			if av, ok := values[tok.value]; ok {
				return av, nil
			}
		}
		return nil, fmt.Errorf("expression value %q not defined", tok.value)
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
		return "", nil, "", "", nil, nil, fmt.Errorf("expected '=' in key condition, got %q", eqTok.value)
	}

	valTok := nextTok()
	pkValParsed, valErr := resolveValue(valTok)
	if valErr != nil {
		return "", nil, "", "", nil, nil, valErr
	}

	// If first attr is actually the SK, try to find PK by looking ahead.
	// For now assume PK comes first.
	pkVal = pkValParsed

	// No AND — done.
	if peekTok().typ == dynTokEOF {
		return pkAttr, pkVal, "", "", nil, nil, nil
	}

	// AND keyword.
	andTok := nextTok()
	if !strings.EqualFold(andTok.value, "AND") {
		return "", nil, "", "", nil, nil, fmt.Errorf("expected AND in key condition, got %q", andTok.value)
	}

	// SK condition.
	skTok := nextTok()

	// Check for begins_with(sk, :val)
	if strings.ToLower(skTok.value) == "begins_with" && skTok.typ == dynTokIdent {
		// begins_with(skAttr, :prefix)
		if peekTok().typ != dynTokLParen {
			return "", nil, "", "", nil, nil, fmt.Errorf("expected '(' after begins_with")
		}
		nextTok()     // consume (
		_ = nextTok() // skAttr inside parens
		if peekTok().typ != dynTokComma {
			return "", nil, "", "", nil, nil, fmt.Errorf("expected ',' in begins_with")
		}
		nextTok() // consume ,
		prefixTok := nextTok()
		prefix, prefixErr := resolveValue(prefixTok)
		if prefixErr != nil {
			return "", nil, "", "", nil, nil, prefixErr
		}
		if peekTok().typ != dynTokRParen {
			return "", nil, "", "", nil, nil, fmt.Errorf("expected ')' in begins_with")
		}
		nextTok() // consume )
		return pkAttr, pkVal, skAttr, "begins_with", prefix, nil, nil
	}

	// sk op :val or sk BETWEEN :lo AND :hi
	skAttrParsed := resolveToken(skTok)
	_ = skAttrParsed // we use skAttr from schema

	opTok := nextTok()
	if strings.EqualFold(opTok.value, "BETWEEN") {
		loTok := nextTok()
		lo, loErr := resolveValue(loTok)
		if loErr != nil {
			return "", nil, "", "", nil, nil, loErr
		}
		andTok2 := nextTok()
		if !strings.EqualFold(andTok2.value, "AND") {
			return "", nil, "", "", nil, nil, fmt.Errorf("expected AND in BETWEEN, got %q", andTok2.value)
		}
		hiTok := nextTok()
		hi, hiErr := resolveValue(hiTok)
		if hiErr != nil {
			return "", nil, "", "", nil, nil, hiErr
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
		return "", nil, "", "", nil, nil, fmt.Errorf("unsupported SK condition operator: %q", opTok.value)
	}

	valTok2 := nextTok()
	skValParsed, valErr2 := resolveValue(valTok2)
	if valErr2 != nil {
		return "", nil, "", "", nil, nil, valErr2
	}

	return pkAttr, pkVal, skAttr, condOp, skValParsed, nil, nil
}

// evalSKCondition evaluates a sort key condition with DynamoDB's typed comparison:
// Numbers numerically, Strings and Binaries by unsigned byte order (#1454). A value of a
// different type from the condition's does not match.
func evalSKCondition(skVal *AttributeValue, op string, low, high *AttributeValue) bool {
	if op == "begins_with" {
		return avBeginsWith(skVal, low)
	}
	if op == "=" {
		return avEqual(skVal, low)
	}
	c, ok := avOrder(skVal, low)
	if !ok {
		return false
	}
	switch op {
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	case "BETWEEN":
		h, hok := avOrder(skVal, high)
		return hok && c >= 0 && h <= 0
	}
	return false
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

	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, tableName)
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
		// The SET clause ends where the WHERE clause begins.
		if w := strings.Index(strings.ToUpper(setClause), " WHERE "); w >= 0 {
			setClause = strings.TrimSpace(setClause[:w])
		}
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
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	var results []map[string]*AttributeValue
	for _, k := range keys {
		item, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, k)
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

	ik, err := dynamodbItemKeyFor(tbl, item)
	if err != nil {
		return nil, err
	}

	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tbl.TableName, ik)
	defer unlock()
	old, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, ik)
	if err != nil {
		return nil, err
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, ik, old, item); err != nil {
		return nil, fmt.Errorf("dynamodb partiQLInsert: %w", err)
	}
	return nil, nil
}

func (p *DynamoDBPlugin) partiQLUpdate(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause, setClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	var (
		updateExpr string
		names      map[string]string
		values     map[string]*AttributeValue
	)
	if setClause != "" {
		var err error
		updateExpr, names, values, err = partiQLSetToUpdateExpression(setClause)
		if err != nil {
			return nil, &AWSError{
				Code:       "ValidationException",
				Message:    "Failed to apply SET clause: " + err.Error(),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}

	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := p.partiQLUpdateOne(ctx, tbl.TableName, k, whereClause, updateExpr, names, values); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// partiQLUpdateOne applies a PartiQL UPDATE to one item under the item's stripe. An
// item that does not exist or does not match the WHERE clause is left alone.
func (p *DynamoDBPlugin) partiQLUpdateOne(
	ctx *RequestContext,
	tableName, ik, whereClause, updateExpr string,
	names map[string]string,
	values map[string]*AttributeValue,
) error {
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tableName, ik)
	defer unlock()
	current, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	if whereClause != "" {
		if ok, _ := evalDynamoCondition(whereClause, nil, nil, current); !ok {
			return nil
		}
	}
	item := copyItem(current)
	if updateExpr != "" {
		if _, applyErr := applyUpdateExpression(item, updateExpr, names, values); applyErr != nil {
			return &AWSError{
				Code:       "ValidationException",
				Message:    "Failed to apply SET clause: " + applyErr.Error(),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik, current, item); err != nil {
		return fmt.Errorf("dynamodb partiQLUpdate: %w", err)
	}
	return nil
}

// partiQLSetToUpdateExpression rewrites a PartiQL SET clause — `a = 'x', "b" = 5` —
// as an update expression over placeholders, so it is applied by the same code as
// UpdateItem. Each value must be a literal: a single-quoted string (” escapes a
// quote), a number, or true/false. A double-quoted right-hand side is read as a string,
// as substrate has always accepted it.
func partiQLSetToUpdateExpression(setClause string) (string, map[string]string, map[string]*AttributeValue, error) {
	names := map[string]string{}
	values := map[string]*AttributeValue{}
	var actions []string
	for i, part := range partiQLSplitTopLevel(setClause, ',') {
		eq := partiQLIndexTopLevel(part, '=')
		if eq < 0 {
			return "", nil, nil, fmt.Errorf("SET action missing '=': %q", strings.TrimSpace(part))
		}
		lhs := strings.Trim(strings.TrimSpace(part[:eq]), `"`)
		rhs := strings.TrimSpace(part[eq+1:])
		if lhs == "" {
			return "", nil, nil, fmt.Errorf("SET action missing attribute name: %q", strings.TrimSpace(part))
		}
		av, err := partiQLLiteral(rhs)
		if err != nil {
			return "", nil, nil, err
		}
		n, v := "#p"+strconv.Itoa(i), ":p"+strconv.Itoa(i)
		names[n] = lhs
		values[v] = av
		actions = append(actions, n+" = "+v)
	}
	return "SET " + strings.Join(actions, ", "), names, values, nil
}

// partiQLLiteral reads one PartiQL literal value.
func partiQLLiteral(s string) (*AttributeValue, error) {
	switch {
	case len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'':
		v := strings.ReplaceAll(s[1:len(s)-1], "''", "'")
		return &AttributeValue{S: &v}, nil
	case len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"':
		v := s[1 : len(s)-1]
		return &AttributeValue{S: &v}, nil
	case strings.EqualFold(s, "true"), strings.EqualFold(s, "false"):
		b := strings.EqualFold(s, "true")
		return &AttributeValue{BOOL: &b}, nil
	}
	if _, ok := ddbParseNumber(s); ok {
		return &AttributeValue{N: s}, nil
	}
	return nil, fmt.Errorf("unsupported value in SET clause: %q", s)
}

// partiQLSplitTopLevel splits s at sep where sep is outside quotes.
func partiQLSplitTopLevel(s string, sep byte) []string {
	var parts []string
	start := 0
	for {
		i := partiQLIndexTopLevel(s[start:], sep)
		if i < 0 {
			return append(parts, s[start:])
		}
		parts = append(parts, s[start:start+i])
		start += i + 1
	}
}

// partiQLIndexTopLevel returns the index of the first sep in s outside quotes, or -1.
func partiQLIndexTopLevel(s string, sep byte) int {
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == sep:
			return i
		}
	}
	return -1
}

func (p *DynamoDBPlugin) partiQLDelete(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := p.partiQLDeleteOne(ctx, tbl.TableName, k, whereClause); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// partiQLDeleteOne applies a PartiQL DELETE to one item under the item's stripe.
func (p *DynamoDBPlugin) partiQLDeleteOne(ctx *RequestContext, tableName, ik, whereClause string) error {
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tableName, ik)
	defer unlock()
	current, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	if whereClause != "" {
		if ok, _ := evalDynamoCondition(whereClause, nil, nil, current); !ok {
			return nil
		}
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik, current, nil); err != nil {
		return fmt.Errorf("dynamodb partiQLDelete: %w", err)
	}
	return nil
}
