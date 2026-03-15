package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// GluePlugin emulates the AWS Glue ETL and Data Catalog service.
// It handles database, table, connection, crawler, and job CRUD operations
// using the Glue JSON-target protocol (X-Amz-Target: AWSGlue.{Op}).
type GluePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "glue".
func (p *GluePlugin) Name() string { return glueNamespace }

// Initialize sets up the GluePlugin with the provided configuration.
func (p *GluePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for GluePlugin.
func (p *GluePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Glue JSON-target request to the appropriate handler.
func (p *GluePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateDatabase":
		return p.createDatabase(ctx, req)
	case "GetDatabase":
		return p.getDatabase(ctx, req)
	case "GetDatabases":
		return p.getDatabases(ctx, req)
	case "UpdateDatabase":
		return p.updateDatabase(ctx, req)
	case "DeleteDatabase":
		return p.deleteDatabase(ctx, req)
	case "CreateTable":
		return p.createTable(ctx, req)
	case "GetTable":
		return p.getTable(ctx, req)
	case "GetTables":
		return p.getTables(ctx, req)
	case "UpdateTable":
		return p.updateTable(ctx, req)
	case "DeleteTable":
		return p.deleteTable(ctx, req)
	case "CreateConnection":
		return p.createConnection(ctx, req)
	case "GetConnection":
		return p.getConnection(ctx, req)
	case "GetConnections":
		return p.getConnections(ctx, req)
	case "UpdateConnection":
		return p.updateConnection(ctx, req)
	case "DeleteConnection":
		return p.deleteConnection(ctx, req)
	case "CreateCrawler":
		return p.createCrawler(ctx, req)
	case "GetCrawler":
		return p.getCrawler(ctx, req)
	case "GetCrawlers":
		return p.getCrawlers(ctx, req)
	case "StartCrawler":
		return p.startCrawler(ctx, req)
	case "StopCrawler":
		return p.stopCrawler(ctx, req)
	case "UpdateCrawler":
		return p.updateCrawler(ctx, req)
	case "DeleteCrawler":
		return p.deleteCrawler(ctx, req)
	case "CreateJob":
		return p.createJob(ctx, req)
	case "GetJob":
		return p.getJob(ctx, req)
	case "GetJobs":
		return p.getJobs(ctx, req)
	case "UpdateJob":
		return p.updateJob(ctx, req)
	case "DeleteJob":
		return p.deleteJob(ctx, req)
	case "StartJobRun":
		return p.startJobRun(ctx, req)
	case "GetJobRun":
		return p.getJobRun(ctx, req)
	case "GetJobRuns":
		return p.getJobRuns(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "GetTags":
		return p.getTags(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "GluePlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Database operations ---

func (p *GluePlugin) createDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseInput struct {
			Name        string            `json:"Name"`
			Description string            `json:"Description"`
			LocationURI string            `json:"LocationUri"`
			Parameters  map[string]string `json:"Parameters"`
		} `json:"DatabaseInput"`
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	name := input.DatabaseInput.Name
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "DatabaseInput.Name is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:glue:%s:%s:database/%s", reqCtx.Region, reqCtx.AccountID, name)
	db := GlueDatabase{
		Name:        name,
		Description: input.DatabaseInput.Description,
		LocationURI: input.DatabaseInput.LocationURI,
		Parameters:  input.DatabaseInput.Parameters,
		Arn:         arn,
		Tags:        input.Tags,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		CreatedAt:   p.tc.Now(),
	}

	goCtx := context.Background()
	data, err := json.Marshal(db)
	if err != nil {
		return nil, fmt.Errorf("glue createDatabase marshal: %w", err)
	}
	if err := p.state.Put(goCtx, glueNamespace, "database:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name, data); err != nil {
		return nil, fmt.Errorf("glue createDatabase put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "database_names:"+reqCtx.AccountID+"/"+reqCtx.Region, name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) getDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	raw, err := p.state.Get(goCtx, glueNamespace, "database:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name)
	if err != nil {
		return nil, fmt.Errorf("glue getDatabase get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Database " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var db GlueDatabase
	if err := json.Unmarshal(raw, &db); err != nil {
		return nil, fmt.Errorf("glue getDatabase unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Database": db})
}

func (p *GluePlugin) getDatabases(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, glueNamespace, "database_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("glue getDatabases list: %w", err)
	}
	var databases []GlueDatabase
	for _, name := range names {
		raw, err := p.state.Get(goCtx, glueNamespace, "database:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
		if err != nil || raw == nil {
			continue
		}
		var db GlueDatabase
		if json.Unmarshal(raw, &db) != nil {
			continue
		}
		databases = append(databases, db)
	}
	if databases == nil {
		databases = []GlueDatabase{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"DatabaseList": databases})
}

func (p *GluePlugin) updateDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name          string `json:"Name"`
		DatabaseInput struct {
			Description string            `json:"Description"`
			LocationURI string            `json:"LocationUri"`
			Parameters  map[string]string `json:"Parameters"`
		} `json:"DatabaseInput"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "database:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.Name
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue updateDatabase get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Database " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var db GlueDatabase
	if err := json.Unmarshal(raw, &db); err != nil {
		return nil, fmt.Errorf("glue updateDatabase unmarshal: %w", err)
	}
	if input.DatabaseInput.Description != "" {
		db.Description = input.DatabaseInput.Description
	}
	if input.DatabaseInput.LocationURI != "" {
		db.LocationURI = input.DatabaseInput.LocationURI
	}
	if len(input.DatabaseInput.Parameters) > 0 {
		db.Parameters = input.DatabaseInput.Parameters
	}
	updated, _ := json.Marshal(db)
	if err := p.state.Put(goCtx, glueNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("glue updateDatabase put: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) deleteDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, glueNamespace, "database:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name); err != nil {
		return nil, fmt.Errorf("glue deleteDatabase delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, glueNamespace, "database_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

// --- Table operations ---

func (p *GluePlugin) createTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		TableInput   struct {
			Name              string                 `json:"Name"`
			Description       string                 `json:"Description"`
			TableType         string                 `json:"TableType"`
			StorageDescriptor *GlueStorageDescriptor `json:"StorageDescriptor"`
		} `json:"TableInput"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:glue:%s:%s:table/%s/%s", reqCtx.Region, reqCtx.AccountID, input.DatabaseName, input.TableInput.Name)
	tbl := GlueTable{
		Name:              input.TableInput.Name,
		DatabaseName:      input.DatabaseName,
		Description:       input.TableInput.Description,
		TableType:         input.TableInput.TableType,
		StorageDescriptor: input.TableInput.StorageDescriptor,
		Arn:               arn,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
		CreatedAt:         p.tc.Now(),
	}

	goCtx := context.Background()
	data, _ := json.Marshal(tbl)
	if err := p.state.Put(goCtx, glueNamespace, "table:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName+"/"+input.TableInput.Name, data); err != nil {
		return nil, fmt.Errorf("glue createTable put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "table_names:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName, input.TableInput.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) getTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		Name         string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "table:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.DatabaseName + "/" + input.Name
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue getTable get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Table " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var tbl GlueTable
	if err := json.Unmarshal(raw, &tbl); err != nil {
		return nil, fmt.Errorf("glue getTable unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Table": tbl})
}

func (p *GluePlugin) getTables(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, glueNamespace, "table_names:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("glue getTables list: %w", err)
	}
	var tables []GlueTable
	for _, name := range names {
		raw, err := p.state.Get(goCtx, glueNamespace, "table:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName+"/"+name)
		if err != nil || raw == nil {
			continue
		}
		var tbl GlueTable
		if json.Unmarshal(raw, &tbl) != nil {
			continue
		}
		tables = append(tables, tbl)
	}
	if tables == nil {
		tables = []GlueTable{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"TableList": tables})
}

func (p *GluePlugin) updateTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		TableInput   struct {
			Name              string                 `json:"Name"`
			Description       string                 `json:"Description"`
			StorageDescriptor *GlueStorageDescriptor `json:"StorageDescriptor"`
		} `json:"TableInput"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "table:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.DatabaseName + "/" + input.TableInput.Name
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue updateTable get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Table " + input.TableInput.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var tbl GlueTable
	if err := json.Unmarshal(raw, &tbl); err != nil {
		return nil, fmt.Errorf("glue updateTable unmarshal: %w", err)
	}
	if input.TableInput.Description != "" {
		tbl.Description = input.TableInput.Description
	}
	if input.TableInput.StorageDescriptor != nil {
		tbl.StorageDescriptor = input.TableInput.StorageDescriptor
	}
	updated, _ := json.Marshal(tbl)
	if err := p.state.Put(goCtx, glueNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("glue updateTable put: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) deleteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		Name         string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, glueNamespace, "table:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName+"/"+input.Name); err != nil {
		return nil, fmt.Errorf("glue deleteTable delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, glueNamespace, "table_names:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.DatabaseName, input.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

// --- Connection operations ---

func (p *GluePlugin) createConnection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ConnectionInput struct {
			Name                 string            `json:"Name"`
			Description          string            `json:"Description"`
			ConnectionType       string            `json:"ConnectionType"`
			ConnectionProperties map[string]string `json:"ConnectionProperties"`
		} `json:"ConnectionInput"`
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:glue:%s:%s:connection/%s", reqCtx.Region, reqCtx.AccountID, input.ConnectionInput.Name)
	conn := GlueConnection{
		Name:                 input.ConnectionInput.Name,
		Description:          input.ConnectionInput.Description,
		ConnectionType:       input.ConnectionInput.ConnectionType,
		ConnectionProperties: input.ConnectionInput.ConnectionProperties,
		Arn:                  arn,
		Tags:                 input.Tags,
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
		CreatedAt:            p.tc.Now(),
	}

	goCtx := context.Background()
	data, _ := json.Marshal(conn)
	if err := p.state.Put(goCtx, glueNamespace, "connection:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+conn.Name, data); err != nil {
		return nil, fmt.Errorf("glue createConnection put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "connection_names:"+reqCtx.AccountID+"/"+reqCtx.Region, conn.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) getConnection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	raw, err := p.state.Get(goCtx, glueNamespace, "connection:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name)
	if err != nil {
		return nil, fmt.Errorf("glue getConnection get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Connection " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var conn GlueConnection
	if err := json.Unmarshal(raw, &conn); err != nil {
		return nil, fmt.Errorf("glue getConnection unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Connection": conn})
}

func (p *GluePlugin) getConnections(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, glueNamespace, "connection_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("glue getConnections list: %w", err)
	}
	var connections []GlueConnection
	for _, name := range names {
		raw, err := p.state.Get(goCtx, glueNamespace, "connection:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
		if err != nil || raw == nil {
			continue
		}
		var conn GlueConnection
		if json.Unmarshal(raw, &conn) != nil {
			continue
		}
		connections = append(connections, conn)
	}
	if connections == nil {
		connections = []GlueConnection{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"ConnectionList": connections})
}

func (p *GluePlugin) updateConnection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name            string `json:"Name"`
		ConnectionInput struct {
			Description          string            `json:"Description"`
			ConnectionType       string            `json:"ConnectionType"`
			ConnectionProperties map[string]string `json:"ConnectionProperties"`
		} `json:"ConnectionInput"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "connection:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.Name
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue updateConnection get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Connection " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var conn GlueConnection
	if err := json.Unmarshal(raw, &conn); err != nil {
		return nil, fmt.Errorf("glue updateConnection unmarshal: %w", err)
	}
	if input.ConnectionInput.Description != "" {
		conn.Description = input.ConnectionInput.Description
	}
	if input.ConnectionInput.ConnectionType != "" {
		conn.ConnectionType = input.ConnectionInput.ConnectionType
	}
	if len(input.ConnectionInput.ConnectionProperties) > 0 {
		conn.ConnectionProperties = input.ConnectionInput.ConnectionProperties
	}
	updated, _ := json.Marshal(conn)
	if err := p.state.Put(goCtx, glueNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("glue updateConnection put: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) deleteConnection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ConnectionName string `json:"ConnectionName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, glueNamespace, "connection:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.ConnectionName); err != nil {
		return nil, fmt.Errorf("glue deleteConnection delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, glueNamespace, "connection_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.ConnectionName)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

// --- Crawler operations ---

func (p *GluePlugin) createCrawler(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name         string                 `json:"Name"`
		Role         string                 `json:"Role"`
		DatabaseName string                 `json:"DatabaseName"`
		Description  string                 `json:"Description"`
		Targets      map[string]interface{} `json:"Targets"`
		Tags         map[string]string      `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:glue:%s:%s:crawler/%s", reqCtx.Region, reqCtx.AccountID, input.Name)
	crawler := GlueCrawler{
		Name:         input.Name,
		Role:         input.Role,
		DatabaseName: input.DatabaseName,
		Description:  input.Description,
		State:        "READY",
		Targets:      input.Targets,
		Arn:          arn,
		Tags:         input.Tags,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
		CreatedAt:    p.tc.Now(),
	}

	goCtx := context.Background()
	data, _ := json.Marshal(crawler)
	if err := p.state.Put(goCtx, glueNamespace, "crawler:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name, data); err != nil {
		return nil, fmt.Errorf("glue createCrawler put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "crawler_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) getCrawler(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	raw, err := p.state.Get(goCtx, glueNamespace, "crawler:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name)
	if err != nil {
		return nil, fmt.Errorf("glue getCrawler get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Crawler " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var crawler GlueCrawler
	if err := json.Unmarshal(raw, &crawler); err != nil {
		return nil, fmt.Errorf("glue getCrawler unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Crawler": crawler})
}

func (p *GluePlugin) getCrawlers(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, glueNamespace, "crawler_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("glue getCrawlers list: %w", err)
	}
	var crawlers []GlueCrawler
	for _, name := range names {
		raw, err := p.state.Get(goCtx, glueNamespace, "crawler:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
		if err != nil || raw == nil {
			continue
		}
		var c GlueCrawler
		if json.Unmarshal(raw, &c) != nil {
			continue
		}
		crawlers = append(crawlers, c)
	}
	if crawlers == nil {
		crawlers = []GlueCrawler{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Crawlers": crawlers})
}

// startCrawler is a deterministic no-op: crawlers always stay READY in the emulator.
func (p *GluePlugin) startCrawler(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

// stopCrawler is a deterministic no-op: crawlers always stay READY in the emulator.
func (p *GluePlugin) stopCrawler(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) updateCrawler(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name        string                 `json:"Name"`
		Description string                 `json:"Description"`
		Targets     map[string]interface{} `json:"Targets"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "crawler:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.Name
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue updateCrawler get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Crawler " + input.Name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var crawler GlueCrawler
	if err := json.Unmarshal(raw, &crawler); err != nil {
		return nil, fmt.Errorf("glue updateCrawler unmarshal: %w", err)
	}
	if input.Description != "" {
		crawler.Description = input.Description
	}
	if len(input.Targets) > 0 {
		crawler.Targets = input.Targets
	}
	updated, _ := json.Marshal(crawler)
	if err := p.state.Put(goCtx, glueNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("glue updateCrawler put: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) deleteCrawler(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, glueNamespace, "crawler:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name); err != nil {
		return nil, fmt.Errorf("glue deleteCrawler delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, glueNamespace, "crawler_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

// --- Job operations ---

func (p *GluePlugin) createJob(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name        string            `json:"Name"`
		Role        string            `json:"Role"`
		Description string            `json:"Description"`
		Command     GlueJobCommand    `json:"Command"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:glue:%s:%s:job/%s", reqCtx.Region, reqCtx.AccountID, input.Name)
	job := GlueJob{
		Name:        input.Name,
		Role:        input.Role,
		Description: input.Description,
		Command:     input.Command,
		Arn:         arn,
		Tags:        input.Tags,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		CreatedAt:   p.tc.Now(),
	}

	goCtx := context.Background()
	data, _ := json.Marshal(job)
	if err := p.state.Put(goCtx, glueNamespace, "job:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.Name, data); err != nil {
		return nil, fmt.Errorf("glue createJob put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "job_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.Name)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Name": input.Name})
}

func (p *GluePlugin) getJob(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName string `json:"JobName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	raw, err := p.state.Get(goCtx, glueNamespace, "job:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName)
	if err != nil {
		return nil, fmt.Errorf("glue getJob get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Job " + input.JobName + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var job GlueJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("glue getJob unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Job": job})
}

func (p *GluePlugin) getJobs(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, glueNamespace, "job_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("glue getJobs list: %w", err)
	}
	var jobs []GlueJob
	for _, name := range names {
		raw, err := p.state.Get(goCtx, glueNamespace, "job:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
		if err != nil || raw == nil {
			continue
		}
		var j GlueJob
		if json.Unmarshal(raw, &j) != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if jobs == nil {
		jobs = []GlueJob{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Jobs": jobs})
}

func (p *GluePlugin) updateJob(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName   string `json:"JobName"`
		JobUpdate struct {
			Description string          `json:"Description"`
			Command     *GlueJobCommand `json:"Command"`
		} `json:"JobUpdate"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "job:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.JobName
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue updateJob get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Job " + input.JobName + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var job GlueJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("glue updateJob unmarshal: %w", err)
	}
	if input.JobUpdate.Description != "" {
		job.Description = input.JobUpdate.Description
	}
	if input.JobUpdate.Command != nil {
		job.Command = *input.JobUpdate.Command
	}
	updated, _ := json.Marshal(job)
	if err := p.state.Put(goCtx, glueNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("glue updateJob put: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"JobName": input.JobName})
}

func (p *GluePlugin) deleteJob(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName string `json:"JobName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, glueNamespace, "job:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName); err != nil {
		return nil, fmt.Errorf("glue deleteJob delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, glueNamespace, "job_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.JobName)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"JobName": input.JobName})
}

func (p *GluePlugin) startJobRun(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName string `json:"JobName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	raw, err := p.state.Get(goCtx, glueNamespace, "job:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName)
	if err != nil {
		return nil, fmt.Errorf("glue startJobRun get job: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Job " + input.JobName + " not found.", HTTPStatus: http.StatusNotFound}
	}

	now := p.tc.Now()
	runID := "jr_" + randomHex(16)
	run := GlueJobRun{
		ID:          runID,
		JobName:     input.JobName,
		JobRunState: "SUCCEEDED",
		StartedOn:   now,
		CompletedOn: now,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}

	data, _ := json.Marshal(run)
	if err := p.state.Put(goCtx, glueNamespace, "jobrun:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName+"/"+runID, data); err != nil {
		return nil, fmt.Errorf("glue startJobRun put: %w", err)
	}
	updateStringIndex(goCtx, p.state, glueNamespace, "jobrun_ids:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName, runID)
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"JobRunId": runID})
}

func (p *GluePlugin) getJobRun(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName string `json:"JobName"`
		RunID   string `json:"RunId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "jobrun:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.JobName + "/" + input.RunID
	raw, err := p.state.Get(goCtx, glueNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("glue getJobRun get: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{Code: "EntityNotFoundException", Message: "Job run " + input.RunID + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var run GlueJobRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return nil, fmt.Errorf("glue getJobRun unmarshal: %w", err)
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"JobRun": run})
}

func (p *GluePlugin) getJobRuns(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		JobName string `json:"JobName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, glueNamespace, "jobrun_ids:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName)
	if err != nil {
		return nil, fmt.Errorf("glue getJobRuns list: %w", err)
	}
	var runs []GlueJobRun
	for _, id := range ids {
		raw, err := p.state.Get(goCtx, glueNamespace, "jobrun:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.JobName+"/"+id)
		if err != nil || raw == nil {
			continue
		}
		var run GlueJobRun
		if json.Unmarshal(raw, &run) != nil {
			continue
		}
		runs = append(runs, run)
	}
	if runs == nil {
		runs = []GlueJobRun{}
	}
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"JobRuns": runs})
}

// --- Tagging operations ---

func (p *GluePlugin) tagResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string            `json:"ResourceArn"`
		TagsToAdd   map[string]string `json:"TagsToAdd"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	ns, key, err := resolveGlueARN(input.ResourceArn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if mergeErr := p.mergeGlueTags(goCtx, ns, key, input.TagsToAdd, nil); mergeErr != nil {
		return nil, mergeErr
	}
	_ = reqCtx
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) untagResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn  string   `json:"ResourceArn"`
		TagsToRemove []string `json:"TagsToRemove"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	ns, key, err := resolveGlueARN(input.ResourceArn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if mergeErr := p.mergeGlueTags(goCtx, ns, key, nil, input.TagsToRemove); mergeErr != nil {
		return nil, mergeErr
	}
	_ = reqCtx
	return glueJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *GluePlugin) getTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	ns, key, err := resolveGlueARN(input.ResourceArn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValueException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	tags, err := p.loadGlueTags(goCtx, ns, key)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	_ = reqCtx
	return glueJSONResponse(http.StatusOK, map[string]interface{}{"Tags": tags})
}

// --- Internal helpers ---

// resolveGlueARN parses a Glue ARN and returns the state (namespace, key).
// Glue ARN format: arn:aws:glue:{region}:{acct}:{resourceType}/{name}.
func resolveGlueARN(arn string) (ns, key string, err error) {
	const prefix = "arn:aws:glue:"
	if !strings.HasPrefix(arn, prefix) {
		return "", "", fmt.Errorf("invalid Glue ARN: %q", arn)
	}
	rest := arn[len(prefix):]
	// rest = "{region}:{acct}:{resourceType}/{name}"
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid Glue ARN: %q", arn)
	}
	region := parts[0]
	acct := parts[1]
	resource := parts[2]

	slashIdx := strings.IndexByte(resource, '/')
	if slashIdx < 0 {
		return "", "", fmt.Errorf("invalid Glue ARN resource %q", resource)
	}
	rtype := resource[:slashIdx]
	rname := resource[slashIdx+1:]

	switch rtype {
	case "database":
		return glueNamespace, "database:" + acct + "/" + region + "/" + rname, nil
	case "table":
		// table/{dbName}/{tableName}
		dbSlash := strings.IndexByte(rname, '/')
		if dbSlash < 0 {
			return glueNamespace, "table:" + acct + "/" + region + "/" + rname, nil
		}
		dbName := rname[:dbSlash]
		tblName := rname[dbSlash+1:]
		return glueNamespace, "table:" + acct + "/" + region + "/" + dbName + "/" + tblName, nil
	case "connection":
		return glueNamespace, "connection:" + acct + "/" + region + "/" + rname, nil
	case "crawler":
		return glueNamespace, "crawler:" + acct + "/" + region + "/" + rname, nil
	case "job":
		return glueNamespace, "job:" + acct + "/" + region + "/" + rname, nil
	default:
		return "", "", fmt.Errorf("unsupported Glue resource type %q in ARN", rtype)
	}
}

// mergeGlueTags loads the resource at ns/key, merges addTags and removes removeKeys.
func (p *GluePlugin) mergeGlueTags(goCtx context.Context, ns, key string, addTags map[string]string, removeKeys []string) error {
	raw, err := p.state.Get(goCtx, ns, key)
	if err != nil {
		return fmt.Errorf("glue mergeGlueTags get: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("glue resource not found at %s/%s", ns, key)
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("glue mergeGlueTags unmarshal: %w", err)
	}

	existingTags := make(map[string]string)
	if tagRaw, ok := obj["Tags"]; ok && tagRaw != nil {
		if tagMap, ok := tagRaw.(map[string]interface{}); ok {
			for k, v := range tagMap {
				if sv, ok := v.(string); ok {
					existingTags[k] = sv
				}
			}
		}
	}
	merged := mergeStringMap(existingTags, addTags, removeKeys)
	obj["Tags"] = merged

	updated, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("glue mergeGlueTags marshal: %w", err)
	}
	return p.state.Put(goCtx, ns, key, updated)
}

// loadGlueTags loads the Tags map for a Glue resource.
func (p *GluePlugin) loadGlueTags(goCtx context.Context, ns, key string) (map[string]string, error) {
	raw, err := p.state.Get(goCtx, ns, key)
	if err != nil {
		return nil, fmt.Errorf("glue loadGlueTags get: %w", err)
	}
	if raw == nil {
		return nil, fmt.Errorf("glue resource not found at %s/%s", ns, key)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("glue loadGlueTags unmarshal: %w", err)
	}
	if tagRaw, ok := obj["Tags"]; ok && tagRaw != nil {
		if tagMap, ok := tagRaw.(map[string]interface{}); ok {
			result := make(map[string]string, len(tagMap))
			for k, v := range tagMap {
				if sv, ok := v.(string); ok {
					result[k] = sv
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// glueJSONResponse serializes v to JSON and returns an AWSResponse.
func glueJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("glue json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
