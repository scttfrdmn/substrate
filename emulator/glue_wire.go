package emulator

import "time"

// Glue answered its persisted records straight to the caller. The types below are the published
// shapes, projected from those records, on the pattern emulator/ecr_wire.go established (#1090)
// and emulator/efs_wire.go (#1304) and emulator/ecs_wire.go (#1309) followed.
//
// All six Glue records — GlueDatabase, GlueTable, GlueConnection, GlueCrawler, GlueJob and
// GlueJobRun (glue_types.go) — were handed to glueJSONResponse directly at every one of the twelve
// sites that answer a resource, so every one of those responses carried AccountID and Region
// unconditionally (neither has omitempty) and `ever_tagged` from whichever record had been tagged.
// Twenty-one baseline lines, the largest single block on #756 after ECS.
//
// # The timestamp is a near-miss on five of the six records
//
// Substrate spells one field `CreatedAt` on every record it persists. Glue publishes that
// timestamp under four different names, and on no shape is it `CreatedAt`:
//
//	record           substrate      published      authority
//	GlueDatabase     CreatedAt      CreateTime     API_Database
//	GlueTable        CreatedAt      CreateTime     API_Table
//	GlueConnection   CreatedAt      CreationTime   API_Connection
//	GlueCrawler      CreatedAt      CreationTime   API_Crawler
//	GlueJob          CreatedAt      CreatedOn      API_Job
//	GlueJobRun       StartedOn      StartedOn      API_JobRun — already right
//
// So the leak here is not merely an extra member, it is a near-miss of a real one on five records
// out of six: a caller reading `CreateTime` off a substrate Glue response found nothing, while
// `CreatedAt` sat beside it holding the value. That is the same shape of defect the
// check-wire-bookkeeping.sh header calls out for EFS, five times over, and it is why Glue was
// taken before the services with fewer declared fields.
//
// Every one of those members is Type: Timestamp, Required: No, so each is a pointer here and is
// omitted when the record's time is zero. Two things make the pointer necessary rather than
// decorative: omitempty has no effect on a struct type, and EpochSeconds.MarshalJSON renders the
// zero time as JSON null — so a plain field would report `"CreateTime":null` where AWS omits the
// member. ecsTaskOut.StartedAt is the same decision; efsFileSystemOut.CreationTime is the
// contrary case, a bare EpochSeconds because API_FileSystemDescription marks it Required: Yes.
//
// # Glue rendered every timestamp in the wrong format, and the projection fixes it
//
// The record's fields are time.Time, which marshals as an RFC3339 string. Glue's protocol is JSON
// and API_GetDatabase's own Response Syntax is literally `"CreateTime": number` — epoch seconds.
// That divergence is #1305, tracked against ACM, Redshift Data and Firehose; Glue was a fourth
// instance and is fixed in passing, because a projection has to choose a type for the member and
// choosing the published one costs nothing. See glueTimeOrNil.
//
// # Two members AWS does not publish on any Glue shape
//
// Projecting is where a response's membership is decided, so a member that is not on the published
// shape cannot be carried into a type whose whole purpose is to hold only published members. Two
// were resolved rather than copied:
//
//   - Arn, declared on all five of GlueDatabase, GlueTable, GlueConnection, GlueCrawler and
//     GlueJob. None of API_Database, API_Table, API_Connection, API_Crawler or API_Job has an Arn
//     member; neither has API_JobRun. A consumer that needs one builds it from the account, the
//     Region and the name, which is what the real API requires of it — and the SDK's own decoder
//     was discarding the member anyway, because the shape it decodes into has no field for it.
//
//   - Tags, declared on GlueDatabase, GlueConnection, GlueCrawler and GlueJob. No Glue shape
//     publishes tags inline. Glue's tag surface is GetTags/TagResource/UntagResource, which
//     substrate implements, and that is the only place a tag is observable. This is unlike ECS,
//     where the tags member moved from a nested object to a top-level response element: here there
//     is no published home for it on these operations at all.
//
// Both stay on the records, and both are still read — TaggingPlugin.scanGlueDatabases
// (tagging_plugin.go) reads Arn, Tags and EverTagged off the stored GlueDatabase to answer
// GetResources. Dropping either from the record would take a cross-service answer with it.
//
// # Where the leaked AccountID legitimately goes
//
// API_Database and API_Table each publish CatalogId, "the ID of the Data Catalog in which the
// database resides" — which is the AWS account ID. So on those two shapes the account is a
// published member under a published name, and the projection reports it from reqCtx.AccountID.
// That is the move #1304 made for EFS, where the leaked AccountID became the published OwnerId,
// and substrate already takes this position elsewhere: the CloudFormation deployer answers
// AWS::Glue::Database's CatalogId attribute with the deploying account.
//
// API_Connection, API_Crawler, API_Job and API_JobRun publish no CatalogId — each page was read —
// so those four shapes report no account at all.
//
// # What stays on the records
//
// AccountID, Region, CreatedAt, Arn, Tags and EverTagged stay in glue_types.go, untouched. The
// state keys already scope every record by account and Region so nothing reads those two back, but
// a persisted member is not free to remove: ecr_wire.go's rule is that the record's encoding is
// what MemoryStateManager snapshots and a replay reads, so a projection changes the response and
// leaves the record alone. The twenty-one baseline lines therefore stay in
// scripts/wire-bookkeeping-baseline.txt and are discharged in
// scripts/wire-bookkeeping-projected.txt instead.
//
// # What substrate does not model
//
// Each type below lists the published members it omits. They are absent rather than present and
// zero, per #1013: a count or a status reported as its zero value reads as a measurement, and
// substrate has not measured anything. Two are worth naming because a consumer is likely to look
// for them. API_Crawler.LastCrawl and API_JobRun.ErrorMessage both describe the outcome of work
// Glue performed, and substrate runs no crawl and no job — the scope boundary in doc.go — so it
// has nothing to report there. API_Table.UpdateTime and API_Connection.LastUpdatedTime are
// published and substrate persists no update timestamp at all, so they are absent for want of a
// value rather than by choice.

// glueTimeOrNil converts a record's time.Time to the epoch-seconds form Glue's JSON protocol
// publishes, returning nil for the zero time so an unset optional timestamp is omitted from the
// body rather than reported as JSON null.
//
// It takes a time.Time where ecsTimeOrNil takes an EpochSeconds, because ECS already stored the
// right type and Glue does not. Converting here rather than retyping the record is the whole
// point: see ecr_wire.go on why a persisted field's encoding cannot change.
func glueTimeOrNil(t time.Time) *EpochSeconds {
	if t.IsZero() {
		return nil
	}
	e := EpochSeconds(t)
	return &e
}

// glueDatabaseOut is the Database of GetDatabase and GetDatabases (under `DatabaseList`).
//
// Member names follow API_Database. Name is its one Required: Yes member; the rest are Required:
// No. The members substrate does not model — CreateTableDefaultPermissions, FederatedDatabase and
// TargetDatabase — are absent rather than present and empty.
type glueDatabaseOut struct {
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
	LocationURI string            `json:"LocationUri,omitempty"`
	Parameters  map[string]string `json:"Parameters,omitempty"`
	CatalogID   string            `json:"CatalogId,omitempty"`
	CreateTime  *EpochSeconds     `json:"CreateTime,omitempty"`
}

// glueDatabaseToWire projects a persisted database onto the published shape. catalogID is the
// request's account, which is what API_Database's CatalogId is; see the header.
func glueDatabaseToWire(db GlueDatabase, catalogID string) glueDatabaseOut {
	return glueDatabaseOut{
		Name:        db.Name,
		Description: db.Description,
		LocationURI: db.LocationURI,
		Parameters:  db.Parameters,
		CatalogID:   catalogID,
		CreateTime:  glueTimeOrNil(db.CreatedAt),
	}
}

// glueDatabasesToWire projects a slice of persisted databases, for GetDatabases.
func glueDatabasesToWire(databases []GlueDatabase, catalogID string) []glueDatabaseOut {
	out := make([]glueDatabaseOut, 0, len(databases))
	for _, db := range databases {
		out = append(out, glueDatabaseToWire(db, catalogID))
	}
	return out
}

// glueTableOut is the Table of GetTable and GetTables (under `TableList`).
//
// Member names follow API_Table. Name is its one Required: Yes member. The members substrate does
// not model — CreatedBy, FederatedTable, IsMaterializedView, IsMultiDialectView,
// IsRegisteredWithLakeFormation, LastAccessTime, LastAnalyzedTime, Owner, Retention, Status,
// TargetTable, UpdateTime, VersionId, ViewDefinition, ViewExpandedText and ViewOriginalText — are
// absent rather than present and empty.
type glueTableOut struct {
	Name              string                 `json:"Name"`
	DatabaseName      string                 `json:"DatabaseName,omitempty"`
	Description       string                 `json:"Description,omitempty"`
	TableType         string                 `json:"TableType,omitempty"`
	StorageDescriptor *GlueStorageDescriptor `json:"StorageDescriptor,omitempty"`
	PartitionKeys     []GlueColumn           `json:"PartitionKeys,omitempty"`
	Parameters        map[string]string      `json:"Parameters,omitempty"`
	CatalogID         string                 `json:"CatalogId,omitempty"`
	CreateTime        *EpochSeconds          `json:"CreateTime,omitempty"`
}

// glueTableToWire projects a persisted table onto the published shape.
func glueTableToWire(tbl GlueTable, catalogID string) glueTableOut {
	return glueTableOut{
		Name:              tbl.Name,
		DatabaseName:      tbl.DatabaseName,
		Description:       tbl.Description,
		TableType:         tbl.TableType,
		StorageDescriptor: tbl.StorageDescriptor,
		PartitionKeys:     tbl.PartitionKeys,
		Parameters:        tbl.Parameters,
		CatalogID:         catalogID,
		CreateTime:        glueTimeOrNil(tbl.CreatedAt),
	}
}

// glueTablesToWire projects a slice of persisted tables, for GetTables.
func glueTablesToWire(tables []GlueTable, catalogID string) []glueTableOut {
	out := make([]glueTableOut, 0, len(tables))
	for _, tbl := range tables {
		out = append(out, glueTableToWire(tbl, catalogID))
	}
	return out
}

// glueConnectionOut is the Connection of GetConnection and GetConnections (under
// `ConnectionList`).
//
// Member names follow API_Connection, on which every member is Required: No. API_Connection
// publishes no CatalogId, so no account is reported here. The members substrate does not model —
// AthenaProperties, AuthenticationConfiguration, CompatibleComputeEnvironments,
// ConnectionSchemaVersion, LastConnectionValidationTime, LastUpdatedBy, LastUpdatedTime,
// MatchCriteria, PhysicalConnectionRequirements, PythonProperties, SparkProperties, Status and
// StatusReason — are absent rather than present and empty. Status is the one worth naming: it
// reports whether Glue could reach the data source, which substrate does not attempt, so READY
// would be a claim it cannot make.
type glueConnectionOut struct {
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	CreationTime         *EpochSeconds     `json:"CreationTime,omitempty"`
}

// glueConnectionToWire projects a persisted connection onto the published shape.
func glueConnectionToWire(conn GlueConnection) glueConnectionOut {
	return glueConnectionOut{
		Name:                 conn.Name,
		Description:          conn.Description,
		ConnectionType:       conn.ConnectionType,
		ConnectionProperties: conn.ConnectionProperties,
		CreationTime:         glueTimeOrNil(conn.CreatedAt),
	}
}

// glueConnectionsToWire projects a slice of persisted connections, for GetConnections.
func glueConnectionsToWire(connections []GlueConnection) []glueConnectionOut {
	out := make([]glueConnectionOut, 0, len(connections))
	for _, conn := range connections {
		out = append(out, glueConnectionToWire(conn))
	}
	return out
}

// glueCrawlerOut is the Crawler of GetCrawler and GetCrawlers (under `Crawlers`).
//
// Member names follow API_Crawler, on which every member is Required: No. The members substrate
// does not model — Classifiers, Configuration, CrawlElapsedTime, CrawlerSecurityConfiguration,
// LakeFormationConfiguration, LastCrawl, LastUpdated, LineageConfiguration, RecrawlPolicy,
// Schedule, SchemaChangePolicy, TablePrefix and Version — are absent rather than present and
// empty. LastCrawl and CrawlElapsedTime are the ones a consumer is most likely to look for, and
// their absence is the scope boundary rather than an oversight: substrate does not run the crawl,
// so it has no crawl to report on.
type glueCrawlerOut struct {
	Name         string                 `json:"Name"`
	Role         string                 `json:"Role,omitempty"`
	DatabaseName string                 `json:"DatabaseName,omitempty"`
	Description  string                 `json:"Description,omitempty"`
	State        string                 `json:"State,omitempty"`
	Targets      map[string]interface{} `json:"Targets,omitempty"`
	CreationTime *EpochSeconds          `json:"CreationTime,omitempty"`
}

// glueCrawlerToWire projects a persisted crawler onto the published shape.
func glueCrawlerToWire(c GlueCrawler) glueCrawlerOut {
	return glueCrawlerOut{
		Name:         c.Name,
		Role:         c.Role,
		DatabaseName: c.DatabaseName,
		Description:  c.Description,
		State:        c.State,
		Targets:      c.Targets,
		CreationTime: glueTimeOrNil(c.CreatedAt),
	}
}

// glueCrawlersToWire projects a slice of persisted crawlers, for GetCrawlers.
func glueCrawlersToWire(crawlers []GlueCrawler) []glueCrawlerOut {
	out := make([]glueCrawlerOut, 0, len(crawlers))
	for _, c := range crawlers {
		out = append(out, glueCrawlerToWire(c))
	}
	return out
}

// glueJobOut is the Job of GetJob and GetJobs (under `Jobs`).
//
// Member names follow API_Job, on which every member is Required: No. The members substrate does
// not model — AllocatedCapacity, CodeGenConfigurationNodes, Connections, DefaultArguments,
// ExecutionClass, ExecutionProperty, GlueVersion, JobMode, JobRunQueuingEnabled, LastModifiedOn,
// LogUri, MaintenanceWindow, MaxCapacity, MaxRetries, NonOverridableArguments,
// NotificationProperty, NumberOfWorkers, ProfileName, SecurityConfiguration, SourceControlDetails,
// Timeout and WorkerType — are absent rather than present and zero. The sizing members are the
// clearest case: substrate allocates no DPU, so reporting MaxCapacity 0 would describe a job
// nobody configured.
type glueJobOut struct {
	Name        string         `json:"Name"`
	Role        string         `json:"Role,omitempty"`
	Description string         `json:"Description,omitempty"`
	Command     GlueJobCommand `json:"Command"`
	CreatedOn   *EpochSeconds  `json:"CreatedOn,omitempty"`
}

// glueJobToWire projects a persisted job onto the published shape.
func glueJobToWire(job GlueJob) glueJobOut {
	return glueJobOut{
		Name:        job.Name,
		Role:        job.Role,
		Description: job.Description,
		Command:     job.Command,
		CreatedOn:   glueTimeOrNil(job.CreatedAt),
	}
}

// glueJobsToWire projects a slice of persisted jobs, for GetJobs.
func glueJobsToWire(jobs []GlueJob) []glueJobOut {
	out := make([]glueJobOut, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, glueJobToWire(job))
	}
	return out
}

// glueJobRunOut is the JobRun of GetJobRun and GetJobRuns (under `JobRuns`).
//
// Member names follow API_JobRun, on which every member is Required: No. This is the one record
// whose timestamps substrate already named correctly; only their format and the two bookkeeping
// members change. The members substrate does not model — AllocatedCapacity, Arguments, Attempt,
// DPUSeconds, ErrorMessage, ExecutionClass, ExecutionRoleSessionPolicy, ExecutionTime,
// GlueVersion, JobMode, JobRunQueuingEnabled, LastModifiedOn, LogGroupName, MaintenanceWindow,
// MaxCapacity, NotificationProperty, NumberOfWorkers, PredecessorRuns, PreviousRunId, ProfileName,
// SecurityConfiguration, StateDetail, Timeout, TriggerName and WorkerType — are absent rather than
// present and empty. ErrorMessage and ExecutionTime are the ones a consumer would reach for, and
// substrate executes no job, so it measures neither.
type glueJobRunOut struct {
	ID          string        `json:"Id"`
	JobName     string        `json:"JobName,omitempty"`
	JobRunState string        `json:"JobRunState,omitempty"`
	StartedOn   *EpochSeconds `json:"StartedOn,omitempty"`
	CompletedOn *EpochSeconds `json:"CompletedOn,omitempty"`
}

// glueJobRunToWire projects a persisted job run onto the published shape.
func glueJobRunToWire(run GlueJobRun) glueJobRunOut {
	return glueJobRunOut{
		ID:          run.ID,
		JobName:     run.JobName,
		JobRunState: run.JobRunState,
		StartedOn:   glueTimeOrNil(run.StartedOn),
		CompletedOn: glueTimeOrNil(run.CompletedOn),
	}
}

// glueJobRunsToWire projects a slice of persisted job runs, for GetJobRuns.
func glueJobRunsToWire(runs []GlueJobRun) []glueJobRunOut {
	out := make([]glueJobRunOut, 0, len(runs))
	for _, run := range runs {
		out = append(out, glueJobRunToWire(run))
	}
	return out
}
