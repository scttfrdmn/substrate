package emulator

import "time"

// RDS was already answering from a projection when #756 reached it, which makes it a different job
// from the services before it. The types below are the published shapes; they were lifted out of
// rds_plugin.go unchanged except where noted, on the pattern emulator/ecr_wire.go established
// (#1090) and emulator/efs_wire.go (#1304), emulator/ecs_wire.go (#1309) and emulator/glue_wire.go
// (#1310) followed.
//
// # Why this file is a pin rather than a fix
//
// All five RDS records — RDSDBInstance, RDSDBCluster, RDSDBSnapshot, RDSDBSubnetGroup and
// RDSDBParameterGroup (rds_types.go) — carry AccountID and Region, and three of them carry CreatedAt
// and EverTagged: sixteen baseline lines, the largest single block on #756 after ECS and Glue. But
// `grep 'RDSDB[A-Za-z]* .*`xml:'` matches nothing in the tree. Not one record is XML-encoded
// directly; every one of the seventeen sites that answers a resource hands a *…ToXML item struct to
// rdsXMLResponse, so no bookkeeping member could reach a body even before this change.
//
// What was missing was therefore not a projection but the assertion that one exists: the sixteen
// lines were reachable-by-declaration with nothing pinning them shut, exactly the position ECS's
// four already-projecting records were in at #1308. emulator/rds_wire_test.go is that pin and
// scripts/wire-bookkeeping-projected.txt cites it. The lines stay in
// scripts/wire-bookkeeping-baseline.txt because ecr_wire.go's rule forbids removing them:
// `json:"-"` and retyping a field in place both change the format of every recorded run, since
// MemoryStateManager snapshots those bytes and a replay reads them back.
//
// # The leaked account and Region have no published home here, and SourceRegion is not it
//
// Unlike EFS, where the leaked AccountID became the published OwnerId (#1304), and Glue, where it
// became CatalogId (#1310), no RDS shape publishes an account or a Region member. Every one of the
// five publishes the resource's ARN instead — DBInstanceArn, DBClusterArn, DBSnapshotArn,
// DBSubnetGroupArn, DBParameterGroupArn — and both values are recoverable from it, which is what the
// real API requires of a caller.
//
// API_DBSnapshot.SourceRegion looks like the exception and is not. It is "the AWS Region that the DB
// snapshot was created in or copied from", and it carries a value only for a cross-account or
// cross-Region copy. Substrate performs no copy, so reporting the request's Region there would tell
// a caller the snapshot had been copied from somewhere. Absent is the honest answer.
//
// # The creation time was persisted and reported nowhere
//
// Three records store CreatedAt from the simulated clock, and no item struct carried it. AWS
// publishes a creation timestamp on each of those three shapes, under a different name on each:
//
//	record            substrate   published            authority
//	RDSDBInstance     CreatedAt   InstanceCreateTime   API_DBInstance
//	RDSDBCluster      CreatedAt   ClusterCreateTime    API_DBCluster
//	RDSDBSnapshot     CreatedAt   SnapshotCreateTime   API_DBSnapshot
//
// So this is a gap rather than a #1013 "substrate has not measured it" absence: substrate holds the
// value and a consumer asking the question got nothing. All three are Type: Timestamp, Required: No,
// so each is a pointer here and is omitted when the record's time is zero — see rdsTimeOrNil.
//
// API_DBSubnetGroup and API_DBParameterGroup publish no creation-time member at all, which is why
// those two records store no CreatedAt and why nothing is added to their item structs.
//
// The format needs no fix, unlike Glue's. The AWS Query protocol's default timestamp format is
// ISO8601 and encoding/xml renders a time.Time as RFC3339Nano, which satisfies it — so RDS is not an
// instance of #1305. redshift_plugin.go's ClusterCreateTime is the in-tree precedent for a bare
// time.Time in a query-protocol item struct.
//
// # Two cluster-shape errors the single item struct resolves
//
// xmlClusterItem used to be declared inline in three separate functions, and the three did not
// agree. Consolidating them into xmlDBClusterItem fixes both disagreements:
//
//   - DeleteDBCluster reported three members — DBClusterIdentifier, Status and DBClusterArn — of the
//     eleven its copy of the record held, because its inline struct declared only those three.
//     API_DeleteDBCluster's response element is the full DBCluster, so the other eight were a
//     truncation rather than a choice. It now answers the same membership the other two sites do.
//
//   - DBSubnetGroup was nested. All three sites rendered `DBSubnetGroup>DBSubnetGroupName`, but
//     API_DBCluster.DBSubnetGroup is Type: String — the group's name, flat — so an SDK decoding
//     DBCluster.DBSubnetGroup read the empty string off a response that held the name one element
//     deeper. The nested form is correct on the *instance*, where API_DBInstance.DBSubnetGroup is a
//     DBSubnetGroup object, and that is how it reached the cluster. xmlDBInstanceItem keeps it.
//
// # A third element name the assertion caught
//
// Writing the test below surfaced one more: setDBInstanceStatus builds its envelope with placeholder
// element names and substitutes them afterwards, and the result element's placeholder was never
// declared — the field was untagged, so encoding/xml named the element from the field and wrote
// <Result>, which the substitution for "XMLResult" could not match. Start, Stop and
// RebootDBInstance therefore answered <Result> where API_StartDBInstance publishes
// StartDBInstanceResult, and an SDK decoding any of the three found no result element. Fixed at the
// site in rds_plugin.go; it is a response element name rather than a projection, so nothing in this
// file changes for it.
//
// # What stays on the records
//
// AccountID, Region, CreatedAt, Tags and EverTagged stay in rds_types.go, untouched. The state keys
// already scope every record by account and Region so nothing reads those two back, but a persisted
// member is not free to remove, per ecr_wire.go's rule above — and Tags and EverTagged are read off
// the stored record to answer the Resource Groups Tagging API's GetResources.
//
// One inert side-effect, recorded so it is not re-discovered as a defect: tagging edits the record
// as raw JSON through mergeRecordStringMapTags, whose taggingStampRecordEverTagged
// (tagging_ever_tagged.go) writes an `ever_tagged` member to whatever map it is handed. So tagging a
// snapshot or a parameter group adds that member to the stored JSON even though neither
// RDSDBSnapshot nor RDSDBParameterGroup declares a field for it. It is not a baseline line — the
// baseline counts Go declarations — and no item struct has the element, so it cannot reach a body.
//
// # What substrate does not model
//
// Each type below lists the published members it omits. They are absent rather than present and
// zero, per #1013: a count, a status or a timestamp reported as its zero value reads as a
// measurement, and substrate has measured nothing. Three are worth naming because a consumer is
// likely to look for them. API_DBInstance.PendingModifiedValues and API_DBCluster.PercentProgress
// both describe work in flight, and substrate's resources reach their terminal status on the
// operation that creates them, so there is nothing in flight to report.
// API_DBSubnetGroup.Subnets is published and createDBSubnetGroup never reads the request's
// SubnetIds, so it is absent for want of a value rather than by choice.

// rdsTimeOrNil returns nil for the zero time, so an unset optional timestamp is omitted from the
// body rather than rendered as the year-one instant encoding/xml would otherwise write.
//
// It returns *time.Time where glueTimeOrNil returns *EpochSeconds, because the Query protocol
// publishes a timestamp as ISO8601 and Glue's JSON protocol publishes it as epoch seconds. A pointer
// is necessary rather than decorative in both: `omitempty` has no effect on a struct type in
// encoding/xml any more than in encoding/json, so a plain field would report
// `<ClusterCreateTime>0001-01-01T00:00:00Z</ClusterCreateTime>` where AWS omits the member.
func rdsTimeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// xmlDBInstanceItem is the DBInstance of CreateDBInstance, DescribeDBInstances, ModifyDBInstance,
// DeleteDBInstance, StartDBInstance, StopDBInstance, RebootDBInstance and
// RestoreDBInstanceFromDBSnapshot.
//
// Member names follow API_DBInstance, on which every member is Required: No. DBSubnetGroup is an
// object here — API_DBSubnetGroup — unlike the cluster's, which is a plain string; see the header.
// The members substrate does not model are absent rather than present and zero, and there are many,
// API_DBInstance being the widest shape in the service: AvailabilityZone, BackupRetentionPeriod,
// CACertificateIdentifier, DBName, DBParameterGroups, DbiResourceId, DeletionProtection, Iops,
// KmsKeyId, LatestRestorableTime, LicenseModel, OptionGroupMemberships, PendingModifiedValues,
// PreferredBackupWindow, PreferredMaintenanceWindow, PubliclyAccessible, StorageEncrypted,
// StorageType, TagList and VpcSecurityGroups among them.
type xmlDBInstanceItem struct {
	DBInstanceIdentifier string      `xml:"DBInstanceIdentifier"`
	DBInstanceClass      string      `xml:"DBInstanceClass"`
	Engine               string      `xml:"Engine"`
	EngineVersion        string      `xml:"EngineVersion"`
	DBInstanceStatus     string      `xml:"DBInstanceStatus"`
	MasterUsername       string      `xml:"MasterUsername"`
	AllocatedStorage     int         `xml:"AllocatedStorage"`
	DBInstanceArn        string      `xml:"DBInstanceArn"`
	MultiAZ              bool        `xml:"MultiAZ"`
	DBSubnetGroupName    string      `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	InstanceCreateTime   *time.Time  `xml:"InstanceCreateTime,omitempty"`
	Endpoint             xmlEndpoint `xml:"Endpoint"`
}

// xmlEndpoint is the Endpoint of API_DBInstance. HostedZoneId is the third published member and
// substrate mints no hosted zone for a DB instance, so it is absent.
type xmlEndpoint struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// dbInstanceToXML projects a persisted DB instance onto the published shape.
func dbInstanceToXML(inst RDSDBInstance) xmlDBInstanceItem {
	return xmlDBInstanceItem{
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		DBInstanceClass:      inst.DBInstanceClass,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		DBInstanceStatus:     inst.DBInstanceStatus,
		MasterUsername:       inst.MasterUsername,
		AllocatedStorage:     inst.AllocatedStorage,
		DBInstanceArn:        inst.DBInstanceArn,
		MultiAZ:              inst.MultiAZ,
		DBSubnetGroupName:    inst.DBSubnetGroupName,
		InstanceCreateTime:   rdsTimeOrNil(inst.CreatedAt),
		Endpoint:             xmlEndpoint{Address: inst.Endpoint.Address, Port: inst.Endpoint.Port},
	}
}

// xmlDBClusterItem is the DBCluster of CreateDBCluster, DescribeDBClusters and DeleteDBCluster.
//
// Member names follow API_DBCluster, on which every member is Required: No. One struct serves all
// three sites, which is what makes DeleteDBCluster report the same membership as the other two; see
// the header on the truncation and on DBSubnetGroup's flat type. The members substrate does not
// model are absent rather than present and zero: AllocatedStorage, AvailabilityZones,
// BackupRetentionPeriod, DatabaseName, DBClusterMembers, DBClusterParameterGroup,
// DbClusterResourceId, DeletionProtection, EarliestRestorableTime, EngineMode, HostedZoneId,
// LatestRestorableTime, PercentProgress, PreferredBackupWindow, PreferredMaintenanceWindow,
// StorageEncrypted, TagList and VpcSecurityGroups among them.
type xmlDBClusterItem struct {
	DBClusterIdentifier string     `xml:"DBClusterIdentifier"`
	Engine              string     `xml:"Engine"`
	EngineVersion       string     `xml:"EngineVersion"`
	Status              string     `xml:"Status"`
	Endpoint            string     `xml:"Endpoint"`
	ReaderEndpoint      string     `xml:"ReaderEndpoint"`
	Port                int        `xml:"Port"`
	MasterUsername      string     `xml:"MasterUsername"`
	DBSubnetGroup       string     `xml:"DBSubnetGroup,omitempty"`
	MultiAZ             bool       `xml:"MultiAZ"`
	ClusterCreateTime   *time.Time `xml:"ClusterCreateTime,omitempty"`
	DBClusterArn        string     `xml:"DBClusterArn"`
}

// dbClusterToXML projects a persisted DB cluster onto the published shape.
func dbClusterToXML(c RDSDBCluster) xmlDBClusterItem {
	return xmlDBClusterItem{
		DBClusterIdentifier: c.DBClusterIdentifier,
		Engine:              c.Engine,
		EngineVersion:       c.EngineVersion,
		Status:              c.Status,
		Endpoint:            c.Endpoint,
		ReaderEndpoint:      c.ReaderEndpoint,
		Port:                c.Port,
		MasterUsername:      c.MasterUsername,
		DBSubnetGroup:       c.DBSubnetGroupName,
		MultiAZ:             c.MultiAZ,
		ClusterCreateTime:   rdsTimeOrNil(c.CreatedAt),
		DBClusterArn:        c.DBClusterArn,
	}
}

// xmlDBSnapshotItem is the DBSnapshot of CreateDBSnapshot, DescribeDBSnapshots and
// DeleteDBSnapshot.
//
// Member names follow API_DBSnapshot, on which every member is Required: No. SnapshotCreateTime is
// the one that "changes for the copy when the snapshot is copied"; substrate copies no snapshot, so
// OriginalSnapshotCreateTime would be the same value under a name claiming a copy had happened and
// is absent, as are InstanceCreateTime (substrate does not carry the source instance's creation time
// onto the snapshot), SnapshotDatabaseTime, SourceRegion — see the header — AvailabilityZone,
// Encrypted, EngineVersion, KmsKeyId, MasterUsername, OptionGroupName, PercentProgress, Port,
// SnapshotTarget, StorageType, TagList and VpcId.
type xmlDBSnapshotItem struct {
	DBSnapshotIdentifier string     `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string     `xml:"DBInstanceIdentifier"`
	SnapshotType         string     `xml:"SnapshotType"`
	Status               string     `xml:"Status"`
	Engine               string     `xml:"Engine"`
	AllocatedStorage     int        `xml:"AllocatedStorage"`
	SnapshotCreateTime   *time.Time `xml:"SnapshotCreateTime,omitempty"`
	DBSnapshotArn        string     `xml:"DBSnapshotArn"`
}

// dbSnapshotToXML projects a persisted DB snapshot onto the published shape.
func dbSnapshotToXML(snap RDSDBSnapshot) xmlDBSnapshotItem {
	return xmlDBSnapshotItem{
		DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
		DBInstanceIdentifier: snap.DBInstanceIdentifier,
		SnapshotType:         snap.SnapshotType,
		Status:               snap.Status,
		Engine:               snap.Engine,
		AllocatedStorage:     snap.AllocatedStorage,
		SnapshotCreateTime:   rdsTimeOrNil(snap.CreatedAt),
		DBSnapshotArn:        snap.DBSnapshotArn,
	}
}

// xmlDBSubnetGroupItem is the DBSubnetGroup of CreateDBSubnetGroup and DescribeDBSubnetGroups.
//
// Member names follow API_DBSubnetGroup, on which every member is Required: No and which publishes
// no creation-time member — so unlike the three shapes above, nothing here reports a timestamp. Of
// its seven members substrate models five; Subnets and SupportedNetworkTypes are absent because
// createDBSubnetGroup never reads the request's SubnetIds, so there is no value to report.
type xmlDBSubnetGroupItem struct {
	DBSubnetGroupName        string `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string `xml:"DBSubnetGroupDescription"`
	SubnetGroupStatus        string `xml:"SubnetGroupStatus"`
	VpcID                    string `xml:"VpcId"`
	DBSubnetGroupArn         string `xml:"DBSubnetGroupArn"`
}

// dbSubnetGroupToXML projects a persisted DB subnet group onto the published shape.
func dbSubnetGroupToXML(sg RDSDBSubnetGroup) xmlDBSubnetGroupItem {
	return xmlDBSubnetGroupItem{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		SubnetGroupStatus:        sg.SubnetGroupStatus,
		VpcID:                    sg.VpcID,
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
	}
}

// xmlDBParameterGroupItem is the DBParameterGroup of CreateDBParameterGroup and
// DescribeDBParameterGroups.
//
// It is the one shape in the service substrate models completely: these four members are
// API_DBParameterGroup's four members, so nothing is omitted and nothing is invented. Like
// API_DBSubnetGroup it publishes no creation-time member.
type xmlDBParameterGroupItem struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
	DBParameterGroupArn    string `xml:"DBParameterGroupArn"`
}

// dbParamGroupToXML projects a persisted DB parameter group onto the published shape.
func dbParamGroupToXML(pg RDSDBParameterGroup) xmlDBParameterGroupItem {
	return xmlDBParameterGroupItem{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
		DBParameterGroupArn:    pg.DBParameterGroupArn,
	}
}
