package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// RDS answers every resource through an item struct in rds_wire.go rather than by encoding the
// persisted record, and nothing asserted that (#756). These tests are that assertion: for each of
// the five records, every operation that answers it is driven and the body's element names are
// compared against the published shape's — so AccountID, Region, CreatedAt and `ever_tagged` cannot
// reach a body, and neither can anything else substrate has not deliberately published.
//
// Element *names*, not a decode into a Go struct, for the reason glue_wire_test.go gives: decoding
// into a type that has no field for an extra member is exactly the step that hides it. The XML
// equivalent of Glue's "decode to map[string]json.RawMessage and assert keys absent" is stronger
// here, because a path rather than a bare name is compared: it also catches a published member
// rendered at the wrong depth, which is how the cluster's DBSubnetGroup came to be nested.

// rdsWireClock is the instant setupRDSWirePlugin starts the simulated clock at, so every projected
// timestamp has a known value. Non-zero on purpose: rdsTimeOrNil omits the zero time, so a test
// whose clock was the zero time would see InstanceCreateTime, ClusterCreateTime and
// SnapshotCreateTime absent from every body and call that a pass.
//
// The controller advances a small deterministic step per observation rather than standing still, so
// the assertions below bound the value rather than equate it — still nothing read off the wall
// clock.
var rdsWireClock = time.Unix(1700000000, 0).UTC()

// rdsWireAccount and rdsWireRegion scope every state key the plugin writes, and are what the ARNs
// in the responses are built from.
const (
	rdsWireAccount = "123456789012"
	rdsWireRegion  = "us-east-1"
)

// The published member paths of each shape, one entry per element the body may carry, relative to
// the record's own element. A nested element appears as "parent/child" so a member rendered at the
// wrong depth fails rather than passing on its leaf name.
//
// Each list is the published shape's membership minus what substrate does not model; rds_wire.go
// records which members those are and why each is absent rather than present and zero.
var (
	// API_DBInstance. DBSubnetGroup is an object here — unlike the cluster's — so its name is one
	// element deeper, and Endpoint is API_Endpoint.
	rdsWireDBInstanceMembers = []string{
		"AllocatedStorage",
		"DBInstanceArn",
		"DBInstanceClass",
		"DBInstanceIdentifier",
		"DBInstanceStatus",
		"DBSubnetGroup",
		"DBSubnetGroup/DBSubnetGroupName",
		"Endpoint",
		"Endpoint/Address",
		"Endpoint/Port",
		"Engine",
		"EngineVersion",
		"InstanceCreateTime",
		"MasterUsername",
		"MultiAZ",
	}

	// API_DBCluster. DBSubnetGroup is Type: String, so there is deliberately no
	// "DBSubnetGroup/DBSubnetGroupName" entry; that absence is the pin on the flattening.
	rdsWireDBClusterMembers = []string{
		"ClusterCreateTime",
		"DBClusterArn",
		"DBClusterIdentifier",
		"DBSubnetGroup",
		"Endpoint",
		"Engine",
		"EngineVersion",
		"MasterUsername",
		"MultiAZ",
		"Port",
		"ReaderEndpoint",
		"Status",
	}

	// API_DBSnapshot.
	rdsWireDBSnapshotMembers = []string{
		"AllocatedStorage",
		"DBInstanceIdentifier",
		"DBSnapshotArn",
		"DBSnapshotIdentifier",
		"Engine",
		"SnapshotCreateTime",
		"SnapshotType",
		"Status",
	}

	// API_DBSubnetGroup, which publishes no creation-time member — so unlike the three above,
	// nothing here reports a timestamp.
	rdsWireDBSubnetGroupMembers = []string{
		"DBSubnetGroupArn",
		"DBSubnetGroupDescription",
		"DBSubnetGroupName",
		"SubnetGroupStatus",
		"VpcId",
	}

	// API_DBParameterGroup, in full: substrate models all four of its members and no others.
	rdsWireDBParameterGroupMembers = []string{
		"DBParameterGroupArn",
		"DBParameterGroupFamily",
		"DBParameterGroupName",
		"Description",
	}
)

// setupRDSWirePlugin returns the RDS plugin, a request context and the state manager behind it.
//
// The state manager is handed back because half of what is under test is that the record keeps the
// members the response drops — and because it is the only place the anchors below can read
// `ever_tagged` from, that member having no published home to read it back through.
//
// Its own harness rather than newRDSTestServer, which seeds the clock from time.Now(): a timestamp
// assertion needs a known instant. The plugin is called directly rather than through an httptest
// server because what is under test is the bytes of a response body, and the server adds nothing to
// those.
func setupRDSWirePlugin(t *testing.T) (*emulator.RDSPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.RDSPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(rdsWireClock)},
	}), "emulator.RDSPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: rdsWireAccount,
		Region:    rdsWireRegion,
		RequestID: "req-rds-wire",
	}, state
}

// rdsWire issues one query-protocol action and returns the raw response body, failing the test on
// anything but 200.
func rdsWire(t *testing.T, p *emulator.RDSPlugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "rds",
		Operation: action,
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// rdsWireMembers walks an RDS response body and returns every element inside wrapper, keyed by its
// path relative to wrapper and valued with the text it carries.
//
// A list response carries wrapper more than once (DescribeDBInstancesResult>DBInstances>DBInstance),
// and the paths are unioned across every occurrence — so an extra member on any item in the page
// fails, not just on the first.
func rdsWireMembers(t *testing.T, action string, body []byte, wrapper string) map[string]string {
	t.Helper()
	out := map[string]string{}
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	inside := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "%s: decode body: %s", action, body)
		switch el := tok.(type) {
		case xml.StartElement:
			if !inside {
				// Everything above the record — the response envelope and the result element — is
				// skipped, so one expectation serves every operation that answers the record.
				inside = el.Name.Local == wrapper
				continue
			}
			path = append(path, el.Name.Local)
			key := strings.Join(path, "/")
			if _, seen := out[key]; !seen {
				out[key] = ""
			}
		case xml.EndElement:
			switch {
			case !inside:
			case len(path) == 0:
				inside = false // wrapper's own end tag
			default:
				path = path[:len(path)-1]
			}
		case xml.CharData:
			if !inside || len(path) == 0 {
				continue
			}
			if text := strings.TrimSpace(string(el)); text != "" {
				out[strings.Join(path, "/")] += text
			}
		}
	}
	require.NotEmpty(t, out, "%s: body carries no %s element: %s", action, wrapper, body)
	return out
}

// rdsWireAssertMembers requires that the record's elements are exactly the published ones.
//
// Equality rather than a set of NotContains assertions: it subsumes every absence check at once,
// and it cannot go vacuous the way an absence check on a member the body could not have carried
// does.
func rdsWireAssertMembers(t *testing.T, action string, body []byte, wrapper string, want []string) map[string]string {
	t.Helper()
	members := rdsWireMembers(t, action, body, wrapper)
	assert.Equal(t, want, slices.Sorted(maps.Keys(members)),
		"%s: %s must carry exactly the published members: %s", action, wrapper, body)
	return members
}

// rdsWireTime parses a projected timestamp and requires it to be the simulated clock's, within a
// tolerance.
//
// Bounded rather than equated because TimeController advances a deterministic step per observation;
// a test demanding the seeded instant exactly fails on the second request of a run.
func rdsWireTime(t *testing.T, action string, members map[string]string, member string) {
	t.Helper()
	raw, ok := members[member]
	require.True(t, ok, "%s must report %s", action, member)
	ts, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err, "%s: %s is not an ISO8601 instant: %q", action, member, raw)
	assert.WithinDuration(t, rdsWireClock, ts, time.Second, "%s: %s", action, member)
}

// rdsWireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func rdsWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "rds", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// rdsWireKey builds the scoped state key the plugin stores a record under.
func rdsWireKey(prefix, id string) string {
	return prefix + ":" + rdsWireAccount + "/" + rdsWireRegion + "/" + id
}

// rdsWireTag tags a resource through RDS's own AddTagsToResource and proves through
// ListTagsForResource that the tag landed, then requires that the stored record carries
// `ever_tagged`.
//
// This is the presence anchor the absence assertions need. `ever_tagged` is `,omitempty`, so it is
// absent from a record that has never been tagged — and an assertion that a response omits a member
// the record could not have supplied passes without testing anything. Tagging first makes the
// member exist at the moment every later response is rendered.
func rdsWireTag(t *testing.T, p *emulator.RDSPlugin, ctx *emulator.RequestContext, state emulator.StateManager, arn, key string) {
	t.Helper()
	rdsWire(t, p, ctx, "AddTagsToResource", map[string]string{
		"ResourceName":        arn,
		"Tags.member.1.Key":   "wire",
		"Tags.member.1.Value": "anchor",
	})

	// Read back through the tag operation, never out of the state store — #765's rule.
	listed := rdsWire(t, p, ctx, "ListTagsForResource", map[string]string{"ResourceName": arn})
	assert.Contains(t, string(listed), "<Key>wire</Key>", "AddTagsToResource stored no tag for %s", arn)

	record := rdsWireRecord(t, state, key)
	require.Equal(t, "true", string(record["ever_tagged"]),
		"%s must carry ever_tagged before an absence assertion on it means anything", key)
}

func TestRDSWire_DBInstanceResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	create := map[string]string{
		"DBInstanceIdentifier": "wire-db",
		"DBInstanceClass":      "db.t3.micro",
		"Engine":               "postgres",
		"EngineVersion":        "16.3",
		"MasterUsername":       "admin",
		"AllocatedStorage":     "20",
		"MultiAZ":              "true",
		"DBSubnetGroupName":    "wire-subnets",
	}
	created := rdsWireAssertMembers(t, "CreateDBInstance",
		rdsWire(t, p, ctx, "CreateDBInstance", create), "DBInstance", rdsWireDBInstanceMembers)
	rdsWireTime(t, "CreateDBInstance", created, "InstanceCreateTime")
	assert.Equal(t, "wire-subnets", created["DBSubnetGroup/DBSubnetGroupName"],
		"API_DBInstance.DBSubnetGroup is a DBSubnetGroup object, so the name is one element deeper")

	arn := created["DBInstanceArn"]
	require.NotEmpty(t, arn, "CreateDBInstance must report DBInstanceArn")
	instanceKey := rdsWireKey("dbinstance", "wire-db")

	// Create's own body is rendered before any tag exists, so it anchors on the three members that
	// are always set: AccountID, Region and CreatedAt. InstanceCreateTime above proves CreatedAt was
	// non-zero; these two have no published home at all, which is the whole point.
	record := rdsWireRecord(t, state, instanceKey)
	require.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "CreateDBInstance must persist AccountID")
	require.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "CreateDBInstance must persist Region")

	// Every response after this one is rendered from a record carrying ever_tagged.
	rdsWireTag(t, p, ctx, state, arn, instanceKey)

	// A source snapshot for RestoreDBInstanceFromDBSnapshot, taken here so the restore below has
	// something to restore from. Its own body is asserted by the snapshot test.
	rdsWire(t, p, ctx, "CreateDBSnapshot", map[string]string{
		"DBSnapshotIdentifier": "wire-db-snap",
		"DBInstanceIdentifier": "wire-db",
	})

	// The remaining seven operations that answer a DBInstance. Start, Stop and Reboot all go through
	// setDBInstanceStatus, the one site that does not use rdsXMLResponse, so each is driven rather
	// than assumed to agree with the others.
	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"DescribeDBInstances", map[string]string{"DBInstanceIdentifier": "wire-db"}},
		{"ModifyDBInstance", map[string]string{"DBInstanceIdentifier": "wire-db", "AllocatedStorage": "40"}},
		{"StopDBInstance", map[string]string{"DBInstanceIdentifier": "wire-db"}},
		{"StartDBInstance", map[string]string{"DBInstanceIdentifier": "wire-db"}},
		{"RebootDBInstance", map[string]string{"DBInstanceIdentifier": "wire-db"}},
		{"RestoreDBInstanceFromDBSnapshot", map[string]string{
			"DBSnapshotIdentifier": "wire-db-snap",
			"DBInstanceIdentifier": "wire-db-restored",
			"DBInstanceClass":      "db.t3.micro",
			"DBSubnetGroupName":    "wire-subnets",
		}},
		// Last: it deletes the record the five above read.
		{"DeleteDBInstance", map[string]string{"DBInstanceIdentifier": "wire-db"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			members := rdsWireAssertMembers(t, tc.action,
				rdsWire(t, p, ctx, tc.action, tc.params), "DBInstance", rdsWireDBInstanceMembers)
			rdsWireTime(t, tc.action, members, "InstanceCreateTime")
		})
	}
}

func TestRDSWire_DBClusterResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	created := rdsWireAssertMembers(t, "CreateDBCluster",
		rdsWire(t, p, ctx, "CreateDBCluster", map[string]string{
			"DBClusterIdentifier": "wire-cluster",
			"Engine":              "aurora-postgresql",
			"EngineVersion":       "15.4",
			"MasterUsername":      "admin",
			"MultiAZ":             "true",
			"DBSubnetGroupName":   "wire-subnets",
		}), "DBCluster", rdsWireDBClusterMembers)
	rdsWireTime(t, "CreateDBCluster", created, "ClusterCreateTime")

	// API_DBCluster.DBSubnetGroup is Type: String — the group's name, flat. Asserting the value and
	// not merely the element is what pins the flattening: the nested form rendered an empty
	// DBSubnetGroup with the name inside it, which an SDK decoded as "".
	assert.Equal(t, "wire-subnets", created["DBSubnetGroup"],
		"API_DBCluster.DBSubnetGroup must carry the group name directly")

	arn := created["DBClusterArn"]
	require.NotEmpty(t, arn, "CreateDBCluster must report DBClusterArn")
	clusterKey := rdsWireKey("dbcluster", "wire-cluster")

	record := rdsWireRecord(t, state, clusterKey)
	require.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "CreateDBCluster must persist AccountID")
	require.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "CreateDBCluster must persist Region")

	rdsWireTag(t, p, ctx, state, arn, clusterKey)

	described := rdsWireAssertMembers(t, "DescribeDBClusters",
		rdsWire(t, p, ctx, "DescribeDBClusters", map[string]string{"DBClusterIdentifier": "wire-cluster"}),
		"DBCluster", rdsWireDBClusterMembers)
	rdsWireTime(t, "DescribeDBClusters", described, "ClusterCreateTime")

	// DeleteDBCluster reported three of the eleven members it held, because its own inline struct
	// declared only those three; API_DeleteDBCluster's response element is the full DBCluster. The
	// membership assertion covers the expansion, and these two cover the thing a truncation would
	// not have shown — that the expanded members carry the values the cluster had.
	deleted := rdsWireAssertMembers(t, "DeleteDBCluster",
		rdsWire(t, p, ctx, "DeleteDBCluster", map[string]string{"DBClusterIdentifier": "wire-cluster"}),
		"DBCluster", rdsWireDBClusterMembers)
	rdsWireTime(t, "DeleteDBCluster", deleted, "ClusterCreateTime")
	assert.Equal(t, "deleting", deleted["Status"], "DeleteDBCluster must report the cluster deleting")
	assert.Equal(t, "admin", deleted["MasterUsername"], "DeleteDBCluster must report the cluster's own members")
	assert.Equal(t, "wire-cluster.cluster-ro-xxx."+rdsWireRegion+".rds.amazonaws.com", deleted["ReaderEndpoint"],
		"DeleteDBCluster must report the cluster's own members")
}

func TestRDSWire_DBSnapshotResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	rdsWire(t, p, ctx, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "wire-db",
		"Engine":               "postgres",
		"AllocatedStorage":     "20",
	})

	created := rdsWireAssertMembers(t, "CreateDBSnapshot",
		rdsWire(t, p, ctx, "CreateDBSnapshot", map[string]string{
			"DBSnapshotIdentifier": "wire-snap",
			"DBInstanceIdentifier": "wire-db",
		}), "DBSnapshot", rdsWireDBSnapshotMembers)
	rdsWireTime(t, "CreateDBSnapshot", created, "SnapshotCreateTime")

	arn := created["DBSnapshotArn"]
	require.NotEmpty(t, arn, "CreateDBSnapshot must report DBSnapshotArn")
	snapshotKey := rdsWireKey("dbsnapshot", "wire-snap")

	record := rdsWireRecord(t, state, snapshotKey)
	require.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "CreateDBSnapshot must persist AccountID")
	require.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "CreateDBSnapshot must persist Region")

	// RDSDBSnapshot declares no EverTagged field, yet tagging it writes `ever_tagged` into the stored
	// record anyway: taggingStampRecordEverTagged edits raw JSON and adds the member to whatever map
	// it is handed. rdsWireTag requires that member, so tagging here is not decoration — it proves
	// that stamp cannot reach a snapshot body either, which is the only thing keeping it inert.
	rdsWireTag(t, p, ctx, state, arn, snapshotKey)

	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"DescribeDBSnapshots", map[string]string{"DBSnapshotIdentifier": "wire-snap"}},
		{"DeleteDBSnapshot", map[string]string{"DBSnapshotIdentifier": "wire-snap"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			members := rdsWireAssertMembers(t, tc.action,
				rdsWire(t, p, ctx, tc.action, tc.params), "DBSnapshot", rdsWireDBSnapshotMembers)
			rdsWireTime(t, tc.action, members, "SnapshotCreateTime")
		})
	}
}

func TestRDSWire_DBSubnetGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	created := rdsWireAssertMembers(t, "CreateDBSubnetGroup",
		rdsWire(t, p, ctx, "CreateDBSubnetGroup", map[string]string{
			"DBSubnetGroupName":        "wire-subnets",
			"DBSubnetGroupDescription": "subnets for the wire test",
			"VpcId":                    "vpc-0wire",
			"SubnetIds.member.1":       "subnet-1",
		}), "DBSubnetGroup", rdsWireDBSubnetGroupMembers)

	// API_DBSubnetGroup publishes Subnets and substrate reads no SubnetIds, so the member is absent
	// for want of a value rather than reported empty (#1013) — asserted here because the request
	// above does supply one.
	assert.NotContains(t, created, "Subnets",
		"CreateDBSubnetGroup models no subnet, so Subnets must be absent rather than empty")

	arn := created["DBSubnetGroupArn"]
	require.NotEmpty(t, arn, "CreateDBSubnetGroup must report DBSubnetGroupArn")
	subnetGroupKey := rdsWireKey("dbsubnetgroup", "wire-subnets")

	record := rdsWireRecord(t, state, subnetGroupKey)
	require.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "CreateDBSubnetGroup must persist AccountID")
	require.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "CreateDBSubnetGroup must persist Region")

	rdsWireTag(t, p, ctx, state, arn, subnetGroupKey)

	// DeleteDBSubnetGroup answers an empty result and so has no membership to assert.
	rdsWireAssertMembers(t, "DescribeDBSubnetGroups",
		rdsWire(t, p, ctx, "DescribeDBSubnetGroups", map[string]string{"DBSubnetGroupName": "wire-subnets"}),
		"DBSubnetGroup", rdsWireDBSubnetGroupMembers)
}

func TestRDSWire_DBParameterGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	created := rdsWireAssertMembers(t, "CreateDBParameterGroup",
		rdsWire(t, p, ctx, "CreateDBParameterGroup", map[string]string{
			"DBParameterGroupName":   "wire-params",
			"DBParameterGroupFamily": "postgres16",
			"Description":            "parameters for the wire test",
		}), "DBParameterGroup", rdsWireDBParameterGroupMembers)
	require.NotEmpty(t, created["DBParameterGroupArn"], "CreateDBParameterGroup must report DBParameterGroupArn")

	// RDSDBParameterGroup declares neither CreatedAt nor EverTagged — API_DBParameterGroup publishes
	// no creation time, and rdsResolveARN reaches no `pg:` ARN so the record cannot be tagged through
	// RDS at all. Its two baseline lines are AccountID and Region, and both are set on every record
	// the plugin writes, so the anchor is reading them back out of it.
	record := rdsWireRecord(t, state, rdsWireKey("dbparamgroup", "wire-params"))
	require.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "CreateDBParameterGroup must persist AccountID")
	require.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "CreateDBParameterGroup must persist Region")

	// DeleteDBParameterGroup answers an empty result and so has no membership to assert.
	rdsWireAssertMembers(t, "DescribeDBParameterGroups",
		rdsWire(t, p, ctx, "DescribeDBParameterGroups", map[string]string{"DBParameterGroupName": "wire-params"}),
		"DBParameterGroup", rdsWireDBParameterGroupMembers)
}

// TestRDSWire_StatusOperationsNameTheirPublishedResultElement covers the one site that builds its
// envelope by substituting placeholder element names rather than declaring them.
//
// Start, Stop and RebootDBInstance answered <Result> because the result field was untagged, so
// encoding/xml named the element from the field and the substitution had nothing to match. An SDK
// decoding any of the three found no result element at all (#756).
func TestRDSWire_StatusOperationsNameTheirPublishedResultElement(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupRDSWirePlugin(t)

	rdsWire(t, p, ctx, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "wire-db",
		"Engine":               "postgres",
	})

	for _, tc := range []struct{ action, status string }{
		{"StopDBInstance", "stopped"},
		{"StartDBInstance", "available"},
		{"RebootDBInstance", "available"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			body := string(rdsWire(t, p, ctx, tc.action, map[string]string{"DBInstanceIdentifier": "wire-db"}))
			assert.Contains(t, body, "<"+tc.action+"Response ", "%s names its response element", tc.action)
			assert.Contains(t, body, "<"+tc.action+"Result>", "%s names its result element", tc.action)
			assert.NotContains(t, body, "<Result>", "%s must not answer an unnamed result element", tc.action)
			assert.NotContains(t, body, "placeholder", "%s must substitute every placeholder", tc.action)
			assert.Contains(t, body, "<DBInstanceStatus>"+tc.status+"</DBInstanceStatus>",
				"%s must report the instance %s", tc.action, tc.status)
		})
	}
}

// TestRDSWire_ProjectionLeavesTheRecordIntact is the other half of the projection: the members the
// responses drop are still persisted.
//
// This is why the sixteen baseline lines stay in scripts/wire-bookkeeping-baseline.txt rather than
// being deleted — ecr_wire.go's rule is that the record's encoding is what MemoryStateManager
// snapshots and a replay reads back, so changing it changes the format of every recorded run. It is
// also what keeps the Resource Groups Tagging API answering: its rds arm reads Tags and EverTagged
// off these stored records.
func TestRDSWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRDSWirePlugin(t)

	instanceARN := rdsWireMembers(t, "CreateDBInstance", rdsWire(t, p, ctx, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "wire-db",
		"Engine":               "postgres",
		"Tags.member.1.Key":    "owner",
		"Tags.member.1.Value":  "platform",
	}), "DBInstance")["DBInstanceArn"]

	clusterARN := rdsWireMembers(t, "CreateDBCluster", rdsWire(t, p, ctx, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "wire-cluster",
		"Tags.member.1.Key":   "owner",
		"Tags.member.1.Value": "platform",
	}), "DBCluster")["DBClusterArn"]

	snapshotARN := rdsWireMembers(t, "CreateDBSnapshot", rdsWire(t, p, ctx, "CreateDBSnapshot", map[string]string{
		"DBSnapshotIdentifier": "wire-snap",
		"DBInstanceIdentifier": "wire-db",
		"Tags.member.1.Key":    "owner",
		"Tags.member.1.Value":  "platform",
	}), "DBSnapshot")["DBSnapshotArn"]

	subnetGroupARN := rdsWireMembers(t, "CreateDBSubnetGroup", rdsWire(t, p, ctx, "CreateDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":   "wire-subnets",
		"VpcId":               "vpc-0wire",
		"Tags.member.1.Key":   "owner",
		"Tags.member.1.Value": "platform",
	}), "DBSubnetGroup")["DBSubnetGroupArn"]

	rdsWire(t, p, ctx, "CreateDBParameterGroup", map[string]string{
		"DBParameterGroupName":   "wire-params",
		"DBParameterGroupFamily": "postgres16",
		"Tags.member.1.Key":      "owner",
		"Tags.member.1.Value":    "platform",
	})

	for _, arnKey := range []struct{ arn, key string }{
		{instanceARN, rdsWireKey("dbinstance", "wire-db")},
		{clusterARN, rdsWireKey("dbcluster", "wire-cluster")},
		{snapshotARN, rdsWireKey("dbsnapshot", "wire-snap")},
		{subnetGroupARN, rdsWireKey("dbsubnetgroup", "wire-subnets")},
	} {
		require.NotEmpty(t, arnKey.arn, "%s: the create response must report an ARN", arnKey.key)
		rdsWireTag(t, p, ctx, state, arnKey.arn, arnKey.key)
	}

	for _, tc := range []struct {
		key       string
		timestamp bool
	}{
		{rdsWireKey("dbinstance", "wire-db"), true},
		{rdsWireKey("dbcluster", "wire-cluster"), true},
		{rdsWireKey("dbsnapshot", "wire-snap"), true},
		{rdsWireKey("dbsubnetgroup", "wire-subnets"), false},
		{rdsWireKey("dbparamgroup", "wire-params"), false},
	} {
		t.Run(tc.key, func(t *testing.T) {
			record := rdsWireRecord(t, state, tc.key)
			assert.Equal(t, `"`+rdsWireAccount+`"`, string(record["AccountID"]), "AccountID must stay on the record")
			assert.Equal(t, `"`+rdsWireRegion+`"`, string(record["Region"]), "Region must stay on the record")
			assert.Contains(t, string(record["Tags"]), `"owner":"platform"`, "Tags must stay on the record")

			if !tc.timestamp {
				assert.NotContains(t, record, "CreatedAt",
					"neither API_DBSubnetGroup nor API_DBParameterGroup publishes a creation time, "+
						"so neither record carries one")
				return
			}
			var created time.Time
			require.NoError(t, json.Unmarshal(record["CreatedAt"], &created), "decode CreatedAt: %s", record["CreatedAt"])
			assert.WithinDuration(t, rdsWireClock, created, time.Second, "CreatedAt must stay on the record")
		})
	}
}

// TestRDSWire_TimeOrNilOmitsTheZeroInstant covers rdsTimeOrNil's nil arm, which no request reaches:
// every handler that stores a timestamp sets it from the simulated clock. Without the arm an unset
// optional timestamp would be reported as 0001-01-01T00:00:00Z, a value AWS omits the member for.
func TestRDSWire_TimeOrNilOmitsTheZeroInstant(t *testing.T) {
	t.Parallel()
	assert.Nil(t, emulator.RDSTimeOrNilForTest(time.Time{}), "the zero instant must be omitted")

	got := emulator.RDSTimeOrNilForTest(rdsWireClock)
	require.NotNil(t, got, "a set instant must be reported")
	assert.Equal(t, rdsWireClock, *got, "a set instant must be reported unchanged")
}
