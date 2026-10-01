package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// glueWireClock is the instant setupGlueWirePlugin starts the simulated clock at, so every
// projected timestamp has a known value. Non-zero on purpose: glueTimeOrNil omits the zero time,
// and a test whose clock was the zero time would see every timestamp member absent and call that a
// pass.
//
// The controller advances a small deterministic step per observation rather than standing still, so
// the assertions below bound the value rather than equate it — a second's tolerance over a run that
// makes a few dozen observations, and still nothing read off the wall clock.
var glueWireClock = time.Unix(1700000000, 0).UTC()

// setupGlueWirePlugin returns the Glue plugin, a request context and the state manager behind it.
//
// The state manager is handed back so TestGlueWire_ProjectionLeavesTheRecordIntact can read the
// records the responses are projected from. The other Glue tests drive the plugin through an
// httptest server (newGlueTestServer); this one calls HandleRequest directly, because what is under
// test is the bytes of a response body and the server adds nothing to those.
func setupGlueWirePlugin(t *testing.T) (*emulator.GluePlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.GluePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(glueWireClock)},
	}), "emulator.GluePlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-glue-wire",
	}, state
}

// glueWire issues op and returns the raw response body, failing the test on anything but 200.
// Raw bytes rather than a decoded struct on purpose: what is under test is which members the body
// has, and a decode into a Go type is exactly the step that hides an extra one.
func glueWire(t *testing.T, p *emulator.GluePlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s body", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "glue",
		Operation: op,
		Body:      raw,
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// glueObject returns the record object a Get* response carries under member, as raw JSON.
func glueObject(t *testing.T, op string, body []byte, member string) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &out), "%s: decode response: %s", op, body)
	raw, ok := out[member]
	require.True(t, ok, "%s must answer a %s member: %s", op, member, body)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields), "%s: decode %s: %s", op, member, body)
	return fields
}

// glueOnlyListed returns the single record object a Get*s response carries under member, as raw
// JSON, requiring that the list holds exactly one.
func glueOnlyListed(t *testing.T, op string, body []byte, member string) map[string]json.RawMessage {
	t.Helper()
	var out map[string][]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &out), "%s: decode response: %s", op, body)
	list, ok := out[member]
	require.True(t, ok, "%s must answer a %s member: %s", op, member, body)
	require.Len(t, list, 1, "%s must answer one record: %s", op, body)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(list[0], &fields), "%s: decode %s[0]: %s", op, member, body)
	return fields
}

// glueNoBookkeeping asserts that a projected record object carries none of substrate's own members.
//
// AccountID, Region and CreatedAt carry no omitempty on any Glue record, so an unprojected record
// reports all three unconditionally and those assertions are non-vacuous wherever they run.
// `ever_tagged` is `,omitempty`, so it is absent whenever the flag is false whatever the projection
// does — every caller below therefore tags the resource first and anchors on a GetTags response
// proving the tag landed. TestGlueWire_ProjectionLeavesTheRecordIntact is the other half of that
// anchor: it proves the same call sequence leaves ever_tagged true in the record.
func glueNoBookkeeping(t *testing.T, op string, fields map[string]json.RawMessage) {
	t.Helper()
	for _, member := range []string{"AccountID", "Region", "CreatedAt", "ever_tagged"} {
		_, present := fields[member]
		assert.False(t, present, "%s: response carries substrate's internal %q member", op, member)
	}
}

// glueNoInventedMembers asserts that a projected record object carries neither Arn nor Tags.
//
// No Glue shape publishes either — not API_Database, API_Table, API_Connection, API_Crawler,
// API_Job or API_JobRun. Tags are observable through GetTags alone, which is what every caller
// below anchors on, so the assertion does not claim the tag went missing, only that it is not
// reported where AWS reports nothing.
func glueNoInventedMembers(t *testing.T, op string, fields map[string]json.RawMessage) {
	t.Helper()
	for _, member := range []string{"Arn", "Tags"} {
		_, present := fields[member]
		assert.False(t, present, "%s: no Glue shape publishes a %q member", op, member)
	}
}

// glueEpochMember asserts that the published timestamp member is present, is a JSON number, and
// holds the frozen clock's epoch seconds.
//
// This is the non-vacuous half of the rename. `CreatedAt` being absent proves nothing on its own —
// a projection that dropped the timestamp entirely would pass that — so the published name has to
// be shown carrying the value instead. The number check is the #1305 half: Glue's JSON protocol
// publishes a Timestamp as epoch seconds, and API_GetDatabase's Response Syntax says
// `"CreateTime": number`, where a time.Time would have rendered an RFC3339 string.
func glueEpochMember(t *testing.T, op string, fields map[string]json.RawMessage, member string) {
	t.Helper()
	raw, ok := fields[member]
	require.True(t, ok, "%s must publish %s: %v", op, member, fields)

	var seconds float64
	require.NoError(t, json.Unmarshal(raw, &seconds),
		"%s: %s must be a JSON number — Glue publishes a Timestamp as epoch seconds, got %s",
		op, member, raw)
	assert.InDelta(t, float64(glueWireClock.Unix()), seconds, 1,
		"%s: %s must report the simulated clock", op, member)
}

// glueTagAndAnchor tags arn through Glue's own TagResource and asserts through GetTags that the tag
// landed, returning nothing but a guarantee.
//
// The tag is what makes every `ever_tagged` assertion non-vacuous: mergeGlueTags stamps the flag
// through taggingStampRecordAnyEverTagged (#938), so a record that has been tagged has the flag set
// and an unprojected response would report it. Without this the absence assertion passes on a body
// that could not have carried the member — the vacuous assertion #1304 shipped on EFS before it was
// caught, and the reason scripts/wire-bookkeeping-projected.txt's header says what its third column
// has to prove.
func glueTagAndAnchor(t *testing.T, p *emulator.GluePlugin, ctx *emulator.RequestContext, arn string) {
	t.Helper()
	glueWire(t, p, ctx, "TagResource", map[string]any{
		"ResourceArn": arn,
		"TagsToAdd":   map[string]string{"owner": "wire-test"},
	})
	listed := glueWire(t, p, ctx, "GetTags", map[string]any{"ResourceArn": arn})
	assert.Contains(t, string(listed), `"owner"`,
		"the anchor: %s must report the tag, or ever_tagged is false and every assertion below is vacuous: %s",
		arn, listed)
}

// TestGlueWire_DatabaseResponsesCarryNoBookkeepingMember covers the two sites that answer a
// GlueDatabase: GetDatabase and GetDatabases.
//
// It also pins CatalogId, which is where the leaked AccountID legitimately goes: API_Database
// publishes CatalogId, "the ID of the Data Catalog in which the database resides", which is the
// account. So the account is still observable here — under the name AWS publishes it under, rather
// than under substrate's.
func TestGlueWire_DatabaseResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "wire-db", "Description": "projected"},
	})
	glueTagAndAnchor(t, p, ctx, "arn:aws:glue:us-east-1:123456789012:database/wire-db")

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetDatabase", map[string]any{"Name": "wire-db"}, "Database", false},
		{"GetDatabases", map[string]any{}, "DatabaseList", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "CreateTime")

			assert.JSONEq(t, `"wire-db"`, string(fields["Name"]),
				"%s must answer the database, or every absence assertion above is vacuous", tc.op)
			assert.JSONEq(t, `"123456789012"`, string(fields["CatalogId"]),
				"%s: API_Database publishes CatalogId, which is the account", tc.op)
		})
	}
}

// TestGlueWire_TableResponsesCarryNoBookkeepingMember covers the two sites that answer a GlueTable:
// GetTable and GetTables.
//
// GlueTable declares no Tags and no EverTagged — Glue tables are reachable through resolveGlueARN
// but the record has no field for either — so this is the one taggable-ARN record whose
// `ever_tagged` assertion would be vacuous, and no tag is written here. Its three baseline lines
// are AccountID, Region and CreatedAt, and all three are non-vacuous without one.
func TestGlueWire_TableResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "wire-tbl-db"},
	})
	glueWire(t, p, ctx, "CreateTable", map[string]any{
		"DatabaseName": "wire-tbl-db",
		"TableInput": map[string]any{
			"Name":      "wire-tbl",
			"TableType": "EXTERNAL_TABLE",
		},
	})

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetTable", map[string]any{"DatabaseName": "wire-tbl-db", "Name": "wire-tbl"}, "Table", false},
		{"GetTables", map[string]any{"DatabaseName": "wire-tbl-db"}, "TableList", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "CreateTime")

			assert.JSONEq(t, `"wire-tbl"`, string(fields["Name"]),
				"%s must answer the table, or every absence assertion above is vacuous", tc.op)
			assert.JSONEq(t, `"123456789012"`, string(fields["CatalogId"]),
				"%s: API_Table publishes CatalogId, which is the account", tc.op)
		})
	}
}

// TestGlueWire_ConnectionResponsesCarryNoBookkeepingMember covers the two sites that answer a
// GlueConnection: GetConnection and GetConnections.
//
// API_Connection publishes no CatalogId — the page was read — so unlike the database and the table
// this shape reports no account at all, and the assertion below is that it does not.
func TestGlueWire_ConnectionResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "wire-conn",
			"ConnectionType": "JDBC",
		},
	})
	glueTagAndAnchor(t, p, ctx, "arn:aws:glue:us-east-1:123456789012:connection/wire-conn")

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetConnection", map[string]any{"Name": "wire-conn"}, "Connection", false},
		{"GetConnections", map[string]any{}, "ConnectionList", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "CreationTime")

			assert.JSONEq(t, `"wire-conn"`, string(fields["Name"]),
				"%s must answer the connection, or every absence assertion above is vacuous", tc.op)
			_, hasCatalog := fields["CatalogId"]
			assert.False(t, hasCatalog, "%s: API_Connection publishes no CatalogId", tc.op)
		})
	}
}

// TestGlueWire_CrawlerResponsesCarryNoBookkeepingMember covers the two sites that answer a
// GlueCrawler: GetCrawler and GetCrawlers.
func TestGlueWire_CrawlerResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "wire-crawl-db"},
	})
	glueWire(t, p, ctx, "CreateCrawler", map[string]any{
		"Name":         "wire-crawler",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
		"DatabaseName": "wire-crawl-db",
	})
	glueTagAndAnchor(t, p, ctx, "arn:aws:glue:us-east-1:123456789012:crawler/wire-crawler")

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetCrawler", map[string]any{"Name": "wire-crawler"}, "Crawler", false},
		{"GetCrawlers", map[string]any{}, "Crawlers", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "CreationTime")

			assert.JSONEq(t, `"wire-crawler"`, string(fields["Name"]),
				"%s must answer the crawler, or every absence assertion above is vacuous", tc.op)
		})
	}
}

// TestGlueWire_JobResponsesCarryNoBookkeepingMember covers the two sites that answer a GlueJob:
// GetJob and GetJobs.
//
// API_Job spells the creation timestamp CreatedOn, a third published name for the one field
// substrate calls CreatedAt.
func TestGlueWire_JobResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateJob", map[string]any{
		"Name": "wire-job",
		"Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{
			"Name":           "glueetl",
			"ScriptLocation": "s3://wire/script.py",
		},
	})
	glueTagAndAnchor(t, p, ctx, "arn:aws:glue:us-east-1:123456789012:job/wire-job")

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetJob", map[string]any{"JobName": "wire-job"}, "Job", false},
		{"GetJobs", map[string]any{}, "Jobs", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "CreatedOn")

			assert.JSONEq(t, `"wire-job"`, string(fields["Name"]),
				"%s must answer the job, or every absence assertion above is vacuous", tc.op)
			assert.Contains(t, string(fields["Command"]), "glueetl",
				"%s must answer the job's Command, a published member", tc.op)
		})
	}
}

// TestGlueWire_JobRunResponsesCarryNoBookkeepingMember covers the two sites that answer a
// GlueJobRun: GetJobRun and GetJobRuns.
//
// This is the one record whose timestamps substrate already named as API_JobRun publishes them, so
// only the format and the two bookkeeping members change here: StartedOn and CompletedOn must be
// JSON numbers rather than the RFC3339 strings a time.Time rendered. GlueJobRun declares no
// EverTagged and job runs are not a taggable ARN type, so there is no tag to anchor on — its two
// baseline lines are AccountID and Region, both of which lacked omitempty and so were reported
// unconditionally.
func TestGlueWire_JobRunResponsesCarryNoBookkeepingMember(t *testing.T) {
	p, ctx, _ := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateJob", map[string]any{
		"Name":    "wire-run-job",
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{"Name": "glueetl"},
	})
	started := glueWire(t, p, ctx, "StartJobRun", map[string]any{"JobName": "wire-run-job"})
	var startOut struct {
		JobRunID string `json:"JobRunId"`
	}
	require.NoError(t, json.Unmarshal(started, &startOut), "decode StartJobRun: %s", started)
	require.NotEmpty(t, startOut.JobRunID, "StartJobRun must answer a run id: %s", started)

	for _, tc := range []struct {
		op     string
		body   map[string]any
		member string
		list   bool
	}{
		{"GetJobRun", map[string]any{"JobName": "wire-run-job", "RunId": startOut.JobRunID}, "JobRun", false},
		{"GetJobRuns", map[string]any{"JobName": "wire-run-job"}, "JobRuns", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := glueWire(t, p, ctx, tc.op, tc.body)
			var fields map[string]json.RawMessage
			if tc.list {
				fields = glueOnlyListed(t, tc.op, body, tc.member)
			} else {
				fields = glueObject(t, tc.op, body, tc.member)
			}

			glueNoBookkeeping(t, tc.op, fields)
			glueNoInventedMembers(t, tc.op, fields)
			glueEpochMember(t, tc.op, fields, "StartedOn")
			glueEpochMember(t, tc.op, fields, "CompletedOn")

			assert.JSONEq(t, `"`+startOut.JobRunID+`"`, string(fields["Id"]),
				"%s must answer the run, or every absence assertion above is vacuous", tc.op)
			assert.JSONEq(t, `"SUCCEEDED"`, string(fields["JobRunState"]),
				"%s: a substrate job run is SUCCEEDED from the moment it is started", tc.op)
		})
	}
}

// TestGlueWire_UnsetTimestampIsOmittedRatherThanNull covers the one arm of glueTimeOrNil no request
// reaches, because every Glue handler sets the record's timestamp from the simulated clock.
//
// It is what the pointer buys. `,omitempty` has no effect on a struct type and a zero EpochSeconds
// marshals as JSON null, so a bare field would report `"CreateTime":null` for a record whose time is
// zero — a member every one of the six shapes marks Required: No and AWS therefore omits. The arm is
// reachable in practice from a state encoding this substrate did not write: a snapshot from an older
// version, or one a replayed event log restored that predates the field.
func TestGlueWire_UnsetTimestampIsOmittedRatherThanNull(t *testing.T) {
	assert.Nil(t, emulator.GlueTimeOrNilForTest(time.Time{}),
		"the zero time must project to nil, so the member is omitted rather than reported null")

	set := emulator.GlueTimeOrNilForTest(glueWireClock)
	require.NotNil(t, set, "a set time must project to a value, or the nil case above proves nothing")

	marshaled, err := json.Marshal(struct {
		CreateTime *emulator.EpochSeconds `json:"CreateTime,omitempty"`
	}{CreateTime: set})
	require.NoError(t, err)
	assert.JSONEq(t, `{"CreateTime":1700000000}`, string(marshaled),
		"a set time renders as the epoch-seconds number Glue's JSON protocol publishes")
}

// TestGlueWire_ProjectionLeavesTheRecordIntact is the other half of the claim the six tests above
// make. The projection changes the response, not the state encoding a recorded run replays from, so
// every bookkeeping field must still be persisted on every record — which is why the twenty-one
// baseline lines stay and scripts/wire-bookkeeping-projected.txt is what discharges them.
//
// Arn, Tags and EverTagged are the three with live readers across a service boundary:
// TaggingPlugin.scanGlueDatabases reads all three off the stored GlueDatabase to answer
// GetResources, so a projection that had quietly dropped any of them from the record would take a
// cross-service answer with it. This test is also what makes the `ever_tagged` absence assertions
// above non-vacuous, by proving the same TagResource call leaves the flag true in the record.
func TestGlueWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	p, ctx, state := setupGlueWirePlugin(t)

	glueWire(t, p, ctx, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "rec-db"},
	})
	glueWire(t, p, ctx, "CreateTable", map[string]any{
		"DatabaseName": "rec-db",
		"TableInput":   map[string]any{"Name": "rec-tbl"},
	})
	glueWire(t, p, ctx, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{"Name": "rec-conn", "ConnectionType": "JDBC"},
	})
	glueWire(t, p, ctx, "CreateCrawler", map[string]any{
		"Name":         "rec-crawler",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
		"DatabaseName": "rec-db",
	})
	glueWire(t, p, ctx, "CreateJob", map[string]any{
		"Name":    "rec-job",
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{"Name": "glueetl"},
	})
	glueWire(t, p, ctx, "StartJobRun", map[string]any{"JobName": "rec-job"})

	const base = "arn:aws:glue:us-east-1:123456789012:"
	for _, arn := range []string{
		base + "database/rec-db",
		base + "connection/rec-conn",
		base + "crawler/rec-crawler",
		base + "job/rec-job",
	} {
		glueTagAndAnchor(t, p, ctx, arn)
	}

	for _, tc := range []struct {
		prefix string
		// taggable records declare Tags and EverTagged; GlueTable and GlueJobRun declare neither.
		taggable bool
		// GlueJobRun is the one record with no Arn and no CreatedAt.
		hasArn       bool
		hasCreatedAt bool
	}{
		{"database:", true, true, true},
		{"table:", false, true, true},
		{"connection:", true, true, true},
		{"crawler:", true, true, true},
		{"job:", true, true, true},
		{"jobrun:", false, false, false},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			keys, err := state.List(t.Context(), "glue", tc.prefix)
			require.NoError(t, err)
			require.Len(t, keys, 1, "one %s record was created", tc.prefix)

			raw, err := state.Get(t.Context(), "glue", keys[0])
			require.NoError(t, err)
			var record struct {
				AccountID  string            `json:"AccountID"`
				Region     string            `json:"Region"`
				Arn        string            `json:"Arn"`
				CreatedAt  time.Time         `json:"CreatedAt"`
				Tags       map[string]string `json:"Tags"`
				EverTagged bool              `json:"ever_tagged"`
			}
			require.NoError(t, json.Unmarshal(raw, &record), "decode %s: %s", keys[0], raw)

			assert.Equal(t, "123456789012", record.AccountID,
				"the record still scopes itself to an account")
			assert.Equal(t, "us-east-1", record.Region, "the record still scopes itself to a Region")
			if tc.hasCreatedAt {
				assert.WithinDuration(t, glueWireClock, record.CreatedAt, time.Second,
					"the record still stores the creation time the projection renames on the way out")
			}
			if tc.hasArn {
				assert.NotEmpty(t, record.Arn, "scanGlueDatabases reads the ARN off the record")
			}
			if tc.taggable {
				assert.Equal(t, map[string]string{"owner": "wire-test"}, record.Tags,
					"scanGlueDatabases reads the tags off the record")
				assert.True(t, record.EverTagged,
					"TagResource stamped the flag the tagging plugin reads")
			}
		})
	}
}
