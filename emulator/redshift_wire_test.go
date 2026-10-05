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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for Redshift's four
// records (#756).
//
// Redshift renders every one of its ten operations through a projection struct that holds
// `xml` tags — redshiftClusterXML, redshiftParamGroupData, redshiftSubnetGroupData and
// redshiftSnapshotData — while emulator/redshift_types.go carries no `xml` tag at all. So the
// projection already exists in code and the eight baseline lines are AccountID and Region on
// the four persisted records; what was missing was this file, the citation the projected
// inventory requires before it will accept a record.
//
// # Why this asserts absence rather than exact membership
//
// emulator/rds_wire_test.go, the strongest form in the suite, walks into the element that
// holds a record and requires its members to be *exactly* the published ones. That form is
// not available here, because Redshift's envelope is wrong in two ways that #1208 owns and
// this file must not pin: there is no `<XxxResponse>` root and no `ResponseMetadata`, and a
// cluster is wrapped in a spurious `<member>` while the four list elements flatten to
// `<member>` where the reference names each for its member type (`<Clusters><Cluster>`). An
// expectation keyed on either shape would have to be rewritten by the PR that fixes it, and
// a membership list is only honest about a document whose nesting is honest.
//
// What holds regardless of the envelope is that no element anywhere in a Redshift response
// names a bookkeeping member. That is the claim the inventory needs, it survives #1208, and
// it is the form emulator/configservice_wire_test.go uses for the same reason: assert over
// the whole document rather than relative to a wrapper whose name is in dispute.

// redshiftWireClock is the instant every fixture below starts the simulated clock at.
//
// A seeded baseline rather than setupRedshiftPlugin's time.Now(), so nothing here reads the
// wall clock. No assertion below equates a timestamp: TimeController.Now advances from its
// baseline by the wall time elapsed since it was set, so the only wall-clock-free thing to say
// about a projected timestamp is that it is present — which the membership walk covers anyway.
var redshiftWireClock = time.Unix(1700000000, 0).UTC()

// redshiftWireAccount and redshiftWireRegion scope every state key the plugin writes, and are
// what the cluster ARN and the endpoint address are built from.
const (
	redshiftWireAccount = "123456789012"
	redshiftWireRegion  = "us-east-1"
)

// redshiftBookkeepingMembers are the members every Redshift record declares and no Redshift
// shape publishes, each listed once in the spelling an element would take.
//
// Compared case-insensitively by redshiftWireAssertNoBookkeepingMember, because a leak could
// arrive under either spelling the record already carries: encoding/xml would name an element
// from the Go field (`AccountID`, `Region`) since redshift_types.go declares no `xml` tag,
// while the `json` tags the persisted document uses are `accountID` and `region`. Neither
// member carries `,omitempty`, so unlike the ever_tagged class these cannot be absent for
// free — every record the plugin writes has both populated, which is what the record
// assertion in each test below reads back and what makes an absence assertion worth making.
//
// No published member of the four shapes folds to either name. Snapshot publishes
// SourceRegion and Cluster publishes AvailabilityZone, and a fold is an equality rather than
// a substring test, so neither collides.
var redshiftBookkeepingMembers = []string{"AccountID", "Region"}

// setupRedshiftWirePlugin returns the Redshift plugin, a request context and the state
// manager behind it.
//
// The state manager is handed back because half of what each test asserts is that the record
// keeps the two members its response drops — and a record is the only place either can be
// read from, neither having a published home to read it back through.
//
// Its own harness rather than setupRedshiftPlugin, which seeds the clock from time.Now(): a
// timestamp assertion needs a known instant.
func setupRedshiftWirePlugin(t *testing.T) (*emulator.RedshiftPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.RedshiftPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(redshiftWireClock)},
	}), "emulator.RedshiftPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: redshiftWireAccount,
		Region:    redshiftWireRegion,
		RequestID: "req-redshift-wire",
	}, state
}

// redshiftWire issues one query-protocol action and returns the raw response body, failing the
// test on anything but 200.
//
// The raw bytes are the point of this file. Every other Redshift test decodes into a Go
// struct, which is exactly the step that hides a member the struct does not declare.
func redshiftWire(t *testing.T, p *emulator.RedshiftPlugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "redshift",
		Operation: action,
		Path:      "/",
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// redshiftWireAssertNoBookkeepingMember fails if any element in the response document is named
// for a bookkeeping member, at any depth, reporting the path so a failure names the element
// rather than the service.
//
// The whole document rather than a record's subtree, for the reason this file's comment gives:
// the element that holds a record is named wrongly today and the assertion must outlive the
// fix. Walking everything is also strictly stronger — a member rendered on the envelope rather
// than on the record would fail here and pass a subtree walk.
func redshiftWireAssertNoBookkeepingMember(t *testing.T, action string, body []byte) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "%s: decode body: %s", action, body)
		switch el := tok.(type) {
		case xml.StartElement:
			path = append(path, el.Name.Local)
			for _, member := range redshiftBookkeepingMembers {
				assert.False(t, strings.EqualFold(el.Name.Local, member),
					"%s answered %s: %s is substrate's bookkeeping and no Redshift shape publishes it",
					action, strings.Join(path, "/"), member)
			}
		case xml.EndElement:
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
		}
	}
}

// redshiftWireRecord returns the record at key as raw JSON, so a member with no published home
// can be read without a Go type deciding which members exist.
func redshiftWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "redshift", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// redshiftWireRequireScoped requires that the stored record carries both bookkeeping members.
//
// This is the presence anchor the absence assertions need on the record side: an assertion
// that a response omits a member its record never held would pass without testing anything.
// The members are read under the `json` tags the record is persisted with, which are the
// spellings a response rendered from the record would have carried.
func redshiftWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	record := redshiftWireRecord(t, state, key)
	require.Equal(t, `"`+redshiftWireAccount+`"`, string(record["accountID"]),
		"%s must persist accountID before an absence assertion on it means anything", key)
	require.Equal(t, `"`+redshiftWireRegion+`"`, string(record["region"]),
		"%s must persist region before an absence assertion on it means anything", key)
}

// redshiftWireKey builds the scoped state key the plugin stores a record under.
func redshiftWireKey(prefix, id string) string {
	return prefix + ":" + redshiftWireAccount + "/" + redshiftWireRegion + "/" + id
}

func TestRedshiftWire_ClusterResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRedshiftWirePlugin(t)

	created := redshiftWire(t, p, ctx, "CreateCluster", map[string]string{
		"ClusterIdentifier": "wire-cluster",
		"NodeType":          "ra3.xlplus",
		"MasterUsername":    "admin",
		"DBName":            "wiredb",
		"NumberOfNodes":     "2",
	})
	redshiftWireRequireScoped(t, state, redshiftWireKey("cluster", "wire-cluster"))

	// The endpoint address is built from the account and the Region, so both values are in every
	// body below as substrings. That is why the assertion is on the element name: the values are
	// published, inside the endpoint address, and only the names are not. (ClusterNamespaceArn
	// used to carry them too, as a cluster ARN; it is omitted since #1199.)
	require.Contains(t, string(created), "wire-cluster."+redshiftWireAccount+"."+redshiftWireRegion+".redshift.amazonaws.com",
		"CreateCluster must report the endpoint address, so the absence assertion is about the member name")

	for _, tc := range []struct {
		action string
		params map[string]string
		anchor string
	}{
		{"CreateCluster", nil, "<NodeType>ra3.xlplus</NodeType>"},
		{"DescribeClusters", map[string]string{"ClusterIdentifier": "wire-cluster"}, "<MasterUsername>admin</MasterUsername>"},
		{"ModifyCluster", map[string]string{"ClusterIdentifier": "wire-cluster", "NumberOfNodes": "4"}, "<NumberOfNodes>4</NumberOfNodes>"},
		// Last: it deletes the record the two above read.
		{"DeleteCluster", map[string]string{"ClusterIdentifier": "wire-cluster", "SkipFinalClusterSnapshot": "true"}, "<DBName>wiredb</DBName>"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			body := created
			if tc.params != nil {
				body = redshiftWire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render the cluster for an absence to mean anything", tc.action)
			redshiftWireAssertNoBookkeepingMember(t, tc.action, body)
		})
	}
}

func TestRedshiftWire_ClusterParameterGroupResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRedshiftWirePlugin(t)

	created := redshiftWire(t, p, ctx, "CreateClusterParameterGroup", map[string]string{
		"ParameterGroupName":   "wire-params",
		"ParameterGroupFamily": "redshift-1.0",
		"Description":          "parameters for the wire test",
	})
	redshiftWireRequireScoped(t, state, redshiftWireKey("paramgroup", "wire-params"))

	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"CreateClusterParameterGroup", nil},
		{"DescribeClusterParameterGroups", map[string]string{}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			body := created
			if tc.params != nil {
				body = redshiftWire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), "<ParameterGroupFamily>redshift-1.0</ParameterGroupFamily>",
				"presence anchor: %s has to render the parameter group for an absence to mean anything", tc.action)
			redshiftWireAssertNoBookkeepingMember(t, tc.action, body)
		})
	}
}

func TestRedshiftWire_ClusterSubnetGroupResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRedshiftWirePlugin(t)

	// The three members API_CreateClusterSubnetGroup marks Required. VpcId is not one of them; it is a
	// response member, and #1197 stopped reading it off the request.
	created := redshiftWire(t, p, ctx, "CreateClusterSubnetGroup", map[string]string{
		"ClusterSubnetGroupName":       "wire-subnets",
		"Description":                  "subnets for the wire test",
		"SubnetIds.SubnetIdentifier.1": "subnet-0wire",
	})
	redshiftWireRequireScoped(t, state, redshiftWireKey("subnetgroup", "wire-subnets"))

	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"CreateClusterSubnetGroup", nil},
		{"DescribeClusterSubnetGroups", map[string]string{}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			body := created
			if tc.params != nil {
				body = redshiftWire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), "<ClusterSubnetGroupName>wire-subnets</ClusterSubnetGroupName>",
				"presence anchor: %s has to render the subnet group for an absence to mean anything", tc.action)
			redshiftWireAssertNoBookkeepingMember(t, tc.action, body)
		})
	}
}

func TestRedshiftWire_SnapshotResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupRedshiftWirePlugin(t)

	redshiftWire(t, p, ctx, "CreateCluster", map[string]string{
		"ClusterIdentifier": "wire-cluster", "NodeType": "ra3.xlplus", "MasterUsername": "admin",
	})

	created := redshiftWire(t, p, ctx, "CreateClusterSnapshot", map[string]string{
		"ClusterIdentifier":  "wire-cluster",
		"SnapshotIdentifier": "wire-snap",
	})
	redshiftWireRequireScoped(t, state, redshiftWireKey("snapshot", "wire-snap"))

	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"CreateClusterSnapshot", nil},
		{"DescribeClusterSnapshots", map[string]string{}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			body := created
			if tc.params != nil {
				body = redshiftWire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), "<SnapshotIdentifier>wire-snap</SnapshotIdentifier>",
				"presence anchor: %s has to render the snapshot for an absence to mean anything", tc.action)
			redshiftWireAssertNoBookkeepingMember(t, tc.action, body)
		})
	}
}
