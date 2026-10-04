package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The member walks the raw-bytes assertions in scripts/wire-bookkeeping-projected.txt are made with
// (#756), shared by the services whose wire tests were written after the first several.
//
// Each earlier wire test carries its own copy (emulator/backup_wire_test.go,
// emulator/codedeploy_wire_test.go, emulator/redshift_wire_test.go, emulator/ec2_wire_test.go and
// others); these are the same two walks, parameterized by the member list, so a new service names
// its members rather than copying a walker. Both compare a member name case-insensitively and as an
// equality, never as a substring: the account and the Region a record
// carries also appear inside published values such as ARNs, and a substring test would collide with
// them.

// wireAssertNoMemberJSON fails if any member of the JSON document body is named for one of members,
// at any depth, reporting the `$.a.b[0].c` path so a failure names the member rather than the
// service.
//
// The whole document rather than a record's subtree, which is strictly stronger: a member rendered on
// the envelope rather than on the record would fail here and pass a subtree walk.
func wireAssertNoMemberJSON(t *testing.T, site string, body []byte, members []string) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", site, body)
	wireWalkJSON(t, site, "$", doc, members)
}

// wireWalkJSON recurses through a decoded JSON document asserting on every member name it meets.
func wireWalkJSON(t *testing.T, site, path string, node any, members []string) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(key, member),
					"%s answered %s: %s is substrate's bookkeeping and no published shape carries it",
					site, child, member)
			}
			wireWalkJSON(t, site, child, value, members)
		}
	case []any:
		for i, value := range typed {
			wireWalkJSON(t, site, fmt.Sprintf("%s[%d]", path, i), value, members)
		}
	}
}

// wireAssertNoMemberXML fails if any element of the XML document body is named for one of members,
// at any depth, reporting the `/a/b/c` path.
//
// When keyText is set it also fails on the character data of an element named keyText. That is for
// the query services that render a string map as `<entry><key>…</key><value>…</value></entry>`: there
// a leaked member arrives as the *text* of a `<key>`, not as an element name, and an element walk
// alone would pass it.
func wireAssertNoMemberXML(t *testing.T, site string, body []byte, members []string, keyText string) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoErrorf(t, err, "%s: decode body: %s", site, body)
		switch el := tok.(type) {
		case xml.StartElement:
			path = append(path, el.Name.Local)
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(el.Name.Local, member),
					"%s answered %s: %s is substrate's bookkeeping and no published shape carries it",
					site, strings.Join(path, "/"), member)
			}
		case xml.CharData:
			if keyText == "" || len(path) == 0 || path[len(path)-1] != keyText {
				continue
			}
			text := strings.TrimSpace(string(el))
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(text, member),
					"%s answered %s=%q: %s is substrate's bookkeeping and no published shape carries it",
					site, strings.Join(path, "/"), text, member)
			}
		case xml.EndElement:
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
		}
	}
}

// wireSetup initializes p over a fresh state store with a clock frozen at a fixed instant, and returns
// a request context and the store. The store is handed back because a record is the only place its
// bookkeeping members can be read from.
//
// The clock is frozen, Freeze then SetTime as TimeController.Freeze documents, so a test may assert a
// rendered date exactly. Unfrozen, the clock advances by the wall time elapsed since it was set, and
// an exact assertion then fails under load: TestBatchWire's createdAt did, in a parallel `make test`.
func wireSetup(t *testing.T, p emulator.Plugin, requestID string) (*emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	return &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: requestID,
		IDs:       emulator.NewIDMint(requestID),
	}, state
}

// wireJSONTarget issues one awsJson operation through its X-Amz-Target and returns the raw body,
// failing on anything but a 2xx.
func wireJSONTarget(t *testing.T, p emulator.Plugin, ctx *emulator.RequestContext, service, target, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   service,
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": target + "." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s answered %d: %s", op, resp.StatusCode, resp.Body)
	return resp.Body
}

// wireREST issues one REST request and returns the raw body, failing on anything but a 2xx.
func wireREST(t *testing.T, p emulator.Plugin, ctx *emulator.RequestContext, service, method, path string, body map[string]any) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err, "marshal %s %s", method, path)
	}
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:    service,
		HTTPMethod: method,
		Path:       path,
		Body:       raw,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Params:     map[string]string{},
	})
	require.NoError(t, err, "%s %s", method, path)
	require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s %s answered %d: %s", method, path, resp.StatusCode, resp.Body)
	return resp.Body
}

// wireRequireHeld requires that the record at key in namespace carries each named member with a
// non-empty value — the presence anchor without which an absence assertion would pass without testing
// anything.
func wireRequireHeld(t *testing.T, state emulator.StateManager, namespace, key string, members ...string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), namespace, key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s/%s", namespace, key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	for _, member := range members {
		require.NotEmptyf(t, record[member], "%s must persist %s before an absence assertion on it means anything", key, member)
		require.NotEqualf(t, `""`, string(record[member]), "%s persists an empty %s", key, member)
	}
	return record
}

// wireCase is one operation driven by wireRunJSON. A non-nil call issues it; a nil call reuses held.
type wireCase struct {
	op     string
	call   func() []byte
	held   []byte
	anchor string
}

// wireRunJSON drives each case as a subtest: the presence anchor first, then the walk.
func wireRunJSON(t *testing.T, members []string, cases []wireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if tc.call != nil {
				body = tc.call()
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, members)
		})
	}
}

// wireRecordByPrefix returns the single record in namespace whose key starts with prefix, as raw JSON.
// It is for the services that key a record by something a test does not otherwise hold, such as a
// queue URL.
func wireRecordByPrefix(t *testing.T, state emulator.StateManager, namespace, prefix string) map[string]json.RawMessage {
	t.Helper()
	keys, err := state.List(t.Context(), namespace, prefix)
	require.NoError(t, err, "state.List %s", prefix)
	require.Lenf(t, keys, 1, "want exactly one %s record, have %v", prefix, keys)
	data, err := state.Get(t.Context(), namespace, keys[0])
	require.NoError(t, err, "state.Get %s", keys[0])
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", keys[0], data)
	return record
}
