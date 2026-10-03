package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for ACMCertificate (#756), and
// the published form of its dates (#1305).
//
// ACMCertificate declares AccountID and Region under wire-visible `json` tags, and EverTagged as
// `ever_tagged,omitempty`. DescribeCertificate used to answer the record whole; it answers
// emulator/acm_wire.go's projection now, and this file is what says so. The other seven routed
// operations answer an ARN, a summary list, tags or nothing, and are driven anyway.

// acmWireClock is the instant every fixture below runs at, on a frozen clock.
var acmWireClock = time.Unix(1700000000, 0).UTC()

// acmWireAccount and acmWireRegion scope every state key the plugin writes.
const (
	acmWireAccount = "123456789012"
	acmWireRegion  = "us-east-1"
)

// acmBookkeepingMembers are the members ACMCertificate declares and no ACM shape publishes.
// `ever_tagged` is listed in its own right because a fold does not reach a snake_case spelling.
var acmBookkeepingMembers = []string{"AccountID", "Region", "EverTagged", "ever_tagged"}

// setupACMWirePlugin returns the ACM plugin, a request context and the state manager behind it.
// Freeze then SetTime, in the order TimeController.Freeze documents.
func setupACMWirePlugin(t *testing.T) (*emulator.ACMPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(acmWireClock)
	tc.Freeze()
	tc.SetTime(acmWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.ACMPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.ACMPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: acmWireAccount,
		Region:    acmWireRegion,
		RequestID: "req-acm-wire",
		IDs:       emulator.NewIDMint("req-acm-wire"),
	}, state
}

// acmWire issues one operation and returns the raw response body, failing on anything but 200.
func acmWire(t *testing.T, p *emulator.ACMPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	resp, err := p.HandleRequest(ctx, acmRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// acmWireRecord returns the certificate record as raw JSON.
func acmWireRecord(t *testing.T, state emulator.StateManager, arn string) map[string]json.RawMessage {
	t.Helper()
	key := "cert:" + acmWireAccount + "/" + acmWireRegion + "/" + arn
	data, err := state.Get(t.Context(), "acm", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

func TestACMWire_CertificateResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupACMWirePlugin(t)

	requested := acmWire(t, p, ctx, "RequestCertificate", map[string]any{"DomainName": "wire.example.com"})
	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(requested, &out), "decode RequestCertificate: %s", requested)
	arn := out.CertificateArn
	require.NotEmpty(t, arn, "RequestCertificate must report an ARN")

	record := acmWireRecord(t, state, arn)
	require.JSONEq(t, `"`+acmWireAccount+`"`, string(record["AccountID"]), "the certificate must persist AccountID")
	require.JSONEq(t, `"`+acmWireRegion+`"`, string(record["Region"]), "the certificate must persist Region")

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	tagged := acmWire(t, p, ctx, "AddTagsToCertificate", map[string]any{"CertificateArn": arn, "Tags": []map[string]string{{"Key": "team", "Value": "wire"}}})
	require.JSONEq(t, "true", string(acmWireRecord(t, state, arn)["ever_tagged"]),
		"the certificate must persist ever_tagged before an absence assertion on it means anything")

	cert := map[string]any{"CertificateArn": arn}
	described := acmWire(t, p, ctx, "DescribeCertificate", cert)

	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "RequestCertificate", held: requested, anchor: `"CertificateArn"`},
		{op: "AddTagsToCertificate", held: tagged, anchor: "{}"},
		{op: "DescribeCertificate", held: described, anchor: `"DomainName":"wire.example.com"`},
		{op: "ListCertificates", anchor: `"DomainName":"wire.example.com"`},
		{op: "ListTagsForCertificate", body: cert, anchor: `"Key":"team"`},
		{op: "RemoveTagsFromCertificate", body: map[string]any{"CertificateArn": arn, "Tags": []map[string]string{{"Key": "team"}}}, anchor: "{}"},
		{op: "RenewCertificate", body: cert, anchor: "{}"},
		// Last: it removes the record every case above reads.
		{op: "DeleteCertificate", body: cert, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = acmWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, acmBookkeepingMembers)
		})
	}

	// After the walk, so a regression is reported as a bookkeeping leak first.
	var desc struct {
		Certificate map[string]json.RawMessage `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(described, &desc), "decode DescribeCertificate: %s", described)
	// API_CertificateDetail publishes no Tags: a certificate's tags are ListTagsForCertificate's.
	require.NotContains(t, slices.Sorted(maps.Keys(desc.Certificate)), "Tags",
		"API_CertificateDetail publishes no Tags: %s", described)
	// Each published Timestamp is epoch seconds under awsJson1_1, and the clock is frozen (#1305).
	// NotAfter is a year on, so it is checked as a number rather than by value.
	for _, member := range []string{"CreatedAt", "IssuedAt", "NotBefore"} {
		require.JSONEqf(t, "1700000000.000", string(desc.Certificate[member]), "%s must be epoch seconds: %s", member, described)
	}
	var notAfter float64
	require.NoError(t, json.Unmarshal(desc.Certificate["NotAfter"], &notAfter), "NotAfter must be a number: %s", described)
	require.Greater(t, notAfter, 1700000000.0, "NotAfter is after the certificate was issued")
}
