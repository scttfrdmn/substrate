package emulator_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Every ECR date a response renders is epoch seconds (#1403).
//
// ECR speaks awsJson1_1, where a Timestamp is a JSON number of epoch seconds with a fraction, and an
// SDK's timestamp deserializer refuses a string. API_ImageDetail types imagePushedAt as Timestamp,
// API_AuthorizationData types expiresAt as Timestamp ("The Unix time in seconds and milliseconds"),
// and API_Repository types createdAt as Timestamp. imagePushedAt and expiresAt used to render as
// RFC3339 strings, so DescribeImages and GetAuthorizationToken failed to decode. The clock is frozen
// at a sub-second instant so the fraction is asserted, not just the type.

// ecrDatesClock is the instant every fixture below runs at: a whole second plus 123 milliseconds.
var ecrDatesClock = time.Unix(1700000000, 123_000_000).UTC()

func TestECRDates_EveryRenderedDateIsEpochSeconds(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(ecrDatesClock)
	tc.Freeze()
	tc.SetTime(ecrDatesClock)
	p := &emulator.ECRPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   emulator.NewMemoryStateManager(),
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	ctx := &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ecr-dates", IDs: emulator.NewIDMint("req-ecr-dates"),
	}
	call := func(op string, body map[string]any) map[string]json.RawMessage {
		t.Helper()
		resp, err := p.HandleRequest(ctx, ecrRequest(t, op, body))
		require.NoError(t, err, "%s", op)
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(resp.Body, &doc), "decode %s: %s", op, resp.Body)
		return doc
	}
	// first decodes the member of the first element of the named array.
	first := func(op string, doc map[string]json.RawMessage, array, member string) json.RawMessage {
		t.Helper()
		var items []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(doc[array], &items), "%s: decode %s", op, array)
		require.NotEmpty(t, items, "%s answered no %s", op, array)
		return items[0][member]
	}

	created := call("CreateRepository", map[string]any{"repositoryName": "dates"})
	var repo map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created["repository"], &repo), "decode repository")
	call("PutImage", map[string]any{"repositoryName": "dates", "imageTag": "v1", "imageManifest": `{"schemaVersion":2}`})

	for _, c := range []struct {
		site string
		raw  func() json.RawMessage
		want string
	}{
		{"CreateRepository repository.createdAt", func() json.RawMessage { return repo["createdAt"] }, "1700000000.123"},
		{"DescribeImages imageDetails[].imagePushedAt", func() json.RawMessage {
			return first("DescribeImages", call("DescribeImages", map[string]any{"repositoryName": "dates"}), "imageDetails", "imagePushedAt")
		}, "1700000000.123"},
		// API_AuthorizationData: "Authorization tokens are valid for 12 hours."
		{"GetAuthorizationToken authorizationData[].expiresAt", func() json.RawMessage {
			return first("GetAuthorizationToken", call("GetAuthorizationToken", map[string]any{}), "authorizationData", "expiresAt")
		}, "1700043200.123"},
	} {
		t.Run(c.site, func(t *testing.T) {
			raw := c.raw()
			require.NotEmpty(t, raw, "%s is absent", c.site)
			require.NotEqual(t, byte('"'), raw[0], "%s answered %s, a string; awsJson1_1 publishes a Timestamp as a number", c.site, raw)
			require.Equal(t, c.want, string(raw), "%s must be epoch seconds to three decimals", c.site)
		})
	}
}
