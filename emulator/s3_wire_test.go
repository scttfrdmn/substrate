package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for S3Bucket (#756).
//
// S3Bucket declares AccountID as `account_id` and Region as `region`, plus EverTagged as
// `ever_tagged,omitempty`, because the record is what MemoryStateManager snapshots and a replay reads
// back. None reaches a body: every bucket response is marshaled from an XML struct declared for its
// operation, and no XML-tagged field is typed as the record. The published bucket members that sound
// alike are different names — ListBuckets' BucketRegion, GetBucketLocation's LocationConstraint — and
// the walk is a case-folded equality, so neither collides.

// s3BookkeepingMembers are the members S3Bucket declares and no S3 shape publishes as an element. The
// snake_case spellings are listed in their own right because a fold does not reach them.
var s3BookkeepingMembers = []string{"AccountID", "account_id", "Region", "EverTagged", "ever_tagged"}

// s3WireBody issues one request and returns the body, failing on anything but a 2xx.
func s3WireBody(t *testing.T, srv *emulator.Server, method, path string, body []byte) []byte {
	t.Helper()
	w := s3Request(t, srv, method, path, body, nil)
	require.Truef(t, w.Code >= 200 && w.Code < 300, "%s %s answered %d: %s", method, path, w.Code, w.Body.String())
	return w.Body.Bytes()
}

// s3WireRecord returns the bucket record as raw JSON, found by listing the namespace so the test does
// not depend on how the plugin keys it.
func s3WireRecord(t *testing.T, state emulator.StateManager, bucket string) map[string]json.RawMessage {
	t.Helper()
	keys, err := state.List(t.Context(), "s3", "")
	require.NoError(t, err, "state.List s3")
	for _, key := range keys {
		if !strings.HasSuffix(key, bucket) || strings.Contains(key, bucket+"/") {
			continue
		}
		data, getErr := state.Get(t.Context(), "s3", key)
		require.NoError(t, getErr, "state.Get %s", key)
		var record map[string]json.RawMessage
		if json.Unmarshal(data, &record) == nil && string(record["name"]) == `"`+bucket+`"` {
			return record
		}
	}
	require.Failf(t, "no record", "no bucket record for %s among %v", bucket, keys)
	return nil
}

func TestS3Wire_BucketResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newS3TestServerWithState(t, state)

	const bucket = "wire-bookkeeping-bucket"
	created := s3WireBody(t, srv, http.MethodPut, "/"+bucket, nil)

	record := s3WireRecord(t, state, bucket)
	for _, member := range []string{"account_id", "region"} {
		require.NotEmptyf(t, record[member], "the bucket must persist %s before an absence assertion on it means anything", member)
		require.NotEqualf(t, `""`, string(record[member]), "the bucket persists an empty %s", member)
	}

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	tagging := []byte(`<Tagging><TagSet><Tag><Key>team</Key><Value>wire</Value></Tag></TagSet></Tagging>`)
	tagged := s3WireBody(t, srv, http.MethodPut, "/"+bucket+"?tagging", tagging)
	require.JSONEq(t, "true", string(s3WireRecord(t, state, bucket)["ever_tagged"]),
		"the bucket must persist ever_tagged before an absence assertion on it means anything")

	for _, tc := range []struct {
		op, method, path string
		held             []byte
		anchor           string
	}{
		{op: "CreateBucket", held: created},
		{op: "PutBucketTagging", held: tagged},
		{op: "HeadBucket", method: http.MethodHead, path: "/" + bucket},
		{op: "ListBuckets", method: http.MethodGet, path: "/", anchor: "<Name>" + bucket + "</Name>"},
		// GetBucketLocation would answer the bucket too, and is not driven: substrate does not route
		// ?location, which is refused as NotImplemented rather than answered (#1349).
		{op: "GetBucketTagging", method: http.MethodGet, path: "/" + bucket + "?tagging", anchor: "<Key>team</Key>"},
		{op: "DeleteBucketTagging", method: http.MethodDelete, path: "/" + bucket + "?tagging"},
		// Last: it removes the record every case above reads.
		{op: "DeleteBucket", method: http.MethodDelete, path: "/" + bucket},
	} {
		t.Run(tc.op, func(t *testing.T) {
			// A held case has no method: it reuses the response the test already has, which for
			// CreateBucket and PutBucketTagging is an empty body. Keying on the body instead would
			// re-issue them as GET / and walk ListBuckets in their place.
			body := tc.held
			if tc.method != "" {
				body = s3WireBody(t, srv, tc.method, tc.path, nil)
			}
			if tc.anchor != "" {
				require.Containsf(t, string(body), tc.anchor,
					"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			}
			// Several of these answer no body at all, which the walk accepts; the ones that render
			// the bucket carry an anchor above.
			if len(body) == 0 {
				return
			}
			wireAssertNoMemberXML(t, tc.op, body, s3BookkeepingMembers, "")
		})
	}
}
