package emulator_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// State is held in Go maps, whose iteration order is randomized per process, and
// [emulator.StateManager.List] returned that order until #865. Every test here
// asserts an order, so every one of them could pass by luck on a single run; each
// therefore either seeds its input in an order that is the *reverse* of the
// expected output, or asserts two successive calls agree, so that map order
// cannot be mistaken for the contract being honored.
//
// Each case goes over the wire rather than reading state back, because the defect
// is what a caller *observes*: a listing whose members are in state order is only
// a defect at the point it reaches a response body.

// newS3ClockTestServer is [newS3TestServerWithFaultFS] with the clock handed back,
// so a test can place two writes at known simulated instants.
//
// It exists because the shared helper keeps its [emulator.TimeController] private,
// and the documented multipart-upload order is *by initiation time* — an ordering
// no test can assert without setting one. Advancing the clock by whole seconds also
// keeps the assertion off the wall clock: successive live calls differ by
// microseconds, which the RFC 3339 rendering does not show and which would leave
// the test's outcome resting on scheduling.
func newS3ClockTestServer(t *testing.T) (*emulator.Server, *emulator.TimeController) {
	t.Helper()

	cfg := emulator.DefaultConfig()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	state := emulator.NewMemoryStateManager()

	s3p := &emulator.S3Plugin{}
	require.NoError(t, s3p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))

	registry := emulator.NewPluginRegistry()
	registry.Register(s3p)

	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})

	srv := emulator.NewServer(
		*cfg,
		registry,
		store,
		state,
		tc,
		logger,
		emulator.ServerOptions{Costs: emulator.NewCostController(emulator.CostConfig{Enabled: true})},
	)

	return srv, tc
}

// TestMemoryStateManagerListIsLexicographic asserts the ordering contract
// [emulator.StateManager.List]'s doc comment states, directly on the one
// non-test implementation.
//
// The keys are written in an order that is neither the expected output nor its
// reverse, and there are enough of them that a passing map walk would be a
// coincidence rather than a plausible accident (#865).
func TestMemoryStateManagerListIsLexicographic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := emulator.NewMemoryStateManager()

	for _, k := range []string{"item:delta", "item:alpha", "item:foxtrot", "item:charlie", "other:zulu", "item:bravo", "item:echo"} {
		require.NoError(t, m.Put(ctx, "ns", k, []byte("v")))
	}

	got, err := m.List(ctx, "ns", "item:")
	require.NoError(t, err)
	require.Equal(t, []string{
		"item:alpha", "item:bravo", "item:charlie", "item:delta", "item:echo", "item:foxtrot",
	}, got, "List must return matching keys sorted lexicographically")

	all, err := m.List(ctx, "ns", "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"item:alpha", "item:bravo", "item:charlie", "item:delta", "item:echo", "item:foxtrot", "other:zulu",
	}, all, "an empty prefix must return every key in the namespace, still sorted")

	absent, err := m.List(ctx, "missing", "")
	require.NoError(t, err)
	require.Empty(t, absent, "an absent namespace must list empty, not error")
}

// TestS3ListBucketsIsSortedAndRepeatable covers the plainest case of #865: the
// handler renders [emulator.StateManager.List]'s return straight into
// ListAllMyBucketsResult with no sort of its own (s3_plugin.go listBuckets), so
// before the fix the bucket order was Go's map order.
//
// Two assertions, because they fail for different reasons. Sorted order catches a
// handler that reports state order; byte-identical successive bodies catch the
// thing that makes the defect a *replay* defect rather than an untidiness — two
// identical calls in one process answering differently.
//
// Lexicographic order here is substrate's reading, not AWS's: API_ListBuckets
// documents no order at all. It rests on the replay guarantee.
func TestS3ListBucketsIsSortedAndRepeatable(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	// Created in reverse of the expected output, so state order and sorted order
	// cannot be confused for one another.
	created := []string{"zeta-bucket", "yankee-bucket", "x-ray-bucket", "whiskey-bucket",
		"victor-bucket", "uniform-bucket", "tango-bucket", "sierra-bucket"}
	for _, b := range created {
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/"+b, nil, nil).Code, "create bucket %s", b)
	}

	type bucketEntry struct {
		Name string `xml:"Name"`
	}
	var parsed struct {
		Buckets struct {
			Bucket []bucketEntry `xml:"Bucket"`
		} `xml:"Buckets"`
	}

	first := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, first.Code)
	require.NoError(t, xml.Unmarshal(first.Body.Bytes(), &parsed))

	got := make([]string, 0, len(parsed.Buckets.Bucket))
	for _, b := range parsed.Buckets.Bucket {
		got = append(got, b.Name)
	}
	require.Equal(t, []string{
		"sierra-bucket", "tango-bucket", "uniform-bucket", "victor-bucket",
		"whiskey-bucket", "x-ray-bucket", "yankee-bucket", "zeta-bucket",
	}, got, "ListBuckets must report buckets sorted, not in state order")

	second := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, second.Code)
	require.True(t, bytes.Equal(first.Body.Bytes(), second.Body.Bytes()),
		"two ListBuckets calls over unchanged state must return byte-identical bodies")
}

// TestS3ListMultipartUploadsSortsByKeyThenInitiationTime asserts the one order in
// this release that AWS publishes rather than substrate inferring it:
// API_ListMultipartUploads' "Sorting of multipart uploads in response" section
// gives ascending object key, then ascending initiation time among uploads sharing
// a key.
//
// The three uploads are initiated in the exact reverse of the expected order, and
// the two sharing a key are placed ten simulated seconds apart, so the within-key
// result is decided by initiation time alone — not by creation order, and not by
// the upload IDs, which are random and are only the last-resort tie-break.
func TestS3ListMultipartUploadsSortsByKeyThenInitiationTime(t *testing.T) {
	t.Parallel()
	srv, tc := newS3ClockTestServer(t)

	const bucket = "mpu-order"
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")

	initiate := func(key string, at time.Time) string {
		t.Helper()
		tc.SetTime(at)
		w := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploads", nil, nil)
		require.Equal(t, http.StatusOK, w.Code, "initiate upload for %s", key)
		var ir struct {
			UploadID string `xml:"UploadId"`
		}
		require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &ir))
		require.NotEmpty(t, ir.UploadID)
		return ir.UploadID
	}

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Reverse of the expected output: latest key first, and the later of the two
	// same-key uploads before the earlier one.
	initiate("beta", base.Add(30*time.Second))
	alphaLate := initiate("alpha", base.Add(20*time.Second))
	alphaEarly := initiate("alpha", base.Add(10*time.Second))

	type uploadEntry struct {
		Key       string `xml:"Key"`
		UploadID  string `xml:"UploadId"`
		Initiated string `xml:"Initiated"`
	}
	var parsed struct {
		Uploads []uploadEntry `xml:"Upload"`
	}
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &parsed))
	require.Len(t, parsed.Uploads, 3)

	gotKeys := []string{parsed.Uploads[0].Key, parsed.Uploads[1].Key, parsed.Uploads[2].Key}
	require.Equal(t, []string{"alpha", "alpha", "beta"}, gotKeys,
		"uploads must be sorted by object key ascending")

	require.Equal(t, alphaEarly, parsed.Uploads[0].UploadID,
		"among uploads sharing a key, the one initiated first must come first")
	require.Equal(t, alphaLate, parsed.Uploads[1].UploadID,
		"among uploads sharing a key, the later initiation must come second")

	require.Equal(t, "2026-01-01T12:00:10Z", parsed.Uploads[0].Initiated)
	require.Equal(t, "2026-01-01T12:00:20Z", parsed.Uploads[1].Initiated)
	require.Equal(t, "2026-01-01T12:00:30Z", parsed.Uploads[2].Initiated)
}

// TestS3ListObjectVersionsOrdersKeysThenVersions asserts the order
// ListObjectVersions' own cursor requires. AWS states none in prose, but
// NextKeyMarker is "the first key not returned that satisfies the search
// criteria" and a CommonPrefixes entry "is filtered out from results if it is not
// lexicographically greater than the key-marker" — so a caller paging with
// KeyMarker over an unstable key order could skip or repeat a key.
//
// Keys are written in reverse of the expected output. The two versions of one key
// prove the second half of the order: newest first, which comes from the stored
// version list being built by prepend rather than from any sort.
func TestS3ListObjectVersionsOrdersKeysThenVersions(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	const bucket = "versions-order"
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil).Code,
		"enable versioning")

	putObject := func(key, body string) string {
		t.Helper()
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte(body), nil)
		require.Equal(t, http.StatusOK, w.Code, "put %s", key)
		vid := w.Header().Get("x-amz-version-id")
		require.NotEmpty(t, vid, "a versioned put must report a version ID")
		return vid
	}

	// Reverse of the expected key order.
	putObject("gamma", "g")
	putObject("beta", "b")
	alphaV1 := putObject("alpha", "a1")
	alphaV2 := putObject("alpha", "a2")

	var parsed struct {
		Versions []struct {
			Key       string `xml:"Key"`
			VersionID string `xml:"VersionId"`
			IsLatest  bool   `xml:"IsLatest"`
		} `xml:"Version"`
	}
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?versions", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &parsed))
	require.Len(t, parsed.Versions, 4)

	gotKeys := make([]string, 0, 4)
	for _, v := range parsed.Versions {
		gotKeys = append(gotKeys, v.Key)
	}
	require.Equal(t, []string{"alpha", "alpha", "beta", "gamma"}, gotKeys,
		"keys must be lexicographic, as ListObjectVersions' KeyMarker cursor requires")

	require.Equal(t, alphaV2, parsed.Versions[0].VersionID, "the newest version of a key comes first")
	require.True(t, parsed.Versions[0].IsLatest, "the first version of a key is the current one")
	require.Equal(t, alphaV1, parsed.Versions[1].VersionID, "the older version follows")
	require.False(t, parsed.Versions[1].IsLatest)
}

// TestEC2DescribeInstancesReservationSetIsOrdered covers the half of #865 that
// sorting [emulator.StateManager.List] did not reach.
//
// DescribeInstances buckets instances into reservations through a
// map[string]*reservationItem and then ranges that map to build reservationSet, so
// the instances *within* a reservation became deterministic with the List sort
// while the reservations themselves stayed in Go map order. Six separate
// RunInstances calls make that visible; one or two would pass on a map walk often
// enough to be worthless.
//
// AWS documents no order for reservationSet, so ascending reservation ID is
// substrate's reading. The IDs are minted randomly and so cannot be predicted,
// which is why the assertion is that they arrive ascending and that two calls agree
// — both of which a map walk fails and neither of which assumes a value.
func TestEC2DescribeInstancesReservationSetIsOrdered(t *testing.T) {
	ts := newEC2TestServer(t)

	for range 6 {
		resp := ec2Request(t, ts, map[string]string{
			"Action":       "RunInstances",
			"ImageId":      ec2TestImage,
			"InstanceType": "t3.micro",
			"MinCount":     "1",
			"MaxCount":     "1",
		})
		require.Equal(t, http.StatusOK, resp.StatusCode, "RunInstances")
		require.NoError(t, resp.Body.Close())
	}

	describe := func() ([]string, []byte) {
		t.Helper()
		resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
		require.Equal(t, http.StatusOK, resp.StatusCode, "DescribeInstances")
		defer resp.Body.Close() //nolint:errcheck
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var parsed struct {
			Reservations []struct {
				ReservationID string `xml:"reservationId"`
			} `xml:"reservationSet>item"`
		}
		require.NoError(t, xml.Unmarshal(raw, &parsed))
		ids := make([]string, 0, len(parsed.Reservations))
		for _, r := range parsed.Reservations {
			ids = append(ids, r.ReservationID)
		}
		return ids, raw
	}

	ids, firstBody := describe()
	require.Len(t, ids, 6, "six RunInstances calls make six reservations")
	require.IsIncreasing(t, ids, "reservationSet must be ordered by reservation ID, not by map order")

	_, secondBody := describe()
	require.True(t, bytes.Equal(firstBody, secondBody),
		"two DescribeInstances calls over unchanged state must return byte-identical bodies")
}

// TestS3ListingsAreDeterministicAcrossCalls is the assertion that speaks to the
// replay guarantee rather than to any one operation's documented order: over
// unchanged state, a listing's bytes must not move.
//
// It is separate from the per-operation tests because it is the property that
// makes the whole class a correctness bug — a recorded run cannot be replayed
// byte-for-byte if a response body reorders itself between two reads of the same
// state — and because it covers the listings whose order AWS documents nowhere.
func TestS3ListingsAreDeterministicAcrossCalls(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	const bucket = "determinism"
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")

	for i := range 12 {
		key := fmt.Sprintf("obj-%02d", (i*7)%12) // written in a scrambled order
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("x"), nil).Code, "put %s", key)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPost, "/"+bucket+"/mpu-"+key+"?uploads", nil, nil).Code, "initiate %s", key)
	}

	for _, path := range []string{"/", "/" + bucket + "?uploads", "/" + bucket + "?versions", "/" + bucket} {
		first := s3Request(t, srv, http.MethodGet, path, nil, nil)
		require.Equal(t, http.StatusOK, first.Code, "GET %s", path)
		second := s3Request(t, srv, http.MethodGet, path, nil, nil)
		require.Equal(t, http.StatusOK, second.Code, "GET %s", path)
		require.True(t, bytes.Equal(first.Body.Bytes(), second.Body.Bytes()),
			"GET %s returned two different bodies for unchanged state", path)
	}
}
