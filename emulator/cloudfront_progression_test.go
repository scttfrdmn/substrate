package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A distribution reports InProgress for a seeded number of observations after a create and after
// every update, then Deployed (#1381). Every assertion reads the raw XML a waiter would read: the
// Status element of GetDistribution, ListDistributions and the create and update responses.

// cfProgSeedPath is the distribution-status seed endpoint.
const cfProgSeedPath = "/v1/cloudfront/distribution-status"

// cfProgConfig is a configuration CreateDistribution accepts and UpdateDistribution can be sent
// back as read: GetDistributionConfig answers it with the defaults the update requires.
const cfProgConfig = `<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
	`<CallerReference>prog</CallerReference><Comment>prog</Comment><Enabled>true</Enabled></DistributionConfig>`

// cfProgStatus matches the Status element of a distribution document.
var cfProgStatus = regexp.MustCompile(`<Status>([^<]*)</Status>`)

// cfProgCall issues one signed CloudFront request against ts, with If-Match when ifMatch is set,
// and returns the status, body and ETag header.
func cfProgCall(t *testing.T, ts *emulator.TestServer, method, path, ifMatch, body string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Host = "cloudfront.amazonaws.com"
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1"+
		"/cloudfront/aws4_request, SignedHeaders=host, Signature=fake")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // drained below.
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw), resp.Header.Get("ETag")
}

// cfProgSeed posts a distribution-status seed and requires it accepted.
func cfProgSeed(t *testing.T, ts *emulator.TestServer, body string) {
	t.Helper()
	status, got := cpControlPlaneRequest(t, ts, http.MethodPost, cfProgSeedPath, []byte(body))
	require.Equal(t, http.StatusOK, status, "seed %s: %s", body, got)
}

// cfProgCreate creates a distribution and returns its ID, ETag and the Status the create reported.
func cfProgCreate(t *testing.T, ts *emulator.TestServer) (id, etag, status string) {
	t.Helper()
	code, body, etag := cfProgCall(t, ts, http.MethodPost, "/2020-05-31/distribution", "", cfProgConfig)
	require.Equal(t, http.StatusCreated, code, "CreateDistribution: %s", body)
	id = regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(body)[1]
	return id, etag, cfProgStatus.FindStringSubmatch(body)[1]
}

// cfProgGet answers the Status one GetDistribution of id reports, and the ETag.
func cfProgGet(t *testing.T, ts *emulator.TestServer, id string) (status, etag string) {
	t.Helper()
	code, body, etag := cfProgCall(t, ts, http.MethodGet, "/2020-05-31/distribution/"+id, "", "")
	require.Equal(t, http.StatusOK, code, "GetDistribution: %s", body)
	return cfProgStatus.FindStringSubmatch(body)[1], etag
}

// cfProgUpdate sends the distribution's own configuration back, with Enabled set as given, and
// returns the Status the update reported and its ETag.
func cfProgUpdate(t *testing.T, ts *emulator.TestServer, id, etag string, enabled bool) (status, newETag string) {
	t.Helper()
	code, cfg, cfgETag := cfProgCall(t, ts, http.MethodGet, "/2020-05-31/distribution/"+id+"/config", "", "")
	require.Equal(t, http.StatusOK, code, "GetDistributionConfig: %s", cfg)
	require.Equal(t, etag, cfgETag, "the config read answers the current version")
	cfg = strings.TrimPrefix(cfg, `<?xml version="1.0" encoding="UTF-8"?>`+"\n")
	if !enabled {
		cfg = strings.Replace(cfg, "<Enabled>true</Enabled>", "<Enabled>false</Enabled>", 1)
	}
	code, body, newETag := cfProgCall(t, ts, http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, cfg)
	require.Equal(t, http.StatusOK, code, "UpdateDistribution: %s", body)
	return cfProgStatus.FindStringSubmatch(body)[1], newETag
}

func TestCloudFrontProgression_AnUnseededDistributionIsDeployedAtOnce(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	id, etag, created := cfProgCreate(t, ts)
	assert.Equal(t, "Deployed", created, "the default window is zero observations, as before #1381")
	got, _ := cfProgGet(t, ts, id)
	assert.Equal(t, "Deployed", got)
	updated, _ := cfProgUpdate(t, ts, id, etag, true)
	assert.Equal(t, "Deployed", updated)
}

func TestCloudFrontProgression_ASeededWindowIsObservedAfterCreateAndAfterUpdate(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	cfProgSeed(t, ts, `{"distributionId":"*","inProgressObservations":2}`)

	id, etag, created := cfProgCreate(t, ts)
	assert.Equal(t, "InProgress", created, "the create's response peeks at the first observation")
	var seen []string
	for range 3 {
		status, _ := cfProgGet(t, ts, id)
		seen = append(seen, status)
	}
	assert.Equal(t, []string{"InProgress", "InProgress", "Deployed"}, seen,
		"two observations in progress, then Deployed; the create's own response spent none")

	updated, etag := cfProgUpdate(t, ts, id, etag, true)
	assert.Equal(t, "InProgress", updated, "an update starts a new propagation")
	seen = seen[:0]
	for range 3 {
		status, current := cfProgGet(t, ts, id)
		require.Equal(t, etag, current)
		seen = append(seen, status)
	}
	assert.Equal(t, []string{"InProgress", "InProgress", "Deployed"}, seen,
		"the update restarted the countdown rather than inheriting a settled one")
}

// ListDistributions spends one observation of each distribution's own countdown, not one shared
// countdown per distribution listed (#582).
func TestCloudFrontProgression_ListDistributionsObservesEachDistributionOnce(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	cfProgSeed(t, ts, `{"distributionId":"*","inProgressObservations":1}`)
	cfProgCreate(t, ts)
	cfProgCreate(t, ts)

	list := func() []string {
		code, body, _ := cfProgCall(t, ts, http.MethodGet, "/2020-05-31/distribution", "", "")
		require.Equal(t, http.StatusOK, code, "ListDistributions: %s", body)
		var statuses []string
		for _, m := range cfProgStatus.FindAllStringSubmatch(body, -1) {
			statuses = append(statuses, m[1])
		}
		return statuses
	}
	assert.Equal(t, []string{"InProgress", "InProgress"}, list(), "each distribution's first observation")
	assert.Equal(t, []string{"Deployed", "Deployed"}, list(), "each distribution's second observation")
}

func TestCloudFrontProgression_AnIDSeedWinsAndClearingRestoresDeployed(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	idA, _, _ := cfProgCreate(t, ts)
	idB, _, _ := cfProgCreate(t, ts)
	cfProgSeed(t, ts, `{"distributionId":"*","inProgressObservations":1}`)
	cfProgSeed(t, ts, `{"distributionId":"`+idA+`","inProgressObservations":3}`)

	for i := range 3 {
		got, _ := cfProgGet(t, ts, idA)
		require.Equalf(t, "InProgress", got, "the ID seed governs %s, observation %d", idA, i+1)
	}
	got, _ := cfProgGet(t, ts, idB)
	assert.Equal(t, "InProgress", got, "the wildcard governs the other distribution")
	got, _ = cfProgGet(t, ts, idB)
	assert.Equal(t, "Deployed", got)

	status, out := cpControlPlaneRequest(t, ts, http.MethodDelete, cfProgSeedPath+"?distributionId="+idA, nil)
	require.Equal(t, http.StatusOK, status, "%s", out)
	got, _ = cfProgGet(t, ts, idA)
	assert.Equal(t, "InProgress", got, "with its ID seed cleared, the wildcard governs it from a fresh countdown")

	status, out = cpControlPlaneRequest(t, ts, http.MethodDelete, cfProgSeedPath, nil)
	require.Equal(t, http.StatusOK, status, "%s", out)
	for _, id := range []string{idA, idB} {
		got, _ = cfProgGet(t, ts, id)
		assert.Equalf(t, "Deployed", got, "with every seed cleared, %s reads its record again", id)
	}
}

func TestCloudFrontProgression_ASeedWithANegativeCountIsRefused(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, out := cpControlPlaneRequest(t, ts, http.MethodPost, cfProgSeedPath,
		[]byte(`{"distributionId":"*","inProgressObservations":-1}`))
	assert.Equal(t, http.StatusBadRequest, status, "%s", out)
	assert.Contains(t, out, "inProgressObservations")
}

// API_DeleteDistribution publishes DistributionNotDisabled and no refusal for a distribution still
// InProgress, so the disable-then-delete sequence succeeds inside the window.
func TestCloudFrontProgression_ADisabledDistributionDeletesDuringTheWindow(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	cfProgSeed(t, ts, `{"distributionId":"*","inProgressObservations":5}`)
	id, etag, _ := cfProgCreate(t, ts)

	disabled, etag := cfProgUpdate(t, ts, id, etag, false)
	require.Equal(t, "InProgress", disabled, "the disable is an update, so it is in progress")
	code, body, _ := cfProgCall(t, ts, http.MethodDelete, "/2020-05-31/distribution/"+id, etag, "")
	require.Equal(t, http.StatusNoContent, code, "DeleteDistribution: %s", body)
	code, _, _ = cfProgCall(t, ts, http.MethodGet, "/2020-05-31/distribution/"+id, "", "")
	assert.Equal(t, http.StatusNotFound, code, "the distribution is gone")
}

// The seed is a control-plane write recorded in position, so a replay restarts the countdown at
// the same point and every replayed observation answers what the recording answered (#1140).
func TestCloudFrontProgression_ASeededWindowReplaysIdentically(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	cfProgSeed(t, ts, `{"distributionId":"*","inProgressObservations":2}`)
	id, etag, _ := cfProgCreate(t, ts)
	for range 3 {
		cfProgGet(t, ts, id)
	}
	_, etag = cfProgUpdate(t, ts, id, etag, true)
	recorded := make([]string, 0, 3)
	for range 3 {
		status, _ := cfProgGet(t, ts, id)
		recorded = append(recorded, status)
	}
	require.Equal(t, []string{"InProgress", "InProgress", "Deployed"}, recorded,
		"the recording must hold a window, or the replay has nothing to reproduce")
	require.NotEmpty(t, etag)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.SkippedEvents, "the seed is re-executed, not skipped")
	assert.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences,
		"every replayed observation answers the recorded Status: %s", replayDifferenceSummary(results))
}

// The CloudFormation stack delete reads the configuration, disables and deletes; under a seeded
// window it still completes, because nothing refuses a delete in progress and its read spends no
// observation.
func TestCloudFrontProgression_AStackDeleteCompletesUnderASeededWindow(t *testing.T) {
	d, state, _, _ := newSweepDeployer(t)
	seed, err := json.Marshal(map[string]any{"distributionId": "*", "inProgressObservations": 5})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "cloudfront-dist-ctrl", "status:*", seed))
	id := cfnDeployedDistributionID(t, d, "cf-window")

	require.NoError(t, d.DeleteStack(context.Background(), "cf-window"))
	assert.Equal(t, http.StatusNotFound, cfnDistributionStatus(t, d, id), "the distribution dies with its stack")
}

// A store fault in the countdown is an error from every operation that reads or restarts it, never
// a Status answered over a countdown that could not be read or reset.
func TestCloudFrontProgression_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		suffix string
		body   func(readConfig string) string
	}{
		{"GetDistribution, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodGet, "", nil},
		{"GetDistribution, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, http.MethodGet, "", nil},
		{"GetDistribution, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, http.MethodGet, "", nil},
		{"GetDistribution, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "status:" }, http.MethodGet, "", nil},
		{"ListDistributions, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, http.MethodGet, "list", nil},
		{"UpdateDistribution, countdown reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, http.MethodPut, "/config", func(cfg string) string { return cfg }},
		{"UpdateDistribution, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodPut, "/config", func(cfg string) string { return cfg }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := cfOACPluginOn(t, fault)
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-cf-prog-fault", IDs: emulator.NewIDMint("req-cf-prog-fault")}
			seed, err := json.Marshal(map[string]any{"distributionId": "*", "inProgressObservations": 3})
			require.NoError(t, err)
			require.NoError(t, fault.inner.Put(context.Background(), "cloudfront-dist-ctrl", "status:*", seed))
			resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "cloudfront", HTTPMethod: http.MethodPost, Path: "/2020-05-31/distribution", Body: []byte(cfProgConfig),
				Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
			})
			require.NoError(t, err, "the healthy create")
			id := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(string(resp.Body))[1]
			etag := resp.Headers["ETag"]
			// An update must send the configuration as read, defaults included (#1271).
			cfgResp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "cloudfront", HTTPMethod: http.MethodGet, Path: "/2020-05-31/distribution/" + id + "/config",
				Headers: map[string]string{}, Params: map[string]string{},
			})
			require.NoError(t, err, "the healthy config read")
			readConfig := strings.TrimPrefix(string(cfgResp.Body), `<?xml version="1.0" encoding="UTF-8"?>`+"\n")

			tc.arm(fault)
			path := "/2020-05-31/distribution/" + id + tc.suffix
			if tc.suffix == "list" {
				path = "/2020-05-31/distribution"
			}
			body := ""
			headers := map[string]string{"Content-Type": "application/xml"}
			if tc.body != nil {
				body = tc.body(readConfig)
				headers["If-Match"] = etag
			}
			_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "cloudfront", HTTPMethod: tc.method, Path: path, Body: []byte(body), Headers: headers, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}

// DeleteDistribution removes the deleted distribution's countdown; a store fault doing so is an
// error, not a 204 over a counter left behind.
func TestCloudFrontProgression_ADeleteCountdownFaultIsAnError(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	p := cfOACPluginOn(t, fault)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-cf-prog-del", IDs: emulator.NewIDMint("req-cf-prog-del")}
	call := func(method, path, ifMatch, body string) (*emulator.AWSResponse, error) {
		headers := map[string]string{"Content-Type": "application/xml"}
		if ifMatch != "" {
			headers["If-Match"] = ifMatch
		}
		return p.HandleRequest(ctx, &emulator.AWSRequest{Service: "cloudfront", HTTPMethod: method, Path: path, Body: []byte(body), Headers: headers, Params: map[string]string{}})
	}
	resp, err := call(http.MethodPost, "/2020-05-31/distribution", "", cfProgConfig)
	require.NoError(t, err)
	id := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(string(resp.Body))[1]
	cfgResp, err := call(http.MethodGet, "/2020-05-31/distribution/"+id+"/config", "", "")
	require.NoError(t, err)
	cfg := strings.Replace(strings.TrimPrefix(string(cfgResp.Body), `<?xml version="1.0" encoding="UTF-8"?>`+"\n"),
		"<Enabled>true</Enabled>", "<Enabled>false</Enabled>", 1)
	upd, err := call(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", cfgResp.Headers["ETag"], cfg)
	require.NoError(t, err, "the healthy disable")

	fault.failDelete = "observed:"
	_, err = call(http.MethodDelete, "/2020-05-31/distribution/"+id, upd.Headers["ETag"], "")
	require.Error(t, err, "a countdown that cannot be removed fails the delete")
	var awsErr *emulator.AWSError
	require.Falsef(t, errors.As(err, &awsErr), "a store fault answered as the published %v", awsErr)
}
