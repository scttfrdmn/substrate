package emulator_test

import (
	"bytes"
	"crypto/md5" // nosemgrep
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// detEpoch is the instant the deterministic-run tests pin the clock at.
var detEpoch = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// detSeededRequestID is the shape a seeded [emulator.RequestIDSource] assigns.
var detSeededRequestID = regexp.MustCompile(`^req-[0-9a-f]{8}-[1-9][0-9]*$`)

// detExchange is one request and the answer it got.
type detExchange struct {
	name   string
	status int
	body   []byte
}

// detBodyRequestID extracts the RequestId a Query-protocol body renders. Substrate sets no
// request-ID header, so the body is where a caller reads it.
func detBodyRequestID(body []byte) string {
	m := regexp.MustCompile(`<RequestId>([^<]+)</RequestId>`).FindSubmatch(body)
	if m == nil {
		return ""
	}
	return string(m[1])
}

// detRecordedRequestIDs returns the request ID of every event the server recorded, in
// order: the top-level requests and the internal dispatches derived from them.
func detRecordedRequestIDs(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()
	events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{})
	require.NoError(t, err)
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.RequestID)
	}
	return out
}

// detClient issues raw AWS requests at one test server and keeps every answer, so two
// servers' transcripts can be compared byte for byte.
type detClient struct {
	t   *testing.T
	ts  *emulator.TestServer
	log []detExchange
}

func (c *detClient) do(name, method, host, path, contentType string, headers map[string]string, body []byte) []byte {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.ts.URL+path, bytes.NewReader(body))
	require.NoError(c.t, err)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(c.t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	require.NoError(c.t, resp.Body.Close())
	require.Less(c.t, resp.StatusCode, 300, "%s: %s", name, out)
	c.log = append(c.log, detExchange{name: name, status: resp.StatusCode, body: out})
	return out
}

func (c *detClient) query(host, version string, params map[string]string) []byte {
	c.t.Helper()
	form := url.Values{"Version": {version}}
	for k, v := range params {
		form.Set(k, v)
	}
	return c.do(params["Action"], http.MethodPost, host, "/", "application/x-www-form-urlencoded", nil,
		[]byte(form.Encode()))
}

func (c *detClient) json(host, target string, body any) []byte {
	c.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(c.t, err)
	return c.do(target, http.MethodPost, host, "/", "application/x-amz-json-1.1",
		map[string]string{"X-Amz-Target": target}, raw)
}

func (c *detClient) s3(name, method, path string, body []byte) []byte {
	c.t.Helper()
	return c.do(name, method, "s3.us-east-1.amazonaws.com", path, "", nil, body)
}

// detMD5Hex is the ETag S3 answers for a part: the hex MD5 of its bytes.
func detMD5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec // S3 ETags are MD5 by definition.
	return hex.EncodeToString(sum[:])
}

// detScript is the sequence of calls #1598's acceptance names: a VPC, an instance, a KMS
// key, an IAM user, a whole S3 multipart upload, an Organizations account and a Firehose
// record. CreateOrganization is not a routed operation, so the organization is created on
// its first observation, by DescribeOrganization, as substrate always creates it.
func detScript(c *detClient) {
	const ec2Host, ec2Version = "ec2.us-east-1.amazonaws.com", "2016-11-15"
	c.query(ec2Host, ec2Version, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.40.0.0/16"})
	c.query(ec2Host, ec2Version, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1",
	})
	c.json("kms.us-east-1.amazonaws.com", "TrentService.CreateKey", map[string]any{"Description": "det"})
	c.query("iam.amazonaws.com", "2010-05-08", map[string]string{"Action": "CreateUser", "UserName": "det-user"})

	c.s3("CreateBucket", http.MethodPut, "/det-bucket", nil)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(c.t, xml.Unmarshal(c.s3("CreateMultipartUpload", http.MethodPost, "/det-bucket/big.bin?uploads", nil), &initiated))
	require.NotEmpty(c.t, initiated.UploadID)
	part := strings.Repeat("p", 1024)
	c.s3("UploadPart", http.MethodPut, "/det-bucket/big.bin?partNumber=1&uploadId="+url.QueryEscape(initiated.UploadID), []byte(part))
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`,
		detMD5Hex(part))
	c.s3("CompleteMultipartUpload", http.MethodPost, "/det-bucket/big.bin?uploadId="+url.QueryEscape(initiated.UploadID), []byte(complete))

	const orgHost = "organizations.us-east-1.amazonaws.com"
	c.json(orgHost, "AWSOrganizationsV20161128.DescribeOrganization", map[string]any{})
	c.json(orgHost, "AWSOrganizationsV20161128.CreateAccount", map[string]any{
		"AccountName": "det-member", "Email": "det-member@example.com",
	})
	c.json(orgHost, "AWSOrganizationsV20161128.ListAccounts", map[string]any{})

	const firehoseHost = "firehose.us-east-1.amazonaws.com"
	c.json(firehoseHost, "Firehose_20150804.CreateDeliveryStream", map[string]any{"DeliveryStreamName": "det-stream"})
	c.json(firehoseHost, "Firehose_20150804.PutRecord", map[string]any{
		"DeliveryStreamName": "det-stream", "Record": map[string]any{"Data": "ZGV0"},
	})
	c.json(firehoseHost, "Firehose_20150804.PutRecordBatch", map[string]any{
		"DeliveryStreamName": "det-stream",
		"Records":            []map[string]any{{"Data": "YQ=="}, {"Data": "Yg=="}},
	})
}

// The acceptance test of #1598: two fresh servers in deterministic-run mode, given the same
// calls, answer byte-identical bodies and the same request IDs.
func TestDeterministicRun_TwoFreshServersAnswerByteIdenticalBodies(t *testing.T) {
	t.Parallel()

	run := func() ([]detExchange, []string) {
		c := &detClient{t: t, ts: emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch))}
		detScript(c)
		return c.log, detRecordedRequestIDs(t, c.ts)
	}
	first, firstIDs := run()
	second, secondIDs := run()

	require.Len(t, second, len(first))
	for i := range first {
		assert.Equal(t, first[i].status, second[i].status, "%s status", first[i].name)
		assert.Equal(t, string(first[i].body), string(second[i].body), "%s body", first[i].name)
	}
	assert.Equal(t, firstIDs, secondIDs, "the recorded request IDs agree too")
	seeded := 0
	for _, id := range firstIDs {
		if detSeededRequestID.MatchString(id) {
			seeded++
		}
	}
	assert.Equal(t, len(first), seeded, "one seeded request ID per top-level request: %v", firstIDs)

	// The bodies render the pinned epoch, not the wall clock.
	var created struct {
		KeyMetadata struct {
			CreationDate float64 `json:"CreationDate"`
		} `json:"KeyMetadata"`
	}
	require.NoError(t, json.Unmarshal(first[2].body, &created))
	assert.Equal(t, float64(detEpoch.Unix()), created.KeyMetadata.CreationDate, "KMS CreationDate is the epoch")
}

// Seeding alone, on a running clock, already makes the minted identifiers repeat: they
// derive from the request ID, not from the time.
func TestDeterministicRun_SeededIDsRepeatOnARunningClock(t *testing.T) {
	t.Parallel()

	createVPC := func(ts *emulator.TestServer) string {
		c := &detClient{t: t, ts: ts}
		var out struct {
			VpcID string `xml:"vpc>vpcId"`
		}
		require.NoError(t, xml.Unmarshal(c.query("ec2.us-east-1.amazonaws.com", "2016-11-15",
			map[string]string{"Action": "CreateVpc", "CidrBlock": "10.41.0.0/16"}), &out))
		require.NotEmpty(t, out.VpcID)
		return out.VpcID
	}
	assert.Equal(t, createVPC(emulator.StartTestServer(t)), createVPC(emulator.StartTestServer(t)),
		"a default test server is seeded, so two servers mint one VPC ID")
	assert.NotEqual(t, createVPC(emulator.StartTestServer(t, emulator.WithoutSeededIDs())),
		createVPC(emulator.StartTestServer(t, emulator.WithoutSeededIDs())),
		"WithoutSeededIDs restores per-server identifiers")
}

func TestDeterministicRun_UnseededServersKeepTheWallClockRequestIDFormat(t *testing.T) {
	t.Parallel()
	c := &detClient{t: t, ts: emulator.StartTestServer(t, emulator.WithoutSeededIDs())}
	c.query("iam.amazonaws.com", "2010-05-08", map[string]string{"Action": "ListUsers"})
	assert.Regexp(t, `^req-[0-9]+-[0-9a-f]{8}$`, detBodyRequestID(c.log[0].body))
	assert.False(t, c.ts.RequestIDs().Seeded())
}

// ResetState rewinds the request IDs and returns a pinned clock to its epoch, so a test
// case sharing a server answers what a fresh one would.
func TestDeterministicRun_ResetRewindsTheSequenceAndRepinsTheClock(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch))

	first := &detClient{t: t, ts: ts}
	detScript(first)
	ts.AdvanceTime(3 * time.Hour)
	require.Equal(t, detEpoch.Add(3*time.Hour), ts.TimeController().Now())

	ts.ResetState(t)
	assert.Equal(t, detEpoch, ts.TimeController().Now(), "the clock is back at the epoch")
	assert.True(t, ts.TimeFrozen(), "and still pinned")
	assert.Zero(t, ts.RequestIDs().Drawn(), "the sequence is rewound")

	second := &detClient{t: t, ts: ts}
	detScript(second)
	require.Len(t, second.log, len(first.log))
	for i := range first.log {
		assert.Equal(t, string(first.log[i].body), string(second.log[i].body), "%s body", first.log[i].name)
	}
}

// A replay dispatches every event under its recorded request ID, so it draws nothing from
// the live server's sequence.
func TestDeterministicRun_AReplayDoesNotAdvanceTheLiveSequence(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch), emulator.WithRecordedBodies())
	c := &detClient{t: t, ts: ts}
	c.query("iam.amazonaws.com", "2010-05-08", map[string]string{"Action": "CreateUser", "UserName": "replayed"})
	drawn := ts.RequestIDs().Drawn()
	require.EqualValues(t, 1, drawn)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Equal(t, results.TotalEvents, results.SuccessEvents, "%s", replayDifferenceSummary(results))
	assert.Equal(t, drawn, ts.RequestIDs().Drawn())
}

// The acceptance test of #1600: a recorded multipart upload replays with zero differences.
// Before it, the replayed CreateMultipartUpload minted a new UploadId from the wall clock
// and the recorded UploadPart answered NoSuchUpload.
func TestDeterministicRun_AMultipartUploadReplaysWithZeroDifferences(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	ts.FreezeTime()

	c := &detClient{t: t, ts: ts}
	c.s3("CreateBucket", http.MethodPut, "/mpu-replay", nil)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(c.s3("CreateMultipartUpload", http.MethodPost, "/mpu-replay/k?uploads", nil), &initiated))
	assert.Regexp(t, `^mpu-[0-9a-f]{32}$`, initiated.UploadID)
	part := strings.Repeat("x", 512)
	c.s3("UploadPart", http.MethodPut, "/mpu-replay/k?partNumber=1&uploadId="+initiated.UploadID, []byte(part))
	c.s3("CompleteMultipartUpload", http.MethodPost, "/mpu-replay/k?uploadId="+initiated.UploadID,
		[]byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`, detMD5Hex(part))))

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid, "%v", results.StateErrors)
}

// Two Firehose records put on a frozen clock got one RecordId each from the clock's
// nanoseconds, so they collided; they are minted now.
func TestDeterministicRun_FirehoseRecordIDsAreDistinctOnAFrozenClock(t *testing.T) {
	t.Parallel()
	c := &detClient{t: t, ts: emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch))}
	const host = "firehose.us-east-1.amazonaws.com"
	c.json(host, "Firehose_20150804.CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s"})
	seen := map[string]bool{}
	for range 2 {
		var out struct {
			RecordID string `json:"RecordId"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "Firehose_20150804.PutRecord", map[string]any{
			"DeliveryStreamName": "s", "Record": map[string]any{"Data": "YQ=="},
		}), &out))
		seen[out.RecordID] = true
	}
	var batch struct {
		RequestResponses []struct {
			RecordID string `json:"RecordId"`
		} `json:"RequestResponses"`
	}
	require.NoError(t, json.Unmarshal(c.json(host, "Firehose_20150804.PutRecordBatch", map[string]any{
		"DeliveryStreamName": "s", "Records": []map[string]any{{"Data": "YQ=="}, {"Data": "Yg=="}},
	}), &batch))
	for _, r := range batch.RequestResponses {
		seen[r.RecordID] = true
	}
	assert.Len(t, seen, 4, "four records, four RecordIds: %v", seen)
}

// Organizations identifiers come from the request's mint in the formats the API reference
// publishes, so they repeat across seeded runs.
func TestDeterministicRun_OrganizationsIDsAreMintedInThePublishedFormats(t *testing.T) {
	t.Parallel()
	const host = "organizations.us-east-1.amazonaws.com"
	type ids struct{ org, root, ou, policy, account, car string }
	collect := func() ids {
		c := &detClient{t: t, ts: emulator.StartTestServer(t)}
		var org struct {
			Organization struct {
				ID string `json:"Id"`
			} `json:"Organization"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.DescribeOrganization", map[string]any{}), &org))
		var roots struct {
			Roots []struct {
				ID string `json:"Id"`
			} `json:"Roots"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.ListRoots", map[string]any{}), &roots))
		require.Len(t, roots.Roots, 1)
		var ou struct {
			OrganizationalUnit struct {
				ID string `json:"Id"`
			} `json:"OrganizationalUnit"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.CreateOrganizationalUnit",
			map[string]any{"ParentId": roots.Roots[0].ID, "Name": "det-ou"}), &ou))
		var pol struct {
			Policy struct {
				PolicySummary struct {
					ID string `json:"Id"`
				} `json:"PolicySummary"`
			} `json:"Policy"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.CreatePolicy", map[string]any{
			"Name": "det-scp", "Description": "d", "Type": "SERVICE_CONTROL_POLICY",
			"Content": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		}), &pol))
		var car struct {
			CreateAccountStatus struct {
				ID string `json:"Id"`
			} `json:"CreateAccountStatus"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.CreateAccount", map[string]any{
			"AccountName": "det", "Email": "det@example.com",
		}), &car))
		var status struct {
			CreateAccountStatus struct {
				AccountID string `json:"AccountId"`
			} `json:"CreateAccountStatus"`
		}
		require.NoError(t, json.Unmarshal(c.json(host, "AWSOrganizationsV20161128.DescribeCreateAccountStatus",
			map[string]any{"CreateAccountRequestId": car.CreateAccountStatus.ID}), &status))
		return ids{
			org: org.Organization.ID, root: roots.Roots[0].ID, ou: ou.OrganizationalUnit.ID,
			policy: pol.Policy.PolicySummary.ID, account: status.CreateAccountStatus.AccountID,
			car: car.CreateAccountStatus.ID,
		}
	}
	first := collect()
	assert.Equal(t, first, collect(), "two seeded servers mint the same Organizations IDs")

	// Patterns from the AWS Organizations API reference.
	assert.Regexp(t, `^o-[a-z0-9]{10,32}$`, first.org)
	assert.Regexp(t, `^r-[0-9a-z]{4,32}$`, first.root)
	assert.Regexp(t, `^ou-[0-9a-z]{4,32}-[a-z0-9]{8,32}$`, first.ou)
	assert.True(t, strings.HasPrefix(first.ou, "ou-"+first.root[2:]+"-"), "an OU ID carries its root's suffix: %s", first.ou)
	assert.Regexp(t, `^p-[0-9a-zA-Z_]{8,128}$`, first.policy)
	assert.Regexp(t, `^\d{12}$`, first.account)
	assert.Regexp(t, `^car-[a-z0-9]{8,32}$`, first.car)
}

// GET and POST /v1/control/run report and change the seed and the epoch over HTTP.
func TestDeterministicRun_ControlRunEndpoint(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithoutSeededIDs())

	type runState struct {
		Seed  int64  `json:"seed"`
		Epoch string `json:"epoch"`
		Drawn uint64 `json:"drawn"`
	}
	call := func(method, body string) (int, runState) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+"/v1/control/run", strings.NewReader(body))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		var out runState
		if resp.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		}
		return resp.StatusCode, out
	}

	status, got := call(http.MethodGet, "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, runState{}, got, "unseeded and unpinned")

	status, got = call(http.MethodPost, `{"seed":42,"epoch":"2026-01-02T03:04:05Z"}`)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, runState{Seed: 42, Epoch: "2026-01-02T03:04:05Z"}, got)
	assert.True(t, ts.TimeFrozen())
	assert.Equal(t, detEpoch, ts.TimeController().Now())

	c := &detClient{t: t, ts: ts}
	c.query("iam.amazonaws.com", "2010-05-08", map[string]string{"Action": "ListUsers"})
	rid := detBodyRequestID(c.log[0].body)
	assert.Regexp(t, detSeededRequestID, rid)
	assert.True(t, strings.HasSuffix(rid, "-1"), "the first of the new sequence: %s", rid)
	_, got = call(http.MethodGet, "")
	assert.EqualValues(t, 1, got.Drawn)

	for _, bad := range []string{`{"epoch":"yesterday"}`, `not json`} {
		status, _ = call(http.MethodPost, bad)
		assert.Equal(t, http.StatusBadRequest, status, bad)
	}
	_, got = call(http.MethodGet, "")
	assert.Equal(t, "2026-01-02T03:04:05Z", got.Epoch, "a refused change leaves the run as it was")

	status, got = call(http.MethodPost, `{"epoch":""}`)
	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, got.Epoch)
	assert.EqualValues(t, 42, got.Seed, "an absent seed is left alone")
	assert.False(t, ts.TimeFrozen(), "the empty epoch unpins the clock")

	status, got = call(http.MethodPost, `{"seed":0}`)
	require.Equal(t, http.StatusOK, status)
	assert.Zero(t, got.Seed)
	assert.False(t, ts.RequestIDs().Seeded())
}

// Pinning, unpinning and resetting concurrently never leaves the run reporting an epoch for a
// running clock, or no epoch for a clock one of them froze: the flag and the clock change
// under one lock.
func TestDeterministicRun_ConcurrentPinUnpinAndResetStayConsistent(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	type runState struct {
		Epoch  string `json:"epoch"`
		Frozen bool   `json:"frozen"`
	}
	post := func(path, body string) {
		resp, err := http.Post(ts.URL+path, "application/json", strings.NewReader(body)) //nolint:noctx
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	get := func() runState {
		resp, err := http.Get(ts.URL + "/v1/control/run") //nolint:noctx
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		var out runState
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		return out
	}

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for range 25 {
				switch i {
				case 0:
					post("/v1/control/run", `{"epoch":"2026-01-02T03:04:05Z"}`)
				case 1:
					post("/v1/control/run", `{"epoch":""}`)
				case 2:
					post("/v1/state/reset", "")
				default:
					post("/v1/control/time", `{"frozen":false}`)
				}
			}
		})
	}
	var inconsistent []runState
	var mu sync.Mutex
	wg.Go(func() {
		for range 50 {
			got := get()
			if (got.Epoch != "") != got.Frozen {
				mu.Lock()
				inconsistent = append(inconsistent, got)
				mu.Unlock()
			}
		}
	})
	wg.Wait()

	assert.Empty(t, inconsistent, "an epoch is reported exactly when the clock is frozen")
	final := get()
	assert.Equal(t, final.Frozen, ts.TimeFrozen())
	assert.Equal(t, final.Epoch != "", ts.TimeFrozen())
}

// Unfreezing a pinned clock, over HTTP or in process, unpins it, so a reset leaves it running.
func TestDeterministicRun_UnfreezingAPinnedClockUnpinsIt(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"http", "in-process"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch))
			if how == "http" {
				resp, err := http.Post(ts.URL+"/v1/control/time", "application/json", //nolint:noctx
					strings.NewReader(`{"frozen":false}`))
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)
			} else {
				ts.UnfreezeTime()
			}
			ts.ResetState(t)
			assert.False(t, ts.TimeFrozen(), "the reset does not re-pin an unpinned clock")

			resp, err := http.Get(ts.URL + "/v1/control/run") //nolint:noctx
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck
			var got struct {
				Epoch  string `json:"epoch"`
				Frozen bool   `json:"frozen"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
			assert.Empty(t, got.Epoch)
			assert.False(t, got.Frozen)
		})
	}
}

// POST /v1/control/time freezes and unfreezes the clock, the HTTP form of FreezeTimeAt.
func TestDeterministicRun_ControlTimeFreezes(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	type timeState struct {
		SimulatedTime string `json:"simulated_time"`
		Frozen        bool   `json:"frozen"`
	}
	post := func(body string) (int, timeState) {
		t.Helper()
		resp, err := http.Post(ts.URL+"/v1/control/time", "application/json", strings.NewReader(body)) //nolint:noctx
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		var out timeState
		if resp.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		}
		return resp.StatusCode, out
	}

	status, got := post(`{"time":"2026-01-02T03:04:05Z","frozen":true}`)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, timeState{SimulatedTime: "2026-01-02T03:04:05Z", Frozen: true}, got)
	assert.Equal(t, detEpoch, ts.TimeController().Now(), "frozen at exactly the time given")

	status, got = post(`{"time":"2026-01-02T04:04:05Z"}`)
	require.Equal(t, http.StatusOK, status)
	assert.True(t, got.Frozen, "moving a frozen clock leaves it frozen")

	status, got = post(`{"frozen":false}`)
	require.Equal(t, http.StatusOK, status)
	assert.False(t, got.Frozen)
	assert.False(t, ts.TimeFrozen())

	status, _ = post(`{}`)
	assert.Equal(t, http.StatusBadRequest, status, "a body naming neither field changes nothing")
}

func TestRequestIDSource(t *testing.T) {
	t.Parallel()

	t.Run("nil and unseeded sources assign the wall-clock format", func(t *testing.T) {
		t.Parallel()
		var nilSource *emulator.RequestIDSource
		for _, s := range []*emulator.RequestIDSource{nilSource, emulator.NewRequestIDSource(0)} {
			a, b := s.Next(), s.Next()
			assert.Regexp(t, `^req-[0-9]+-[0-9a-f]{8}$`, a)
			assert.NotEqual(t, a, b)
			assert.False(t, s.Seeded())
			assert.Zero(t, s.Drawn())
			s.Rewind()
		}
	})

	t.Run("a seed fixes the sequence and a reseed or rewind restarts it", func(t *testing.T) {
		t.Parallel()
		a, b := emulator.NewRequestIDSource(7), emulator.NewRequestIDSource(7)
		first := []string{a.Next(), a.Next()}
		assert.Equal(t, first, []string{b.Next(), b.Next()})
		assert.Regexp(t, detSeededRequestID, first[0])
		assert.True(t, strings.HasSuffix(first[0], "-1") && strings.HasSuffix(first[1], "-2"), "%v", first)
		assert.EqualValues(t, 2, a.Drawn())

		a.Rewind()
		assert.Equal(t, first[0], a.Next())

		other := emulator.NewRequestIDSource(8)
		assert.NotEqual(t, first[0], other.Next(), "another seed, another prefix")
		other.SetSeed(7)
		assert.Equal(t, first[0], other.Next(), "reseeding restarts the sequence")
		assert.EqualValues(t, 7, other.Seed())

		for _, reserved := range []string{"req-clock-", "req-cfn-", "req-apigw-"} {
			assert.False(t, strings.HasPrefix(first[0], reserved), "%s avoids %s", first[0], reserved)
		}
	})

	t.Run("concurrent draws are distinct", func(t *testing.T) {
		t.Parallel()
		s := emulator.NewRequestIDSource(3)
		var mu sync.Mutex
		seen := map[string]bool{}
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				for range 50 {
					id := s.Next()
					mu.Lock()
					seen[id] = true
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		assert.Len(t, seen, 800)
		assert.EqualValues(t, 800, s.Drawn())
	})
}

func TestRunCfg_NewTimeController(t *testing.T) {
	t.Parallel()

	tc, err := emulator.RunCfg{Epoch: "2026-01-02T03:04:05Z"}.NewTimeController()
	require.NoError(t, err)
	assert.True(t, tc.Frozen())
	assert.Equal(t, detEpoch, tc.Now())

	tc, err = emulator.RunCfg{}.NewTimeController()
	require.NoError(t, err)
	assert.False(t, tc.Frozen())

	_, err = emulator.RunCfg{Epoch: "soon"}.NewTimeController()
	assert.Error(t, err)
}

// A Client given a server's source seeds its deployments from the same sequence.
func TestDeterministicRun_ClientDeployDrawsFromTheGivenSource(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithDeterministicRun(detEpoch))
	client := emulator.NewClient(ts.Registry(), ts.Store(), ts.StateManager(), ts.TimeController(),
		emulator.NewDefaultLogger(0, false), emulator.WithClientRequestIDs(ts.RequestIDs()))
	first, err := client.Deploy(t.Context(), `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"det-q"}}}}`,
		emulator.Intent{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, ts.RequestIDs().Drawn(), "the deployment drew one request ID")

	// The stream is named from that request ID, not the clock's nanoseconds, so two
	// deployments on the frozen clock record into two streams.
	second, err := client.Deploy(t.Context(), `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"det-q2"}}}}`,
		emulator.Intent{})
	require.NoError(t, err)
	assert.Regexp(t, `^deploy-[0-9a-f]{8}-1$`, first.StreamID)
	assert.NotEqual(t, first.StreamID, second.StreamID)

	// A dispatched resource call's Duration is its wall latency, not the distance between
	// the pinned epoch and the wall clock.
	events, err := ts.Store().GetStream(t.Context(), first.StreamID)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	for _, e := range events {
		assert.Less(t, e.Duration, time.Minute, "%s %s", e.Service, e.Operation)
	}
}

// A proxy event's requestContext.requestId is a UUID minted from the request that reached
// the integration, so a replay of that request builds the payload it recorded (#1600).
func TestAPIGatewayProxyEvent_RequestIDDerivesFromTheRequest(t *testing.T) {
	t.Parallel()
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for _, v2 := range []bool{false, true} {
		requestID := func(id string) string {
			t.Helper()
			req := &emulator.AWSRequest{Operation: "GET", Headers: map[string]string{}}
			raw, err := emulator.BuildProxyEventFromRequestIDForTest(id, v2, req, "api", "prod", "/r")
			require.NoError(t, err)
			var event struct {
				RequestContext struct {
					RequestID string `json:"requestId"`
				} `json:"requestContext"`
			}
			require.NoError(t, json.Unmarshal(raw, &event))
			return event.RequestContext.RequestID
		}
		first := requestID("req-a")
		assert.Regexp(t, uuid, first, "v2=%v", v2)
		assert.Equal(t, first, requestID("req-a"), "one request, one requestId (v2=%v)", v2)
		assert.NotEqual(t, first, requestID("req-b"), "two requests, two requestIds (v2=%v)", v2)
	}
}

// The invocation's request ID stays the mint's first draw, as it was before the event's
// requestId was minted at all, so a recording made before #1600 replays its invocation under
// the ID it recorded. The event's requestId is the second draw.
func TestAPIGatewayProxyEvent_TheInvocationIDKeepsTheFirstDraw(t *testing.T) {
	t.Parallel()
	invoke, event := emulator.APIGatewayProxyRequestIDsForTest("req-recorded")
	assert.Equal(t, emulator.APIGatewayInternalRequestIDForTest("req-recorded"), invoke,
		"the invocation ID is a fresh mint's first draw")
	assert.Equal(t, emulator.IDMintUUIDAtDrawForTest("req-recorded", 2), event,
		"the event's requestId is its second")
	assert.NotEqual(t, emulator.IDMintUUIDAtDrawForTest("req-recorded", 1), event)
}

// A credit's expiry follows the simulated clock when one is wired, and credits are numbered
// rather than stamped with the wall clock, so two runs name them the same (#1600).
func TestCostController_CreditsFollowTheSimulatedClock(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(detEpoch)
	tc.Freeze()
	tc.SetTime(detEpoch)
	costs := emulator.NewCostController(emulator.CostConfig{
		Enabled: true, Overrides: map[string]float64{"s3/PutObject": 1},
	}, emulator.WithCostTimeController(tc))

	assert.Equal(t, "credit-1", costs.AddCredit(emulator.Credit{ID: "credit-1", Amount: 1}), "a caller's own ID is kept")
	assert.Equal(t, "credit-2", costs.AddCredit(emulator.Credit{Amount: 10, ExpiresAt: detEpoch.Add(time.Hour)}),
		"and a minted one skips it")
	require.True(t, costs.RemoveCredit("credit-1"))

	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	assert.Zero(t, costs.CostForRequestWithCredits(req), "the credit applies before its simulated expiry")
	tc.SetTime(detEpoch.Add(2 * time.Hour))
	assert.InDelta(t, 1.0, costs.CostForRequestWithCredits(req), 1e-9, "and not after it")

	// With no clock wired, expiry is judged against the wall clock, as it always was.
	wall := emulator.NewCostController(emulator.CostConfig{
		Enabled: true, Overrides: map[string]float64{"s3/PutObject": 1},
	})
	wall.AddCredit(emulator.Credit{Amount: 10, ExpiresAt: detEpoch.Add(time.Hour)})
	assert.InDelta(t, 1.0, wall.CostForRequestWithCredits(req), 1e-9, "a 2026-01-02 expiry has passed on the wall clock")
}

// run.seed and run.epoch load from SUBSTRATE_RUN_SEED and SUBSTRATE_RUN_EPOCH, which only
// works because their viper defaults are registered.
func TestLoadConfig_RunSectionFromTheEnvironment(t *testing.T) {
	t.Setenv("SUBSTRATE_RUN_SEED", "42")
	t.Setenv("SUBSTRATE_RUN_EPOCH", "2026-01-02T03:04:05Z")

	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.EqualValues(t, 42, cfg.Run.Seed)
	assert.Equal(t, "2026-01-02T03:04:05Z", cfg.Run.Epoch)

	epoch, ok, err := cfg.Run.ParseEpoch()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, detEpoch, epoch)
}

func TestLoadConfig_RefusesAnUnparseableRunEpoch(t *testing.T) {
	t.Setenv("SUBSTRATE_RUN_EPOCH", "tomorrow")
	_, err := emulator.LoadConfig("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "run.epoch")
}

func TestDefaultConfig_RunIsUnseededAndUnpinned(t *testing.T) {
	t.Parallel()
	cfg := emulator.DefaultConfig()
	assert.Zero(t, cfg.Run.Seed)
	assert.Empty(t, cfg.Run.Epoch)
}

// A server built from a config whose run section pins an epoch starts frozen there, and a
// state reset returns it there.
func TestNewServer_PinsTheClockAtRunEpoch(t *testing.T) {
	t.Parallel()
	cfg := emulator.DefaultConfig()
	cfg.Run = emulator.RunCfg{Seed: 5, Epoch: "2026-01-02T03:04:05Z"}
	tc := emulator.NewTimeController(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	srv := emulator.NewServer(*cfg, emulator.NewPluginRegistry(), emulator.NewEventStore(emulator.EventStoreConfig{}),
		emulator.NewMemoryStateManager(), tc, emulator.NewDefaultLogger(0, false))
	assert.True(t, tc.Frozen())
	assert.Equal(t, detEpoch, tc.Now())
	assert.EqualValues(t, 5, srv.RequestIDs().Seed())

	tc.SetTime(detEpoch.Add(time.Hour))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/state/reset", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, detEpoch, tc.Now())

	// An unusable epoch on a hand-built config is logged and ignored rather than fatal.
	cfg.Run.Epoch = "never"
	other := emulator.NewTimeController(detEpoch)
	emulator.NewServer(*cfg, emulator.NewPluginRegistry(), emulator.NewEventStore(emulator.EventStoreConfig{}),
		emulator.NewMemoryStateManager(), other, emulator.NewDefaultLogger(0, false))
	assert.False(t, other.Frozen())
}
