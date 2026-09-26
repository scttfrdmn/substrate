package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #856: an identifier a replay mints is the one its recording minted.
//
// Substrate drew its identifiers from crypto/rand, so a replayed create answered with a new
// one and every later request naming the old one diverged. The assertions here are in two
// layers. The mint's own properties are asserted directly, because they are the reasons the
// source was chosen over hashing the request or seeding a per-plugin PRNG. Everything else is
// asserted through the wire — a replayed stream containing four creates, with body comparison
// and state-hash validation both on — because that is the observation a consumer makes, and
// the one that could not be made at all before this change (#1140's tests had to work around
// it).

// idsUUIDv4Shape matches the form AWS publishes for an identifier it documents as a UUID:
// 8-4-4-4-12 lowercase hex with the version nibble 4 and the variant bits set.
var idsUUIDv4Shape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// TestIDMint_TwoMintsOverOneSeedAgree is the property a replay rests on: the mint holds no
// state beyond its seed and its ordinal, so a second mint over the same seed reproduces the
// first mint's sequence draw for draw — in this process or any other.
//
// This is what a per-plugin seeded PRNG cannot give. Its seed is per process, which
// [emulator.OmicsPlugin]'s own doc comment says, and the sequence it produces depends on
// which goroutine won a mutex.
func TestIDMint_TwoMintsOverOneSeedAgree(t *testing.T) {
	t.Parallel()

	first := emulator.NewIDMint("req-1758326400000000000-0a1b2c3d")
	second := emulator.NewIDMint("req-1758326400000000000-0a1b2c3d")

	for draw := range 4 {
		assert.Equal(t, first.Hex(8), second.Hex(8), "draw %d", draw)
	}
	assert.True(t, first.Derived(), "a mint over a request id derives its identifiers")
}

// TestIDMint_EachDrawDiffers covers the ordinal, which is what makes one request able to
// publish more than one identifier — a RunInstances with MaxCount above one, or the two
// halves of an access key.
func TestIDMint_EachDrawDiffers(t *testing.T) {
	t.Parallel()

	m := emulator.NewIDMint("req-ordinal")
	seen := map[string]bool{}
	for range 64 {
		id := m.Hex(8)
		require.False(t, seen[id], "draw %s repeated an earlier one", id)
		seen[id] = true
	}
}

// TestIDMint_TwoSeedsDisagree is the other half of the same property, and the reason the
// source is the request *id* rather than the request: two CreateAccessKey calls for one user
// are byte-identical requests, so hashing the request would mint one key twice.
func TestIDMint_TwoSeedsDisagree(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		emulator.NewIDMint("req-a").Hex(8),
		emulator.NewIDMint("req-b").Hex(8),
	)
}

// TestIDMint_ShapesAreWhatEachSitePublishes pins the widths and alphabets, because a mint
// site's own test asserts the prefix and the total length and would pass on a value of the
// wrong width if the helper padded.
func TestIDMint_ShapesAreWhatEachSitePublishes(t *testing.T) {
	t.Parallel()

	m := emulator.NewIDMint("req-shapes")

	assert.Len(t, m.Hex(8), 16, "Hex is two characters per byte")
	assert.Regexp(t, `^[0-9a-f]+$`, m.Hex(8), "lowercase, as every AWS identifier renders hex")

	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	chars := m.Chars(17, alphabet)
	assert.Len(t, chars, 17)
	for _, c := range chars {
		assert.Contains(t, alphabet, string(c), "%q is outside the alphabet", c)
	}

	digits := m.Digits(12)
	assert.Len(t, digits, 12)
	assert.Regexp(t, `^[0-9]+$`, digits)

	assert.Regexp(t, idsUUIDv4Shape, m.UUID())

	raw, err := base64.StdEncoding.DecodeString(m.Base64(30))
	require.NoError(t, err)
	assert.Len(t, raw, 30, "Base64 encodes exactly the bytes asked for")

	assert.Empty(t, m.Hex(0), "a zero width is an empty identifier, not a panic")
	assert.Empty(t, m.Chars(-1, alphabet))
}

// TestIDMint_AWiderIdentifierExtendsPastOneDigest covers the counter-mode extension, which
// exists for STS's 96-byte session token — the one current caller past one SHA-256 digest.
//
// The assertion is that the tail is not a repeat of the head, which is what a naive
// implementation produces: HMAC over the ordinal alone yields the same 32 bytes for every
// block, so a 96-byte draw would be one digest three times over.
func TestIDMint_AWiderIdentifierExtendsPastOneDigest(t *testing.T) {
	t.Parallel()

	raw, err := base64.StdEncoding.DecodeString(emulator.NewIDMint("req-wide").Base64(96))
	require.NoError(t, err)
	require.Len(t, raw, 96)

	assert.NotEqual(t, raw[0:32], raw[32:64], "the second block is not the first repeated")
	assert.NotEqual(t, raw[32:64], raw[64:96], "nor the third the second")
}

// TestIDMint_ASeedlessMintDrawsFromCryptoRand pins the documented fallback, which is what a
// mint site reached outside a request keeps — the CloudFormation deployer's internal request
// contexts, and the debug UI's. It is the thing later tiers remove by threading a real
// request id into those contexts; until then the behavior has to be today's, because minting
// a constant would collide two resources onto one identifier.
func TestIDMint_ASeedlessMintDrawsFromCryptoRand(t *testing.T) {
	t.Parallel()

	var nilMint *emulator.IDMint
	assert.False(t, nilMint.Derived(), "a nil mint derives nothing")
	assert.False(t, emulator.NewIDMint("").Derived(), "nor does a mint with no request id")

	// Well-formed all the same: a handler holding one must still be able to answer.
	assert.Len(t, nilMint.Hex(8), 16)
	assert.Regexp(t, idsUUIDv4Shape, nilMint.UUID())

	assert.NotEqual(t, emulator.NewIDMint("").Hex(16), emulator.NewIDMint("").Hex(16),
		"two seedless mints draw independently, so nothing collides while the fallback stands")
}

// TestIDs_TwoAccessKeysForOneUserDiffer is #856's AC3 at the smallest site that has it: two
// byte-identical requests, which is the case that rules out deriving from the request.
//
// It matters more here than anywhere else in the tier. An access key is what
// resolvePrincipal looks a caller up by, so a key minted twice — or minted differently on
// replay — leaves a replayed Authorization header naming nothing in state, and CheckAccess
// fails open (#833).
func TestIDs_TwoAccessKeysForOneUserDiffer(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	pipelineIAMCall(t, ts, "CreateUser", map[string]string{"UserName": "ids-two-keys"})
	first := idsAccessKey(t, ts, "ids-two-keys")
	second := idsAccessKey(t, ts, "ids-two-keys")

	assert.NotEqual(t, first.id, second.id, "two CreateAccessKey calls mint two keys")
	assert.NotEqual(t, first.secret, second.secret, "and two secrets")
	for _, id := range []string{first.id, second.id} {
		assert.True(t, strings.HasPrefix(id, "AKIA"), "%s", id)
		assert.Len(t, id, 21, "%s", id)
	}
}

// TestIDs_OneRunInstancesMintsDistinctIdentifiers is the ordinal's real test: three
// instances and six volumes come out of a *single* request, so a mint that ignored its
// ordinal would publish one instance three times.
func TestIDs_OneRunInstancesMintsDistinctIdentifiers(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	instances := idsRunInstances(t, ts, 3)
	require.Len(t, instances, 3)
	assert.Len(t, idsUnique(instances), 3, "three instance ids, not one repeated: %v", instances)

	volumes := idsVolumeIDs(t, ts)
	require.Len(t, volumes, 6, "a root volume and the declared /dev/sdf for each instance")
	assert.Len(t, idsUnique(volumes), 6, "every launch-created volume has its own id: %v", volumes)
}

// TestReplay_ACreateReplaysWithTheIdentifiersItMinted is #856's AC4, and the assertion
// substrate could not make before it.
//
// The stream is four creates that *name each other* — a subnet inside the recorded VPC, an
// instance inside the recorded subnet, an access key on the recorded user — so a single
// re-minted identifier is not a cosmetic difference in one response: the request that names
// it answers `InvalidVpcID.NotFound` or `NoSuchEntity`, and every describe after it reports
// an absent item.
//
// Both comparisons are switched on deliberately. [emulator.WithRecordedBodies] is what makes
// the replayed response bytes comparable at all, and
// [emulator.WithRecordedStateHashes] with `ValidateState` is what makes the *state* the
// replay reached comparable — which is the half that was reporting a mismatch for every
// stream containing a create, correctly, and so made state validation useless.
func TestReplay_ACreateReplaysWithTheIdentifiersItMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordInterlockedCreates(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"a replayed create mints the identifier its recording minted: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// TestReplay_ACreateReplaysTheSameWayTwice covers the property a regression fixture needs:
// replaying is not a one-shot reproduction that a second attempt perturbs.
//
// Each replay opens by resetting state, so the second one starts where the first did; if the
// mint drew from anywhere but the recorded request id, the second replay would diverge where
// the first did not.
func TestReplay_ACreateReplaysTheSameWayTwice(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	idsRecordInterlockedCreates(t, ts)

	engine := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true})
	for attempt := range 2 {
		results, err := engine.Replay(t.Context(), replayStreamID)
		require.NoError(t, err)
		assert.Empty(t, results.Differences, "attempt %d: %s", attempt,
			replayDifferenceSummary(results))
		assert.True(t, results.StateValid, "attempt %d: %v", attempt, results.StateErrors)
	}
}

// TestReplay_ACreateReplaysTheSameWayInAnotherServer is the cross-process half — the one
// thing a per-plugin seeded PRNG explicitly cannot give, since its seed is per process.
//
// The stream is copied into a second server's store and replayed against that server's own
// empty state manager, plugins and clock. Nothing of the first server reaches it except the
// events, which is what an exported regression fixture is.
func TestReplay_ACreateReplaysTheSameWayInAnotherServer(t *testing.T) {
	t.Parallel()
	recorder := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	idsRecordInterlockedCreates(t, recorder)

	recorded, err := recorder.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.NotEmpty(t, recorded)

	replayer := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	for _, event := range recorded {
		require.NoError(t, replayer.Store().RecordEvent(t.Context(), event))
	}

	results, err := replayEngineFor(replayer, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences,
		"a stream carries its identifiers with it: %s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid, "%v", results.StateErrors)
}

// idsRecordInterlockedCreates records four creates that name each other, which is the stream
// the replay assertions are about. Each step requires the previous one's minted identifier,
// so a re-minted one turns the next request into an error rather than into a different
// response.
func idsRecordInterlockedCreates(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// A frozen clock, so the only thing the assertions can be about is the identifiers.
	// A replay sets the clock to the recorded event's own timestamp, and a record whose
	// handler reads an unfrozen clock stamps its record a few hundred nanoseconds *after*
	// that — invisible in a response that renders seconds, visible in a state hash taken
	// over a JSON time.Time. That divergence is a separate defect from #856's, and pinning
	// the clock is what CLAUDE.md asks of any test either way.
	ts.FreezeTime()

	var vpc struct {
		VpcID string `xml:"vpc>vpcId"`
	}
	require.NoError(t, xml.Unmarshal(idsEC2Call(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16",
	}), &vpc))
	require.NotEmpty(t, vpc.VpcID)

	var subnet struct {
		SubnetID string `xml:"subnet>subnetId"`
	}
	require.NoError(t, xml.Unmarshal(idsEC2Call(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc.VpcID, "CidrBlock": "10.0.1.0/24",
	}), &subnet))
	require.NotEmpty(t, subnet.SubnetID)

	var run struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(idsEC2Call(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1", "SubnetId": subnet.SubnetID,
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "10",
	}), &run))
	require.Len(t, run.Instances, 1)

	// A describe naming the minted identifier, so the stream asserts more than that four
	// responses match: a re-minted instance id makes this one report an empty set.
	idsEC2Call(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": run.Instances[0].InstanceID,
	})

	pipelineIAMCall(t, ts, "CreateUser", map[string]string{"UserName": "ids-replay"})
	key := idsAccessKey(t, ts, "ids-replay")
	pipelineIAMCall(t, ts, "ListAccessKeys", map[string]string{"UserName": "ids-replay"})
	// And a request that *names* the minted key, which is the shape #833 depends on: a
	// re-minted id answers NoSuchEntity here rather than merely reading differently.
	pipelineIAMCall(t, ts, "DeleteAccessKey", map[string]string{
		"UserName": "ids-replay", "AccessKeyId": key.id,
	})
}

// idsAccessKey is one CreateAccessKey call, returning the credential it minted.
type idsCredential struct {
	id     string
	secret string
}

// idsAccessKey mints an access key for userName over the wire.
func idsAccessKey(t *testing.T, ts *emulator.TestServer, userName string) idsCredential {
	t.Helper()

	var parsed struct {
		ID     string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
		Secret string `xml:"CreateAccessKeyResult>AccessKey>SecretAccessKey"`
	}
	body := pipelineIAMCall(t, ts, "CreateAccessKey", map[string]string{"UserName": userName})
	require.NoError(t, xml.Unmarshal(body, &parsed))
	require.NotEmpty(t, parsed.ID, "CreateAccessKey minted no key: %s", body)
	return idsCredential{id: parsed.ID, secret: parsed.Secret}
}

// idsRunInstances launches count instances in one request, each with a declared data volume,
// and returns the instance ids in the order the response lists them.
func idsRunInstances(t *testing.T, ts *emulator.TestServer, count int) []string {
	t.Helper()

	var run struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(idsEC2Call(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": strconv.Itoa(count),
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "10",
	}), &run))

	ids := make([]string, 0, len(run.Instances))
	for _, inst := range run.Instances {
		ids = append(ids, inst.InstanceID)
	}
	return ids
}

// idsVolumeIDs returns every volume DescribeVolumes reports.
func idsVolumeIDs(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()

	var described struct {
		Volumes []struct {
			VolumeID string `xml:"volumeId"`
		} `xml:"volumeSet>item"`
	}
	require.NoError(t, xml.Unmarshal(
		idsEC2Call(t, ts, map[string]string{"Action": "DescribeVolumes"}), &described))

	ids := make([]string, 0, len(described.Volumes))
	for _, vol := range described.Volumes {
		ids = append(ids, vol.VolumeID)
	}
	return ids
}

// idsUnique returns the distinct members of ids, so an assertion on a count says "these
// repeated" rather than only that the lengths disagree.
func idsUnique(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// idsEC2Call issues an unsigned EC2 query-protocol request against a [emulator.TestServer]
// and returns the response body, requiring a 200.
//
// Unsigned, like the IAM helper next to it: enforcement keys off resolving to an IAM entity,
// and a setup call must not be subject to the policies it is creating.
func idsEC2Call(t *testing.T, ts *emulator.TestServer, params map[string]string) []byte {
	t.Helper()

	form := url.Values{}
	form.Set("Version", "2016-11-15")
	for k, v := range params {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", params["Action"], body)
	return body
}

// Tier 2 of #856: the shared UUID-shaped draw site, and the messaging/storage family.
//
// [emulator.IDMint] arrived with EC2, IAM and STS moved onto it. The assertions below are
// about the second of substrate's two shared draw sites — the one Lambda declares and six
// other services publish an identifier from — and about the SQS/SNS/EFS/FSx/Transfer group
// that moved with it. They are the same two layers as above: a distinctness property
// asserted directly on one request that mints several identifiers, and a replayed stream
// asserted over the wire.

// TestIDs_OneSendMessageBatchMintsDistinctMessageIDs is the ordinal's test for the shared
// generator, in the same shape as the RunInstances one above: several identifiers from a
// single request, so a mint that ignored its ordinal would answer one ID three times.
//
// SQS is the interesting caller because it mints twice per message on two different paths —
// a message ID on send and a receipt handle on receive — so the two must not collide either.
func TestIDs_OneSendMessageBatchMintsDistinctMessageIDs(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	queueURL := idsSQSQueue(t, ts, "ids-batch")

	params := map[string]string{"Action": "SendMessageBatch", "QueueUrl": queueURL}
	for i := 1; i <= 3; i++ {
		params["SendMessageBatchRequestEntry."+strconv.Itoa(i)+".Id"] = "e" + strconv.Itoa(i)
		params["SendMessageBatchRequestEntry."+strconv.Itoa(i)+".MessageBody"] = "same body"
	}
	var batch struct {
		Entries []struct {
			MessageID string `xml:"MessageId"`
		} `xml:"SendMessageBatchResult>SendMessageBatchResultEntry"`
	}
	require.NoError(t, xml.Unmarshal(idsSQSCall(t, ts, params), &batch))
	require.Len(t, batch.Entries, 3)

	minted := make([]string, 0, 3)
	for _, e := range batch.Entries {
		require.NotEmpty(t, e.MessageID)
		minted = append(minted, e.MessageID)
	}
	assert.Len(t, idsUnique(minted), 3,
		"one SendMessageBatch mints one message ID per entry, and the bodies are identical: %v",
		minted)
}

// TestReplay_TheSharedMintReplaysWithTheIdentifiersItMinted is the wire-level assertion for
// Tier 2, and the same claim [TestReplay_ACreateReplaysWithTheIdentifiersItMinted] makes for
// EC2 and IAM: a stream whose later requests *name* the identifiers its earlier ones minted
// replays with no differences and reaches the recorded state.
//
// The interlocking is what makes it more than a comparison of five responses. A re-minted
// queue URL, subscription ARN, file-system ID or receipt handle turns the request that names
// it into a refusal rather than into a differently-worded success.
func TestReplay_TheSharedMintReplaysWithTheIdentifiersItMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordSharedMintCreates(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"an identifier from the shared mint replays as the one recorded: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// idsRecordSharedMintCreates records the Tier 2 stream: a create per family that mints
// through the shared generator or through one of the messaging/storage generators, each
// followed by a request naming what the create minted.
func idsRecordSharedMintCreates(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// Frozen for the reason [idsRecordInterlockedCreates] gives: a replay pins the clock to
	// the recorded event's timestamp, so a handler that stamps a record off a live clock
	// diverges in the state hash for a reason that has nothing to do with an identifier.
	ts.FreezeTime()

	// SQS: a message ID from the shared generator on send, a receipt handle on receive, and
	// a delete that names the handle.
	queueURL := idsSQSQueue(t, ts, "ids-shared-mint")
	idsSQSCall(t, ts, map[string]string{
		"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": "one",
	})
	var received struct {
		Handles []string `xml:"ReceiveMessageResult>Message>ReceiptHandle"`
	}
	require.NoError(t, xml.Unmarshal(idsSQSCall(t, ts, map[string]string{
		"Action": "ReceiveMessage", "QueueUrl": queueURL, "MaxNumberOfMessages": "1",
	}), &received))
	require.Len(t, received.Handles, 1, "the message just sent has to be received")
	idsSQSCall(t, ts, map[string]string{
		"Action": "DeleteMessage", "QueueUrl": queueURL, "ReceiptHandle": received.Handles[0],
	})

	// SNS: a subscription ID, then a get that names the ARN carrying it.
	var topic struct {
		ARN string `xml:"CreateTopicResult>TopicArn"`
	}
	require.NoError(t, xml.Unmarshal(idsSNSCall(t, ts, map[string]string{
		"Action": "CreateTopic", "Name": "ids-shared-mint",
	}), &topic))
	require.NotEmpty(t, topic.ARN)

	var sub struct {
		ARN string `xml:"SubscribeResult>SubscriptionArn"`
	}
	require.NoError(t, xml.Unmarshal(idsSNSCall(t, ts, map[string]string{
		"Action": "Subscribe", "TopicArn": topic.ARN,
		"Protocol": "email", "Endpoint": "nobody@example.invalid",
	}), &sub))
	require.NotEmpty(t, sub.ARN)
	idsSNSCall(t, ts, map[string]string{
		"Action": "GetSubscriptionAttributes", "SubscriptionArn": sub.ARN,
	})

	// EFS: a file-system ID, then an access point and a mount target that name it.
	var fs struct {
		ID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(idsEFSCall(t, ts, http.MethodPost, "/2015-02-01/file-systems",
		map[string]any{"CreationToken": "ids-shared-mint"}), &fs))
	require.NotEmpty(t, fs.ID)
	idsEFSCall(t, ts, http.MethodPost, "/2015-02-01/access-points",
		map[string]any{"ClientToken": "ids-ap", "FileSystemId": fs.ID})
	idsEFSCall(t, ts, http.MethodGet, "/2015-02-01/file-systems?FileSystemId="+fs.ID, nil)
}

// idsSQSQueue creates one SQS queue and returns its URL.
func idsSQSQueue(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()

	var created struct {
		URL string `xml:"CreateQueueResult>QueueUrl"`
	}
	body := idsSQSCall(t, ts, map[string]string{"Action": "CreateQueue", "QueueName": name})
	require.NoError(t, xml.Unmarshal(body, &created))
	require.NotEmpty(t, created.URL, "CreateQueue returned no URL: %s", body)
	return created.URL
}

// idsSQSCall issues an unsigned SQS query-protocol request and returns the response body.
func idsSQSCall(t *testing.T, ts *emulator.TestServer, params map[string]string) []byte {
	t.Helper()
	return idsQueryCall(t, ts, "sqs.us-east-1.amazonaws.com", "2012-11-05", params)
}

// idsSNSCall issues an unsigned SNS query-protocol request and returns the response body.
func idsSNSCall(t *testing.T, ts *emulator.TestServer, params map[string]string) []byte {
	t.Helper()
	return idsQueryCall(t, ts, "sns.us-east-1.amazonaws.com", "2010-03-31", params)
}

// idsQueryCall issues one unsigned query-protocol request against host and requires a 200.
func idsQueryCall(t *testing.T, ts *emulator.TestServer, host, version string,
	params map[string]string,
) []byte {
	t.Helper()

	form := url.Values{}
	form.Set("Version", version)
	for k, v := range params {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", params["Action"], body)
	return body
}

// idsEFSCall issues one EFS REST/JSON request and requires a 2xx.
func idsEFSCall(t *testing.T, ts *emulator.TestServer, method, path string, body any) []byte {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = "elasticfilesystem.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", method, path, out)
	return out
}

// Tier 3 of #856: the compute and edge family — API Gateway (v1 and v2), AppSync, Batch, EMR
// Serverless, ECR, ELB and Route 53.
//
// Same two layers as the tiers above. One property is asserted on the mint directly, because
// this tier is the first to change an identifier's *alphabet* rather than only its source, and
// one recorded stream is asserted over the wire, because interlocked creates are what a
// re-minted identifier actually breaks.

// TestIDMint_TheAPIGatewayAlphabetIsTheWholePublishedSet pins the one rendering change tier 3
// makes.
//
// The crypto/rand form of generateAPIGatewayID read five bytes and mapped each *nibble*
// through a 36-character alphabet, so only the first sixteen characters — `a` through `p` —
// could ever appear in an API Gateway identifier and a digit never did. [emulator.IDMint.Chars]
// draws one byte per character, which is both the correct use of the seam and the alphabet API
// Gateway publishes. The seed is fixed, so this is a deterministic statement about the mapping
// and not a sample of a random source.
func TestIDMint_TheAPIGatewayAlphabetIsTheWholePublishedSet(t *testing.T) {
	t.Parallel()

	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	mint := emulator.NewIDMint("ids-tier3-alphabet")

	used := map[rune]bool{}
	for range 50 {
		id := mint.Chars(10, alphabet)
		require.Len(t, id, 10)
		for _, c := range id {
			require.True(t, strings.ContainsRune(alphabet, c),
				"%q is outside the published alphabet", c)
			used[c] = true
		}
	}

	beyondNibbleRange := 0
	for c := range used {
		if strings.IndexRune(alphabet, c) >= 16 {
			beyondNibbleRange++
		}
	}
	assert.Positive(t, beyondNibbleRange,
		"the nibble mapping this replaces could reach only a-p; %d of the alphabet's last 20 "+
			"characters appear", beyondNibbleRange)
}

// TestIDs_OneCreateRestApiMintsDistinctIdentifiers is the ordinal's test for this tier: one
// request that mints two identifiers, the API's own and its root resource's.
//
// A mint that ignored its ordinal would answer the same value for both, and every later
// request addressing the root resource would address the API instead.
func TestIDs_OneCreateRestApiMintsDistinctIdentifiers(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	var api struct {
		ID             string `json:"id"`
		RootResourceID string `json:"rootResourceId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodPost,
		"/restapis", map[string]any{"name": "ids-tier3"}), &api))

	require.Len(t, api.ID, 10, "an API Gateway identifier is ten characters")
	require.Len(t, api.RootResourceID, 10)
	assert.NotEqual(t, api.ID, api.RootResourceID,
		"one CreateRestApi mints the API's id and its root resource's id, not one value twice")
}

// TestIDs_OneCreateHostedZoneMintsAZoneAndAChangeID is the same property on the other kind of
// two-draw request: Route 53 answers a zone identifier and the identifier of the change that
// created it, from one call.
func TestIDs_OneCreateHostedZoneMintsAZoneAndAChangeID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	zoneID, changeID := idsCreateHostedZone(t, ts, "ids-tier3.example.")
	assert.NotEqual(t, zoneID, changeID,
		"a hosted zone and the change that created it are two identifiers: %q and %q",
		zoneID, changeID)
	assert.Contains(t, changeID, "/change/C", "a change id keeps its published prefix")
}

// TestReplay_TheComputeAndEdgeFamiliesReplayWithTheIdentifiersTheyMinted is the wire-level
// assertion for tier 3, and the same claim the tiers above make for EC2/IAM and for the
// messaging/storage group: a stream whose later requests *name* what its earlier ones minted
// replays with no differences and reaches the recorded state.
//
// Each of the seven services contributes a create followed by a request that can only succeed
// against the identifier that create minted — a resource under a REST API's root, an API key
// on an AppSync API, a record set in a hosted zone, a listener on a load balancer and its
// target group, a DescribeJobs naming a job id, a GetJobRun naming an application and a run,
// and a BatchGetImage naming an image digest. A re-minted identifier turns one of those into a
// refusal rather than into a differently-worded success.
func TestReplay_TheComputeAndEdgeFamiliesReplayWithTheIdentifiersTheyMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordComputeAndEdgeCreates(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"a compute or edge identifier replays as the one recorded: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// idsRecordComputeAndEdgeCreates records the tier-3 stream, one interlocked pair or triple per
// service.
func idsRecordComputeAndEdgeCreates(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// Frozen for the reason [idsRecordInterlockedCreates] gives: a replay pins the clock to the
	// recorded event's timestamp, so a handler stamping a record off a live clock diverges in
	// the state hash for a reason that has nothing to do with an identifier.
	ts.FreezeTime()

	idsRecordAPIGateway(t, ts)
	idsRecordAppSync(t, ts)
	idsRecordRoute53(t, ts)
	idsRecordELB(t, ts)
	idsRecordBatchAndEMRServerless(t, ts)
	idsRecordECR(t, ts)
}

// idsRecordAPIGateway records a REST API, a resource under the root resource it minted, and a
// deployment.
func idsRecordAPIGateway(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var api struct {
		ID             string `json:"id"`
		RootResourceID string `json:"rootResourceId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodPost,
		"/restapis", map[string]any{"name": "ids-tier3"}), &api))
	require.NotEmpty(t, api.ID)
	require.NotEmpty(t, api.RootResourceID)

	idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodPost,
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID,
		map[string]any{"pathPart": "things"})
	idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodPost,
		"/restapis/"+api.ID+"/deployments", map[string]any{"stageName": "prod"})
	idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodGet,
		"/restapis/"+api.ID+"/resources", nil)
}

// idsRecordAppSync records a GraphQL API, then an API key and a function on it.
func idsRecordAppSync(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var created struct {
		API struct {
			APIID string `json:"apiId"`
		} `json:"graphqlApi"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsAppSyncHost, http.MethodPost,
		"/v1/apis", map[string]any{"name": "ids-tier3", "authenticationType": "API_KEY"}),
		&created))
	apiID := created.API.APIID
	require.NotEmpty(t, apiID)

	idsRESTCall(t, ts, idsAppSyncHost, http.MethodPost,
		"/v1/apis/"+apiID+"/apikeys", map[string]any{"description": "ids-tier3"})

	var fn struct {
		Function struct {
			FunctionID string `json:"functionId"`
		} `json:"functionConfiguration"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsAppSyncHost, http.MethodPost,
		"/v1/apis/"+apiID+"/functions", map[string]any{"name": "idsFn"}), &fn))
	require.NotEmpty(t, fn.Function.FunctionID)

	// Two reads that name what was minted: the function by its own id, and the key collection
	// on the API's id.
	idsRESTCall(t, ts, idsAppSyncHost, http.MethodGet,
		"/v1/apis/"+apiID+"/functions/"+fn.Function.FunctionID, nil)
	idsRESTCall(t, ts, idsAppSyncHost, http.MethodGet, "/v1/apis/"+apiID+"/apikeys", nil)
}

// idsRecordRoute53 records a hosted zone, a record set inside it, and a read of the zone.
func idsRecordRoute53(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	zoneID, _ := idsCreateHostedZone(t, ts, "ids-tier3.example.")
	idsXMLCall(t, ts, idsRoute53Host, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		`<ChangeResourceRecordSetsRequest><ChangeBatch><Changes><Change>`+
			`<Action>CREATE</Action><ResourceRecordSet><Name>www.ids-tier3.example.</Name>`+
			`<Type>A</Type><TTL>300</TTL><ResourceRecords><ResourceRecord>`+
			`<Value>192.0.2.1</Value></ResourceRecord></ResourceRecords>`+
			`</ResourceRecordSet></Change></Changes></ChangeBatch>`+
			`</ChangeResourceRecordSetsRequest>`)
	idsXMLCall(t, ts, idsRoute53Host, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
}

// idsCreateHostedZone creates one hosted zone and returns its id and the change id the create
// answered with.
func idsCreateHostedZone(t *testing.T, ts *emulator.TestServer, name string) (zoneID, changeID string) {
	t.Helper()

	var created struct {
		Zone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
		Change struct {
			ID string `xml:"Id"`
		} `xml:"ChangeInfo"`
	}
	body := idsXMLCall(t, ts, idsRoute53Host, http.MethodPost, "/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest><Name>`+name+`</Name>`+
			`<CallerReference>ids-tier3</CallerReference></CreateHostedZoneRequest>`)
	require.NoError(t, xml.Unmarshal(body, &created))
	require.NotEmpty(t, created.Zone.ID, "CreateHostedZone returned no zone: %s", body)

	// The zone id is reported as "/hostedzone/Z…" and addressed as either form.
	return strings.TrimPrefix(created.Zone.ID, "/hostedzone/"), created.Change.ID
}

// idsRecordELB records a load balancer, a target group, and a listener naming both — three
// draws of the ELB suffix, each carried in an ARN a later request must match.
func idsRecordELB(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var lb struct {
		ARNs []string `xml:"CreateLoadBalancerResult>LoadBalancers>member>LoadBalancerArn"`
	}
	require.NoError(t, xml.Unmarshal(idsELBCall(t, ts, map[string]string{
		"Action": "CreateLoadBalancer", "Name": "ids-tier3-lb",
		"Subnets.member.1": "subnet-0123456789abcdef0",
	}), &lb))
	require.Len(t, lb.ARNs, 1)

	var tg struct {
		ARNs []string `xml:"CreateTargetGroupResult>TargetGroups>member>TargetGroupArn"`
	}
	require.NoError(t, xml.Unmarshal(idsELBCall(t, ts, map[string]string{
		"Action": "CreateTargetGroup", "Name": "ids-tier3-tg",
		"Protocol": "HTTP", "Port": "80", "VpcId": "vpc-0123456789abcdef0",
	}), &tg))
	require.Len(t, tg.ARNs, 1)

	idsELBCall(t, ts, map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lb.ARNs[0],
		"Protocol": "HTTP", "Port": "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tg.ARNs[0],
	})
	idsELBCall(t, ts, map[string]string{
		"Action": "DescribeListeners", "LoadBalancerArn": lb.ARNs[0],
	})
}

// idsRecordBatchAndEMRServerless records a Batch job and an EMR Serverless application and job
// run, each followed by a read naming what was minted.
func idsRecordBatchAndEMRServerless(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var job struct {
		JobID string `json:"jobId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsBatchHost, http.MethodPost,
		"/v1/submitjob", map[string]any{
			"jobName": "ids-tier3", "jobQueue": "ids-queue", "jobDefinition": "ids-def",
		}), &job))
	require.NotEmpty(t, job.JobID)
	idsRESTCall(t, ts, idsBatchHost, http.MethodPost, "/v1/describejobs",
		map[string]any{"jobs": []string{job.JobID}})

	var app struct {
		ApplicationID string `json:"applicationId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsEMRServerlessHost, http.MethodPost,
		"/applications", map[string]any{
			"name": "ids-tier3", "type": "SPARK", "releaseLabel": "emr-6.9.0",
		}), &app))
	require.NotEmpty(t, app.ApplicationID)

	var run struct {
		JobRunID string `json:"jobRunId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsEMRServerlessHost, http.MethodPost,
		"/applications/"+app.ApplicationID+"/jobruns", map[string]any{"name": "ids-run"}), &run))
	require.NotEmpty(t, run.JobRunID)
	idsRESTCall(t, ts, idsEMRServerlessHost, http.MethodGet,
		"/applications/"+app.ApplicationID+"/jobruns/"+run.JobRunID, nil)
}

// idsRecordECR records a repository, an image whose digest the emulator mints, and a
// BatchGetImage naming that digest.
func idsRecordECR(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	idsECRCall(t, ts, "CreateRepository", map[string]any{"repositoryName": "ids-tier3"})

	var put struct {
		Image struct {
			ImageID struct {
				ImageDigest string `json:"imageDigest"`
			} `json:"imageId"`
		} `json:"image"`
	}
	require.NoError(t, json.Unmarshal(idsECRCall(t, ts, "PutImage", map[string]any{
		"repositoryName": "ids-tier3", "imageTag": "v1",
		"imageManifest": `{"schemaVersion":2}`,
	}), &put))
	digest := put.Image.ImageID.ImageDigest
	require.NotEmpty(t, digest)

	idsECRCall(t, ts, "BatchGetImage", map[string]any{
		"repositoryName": "ids-tier3",
		"imageIds":       []map[string]string{{"imageDigest": digest}},
	})
}

// The hosts the tier-3 services are addressed at. Substrate routes by Host header, so these
// are what select the plugin for a request that carries no X-Amz-Target.
const (
	idsAPIGatewayHost    = "apigateway.us-east-1.amazonaws.com"
	idsAppSyncHost       = "appsync.us-east-1.amazonaws.com"
	idsRoute53Host       = "route53.amazonaws.com"
	idsBatchHost         = "batch.us-east-1.amazonaws.com"
	idsEMRServerlessHost = "emr-serverless.us-east-1.amazonaws.com"
	idsECRHost           = "api.ecr.us-east-1.amazonaws.com"
	idsELBHost           = "elasticloadbalancing.us-east-1.amazonaws.com"
)

// idsELBCall issues one unsigned ELBv2 query-protocol request and returns the response body.
func idsELBCall(t *testing.T, ts *emulator.TestServer, params map[string]string) []byte {
	t.Helper()
	return idsQueryCall(t, ts, idsELBHost, "2015-12-01", params)
}

// idsRESTCall issues one REST/JSON request against host and requires a 2xx.
func idsRESTCall(t *testing.T, ts *emulator.TestServer, host, method, path string, body any) []byte {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = host
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s %s: %s", host, method, path, out)
	return out
}

// idsXMLCall issues one REST/XML request against host and requires a 2xx.
func idsXMLCall(t *testing.T, ts *emulator.TestServer, host, method, path, body string) []byte {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = host
	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s %s: %s", host, method, path, out)
	return out
}

// idsECRCall issues one ECR JSON-1.1 request, which is target-routed rather than path-routed.
func idsECRCall(t *testing.T, ts *emulator.TestServer, op string, body any) []byte {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(string(raw)))
	require.NoError(t, err)
	req.Host = idsECRHost
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, out)
	return out
}

// Tier 4 of #856: the identity, keys and certificates family — Cognito (both APIs), IAM Identity
// Center, KMS, ACM, Secrets Manager, WAFv2 and API Gateway's API keys.
//
// What this tier adds to the property the tiers above assert is the kind of value being derived.
// These services mint things a caller *authenticates or decrypts or locks with* and not only things
// it addresses a resource by: a Cognito client secret, a KMS data key, a WAFv2 lock token, a secret
// version id. ids.go's file comment argues why those belong in the mint, and the stream below is the
// assertion that a recorded one replays as itself.

// TestIDs_OneCreateUserPoolClientMintsAClientIDAndASecret is the ordinal's test for this tier, on
// the one request in the tree that draws three times: the client id, and the two halves a client
// secret is concatenated from.
//
// A mint that ignored its ordinal would answer one value three times over, so a client id would be
// the first half of its own secret — and the secret would be that half twice.
func TestIDs_OneCreateUserPoolClientMintsAClientIDAndASecret(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	poolID := idsCognitoUserPool(t, ts, "ids-tier4")

	var created struct {
		Client struct {
			ClientID     string `json:"ClientId"`
			ClientSecret string `json:"ClientSecret"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCognitoIDPHost,
		"AWSCognitoIdentityProviderService.CreateUserPoolClient", map[string]any{
			"UserPoolId": poolID, "ClientName": "ids-tier4", "GenerateSecret": true,
		}), &created))

	id, secret := created.Client.ClientID, created.Client.ClientSecret
	require.Len(t, id, 12, "a Cognito client id is twelve characters")
	require.Len(t, secret, 24,
		"and a client secret is two of them, which is exactly the published 24-character minimum")

	assert.NotEqual(t, id, secret[:12], "a client id is not the first half of its own secret")
	assert.NotEqual(t, secret[:12], secret[12:], "and a secret's two halves are two draws")
}

// TestReplay_TheIdentityAndKeyFamiliesReplayWithTheIdentifiersTheyMinted is the wire-level
// assertion for tier 4, and the same claim the three tiers above make for their families: a stream
// whose later requests *name* what its earlier ones minted replays with no differences and reaches
// the recorded state.
//
// Every service contributes a create followed by at least one request that can only succeed against
// what that create minted — a user pool client on a pool id, a sign-up on a client id, credentials
// on an identity id, a permission set on a lazily-minted instance ARN, a Decrypt of a data key's
// ciphertext, tags on a certificate ARN, a GetSecretValue naming a version id, an UpdateWebACL
// holding a lock token, and a GetApiKey naming a key id. A re-minted value breaks one of those
// rather than merely rewording it: a refusal (WAFOptimisticLockException for the ACL, a not-found
// for most), or — see [idsRecordSecretsManager] — a 200 that has quietly lost a member.
func TestReplay_TheIdentityAndKeyFamiliesReplayWithTheIdentifiersTheyMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordIdentityAndKeyCreates(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"an identity, key or certificate identifier replays as the one recorded: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// idsRecordIdentityAndKeyCreates records the tier-4 stream, one interlocked group per service.
func idsRecordIdentityAndKeyCreates(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// Frozen for the reason [idsRecordInterlockedCreates] gives: a replay pins the clock to the
	// recorded event's timestamp, so a handler stamping a record off a live clock diverges in the
	// state hash for a reason that has nothing to do with an identifier.
	ts.FreezeTime()

	idsRecordCognitoIDP(t, ts)
	idsRecordCognitoIdentity(t, ts)
	idsRecordSSO(t, ts)
	idsRecordKMS(t, ts)
	idsRecordACM(t, ts)
	idsRecordSecretsManager(t, ts)
	idsRecordWAFv2(t, ts)
	idsRecordAPIGatewayAPIKey(t, ts)
}

// idsRecordCognitoIDP records a user pool, a client with a secret on it, a sign-up against that
// client id, and two reads naming what was minted.
func idsRecordCognitoIDP(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	poolID := idsCognitoUserPool(t, ts, "ids-tier4-pool")

	var created struct {
		Client struct {
			ClientID     string `json:"ClientId"`
			ClientSecret string `json:"ClientSecret"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCognitoIDPHost,
		"AWSCognitoIdentityProviderService.CreateUserPoolClient", map[string]any{
			"UserPoolId": poolID, "ClientName": "ids-tier4", "GenerateSecret": true,
		}), &created))
	clientID := created.Client.ClientID
	require.NotEmpty(t, clientID)
	require.NotEmpty(t, created.Client.ClientSecret)

	// SignUp is addressed by the client id alone — the handler finds the pool through it — and
	// mints the user's `sub`, so it is both a read of one minted value and a draw of another.
	idsJSONTargetCall(t, ts, idsCognitoIDPHost, "AWSCognitoIdentityProviderService.SignUp",
		map[string]any{"ClientId": clientID, "Username": "ids-user", "Password": "Passw0rd!"})

	idsJSONTargetCall(t, ts, idsCognitoIDPHost,
		"AWSCognitoIdentityProviderService.DescribeUserPoolClient",
		map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	idsJSONTargetCall(t, ts, idsCognitoIDPHost,
		"AWSCognitoIdentityProviderService.AdminGetUser",
		map[string]any{"UserPoolId": poolID, "Username": "ids-user"})
}

// idsCognitoUserPool creates one user pool and returns the `{region}_{12 chars}` id it minted.
//
// The member is read as `UserPoolId` and not as the `Id` that API_UserPoolType publishes, which is
// substrate's own wire shape rather than AWS's — a fidelity gap this tier found and #1286 tracks.
// Reading the member substrate actually sends is what keeps this a test about the mint.
func idsCognitoUserPool(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()

	var created struct {
		Pool struct {
			ID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCognitoIDPHost,
		"AWSCognitoIdentityProviderService.CreateUserPool",
		map[string]any{"PoolName": name}), &created))

	region, suffix, ok := strings.Cut(created.Pool.ID, "_")
	require.True(t, ok, "a user pool id is {region}_{suffix}, got %q", created.Pool.ID)
	require.NotEmpty(t, region)
	require.Len(t, suffix, 12, "the minted half of a user pool id is twelve characters")
	return created.Pool.ID
}

// idsRecordCognitoIdentity records an identity pool, a read of it, and a GetId followed by
// credentials for the identity id that GetId minted.
func idsRecordCognitoIdentity(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var pool struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCognitoIdentityHost,
		"AWSCognitoIdentityService.CreateIdentityPool", map[string]any{
			"IdentityPoolName": "ids-tier4", "AllowUnauthenticatedIdentities": true,
		}), &pool))
	require.NotEmpty(t, pool.IdentityPoolID)

	idsJSONTargetCall(t, ts, idsCognitoIdentityHost,
		"AWSCognitoIdentityService.DescribeIdentityPool",
		map[string]any{"IdentityPoolId": pool.IdentityPoolID})

	var got struct {
		IdentityID string `json:"IdentityId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCognitoIdentityHost,
		"AWSCognitoIdentityService.GetId",
		map[string]any{"IdentityPoolId": pool.IdentityPoolID}), &got))
	require.NotEmpty(t, got.IdentityID)

	idsJSONTargetCall(t, ts, idsCognitoIdentityHost,
		"AWSCognitoIdentityService.GetCredentialsForIdentity",
		map[string]any{"IdentityId": got.IdentityID})
}

// idsRecordSSO records the lazily-minted instance, a permission set under it, an account assignment
// naming that permission set, and a listing naming both.
//
// ListInstances is the request that *creates* the instance, which is the laziness sso_types.go's
// preamble records: the instance ARN and its identity store id belong to the ordinal stream of a
// read.
func idsRecordSSO(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var instances struct {
		Instances []struct {
			InstanceArn     string `json:"InstanceArn"`
			IdentityStoreID string `json:"IdentityStoreId"`
		} `json:"Instances"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsSSOHost,
		"SWBExternalService.ListInstances", map[string]any{}), &instances))
	require.Len(t, instances.Instances, 1)
	instanceArn := instances.Instances[0].InstanceArn
	require.NotEmpty(t, instanceArn)
	require.NotEmpty(t, instances.Instances[0].IdentityStoreID)

	var permSet struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
		} `json:"PermissionSet"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsSSOHost,
		"SWBExternalService.CreatePermissionSet", map[string]any{
			"InstanceArn": instanceArn, "Name": "ids-tier4",
		}), &permSet))
	permSetArn := permSet.PermissionSet.PermissionSetArn
	require.NotEmpty(t, permSetArn)
	require.Contains(t, permSetArn, instanceArn,
		"a permission set ARN is a child of the instance ARN, so one mint carries the other")

	idsJSONTargetCall(t, ts, idsSSOHost, "SWBExternalService.DescribePermissionSet",
		map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": permSetArn})
	idsJSONTargetCall(t, ts, idsSSOHost, "SWBExternalService.CreateAccountAssignment",
		map[string]any{
			"InstanceArn": instanceArn, "PermissionSetArn": permSetArn,
			"TargetId": "123456789012", "TargetType": "AWS_ACCOUNT",
			"PrincipalType": "USER", "PrincipalId": "ids-principal",
		})
	idsJSONTargetCall(t, ts, idsSSOHost, "SWBExternalService.ListAccountAssignments",
		map[string]any{
			"InstanceArn": instanceArn, "PermissionSetArn": permSetArn,
			"AccountId": "123456789012",
		})
}

// idsRecordKMS records a key, a data key wrapped by it, and a Decrypt of the ciphertext that
// GenerateDataKey answered with.
//
// The data key is the one value in this tier that is not an identifier: it is reported as the
// response's Plaintext and wrapped into its CiphertextBlob, so a re-minted key changes both members
// of the recorded response. [kmsStubDataKey]'s comment records why that puts it in #856's scope.
func idsRecordKMS(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var key struct {
		Metadata struct {
			KeyID string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsKMSHost,
		"TrentService.CreateKey", map[string]any{"Description": "ids-tier4"}), &key))
	keyID := key.Metadata.KeyID
	require.NotEmpty(t, keyID)

	var dataKey struct {
		CiphertextBlob string `json:"CiphertextBlob"`
		Plaintext      string `json:"Plaintext"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsKMSHost,
		"TrentService.GenerateDataKey",
		map[string]any{"KeyId": keyID, "KeySpec": "AES_256"}), &dataKey))
	require.NotEmpty(t, dataKey.CiphertextBlob)
	require.NotEmpty(t, dataKey.Plaintext)

	idsJSONTargetCall(t, ts, idsKMSHost, "TrentService.Decrypt",
		map[string]any{"CiphertextBlob": dataKey.CiphertextBlob})
	idsJSONTargetCall(t, ts, idsKMSHost, "TrentService.DescribeKey",
		map[string]any{"KeyId": keyID})
}

// idsRecordACM records a certificate and two requests naming the ARN its id was minted into.
func idsRecordACM(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var cert struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsACMHost,
		"CertificateManager.RequestCertificate",
		map[string]any{"DomainName": "ids-tier4.example.com"}), &cert))
	require.NotEmpty(t, cert.CertificateArn)

	idsJSONTargetCall(t, ts, idsACMHost, "CertificateManager.AddTagsToCertificate",
		map[string]any{
			"CertificateArn": cert.CertificateArn,
			"Tags":           []map[string]string{{"Key": "tier", "Value": "4"}},
		})
	idsJSONTargetCall(t, ts, idsACMHost, "CertificateManager.DescribeCertificate",
		map[string]any{"CertificateArn": cert.CertificateArn})
}

// idsRecordSecretsManager records a secret, a second version of it, and a GetSecretValue naming the
// version id the create minted.
//
// That last request is the quietest interlock in the tier: a re-minted version id does not make it
// fail, it makes it answer 200 with no `SecretString` at all, because substrate reports an unknown
// version by omitting the member. Reverting the minter turns the recorded `"first"` into `""`.
func idsRecordSecretsManager(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var created struct {
		ARN       string `json:"ARN"`
		VersionID string `json:"VersionId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsSecretsManagerHost,
		"secretsmanager.CreateSecret",
		map[string]any{"Name": "ids-tier4", "SecretString": "first"}), &created))
	require.NotEmpty(t, created.ARN)
	require.Len(t, created.VersionID, 16,
		"a version id is sixteen uppercase hex characters until #1285 widens it")

	var put struct {
		VersionID string `json:"VersionId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsSecretsManagerHost,
		"secretsmanager.PutSecretValue",
		map[string]any{"SecretId": "ids-tier4", "SecretString": "second"}), &put))
	require.NotEqual(t, created.VersionID, put.VersionID,
		"two versions of one secret are two draws")

	idsJSONTargetCall(t, ts, idsSecretsManagerHost, "secretsmanager.GetSecretValue",
		map[string]any{"SecretId": "ids-tier4", "VersionId": created.VersionID})
	idsJSONTargetCall(t, ts, idsSecretsManagerHost, "secretsmanager.ListSecretVersionIds",
		map[string]any{"SecretId": "ids-tier4"})
}

// idsRecordWAFv2 records a web ACL and an update holding the lock token the create answered with,
// which is the tier's one interlock that fails as WAFOptimisticLockException rather than as a
// not-found.
func idsRecordWAFv2(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var created struct {
		Summary struct {
			ID        string `json:"Id"`
			LockToken string `json:"LockToken"`
		} `json:"Summary"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsWAFv2Host,
		"AWSWAF_20190729.CreateWebACL", map[string]any{
			"Name": "ids-tier4", "Scope": "REGIONAL",
			"DefaultAction": map[string]any{"Allow": map[string]any{}},
		}), &created))
	id, lockToken := created.Summary.ID, created.Summary.LockToken
	require.NotEmpty(t, id)
	require.NotEmpty(t, lockToken)
	require.NotEqual(t, id, lockToken, "an ACL's id and its first lock token are two draws")

	idsJSONTargetCall(t, ts, idsWAFv2Host, "AWSWAF_20190729.UpdateWebACL", map[string]any{
		"Name": "ids-tier4", "Scope": "REGIONAL", "Id": id, "LockToken": lockToken,
		"Description": "updated under the recorded lock token",
	})
	idsJSONTargetCall(t, ts, idsWAFv2Host, "AWSWAF_20190729.GetWebACL",
		map[string]any{"Name": "ids-tier4", "Scope": "REGIONAL", "Id": id})
}

// idsRecordAPIGatewayAPIKey records an API key and a read naming its id.
//
// The key is path-routed rather than target-routed, and it is in this tier rather than tier 3
// because its two values came from ACM's certificate-ID generator until [generateAPIGatewayAPIKey]
// existed — one service minting another's identifiers.
func idsRecordAPIGatewayAPIKey(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var key struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodPost,
		"/apikeys", map[string]any{"name": "ids-tier4", "enabled": true}), &key))
	require.NotEmpty(t, key.ID)
	require.NotEmpty(t, key.Value)
	require.NotEqual(t, key.ID, key.Value,
		"one CreateApiKey draws the key's id and its value separately")

	idsRESTCall(t, ts, idsAPIGatewayHost, http.MethodGet, "/apikeys/"+key.ID, nil)
}

// The hosts the tier-4 services are addressed at.
const (
	idsCognitoIDPHost      = "cognito-idp.us-east-1.amazonaws.com"
	idsCognitoIdentityHost = "cognito-identity.us-east-1.amazonaws.com"
	idsSSOHost             = "sso.us-east-1.amazonaws.com"
	idsKMSHost             = "kms.us-east-1.amazonaws.com"
	idsACMHost             = "acm.us-east-1.amazonaws.com"
	idsSecretsManagerHost  = "secretsmanager.us-east-1.amazonaws.com"
	idsWAFv2Host           = "wafv2.us-east-1.amazonaws.com"
)

// idsJSONTargetCall issues one JSON-1.1 request routed by X-Amz-Target and requires a 2xx. target
// is the whole header value, prefix included, because the seven services in this tier publish seven
// different prefixes.
func idsJSONTargetCall(t *testing.T, ts *emulator.TestServer, host, target string, body any) []byte {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(string(raw)))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", host, target, out)
	return out
}

// Tier 5 of #856: the analytics family — Athena, Redshift Data, Glue, Timestream, OpenSearch and
// QuickSight.
//
// What this tier adds to the property the tiers above assert is the *shape of the call* the
// identifier gates. An analytics identifier names a submission rather than a resource: a query
// execution, a statement, a job run, a scroll cursor. Nothing else addresses it — there is no name
// to fall back on the way an S3 bucket or a Glue job has one — so the minted value is the only
// handle a consumer's poll loop holds, which is why an underived one broke the loop outright rather
// than merely reporting a different string. Athena is the extreme: `GetQueryExecution`,
// `GetQueryResults` and `StopQueryExecution` all key on the one id `StartQueryExecution` returned.

// TestIDs_OneBulkIndexMintsOneIDPerDocument is the ordinal's test for this tier, on the one request
// in the tree that draws an unbounded number of times: a `_bulk` body whose actions carry no `_id`
// mints one per document.
//
// A mint that ignored its ordinal would answer one id for all three, and here that is worse than a
// cosmetic collision — the documents are keyed by it, so the second and third would overwrite the
// first and one document would be left where three were indexed.
func TestIDs_OneBulkIndexMintsOneIDPerDocument(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	body := strings.Join([]string{
		`{"index":{}}`, `{"n":1}`,
		`{"index":{}}`, `{"n":2}`,
		`{"index":{}}`, `{"n":3}`,
	}, "\n") + "\n"

	var bulk struct {
		Items []map[string]struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(idsNDJSONCall(t, ts, idsOpenSearchHost,
		"/ids-tier5-bulk/_bulk", body), &bulk))
	require.Len(t, bulk.Items, 3, "one item per action")

	seen := make(map[string]bool, 3)
	for _, item := range bulk.Items {
		for op, res := range item {
			require.Equal(t, "index", op)
			require.Len(t, res.ID, 16, "a generated document id is sixteen URL-safe base64 characters")
			assert.Equal(t, "created", res.Result,
				"each document is new, which it would not be if two shared an id")
			seen[res.ID] = true
		}
	}
	assert.Len(t, seen, 3, "three documents indexed in one request draw three ids")
}

// TestReplay_TheAnalyticsFamilyReplaysWithTheIdentifiersItMinted is the wire-level assertion for
// tier 5, and the same claim the four tiers above make for their families: a stream whose later
// requests *name* what its earlier ones minted replays with no differences and reaches the recorded
// state.
//
// Every service contributes a submission followed by the read that can only reach it through the
// minted handle — a GetQueryExecution and a GetQueryResults on an Athena query id, a
// DescribeStatement and a GetStatementResult on a Redshift Data statement id, a GetJobRun on a Glue
// run id, a document GET and a scroll continuation on OpenSearch's two kinds of generated id, and a
// DescribeIngestion whose URL path is the ingestion id CreateDataSet minted. A re-minted value
// refuses each of those by name: InvalidRequestException for Athena, ResourceNotFoundException for
// Redshift Data, EntityNotFoundException for Glue, a 404 for a document and an expired-context
// refusal for a scroll. Timestream is the exception and is here for the other half of the property:
// its QueryId reaches no later call, so what a fresh draw costs there is a body difference rather
// than a refusal — which is still a difference, and still enough to make a recorded Query fail to
// replay.
func TestReplay_TheAnalyticsFamilyReplaysWithTheIdentifiersItMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordAnalyticsSubmissions(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the submissions")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"an analytics submission's identifier replays as the one recorded: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// idsRecordAnalyticsSubmissions records the tier-5 stream, one interlocked group per service.
func idsRecordAnalyticsSubmissions(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// Frozen for the reason [idsRecordInterlockedCreates] gives: a replay pins the clock to the
	// recorded event's timestamp, so a handler stamping a record off a live clock diverges in the
	// state hash for a reason that has nothing to do with an identifier.
	ts.FreezeTime()

	idsRecordAthena(t, ts)
	idsRecordRedshiftData(t, ts)
	idsRecordGlue(t, ts)
	idsRecordTimestream(t, ts)
	idsRecordOpenSearch(t, ts)
	idsRecordQuickSight(t, ts)
}

// idsRecordAthena records a query execution and the two reads that address it by the id it minted.
func idsRecordAthena(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var started struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsAthenaHost,
		"AmazonAthena.StartQueryExecution", map[string]any{
			"QueryString": "SELECT 1",
			"ResultConfiguration": map[string]any{
				"OutputLocation": "s3://ids-tier5-results/",
			},
		}), &started))
	idsRequireHexUUID(t, started.QueryExecutionID, "an Athena query execution id")

	idsJSONTargetCall(t, ts, idsAthenaHost, "AmazonAthena.GetQueryExecution",
		map[string]any{"QueryExecutionId": started.QueryExecutionID})
	idsJSONTargetCall(t, ts, idsAthenaHost, "AmazonAthena.GetQueryResults",
		map[string]any{"QueryExecutionId": started.QueryExecutionID})
}

// idsRecordRedshiftData records a statement and the two reads that address it by its id.
func idsRecordRedshiftData(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var executed struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsRedshiftDataHost,
		"RedshiftData.ExecuteStatement", map[string]any{
			"WorkgroupName": "ids-tier5", "Database": "dev", "Sql": "SELECT 1",
		}), &executed))
	idsRequireHexUUID(t, executed.ID, "a Redshift Data statement id")

	idsJSONTargetCall(t, ts, idsRedshiftDataHost, "RedshiftData.DescribeStatement",
		map[string]any{"Id": executed.ID})
	idsJSONTargetCall(t, ts, idsRedshiftDataHost, "RedshiftData.GetStatementResult",
		map[string]any{"Id": executed.ID})
}

// idsRecordGlue records a job, a run of it, and the GetJobRun that names the run id.
//
// The job is addressed by the caller's own name throughout, so the run id is the only value in the
// group substrate mints — which is what makes GetJobRun a read of the mint and not of the request.
func idsRecordGlue(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	idsJSONTargetCall(t, ts, idsGlueHost, "AWSGlue.CreateJob", map[string]any{
		"Name": "ids-tier5-job", "Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://ids-tier5/etl.py"},
	})

	var run struct {
		JobRunID string `json:"JobRunId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsGlueHost, "AWSGlue.StartJobRun",
		map[string]any{"JobName": "ids-tier5-job"}), &run))
	require.True(t, strings.HasPrefix(run.JobRunID, "jr_"),
		"a Glue job run id keeps its jr_ prefix, got %q", run.JobRunID)
	require.Len(t, run.JobRunID, len("jr_")+32, "and 32 hex characters after it")

	idsJSONTargetCall(t, ts, idsGlueHost, "AWSGlue.GetJobRun",
		map[string]any{"JobName": "ids-tier5-job", "RunId": run.JobRunID})
}

// idsRecordTimestream records one Query, whose QueryId nothing addresses.
//
// It is in the stream for what [timestreamQueryID]'s comment records: the id is a response member no
// later call names, so it can only be observed by being compared against the recording — which is
// exactly what a replay does.
func idsRecordTimestream(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	var queried struct {
		QueryID string `json:"QueryId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsTimestreamQueryHost,
		"Timestream_20181101.Query",
		map[string]any{"QueryString": "SELECT 1"}), &queried))
	require.Len(t, queried.QueryID, 32,
		"a Timestream QueryId is 32 hex characters, the alphabet [a-zA-Z0-9]+ admits")
	require.NotContains(t, queried.QueryID, "-",
		"and not a UUID: the published pattern excludes the hyphens one would carry")
}

// idsRecordOpenSearch records two documents indexed without an `_id`, a read of the first by the id
// the index minted, and a scrolled search followed by its continuation.
//
// Both kinds of generated value are here because they break differently: a re-minted document id
// answers the recorded GET with a 404, and a re-minted scroll id answers the recorded continuation
// with search_context_missing_exception against a cursor the recording had just opened.
func idsRecordOpenSearch(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	const index = "ids-tier5-index"

	first := idsOpenSearchIndexDoc(t, ts, index, map[string]any{"n": 1})
	idsOpenSearchIndexDoc(t, ts, index, map[string]any{"n": 2})

	idsRESTCall(t, ts, idsOpenSearchHost, http.MethodGet, "/"+index+"/_doc/"+first, nil)

	// A page of one over two documents, so the scroll has a second page to continue into; the TTL
	// travels in the body rather than in a `?scroll=` parameter to keep the group a test about the
	// identifier and not about query-string round-tripping.
	var searched struct {
		ScrollID string `json:"_scroll_id"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsOpenSearchHost, http.MethodPost,
		"/"+index+"/_search", map[string]any{"size": 1, "scroll": "1m"}), &searched))
	require.Len(t, searched.ScrollID, 16, "a scroll id is minted in the same shape a document id is")
	require.NotEqual(t, first, searched.ScrollID, "and is a separate draw")

	idsRESTCall(t, ts, idsOpenSearchHost, http.MethodPost, "/_search/scroll",
		map[string]any{"scroll_id": searched.ScrollID})
}

// idsOpenSearchIndexDoc indexes one document with no `_id` and returns the id OpenSearch minted.
func idsOpenSearchIndexDoc(t *testing.T, ts *emulator.TestServer, index string, doc any) string {
	t.Helper()

	var indexed struct {
		ID     string `json:"_id"`
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsOpenSearchHost, http.MethodPost,
		"/"+index+"/_doc", doc), &indexed))
	require.Equal(t, "created", indexed.Result)
	require.Len(t, indexed.ID, 16, "a generated document id is sixteen URL-safe base64 characters")
	return indexed.ID
}

// idsRecordQuickSight records a data source, a read of it, a data set, and the DescribeIngestion
// whose path is the ingestion id CreateDataSet minted.
//
// The data source and data set ids are the caller's own, which is why the ingestion id is the value
// under test: it is the one identifier in the group substrate chooses, and the only one a replay
// could get wrong.
func idsRecordQuickSight(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	const base = "/accounts/123456789012/"

	idsRESTCall(t, ts, idsQuickSightHost, http.MethodPost, base+"data-sources", map[string]any{
		"DataSourceId": "ids-tier5-source", "Name": "ids-tier5", "Type": "ATHENA",
	})
	idsRESTCall(t, ts, idsQuickSightHost, http.MethodGet, base+"data-sources/ids-tier5-source", nil)

	var set struct {
		IngestionID string `json:"IngestionId"`
		RequestID   string `json:"RequestId"`
	}
	require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsQuickSightHost, http.MethodPost,
		base+"data-sets", map[string]any{
			"DataSetId": "ids-tier5-set", "Name": "ids-tier5",
		}), &set))
	idsRequireHexUUID(t, set.IngestionID, "a QuickSight ingestion id")
	assert.NotEqual(t, set.IngestionID, set.RequestID,
		"one CreateDataSet draws the ingestion id and the response's request id separately")

	idsRESTCall(t, ts, idsQuickSightHost, http.MethodGet,
		base+"data-sets/ids-tier5-set/ingestions/"+set.IngestionID, nil)
}

// idsRequireHexUUID requires that id is in the 8-4-4-4-12 lowercase-hex shape [IDMint.HexUUID]
// renders — the form five of this tier's six identifiers publish.
func idsRequireHexUUID(t *testing.T, id, what string) {
	t.Helper()

	require.Len(t, id, 36, "%s is 36 characters, got %q", what, id)
	for i, group := range strings.Split(id, "-") {
		require.Len(t, group, []int{8, 4, 4, 4, 12}[i], "%s group %d of %q", what, i, id)
		require.Regexp(t, "^[0-9a-f]+$", group, "%s is lowercase hex: %q", what, id)
	}
}

// The hosts the tier-5 services are addressed at. Timestream is two endpoints and this is the query
// one, because Query is the operation that mints.
const (
	idsAthenaHost          = "athena.us-east-1.amazonaws.com"
	idsRedshiftDataHost    = "redshift-data.us-east-1.amazonaws.com"
	idsGlueHost            = "glue.us-east-1.amazonaws.com"
	idsTimestreamQueryHost = "query.timestream.us-east-1.amazonaws.com"
	idsOpenSearchHost      = "search-ids-tier5.us-east-1.es.amazonaws.com"
	idsQuickSightHost      = "quicksight.us-east-1.amazonaws.com"
)

// idsNDJSONCall issues one newline-delimited-JSON request, which is what OpenSearch's `_bulk` takes
// and the only body in these tests that is not a single JSON document.
func idsNDJSONCall(t *testing.T, ts *emulator.TestServer, host, path, body string) []byte {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path,
		strings.NewReader(body))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", host, path, out)
	return out
}

// Tier 6 of #856: the CI/CD family — CodeBuild, CodeDeploy and CodePipeline.
//
// What this tier adds is that *three renderings* of the same sixteen derived bytes coexist in one
// family, and which one a site gets is decided by what its caller already parses rather than by
// what looks tidiest. CodePipeline publishes a UUID pattern and its draw site set RFC 4122's
// version and variant nibbles, so it mints through [emulator.IDMint.UUID]. CodeBuild's build id and
// CodeDeploy's two identity ids publish no pattern at all and their draw sites did *not* set those
// nibbles, so they keep [emulator.IDMint.HexUUID] — #856 changes where an identifier comes from,
// not which bytes a caller sees. CodeDeploy's deployment id is neither: it is `d-` and nine
// uppercase alphanumerics, a shape with no published pattern that AWS's own sample response shows.
//
// The tier has no unbounded-draw site — every one of the five operations mints exactly once — so the
// ordinal assertion the tiers above make on a batch has nothing to bite on here. What replaces it is
// the rendering assertion below, which is the property this family can get wrong.

// TestIDs_TheCICDFamilyKeepsTheRenderingsItsCallersParse pins the three shapes against the two
// things that decide them: the published pattern where there is one, and the bytes the crypto/rand
// form produced where there is not.
//
// The substantive claim is the version nibble. A CodePipeline execution id is published as
// `[0-9a-f]{8}-…`, which admits any hex digit in the version position and so would be satisfied by
// a plain hex rendering too; substrate keeps the `4` because its draw site always set it and a
// consumer validating the value as a version-4 UUID would start failing if it vanished.
func TestIDs_TheCICDFamilyKeepsTheRenderingsItsCallersParse(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()

	idsJSONTargetCall(t, ts, idsCodePipelineHost, "CodePipeline_20150709.CreatePipeline",
		map[string]any{"pipeline": map[string]any{"name": "ids-tier6-pipeline"}})
	var started struct {
		PipelineExecutionID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodePipelineHost,
		"CodePipeline_20150709.StartPipelineExecution",
		map[string]any{"name": "ids-tier6-pipeline"}), &started))
	idsRequireHexUUID(t, started.PipelineExecutionID, "a pipeline execution id")
	assert.Equal(t, "4", started.PipelineExecutionID[14:15],
		"a pipeline execution id is a version-4 UUID: %q", started.PipelineExecutionID)
	assert.Contains(t, "89ab", started.PipelineExecutionID[19:20],
		"and carries RFC 4122's variant bits: %q", started.PipelineExecutionID)

	var deployed struct {
		DeploymentID string `json:"deploymentId"`
	}
	idsJSONTargetCall(t, ts, idsCodeDeployHost, "CodeDeploy_20141006.CreateApplication",
		map[string]any{"applicationName": "ids-tier6-app"})
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost,
		"CodeDeploy_20141006.CreateDeployment",
		map[string]any{"applicationName": "ids-tier6-app"}), &deployed))
	require.Len(t, deployed.DeploymentID, len("d-")+9,
		"a deployment id is d- and nine characters, as AWS's own sample response shows")
	assert.Regexp(t, `^d-[A-Z0-9]{9}$`, deployed.DeploymentID,
		"in uppercase letters and digits: %q", deployed.DeploymentID)
}

// TestReplay_TheCICDFamilyReplaysWithTheIdentifiersItMinted is the wire-level assertion for tier 6,
// the same claim the five tiers above make for their families.
//
// Two of the five identifiers are *addressed* by a later request in the stream, and those are the
// ones a fresh draw refuses outright: `BatchGetBuilds` reports a re-minted build id under
// `buildsNotFound` — a 200 whose `builds` list is empty, which is how an underived id stalls a
// consumer's poll loop rather than failing it — and `GetPipelineExecution` answers
// `PipelineExecutionNotFoundException`. The other three are reported rather than addressed:
// CodeDeploy's application and deployment-group ids are echoed by reads keyed on names, so what a
// fresh draw costs there is a body difference and a `state_hash_after` mismatch on the create,
// which is still enough to make the recording unreplayable. `GetDeployment` sits on the addressed
// side with the deployment id.
func TestReplay_TheCICDFamilyReplaysWithTheIdentifiersItMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	idsRecordCICDCreates(t, ts)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"a CI/CD identifier replays as the one recorded: %s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// idsRecordCICDCreates records the tier-6 stream, one interlocked group per service.
func idsRecordCICDCreates(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	// Frozen for the reason [idsRecordInterlockedCreates] gives: all three plugins stamp their
	// records off the time controller, so a live clock diverges in the state hash for a reason that
	// has nothing to do with an identifier.
	ts.FreezeTime()

	idsRecordCodeBuild(t, ts)
	idsRecordCodeDeploy(t, ts)
	idsRecordCodePipeline(t, ts)
}

// idsRecordCodeBuild records a project, a build of it, and the BatchGetBuilds that names the build
// id — the one read in the family that answers a wrong id with a 200.
func idsRecordCodeBuild(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	const project = "ids-tier6-project"

	idsJSONTargetCall(t, ts, idsCodeBuildHost, "CodeBuild_20161006.CreateProject", map[string]any{
		"name":        project,
		"serviceRole": "arn:aws:iam::123456789012:role/CodeBuildRole",
		"source":      map[string]any{"type": "GITHUB", "location": "https://example.invalid/r"},
	})

	var build struct {
		Build struct {
			ID string `json:"id"`
		} `json:"build"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeBuildHost,
		"CodeBuild_20161006.StartBuild", map[string]any{"projectName": project}), &build))
	uuid, ok := strings.CutPrefix(build.Build.ID, project+":")
	require.True(t, ok, "a build id is the project name and a colon, then the minted half: %q",
		build.Build.ID)
	idsRequireHexUUID(t, uuid, "the minted half of a build id")

	var got struct {
		Builds         []map[string]any `json:"builds"`
		BuildsNotFound []string         `json:"buildsNotFound"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeBuildHost,
		"CodeBuild_20161006.BatchGetBuilds",
		map[string]any{"ids": []string{build.Build.ID}}), &got))
	require.Len(t, got.Builds, 1, "the recorded read reaches the build by its minted id")
	require.Empty(t, got.BuildsNotFound,
		"and a wrong id would land here, in a 200 rather than an error")
}

// idsRecordCodeDeploy records an application, a deployment group, a deployment, and a read of each.
//
// The group exercises both halves of the tier. GetApplication and GetDeploymentGroup are keyed on
// the caller's own names and merely echo the two identity ids, so what an underived one costs there
// is a body difference and a state-hash mismatch; GetDeployment takes the deployment id itself, and
// an underived one is refused outright.
func idsRecordCodeDeploy(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	const (
		app   = "ids-tier6-app"
		group = "ids-tier6-group"
	)

	var created struct {
		ApplicationID string `json:"applicationId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost,
		"CodeDeploy_20141006.CreateApplication", map[string]any{
			"applicationName": app, "computePlatform": "Server",
		}), &created))
	idsRequireHexUUID(t, created.ApplicationID, "a CodeDeploy application id")
	idsJSONTargetCall(t, ts, idsCodeDeployHost, "CodeDeploy_20141006.GetApplication",
		map[string]any{"applicationName": app})

	var grouped struct {
		DeploymentGroupID string `json:"deploymentGroupId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost,
		"CodeDeploy_20141006.CreateDeploymentGroup", map[string]any{
			"applicationName": app, "deploymentGroupName": group,
			"serviceRoleArn": "arn:aws:iam::123456789012:role/CodeDeployRole",
		}), &grouped))
	idsRequireHexUUID(t, grouped.DeploymentGroupID, "a CodeDeploy deployment-group id")
	require.NotEqual(t, created.ApplicationID, grouped.DeploymentGroupID,
		"two requests minting the same shape still mint different values")
	idsJSONTargetCall(t, ts, idsCodeDeployHost, "CodeDeploy_20141006.GetDeploymentGroup",
		map[string]any{"applicationName": app, "deploymentGroupName": group})

	var deployed struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost,
		"CodeDeploy_20141006.CreateDeployment", map[string]any{
			"applicationName": app, "deploymentGroupName": group,
		}), &deployed))
	require.Regexp(t, `^d-[A-Z0-9]{9}$`, deployed.DeploymentID)
	idsJSONTargetCall(t, ts, idsCodeDeployHost, "CodeDeploy_20141006.GetDeployment",
		map[string]any{"deploymentId": deployed.DeploymentID})
}

// idsRecordCodePipeline records a pipeline, an execution of it, and the GetPipelineExecution that
// addresses the execution by the id StartPipelineExecution minted.
func idsRecordCodePipeline(t *testing.T, ts *emulator.TestServer) {
	t.Helper()

	const pipeline = "ids-tier6-pipeline"

	idsJSONTargetCall(t, ts, idsCodePipelineHost, "CodePipeline_20150709.CreatePipeline",
		map[string]any{"pipeline": map[string]any{
			"name":    pipeline,
			"roleArn": "arn:aws:iam::123456789012:role/CodePipelineRole",
		}})

	var started struct {
		PipelineExecutionID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodePipelineHost,
		"CodePipeline_20150709.StartPipelineExecution",
		map[string]any{"name": pipeline}), &started))
	idsRequireHexUUID(t, started.PipelineExecutionID, "a pipeline execution id")

	idsJSONTargetCall(t, ts, idsCodePipelineHost, "CodePipeline_20150709.GetPipelineExecution",
		map[string]any{
			"pipelineName":        pipeline,
			"pipelineExecutionId": started.PipelineExecutionID,
		})
}

// The hosts the tier-6 services are addressed at.
const (
	idsCodeBuildHost    = "codebuild.us-east-1.amazonaws.com"
	idsCodeDeployHost   = "codedeploy.us-east-1.amazonaws.com"
	idsCodePipelineHost = "codepipeline.us-east-1.amazonaws.com"
)
