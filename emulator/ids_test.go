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
