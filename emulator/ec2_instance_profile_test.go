package emulator_test

// An instance's iamInstanceProfile id is resolved, not drawn per read (#1291).
//
// The bug these tests pin was invisible to every existing assertion because the value was
// well-formed on every call: `ec2InstanceItemFor` minted `"AIPA" + randomHex(8)` each time it
// rendered an instance, so a DescribeInstances answered 200 with a plausible id that was
// different from the one the previous DescribeInstances answered with. Nothing in a single
// response is wrong, which is why the assertion has to be made **twice** — that is the shape of
// the whole file: call, call again, compare.
//
// These run against the full test server rather than the EC2-only rig, because the fix is a
// cross-service resolution: IAM has to be reachable for a profile to be created through it.

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ipEC2Host = "ec2.us-east-1.amazonaws.com"
	ipIAMHost = "iam.amazonaws.com"
	// ipAccount is the account StartTestServer serves, so a bare profile name resolves here.
	ipAccount = "123456789012"
	// ipAIPAShape is IAM's own rendering: 21 characters, `[A-Z0-9]` behind the prefix. Both it
	// and the old lowercase-hex form satisfy API_InstanceProfile's published `[\w]+` and its
	// 16–128 length, so the shape is asserted against substrate's IAM plugin rather than
	// against a pattern AWS publishes for it.
	ipAIPAShape = `^AIPA[A-Z0-9]{17}$`
)

// ipInstanceProfile launches nothing; it reads the profile element off one instance.
func ipInstanceProfile(t *testing.T, ts *emulator.TestServer, instID string) (arn, id string) {
	t.Helper()
	body := idsQueryCall(t, ts, ipEC2Host, "2016-11-15", map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instID,
	})
	var out struct {
		Instances []struct {
			Profile *struct {
				ARN string `xml:"arn"`
				ID  string `xml:"id"`
			} `xml:"iamInstanceProfile"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &out))
	require.Len(t, out.Instances, 1, "DescribeInstances: %s", body)
	if out.Instances[0].Profile == nil {
		return "", ""
	}
	return out.Instances[0].Profile.ARN, out.Instances[0].Profile.ID
}

// ipRunInstance launches one instance with the given IamInstanceProfile parameter, and returns
// its id alongside the profile element RunInstances itself reported.
func ipRunInstance(t *testing.T, ts *emulator.TestServer, param, value string) (instID, arn, id string) {
	t.Helper()
	params := map[string]string{
		"Action":       "RunInstances",
		"ImageId":      ec2TestImage,
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
	}
	if param != "" {
		params[param] = value
	}
	body := idsQueryCall(t, ts, ipEC2Host, "2016-11-15", params)
	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			Profile    *struct {
				ARN string `xml:"arn"`
				ID  string `xml:"id"`
			} `xml:"iamInstanceProfile"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &out))
	require.Len(t, out.Instances, 1, "RunInstances: %s", body)
	if out.Instances[0].Profile == nil {
		return out.Instances[0].InstanceID, "", ""
	}
	return out.Instances[0].InstanceID, out.Instances[0].Profile.ARN, out.Instances[0].Profile.ID
}

// ipCreateInstanceProfile creates a profile through IAM and returns the id IAM stored.
func ipCreateInstanceProfile(t *testing.T, ts *emulator.TestServer, name, path string) string {
	t.Helper()
	req := map[string]string{"InstanceProfileName": name}
	if path != "" {
		req["Path"] = path
	}
	body := idsJSONTargetCall(t, ts, ipIAMHost,
		"AmazonIdentityManagementService.CreateInstanceProfile", req)
	var out struct {
		ID  string `xml:"CreateInstanceProfileResult>InstanceProfile>InstanceProfileId"`
		ARN string `xml:"CreateInstanceProfileResult>InstanceProfile>Arn"`
	}
	require.NoError(t, xml.Unmarshal(body, &out))
	require.NotEmpty(t, out.ID, "CreateInstanceProfile: %s", body)
	return out.ID
}

// TestEC2_InstanceProfileIDIsStableAcrossReads is the acceptance criterion the drawn id could
// not meet: a read repeated is a read unchanged.
//
// A consumer that describes an instance twice and compares — a drift check, a Terraform
// refresh, a cache — saw a change that did not happen, and the response was a 200 both times.
func TestEC2_InstanceProfileIDIsStableAcrossReads(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	instID, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "stable-profile")
	require.NotEmpty(t, runID, "RunInstances must report the profile it was given")

	firstARN, firstID := ipInstanceProfile(t, ts, instID)
	secondARN, secondID := ipInstanceProfile(t, ts, instID)

	assert.Equal(t, firstID, secondID, "two describes of one instance report one profile id")
	assert.Equal(t, firstARN, secondARN, "and one ARN")
	assert.Equal(t, runID, firstID, "RunInstances and DescribeInstances agree on the id")
	assert.Equal(t, runARN, firstARN, "and on the ARN")
	assert.Regexp(t, ipAIPAShape, firstID, "rendered the way IAM renders its own: %q", firstID)
}

// TestEC2_InstanceProfileIDIsTheOneIAMStored is the half that makes the id mean something.
//
// Before this the value was unrelated to the AIPA… id IAM minted for the profile, so a caller
// that took the id from a describe and handed it to IAM got a not-found.
func TestEC2_InstanceProfileIDIsTheOneIAMStored(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	stored := ipCreateInstanceProfile(t, ts, "resolved-profile", "")
	instID, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "resolved-profile")

	assert.Equal(t, stored, runID, "RunInstances reports the id IAM stored")
	_, describedID := ipInstanceProfile(t, ts, instID)
	assert.Equal(t, stored, describedID, "and so does DescribeInstances")
	assert.Equal(t,
		"arn:aws:iam::"+ipAccount+":instance-profile/resolved-profile", runARN,
		"with the ARN IAM stored")
}

// TestEC2_InstanceProfileARNCarriesTheStoredPath pins what resolving buys beyond the id. A
// synthesized ARN cannot know a path, so a profile created at /dev/ was reported at the root —
// an ARN that names nothing, in the member a caller passes to iam:PassRole conditions.
func TestEC2_InstanceProfileARNCarriesTheStoredPath(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	ipCreateInstanceProfile(t, ts, "pathed-profile", "/dev/")
	_, runARN, _ := ipRunInstance(t, ts, "IamInstanceProfile.Name", "pathed-profile")

	assert.Equal(t, "arn:aws:iam::"+ipAccount+":instance-profile/dev/pathed-profile", runARN)
}

// TestEC2_InstanceProfileIDIsDerivedWhenIAMHasNoProfile covers the case substrate permits and
// AWS does not: an instance launched with a profile name that was never created.
//
// Deriving rather than drawing is what keeps the stability property for that instance too, and
// two names must not collide onto one id.
func TestEC2_InstanceProfileIDIsDerivedWhenIAMHasNoProfile(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	firstInst, _, firstID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "never-created")
	require.Regexp(t, ipAIPAShape, firstID, "a derived id is indistinguishable from a stored one")

	_, again := ipInstanceProfile(t, ts, firstInst)
	assert.Equal(t, firstID, again, "a derived id is stable across reads")

	_, _, otherID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "also-never-created")
	assert.NotEqual(t, firstID, otherID, "two profiles do not share an id")

	// Same name, second instance: the id belongs to the profile, not to the instance, so it
	// must repeat — which is also what makes it the id IAM would report if the profile existed.
	_, _, sameName := ipRunInstance(t, ts, "IamInstanceProfile.Name", "never-created")
	assert.Equal(t, firstID, sameName, "one profile name is one id, whichever instance names it")
}

// TestEC2_InstanceProfileARNResolvesInItsOwnAccount pins #826's rule on this resolution: the
// account comes from the ARN, never from the caller's context.
//
// Without it, an instance launched with another account's profile ARN reported the *caller's*
// same-named profile's id — a cross-account read that answers with local state, which is the
// failure mode #826 exists to prevent rather than a cosmetic one.
func TestEC2_InstanceProfileARNResolvesInItsOwnAccount(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	local := ipCreateInstanceProfile(t, ts, "shared-name", "")
	const foreign = "arn:aws:iam::999999999999:instance-profile/shared-name"
	_, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Arn", foreign)

	assert.Equal(t, foreign, runARN, "the ARN the launch named is reported verbatim")
	assert.NotEqual(t, local, runID,
		"another account's profile must not resolve to the caller's same-named one")
	assert.Regexp(t, ipAIPAShape, runID, "and is derived instead: %q", runID)

	// The local profile still resolves for an instance that names it, so the guard did not
	// simply stop resolving.
	_, _, localID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "shared-name")
	assert.Equal(t, local, localID)
}

// TestReplay_AnInstanceProfileReplaysWithTheIDIAMStored is the assertion in #1266's set that could
// not be made before the resolution: the id was drawn while *rendering*, so a replayed
// `RunInstances` and a replayed `DescribeInstances` each answered a new `AIPA…` — a body difference
// in two events, and a state-hash difference in neither, because nothing stored the value.
//
// It is deliberately the weakest of this file's assertions, and worth saying why rather than
// implying it catches something the others do not. Stability across two describes *in one run* is
// the stronger property: the resolution's inputs are the instance's recorded profile name and IAM's
// own record, both of which a replay restores by re-executing the recorded creates, so any
// regression that moved the value across a replay would move it across two describes first and trip
// this test's own precondition. Sabotaging the resolver back to a per-read draw fails at that
// precondition, not at the replay comparison, and so does deriving the id from the request ID.
//
// What it does guard is the migration #1266 named as its third option — storing a resolved id on
// the instance record at launch and making the describe a pure render. That id would land in
// `state_hash_after`, where a re-minted one diverges on a replay while two describes in one run
// still agree, and this is the test that would say so.
func TestReplay_AnInstanceProfileReplaysWithTheIDIAMStored(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	// Frozen, for the reason idsRecordInterlockedCreates records: a replay sets the clock to the
	// recorded event's own timestamp, and an unfrozen handler stamps a record just after it.
	ts.FreezeTime()

	// A path, so the ARN under comparison is one a synthesized value could not have produced.
	ipCreateInstanceProfile(t, ts, "replayed-profile", "/dev/")
	instID, _, runID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "replayed-profile")
	require.NotEmpty(t, runID, "the recorded launch has to report a profile")
	_, describedID := ipInstanceProfile(t, ts, instID)
	require.Equal(t, runID, describedID, "precondition: the recording itself is consistent")

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the launch and the describe")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"a replayed describe reports the id its recording reported: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// TestEC2_InstanceProfileARNWithAPathResolves pins the ARN direction of the path handling. IAM
// keys a profile by its name alone, but an ARN for a profile at a path spells the path out —
// `instance-profile/team/app` — so the name is the last segment and not the whole resource. Taking
// the resource verbatim would look up `team/app`, find nothing, and derive an id for a profile IAM
// has a record of.
func TestEC2_InstanceProfileARNWithAPathResolves(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	stored := ipCreateInstanceProfile(t, ts, "pathed-by-arn", "/team/")
	arn := "arn:aws:iam::" + ipAccount + ":instance-profile/team/pathed-by-arn"
	_, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Arn", arn)

	assert.Equal(t, stored, runID, "the ARN's last segment is the profile's name")
	assert.Equal(t, arn, runARN)
}

// TestEC2_InstanceProfileARNWithoutAnAccountResolvesForTheCaller covers the other side of #826's
// rule: the account comes from the ARN *when the ARN has one*. An ARN that omits the account field
// names no other account, so the caller's is the only one it can mean — and falling back is what
// keeps a profile the caller created reachable through such an ARN instead of silently derived.
func TestEC2_InstanceProfileARNWithoutAnAccountResolvesForTheCaller(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	stored := ipCreateInstanceProfile(t, ts, "accountless", "")
	_, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Arn",
		"arn:aws:iam:::instance-profile/accountless")

	assert.Equal(t, stored, runID)
	assert.Equal(t, "arn:aws:iam::"+ipAccount+":instance-profile/accountless", runARN,
		"a resolved profile reports the ARN IAM stored, account included")
}

// TestEC2_InstanceProfileARNNamingNoNameIsNotLookedUp pins that an ARN ending at the resource type
// resolves nothing rather than reading state under a blank name, which is a key that could match
// another record as substrate's key layout changes.
func TestEC2_InstanceProfileARNNamingNoNameIsNotLookedUp(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	const arn = "arn:aws:iam::" + ipAccount + ":instance-profile/"
	_, runARN, runID := ipRunInstance(t, ts, "IamInstanceProfile.Arn", arn)

	assert.Equal(t, arn, runARN, "the ARN the launch named is still reported verbatim")
	assert.Regexp(t, ipAIPAShape, runID, "and the id is derived: %q", runID)
}

// TestEC2_InstanceProfileIDIsDerivedWhenIAMsRecordIsUnusable covers a record the IAM plugin would
// not write, which a replayed event log restoring state written by an older substrate can present:
// bytes that do not parse, and a record that parses but carries no id or ARN.
//
// Reporting what such a record holds would put an empty `id` into a member a caller reads, so the
// resolution treats it as absent and derives — the same answer as for a profile that was never
// created, which is what it effectively is.
func TestEC2_InstanceProfileIDIsDerivedWhenIAMsRecordIsUnusable(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ctx := context.Background()

	ns, key := emulator.IAMInstanceProfileStateKeyForTest(ipAccount, "unparseable")
	require.NoError(t, ts.StateManager().Put(ctx, ns, key, []byte("{not json")))
	_, _, unparseableID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "unparseable")
	assert.Regexp(t, ipAIPAShape, unparseableID, "derived, not empty: %q", unparseableID)

	ns, key = emulator.IAMInstanceProfileStateKeyForTest(ipAccount, "fieldless")
	require.NoError(t, ts.StateManager().Put(ctx, ns, key,
		[]byte(`{"InstanceProfileName":"fieldless"}`)))
	_, fieldlessARN, fieldlessID := ipRunInstance(t, ts, "IamInstanceProfile.Name", "fieldless")
	assert.Regexp(t, ipAIPAShape, fieldlessID, "derived, not empty: %q", fieldlessID)
	assert.Equal(t, "arn:aws:iam::"+ipAccount+":instance-profile/fieldless", fieldlessARN,
		"and the ARN is the one the bare name implies, not the record's empty one")
}

// TestEC2_AnInstanceWithNoProfileReportsNoProfileElement pins the absent case, which the old
// code got right by accident of an `if` and which the resolver now has to decide.
//
// `iamInstanceProfile` is Required: No, so an empty element would claim the instance carries a
// profile whose id and ARN are blank.
func TestEC2_AnInstanceWithNoProfileReportsNoProfileElement(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	instID, arn, id := ipRunInstance(t, ts, "", "")
	assert.Empty(t, arn)
	assert.Empty(t, id)

	describedARN, describedID := ipInstanceProfile(t, ts, instID)
	assert.Empty(t, describedARN)
	assert.Empty(t, describedID)
}
