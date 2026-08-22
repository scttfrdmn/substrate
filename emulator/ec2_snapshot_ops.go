package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// The two snapshot-creating operations beyond CreateSnapshot (#709).
//
// Through v0.106.0 CreateSnapshot was the whole of substrate's snapshot creation, so a
// consumer whose IaC snapshots an instance rather than a volume, or copies a snapshot before
// sharing it, reached the dispatcher's default arm and read InvalidAction — an answer that
// tells a caller the endpoint does not speak EC2 rather than that substrate has a gap.
//
// Provenance, which is the same for both operations: neither API_CreateSnapshots.html nor
// API_CopySnapshot.html publishes a single operation-specific error — both Errors sections
// point only at the common types — and CreateSnapshots has no Examples section at all. So
// every refusal below is either a code from EC2's client-error table (which describes the
// condition rather than quoting a wire message) or substrate's own reading of a documented
// rule AWS states in prose without naming a code. Each says which it is.

// ec2MaxExcludedDataVolumes is the number of volume IDs one CreateSnapshots request may
// exclude: "You can specify up to 40 volume IDs per request", from InstanceSpecification's own
// text.
const ec2MaxExcludedDataVolumes = 40

// ec2AttachedVolume pairs a volume with the device name it is attached at, which
// CreateSnapshots needs together: the device decides whether the volume is the instance's
// boot volume, and the volume carries the size and encryption the snapshot inherits.
type ec2AttachedVolume struct {
	volume EC2Volume
	device string
}

// ec2InstanceAttachedVolumes returns every volume attached to instanceID, ordered by device
// name.
//
// The ordering is not cosmetic. [StateManager.List] returns keys in Go map order, so the
// snapshotSet of a CreateSnapshots call over a multi-volume instance would otherwise come
// back in a different order on every run — and a test asserting on the first item, or a
// consumer reading snapshotSet[0], would pass or fail by iteration order. A device name is
// unique per instance, so sorting on it is a total order.
//
// A record that cannot be read or unmarshalled is skipped rather than failing the request,
// matching [EC2Plugin.ec2InstanceRootVolume]'s treatment of the same scan: one corrupt
// volume record should not make an instance unsnapshottable.
func (p *EC2Plugin) ec2InstanceAttachedVolumes(reqCtx *RequestContext, instanceID string) ([]ec2AttachedVolume, error) {
	ctx := context.Background()
	keys, err := p.state.List(ctx, ec2Namespace, ec2VolumeStatePrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("ec2 instance volumes list: %w", err)
	}
	var out []ec2AttachedVolume
	for _, key := range keys {
		data, getErr := p.state.Get(ctx, ec2Namespace, key)
		if getErr != nil || data == nil {
			continue
		}
		var vol EC2Volume
		if json.Unmarshal(data, &vol) != nil {
			continue
		}
		for _, att := range vol.Attachments {
			if att.InstanceID == instanceID {
				out = append(out, ec2AttachedVolume{volume: vol, device: att.Device})
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].device < out[j].device })
	return out, nil
}

// ec2SnapshotInfoItem is one member of a CreateSnapshots snapshotSet.
//
// It is a separate type from the item [EC2Plugin.describeSnapshots] renders, and cannot
// reuse [EC2Plugin.createSnapshot]'s response struct either, for one reason: AWS's
// SnapshotInfo names the state member **state**, where CreateSnapshot's and
// DescribeSnapshots' responses both name it **status**. A caller unmarshalling
// SnapshotInfo reads State; sharing a struct would hand them an empty string.
//
// The members substrate does not render are omissions with reasons rather than oversights:
//
//   - progress — substrate stores none, the same reason DescribeSnapshots and CreateSnapshot
//     render none. It arrives with the seedable progression in #715.
//   - availabilityZone — documented as "The Availability Zone or Local Zone of the
//     snapshots", and the singular CreateSnapshot response has no such member at all, so it
//     reads as the placement member for a Location=local snapshot. Substrate models only
//     regional snapshots, so rendering the source volume's AZ here would claim a local
//     snapshot that does not exist.
//   - outpostArn and sseType — neither Outposts nor a specific server-side encryption type is
//     modeled, and inventing "sse-ebs" would be indistinguishable to a caller from an
//     observation.
type ec2SnapshotInfoItem struct {
	SnapshotID  string       `xml:"snapshotId"`
	VolumeID    string       `xml:"volumeId,omitempty"`
	VolumeSize  int64        `xml:"volumeSize"`
	State       string       `xml:"state"`
	StartTime   string       `xml:"startTime"`
	OwnerID     string       `xml:"ownerId"`
	Description string       `xml:"description,omitempty"`
	Encrypted   bool         `xml:"encrypted"`
	Tags        []ec2TagItem `xml:"tagSet>item"`
}

// createSnapshots snapshots every EBS volume attached to one instance in a single call.
//
// # What is read
//
// InstanceSpecification is the one required parameter, and its InstanceId is required in
// turn. The instance is resolved against state, so an absent one answers
// InvalidInstanceID.NotFound rather than an empty snapshotSet — an empty set is what an
// instance with no volumes gets, and the two must not be confused.
//
// ExcludeBootVolume drops the volume attached at a root device name (see [ec2IsRootDevice]).
// ExcludeDataVolumeId.N — singular member, indexed, not a plural — drops named volumes, up
// to [ec2MaxExcludedDataVolumes] of them. Naming the root volume there is refused, because
// AWS refuses it: "If you specify the ID of the root volume, the request fails. To exclude
// the root volume, use ExcludeBootVolume." A volume ID that is not attached to the instance
// is *not* refused: it excludes nothing, which is what the parameter asks for.
//
// CopyTagsFromSource has the single valid value "volume", and any other value is refused
// rather than ignored. When it is set, each snapshot inherits its own source volume's tags —
// its own, not the instance's, since AWS scopes the copy to the source of that snapshot.
//
// Description and TagSpecification.N apply to every snapshot the call creates. Where a
// TagSpecification key collides with a copied volume tag the request's value wins, since a
// tag the caller wrote in this request is the more specific instruction; AWS settles neither
// the precedence nor the ordering, so both are substrate's and are stated here.
//
// Location and OutpostArn are not read, for the reason [EC2Plugin.createSnapshot] does not
// read them: both are documented as supported only for a Local Zone or an Outpost, and
// neither is modeled, so honoring them would place the snapshots somewhere substrate
// cannot describe them from. Location's own default is "regional", which is what substrate
// produces — but a request asking for "local" gets a regional snapshot rather than a
// refusal, exactly as the sibling operation behaves.
//
// # The state it reports
//
// state is "completed" at once, as CreateSnapshot's status is, and for the same reason:
// substrate advances no snapshot asynchronously, so a waiter succeeds on its first poll
// instead of depending on wall-clock time.
func (p *EC2Plugin) createSnapshots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instanceID := req.Params["InstanceSpecification.InstanceId"]
	if instanceID == "" {
		return nil, ec2MissingParameter("InstanceSpecification.InstanceId")
	}

	copyTags := req.Params["CopyTagsFromSource"]
	if copyTags != "" && copyTags != "volume" {
		return nil, ec2InvalidParameterValue("CopyTagsFromSource", copyTags,
			"The only supported value is volume.")
	}

	excluded := extractIndexedParams(req.Params, "InstanceSpecification.ExcludeDataVolumeId")
	if len(excluded) > ec2MaxExcludedDataVolumes {
		return nil, ec2InvalidParameterValue("InstanceSpecification.ExcludeDataVolumeId",
			strconv.Itoa(len(excluded)),
			fmt.Sprintf("You can specify up to %d volume IDs per request.", ec2MaxExcludedDataVolumes))
	}

	inst, awsErr, err := p.loadInstance(reqCtx, instanceID, "createSnapshots")
	if err != nil || awsErr != nil {
		return nil, errOrAWS(err, awsErr)
	}

	attached, err := p.ec2InstanceAttachedVolumes(reqCtx, inst.InstanceID)
	if err != nil {
		return nil, err
	}

	requestTags := ec2LaunchTagsForResource(req.Params, "snapshot")
	excludeBoot := req.Params["InstanceSpecification.ExcludeBootVolume"] == "true"

	// The root volume's ID is needed before anything is written, because naming it in
	// ExcludeDataVolumeId.N is a refusal and the rollback rule in [ec2CheckReservedTagKeys]
	// applies to every tag-on-create path: a refused request must leave no snapshot behind.
	rootVolumeID := ""
	for _, av := range attached {
		if ec2IsRootDevice(av.device) {
			rootVolumeID = av.volume.VolumeID
			break
		}
	}
	excludedIDs := make(map[string]struct{}, len(excluded))
	for _, id := range excluded {
		if rootVolumeID != "" && id == rootVolumeID {
			return nil, ec2InvalidParameterValue("InstanceSpecification.ExcludeDataVolumeId", id,
				"The root volume cannot be excluded by ID; use ExcludeBootVolume.")
		}
		excludedIDs[id] = struct{}{}
	}

	// Every snapshot is built and its tags checked before the first Put, for the rollback
	// rule above: a five-volume instance whose fourth volume carries a reserved tag key must
	// not leave three snapshots behind.
	now := p.tc.Now().UTC().Format(time.RFC3339)
	var snapshots []EC2Snapshot
	for _, av := range attached {
		if _, skip := excludedIDs[av.volume.VolumeID]; skip {
			continue
		}
		if excludeBoot && ec2IsRootDevice(av.device) {
			continue
		}
		tags := requestTags
		if copyTags == "volume" {
			tags = ec2MergeTags(av.volume.Tags, requestTags)
		}
		if tagErr := ec2CheckTagRules(tags); tagErr != nil {
			return nil, tagErr
		}
		if tagErr := ec2CheckTagLimit(nil, tags); tagErr != nil {
			return nil, tagErr
		}
		snapshots = append(snapshots, EC2Snapshot{
			SnapshotID:  generateEBSSnapshotID(),
			VolumeID:    av.volume.VolumeID,
			VolumeSize:  int64(av.volume.Size),
			State:       "completed",
			StartTime:   now,
			Encrypted:   av.volume.Encrypted,
			Description: req.Params["Description"],
			Tags:        tags,
			AccountID:   reqCtx.AccountID,
			Region:      reqCtx.Region,
		})
	}

	items := make([]ec2SnapshotInfoItem, 0, len(snapshots))
	for _, snap := range snapshots {
		if err := p.ec2PutSnapshot(reqCtx, snap); err != nil {
			return nil, err
		}
		items = append(items, ec2SnapshotInfoItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			VolumeSize:  snap.VolumeSize,
			State:       snap.State,
			StartTime:   snap.StartTime,
			OwnerID:     snap.AccountID,
			Description: snap.Description,
			Encrypted:   snap.Encrypted,
			Tags:        ec2TagItems(snap.Tags),
		})
	}

	// snapshotSet carries no omitempty: an instance with no volumes left to snapshot is
	// "none", and an SDK reads an omitted element as nil rather than as an empty slice.
	type response struct {
		XMLName   xml.Name              `xml:"CreateSnapshotsResponse"`
		XMLNS     string                `xml:"xmlns,attr"`
		Snapshots []ec2SnapshotInfoItem `xml:"snapshotSet>item"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:     "http://ec2.amazonaws.com/doc/2016-11-15/",
		Snapshots: items,
	})
}

// ec2MergeTags returns base with each of override's tags applied, replacing a tag of the
// same key in place rather than appending a duplicate.
//
// Order is base's, then any key override introduces, so the result is the same on every run
// for the same request — which a map-based merge would not be.
func ec2MergeTags(base, override []EC2Tag) []EC2Tag {
	out := make([]EC2Tag, len(base))
	copy(out, base)
	for _, t := range override {
		replaced := false
		for i := range out {
			if out[i].Key == t.Key {
				out[i].Value = t.Value
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, t)
		}
	}
	return out
}

// ec2PutSnapshot writes one snapshot record to state.
//
// It exists because four operations now write snapshots — CreateSnapshot, CreateSnapshots,
// CopySnapshot and the two attribute mutators — and a writer that spelled the key
// differently from [ec2SnapshotStateKey] would produce a snapshot DescribeSnapshots cannot
// see, which is the failure mode [ec2SnapshotStatePrefix]'s own doc comment records.
func (p *EC2Plugin) ec2PutSnapshot(reqCtx *RequestContext, snap EC2Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("ec2 snapshot marshal %s: %w", snap.SnapshotID, err)
	}
	key := ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, snap.SnapshotID)
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return fmt.Errorf("ec2 snapshot state.Put %s: %w", snap.SnapshotID, err)
	}
	return nil
}

// copySnapshot copies an existing snapshot within the region.
//
// # Why a single-region emulator can implement a copy at all
//
// AWS's own description settles it twice. "If the source snapshot is in a Region, you can
// copy it within that Region" makes a same-region copy legal rather than a degenerate case,
// and its Example 1 is exactly that. And DestinationRegion is not routing: "This parameter is
// only valid for specifying the destination Region in a PresignedUrl parameter, where it is
// required … The snapshot copy is sent to the regional endpoint that you sent the HTTP
// request to." So the destination of a copy is the endpoint it was sent to, which is the one
// region substrate serves.
//
// A SourceRegion naming any other region is refused with
// SnapshotCopyUnsupported.InterRegion, whose published description — "Inter-region snapshot
// copy is not supported for this AWS Region" — is precisely substrate's situation. That is a
// code from the client-error table rather than a captured message; the wording below is
// substrate's.
//
// # What is read
//
// SourceRegion and SourceSnapshotId are both required. The source is resolved against state,
// so a malformed ID answers InvalidSnapshotID.Malformed and an absent one
// InvalidSnapshot.NotFound.
//
// Encrypted is accepted only as true. AWS: "Copies of encrypted snapshots are encrypted,
// even if you omit this parameter … You cannot set this parameter to false." A copy of an
// encrypted snapshot is therefore encrypted whatever the request says, and an explicit
// Encrypted=false is refused rather than silently honored or silently dropped — substrate's
// reading of that sentence, since AWS names no code for it.
//
// KmsKeyId is validated but not stored: "If KmsKeyId is specified, the encrypted state must
// be true", so naming a key for an unencrypted copy is refused. Substrate models no KMS key
// on a snapshot, so there is nothing for a caller to read back, and the rule is observable
// only as that refusal.
//
// Description is the request's, empty when absent. AWS does not document what an omitted
// description produces, so substrate stores nothing rather than inventing a "Copied from …"
// string a consumer might assert on.
//
// Tags are the request's TagSpecification.N alone. CopySnapshot has no CopyTagsFromSource —
// that parameter is CreateSnapshots' — so the source's tags are deliberately not carried
// over.
//
// CompletionDurationMinutes, DestinationAvailabilityZone, DestinationOutpostArn,
// DestinationRegion and PresignedUrl are not read. The first is a time-based copy, which has
// nothing to slow down here; the next two place the copy somewhere substrate does not model;
// and the last two are artifacts of signing a cross-region request, which cannot arise.
//
// # The volume ID
//
// The copy records a freshly generated volume ID that names no volume, which is what AWS
// produces: "Snapshots copies have an arbitrary source volume ID. Do not use this volume ID
// for any purpose." Rendering the *source's* volume ID would hand a caller a reference that
// resolves, inviting exactly the use AWS warns against; omitting the member entirely would
// drop an element AWS always sends. An arbitrary ID keeps the shape and makes the warning
// self-enforcing — any operation a caller passes it to answers InvalidVolume.NotFound.
func (p *EC2Plugin) copySnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	sourceRegion := req.Params["SourceRegion"]
	if sourceRegion == "" {
		return nil, ec2MissingParameter("SourceRegion")
	}
	sourceID := req.Params["SourceSnapshotId"]
	if sourceID == "" {
		return nil, ec2MissingParameter("SourceSnapshotId")
	}
	if sourceRegion != reqCtx.Region {
		return nil, &AWSError{
			Code: "SnapshotCopyUnsupported.InterRegion",
			Message: fmt.Sprintf(
				"Inter-region snapshot copy is not supported: the source region %s is not %s.",
				sourceRegion, reqCtx.Region),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	if encrypted, ok := req.Params["Encrypted"]; ok && encrypted != "true" {
		return nil, ec2InvalidParameterValue("Encrypted", encrypted,
			"You cannot set this parameter to false.")
	}
	requestedEncryption := req.Params["Encrypted"] == "true"

	source, found := p.ec2SnapshotResolver(reqCtx)(sourceID)
	if awsErr := ec2RequireResource(ec2SnapshotIDKind, sourceID, found); awsErr != nil {
		return nil, awsErr
	}

	encrypted := source.Encrypted || requestedEncryption
	if kms := req.Params["KmsKeyId"]; kms != "" && !encrypted {
		return nil, ec2InvalidParameterValue("KmsKeyId", kms,
			"If KmsKeyId is specified, the encrypted state must be true.")
	}

	tags := ec2LaunchTagsForResource(req.Params, "snapshot")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}

	snap := EC2Snapshot{
		SnapshotID:  generateEBSSnapshotID(),
		VolumeID:    generateVolumeID(),
		VolumeSize:  source.VolumeSize,
		State:       "completed",
		StartTime:   p.tc.Now().UTC().Format(time.RFC3339),
		Encrypted:   encrypted,
		Description: req.Params["Description"],
		Tags:        tags,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	if err := p.ec2PutSnapshot(reqCtx, snap); err != nil {
		return nil, err
	}

	// snapshotId and tagSet are the whole of the documented response beyond requestId — no
	// status, no volumeSize — so a caller polls DescribeSnapshots for the rest.
	type response struct {
		XMLName    xml.Name     `xml:"CopySnapshotResponse"`
		XMLNS      string       `xml:"xmlns,attr"`
		SnapshotID string       `xml:"snapshotId"`
		Tags       []ec2TagItem `xml:"tagSet>item"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		SnapshotID: snap.SnapshotID,
		Tags:       ec2TagItems(snap.Tags),
	})
}

// ec2InvalidParameterValue returns EC2's InvalidParameterValue for a parameter whose value
// the operation will not accept, in the shape the one captured EC2 example of this code uses
// — "Value (x) for parameter y is invalid." — with a sentence saying why appended.
//
// The captured half is the prefix, from the capture [ec2UnknownInstanceAttribute] records.
// The trailing sentence is substrate's, and in every caller below it is AWS's own prose rule
// quoted or paraphrased, because that rule is the only thing that tells a caller what to
// send instead and none of these refusals is published with a message.
func ec2InvalidParameterValue(param, value, why string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("Value (%s) for parameter %s is invalid. %s", value, param, why),
		HTTPStatus: http.StatusBadRequest,
	}
}
