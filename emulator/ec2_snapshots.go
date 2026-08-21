package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

// ec2SnapshotResolver returns a lookup from snapshot ID to the stored snapshot, for the
// account and region of one request.
//
// It exists so the block-device-mapping validator can check a snapshot's existence and
// size while staying a pure function over its mappings: it takes this lookup rather than
// a [StateManager], which keeps it unit-testable with no state fixture and lets a caller
// that has already resolved the snapshots supply them directly.
//
// A read error reports the snapshot as absent rather than propagating, matching
// [EC2Plugin.ec2CheckSecurityGroups] and [EC2Plugin.resourceTags]: substrate's state
// backends fail only for reasons a caller cannot act on, and reporting "not found" for an
// unreadable record is the same answer a caller gets for one that was never written.
func (p *EC2Plugin) ec2SnapshotResolver(reqCtx *RequestContext) func(string) (EC2Snapshot, bool) {
	return func(snapshotID string) (EC2Snapshot, bool) {
		if snapshotID == "" {
			return EC2Snapshot{}, false
		}
		key := ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, snapshotID)
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			return EC2Snapshot{}, false
		}
		var snap EC2Snapshot
		if json.Unmarshal(data, &snap) != nil {
			return EC2Snapshot{}, false
		}
		return snap, true
	}
}

// createSnapshot creates an EBS snapshot of a volume the account holds.
//
// Before #689 substrate had no CreateSnapshot at all: the only snapshots it held were the
// ones CreateImage mints for an AMI's root device, every one of them recording
// [EC2Snapshot.VolumeSize] as the literal 8. So a consumer could neither create a
// snapshot of a known size nor observe one, which made every size-dependent rule — the
// filter on volume-size, the mapping whose size must not be smaller than its snapshot's —
// a comparison against a constant.
//
// # What is read
//
// VolumeId is the one required parameter and is checked against state, so a snapshot
// cannot exist for a volume that does not. Description and TagSpecification.N are read;
// the tags come through [ec2LaunchTagsForResource] scoped to "snapshot", the same walk
// CreateImage already uses for the snapshots it mints, and they are checked before
// anything is written, per the rollback rule in [ec2CheckReservedTagKeys].
//
// VolumeSize and Encrypted come from the source volume rather than from the request,
// which is what AWS documents: volumeSize is "the size of the volume, in GiB", and
// "snapshots that are taken from encrypted volumes are automatically encrypted".
//
// Location and OutpostArn are not read. Both are documented as supported only for volumes
// in a Local Zone or on an Outpost, neither of which substrate models, so honoring them
// would place the snapshot somewhere substrate cannot describe it from.
//
// # The state it reports
//
// status is "completed" immediately, not the "pending" AWS's own sample response shows.
// Substrate advances no snapshot asynchronously — CreateImage's snapshots have always
// been born completed — so a caller's waiter succeeds on its first poll rather than
// depending on wall-clock time. A seedable pending → completed progression is the shape
// that would let a test exercise the waiting path, and it is a follow-up rather than
// something this operation should invent.
//
// progress is not rendered, for the reason describeSnapshots does not render it either:
// substrate stores none, and status already carries the whole of what a completed
// snapshot's progress would say.
func (p *EC2Plugin) createSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	volumeID := req.Params["VolumeId"]
	if volumeID == "" {
		return nil, ec2MissingParameter("VolumeId")
	}

	vol, found, err := p.ec2Volume(reqCtx, volumeID)
	if err != nil {
		return nil, err
	}
	if awsErr := ec2RequireResource(ec2VolumeIDKind, volumeID, found); awsErr != nil {
		return nil, awsErr
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
		VolumeID:    vol.VolumeID,
		VolumeSize:  int64(vol.Size),
		State:       "completed",
		StartTime:   p.tc.Now().UTC().Format(time.RFC3339),
		Encrypted:   vol.Encrypted,
		Description: req.Params["Description"],
		Tags:        tags,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("ec2 createSnapshot marshal: %w", err)
	}
	key := ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, snap.SnapshotID)
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createSnapshot state.Put: %w", err)
	}

	// Member order follows AWS's own sample response — snapshotId, volumeId, status,
	// startTime, ownerId, volumeSize, description — with the two documented elements it
	// does not show appended. tagSet carries no omitempty for the reason CreateVolume's
	// does not: an SDK maps a present-but-empty element to an empty slice where it maps an
	// omitted one to nil, so omitting it would report "unknown" where AWS reports "none".
	type response struct {
		XMLName     xml.Name     `xml:"CreateSnapshotResponse"`
		XMLNS       string       `xml:"xmlns,attr"`
		SnapshotID  string       `xml:"snapshotId"`
		VolumeID    string       `xml:"volumeId"`
		Status      string       `xml:"status"`
		StartTime   string       `xml:"startTime"`
		OwnerID     string       `xml:"ownerId"`
		VolumeSize  int64        `xml:"volumeSize"`
		Description string       `xml:"description,omitempty"`
		Encrypted   bool         `xml:"encrypted"`
		Tags        []ec2TagItem `xml:"tagSet>item"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:       "http://ec2.amazonaws.com/doc/2016-11-15/",
		SnapshotID:  snap.SnapshotID,
		VolumeID:    snap.VolumeID,
		Status:      snap.State,
		StartTime:   snap.StartTime,
		OwnerID:     snap.AccountID,
		VolumeSize:  snap.VolumeSize,
		Description: snap.Description,
		Encrypted:   snap.Encrypted,
		Tags:        ec2TagItems(snap.Tags),
	})
}

// ec2Volume loads one volume by ID, reporting whether it exists.
//
// A read error is returned rather than swallowed, unlike [EC2Plugin.ec2SnapshotResolver]'s
// treatment of the same failure: this is the source of a snapshot a caller asked for by
// ID, so answering "no such volume" for a volume that is there would report a state
// failure as the caller's mistake.
func (p *EC2Plugin) ec2Volume(reqCtx *RequestContext, volumeID string) (EC2Volume, bool, error) {
	key := ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, volumeID)
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return EC2Volume{}, false, fmt.Errorf("ec2 volume get %s: %w", volumeID, err)
	}
	if data == nil {
		return EC2Volume{}, false, nil
	}
	var vol EC2Volume
	if err := json.Unmarshal(data, &vol); err != nil {
		return EC2Volume{}, false, fmt.Errorf("ec2 volume unmarshal %s: %w", volumeID, err)
	}
	return vol, true, nil
}

// ec2InstanceRootVolume is the ID and size in GiB of the volume attached to instanceID at
// a root device name. It reports an empty ID and [ec2DefaultVolumeSizeGiB] when the
// instance has no root volume in state.
//
// CreateImage needs both, because the snapshot it mints backs the AMI's root device: its
// size was the literal 8 through v0.105.0 — so an AMI made from an instance whose root
// volume was 40 GiB reported 8, and a consumer restoring from that AMI sized their volume
// off a constant — and it recorded no source volume at all, which left the volume-id
// filter #685 added with nothing to select on for the only snapshots substrate could
// produce.
//
// The fallback is not defensive padding: an instance written straight into state by a test
// has no volumes at all, and every launch since #666 materializes a root volume, so the
// fallback is reached only where there is nothing better to read. It is the same constant
// DescribeImages fabricates a mapping for, so the two still agree in that case.
func (p *EC2Plugin) ec2InstanceRootVolume(reqCtx *RequestContext, instanceID string) (volumeID string, sizeGiB int64, err error) {
	ctx := context.Background()
	keys, err := p.state.List(ctx, ec2Namespace, ec2VolumeStatePrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return "", 0, fmt.Errorf("ec2 root volume list: %w", err)
	}
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
			if att.InstanceID == instanceID && ec2IsRootDevice(att.Device) {
				return vol.VolumeID, int64(vol.Size), nil
			}
		}
	}
	return "", ec2DefaultVolumeSizeGiB, nil
}
