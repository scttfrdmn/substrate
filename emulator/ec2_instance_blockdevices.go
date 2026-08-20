package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// ec2BlockDeviceMappingItem is one blockDeviceMapping>item as an *instance* renders
// it, mirroring AWS's InstanceBlockDeviceMapping.
//
// The shape is exactly deviceName plus an ebs sub-element. It is not the shape
// RunInstances and CreateLaunchTemplate *accept* — AWS's request-side
// BlockDeviceMapping carries virtualName, noDevice and a sized EbsBlockDevice, none
// of which appear here — so the two are deliberately separate types rather than one
// reused in both directions.
//
// Declared once at package level for the reason [ec2GroupItem] is: three surfaces
// render this set, and three private copies is exactly how the instance items drifted
// apart before (#444).
type ec2BlockDeviceMappingItem struct {
	// DeviceName is the device the volume is attached at, taken from the attachment
	// rather than from the recorded request.
	DeviceName string `xml:"deviceName"`

	// Ebs is the attached volume. AWS's InstanceBlockDeviceMapping has no other
	// member, so there is nothing to omit beside it.
	Ebs ec2EbsInstanceBlockDeviceItem `xml:"ebs"`
}

// ec2EbsInstanceBlockDeviceItem is AWS's EbsInstanceBlockDevice.
//
// AWS's shape carries eight members — associatedResource, attachTime,
// deleteOnTermination, ebsCardIndex, operator, status, volumeId and volumeOwnerId —
// and notably **no size**, which is why rendering this set does not answer the
// question #666 made DescribeVolumes answer. The four rendered here are the four
// AWS's own sample response shows, in its order, and each one is read straight off a
// stored attachment. The other four are omitted rather than defaulted: substrate
// records nothing behind them, and a fabricated volumeOwnerId is indistinguishable to
// a caller from a real one.
type ec2EbsInstanceBlockDeviceItem struct {
	VolumeID string `xml:"volumeId"`

	// Status is the attachment's state, which uses the four-value AttachmentStatus
	// enum — attaching, attached, detaching, detached. DescribeVolumes' volume-side
	// status is a *five*-value enum that adds busy, so the two must not be rendered
	// through one shared helper.
	Status string `xml:"status"`

	AttachTime          string `xml:"attachTime"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

// ec2BlockDeviceMappingSetXML wraps the mapping items so
// DescribeInstanceAttribute can omit the element entirely.
//
// It exists for the reason [ec2GroupSetXML] does: encoding/xml emits the parent
// element of a `blockDeviceMapping>item` path even when the slice is empty, so a
// caller asking for userData would be told the instance has no block devices. A nil
// pointer is skipped outright.
type ec2BlockDeviceMappingSetXML struct {
	Items []ec2BlockDeviceMappingItem `xml:"item"`
}

// ec2BlockDeviceMappingsByInstance returns every instance's block device mappings in
// one account and region, keyed by instance ID.
//
// The set is derived from the **volume records**, not from the mappings a launch
// recorded, which is what makes it track reality: a volume attached after launch
// appears, a detached one stops appearing, and an instance store device never appears
// because it never became a volume (#666's rule for DescribeVolumes, inherited here
// for free). EC2Instance carries no block-device field at all, so the volumes are
// also the only available source.
//
// The whole set is read once and bucketed rather than listed per instance:
// DescribeInstances already does one List plus a Get per security group per instance,
// and a per-instance List would compound that into an instances × volumes scan.
//
// Each instance's items are ordered by device name. AWS states outright that its own
// order may vary, so no fidelity claim rests on this — but state-listing order is not
// a promise substrate makes, and a deterministic emulator answering one request two
// ways would break the guarantee the project exists for.
func (p *EC2Plugin) ec2BlockDeviceMappingsByInstance(
	accountID, region string,
) (map[string][]ec2BlockDeviceMappingItem, error) {
	ctx := context.Background()
	keys, err := p.state.List(ctx, ec2Namespace, ec2VolumeStatePrefix(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("ec2 block device mapping volume list: %w", err)
	}

	byInstance := make(map[string][]ec2BlockDeviceMappingItem)
	for _, key := range keys {
		data, getErr := p.state.Get(ctx, ec2Namespace, key)
		if getErr != nil || data == nil {
			continue
		}
		var vol EC2Volume
		if json.Unmarshal(data, &vol) != nil {
			continue
		}
		// One volume can carry several attachments in the record's shape, so each is
		// mapped independently rather than only the first: a volume attached to two
		// instances must appear on both, and skipping past the first would silently
		// hide the second.
		for _, att := range vol.Attachments {
			if att.InstanceID == "" {
				continue
			}
			byInstance[att.InstanceID] = append(byInstance[att.InstanceID],
				ec2BlockDeviceMappingItem{
					DeviceName: att.Device,
					Ebs: ec2EbsInstanceBlockDeviceItem{
						VolumeID:            vol.VolumeID,
						Status:              att.State,
						AttachTime:          att.AttachTime,
						DeleteOnTermination: att.DeleteOnTermination,
					},
				})
		}
	}
	for _, items := range byInstance {
		sort.Slice(items, func(i, j int) bool {
			// Volume ID breaks a device-name tie, so two volumes claiming one device
			// — which state can hold even though real EC2 cannot — still order
			// stably rather than by whichever the listing reached first.
			if items[i].DeviceName != items[j].DeviceName {
				return items[i].DeviceName < items[j].DeviceName
			}
			return items[i].Ebs.VolumeID < items[j].Ebs.VolumeID
		})
	}
	return byInstance, nil
}
