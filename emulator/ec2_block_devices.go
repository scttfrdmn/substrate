package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Root device names. Substrate recognizes a mapping as configuring the root volume
// by device name, because it has no per-AMI root device to compare against:
// EC2Image records none, and DescribeImages fabricates /dev/sda1 for every AMI.
//
// Both spellings are AWS's own. Its device naming reference gives the HVM root
// device as "Differs by AMI — /dev/sda1 or /dev/xvda" and the paravirtual one as
// /dev/sda1, so a request naming either is naming the root. An AMI whose real root
// device is neither is out of reach here; that is a documented limit rather than a
// silent one.
const (
	ec2RootDeviceSDA1 = "/dev/sda1"
	ec2RootDeviceXVDA = "/dev/xvda"
)

// ec2DefaultVolumeSizeGiB is the size of a volume whose mapping names none, and of
// the implicit root volume a launch that declares no mapping still gets.
//
// It matches both the mapping DescribeImages fabricates for every AMI and
// CreateVolume's own default, so a launch-created volume and a CreateVolume-created
// one agree about what "unspecified" means.
const ec2DefaultVolumeSizeGiB = 8

// ec2DefaultVolumeType is the type of a volume whose mapping names none.
//
// AWS's EbsBlockDevice documents no default; CreateVolume documents gp2. Substrate
// uses gp2 for both so there is one default in one place rather than two that can
// drift. Real AMIs specify gp3 in their own block device mapping, but substrate's
// AMIs carry no mapping to inherit one from.
const ec2DefaultVolumeType = "gp2"

// ec2ParseBlockDeviceMappings collects every BlockDeviceMapping.N specification
// under prefix, contiguously from N=1 and stopping at the first index that names
// nothing.
//
// prefix is "" for a RunInstances request and "LaunchTemplateData." for a
// template's, so both paths parse the same shape by the same rules rather than by
// two hand-rolled readers that can drift — the same argument
// [ec2ParseNetworkInterfaces] takes, and for the same reason.
//
// Before #666 neither path parsed this at all. A request naming
// BlockDeviceMapping.1.Ebs.VolumeSize=60 was accepted and the value discarded, so
// DescribeVolumes reported nothing for the instance — and since AWS's
// EbsInstanceBlockDevice carries no size member, DescribeVolumes is the *only* API
// surface where a launch-specified volume size is observable at all.
func ec2ParseBlockDeviceMappings(params map[string]string, prefix string) []EC2BlockDeviceMapping {
	var out []EC2BlockDeviceMapping
	for n := 1; ; n++ {
		p := fmt.Sprintf("%sBlockDeviceMapping.%d.", prefix, n)
		bdm, present := ec2ParseBlockDeviceMapping(params, p)
		if !present {
			return out
		}
		out = append(out, bdm)
	}
}

// ec2ParseBlockDeviceMapping reads one mapping's members from the parameters under
// p, reporting whether the index was present at all.
//
// An index is "present" when *any* of its members is, not when a particular one is.
// A mapping naming only NoDevice is as real as one naming a device name and six Ebs
// members, and requiring a chosen member would silently drop a mapping for spelling
// reasons — [ec2ParseNetworkInterface]'s rule, applied here.
func ec2ParseBlockDeviceMapping(params map[string]string, p string) (EC2BlockDeviceMapping, bool) {
	// NoDevice is an empty-string member on the wire: AWS's model gives it a String
	// type and the SDK sends NoDevice="" to mean "suppress this device", so its
	// presence is the signal and its value is not. A hand-built request spelling it
	// "true" is honored too.
	noDeviceRaw, hasNoDevice := params[p+"NoDevice"]

	bdm := EC2BlockDeviceMapping{
		DeviceName:          params[p+"DeviceName"],
		VirtualName:         params[p+"VirtualName"],
		NoDevice:            hasNoDevice && !strings.EqualFold(noDeviceRaw, "false"),
		SnapshotID:          params[p+"Ebs.SnapshotId"],
		VolumeType:          params[p+"Ebs.VolumeType"],
		Encrypted:           strings.EqualFold(params[p+"Ebs.Encrypted"], "true"),
		DeleteOnTermination: params[p+"Ebs.DeleteOnTermination"],
	}
	if n, err := strconv.Atoi(strings.TrimSpace(params[p+"Ebs.VolumeSize"])); err == nil {
		bdm.VolumeSize = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(params[p+"Ebs.Iops"])); err == nil {
		bdm.IOPS = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(params[p+"Ebs.Throughput"])); err == nil {
		bdm.Throughput = n
	}

	present := hasNoDevice || bdm.DeviceName != "" || bdm.VirtualName != "" ||
		bdm.SnapshotID != "" || bdm.VolumeType != "" ||
		params[p+"Ebs.VolumeSize"] != "" || params[p+"Ebs.Iops"] != "" ||
		params[p+"Ebs.Throughput"] != "" || params[p+"Ebs.Encrypted"] != "" ||
		params[p+"Ebs.DeleteOnTermination"] != ""
	return bdm, present
}

// ec2IsRootDevice reports whether a device name names the root volume.
func ec2IsRootDevice(device string) bool {
	return device == ec2RootDeviceSDA1 || device == ec2RootDeviceXVDA
}

// ec2CreatesEBSVolume reports whether a mapping asks for an EBS volume.
//
// NoDevice suppresses the device outright, and a VirtualName with no Ebs.* members
// names an instance store volume, which is not an EBS volume and has no presence in
// DescribeVolumes. Either way the mapping is recorded intent that materializes
// nothing.
func ec2CreatesEBSVolume(bdm EC2BlockDeviceMapping) bool {
	if bdm.NoDevice {
		return false
	}
	if bdm.VirtualName != "" {
		return bdm.SnapshotID != "" || bdm.VolumeSize > 0 || bdm.VolumeType != "" ||
			bdm.IOPS > 0 || bdm.Throughput > 0 || bdm.Encrypted ||
			bdm.DeleteOnTermination != ""
	}
	return true
}

// ec2LaunchVolumesFor resolves the EBS volumes a launch materializes for one
// instance, each already attached to it.
//
// The volumes are real EC2Volume records written to the same "volume:" state
// namespace CreateVolume writes to, so DescribeVolumes is the single source of truth
// for provisioned and launch-created volumes alike — which is how real EC2 unifies
// the two, and the reason #666's consumer had nowhere to look.
//
// **Every launch produces at least one volume.** A real instance always has a root
// volume whether or not the request mentions one, so a launch that declares no
// mapping gets a synthesized 8 GiB gp2 volume at /dev/sda1, matching what
// DescribeImages already reports for every AMI. A mapping that *does* name a root
// device configures that root volume rather than adding a device, which is AWS's own
// rule for a mapping whose device name the AMI's mapping already uses: "the mapping
// you specify replaces the AMI's".
//
// DeleteOnTermination defaults to **true for the root volume and false for a data
// volume**, and that split is substrate's resolution of a conflict between two
// current AWS pages rather than a value either one simply states (#675).
//
// The API reference documents no default for EbsBlockDevice.DeleteOnTermination at
// all. Of the two guide pages that do, block-device-mapping-concepts gives exactly
// this split — "true for the root volume and false for attached volumes" — while
// preserving-volumes-on-termination carries a console-vs-CLI table listing a
// launch-created data volume as Delete when the launch came through the CLI. #666
// followed the second and applied true to everything.
//
// The split wins for two reasons. Both pages agree that the real launch default
// comes from the *AMI's* own block device mapping, and substrate's AMIs carry none
// to inherit — so this is substrate's choice either way, and the non-destructive
// side of a genuine ambiguity is the one a test emulator should take: a volume
// wrongly preserved is visible and correctable, one wrongly deleted is gone. And a
// consumer reading block-device-mapping-concepts, the page that describes the
// mapping shape they are writing, would find substrate wrong rather than
// opinionated.
//
// A volume attached after launch keeps the other default; see attachVolume, where
// the two pages agree and nothing here changes it.
func ec2LaunchVolumesFor(inst *EC2Instance, mappings []EC2BlockDeviceMapping, now string) []EC2Volume {
	volumes := make([]EC2Volume, 0, len(mappings)+1)
	rootDeclared := false

	for _, bdm := range mappings {
		if !ec2CreatesEBSVolume(bdm) {
			continue
		}
		if ec2IsRootDevice(bdm.DeviceName) {
			rootDeclared = true
		}
		volumes = append(volumes, ec2VolumeFromMapping(inst, bdm, now))
	}

	if !rootDeclared {
		volumes = append(volumes, ec2VolumeFromMapping(inst, EC2BlockDeviceMapping{
			DeviceName: ec2RootDeviceSDA1,
		}, now))
	}
	return volumes
}

// ec2VolumeFromMapping builds one volume from one mapping, attached to inst.
//
// The zone comes from the instance rather than from the region's first zone: a
// volume must be in the same Availability Zone as the instance it is attached to, so
// deriving it any other way would let a launch into a non-default zone produce an
// attachment real EC2 cannot have.
func ec2VolumeFromMapping(inst *EC2Instance, bdm EC2BlockDeviceMapping, now string) EC2Volume {
	size := bdm.VolumeSize
	if size <= 0 {
		size = ec2DefaultVolumeSizeGiB
	}
	volType := bdm.VolumeType
	if volType == "" {
		volType = ec2DefaultVolumeType
	}
	device := bdm.DeviceName
	if device == "" {
		device = ec2RootDeviceSDA1
	}
	// The default splits on whether this is the root device; see
	// [ec2LaunchVolumesFor] for why. It keys off the *resolved* device name, so a
	// mapping naming no device at all — which lands on /dev/sda1 above — is treated
	// as the root it became rather than as a data volume.
	//
	// The raw string is what makes an explicit value distinguishable from an absent
	// one, and an explicit value wins over either default.
	deleteOnTermination := ec2IsRootDevice(device)
	if bdm.DeleteOnTermination != "" {
		deleteOnTermination = strings.EqualFold(bdm.DeleteOnTermination, "true")
	}

	return EC2Volume{
		VolumeID:         generateVolumeID(),
		Size:             size,
		VolumeType:       volType,
		AvailabilityZone: inst.AvailabilityZone,
		State:            "in-use",
		SnapshotID:       bdm.SnapshotID,
		Encrypted:        bdm.Encrypted,
		IOPS:             bdm.IOPS,
		Throughput:       bdm.Throughput,
		Attachments: []EC2VolumeAttachment{{
			InstanceID:          inst.InstanceID,
			Device:              device,
			State:               "attached",
			AttachTime:          now,
			DeleteOnTermination: deleteOnTermination,
		}},
		CreateTime: now,
		AccountID:  inst.AccountID,
		Region:     inst.Region,
	}
}

// ec2CreateLaunchVolumes writes the volumes a launch materializes for one instance,
// indexing each one the way CreateVolume does so DescribeVolumes and DeleteVolume
// find them by the same paths.
func (p *EC2Plugin) ec2CreateLaunchVolumes(
	inst *EC2Instance,
	mappings []EC2BlockDeviceMapping,
	now string,
) error {
	for _, vol := range ec2LaunchVolumesFor(inst, mappings, now) {
		data, err := json.Marshal(vol)
		if err != nil {
			return fmt.Errorf("ec2 runInstances volume marshal: %w", err)
		}
		key := ec2VolumeStateKey(vol.AccountID, vol.Region, vol.VolumeID)
		if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
			return fmt.Errorf("ec2 runInstances volume state.Put: %w", err)
		}
		if err := p.appendToList(vol.AccountID+"/"+vol.Region, "volume_ids", vol.VolumeID); err != nil {
			return fmt.Errorf("ec2 runInstances volume index: %w", err)
		}
	}
	return nil
}

// ec2DeleteInstanceVolumes settles the volumes attached to a terminated instance.
//
// A volume whose attachment deletes on termination is removed outright; one that
// does not becomes available with no attachment, which is what a preserved volume
// looks like once its instance is gone. Doing neither would leave a volume reporting
// in-use, attached to a terminated instance, forever — a new inaccuracy in place of
// the old silence, and the reason the parsing in #666 could not land on its own.
func (p *EC2Plugin) ec2DeleteInstanceVolumes(accountID, region, instanceID string) error {
	ctx := context.Background()
	keys, err := p.state.List(ctx, ec2Namespace, ec2VolumeStatePrefix(accountID, region))
	if err != nil {
		return fmt.Errorf("ec2 terminateInstances volume list: %w", err)
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
		attached, deleteOnTermination := ec2VolumeAttachedTo(vol, instanceID)
		if !attached {
			continue
		}
		if deleteOnTermination {
			if err := p.state.Delete(ctx, ec2Namespace, key); err != nil {
				return fmt.Errorf("ec2 terminateInstances volume delete: %w", err)
			}
			removeFromStringIndex(ctx, p.state, ec2Namespace,
				"volume_ids:"+accountID+"/"+region, vol.VolumeID)
			continue
		}
		vol.State = "available"
		vol.Attachments = nil
		newData, marshalErr := json.Marshal(vol)
		if marshalErr != nil {
			return fmt.Errorf("ec2 terminateInstances volume marshal: %w", marshalErr)
		}
		if err := p.state.Put(ctx, ec2Namespace, key, newData); err != nil {
			return fmt.Errorf("ec2 terminateInstances volume state.Put: %w", err)
		}
	}
	return nil
}

// ec2VolumeMatchesAttachmentFilters reports whether a volume satisfies every
// attachment-scoped DescribeVolumes filter that was supplied.
//
// Each filter is satisfied by *any* attachment, which is what a filter on a
// list-valued member means, but the filters are ANDed with each other rather than
// evaluated against one attachment at a time — AWS's filter semantics are per
// filter, not per list element. An empty map means the filter was not supplied and
// so constrains nothing.
func ec2VolumeMatchesAttachmentFilters(
	vol EC2Volume,
	instanceIDs, devices, deleteOnTermination map[string]bool,
) bool {
	matches := func(want map[string]bool, got func(EC2VolumeAttachment) string) bool {
		if len(want) == 0 {
			return true
		}
		for _, att := range vol.Attachments {
			if want[got(att)] {
				return true
			}
		}
		return false
	}
	return matches(instanceIDs, func(a EC2VolumeAttachment) string { return a.InstanceID }) &&
		matches(devices, func(a EC2VolumeAttachment) string { return a.Device }) &&
		matches(deleteOnTermination, func(a EC2VolumeAttachment) string {
			return strconv.FormatBool(a.DeleteOnTermination)
		})
}

// ec2VolumeStateKey is the state key one volume is stored under.
//
// Declared once rather than concatenated at each call site: a launch now writes
// volumes that CreateVolume did not create, and a writer that spells the key
// differently from the readers would produce volumes DescribeVolumes cannot see.
func ec2VolumeStateKey(accountID, region, volumeID string) string {
	return "volume:" + accountID + "/" + region + "/" + volumeID
}

// ec2VolumeStatePrefix is the key prefix every volume in one account and region
// shares, for a List that has no particular volume in mind.
func ec2VolumeStatePrefix(accountID, region string) string {
	return "volume:" + accountID + "/" + region + "/"
}

// ec2VolumeAttachedTo reports whether a volume is attached to the named instance,
// and whether that attachment deletes the volume when the instance terminates.
func ec2VolumeAttachedTo(vol EC2Volume, instanceID string) (attached, deleteOnTermination bool) {
	for _, att := range vol.Attachments {
		if att.InstanceID == instanceID {
			return true, att.DeleteOnTermination
		}
	}
	return false, false
}
