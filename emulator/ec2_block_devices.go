package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Root device names. Substrate recognizes a mapping as configuring the root volume
// by device name rather than by comparing it against the AMI's own root device.
// [EC2Image.RootDeviceName] records one as of #750 — RegisterImage stores what the
// caller sent and a bundled descriptor names its publisher's convention — but the
// launch path still accepts either spelling, because an AMI minted by CreateImage or
// registered before #750 has none to compare against and refusing those launches
// would be a regression a caller cannot act on.
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
// It matches the mapping DescribeImages fabricates for every AMI, so an AMI's reported
// root size and the volume a launch from it produces agree.
//
// It no longer backs CreateVolume, which refuses a request naming neither Size nor
// SnapshotId as of #712: AWS documents a default only on the launch path, where a mapping
// that omits a size is legal, and never on the operation whose whole purpose is to be told
// one. A launch-created volume and a CreateVolume-created one therefore no longer agree
// about what "unspecified" means — because on CreateVolume it is not a request AWS accepts.
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

		// The raw strings are kept beside the parsed ints so a value that does not
		// parse stays distinguishable from an absent one; see the field docs on
		// [EC2BlockDeviceMapping] and [ec2CheckBlockDeviceMappings] (#671).
		VolumeSizeRaw: params[p+"Ebs.VolumeSize"],
		IOPSRaw:       params[p+"Ebs.Iops"],
		ThroughputRaw: params[p+"Ebs.Throughput"],
	}
	if n, err := strconv.Atoi(strings.TrimSpace(bdm.VolumeSizeRaw)); err == nil {
		bdm.VolumeSize = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(bdm.IOPSRaw)); err == nil {
		bdm.IOPS = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(bdm.ThroughputRaw)); err == nil {
		bdm.Throughput = n
	}

	present := hasNoDevice || bdm.DeviceName != "" || bdm.VirtualName != "" ||
		bdm.SnapshotID != "" || bdm.VolumeType != "" ||
		bdm.VolumeSizeRaw != "" || bdm.IOPSRaw != "" ||
		bdm.ThroughputRaw != "" || params[p+"Ebs.Encrypted"] != "" ||
		bdm.DeleteOnTermination != ""
	return bdm, present
}

// ec2NamesEbsMember reports whether a mapping carries any member of the Ebs
// structure.
//
// It is what scopes the "you must specify either a snapshot ID or a volume size"
// refusal. AWS's sentence lives on EbsBlockDevice.VolumeSize — a member *of* the Ebs
// structure — so a mapping that carries no Ebs structure at all names no EBS block
// device for the requirement to have a subject, and substrate keeps applying its 8 GiB
// default to it. A mapping that names Ebs.VolumeType and nothing else is
// unambiguously configuring a volume and has given neither of the two things one of
// which is required.
func ec2NamesEbsMember(bdm EC2BlockDeviceMapping) bool {
	return bdm.SnapshotID != "" || bdm.VolumeType != "" || bdm.Encrypted ||
		bdm.VolumeSizeRaw != "" || bdm.IOPSRaw != "" || bdm.ThroughputRaw != "" ||
		bdm.DeleteOnTermination != ""
}

// ec2VolumeTypesWithoutIOPS names the volume types for which Ebs.Iops is meaningless.
//
// It is deliberately a refusal list rather than the allow list AWS's sentence implies.
// The "supported for io1, io2, and gp3 volumes only" wording lives on
// LaunchTemplateEbsBlockDeviceRequest.Iops, not on the EbsBlockDevice shape
// RunInstances accepts, and the very same member's own paragraph — on both shapes —
// explains what Iops means "for gp2 volumes". AWS contradicts itself within one
// member, so substrate refuses only the three types that appear in neither the
// supported-values list nor that paragraph, and takes the permissive reading of gp2.
// See [ec2CheckBlockDeviceMappings] for why an *absent* type refuses nothing.
var ec2VolumeTypesWithoutIOPS = map[string]bool{"standard": true, "st1": true, "sc1": true}

// ec2CheckBlockDeviceMappings returns the error EC2 raises for a block device mapping
// it will not accept, or nil.
//
// Through v0.104.0 substrate accepted every mapping it could parse and materialized a
// volume from it, so a launch AWS refuses succeeded here — the same class of defect
// #412 closed for an empty ImageId, and worse after #666, because the mapping now
// produces state a consumer asserts against.
//
// # Scope
//
// Only what the API model states. Per-type size and IOPS ranges are deliberately not
// encoded even though the reference lists them: they change, and a stale range is a
// false deny — which is worse than the silence it replaces, because a caller cannot
// work around a refusal of a request AWS accepts. For the same reason both type-scoped
// rules key off the **explicitly named** VolumeType rather than the resolved one:
// substrate resolves an absent type to gp2, but on real EC2 it comes from the AMI's own
// mapping, commonly gp3.
//
// # The snapshot rules
//
// Three of them. Two were added by #689 once a snapshot had a real size to compare
// against: a SnapshotId that is malformed or names nothing is refused, and a VolumeSize
// smaller than the named snapshot's is refused. v0.105.0 deferred the second for the
// reason #689 removes — EC2Snapshot.VolumeSize was the literal 8 at its single producer,
// so the comparison would have tested a constant rather than the caller's request. The
// third, added by #732, refuses a snapshot that is not `completed`, which is the same
// refusal CreateVolume already makes from the same rule.
//
// resolveSnapshot is how the first two reach state: a lookup rather than a
// [StateManager], so this stays a pure function over its arguments and a unit test can
// supply three snapshots without a state fixture. It is required, not optional — a
// nil-means-skip escape would silently disable a refusal, which is the failure mode #689
// exists to close. [EC2Plugin.ec2SnapshotResolver] builds the production one.
//
// observeSnapshot is how the third reaches it, and it is separate because the state a
// snapshot *reports* is not the state its record carries: EC2Snapshot.State is
// "completed" for every snapshot substrate writes, so a rule reading the record would be
// dead code, and the observable state lives behind a seed. It must be
// [EC2Plugin.peekSnapshotStatus] and never [EC2Plugin.observeSnapshotStatus] — the
// countdown advances in one place, so an observing read here would burn one observation
// *per mapping per request* and make "pendingObservations: 2" mean a different number of
// polls depending on how many volumes a launch declared.
//
// Unlike resolveSnapshot, a nil observeSnapshot skips the state rule, and the two
// CreateLaunchTemplate paths pass nil deliberately: a template is not a launch, they
// report mapping problems through `warning` rather than refusing at all, and a snapshot
// that is pending when a template is written may legitimately be complete when the
// template is used. The rule still lives in one place — here — so the launch path and
// RegisterImage cannot drift apart on it.
//
// # Provenance
//
// Four rules are documented verbatim — the size-or-snapshot requirement on
// EbsBlockDevice.VolumeSize, "You can specify a volume size that is equal to or larger
// than the snapshot size" on the same member, InvalidSnapshot.NotFound in the
// client-error table, and "This parameter is valid only for gp3 volumes" on
// Throughput. The snapshot-state rule is AWS's too, from the snapshot-states table
// rather than from either operation's page; only its code is substrate's, as
// [ec2CheckMappingSnapshot] records. The Iops rule rests on the sibling launch-template shape; see
// [ec2VolumeTypesWithoutIOPS]. Refusing a duplicate device name and a virtualName
// beside an Ebs member rests on **substrate's own reading** — AWS documents no rule for
// either, and neither is a thing real EC2 can act on, since one device cannot carry two
// volumes and an instance store device is not an EBS volume. Refusing an unparseable
// number is substrate's own too, and it is the reason the raw strings are recorded at
// all.
//
// Every message is substrate's own; AWS publishes none. The code is documented — the
// client-error table lists InvalidBlockDeviceMapping as "A block device mapping
// parameter is not valid. The returned message indicates the incorrect value." The
// status is a class-level inference: the table says only that client errors are
// "accompanied by a 400-series HTTP response code", and substrate has no code-to-status
// table to consult.
//
// # Placement
//
// Call it after a launch template has been merged in, so a mapping that reaches a
// launch through a template is refused at RunInstances time, and before anything is
// written, so a refusal leaves no state behind. CreateLaunchTemplate deliberately does
// not refuse: its response carries a documented `warning` member of type
// ValidationWarning that exists for "parameters or parameter combinations that are not
// valid", and its Errors section lists none — so a 400 there would be substrate's
// invention. It reports the same problems through that member instead, which is what
// [ec2CollectBlockDeviceMappings] is for.
func ec2CheckBlockDeviceMappings(
	mappings []EC2BlockDeviceMapping,
	resolveSnapshot func(string) (EC2Snapshot, bool),
	observeSnapshot func(EC2Snapshot) (ec2SnapshotObservation, error),
) *AWSError {
	if problems := ec2CollectBlockDeviceMappings(mappings, resolveSnapshot, observeSnapshot); len(problems) > 0 {
		return problems[0]
	}
	return nil
}

// ec2CollectBlockDeviceMappings returns every problem the mapping rules find, in the
// order [ec2CheckBlockDeviceMappings] would have hit them, so its first element is
// exactly what that function refuses with.
//
// The list exists because CreateLaunchTemplate's `warning` member holds one — AWS's
// ValidationWarning is "an error code and an error message […] for each issue that's
// found", plural — while RunInstances refuses with a single error. Collecting is the
// primitive and refusing is the thin wrapper, rather than the other way round: a
// first-error function cannot be widened into a list without losing every problem after
// the first, which is the whole of what a caller wants from a warning (#693).
//
// The elements are [AWSError] values rather than a warning-shaped type so that both
// doors report byte-identical codes and messages by construction — a mapping refused at
// RunInstances and the same mapping warned about at CreateLaunchTemplate cannot drift.
// [ec2ValidationWarningFor] converts them at the wire boundary.
func ec2CollectBlockDeviceMappings(
	mappings []EC2BlockDeviceMapping,
	resolveSnapshot func(string) (EC2Snapshot, bool),
	observeSnapshot func(EC2Snapshot) (ec2SnapshotObservation, error),
) []*AWSError {
	var problems []*AWSError
	seen := make(map[string]bool, len(mappings))
	for _, bdm := range mappings {
		problems = append(problems, ec2CollectBlockDeviceMapping(bdm, resolveSnapshot, observeSnapshot)...)
		// Only a mapping that materializes a volume can collide: NoDevice suppresses
		// its device and a virtualName-only mapping names an instance store device, so
		// neither occupies the device in a way a second mapping could contend for.
		if !ec2CreatesEBSVolume(bdm) || bdm.DeviceName == "" {
			continue
		}
		if seen[bdm.DeviceName] {
			problems = append(problems, ec2InvalidBlockDeviceMapping("",
				"device "+bdm.DeviceName+" is named by more than one mapping"))
			continue
		}
		seen[bdm.DeviceName] = true
	}
	return problems
}

// ec2CollectBlockDeviceMapping applies the rules that concern one mapping in isolation
// and returns every problem it finds.
//
// The unparseable-number checks run first: a garbage size makes every judgement below
// meaningless, and reporting the malformed value is more use to a caller than reporting
// a consequence of it. That is also why an unparseable number is the one problem that
// stops the walk — every rule after it reads a parsed int, so continuing would report
// consequences of the value rather than facts about the request. All three numeric
// members are reported, though, not just the first: they are independent mistakes.
//
// The virtualName conflict runs before the size requirement, since a mapping combining
// the two is wrong in a way that naming a size would not fix. The snapshot checks follow
// the size requirement, because a mapping that names neither a size nor a snapshot has
// no snapshot to look up; they precede the type rules so that a nonexistent snapshot is
// still reported for a mapping that names no volume type, which is where the type rules
// stop.
func ec2CollectBlockDeviceMapping(
	bdm EC2BlockDeviceMapping,
	resolveSnapshot func(string) (EC2Snapshot, bool),
	observeSnapshot func(EC2Snapshot) (ec2SnapshotObservation, error),
) []*AWSError {
	var problems []*AWSError
	device := bdm.DeviceName
	for _, num := range []struct{ member, raw string }{
		{"Ebs.VolumeSize", bdm.VolumeSizeRaw},
		{"Ebs.Iops", bdm.IOPSRaw},
		{"Ebs.Throughput", bdm.ThroughputRaw},
	} {
		if num.raw == "" {
			continue
		}
		if _, err := strconv.Atoi(strings.TrimSpace(num.raw)); err != nil {
			problems = append(problems, ec2InvalidBlockDeviceMapping(device,
				fmt.Sprintf("%s value %q is not an integer", num.member, num.raw)))
		}
	}
	if len(problems) > 0 {
		return problems
	}

	if bdm.NoDevice {
		// A suppressed device carries nothing to validate. AWS documents NoDevice
		// beside the other members rather than as exclusive of them, so a mapping
		// naming both is not refused — it suppresses the device, which is what
		// ec2CreatesEBSVolume already decided.
		return nil
	}

	if bdm.VirtualName != "" && ec2NamesEbsMember(bdm) {
		problems = append(problems, ec2InvalidBlockDeviceMapping(device,
			"virtualName names an instance store device and cannot be combined with an Ebs member"))
	}

	if ec2CreatesEBSVolume(bdm) && ec2NamesEbsMember(bdm) &&
		bdm.VolumeSizeRaw == "" && bdm.SnapshotID == "" {
		problems = append(problems, ec2InvalidBlockDeviceMapping(device,
			"you must specify either a snapshot ID or a volume size"))
	}

	if bdm.SnapshotID != "" {
		if awsErr := ec2CheckMappingSnapshot(bdm, resolveSnapshot, observeSnapshot); awsErr != nil {
			problems = append(problems, awsErr)
		}
	}

	// Both type rules read the named type, never the resolved one; see
	// [ec2CheckBlockDeviceMappings]. An absent type therefore refuses nothing.
	volType := strings.ToLower(bdm.VolumeType)
	if volType == "" {
		return problems
	}
	if bdm.ThroughputRaw != "" && volType != "gp3" {
		problems = append(problems, ec2InvalidBlockDeviceMapping(device,
			"Ebs.Throughput is valid only for gp3 volumes, not "+bdm.VolumeType))
	}
	if bdm.IOPSRaw != "" && ec2VolumeTypesWithoutIOPS[volType] {
		problems = append(problems, ec2InvalidBlockDeviceMapping(device,
			"Ebs.Iops is not supported for "+bdm.VolumeType+" volumes"))
	}
	return problems
}

// ec2CheckMappingSnapshot applies the three rules that concern the snapshot a mapping
// names. The caller has already established that it names one.
//
// The refusals carry different codes on purpose, because they are different mistakes: a
// snapshot substrate cannot find is an InvalidSnapshot.NotFound (or
// InvalidSnapshotID.Malformed) about the *ID*, which is what a caller naming a snapshot
// from another account or a stale run has done, while a size below the snapshot's is an
// InvalidBlockDeviceMapping about the *mapping*, which is what the rest of this
// validator reports. Collapsing them into one code would tell a caller to look in the
// wrong place. The state rule is a third thing again — nothing about the request is wrong,
// the snapshot is simply not usable yet — so it answers IncorrectState, the code
// CreateVolume already gives for the same condition.
//
// # Order
//
// Existence, then state, then size, each refusal being more fundamental than the next: a
// caller told "size 8 is smaller than the snapshot" would fix the size and be refused
// again for the state. This is CreateVolume's order too, deliberately.
//
// The size comparison reads the parsed VolumeSize and gates on it being positive, so a
// mapping that names no size inherits the snapshot's rather than being refused for
// naming 0 — see [ec2ResolveSnapshotSizes], which does the inheriting.
//
// # Provenance of the state rule
//
// AWS's snapshot-states table is the rule: "A snapshot can't be used while it is in the
// pending state", "A snapshot can't be used if it is in the error state", a recoverable
// one "must first [be recovered] from the Recycle Bin", and a recovering one becomes
// "ready for use" only once it reaches completed. So every state but completed is refused,
// and the four reasons are AWS's own. Neither operation's page states it —
// API_RegisterImage.html's Errors section is empty and API_RunInstances.html publishes no
// snapshot error — so IncorrectState is substrate's choice from EC2's client-error table,
// the same provenance shape CreateVolume's copy of this rule carries (#715, #732).
//
// An unreadable seed answers InternalError/500 rather than being read as completed. This
// is the opposite of [EC2Plugin.ec2SnapshotResolver]'s rule for an unreadable *record*,
// and for the reason that one gives: reporting "absent" for an unreadable record is the
// same answer a caller gets for one never written, whereas reading an unreadable seed as
// "completed" would silently disable a refusal.
func ec2CheckMappingSnapshot(
	bdm EC2BlockDeviceMapping,
	resolveSnapshot func(string) (EC2Snapshot, bool),
	observeSnapshot func(EC2Snapshot) (ec2SnapshotObservation, error),
) *AWSError {
	if !ec2SnapshotIDKind.wellFormed(bdm.SnapshotID) {
		return ec2SnapshotIDKind.malformedError(bdm.SnapshotID)
	}
	snap, found := resolveSnapshot(bdm.SnapshotID)
	if !found {
		return ec2SnapshotIDKind.notFoundError(bdm.SnapshotID)
	}
	if observeSnapshot != nil {
		observed, err := observeSnapshot(snap)
		if err != nil {
			return &AWSError{
				Code:       "InternalError",
				Message:    "Failed to read the state of snapshot " + snap.SnapshotID,
				HTTPStatus: http.StatusInternalServerError,
			}
		}
		if observed.State != "completed" {
			return &AWSError{
				Code: "IncorrectState",
				Message: fmt.Sprintf(
					"Snapshot %s is in %s state; a block device mapping can only name a completed snapshot",
					snap.SnapshotID, observed.State),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if bdm.VolumeSize > 0 && int64(bdm.VolumeSize) < snap.VolumeSize {
		return ec2InvalidBlockDeviceMapping(bdm.DeviceName, fmt.Sprintf(
			"Ebs.VolumeSize %d is smaller than the size of snapshot %s (%d GiB)",
			bdm.VolumeSize, snap.SnapshotID, snap.VolumeSize))
	}
	return nil
}

// ec2ResolveSnapshotSizes returns mappings with every absent VolumeSize filled in from
// the snapshot that mapping names, leaving everything else untouched.
//
// Without it a mapping naming a snapshot and no size fell through to
// [ec2DefaultVolumeSizeGiB], so a 30 GiB snapshot produced an 8 GiB volume —
// [EC2BlockDeviceMapping.VolumeSize]'s own field doc already stated the correct rule, so
// the doc was ahead of the code (#689). AWS states it too, on EbsBlockDevice.VolumeSize:
// "If you specify a snapshot, the default is the snapshot size."
//
// It is a separate pass over the mappings rather than a lookup inside
// [ec2VolumeFromMapping] so that function stays pure — it is what
// [ec2LaunchVolumesFor] is built out of, and threading state through it would put a
// state read inside the volume-building loop. The returned slice is fresh, so a caller's
// parsed mappings keep reporting what the request actually said, which is what the
// launch template merge and the recorded-intent members read.
//
// VolumeSizeRaw is the test for absence rather than a zero VolumeSize, for the reason it
// exists at all: "Ebs.VolumeSize=0" is a value a caller supplied, and an unparseable one
// has already been refused by [ec2CollectBlockDeviceMapping].
//
// A snapshot that does not resolve is left alone rather than defaulted, because
// [ec2CheckMappingSnapshot] has already refused it — this pass runs after validation and
// never decides anything a caller can observe as an error.
func ec2ResolveSnapshotSizes(
	mappings []EC2BlockDeviceMapping,
	resolveSnapshot func(string) (EC2Snapshot, bool),
) []EC2BlockDeviceMapping {
	if len(mappings) == 0 {
		return mappings
	}
	out := make([]EC2BlockDeviceMapping, len(mappings))
	copy(out, mappings)
	for i := range out {
		if out[i].SnapshotID == "" || out[i].VolumeSizeRaw != "" {
			continue
		}
		if snap, found := resolveSnapshot(out[i].SnapshotID); found && snap.VolumeSize > 0 {
			out[i].VolumeSize = int(snap.VolumeSize)
		}
	}
	return out
}

// ec2InvalidBlockDeviceMapping returns EC2's InvalidBlockDeviceMapping error, naming
// the offending device when the mapping has one.
//
// The device name is interpolated because a request can carry many mappings and the
// code alone does not say which one was refused — the same reason
// [ec2UnknownInstanceAttribute] interpolates its attribute. A mapping that names no
// device gets the unqualified form rather than a stray "for ": the cross-mapping
// duplicate check names its device in the detail instead.
func ec2InvalidBlockDeviceMapping(device, detail string) *AWSError {
	msg := "Invalid block device mapping: " + detail
	if device != "" {
		msg = "Invalid block device mapping for " + device + ": " + detail
	}
	return &AWSError{
		Code:       "InvalidBlockDeviceMapping",
		Message:    msg,
		HTTPStatus: http.StatusBadRequest,
	}
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
// tags are the launch's volume-scoped TagSpecification tags, applied to every volume
// the launch materializes — including the root volume synthesized when no mapping
// declares one. AWS's rule is per resource type, not per mapping: the request has no
// way to tag one mapping's volume differently from another's (#670).
func ec2LaunchVolumesFor(
	inst *EC2Instance,
	mappings []EC2BlockDeviceMapping,
	tags []EC2Tag,
	now string,
) []EC2Volume {
	volumes := make([]EC2Volume, 0, len(mappings)+1)
	rootDeclared := false

	for _, bdm := range mappings {
		if !ec2CreatesEBSVolume(bdm) {
			continue
		}
		if ec2IsRootDevice(bdm.DeviceName) {
			rootDeclared = true
		}
		volumes = append(volumes, ec2VolumeFromMapping(inst, bdm, tags, now))
	}

	if !rootDeclared {
		volumes = append(volumes, ec2VolumeFromMapping(inst, EC2BlockDeviceMapping{
			DeviceName: ec2RootDeviceSDA1,
		}, tags, now))
	}
	return volumes
}

// ec2VolumeFromMapping builds one volume from one mapping, attached to inst.
//
// The zone comes from the instance rather than from the region's first zone: a
// volume must be in the same Availability Zone as the instance it is attached to, so
// deriving it any other way would let a launch into a non-default zone produce an
// attachment real EC2 cannot have.
func ec2VolumeFromMapping(
	inst *EC2Instance,
	bdm EC2BlockDeviceMapping,
	tags []EC2Tag,
	now string,
) EC2Volume {
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

	// Each volume gets its own copy, so a later CreateTags on one of them cannot be
	// seen through another's slice while both are still in memory.
	var volTags []EC2Tag
	if len(tags) > 0 {
		volTags = append(volTags, tags...)
	}

	return EC2Volume{
		VolumeID:         generateVolumeID(),
		Size:             size,
		Tags:             volTags,
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
	tags []EC2Tag,
	now string,
) error {
	for _, vol := range ec2LaunchVolumesFor(inst, mappings, tags, now) {
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

// ec2VolumeMatchesFilters reports whether a volume satisfies every DescribeVolumes
// filter that was supplied. Filters are ANDed with each other and each one's values
// are ORed, which is AWS's documented rule for all of them.
//
// AWS documents exactly two tag filters for this operation, `tag:<key>` and
// `tag-key`, out of 20 filters in total — there is no `tag-value` (#670).
//
// A `tag:<key>` filter with no values matches nothing. AWS documents no rule for
// that shape — Using_Filtering says only that a filter value cannot be null, and
// offers tag-key for the any-value question — so this is substrate's reading, and
// since #686 it is the reading everywhere: describeImages used to treat the shape as
// tag-key and no longer does.
func ec2VolumeMatchesFilters(vol EC2Volume, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2VolumeMatchesFilter(vol, name, values) {
			return false
		}
	}
	return true
}

// ec2VolumeMatchesFilter evaluates a single DescribeVolumes filter against a volume.
//
// A name this function does not handle is dropped — the volume matches — and since
// #687 that can only be a name DescribeVolumes documents and substrate keeps no state
// to answer, because [ec2VolumeFilterSpec] refuses anything outside AWS's set before
// the scan begins. So the drop is now the *inert* case rather than the unknown one,
// and it is deliberate: refusing `encrypted` would deny a filter real EC2 accepts.
// docs/services.md lists the inert names per operation.
func ec2VolumeMatchesFilter(vol EC2Volume, name string, values []string) bool {
	if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range vol.Tags {
			if t.Key == tagKey && ec2FilterAccepts(values, t.Value) {
				return true
			}
		}
		return false
	}

	// An attachment-scoped filter is satisfied by *any* attachment, which is what a
	// filter on a list-valued member means. The filters are still ANDed with each
	// other rather than evaluated against one attachment at a time — AWS's semantics
	// are per filter, not per list element.
	anyAttachment := func(got func(EC2VolumeAttachment) string) bool {
		for _, att := range vol.Attachments {
			if ec2FilterAccepts(values, got(att)) {
				return true
			}
		}
		return false
	}

	switch name {
	case "volume-id":
		return ec2FilterAccepts(values, vol.VolumeID)
	case "status":
		return ec2FilterAccepts(values, vol.State)
	case "volume-type":
		return ec2FilterAccepts(values, vol.VolumeType)
	case "size":
		return ec2FilterAccepts(values, strconv.Itoa(vol.Size))
	case "availability-zone":
		return ec2FilterAccepts(values, vol.AvailabilityZone)
	case "snapshot-id":
		return ec2FilterAccepts(values, vol.SnapshotID)
	case "tag-key":
		for _, t := range vol.Tags {
			if ec2FilterAccepts(values, t.Key) {
				return true
			}
		}
		return false
	case "attachment.instance-id":
		return anyAttachment(func(a EC2VolumeAttachment) string { return a.InstanceID })
	case "attachment.device":
		return anyAttachment(func(a EC2VolumeAttachment) string { return a.Device })
	case "attachment.delete-on-termination":
		// This was the tree's only case-*insensitive* filter-value comparison, inherited
		// from a hand-rolled loop that lowercased the value so "True" matched a true
		// attachment. AWS documents the opposite twice — Using_Filtering's "Filter values
		// are case sensitive" and the Filter type's own "Filter values are case-sensitive" —
		// and every sibling boolean filter (`encrypted`, `default-for-az`,
		// `map-public-ip-on-launch`) already compared exactly, so #697 makes this one agree
		// rather than keeping a leniency real EC2 does not offer. A caller sending "True"
		// now selects nothing, which is what AWS answers.
		for _, att := range vol.Attachments {
			if ec2FilterAccepts(values, strconv.FormatBool(att.DeleteOnTermination)) {
				return true
			}
		}
		return false
	default:
		return true
	}
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
