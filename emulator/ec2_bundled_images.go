package emulator

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// bundledImage is an AMI substrate resolves without anyone having registered one, so a
// launch that discovers its AMI the way AWS documents — through an SSM public parameter —
// finds an image at the other end (#733).
//
// Every entry is an AWS-published public parameter, and that is the whole membership rule.
// Substrate deliberately bundles no AMI a caller could not also resolve on real AWS: the
// point of refusing an unknown AMI is that a launch which works here works there, and an
// ID that resolves only in substrate would reintroduce exactly that divergence. In
// particular the example IDs AWS's own reference pages use — ami-0abcdef1234567890 and
// ami-1234567890abcdef0 — are *not* bundled, because IaC that hardcodes one fails on real
// AWS and must fail here too.
type bundledImage struct {
	// Parameter is the SSM public parameter name a caller discovers the image through,
	// and the sole input (with the region) to its derived ID.
	Parameter string

	// Name is the AMI's name member. It is the parameter's leaf rather than a
	// version-stamped image name ("al2023-ami-2023.6.20260101.0-kernel-6.1-x86_64"),
	// because a stamped literal here would be a date that goes stale silently and that
	// no caller can act on.
	Name string

	// Description is the human-readable description AWS renders for the image family.
	Description string

	// Architecture is the AMI's architecture, read from the parameter path's own
	// x86_64/arm64/amd64 suffix rather than guessed: the path is the publisher's
	// statement of what the image is.
	Architecture string

	// Platform is "windows" for the Windows entry and empty for every other one, which
	// is the distinction AWS's platform member draws. PlatformDetails and
	// UsageOperation are the billing pair from AWS's "usage operation by platform"
	// table: Windows is "Windows" / "RunInstances:0002" and everything else here is
	// "Linux/UNIX" / "RunInstances".
	//
	// The Ubuntu entries are plain Ubuntu Server rather than Ubuntu Pro, so they bill as
	// Linux/UNIX; the Pro row's "RunInstances:0g00" would name a subscription these
	// parameters do not resolve to.
	Platform        string
	PlatformDetails string
	UsageOperation  string

	// RootDeviceName is the device name of the image's root volume.
	//
	// AWS's device naming reference gives an HVM AMI's root device as "Differs by AMI —
	// /dev/sda1 or /dev/xvda" and publishes no per-AMI table, so the assignment here is
	// substrate's reading of each publisher's convention: Amazon Linux uses /dev/xvda,
	// Windows Server and Canonical's Ubuntu images use /dev/sda1. Both spellings are
	// AWS's own (see ec2_block_devices.go's constants) and a launch naming either still
	// configures the root volume, so a caller that branches on this member gets a real
	// answer while a caller that ignores it is unaffected.
	RootDeviceName string

	// OwnerAlias is the AWS-maintained owner alias the image is published under.
	//
	// "amazon" for the AWS-published parameters, and deliberately empty for the two
	// Canonical Ubuntu entries: the aliases are "defined in an Amazon-maintained list"
	// of which Canonical is not a member, so aliasing them would be an observation
	// nothing backs (#750).
	OwnerAlias string
}

// Values every bundled image shares, named rather than repeated ten times.
//
// Each is invariant across the catalog for a reason a reader can check rather than by
// coincidence: every bundled parameter names an EBS-backed HVM machine image (AWS
// publishes no paravirtual public parameter, and instance-store AMIs are registered from
// an S3 manifest, which no parameter resolves to), and AWS documents xen as the only
// supported hypervisor value. A future entry that breaks one of these belongs on
// [bundledImage] as a field, not here.
const (
	bundledImageRootDeviceType     = "ebs"
	bundledImageVirtualizationType = "hvm"
	bundledImageHypervisor         = "xen"
	bundledImageType               = "machine"
)

// bundledImages is the catalog, with every parameter name taken verbatim from AWS's own
// documentation:
//
//   - /aws/service/ami-amazon-linux-latest/* from
//     https://docs.aws.amazon.com/linux/al2023/ug/ec2.html
//   - /aws/service/ami-windows-latest/* from
//     https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/finding-an-ami-parameter-store.html
//   - /aws/service/ecs/optimized-ami/* from
//     https://docs.aws.amazon.com/AmazonECS/latest/developerguide/retrieve-ecs-optimized_AMI.html
//
// The Canonical Ubuntu paths are Canonical's rather than AWS's — they are published at
// https://cloud-images.ubuntu.com/locator/ec2/, which AWS's own "find an AMI" page links
// to as the Ubuntu reference — and substrate has answered them since #267.
//
// The list is not AWS's whole public-parameter set, and it does not have to be:
// [bundledImageForParameter] falls back to the family an unlisted AMI path names, so a
// parameter substrate has never heard of still resolves to a launchable image rather than
// to an ID nothing backs.
var bundledImages = []bundledImage{
	{
		Parameter:       "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
		Name:            "al2023-ami-kernel-default-x86_64",
		Description:     "Amazon Linux 2023 AMI, kernel default, x86_64",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
		Name:            "al2023-ami-kernel-default-arm64",
		Description:     "Amazon Linux 2023 AMI, kernel default, arm64",
		Architecture:    "arm64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-x86_64",
		Name:            "al2023-ami-minimal-kernel-default-x86_64",
		Description:     "Amazon Linux 2023 minimal AMI, kernel default, x86_64",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-arm64",
		Name:            "al2023-ami-minimal-kernel-default-arm64",
		Description:     "Amazon Linux 2023 minimal AMI, kernel default, arm64",
		Architecture:    "arm64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base",
		Name:            "Windows_Server-2022-English-Full-Base",
		Description:     "Microsoft Windows Server 2022 Full Locale English AMI",
		Architecture:    "x86_64",
		Platform:        "windows",
		PlatformDetails: "Windows",
		UsageOperation:  "RunInstances:0002",
		RootDeviceName:  ec2RootDeviceSDA1,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id",
		Name:            "amazon-linux-2023-ecs-optimized",
		Description:     "Amazon ECS-optimized Amazon Linux 2023 AMI",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
		Name:            "amazon-linux-2-ecs-optimized",
		Description:     "Amazon ECS-optimized Amazon Linux 2 AMI",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceXVDA,
		OwnerAlias:      "amazon",
	},
	{
		Parameter:       "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
		Name:            "ubuntu-noble-24.04-amd64-server",
		Description:     "Canonical Ubuntu Server 24.04 LTS, amd64",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceSDA1,
	},
	{
		Parameter:       "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
		Name:            "ubuntu-jammy-22.04-amd64-server",
		Description:     "Canonical Ubuntu Server 22.04 LTS, amd64",
		Architecture:    "x86_64",
		PlatformDetails: "Linux/UNIX",
		UsageOperation:  "RunInstances",
		RootDeviceName:  ec2RootDeviceSDA1,
	},
}

// bundledImageDefaultParameter is the parameter substrate resolves when something needs an
// AMI and nothing named one — the CloudFormation deployer's AWS::EC2::Instance default, and
// the family fallback for an unlisted Linux AMI path. Amazon Linux 2023 on x86_64 is the
// choice because it is the AMI AWS's own launch examples reach for.
const bundledImageDefaultParameter = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

// bundledImageFamilyDefaults maps a substring of an AMI parameter path to the catalog
// parameter substrate answers an *unlisted* path in that family with.
//
// The substrings are the same four the SSM resolver has recognized since #267, so the set
// of names that resolve does not change — only what they resolve to. Order matters, and is
// the order of this slice rather than a map's, because "/optimized-ami/" and "-latest" can
// both appear in one path.
var bundledImageFamilyDefaults = []struct {
	Match     string
	Parameter string
}{
	{"/ami-windows-latest/", "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base"},
	{"/ubuntu/", "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"},
	{"/optimized-ami/", "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"},
	{"", bundledImageDefaultParameter},
}

// BundledImageID returns the AMI ID substrate resolves for an SSM public parameter in one
// region, or "" if parameterName is not an AMI parameter path.
//
// It is the identifier a test fixture or a generated template should name when it needs an
// AMI that exists: substrate refuses a launch from an AMI it cannot resolve (#733), and
// this is the same value [SSMPlugin] answers GetParameter with, so a fixture that names it
// and a consumer that discovers it through SSM agree.
//
// The ID is region-dependent, exactly as on AWS — an AMI ID names an image in one region
// and nothing in any other — and it is derived rather than random, so it is stable across
// runs, processes and replays.
func BundledImageID(region, parameterName string) string {
	img, ok := bundledImageForParameter(parameterName)
	if !ok {
		return ""
	}
	return img.idIn(region)
}

// idIn returns the image's ID in one region.
//
// The derivation is sha256(region + ":" + parameter), truncated to 17 hex characters —
// AWS's current AMI-ID length. It is shared with [resolveManagedParameter] rather than
// spelled twice so the ID a GetParameter hands out and the ID a launch resolves cannot
// drift apart; that drift is the whole failure mode the shared helper exists to prevent.
func (b bundledImage) idIn(region string) string {
	sum := sha256.Sum256([]byte(region + ":" + b.Parameter))
	return "ami-" + hex.EncodeToString(sum[:])[:17]
}

// image renders the bundled descriptor as the record the rest of the plugin reads.
//
// AccountID is deliberately empty. A public AWS-owned AMI is not owned by the caller, and
// substrate holds no account for the "amazon" owner alias — so attributing it to the
// requesting account would be a claim nothing backs and would make an Owners=self
// DescribeImages match an image the caller does not own. DescribeImages renders
// imageOwnerId omitted rather than empty for exactly that reason (#750); through v0.108.0
// it rendered an empty element, because the member had no omitempty and this comment
// claimed nothing rendered it at all.
//
// The invariants come from the consts above and the varying members from the descriptor,
// so a caller reading a bundled AMI and a caller reading a registered one read the same
// shape — [EC2Plugin.describeImages] renders both through one function for the same reason.
func (b bundledImage) image(region string) EC2Image {
	return EC2Image{
		ImageID:                b.idIn(region),
		Name:                   b.Name,
		Description:            b.Description,
		State:                  "available",
		Region:                 region,
		Architecture:           b.Architecture,
		Platform:               b.Platform,
		PlatformDetails:        b.PlatformDetails,
		UsageOperation:         b.UsageOperation,
		RootDeviceName:         b.RootDeviceName,
		RootDeviceType:         bundledImageRootDeviceType,
		VirtualizationType:     bundledImageVirtualizationType,
		Hypervisor:             bundledImageHypervisor,
		ImageType:              bundledImageType,
		OwnerAlias:             b.OwnerAlias,
		Public:                 true,
		PublicSsmParameterName: b.Parameter,
	}
}

// bundledImageForParameter returns the bundled image an SSM parameter name resolves to.
//
// An exact catalog match wins. Any other /aws/service/ path whose shape says "AMI" —
// the four substrings [bundledImageFamilyDefaults] lists, unchanged from #267's resolver —
// falls back to its family's entry rather than to a hash of its own name. That fallback is
// substrate's reading, and it is load-bearing: hashing the unlisted name would mint an ID
// no catalog entry backs, so GetParameter would hand out an AMI that RunInstances then
// refuses — a self-contradiction between two of substrate's own answers. The cost is that
// two unlisted paths in one family resolve to the same image, which is visible only to a
// caller comparing IDs across paths substrate does not bundle.
func bundledImageForParameter(name string) (bundledImage, bool) {
	if !strings.HasPrefix(name, "/aws/service/") {
		return bundledImage{}, false
	}
	for _, img := range bundledImages {
		if img.Parameter == name {
			return img, true
		}
	}
	isAMI := strings.Contains(name, "ami-") ||
		strings.Contains(name, "/optimized-ami/") ||
		strings.Contains(name, "/ubuntu/") ||
		strings.Contains(name, "-latest")
	if !isAMI {
		return bundledImage{}, false
	}
	for _, fam := range bundledImageFamilyDefaults {
		if fam.Match != "" && !strings.Contains(name, fam.Match) {
			continue
		}
		for _, img := range bundledImages {
			if img.Parameter == fam.Parameter {
				return img, true
			}
		}
	}
	return bundledImage{}, false
}

// bundledImageByID returns the bundled image with the given ID in one region.
//
// The lookup is a scan and a hash per entry rather than a precomputed map, because the map
// would have to be keyed by (region, ID) and substrate serves every AWS region — building
// it would mean enumerating regions, which is the reason the IDs are derived rather than
// written into state at startup in the first place. The catalog is small and the scan runs
// once per launch.
func bundledImageByID(region, imageID string) (bundledImage, bool) {
	if !strings.HasPrefix(imageID, "ami-") {
		return bundledImage{}, false
	}
	for _, img := range bundledImages {
		if img.idIn(region) == imageID {
			return img, true
		}
	}
	return bundledImage{}, false
}
