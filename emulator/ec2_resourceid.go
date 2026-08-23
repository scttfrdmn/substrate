package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// ec2IDKind describes one EC2 resource-ID family: its wire prefix and the two
// error codes real EC2 raises when a caller names an ID explicitly and the ID is
// either syntactically wrong or absent (#391).
//
// AWS's casing is inconsistent across these codes — `InvalidVpcID.NotFound` but
// `InvalidGroupId.Malformed`, `InvalidSnapshot.NotFound` but
// `InvalidSnapshotID.Malformed` — and SDK callers match on the literal string,
// so each pair is spelled out verbatim from the EC2 error reference rather than
// derived from a template.
type ec2IDKind struct {
	// Prefix is the ID's literal wire prefix, including the hyphen ("vpc-").
	Prefix string
	// NotFound is the error code for a well-formed ID that names nothing.
	NotFound string
	// Malformed is the error code for a syntactically invalid ID. It is empty
	// for the resources where EC2 publishes no Malformed variant, in which case
	// a malformed ID is reported as NotFound.
	Malformed string
	// Noun names the resource in an error message ("VPC", "subnet").
	Noun string
}

// EC2 resource-ID kinds, with error codes taken verbatim from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html.
//
// Every operation that names a resource of one of these families answers through the kind
// — via [ec2IDFilter] on a Describe*, or [ec2RequireResource] / [ec2RequireNamedResource]
// on a mutation. A hand-written &AWSError carrying one of these codes is a bug: it is a
// second place for a wording, status or request-ID change to have to land, and it is
// invisible to anyone reading this table (#713). The one deliberate exception is
// [EC2Plugin.ec2CheckSecurityGroups]' membership refusal, which reuses
// InvalidGroup.NotFound for a group that exists in the wrong VPC — a different condition
// from absence, so it cannot share the kind's message.
var (
	ec2InstanceIDKind = ec2IDKind{
		Prefix: "i-", NotFound: "InvalidInstanceID.NotFound",
		Malformed: "InvalidInstanceID.Malformed", Noun: "instance",
	}
	ec2VPCIDKind = ec2IDKind{
		Prefix: "vpc-", NotFound: "InvalidVpcID.NotFound",
		Malformed: "InvalidVpcID.Malformed", Noun: "VPC",
	}
	ec2SubnetIDKind = ec2IDKind{
		Prefix: "subnet-", NotFound: "InvalidSubnetID.NotFound",
		Malformed: "InvalidSubnetID.Malformed", Noun: "subnet",
	}
	ec2SecurityGroupIDKind = ec2IDKind{
		Prefix: "sg-", NotFound: "InvalidGroup.NotFound",
		Malformed: "InvalidGroupId.Malformed", Noun: "security group",
	}
	ec2InternetGatewayIDKind = ec2IDKind{
		Prefix: "igw-", NotFound: "InvalidInternetGatewayID.NotFound",
		Malformed: "InvalidInternetGatewayId.Malformed", Noun: "internet gateway",
	}
	ec2RouteTableIDKind = ec2IDKind{
		Prefix: "rtb-", NotFound: "InvalidRouteTableID.NotFound",
		Malformed: "InvalidRouteTableId.Malformed", Noun: "route table",
	}
	ec2SnapshotIDKind = ec2IDKind{
		Prefix: "snap-", NotFound: "InvalidSnapshot.NotFound",
		Malformed: "InvalidSnapshotID.Malformed", Noun: "snapshot",
	}
	ec2VolumeIDKind = ec2IDKind{
		Prefix: "vol-", NotFound: "InvalidVolume.NotFound",
		Malformed: "InvalidVolumeID.Malformed", Noun: "volume",
	}
	// AMIs are the one family whose absence code names no ID at all —
	// InvalidAMIID.NotFound, "The specified AMI doesn't exist" — which is the
	// cross-naming snapshots already carry the other way round.
	//
	// The reference publishes a third code, InvalidAMIID.Unavailable, for an AMI
	// "deregistered and no longer available, or ... not in a state from which you can
	// launch an instance". Substrate raises it nowhere: it models no
	// deregistered-but-extant image, since DeregisterImage deletes the record, and
	// every AMI it holds is "available". An unavailable AMI is therefore reported as
	// absent, which is what a caller polling for one observes anyway.
	ec2ImageIDKind = ec2IDKind{
		Prefix: "ami-", NotFound: "InvalidAMIID.NotFound",
		Malformed: "InvalidAMIID.Malformed", Noun: "image",
	}
	// EC2 publishes no InvalidAllocationID.Malformed; a malformed allocation ID
	// comes back as InvalidAllocationID.NotFound.
	ec2AllocationIDKind = ec2IDKind{
		Prefix: "eipalloc-", NotFound: "InvalidAllocationID.NotFound", Noun: "allocation",
	}
	// NAT gateways are the one family whose malformed code sits outside the
	// Invalid*ID.Malformed naming: the reference publishes it as NatGatewayMalformed,
	// "The specified NAT gateway ID is not formed correctly. Ensure that you specify the
	// NAT gateway ID in the form nat-xxxxxxxxxxxxxxxxx." Pairing it with
	// InvalidNatGatewayID.NotFound is the same cross-naming the kinds above already
	// carry for snapshots and security groups (#713).
	//
	// The reference also publishes a second absence code, NatGatewayNotFound ("The
	// specified NAT gateway does not exist."), and no per-operation page says which
	// operation raises which — DeleteNatGateway's Errors section is empty, as EC2's are.
	// Substrate answers InvalidNatGatewayID.NotFound everywhere, because that is the
	// spelling every other kind here follows and the one DescribeNatGateways has always
	// published; DeleteNatGateway's NatGatewayNotFound converged onto it.
	ec2NatGatewayIDKind = ec2IDKind{
		Prefix: "nat-", NotFound: "InvalidNatGatewayID.NotFound",
		Malformed: "NatGatewayMalformed", Noun: "NAT gateway",
	}
)

// wellFormed reports whether id has the kind's prefix followed by a non-empty
// run of lowercase hex digits.
//
// Substrate's generators emit 16 hex characters where AWS emits 8 or 17, so the
// length is deliberately not checked: a substrate-minted ID must stay valid, and
// AWS itself accepts both the legacy 8-character and the current 17-character
// form for several resources.
func (k ec2IDKind) wellFormed(id string) bool {
	suffix, ok := strings.CutPrefix(id, k.Prefix)
	if !ok || suffix == "" {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		c := suffix[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// malformedError returns the AWSError for a syntactically invalid id, falling
// back to the NotFound code for kinds with no published Malformed variant.
func (k ec2IDKind) malformedError(id string) *AWSError {
	if k.Malformed == "" {
		return k.notFoundError(id)
	}
	return &AWSError{
		Code:       k.Malformed,
		Message:    "Invalid id: \"" + id + "\"",
		HTTPStatus: http.StatusBadRequest,
	}
}

// notFoundError returns the AWSError for a well-formed id that names nothing.
func (k ec2IDKind) notFoundError(id string) *AWSError {
	return &AWSError{
		Code:       k.NotFound,
		Message:    "The " + k.Noun + " ID '" + id + "' does not exist",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2IDFilter decides which stored resources a Describe* call should return when
// the caller named IDs explicitly, and reports the ones that did not resolve.
//
// Real EC2 treats an explicit ID list as an assertion that every ID exists: a
// missing one fails the whole call with Invalid*.NotFound rather than being
// silently dropped from the result set (#391). An empty list means "describe
// everything" and matches unconditionally.
//
// Usage inside a Describe* loop:
//
//	ids := newEC2IDFilter(extractIndexedParams(req.Params, "VpcId"), ec2VPCIDKind)
//	if err := ids.validate(); err != nil {
//	    return nil, err
//	}
//	... for each stored vpc: if !ids.match(vpc.VPCID) { continue }
//	if err := ids.unresolved(); err != nil {
//	    return nil, err
//	}
type ec2IDFilter struct {
	kind ec2IDKind
	// want holds each requested ID mapped to whether it has been matched.
	want map[string]bool
	// order preserves request order so the reported ID is deterministic.
	order []string
}

// newEC2IDFilter builds a filter over the IDs a request named explicitly.
func newEC2IDFilter(ids []string, kind ec2IDKind) *ec2IDFilter {
	f := &ec2IDFilter{kind: kind, want: make(map[string]bool, len(ids))}
	for _, id := range ids {
		if _, seen := f.want[id]; seen {
			continue
		}
		f.want[id] = false
		f.order = append(f.order, id)
	}
	return f
}

// validate rejects any syntactically invalid requested ID with the kind's
// Malformed code. It must be called before the describe loop, because real EC2
// validates ID syntax before it looks anything up.
func (f *ec2IDFilter) validate() error {
	for _, id := range f.order {
		if !f.kind.wellFormed(id) {
			return f.kind.malformedError(id)
		}
	}
	return nil
}

// match reports whether id belongs in the response, recording that it resolved.
// With no explicit IDs requested, every resource matches.
func (f *ec2IDFilter) match(id string) bool {
	if len(f.want) == 0 {
		return true
	}
	if _, ok := f.want[id]; !ok {
		return false
	}
	f.want[id] = true
	return true
}

// matchOrNamed is [ec2IDFilter.match] for a describe whose resources a caller can also name
// by a non-ID selector: it takes that selector's requested list and this resource's value for
// it, and reports whether the resource belongs in the response.
//
// The two lists **union**, for the reason [ec2SelectedByEither] gives — DescribeSecurityGroups'
// GroupId.N and GroupName.N are two spellings of one question, so intersecting them would
// answer with the empty set while both named groups exist. That helper cannot be used directly
// here, because it takes two plain lists and this one half of the pair carries the ID filter's
// malformed-form and NotFound contract.
//
// match is called unconditionally, so the ID list's resolution bookkeeping still runs whichever
// half selected the resource: an ID a name-union let through was still resolved, and one the
// walk never saw is still the NotFound [ec2IDFilter.unresolved] reports. That cannot let a
// resource in wrongly, because match returning true already means the request named its ID.
//
// The name list has no such contract: a name matching nothing narrows the answer to nothing
// rather than refusing (#749). It is exact membership, never a glob, for the reason
// [ec2SelectedByEither] states.
func (f *ec2IDFilter) matchOrNamed(id string, names []string, name string) bool {
	matched := f.match(id)
	if len(names) == 0 {
		return matched
	}
	return (len(f.order) > 0 && matched) || containsStr(names, name)
}

// unresolved returns the kind's NotFound error for the first requested ID that
// match never saw, or nil if every requested ID resolved. Call it after the
// describe loop.
//
// A resource that a filter (rather than the ID list) excluded still counts as
// resolved, matching EC2: a Describe* with both an ID and a non-matching filter
// returns an empty set, not NotFound.
func (f *ec2IDFilter) unresolved() error {
	for _, id := range f.order {
		if !f.want[id] {
			return f.kind.notFoundError(id)
		}
	}
	return nil
}

// pending returns the requested IDs that match has not seen yet, in request order.
//
// It exists for a describe whose resources do not all live in the store it walks:
// DescribeImages walks the account's own AMIs, and a bundled public AMI is deliberately
// not among them (#733), so a caller naming one would reach [ec2IDFilter.unresolved] and
// be told it does not exist. Resolving what is still pending after the walk, and calling
// match on each one that resolves, keeps "named explicitly" working without making the
// bundled catalog part of an unqualified describe's answer.
//
// It returns nothing when no IDs were requested, so a caller cannot mistake "describe
// everything" for "everything is missing".
func (f *ec2IDFilter) pending() []string {
	var out []string
	for _, id := range f.order {
		if !f.want[id] {
			out = append(out, id)
		}
	}
	return out
}

// requireResource returns the kind's Malformed error for a syntactically invalid id, its
// NotFound error for a well-formed id that named nothing, or nil. found reports whether
// the lookup succeeded.
//
// The typed *AWSError return is for the callers that thread one — returning it through an
// error-typed variable would make a nil result a non-nil error interface holding a nil
// pointer. [ec2RequireResource] is the error-typed form, and does the conversion once.
func (k ec2IDKind) requireResource(id string, found bool) *AWSError {
	switch {
	case !k.wellFormed(id):
		return k.malformedError(id)
	case !found:
		return k.notFoundError(id)
	default:
		return nil
	}
}

// ec2RequireResource returns the kind's Malformed or NotFound error for an ID a
// mutating operation named that substrate holds no state for. found reports
// whether the lookup succeeded.
func ec2RequireResource(kind ec2IDKind, id string, found bool) error {
	if err := kind.requireResource(id, found); err != nil {
		return err
	}
	return nil
}

// ec2RequireNamedResource is [ec2RequireResource] for an ID read from a single named
// request parameter, checking the three conditions in the order real EC2 reports them: an
// omitted parameter is a MissingParameter, a syntactically invalid ID is the kind's
// Malformed, and a well-formed ID that names nothing is the kind's NotFound.
//
// paramName is the wire parameter the ID came from, so the refusal names what the caller
// left out. Without it an omitted ID reads as the empty string and falls through to
// Malformed, reporting an invalid value for a parameter that was never sent.
func ec2RequireNamedResource(kind ec2IDKind, paramName, id string, found bool) error {
	if id == "" {
		return ec2MissingParameter(paramName)
	}
	return ec2RequireResource(kind, id, found)
}

// ec2MissingParameter returns the error EC2 raises for a required-but-absent
// request parameter.
func ec2MissingParameter(name string) error {
	return &AWSError{
		Code:       "MissingParameter",
		Message:    fmt.Sprintf("The request must contain the parameter %s", name),
		HTTPStatus: http.StatusBadRequest,
	}
}
