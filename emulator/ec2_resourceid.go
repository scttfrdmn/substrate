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
	// EC2 publishes no InvalidAllocationID.Malformed; a malformed allocation ID
	// comes back as InvalidAllocationID.NotFound.
	ec2AllocationIDKind = ec2IDKind{
		Prefix: "eipalloc-", NotFound: "InvalidAllocationID.NotFound", Noun: "allocation ID",
	}
	// Likewise EC2 publishes no InvalidNatGatewayID.Malformed.
	ec2NatGatewayIDKind = ec2IDKind{
		Prefix: "nat-", NotFound: "InvalidNatGatewayID.NotFound", Noun: "NAT gateway",
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

// ec2RequireResource returns the kind's Malformed or NotFound error for an ID a
// mutating operation named that substrate holds no state for. found reports
// whether the lookup succeeded.
func ec2RequireResource(kind ec2IDKind, id string, found bool) error {
	switch {
	case !kind.wellFormed(id):
		return kind.malformedError(id)
	case !found:
		return kind.notFoundError(id)
	default:
		return nil
	}
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
