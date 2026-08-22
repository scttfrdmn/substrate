package emulator

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// A snapshot's attributes: describing, modifying and resetting the list of accounts and
// groups that may create volumes from it (#709).
//
// The three operations are modeled on [EC2Plugin.describeInstanceAttribute] and its
// siblings, whose file records why each of these shapes is the way it is. Four of those
// decisions are load-bearing here and are repeated in the places they apply: the attribute
// name is validated before the snapshot is resolved; the wrapper fields are pointers so
// exactly one attribute element marshals; a present-but-empty element is not the same
// observation as an omitted one; and an attribute substrate will not answer is refused
// rather than answered with an invented default.
//
// Provenance: none of the three pages publishes an operation-specific error, so every
// refusal below is substrate's reading of a rule AWS states in prose. Each names the
// sentence it comes from. The message *shape* follows the one captured EC2 rejection of this
// kind (see [ec2UnknownInstanceAttribute]); the text is not itself a capture, and the
// stronger claim that comment makes for the instance wording is deliberately not extended to
// these.

// The snapshot attributes, per DescribeSnapshotAttribute's Valid Values.
//
// Both are answerable, which is why neither is refused on the describe path — and the reason
// differs between them. createVolumePermission is state substrate now holds. productCodes is
// state substrate holds *none of*, and that is a fact about every snapshot it can produce
// rather than a value it would have to invent: nothing in substrate assigns a product code to
// a snapshot, so "no product codes" is true rather than a plausible-looking default. That is
// the distinction [ec2AttributeSupported] draws for instances, where reporting
// sourceDestCheck as false would have been an invention.
const (
	ec2SnapAttrCreateVolumePermission = "createVolumePermission"
	ec2SnapAttrProductCodes           = "productCodes"
)

// ec2MaxSnapshotAttributeModifications is the number of permission changes one
// ModifySnapshotAttribute request may carry: "You can make up to 500 modifications to a
// snapshot in a single operation", from the operation's own description.
const ec2MaxSnapshotAttributeModifications = 500

// ec2CreateVolumePermissionItem is one rendered createVolumePermission entry.
//
// Both members carry omitempty because AWS's CreateVolumePermission has exactly one of them
// set per entry — a permission names either an account or the group "all" — and its own
// Example 1 renders a group-only item as <item><group>all</group></item> with no userId
// element at all.
type ec2CreateVolumePermissionItem struct {
	UserID string `xml:"userId,omitempty"`
	Group  string `xml:"group,omitempty"`
}

// ec2CreateVolumePermissionSetXML wraps the createVolumePermission items so the element can
// be omitted entirely.
//
// A plain []ec2CreateVolumePermissionItem with an `xml:"createVolumePermission>item"` path
// would emit the parent element even when the slice is empty, so a caller who asked for
// productCodes would additionally be told the snapshot has no volume permissions — an
// observation about an attribute they did not ask about, which real AWS does not make. This
// is #669's rule, recorded on [ec2InstanceAttributeXML].
type ec2CreateVolumePermissionSetXML struct {
	Items []ec2CreateVolumePermissionItem `xml:"item"`
}

// ec2ProductCodeSetXML wraps the productCodes items, a pointer for the same reason
// [ec2CreateVolumePermissionSetXML] is.
//
// Items is always empty in substrate, and the element is nonetheless *present* when
// productCodes is the attribute asked for: an SDK maps a present-but-empty element to an
// empty slice and an omitted one to nil, so omitting it would report "unknown" where the
// honest answer is "none".
type ec2ProductCodeSetXML struct {
	Items []ec2ProductCodeItem `xml:"item"`
}

// ec2ProductCodeItem is one rendered productCodes entry, per AWS's ProductCode.
//
// It is declared even though substrate renders none, so the element's shape is fixed by the
// reference rather than by whatever a future writer happens to emit.
type ec2ProductCodeItem struct {
	ProductCodeID   string `xml:"productCode,omitempty"`
	ProductCodeType string `xml:"type,omitempty"`
}

// describeSnapshotAttribute handles DescribeSnapshotAttribute.
//
// Attribute and SnapshotId are both Required: Yes, and "You can specify only one attribute at
// a time" — so unlike ModifySnapshotAttribute there is no wire form that carries the
// selection implicitly and an absent Attribute is a MissingParameter.
//
// The attribute name is checked before the snapshot is resolved, for the reason
// [EC2Plugin.describeInstanceAttribute] records: a snapshot ID that names nothing may be one
// a caller is still waiting on, while an attribute name the operation does not accept is a
// defect in the request that no retry fixes.
func (p *EC2Plugin) describeSnapshotAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	attr := req.Params["Attribute"]
	if attr == "" {
		return nil, ec2MissingParameter("Attribute")
	}
	if attr != ec2SnapAttrCreateVolumePermission && attr != ec2SnapAttrProductCodes {
		return nil, ec2UnknownSnapshotAttribute(attr)
	}
	snapshotID := req.Params["SnapshotId"]
	if snapshotID == "" {
		return nil, ec2MissingParameter("SnapshotId")
	}
	snap, found := p.ec2SnapshotResolver(reqCtx)(snapshotID)
	if awsErr := ec2RequireResource(ec2SnapshotIDKind, snapshotID, found); awsErr != nil {
		return nil, awsErr
	}

	type response struct {
		XMLName    xml.Name `xml:"DescribeSnapshotAttributeResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		SnapshotID string   `xml:"snapshotId"`

		CreateVolumePermission *ec2CreateVolumePermissionSetXML `xml:"createVolumePermission,omitempty"`
		ProductCodes           *ec2ProductCodeSetXML            `xml:"productCodes,omitempty"`
	}
	resp := response{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		SnapshotID: snap.SnapshotID,
	}
	switch attr {
	case ec2SnapAttrCreateVolumePermission:
		items := make([]ec2CreateVolumePermissionItem, 0, len(snap.CreateVolumePermissions))
		for _, perm := range snap.CreateVolumePermissions {
			// The conversion, rather than a field-by-field literal, is what the linter asks
			// for — and it fails to compile if either type gains a member the other lacks,
			// which is the right moment to notice that the wire shape and the record have
			// diverged.
			items = append(items, ec2CreateVolumePermissionItem(perm))
		}
		resp.CreateVolumePermission = &ec2CreateVolumePermissionSetXML{Items: items}
	case ec2SnapAttrProductCodes:
		resp.ProductCodes = &ec2ProductCodeSetXML{}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// modifySnapshotAttribute handles ModifySnapshotAttribute.
//
// # The two wire forms
//
// AWS accepts the same modification two ways, and an SDK picks between them, so both are
// read. The structured form is CreateVolumePermission.Add.N.{UserId,Group} and
// CreateVolumePermission.Remove.N.{UserId,Group}, which is what AWS's own two examples send.
// The flat form is OperationType (add|remove) together with UserId.N and UserGroup.N — note
// **UserGroup.N**, which is the wire member even though the CLI spells the same thing
// --group-names and an SDK spells it GroupNames. A request may use either; when it uses both,
// the two lists are simply concatenated, since AWS documents no precedence and neither form
// is a modifier of the other.
//
// # The rules
//
// Attribute is Required: No here, because the wire form carries the selection — the split
// [EC2Plugin.describeInstanceAttribute] documents for instances. When it is present it must
// name a valid attribute, and productCodes is refused: "Only volume creation permissions can
// be modified."
//
// "You may add or remove specified AWS account IDs from a snapshot's list of create volume
// permissions, but you cannot do both in a single operation." That sentence is scoped to
// **account IDs**, and AWS's own Example 2 proves the scope is not wider: it adds the group
// "all" while removing the account 111122223333 in one request. So the refusal fires only
// when a request both adds and removes a UserId; a group added alongside an account removed
// is a request AWS documents as legal, and refusing it would reject AWS's example.
//
// Group's only valid value is "all", so any other group is refused rather than stored — a
// stored group nothing can grant would be a permission a caller reads back and cannot act on.
//
// "You can share only unencrypted snapshots publicly", so adding the group "all" to an
// encrypted snapshot is refused. This is reachable because [EC2Plugin.createSnapshot] already
// records Encrypted from the source volume. The companion rule — "Snapshots ... with AWS
// Marketplace product codes cannot be made public" — is unreachable rather than unmodeled:
// substrate assigns no product codes, as [ec2SnapAttrProductCodes] records.
//
// A request that names no modification at all succeeds and changes nothing. Every parameter
// but SnapshotId is optional, and AWS documents no refusal for the empty case.
func (p *EC2Plugin) modifySnapshotAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if attr := req.Params["Attribute"]; attr != "" {
		switch attr {
		case ec2SnapAttrCreateVolumePermission:
		case ec2SnapAttrProductCodes:
			return nil, ec2InvalidParameterValue("Attribute", attr,
				"Only volume creation permissions can be modified.")
		default:
			return nil, ec2UnknownSnapshotAttribute(attr)
		}
	}
	snapshotID := req.Params["SnapshotId"]
	if snapshotID == "" {
		return nil, ec2MissingParameter("SnapshotId")
	}

	adds, removes, parseErr := ec2ParseVolumePermissionChanges(req.Params)
	if parseErr != nil {
		return nil, parseErr
	}
	if n := len(adds) + len(removes); n > ec2MaxSnapshotAttributeModifications {
		return nil, ec2InvalidParameterValue("CreateVolumePermission", fmt.Sprint(n),
			fmt.Sprintf("You can make up to %d modifications to a snapshot in a single operation.",
				ec2MaxSnapshotAttributeModifications))
	}
	if ec2PermissionsNameAUser(adds) && ec2PermissionsNameAUser(removes) {
		return nil, ec2InvalidParameterValue("OperationType", "add and remove",
			"You cannot both add and remove account IDs in a single operation.")
	}

	snap, found := p.ec2SnapshotResolver(reqCtx)(snapshotID)
	if awsErr := ec2RequireResource(ec2SnapshotIDKind, snapshotID, found); awsErr != nil {
		return nil, awsErr
	}
	if snap.Encrypted {
		for _, perm := range adds {
			if perm.Group != "" {
				return nil, ec2InvalidParameterValue("Group", perm.Group,
					"You can share only unencrypted snapshots publicly.")
			}
		}
	}

	snap.CreateVolumePermissions = ec2ApplyVolumePermissions(snap.CreateVolumePermissions, adds, removes)
	if err := p.ec2PutSnapshot(reqCtx, snap); err != nil {
		return nil, err
	}
	return ec2SnapshotAttributeReturn("ModifySnapshotAttributeResponse")
}

// resetSnapshotAttribute handles ResetSnapshotAttribute.
//
// Attribute is Required: Yes and takes the same two valid values as the describe, but "only
// the attribute for permission to create volumes can be reset" — so productCodes is accepted
// as a name and refused as a target, exactly as it is on the modify.
//
// Resetting clears the list rather than restoring a remembered default: AWS describes the
// result as "making it a private snapshot that can only be used by the account that created
// it", and an empty list is what a snapshot is created with.
func (p *EC2Plugin) resetSnapshotAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	attr := req.Params["Attribute"]
	if attr == "" {
		return nil, ec2MissingParameter("Attribute")
	}
	switch attr {
	case ec2SnapAttrCreateVolumePermission:
	case ec2SnapAttrProductCodes:
		return nil, ec2InvalidParameterValue("Attribute", attr,
			"Only the attribute for permission to create volumes can be reset.")
	default:
		return nil, ec2UnknownSnapshotAttribute(attr)
	}
	snapshotID := req.Params["SnapshotId"]
	if snapshotID == "" {
		return nil, ec2MissingParameter("SnapshotId")
	}
	snap, found := p.ec2SnapshotResolver(reqCtx)(snapshotID)
	if awsErr := ec2RequireResource(ec2SnapshotIDKind, snapshotID, found); awsErr != nil {
		return nil, awsErr
	}

	snap.CreateVolumePermissions = nil
	if err := p.ec2PutSnapshot(reqCtx, snap); err != nil {
		return nil, err
	}
	return ec2SnapshotAttributeReturn("ResetSnapshotAttributeResponse")
}

// ec2SnapshotAttributeReturn renders the <return>true</return> body the two mutating
// attribute operations share, under the element name root.
//
// Both publish exactly requestId and return, and "return" is documented as "true if the
// request succeeds, and an error otherwise" — so there is no false case to render.
func ec2SnapshotAttributeReturn(root string) (*AWSResponse, error) {
	type response struct {
		XMLName xml.Name
		XMLNS   string `xml:"xmlns,attr"`
		Return  bool   `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLName: xml.Name{Local: root},
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		Return:  true,
	})
}

// ec2ParseVolumePermissionChanges reads both wire forms of a create-volume-permission
// modification, returning the entries to add and to remove.
//
// The structured lists are walked contiguously from index 1 and stop at the first index with
// neither a UserId nor a Group, which is how the query protocol terminates an indexed list
// and what every other indexed walk in the plugin does — see [ec2ParseBlockDeviceMappings].
//
// A Group other than "all" is refused here rather than at the call site, so the flat and
// structured forms cannot disagree about it.
func ec2ParseVolumePermissionChanges(params map[string]string) (adds, removes []EC2CreateVolumePermission, err error) {
	for _, form := range []struct {
		prefix string
		target *[]EC2CreateVolumePermission
	}{
		{"CreateVolumePermission.Add", &adds},
		{"CreateVolumePermission.Remove", &removes},
	} {
		for n := 1; ; n++ {
			userID := params[fmt.Sprintf("%s.%d.UserId", form.prefix, n)]
			group := params[fmt.Sprintf("%s.%d.Group", form.prefix, n)]
			if userID == "" && group == "" {
				break
			}
			if group != "" && group != "all" {
				return nil, nil, ec2InvalidParameterValue("Group", group,
					"The only supported value is all.")
			}
			*form.target = append(*form.target, EC2CreateVolumePermission{UserID: userID, Group: group})
		}
	}

	// The flat form. OperationType decides which list the UserId.N and UserGroup.N values
	// join; it is validated even when neither list is present, since a misspelled operation
	// type is a defect in the request whether or not it would have changed anything.
	opType, hasOpType := params["OperationType"]
	if hasOpType && opType != "add" && opType != "remove" {
		return nil, nil, ec2InvalidParameterValue("OperationType", opType,
			"The supported values are add and remove.")
	}
	var flat []EC2CreateVolumePermission
	for _, userID := range extractIndexedParams(params, "UserId") {
		flat = append(flat, EC2CreateVolumePermission{UserID: userID})
	}
	for _, group := range extractIndexedParams(params, "UserGroup") {
		if group != "all" {
			return nil, nil, ec2InvalidParameterValue("UserGroup", group,
				"The only supported value is all.")
		}
		flat = append(flat, EC2CreateVolumePermission{Group: group})
	}
	if len(flat) > 0 {
		if !hasOpType {
			return nil, nil, ec2MissingParameter("OperationType")
		}
		if opType == "add" {
			adds = append(adds, flat...)
		} else {
			removes = append(removes, flat...)
		}
	}
	return adds, removes, nil
}

// ec2PermissionsNameAUser reports whether any entry names an account ID.
//
// It is what scopes the "cannot both add and remove in a single operation" refusal to account
// IDs, which is the scope AWS's sentence and its Example 2 together establish — see
// [EC2Plugin.modifySnapshotAttribute].
func ec2PermissionsNameAUser(perms []EC2CreateVolumePermission) bool {
	for _, perm := range perms {
		if perm.UserID != "" {
			return true
		}
	}
	return false
}

// ec2ApplyVolumePermissions returns existing with adds merged in and removes taken out.
//
// An add that is already present changes nothing rather than duplicating, and a remove that
// names nothing present is not an error — AWS documents neither as a refusal, and both are
// idempotent in the direction a caller's cleanup loop needs.
//
// Order is existing's, with anything new appended in request order, so the list
// DescribeSnapshotAttribute renders is the same on every run.
func ec2ApplyVolumePermissions(existing, adds, removes []EC2CreateVolumePermission) []EC2CreateVolumePermission {
	out := make([]EC2CreateVolumePermission, 0, len(existing)+len(adds))
	for _, perm := range existing {
		dropped := false
		for _, rm := range removes {
			if rm == perm {
				dropped = true
				break
			}
		}
		if !dropped {
			out = append(out, perm)
		}
	}
	for _, add := range adds {
		present := false
		for _, perm := range out {
			if perm == add {
				present = true
				break
			}
		}
		if !present {
			out = append(out, add)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ec2UnknownSnapshotAttribute returns the error EC2 raises for a snapshot attribute name it
// will not answer.
//
// The wording mirrors [ec2UnknownInstanceAttribute] so the two refusals read alike, but the
// capture that comment cites is for describe-instance-attribute and **is not claimed for
// this**: no capture of a snapshot-attribute rejection was found, and the three snapshot
// pages' Errors sections are empty. The code and status are the analogy; the message is
// substrate's.
func ec2UnknownSnapshotAttribute(attr string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value (" + attr + ") for parameter attribute is invalid. Unknown attribute.",
		HTTPStatus: http.StatusBadRequest,
	}
}
