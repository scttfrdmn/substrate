package emulator

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// AttachNetworkInterface, DetachNetworkInterface, DeleteNetworkInterface and
// ModifyNetworkInterfaceAttribute (#1522). Each takes [EC2Plugin.addrMu], which serializes every
// change to an interface's attachment and addresses; see ec2_eni_ops.go for the shared refusals.

// requireNetworkInterface reads the interface a request names in param, answering
// MissingParameter, Malformed or NotFound as EC2 does. An interface an older build's instance
// reports is read as synthesized ([EC2Plugin.requireInterfaceRecord]).
func (p *EC2Plugin) requireNetworkInterface(reqCtx *RequestContext, param, eniID string) (EC2NetworkInterfaceRecord, error) {
	if eniID == "" {
		return EC2NetworkInterfaceRecord{}, ec2MissingParameter(param)
	}
	if !ec2NetworkInterfaceIDKind.wellFormed(eniID) {
		return EC2NetworkInterfaceRecord{}, ec2NetworkInterfaceIDKind.malformedError(eniID)
	}
	rec, found, err := p.requireInterfaceRecord(reqCtx, eniID)
	if err != nil {
		return rec, err
	}
	if !found {
		return rec, ec2NetworkInterfaceIDKind.notFoundError(eniID)
	}
	return rec, nil
}

// ec2ParseIndex reads a non-negative integer parameter, def when absent.
func ec2ParseIndex(params map[string]string, name string, def int) (int, *AWSError) {
	raw := params[name]
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, ec2InvalidParameterValue(name, raw, "It must be a non-negative integer.")
	}
	return n, nil
}

// attachNetworkInterface handles AttachNetworkInterface. The interface must be available and in
// the instance's Availability Zone, the instance not terminated, and the position — device index
// on network card — free on the instance. The attachment's DeleteOnTermination is false, as for an
// interface a launch attaches rather than creates; it is changed through
// ModifyNetworkInterfaceAttribute.
func (p *EC2Plugin) attachNetworkInterface(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	if req.Params["DeviceIndex"] == "" {
		return nil, ec2MissingParameter("DeviceIndex")
	}
	deviceIndex, awsErr := ec2ParseIndex(req.Params, "DeviceIndex", 0)
	if awsErr != nil {
		return nil, awsErr
	}
	networkCard, awsErr := ec2ParseIndex(req.Params, "NetworkCardIndex", 0)
	if awsErr != nil {
		return nil, awsErr
	}
	instanceID := req.Params["InstanceId"]
	if instanceID == "" {
		return nil, ec2MissingParameter("InstanceId")
	}

	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()
	rec, err := p.requireNetworkInterface(reqCtx, "NetworkInterfaceId", req.Params["NetworkInterfaceId"])
	if err != nil {
		return nil, err
	}
	inst, awsErr, err := p.loadInstance(reqCtx, instanceID, "attachNetworkInterface")
	if err != nil || awsErr != nil {
		return nil, errOrAWS(err, awsErr)
	}
	if inst.State.Name == ec2StateTerminated {
		return nil, ec2CannotTransitionTerminated("attached to")
	}
	if rec.Status != "available" || rec.attached() {
		return nil, ec2NetworkInterfaceInUseError(rec.NetworkInterfaceID)
	}
	if rec.AvailabilityZone != "" && inst.AvailabilityZone != "" && rec.AvailabilityZone != inst.AvailabilityZone {
		return nil, ec2AttachZoneMismatchError()
	}
	idx, err := p.ec2InterfacesByInstance(reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}
	for _, held := range ec2InstanceInterfaces(*inst, idx) {
		if held.Attachment != nil && held.Attachment.DeviceIndex == deviceIndex &&
			held.Attachment.NetworkCardIndex == networkCard {
			return nil, ec2DeviceIndexInUseError(inst.InstanceID, deviceIndex)
		}
	}
	rec.Status = "in-use"
	rec.Attachment = &EC2NetworkInterfaceAttachment{
		AttachmentID:     generateENIAttachmentID(reqCtx.IDs),
		InstanceID:       inst.InstanceID,
		DeviceIndex:      deviceIndex,
		NetworkCardIndex: networkCard,
		AttachTime:       p.tc.Now().UTC().Format(time.RFC3339),
	}
	if err := p.putNetworkInterface(ctx, rec); err != nil {
		return nil, fmt.Errorf("ec2 attachNetworkInterface: %w", err)
	}
	type response struct {
		XMLName          xml.Name `xml:"AttachNetworkInterfaceResponse"`
		XMLNS            string   `xml:"xmlns,attr"`
		AttachmentID     string   `xml:"attachmentId"`
		NetworkCardIndex int      `xml:"networkCardIndex"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:            "http://ec2.amazonaws.com/doc/2016-11-15/",
		AttachmentID:     rec.Attachment.AttachmentID,
		NetworkCardIndex: networkCard,
	})
}

// detachNetworkInterface handles DetachNetworkInterface, by attachment ID. The instance's primary
// interface ([ec2IsPrimaryPosition]) cannot be detached; any other is left available, keeping its
// addresses. Force is accepted and changes nothing, since a substrate detach always completes.
func (p *EC2Plugin) detachNetworkInterface(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	attachmentID := req.Params["AttachmentId"]
	if attachmentID == "" {
		return nil, ec2MissingParameter("AttachmentId")
	}
	if !ec2NetworkInterfaceAttachmentIDKind.wellFormed(attachmentID) {
		return nil, ec2NetworkInterfaceAttachmentIDKind.malformedError(attachmentID)
	}
	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()
	// An attachment an older instance reports is refused for the primary before anything is
	// written, and otherwise its instance's interfaces are stored first.
	_, legacy, err := p.legacyInstanceReporting(reqCtx, func(l EC2NetworkInterfaceRecord) bool {
		return l.Attachment.AttachmentID == attachmentID
	})
	if err != nil {
		return nil, err
	}
	if legacy != nil {
		if ec2IsPrimaryAttachment(legacy.Attachment) {
			return nil, ec2PrimaryDetachError()
		}
		if _, _, err := p.materializeLegacyInterfaces(reqCtx, txn, legacy.NetworkInterfaceID); err != nil {
			return nil, err
		}
	}
	all, err := p.ec2NetworkInterfaces(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}
	for _, rec := range all {
		if !rec.attached() || rec.Attachment.AttachmentID != attachmentID {
			continue
		}
		if ec2IsPrimaryAttachment(rec.Attachment) {
			return nil, ec2PrimaryDetachError()
		}
		rec.detach()
		if err := p.putNetworkInterface(ctx, rec); err != nil {
			return nil, fmt.Errorf("ec2 detachNetworkInterface: %w", err)
		}
		if err := txn.commit(); err != nil {
			return nil, err
		}
		return ec2ReturnTrue("DetachNetworkInterface")
	}
	return nil, ec2NetworkInterfaceAttachmentIDKind.notFoundError(attachmentID)
}

// deleteNetworkInterface handles DeleteNetworkInterface. An interface still attached is refused
// with InvalidNetworkInterface.InUse; an available one is removed and its addresses released.
func (p *EC2Plugin) deleteNetworkInterface(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()
	rec, err := p.requireNetworkInterface(reqCtx, "NetworkInterfaceId", req.Params["NetworkInterfaceId"])
	if err != nil {
		return nil, err
	}
	if rec.Status == "in-use" || rec.attached() {
		return nil, ec2NetworkInterfaceInUseError(rec.NetworkInterfaceID)
	}
	if err := txn.release(rec.SubnetID, rec.addresses(), rec.NetworkInterfaceID); err != nil {
		return nil, err
	}
	if err := p.state.Delete(ctx, ec2Namespace, ec2ENIKey(rec.AccountID, rec.Region, rec.NetworkInterfaceID)); err != nil {
		return nil, fmt.Errorf("ec2 deleteNetworkInterface: %w", err)
	}
	if err := txn.commit(); err != nil {
		return nil, err
	}
	return ec2ReturnTrue("DeleteNetworkInterface")
}

// modifyNetworkInterfaceAttribute handles ModifyNetworkInterfaceAttribute for the four
// attributes substrate records: Description.Value, SourceDestCheck.Value, SecurityGroupId.N and
// Attachment.DeleteOnTermination (with Attachment.AttachmentId naming the interface's current
// attachment). The others the request shape carries are accepted and not recorded.
//
// Only the interface's record is written. An instance's groupSet is its primary interface's
// groups, read from the interface ([ec2InstanceGroups]), so changing them changes what the
// instance reports without a write to the instance record — which TerminateInstances rewrites
// under no lock this operation holds. An interface an older build's instance reports is stored on
// this first write, under the ID it reports ([EC2Plugin.materializeLegacyInterfaces]).
func (p *EC2Plugin) modifyNetworkInterfaceAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	eniID := req.Params["NetworkInterfaceId"]
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()

	// Every refusal is decided on the interface as read, before anything is written.
	current, err := p.requireNetworkInterface(reqCtx, "NetworkInterfaceId", eniID)
	if err != nil {
		return nil, err
	}
	groups := indexedParams(req.Params, "SecurityGroupId.%d")
	if len(groups) > 0 {
		if err := p.ec2CheckSecurityGroups(reqCtx, groups, current.VPCID); err != nil {
			return nil, err
		}
	}
	attachmentID := req.Params["Attachment.AttachmentId"]
	if attachmentID != "" {
		if !ec2NetworkInterfaceAttachmentIDKind.wellFormed(attachmentID) {
			return nil, ec2NetworkInterfaceAttachmentIDKind.malformedError(attachmentID)
		}
		if !current.attached() || current.Attachment.AttachmentID != attachmentID {
			return nil, ec2NetworkInterfaceAttachmentIDKind.notFoundError(attachmentID)
		}
	}

	rec, found, err := p.materializeLegacyInterfaces(reqCtx, txn, eniID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ec2NetworkInterfaceIDKind.notFoundError(eniID)
	}
	if v, ok := req.Params["Description.Value"]; ok {
		rec.Description = v
	}
	if v := req.Params["SourceDestCheck.Value"]; v != "" {
		rec.SourceDestCheckDisabled = !strings.EqualFold(v, "true")
	}
	if len(groups) > 0 {
		rec.SecurityGroupIDs = groups
	}
	if v := req.Params["Attachment.DeleteOnTermination"]; attachmentID != "" && v != "" {
		rec.Attachment.DeleteOnTermination = strings.EqualFold(v, "true")
	}
	if err := p.putNetworkInterface(ctx, rec); err != nil {
		return nil, fmt.Errorf("ec2 modifyNetworkInterfaceAttribute: %w", err)
	}
	if err := txn.commit(); err != nil {
		return nil, err
	}
	return ec2ReturnTrue("ModifyNetworkInterfaceAttribute")
}
