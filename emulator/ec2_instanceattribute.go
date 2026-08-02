package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// EC2 instance attributes, per DescribeInstanceAttribute's documented Valid Values:
//
//	instanceType | kernel | ramdisk | userData | disableApiTermination |
//	instanceInitiatedShutdownBehavior | rootDeviceName | blockDeviceMapping |
//	productCodes | sourceDestCheck | groupSet | ebsOptimized | sriovNetSupport |
//	enaSupport | enclaveOptions | disableApiStop
//
// Substrate reads the four that correspond to state it actually holds. The rest are
// deliberately rejected rather than answered with a default: reporting
// sourceDestCheck as false, say, would be indistinguishable from a real instance
// that has it disabled, and a consumer asserting on it would get a green test built
// on a value substrate invented. An error is the honest answer, and it is the same
// error real EC2 gives for a name it does not accept.
//
// enaSupport is the case that makes this concrete. It is in the Valid Values list,
// and the reference still says "Note that the enaSupport attribute is not
// supported." — real AWS rejects a value its own documentation lists. Substrate
// rejecting it is fidelity rather than a gap.
const (
	ec2AttrUserData              = "userData"
	ec2AttrInstanceType          = "instanceType"
	ec2AttrGroupSet              = "groupSet"
	ec2AttrDisableAPITermination = "disableApiTermination"
)

// ec2InstanceAttributeXML is one DescribeInstanceAttribute response.
//
// Every scalar attribute is wrapped in a <value> element rather than emitted
// directly — <instanceType><value>t1.micro</value></instanceType> — which all three
// of the reference's worked examples show and which matches the AttributeValue shape
// the response elements are typed as. An SDK unmarshals the wrapper into a struct
// with a Value field, so flattening it would leave every field nil.
//
// The attribute fields are pointers so that exactly one appears per response: AWS
// returns only the attribute that was asked for, and an empty non-pointer struct
// would still marshal its element. The distinction matters for the attribute that
// was never set — see [ec2AttributeValueXML].
type ec2InstanceAttributeXML struct {
	XMLName    xml.Name `xml:"DescribeInstanceAttributeResponse"`
	XMLNS      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId,omitempty"`
	InstanceID string   `xml:"instanceId"`

	UserData     *ec2AttributeValueXML `xml:"userData,omitempty"`
	InstanceType *ec2AttributeValueXML `xml:"instanceType,omitempty"`

	// DisableAPITermination carries the wire name disableApiTermination; the Go
	// field follows the initialism convention the linter enforces.
	DisableAPITermination *ec2AttributeValueXML `xml:"disableApiTermination,omitempty"`

	// Groups is an Array of GroupIdentifier, not an AttributeValue — the one
	// supported attribute that is not <value>-wrapped, per the response elements.
	//
	// It is a pointer to a wrapper struct rather than a plain []ec2GroupItem with a
	// `groupSet>item` path, because encoding/xml emits the *parent* element of such a
	// path even when the slice is empty: every response would carry a stray
	// <groupSet></groupSet>, and a caller asking for userData would be told the
	// instance has no security groups. A nil pointer is skipped outright.
	Groups *ec2GroupSetXML `xml:"groupSet,omitempty"`
}

// ec2GroupSetXML wraps the groupSet items so the element can be omitted entirely.
//
// It reuses [ec2GroupItem] so this response cannot drift from the groupSet that
// RunInstances and DescribeInstances report (#444).
type ec2GroupSetXML struct {
	Items []ec2GroupItem `xml:"item"`
}

// ec2AttributeValueXML is AWS's AttributeValue: a single <value> child.
//
// An attribute that was never set is reported as a present-but-empty element —
// <userData/> — rather than an omitted one. The reference cannot settle this: all
// three of its examples show an attribute that *has* a value. The shape comes from
// moto's test_describe_instance_attribute, which asserts `response["UserData"] ==
// {}` against its reimplementation — an empty mapping, which is what an SDK produces
// from a present element with no children. That is weaker provenance than a capture,
// and it is worth stating because the two shapes are not interchangeable to a
// caller: an SDK maps a present-but-empty element to an empty struct and an omitted
// one to nil, so `resp.UserData.Value` panics under one and not the other.
type ec2AttributeValueXML struct {
	Value string `xml:"value,omitempty"`
}

// describeInstanceAttribute handles DescribeInstanceAttribute.
//
// This is the only operation that reads an instance's user data back. Substrate
// recorded UserData at launch, and #453 made a launch template's user data reach the
// instance, but nothing could observe either: the field was write-only, so a
// consumer could not assert that the user data their IaC intended arrived, and
// #453's fallback was only ever tested indirectly (#473).
//
// Per CLAUDE.md's boundary, this reports recorded intent. Substrate does not execute
// user data or cloud-init, and reporting the value back is exactly the observable
// half of it.
func (p *EC2Plugin) describeInstanceAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instID := req.Params["InstanceId"]
	if instID == "" {
		return nil, ec2MissingParameter("InstanceId")
	}
	// Attribute is Required: Yes on this operation, unlike on
	// ModifyInstanceAttribute where the .Value suffix carries the selection.
	attr := req.Params["Attribute"]
	if attr == "" {
		return nil, ec2MissingParameter("Attribute")
	}

	// The attribute name is validated before the instance is looked up, so a
	// request that is wrong in both ways reports the attribute: the instance ID
	// might be a legitimate not-yet-propagated ID, while an unsupported attribute
	// name is a defect in the request that no amount of retrying fixes.
	if !ec2AttributeSupported(attr) {
		return nil, ec2UnknownInstanceAttribute(attr)
	}

	inst, awsErr, err := p.loadInstance(reqCtx, instID, "describeInstanceAttribute")
	if err != nil || awsErr != nil {
		return nil, errOrAWS(err, awsErr)
	}

	resp := ec2InstanceAttributeXML{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		InstanceID: inst.InstanceID,
	}
	switch attr {
	case ec2AttrUserData:
		// Already base64 as stored: RunInstances' UserData parameter is documented
		// as base64-encoded on the way in ("User data must be base64-encoded"), and
		// substrate stores the parameter verbatim. Re-encoding here would double it.
		resp.UserData = &ec2AttributeValueXML{Value: inst.UserData}
	case ec2AttrInstanceType:
		resp.InstanceType = &ec2AttributeValueXML{Value: inst.InstanceType}
	case ec2AttrDisableAPITermination:
		resp.DisableAPITermination = &ec2AttributeValueXML{
			Value: fmt.Sprintf("%t", inst.DisableAPITermination),
		}
	case ec2AttrGroupSet:
		// Always a present element, empty when the instance has no groups, for the
		// same reason an unset userData is present: an SDK distinguishes an empty
		// array from a nil one.
		resp.Groups = &ec2GroupSetXML{Items: p.ec2GroupItems(reqCtx, inst.SecurityGroupIDs)}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2AttributeSupported reports whether substrate can read attr off stored state.
func ec2AttributeSupported(attr string) bool {
	switch attr {
	case ec2AttrUserData, ec2AttrInstanceType, ec2AttrGroupSet, ec2AttrDisableAPITermination:
		return true
	default:
		return false
	}
}

// ec2UnknownInstanceAttribute returns the error EC2 raises for an attribute name it
// will not answer.
//
// This is the strongest message provenance in the release: the wording is captured
// from real AWS in aws/aws-cli#4273, where `aws ec2 describe-instance-attribute
// --attribute enaSupport` returns it, and it is byte-identical to moto's string — an
// independent reimplementation and a capture agreeing. The offending value is
// interpolated, as the capture does.
//
// DescribeInstanceAttribute's own Errors section is empty (it points only at the
// common error types), so the doc could not have supplied this.
func ec2UnknownInstanceAttribute(attr string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value (" + attr + ") for parameter attribute is invalid. Unknown attribute.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2ModifiableWhenStopped names the attributes whose modification requires a
// stopped instance, mapped to the ModifyInstanceAttribute parameter that sets each.
//
// ModifyInstanceAttribute's reference states the rule generally — "To modify some
// attributes, the instance must be stopped" — and names which in two places: Example
// 1, on instanceType, says "The instance must be in the stopped state", and the
// client-error table's IncorrectInstanceState entry says "some instance attributes,
// such as user data, can only be modified if the instance is in a 'stopped' state".
//
// disableApiTermination is deliberately absent. RunInstances' reference says it
// plainly: "You can enable termination protection when you launch an instance, while
// the instance is running, or while the instance is stopped." Gating it would refuse
// a call real EC2 accepts, which is the same class of defect as accepting one real
// EC2 refuses.
var ec2ModifiableWhenStopped = map[string]string{
	ec2AttrUserData:     "UserData.Value",
	ec2AttrInstanceType: "InstanceType.Value",
}

// ec2IncorrectInstanceState returns the error for modifying a stopped-only attribute
// on an instance that is not stopped.
//
// The code is documented — EC2's client-error table lists IncorrectInstanceState with
// user data as its worked example — while the message text is substrate's own: the
// table gives a description of the condition, not the string AWS sends, and no
// capture of this particular rejection was found. The attribute and the state are
// interpolated because they are the two facts a caller needs to act, and neither is
// recoverable from the code alone.
func ec2IncorrectInstanceState(attr, state string) *AWSError {
	return &AWSError{
		Code: "IncorrectInstanceState",
		Message: "The instance '" + attr + "' attribute cannot be modified while the instance is in the '" +
			state + "' state; stop the instance first",
		HTTPStatus: http.StatusBadRequest,
	}
}

// loadInstance reads one instance out of state, returning the AWS error for an ID
// that is malformed or names nothing.
//
// The two error returns are separate because they mean different things to the
// caller: err is an internal failure to wrap, while awsErr is a response to send.
// op names the calling operation in the wrapped error.
func (p *EC2Plugin) loadInstance(reqCtx *RequestContext, instID, op string) (*EC2Instance, *AWSError, error) {
	stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instID
	data, err := p.state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("ec2 %s get: %w", op, err)
	}
	switch {
	case !ec2InstanceIDKind.wellFormed(instID):
		return nil, ec2InstanceIDKind.malformedError(instID), nil
	case data == nil:
		return nil, ec2InstanceIDKind.notFoundError(instID), nil
	}
	var inst EC2Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, nil, fmt.Errorf("ec2 %s unmarshal: %w", op, err)
	}
	return &inst, nil, nil
}

// errOrAWS collapses [EC2Plugin.loadInstance]'s two error returns into the single
// error a handler returns, preferring the internal failure.
//
// A typed-nil *AWSError returned directly as an error interface is non-nil, which is
// the bug this exists to prevent: `return nil, awsErr` on a nil awsErr would send an
// empty error response instead of the success the caller earned.
func errOrAWS(err error, awsErr *AWSError) error {
	if err != nil {
		return err
	}
	if awsErr != nil {
		return awsErr
	}
	return nil
}

// ec2AttributeModificationState reports the attribute a request means to modify and
// whether the instance's state permits it.
//
// It returns the attribute name for the first stopped-only parameter present in the
// request, so the check runs on the same one the apply step will act on. AWS allows
// only one attribute per call ("You can specify only one attribute at a time"), so
// the iteration order over a request naming two is not a behavior substrate needs to
// define — but the map is walked in a fixed order anyway, since a nondeterministic
// error from a fixed request is the opposite of what this emulator is for.
func ec2AttributeModificationState(params map[string]string) string {
	for _, attr := range []string{ec2AttrUserData, ec2AttrInstanceType} {
		if _, ok := params[ec2ModifiableWhenStopped[attr]]; ok {
			return attr
		}
	}
	return ""
}

// ec2InstanceStopped reports whether an instance is in the state a stopped-only
// attribute modification requires.
func ec2InstanceStopped(inst *EC2Instance) bool {
	return strings.EqualFold(inst.State.Name, "stopped")
}
