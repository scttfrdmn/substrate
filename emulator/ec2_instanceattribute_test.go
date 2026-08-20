package emulator_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// instanceAttribute is a decoded DescribeInstanceAttribute response.
//
// Every attribute field is a pointer so the tests can tell an *absent* element from
// a present-but-empty one. That distinction is the whole of #473's hardest
// acceptance criterion: an SDK maps <userData/> to an empty struct and an omitted
// userData to nil, and a consumer's assertion differs between them. A value-typed
// field would decode both to the same zero and the criterion would be untestable.
type instanceAttribute struct {
	XMLName    xml.Name `xml:"DescribeInstanceAttributeResponse"`
	InstanceID string   `xml:"instanceId"`

	UserData     *attrValue `xml:"userData"`
	InstanceType *attrValue `xml:"instanceType"`
	DisableAPI   *attrValue `xml:"disableApiTermination"`

	Groups []struct {
		GroupID   string `xml:"groupId"`
		GroupName string `xml:"groupName"`
	} `xml:"groupSet>item"`
}

// attrValue is AWS's AttributeValue wrapper: a single <value> child.
type attrValue struct {
	Value string `xml:"value"`
}

// describeInstanceAttribute reads one attribute off an instance, requiring 200.
func describeInstanceAttribute(t *testing.T, ts *httptest.Server, instID, attr string) instanceAttribute {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeInstanceAttribute",
		"InstanceId": instID,
		"Attribute":  attr,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"DescribeInstanceAttribute(%s) must succeed", attr)
	var out instanceAttribute
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	return out
}

// modifyInstanceAttribute sends ModifyInstanceAttribute and returns the status.
func modifyInstanceAttribute(t *testing.T, ts *httptest.Server, params map[string]string) int {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	return resp.StatusCode
}

// stopInstance moves an instance to the stopped state, which is what the gated
// attribute modifications require.
func stopInstance(t *testing.T, ts *httptest.Server, instID string) {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action": "StopInstances", "InstanceId.1": instID,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "StopInstances must succeed")
}

// TestEC2_InstanceAttribute_UserData is #473's reason for existing: RunInstances
// recorded UserData and nothing could read it back, so a consumer could not assert
// that the user data their IaC intended reached the instance.
//
// The value is asserted as the exact base64 string the launch sent, not a decoded
// form: AWS documents UserData as base64-encoded on the way in ("User data must be
// base64-encoded"), and substrate stores the parameter verbatim, so re-encoding on
// read would double it. A test asserting the decoded plaintext would pass against
// that bug.
func TestEC2_InstanceAttribute_UserData(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	encoded := base64.StdEncoding.EncodeToString([]byte("#!/bin/bash\necho hello\n"))
	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro", "UserData": encoded,
	})

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData, "userData must be present")
	assert.Equal(t, encoded, got.UserData.Value,
		"the recorded value is returned as stored, still base64")
	assert.Equal(t, id, got.InstanceID, "the response names the instance")

	// Only the attribute asked for comes back. AWS returns one attribute per call,
	// so a response carrying instanceType alongside userData would be wrong even
	// though every value in it is right.
	assert.Nil(t, got.InstanceType, "instanceType must not appear in a userData response")
	assert.Nil(t, got.DisableAPI, "disableApiTermination must not appear either")
}

// TestEC2_InstanceAttribute_UserDataFromLaunchTemplate is the assertion #473 exists
// to make possible, and the regression guard on #453's UserData fallback.
//
// Before DescribeInstanceAttribute the fallback was only ever tested indirectly,
// which means it could have regressed silently: nothing observable would change.
func TestEC2_InstanceAttribute_UserDataFromLaunchTemplate(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	encoded := base64.StdEncoding.EncodeToString([]byte("from-the-template"))
	ltID := createLaunchTemplate(t, ts, "lt-userdata", map[string]string{
		"LaunchTemplateData.ImageId":      "ami-tmpl",
		"LaunchTemplateData.InstanceType": "t3.small",
		"LaunchTemplateData.UserData":     encoded,
	})

	id := runInstance(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateId": ltID,
	})

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData)
	assert.Equal(t, encoded, got.UserData.Value,
		"a launch that took its UserData from a template reports the template's value")
}

// TestEC2_InstanceAttribute_RequestBeatsTemplate pins that the read reports what the
// launch actually resolved rather than either source unconditionally. The request
// wins, per the RunInstances rule that "any additional parameters that you specify
// for the new instance overwrite the corresponding parameters included in the launch
// template".
func TestEC2_InstanceAttribute_RequestBeatsTemplate(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	tmpl := base64.StdEncoding.EncodeToString([]byte("template"))
	req := base64.StdEncoding.EncodeToString([]byte("request"))
	ltID := createLaunchTemplate(t, ts, "lt-userdata-override", map[string]string{
		"LaunchTemplateData.ImageId":  "ami-tmpl",
		"LaunchTemplateData.UserData": tmpl,
	})

	id := runInstance(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateId": ltID,
		"UserData":                        req,
	})

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData)
	assert.Equal(t, req, got.UserData.Value, "the request's UserData wins over the template's")
}

// TestEC2_InstanceAttribute_UnsetIsPresentButEmpty covers the criterion the
// reference cannot settle. All three of its worked examples show an attribute that
// *has* a value, so the unset shape comes from moto's
// test_describe_instance_attribute, which asserts `response["UserData"] == {}` — an
// empty mapping, i.e. a present element with no children.
//
// Both halves are asserted, because they fail differently: an omitted element
// decodes to a nil pointer (and gives an SDK caller a nil deref on .Value), while a
// present element carrying a fabricated value would be worse still.
func TestEC2_InstanceAttribute_UnsetIsPresentButEmpty(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
	})

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData,
		"an unset userData is a present element, not an omitted one")
	assert.Empty(t, got.UserData.Value, "and it carries no value")
}

// TestEC2_InstanceAttribute_SupportedAttributes round-trips the three attributes
// besides userData that read state substrate already holds.
func TestEC2_InstanceAttribute_SupportedAttributes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	vpcID := createVPC(t, ts, "10.9.0.0/16")
	sgID := createSecurityGroup(t, ts, vpcID, "attr-sg")
	subnetID := createSubnet(t, ts, vpcID, "10.9.1.0/24")

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.large",
		"SubnetId": subnetID, "SecurityGroupId.1": sgID,
		"DisableApiTermination": "true",
	})

	t.Run("instanceType", func(t *testing.T) {
		got := describeInstanceAttribute(t, ts, id, "instanceType")
		require.NotNil(t, got.InstanceType)
		assert.Equal(t, "t3.large", got.InstanceType.Value)
	})

	t.Run("disableApiTermination", func(t *testing.T) {
		got := describeInstanceAttribute(t, ts, id, "disableApiTermination")
		require.NotNil(t, got.DisableAPI)
		assert.Equal(t, "true", got.DisableAPI.Value,
			"the launch-time DisableApiTermination is recorded and reported")
	})

	t.Run("groupSet is an array, not a value wrapper", func(t *testing.T) {
		got := describeInstanceAttribute(t, ts, id, "groupSet")
		require.Len(t, got.Groups, 1,
			"groupSet is an Array of GroupIdentifier, so it decodes from groupSet>item")
		assert.Equal(t, sgID, got.Groups[0].GroupID)
		assert.Equal(t, "attr-sg", got.Groups[0].GroupName)
	})
}

// TestEC2_InstanceAttribute_DisableAPITerminationDefaultsFalse pins the documented
// default. It is the zero value, so an instance from an event log written before the
// field existed also reads back this way.
func TestEC2_InstanceAttribute_DisableAPITerminationDefaultsFalse(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	got := describeInstanceAttribute(t, ts, id, "disableApiTermination")
	require.NotNil(t, got.DisableAPI)
	assert.Equal(t, "false", got.DisableAPI.Value,
		"AWS documents the default as false, and it is reported rather than omitted")
}

// TestEC2_InstanceAttribute_UnknownAttribute covers the strongest message provenance
// in this change: the wording is captured from real AWS in aws/aws-cli#4273 and is
// byte-identical to moto's string.
//
// enaSupport is the row that matters most. It is in DescribeInstanceAttribute's own
// Valid Values list, and the same reference says "Note that the enaSupport attribute
// is not supported." — #4273 captures real AWS rejecting exactly it. Substrate
// refusing a value AWS's own docs list is fidelity, not a gap, and a reader who
// checks only the valid-values list would "fix" this into a regression.
//
// blockDeviceMapping used to be a row here and deliberately is not one now: #669 gave
// substrate the state to answer it, so it moved to the supported list rather than
// staying refused. The remaining five still cover all four refusal classes — listed and
// rejected by AWS, not an attribute at all, listed and unmodelled, and right name in the
// wrong case — so the list shrank without losing a class.
// See TestEC2_InstanceBlockDeviceMapping_Attribute for the answer it gives instead.
func TestEC2_InstanceAttribute_UnknownAttribute(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	for _, attr := range []string{
		"enaSupport", // listed by AWS, rejected by AWS
		"abc",        // not an attribute at all
		"kernel",     // listed, and unmodelled by substrate
		"sourceDestCheck",
		"InstanceType", // valid name in the wrong case — the values are case-sensitive
	} {
		t.Run(attr, func(t *testing.T) {
			status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
				"Action": "DescribeInstanceAttribute", "InstanceId": id, "Attribute": attr,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t,
				"Value ("+attr+") for parameter attribute is invalid. Unknown attribute.",
				msg, "the message interpolates the offending value, as the capture does")
		})
	}
}

// TestEC2_InstanceAttribute_AttributeCheckedBeforeInstance pins the ordering: a
// request that is wrong in both ways reports the attribute.
//
// The reasoning is the same one #472's send path uses. A not-found instance ID can be
// a legitimately not-yet-propagated one — AWS's own InvalidInstanceID.NotFound
// description says so — and is worth retrying; an unsupported attribute name is a
// defect in the request that no retry fixes.
func TestEC2_InstanceAttribute_AttributeCheckedBeforeInstance(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "DescribeInstanceAttribute",
		// Well-formed, so it reaches the not-found branch rather than the malformed one.
		"InstanceId": "i-0123456789abcdef0",
		"Attribute":  "enaSupport",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code,
		"the unsupported attribute is reported, not the missing instance")
}

// TestEC2_InstanceAttribute_RequiredParameters covers the two parameters the
// reference marks Required: Yes, and the instance-ID errors.
//
// Attribute being required is the difference from ModifyInstanceAttribute, where it
// is Required: No because the .Value parameter suffix carries the selection. Getting
// that backwards would make an attribute-less describe return some default.
func TestEC2_InstanceAttribute_RequiredParameters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	tests := []struct {
		name   string
		params map[string]string
		code   string
		msg    string
	}{
		{
			name:   "absent Attribute",
			params: map[string]string{"InstanceId": id},
			code:   "MissingParameter",
			msg:    "The request must contain the parameter Attribute",
		},
		{
			name:   "absent InstanceId",
			params: map[string]string{"Attribute": "userData"},
			code:   "MissingParameter",
			msg:    "The request must contain the parameter InstanceId",
		},
		{
			name:   "unknown instance",
			params: map[string]string{"InstanceId": "i-0123456789abcdef0", "Attribute": "userData"},
			code:   "InvalidInstanceID.NotFound",
			msg:    "The instance ID 'i-0123456789abcdef0' does not exist",
		},
		{
			name:   "malformed instance ID",
			params: map[string]string{"InstanceId": "not-an-id", "Attribute": "userData"},
			code:   "InvalidInstanceID.Malformed",
			msg:    `Invalid id: "not-an-id"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{"Action": "DescribeInstanceAttribute"}
			for k, v := range tt.params {
				params[k] = v
			}
			status, code, msg := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.code, code)
			assert.Equal(t, tt.msg, msg)
		})
	}
}

// TestEC2_InstanceAttribute_ModifyUserData covers ModifyInstanceAttribute for
// userData on a stopped instance, which is how a consumer changes user data.
func TestEC2_InstanceAttribute_ModifyUserData(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	first := base64.StdEncoding.EncodeToString([]byte("first"))
	second := base64.StdEncoding.EncodeToString([]byte("second"))
	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro", "UserData": first,
	})
	stopInstance(t, ts, id)

	status := modifyInstanceAttribute(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": id, "UserData.Value": second,
	})
	require.Equal(t, http.StatusOK, status)

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData)
	assert.Equal(t, second, got.UserData.Value, "the modified value reads back")
}

// TestEC2_InstanceAttribute_ModifyUserDataCanClear pins that an empty UserData.Value
// clears the value rather than being read as "unchanged".
//
// AWS's SecureBlobAttributeValue carries whatever the caller sends, including
// nothing, so a presence check is required where the pre-existing InstanceType.Value
// read uses a non-empty one. Treating "" as unchanged would make a deliberate clear a
// silent no-op — the same silent-success shape this tracker exists to close.
func TestEC2_InstanceAttribute_ModifyUserDataCanClear(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"UserData": base64.StdEncoding.EncodeToString([]byte("to-be-cleared")),
	})
	stopInstance(t, ts, id)

	status := modifyInstanceAttribute(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": id, "UserData.Value": "",
	})
	require.Equal(t, http.StatusOK, status)

	got := describeInstanceAttribute(t, ts, id, "userData")
	require.NotNil(t, got.UserData, "a cleared attribute is still a present element")
	assert.Empty(t, got.UserData.Value, "the value is gone, not left as it was")
}

// TestEC2_InstanceAttribute_ModifyRequiresStopped is the behavior change, asserted
// deliberately: substrate used to change the user data or the type of a *running*
// instance, which real EC2 refuses.
//
// The value is re-read after each rejection, because a 400 that had already written
// state would be worse than no check at all — the caller would see an error and the
// change would have happened anyway.
func TestEC2_InstanceAttribute_ModifyRequiresStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		param string
		value string
		attr  string
		read  string
	}{
		{
			name: "userData", param: "UserData.Value", attr: "userData",
			value: base64.StdEncoding.EncodeToString([]byte("new")), read: "userData",
		},
		{
			name: "instanceType", param: "InstanceType.Value", attr: "instanceType",
			value: "t3.2xlarge", read: "instanceType",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)

			original := base64.StdEncoding.EncodeToString([]byte("original"))
			id := runInstance(t, ts, map[string]string{
				"ImageId": "ami-1", "InstanceType": "t3.micro", "UserData": original,
			})

			status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
				"Action": "ModifyInstanceAttribute", "InstanceId": id, tt.param: tt.value,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "IncorrectInstanceState", code)
			assert.Contains(t, msg, tt.attr, "the message names the attribute")
			assert.Contains(t, msg, "running", "and the state that blocked it")

			// Nothing changed.
			got := describeInstanceAttribute(t, ts, id, tt.read)
			switch tt.read {
			case "userData":
				require.NotNil(t, got.UserData)
				assert.Equal(t, original, got.UserData.Value, "the refused change did not land")
			case "instanceType":
				require.NotNil(t, got.InstanceType)
				assert.Equal(t, "t3.micro", got.InstanceType.Value, "the refused change did not land")
			}

			// And the same call succeeds once the instance is stopped, which is what
			// makes this a state gate rather than a blanket rejection.
			stopInstance(t, ts, id)
			assert.Equal(t, http.StatusOK, modifyInstanceAttribute(t, ts, map[string]string{
				"Action": "ModifyInstanceAttribute", "InstanceId": id, tt.param: tt.value,
			}))
		})
	}
}

// TestEC2_InstanceAttribute_TerminationProtectionNotGated pins that
// disableApiTermination is *not* subject to the stopped-state gate.
//
// RunInstances' reference is explicit: "You can enable termination protection when
// you launch an instance, while the instance is running, or while the instance is
// stopped." Gating it would refuse a call real EC2 accepts, which is the same class
// of defect as accepting one real EC2 refuses — just in the other direction, and
// harder to notice because it looks like extra rigor.
func TestEC2_InstanceAttribute_TerminationProtectionNotGated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	// Still running.
	status := modifyInstanceAttribute(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": id,
		"DisableApiTermination.Value": "true",
	})
	require.Equal(t, http.StatusOK, status,
		"termination protection is modifiable on a running instance")

	got := describeInstanceAttribute(t, ts, id, "disableApiTermination")
	require.NotNil(t, got.DisableAPI)
	assert.Equal(t, "true", got.DisableAPI.Value)

	// And it can be turned back off.
	require.Equal(t, http.StatusOK, modifyInstanceAttribute(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": id,
		"DisableApiTermination.Value": "false",
	}))
	off := describeInstanceAttribute(t, ts, id, "disableApiTermination")
	require.NotNil(t, off.DisableAPI)
	assert.Equal(t, "false", off.DisableAPI.Value)
}

// TestEC2_InstanceAttribute_ModifyUnknownInstance covers the errors on the modify
// path, which previously reported substrate-shaped strings ("Instance not found:
// i-abc", "InstanceId is required") and now share the reference's wording with every
// other operation that names an instance ID (#391).
func TestEC2_InstanceAttribute_ModifyUnknownInstance(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": "i-0123456789abcdef0",
		"InstanceType.Value": "t3.micro",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidInstanceID.NotFound", code)
	assert.Equal(t, "The instance ID 'i-0123456789abcdef0' does not exist", msg)

	status, code, msg = ec2ErrorDetail(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceType.Value": "t3.micro",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "MissingParameter", code)
	assert.Equal(t, "The request must contain the parameter InstanceId", msg)
}

// TestEC2_InstanceAttribute_ResponseWrapsTheValue asserts the wire shape directly
// rather than through a decoder, because the decoder is exactly what hides the bug:
// a response emitting <instanceType>t3.micro</instanceType> unwrapped would decode
// into a struct whose Value field is empty, and every value assertion above would
// still fail for a reason that reads like a lookup problem.
//
// All three of the reference's worked examples show the wrapper.
func TestEC2_InstanceAttribute_ResponseWrapsTheValue(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	resp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstanceAttribute", "InstanceId": id, "Attribute": "instanceType",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	wire := string(body)

	assert.Contains(t, wire, "<instanceType><value>t3.micro</value></instanceType>",
		"the value is wrapped in <value>, per all three worked examples")
	assert.Contains(t, wire, `xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"`)
	assert.True(t, strings.Contains(wire, "<instanceId>"+id+"</instanceId>"),
		"the response names the instance alongside the attribute")

	// No other attribute's element rides along. groupSet is the one that can:
	// encoding/xml emits the parent element of a `groupSet>item` path even for an
	// empty slice, so the naive field declaration puts <groupSet></groupSet> in every
	// response — telling a caller who asked about instanceType that the instance has
	// no security groups. A struct decoder cannot see this; only the wire can.
	assert.NotContains(t, wire, "groupSet",
		"an unasked-for attribute contributes no element at all")
	assert.NotContains(t, wire, "userData")
}

// TestEC2_InstanceAttribute_EmptyGroupSetIsStillPresent covers the other side of that
// pointer: when groupSet *is* the attribute asked for and the instance has no groups,
// the element is present and empty rather than omitted — the same shape rule as an
// unset userData, since an SDK distinguishes an empty array from a nil one.
//
// The instance is written into state directly because a launch always resolves a
// security group: substrate falls back to the default VPC's, so there is no request
// that produces a group-less instance. The state is the only place the case exists.
func TestEC2_InstanceAttribute_EmptyGroupSetIsStillPresent(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)

	const instID = "i-00000000000000001"
	inst := map[string]any{
		"instance_id":   instID,
		"instance_type": "t3.micro",
		"image_id":      "ami-1",
		"state":         map[string]any{"code": 16, "name": "running"},
		"account_id":    "000000000000",
		"region":        "us-east-1",
	}
	raw, err := json.Marshal(inst)
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "ec2",
		"instance:000000000000/us-east-1/"+instID, raw))

	resp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstanceAttribute", "InstanceId": instID, "Attribute": "groupSet",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), "<groupSet></groupSet>",
		"groupSet is present and empty when it is what was asked for")
}

// createSubnetInAZ creates a subnet in a named Availability Zone. The existing
// createSubnet helper lets CreateSubnet pick the zone, which is not enough for a
// test whose subject is the zone grouping.
func createSubnetInAZ(t *testing.T, ts *httptest.Server, vpcID, cidr, az string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID,
		"CidrBlock": cidr, "AvailabilityZone": az,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Subnet struct {
			SubnetID         string `xml:"subnetId"`
			AvailabilityZone string `xml:"availabilityZone"`
		} `xml:"subnet"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, az, out.Subnet.AvailabilityZone, "the subnet must hold the zone asked for")
	return out.Subnet.SubnetID
}

// terminateInstances sends TerminateInstances for the given IDs and returns the
// status and body, so a test can assert both the refusal and what it says.
func terminateInstances(t *testing.T, ts *httptest.Server, ids ...string) (int, string) {
	t.Helper()
	params := map[string]string{"Action": "TerminateInstances"}
	for i, id := range ids {
		params[fmt.Sprintf("InstanceId.%d", i+1)] = id
	}
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

// instanceState reads an instance's current state name through DescribeInstances,
// which is how a consumer checks whether their teardown destroyed something.
func instanceState(t *testing.T, ts *httptest.Server, instID string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": instID,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			State      struct {
				Name string `xml:"name"`
			} `xml:"instanceState"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Instances, 1, "DescribeInstances must find %s", instID)
	return out.Instances[0].State.Name
}

// ec2ErrorCodeOf extracts the code from an already-read EC2 error document. It is
// the sibling of ec2ErrorCode, which sends the request itself — TerminateInstances
// tests need the body as well as the code, so the two steps are separate here.
//
// The XPath is the SDK's, with the "ec2" protocol's plural <Errors> wrapper; see
// ec2ErrorCode for why it is not the Query protocol's Error>Code (#591).
func ec2ErrorCodeOf(t *testing.T, body string) string {
	t.Helper()
	var out struct {
		Code string `xml:"Errors>Error>Code"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out), "body was %s", body)
	return out.Code
}

// TestEC2_TerminateInstances_ProtectionRefused is #489's gate. Before this,
// termination protection was recorded and reported but not acted on, so a test
// asserting "my teardown does not destroy the protected instance" passed against a
// code path that ignored protection entirely — the assertion could not fail.
func TestEC2_TerminateInstances_ProtectionRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"DisableApiTermination": "true",
	})

	code, body := terminateInstances(t, ts, id)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "OperationNotPermitted", ec2ErrorCodeOf(t, body))
	assert.Contains(t, body, id, "the refusal names the instance a caller must act on")
	assert.Contains(t, body, "disableApiTermination",
		"and names the attribute to clear")

	assert.Equal(t, "running", instanceState(t, ts, id),
		"a refused terminate leaves the instance running")
}

// TestEC2_TerminateInstances_ProtectionClearedThenTerminates is the sequence a
// consumer's teardown actually performs: clear protection, then terminate.
func TestEC2_TerminateInstances_ProtectionClearedThenTerminates(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"DisableApiTermination": "true",
	})
	require.Equal(t, http.StatusOK, modifyInstanceAttribute(t, ts, map[string]string{
		"Action": "ModifyInstanceAttribute", "InstanceId": id,
		"DisableApiTermination.Value": "false",
	}))

	code, body := terminateInstances(t, ts, id)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Equal(t, "terminated", instanceState(t, ts, id))
}

// TestEC2_TerminateInstances_UnprotectedUnaffected guards the ordinary path: the
// protection check must not refuse a request naming no protected instance.
func TestEC2_TerminateInstances_UnprotectedUnaffected(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	a := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})
	b := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	code, body := terminateInstances(t, ts, a, b)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Equal(t, "terminated", instanceState(t, ts, a))
	assert.Equal(t, "terminated", instanceState(t, ts, b))
}

// TestEC2_TerminateInstances_ProtectionIsZoneScoped is the reference's own
// four-instance worked example, and the assertion neither of #489's two guesses
// would satisfy. Quoting TerminateInstances:
//
//	The specified instances that are in the same Availability Zone as the
//	protected instance are not terminated. The specified instances that are in
//	different Availability Zones, where no other specified instances are
//	protected, are successfully terminated.
//
// So the request reports failure while A and B — unprotected and in a zone with no
// protected instance — are terminated anyway. Refusing the whole request fails this
// test on A and B; terminating per-instance fails it on D.
func TestEC2_TerminateInstances_ProtectionIsZoneScoped(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	launch := func(az string, protected bool) string {
		params := map[string]string{
			"ImageId": "ami-1", "InstanceType": "t3.micro",
			"Placement.AvailabilityZone": az,
		}
		if protected {
			params["DisableApiTermination"] = "true"
		}
		return runInstance(t, ts, params)
	}
	a := launch("us-east-1a", false)
	b := launch("us-east-1a", false)
	c := launch("us-east-1b", true)
	d := launch("us-east-1b", false)

	code, body := terminateInstances(t, ts, a, b, c, d)
	assert.Equal(t, http.StatusBadRequest, code, "the request reports failure")
	assert.Equal(t, "OperationNotPermitted", ec2ErrorCodeOf(t, body))
	assert.Contains(t, body, c, "the error names the protected instance, not a sibling")

	assert.Equal(t, "terminated", instanceState(t, ts, a),
		"A is in a zone with no protected instance, so it terminates")
	assert.Equal(t, "terminated", instanceState(t, ts, b), "and so does B")
	assert.Equal(t, "running", instanceState(t, ts, c), "C is protected")
	assert.Equal(t, "running", instanceState(t, ts, d),
		"D shares C's zone, so it survives despite being unprotected")
}

// TestEC2_TerminateInstances_ProtectedAloneInItsZone is the simpler half of the
// same rule, kept separate because it is the case a per-instance implementation
// also passes: with one instance per zone, zone-scoped and per-instance agree.
func TestEC2_TerminateInstances_ProtectedAloneInItsZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	protected := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"Placement.AvailabilityZone": "us-east-1a",
		"DisableApiTermination":      "true",
	})
	other := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"Placement.AvailabilityZone": "us-east-1b",
	})

	code, _ := terminateInstances(t, ts, protected, other)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "running", instanceState(t, ts, protected))
	assert.Equal(t, "terminated", instanceState(t, ts, other))
}

// TestEC2_TerminateInstances_BadIDTerminatesNothing pins that the pre-flight order
// did not regress: an unresolvable ID fails the whole request before any state is
// written. The reference states it directly — "If you specify multiple instances and
// the request fails (for example, because of a single incorrect instance ID), none of
// the instances are terminated" — and the protection scan runs after that resolution
// pass, so neither check can write partial state.
func TestEC2_TerminateInstances_BadIDTerminatesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	good := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})

	code, _ := terminateInstances(t, ts, good, "i-0deadbeefdeadbeef")
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "running", instanceState(t, ts, good),
		"a request that fails on one ID terminates none of the instances")
}

// TestEC2_TerminateInstances_Idempotent guards the reference's statement that the
// operation is idempotent: terminating an already-terminated instance succeeds. The
// protection check must not turn a repeat call into a refusal.
func TestEC2_TerminateInstances_Idempotent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{"ImageId": "ami-1", "InstanceType": "t3.micro"})
	code, _ := terminateInstances(t, ts, id)
	require.Equal(t, http.StatusOK, code)
	code, body := terminateInstances(t, ts, id)
	assert.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Equal(t, "terminated", instanceState(t, ts, id))
}

// TestEC2_TerminateInstances_ZoneFromSubnet asserts the grouping works for a launch
// that named no Placement.AvailabilityZone, which is the common case: the zone comes
// from the subnet, so two subnets in different zones are two groups.
//
// Without this, protection would look zone-scoped only for callers who pass
// Placement.AvailabilityZone explicitly, and collapse to whole-request refusal for
// everyone else — the wrong behavior, reached by the right code.
func TestEC2_TerminateInstances_ZoneFromSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	vpcID := createVPC(t, ts, "10.0.0.0/16")
	subnetA := createSubnetInAZ(t, ts, vpcID, "10.0.1.0/24", "us-east-1a")
	subnetB := createSubnetInAZ(t, ts, vpcID, "10.0.2.0/24", "us-east-1b")

	protected := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"SubnetId": subnetA, "DisableApiTermination": "true",
	})
	sameZone := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro", "SubnetId": subnetA,
	})
	otherZone := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro", "SubnetId": subnetB,
	})

	code, _ := terminateInstances(t, ts, protected, sameZone, otherZone)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "running", instanceState(t, ts, protected))
	assert.Equal(t, "running", instanceState(t, ts, sameZone),
		"the subnet supplied the zone, so this instance shares the protected one's")
	assert.Equal(t, "terminated", instanceState(t, ts, otherZone))
}

// TestEC2_Instance_ReportsAvailabilityZone asserts the zone is observable, which is
// what makes the rule above assertable by a consumer rather than only internally
// consistent. A caller reasoning about which of their instances a terminate would
// spare has to be able to read the grouping key.
func TestEC2_Instance_ReportsAvailabilityZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// RunInstances reports it directly, so no follow-up describe is needed.
	resp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "MinCount": "1", "MaxCount": "1",
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"Placement.AvailabilityZone": "us-west-2c",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	launched, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	assert.Contains(t, string(launched), "<availabilityZone>us-west-2c</availabilityZone>",
		"RunInstances reports the placement it applied")

	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(launched, &out))
	require.Len(t, out.Instances, 1)
	id := out.Instances[0].InstanceID

	described := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": id,
	})
	require.Equal(t, http.StatusOK, described.StatusCode)
	body, err := io.ReadAll(described.Body)
	require.NoError(t, described.Body.Close())
	require.NoError(t, err)
	assert.Contains(t, string(body), "<availabilityZone>us-west-2c</availabilityZone>",
		"and DescribeInstances reports it too")
}

// TestEC2_DescribeInstances_AvailabilityZoneFilter covers the filter that lets a
// caller enumerate a zone's instances, which is how they would work out what a
// terminate is about to spare.
func TestEC2_DescribeInstances_AvailabilityZoneFilter(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	inA := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"Placement.AvailabilityZone": "eu-west-1a",
	})
	inB := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"Placement.AvailabilityZone": "eu-west-1b",
	})

	resp := ec2Request(t, ts, map[string]string{
		"Action":        "DescribeInstances",
		"Filter.1.Name": "availability-zone", "Filter.1.Value.1": "eu-west-1a",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), inA)
	assert.NotContains(t, string(body), inB)
}

// TestEC2_Instance_AlwaysCarriesAZone pins the invariant the zone grouping depends
// on: every instance has a zone, whatever the launch named. An instance with an empty
// zone would group with every other zoneless instance, so a whole-request refusal
// would masquerade as the zone-scoped rule.
//
// The path that reaches the region-default fallback is a launch naming a SubnetId that
// resolves to nothing — RunInstances does not validate the ID, which is a pre-existing
// looseness this test relies on rather than endorses. An absent SubnetId cannot reach
// it, because the auto-created default subnet already supplies a zone.
func TestEC2_Instance_AlwaysCarriesAZone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	id := runInstance(t, ts, map[string]string{
		"ImageId": "ami-1", "InstanceType": "t3.micro",
		"SubnetId": "subnet-0000000000000dead",
	})

	resp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": id,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Instances []struct {
			AZ string `xml:"placement>availabilityZone"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Instances, 1)
	assert.Equal(t, "us-east-1a", out.Instances[0].AZ,
		"a launch resolving no subnet still lands in the region's first zone")
}
