package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Instance metadata options (#1471).
//
// MetadataOptions is how a launch template or a launch requires IMDSv2 and bounds the PUT hop
// limit, and a consumer's security tests assert both. Substrate parsed neither until #1471:
// CreateLaunchTemplate stored nothing, DescribeLaunchTemplateVersions returned nothing, and an
// instance reported no metadataOptions at all. This file is the one place the five members are
// parsed, validated, merged and rendered, so a template, a launch and a modification cannot
// disagree about them.
//
// The shapes are LaunchTemplateInstanceMetadataOptionsRequest (CreateLaunchTemplate and
// CreateLaunchTemplateVersion), LaunchTemplateInstanceMetadataOptions (their read-back),
// InstanceMetadataOptionsRequest (RunInstances), InstanceMetadataOptionsResponse
// (DescribeInstances and ModifyInstanceMetadataOptions) and ModifyInstanceMetadataOptions'
// own top-level parameters. All five members carry the same names and value sets in each.
//
// What is in scope is the configuration and its observation. What it does inside the instance —
// whether a request without a token is refused, how far a PUT response travels — is
// resource-internal, and substrate does not model it.

// EC2InstanceMetadataOptions is the five instance metadata options, as a launch template, a
// launch or a modification states them.
//
// Every member is optional and kept as stated, with its zero value meaning "not specified":
// AWS evaluates each option separately — "Each metadata option is evaluated separately" (the
// user guide's order of precedence) — so a request naming one option says nothing about the
// other four, and a template read back must not report options it was never given.
type EC2InstanceMetadataOptions struct {
	// HTTPTokens is HttpTokens: optional or required.
	HTTPTokens string `json:"httpTokens,omitempty"`

	// HTTPPutResponseHopLimit is HttpPutResponseHopLimit, 1 to 64; 0 is not specified.
	HTTPPutResponseHopLimit int `json:"httpPutResponseHopLimit,omitempty"`

	// HTTPEndpoint is HttpEndpoint: enabled or disabled.
	HTTPEndpoint string `json:"httpEndpoint,omitempty"`

	// HTTPProtocolIPv6 is HttpProtocolIpv6: enabled or disabled.
	HTTPProtocolIPv6 string `json:"httpProtocolIpv6,omitempty"`

	// InstanceMetadataTags is InstanceMetadataTags: enabled or disabled.
	InstanceMetadataTags string `json:"instanceMetadataTags,omitempty"`
}

// empty reports whether no option is specified.
func (o EC2InstanceMetadataOptions) empty() bool {
	return o == EC2InstanceMetadataOptions{}
}

// overlay returns o with every option over specifies replaced, which is the per-option
// precedence the user guide gives a launch over its template and a modification over the
// instance's current options.
func (o EC2InstanceMetadataOptions) overlay(over EC2InstanceMetadataOptions) EC2InstanceMetadataOptions {
	out := o
	if over.HTTPTokens != "" {
		out.HTTPTokens = over.HTTPTokens
	}
	if over.HTTPPutResponseHopLimit != 0 {
		out.HTTPPutResponseHopLimit = over.HTTPPutResponseHopLimit
	}
	if over.HTTPEndpoint != "" {
		out.HTTPEndpoint = over.HTTPEndpoint
	}
	if over.HTTPProtocolIPv6 != "" {
		out.HTTPProtocolIPv6 = over.HTTPProtocolIPv6
	}
	if over.InstanceMetadataTags != "" {
		out.InstanceMetadataTags = over.InstanceMetadataTags
	}
	return out
}

// ec2MetadataOptionValues returns a string option's published value set. Every shape listed
// in the file comment publishes the same ones: "Valid Values: optional | required" for
// HttpTokens and "Valid Values: disabled | enabled" for the other three strings.
func ec2MetadataOptionValues(member string) []string {
	if member == "HttpTokens" {
		return []string{"optional", "required"}
	}
	return []string{"disabled", "enabled"}
}

// ec2MetadataOptionsHopLimitMax is HttpPutResponseHopLimit's upper bound: "Possible values:
// Integers from 1 to 64".
const ec2MetadataOptionsHopLimitMax = 64

// ec2ParseMetadataOptions reads the five options from the parameters under prefix —
// "LaunchTemplateData.MetadataOptions." for a template, "MetadataOptions." for RunInstances,
// "" for ModifyInstanceMetadataOptions, whose options are top-level — and validates each one
// present against its published values.
//
// A member sent empty is treated as absent rather than refused, which is how every other
// optional EC2 parameter here reads an empty value. A refusal is [ec2InvalidParameterValue],
// naming the parameter as the request spelled it.
func ec2ParseMetadataOptions(params map[string]string, prefix string) (EC2InstanceMetadataOptions, *AWSError) {
	var out EC2InstanceMetadataOptions
	for _, m := range []struct {
		name string
		dst  *string
	}{
		{"HttpTokens", &out.HTTPTokens},
		{"HttpEndpoint", &out.HTTPEndpoint},
		{"HttpProtocolIpv6", &out.HTTPProtocolIPv6},
		{"InstanceMetadataTags", &out.InstanceMetadataTags},
	} {
		v := params[prefix+m.name]
		if v == "" {
			continue
		}
		if valid := ec2MetadataOptionValues(m.name); !containsStr(valid, v) {
			return EC2InstanceMetadataOptions{}, ec2InvalidParameterValue(prefix+m.name, v,
				"Valid values: "+strings.Join(valid, ", ")+".")
		}
		*m.dst = v
	}
	if raw := params[prefix+"HttpPutResponseHopLimit"]; raw != "" {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 1 || n > ec2MetadataOptionsHopLimitMax {
			return EC2InstanceMetadataOptions{}, ec2InvalidParameterValue(prefix+"HttpPutResponseHopLimit", raw,
				fmt.Sprintf("Possible values: integers from 1 to %d.", ec2MetadataOptionsHopLimitMax))
		}
		out.HTTPPutResponseHopLimit = n
	}
	return out, nil
}

// ec2LTMetadataOptionsXML is LaunchTemplateInstanceMetadataOptions, a template version's
// read-back. Every member is omitted when the template did not state it, which is the whole of
// #1471's second half: a version reports what it carries and nothing else.
//
// LaunchTemplateInstanceMetadataOptions also publishes a `state` member (pending | applied). It
// is not rendered: a template is not an instance, nothing is ever applied to one, and the member
// a caller sent never carries it.
type ec2LTMetadataOptionsXML struct {
	HTTPEndpoint            string `xml:"httpEndpoint,omitempty"`
	HTTPProtocolIPv6        string `xml:"httpProtocolIpv6,omitempty"`
	HTTPPutResponseHopLimit int    `xml:"httpPutResponseHopLimit,omitempty"`
	HTTPTokens              string `xml:"httpTokens,omitempty"`
	InstanceMetadataTags    string `xml:"instanceMetadataTags,omitempty"`
}

// ec2LTMetadataOptions renders a template's stored options, or nil when it stated none.
func ec2LTMetadataOptions(o *EC2InstanceMetadataOptions) *ec2LTMetadataOptionsXML {
	if o == nil || o.empty() {
		return nil
	}
	return &ec2LTMetadataOptionsXML{
		HTTPEndpoint:            o.HTTPEndpoint,
		HTTPProtocolIPv6:        o.HTTPProtocolIPv6,
		HTTPPutResponseHopLimit: o.HTTPPutResponseHopLimit,
		HTTPTokens:              o.HTTPTokens,
		InstanceMetadataTags:    o.InstanceMetadataTags,
	}
}

// ec2InstanceMetadataOptionsXML is InstanceMetadataOptionsResponse, an instance's
// metadataOptions in DescribeInstances and RunInstances and the instanceMetadataOptions member
// of ModifyInstanceMetadataOptions.
type ec2InstanceMetadataOptionsXML struct {
	State                   string `xml:"state"`
	HTTPTokens              string `xml:"httpTokens,omitempty"`
	HTTPPutResponseHopLimit int    `xml:"httpPutResponseHopLimit,omitempty"`
	HTTPEndpoint            string `xml:"httpEndpoint"`
	HTTPProtocolIPv6        string `xml:"httpProtocolIpv6"`
	InstanceMetadataTags    string `xml:"instanceMetadataTags"`
}

// ec2InstanceMetadataOptionsFor renders an instance's effective options with the state given.
//
// Three of the five have defaults no AMI or account setting substrate models can move —
// InstanceMetadataOptionsRequest publishes "Default: enabled" for HttpEndpoint and "Default:
// disabled" for HttpProtocolIpv6 and InstanceMetadataTags — so they are always rendered, with
// the default when the launch named none. The other two are left out when unspecified, because
// their defaults come from the AMI: the user guide's order of precedence applies "AMI
// configuration … only to HttpTokens and HttpPutResponseHopLimit", resolving HttpTokens to
// required for an AMI whose ImdsSupport is v2.0 and optional otherwise. Substrate models no
// ImdsSupport, so either value would be a guess about the image, and an omitted member is the
// honest answer a caller cannot mistake for a real one.
func ec2InstanceMetadataOptionsFor(o *EC2InstanceMetadataOptions, state string) *ec2InstanceMetadataOptionsXML {
	var opts EC2InstanceMetadataOptions
	if o != nil {
		opts = *o
	}
	out := &ec2InstanceMetadataOptionsXML{
		State:                   state,
		HTTPTokens:              opts.HTTPTokens,
		HTTPPutResponseHopLimit: opts.HTTPPutResponseHopLimit,
		HTTPEndpoint:            opts.HTTPEndpoint,
		HTTPProtocolIPv6:        opts.HTTPProtocolIPv6,
		InstanceMetadataTags:    opts.InstanceMetadataTags,
	}
	if out.HTTPEndpoint == "" {
		out.HTTPEndpoint = "enabled"
	}
	if out.HTTPProtocolIPv6 == "" {
		out.HTTPProtocolIPv6 = "disabled"
	}
	if out.InstanceMetadataTags == "" {
		out.InstanceMetadataTags = "disabled"
	}
	return out
}

// ec2MetadataOptionsApplied and ec2MetadataOptionsPending are InstanceMetadataOptionsResponse's
// two state values.
const (
	ec2MetadataOptionsApplied = "applied"
	ec2MetadataOptionsPending = "pending"
)

// modifyInstanceMetadataOptions handles ModifyInstanceMetadataOptions (#1471).
//
// Each option the request names replaces the instance's; the rest are kept, which is the
// reference's "If this parameter is not specified, the existing state is maintained" for
// HttpEndpoint and HttpPutResponseHopLimit and the per-option rule the user guide states for all
// five. The options are validated before the instance is read, so a bad value is refused
// whichever instance it names.
//
// The response reports state pending and every later DescribeInstances reports applied. That is
// the reference's description — "When you modify the parameters on a running instance, the API
// responds with a state of 'pending'. After the parameter modifications are successfully
// applied to the instance, the state of the modifications changes from 'pending' to 'applied'
// in subsequent describe-instances API calls" — with the transition settled by the time of the
// next observation, so no test depends on how long one takes.
//
// A terminated instance is refused with IncorrectInstanceState, the code EC2's
// error table gives an instance "in an incorrect state for the requested action"; the reference
// names a running or stopped instance as what the operation modifies. The message is
// substrate's.
func (p *EC2Plugin) modifyInstanceMetadataOptions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instID := req.Params["InstanceId"]
	if instID == "" {
		return nil, ec2MissingParameter("InstanceId")
	}
	opts, awsErr := ec2ParseMetadataOptions(req.Params, "")
	if awsErr != nil {
		return nil, awsErr
	}
	inst, awsErr, err := p.loadInstance(reqCtx, instID, "modifyInstanceMetadataOptions")
	if err != nil || awsErr != nil {
		return nil, errOrAWS(err, awsErr)
	}
	// The stored record holds the settled state only (see ec2_instance_state_control.go), so
	// terminated is the one state this can see; shutting-down is an observation, never stored.
	if inst.State.Name == ec2StateTerminated {
		return nil, &AWSError{
			Code:       "IncorrectInstanceState",
			Message:    "The instance '" + instID + "' is not in a state from which its metadata options can be modified",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var current EC2InstanceMetadataOptions
	if inst.MetadataOptions != nil {
		current = *inst.MetadataOptions
	}
	merged := current.overlay(opts)
	if merged.empty() {
		inst.MetadataOptions = nil
	} else {
		inst.MetadataOptions = &merged
	}

	updated, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceMetadataOptions marshal: %w", err)
	}
	stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instID
	if err := p.state.Put(context.Background(), ec2Namespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceMetadataOptions put: %w", err)
	}

	type response struct {
		XMLName         xml.Name                       `xml:"ModifyInstanceMetadataOptionsResponse"`
		XMLNS           string                         `xml:"xmlns,attr"`
		InstanceID      string                         `xml:"instanceId"`
		MetadataOptions *ec2InstanceMetadataOptionsXML `xml:"instanceMetadataOptions"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:           "http://ec2.amazonaws.com/doc/2016-11-15/",
		InstanceID:      instID,
		MetadataOptions: ec2InstanceMetadataOptionsFor(inst.MetadataOptions, ec2MetadataOptionsPending),
	})
}
