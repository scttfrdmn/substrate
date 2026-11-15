package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// An ImageId of resolve:ssm:<parameter> (#1506).
//
// EC2 accepts a Systems Manager parameter in place of an AMI ID, in a launch template and in
// RunInstances itself, and resolves it "to an AMI ID on launch". The formats are
// API_RequestLaunchTemplateData's ImageId member:
//
//	resolve:ssm:public-parameter
//	resolve:ssm:parameter-name | resolve:ssm:parameter-name:version-number | resolve:ssm:parameter-name:label
//	resolve:ssm:parameter-ARN  | resolve:ssm:parameter-ARN:version-number  | resolve:ssm:parameter-ARN:label
//
// and RunInstances' use of the same prefix is documented by the EC2 user guide's "Reference the
// latest AMIs using Systems Manager public parameters" page (`--image-id
// resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64`) and the
// Systems Manager guide's parameter-store-ec2-aliases page (`--image-id resolve:ssm:/golden-ami`).
// Until #1506 substrate stored the string as given and every launch refused it as
// InvalidAMIID.Malformed, because it does not begin "ami-".
//
// The parameter is read the way Auto Scaling reads EC2's state — from the SSM plugin's own state
// keys ([ssmParameterStateKey]) through the shared StateManager — rather than through a pointer
// to the plugin, which no plugin holds (docs/contributing.md, "Calling another plugin"). A name
// the account has not stored falls back to the bundled public parameters
// ([bundledImageForParameter]), which is the same order [SSMPlugin.getParameter] answers in, so
// the AMI a launch resolves and the AMI GetParameter hands out for one name cannot differ.
//
// What substrate's SSM records constrain:
//
//   - **Versions.** SSM here keeps a parameter's current value only, with no history, so a
//     `:version-number` resolves when it names the current version and is refused otherwise —
//     an older version's value is not held, and answering the current one for it would launch
//     an AMI the caller pinned away from.
//   - **Labels.** LabelParameterVersion is a stub that records nothing, so no label names a
//     version and a `:label` selector is refused.
//   - **Data type.** The EC2 guide requires a user parameter of type String and data type
//     aws:ec2:image. Substrate's SSM does not store DataType, so only the type is checked; the
//     resolved value is then held to the launch's own AMI rules, so a value that is not an AMI
//     is still refused, as InvalidAMIID.Malformed or InvalidAMIID.NotFound.
//   - **Sharing.** A parameter ARN resolves only in the caller's own account and Region:
//     substrate models no parameter sharing, so another account's parameter is not reachable.
//
// The refusal for a parameter that does not resolve is InvalidParameterValue — the common error
// for "a value specified in a parameter is not valid" — with **substrate's own message**: no
// capture of real EC2 refusing an unresolvable resolve:ssm: ImageId was found, and the API
// reference publishes none.

// ec2SSMImagePrefix is the literal prefix of an ImageId that names a Systems Manager parameter.
const ec2SSMImagePrefix = "resolve:ssm:"

// ec2IsSSMImageID reports whether an ImageId names a Systems Manager parameter.
func ec2IsSSMImageID(imageID string) bool {
	return strings.HasPrefix(imageID, ec2SSMImagePrefix)
}

// ec2SSMImageReference is a parsed resolve:ssm: ImageId.
type ec2SSMImageReference struct {
	// name is the parameter name, normalized to a leading slash as SSM's PutParameter stores it.
	name string
	// accountID and region are the ARN form's, empty for the name form.
	accountID, region string
	// isARN records that the reference used the ARN form.
	isARN bool
	// selector is the optional version number or label, empty when none was given.
	selector string
}

// ec2ParseSSMImageReference splits a resolve:ssm: ImageId into its parameter and selector.
//
// A parameter name cannot contain a colon — SSM's naming rule allows letters, numbers and
// `. - _ /` — so in the name form everything after the first colon is the selector, and in the
// ARN form everything after the sixth colon is.
//
// A separator with nothing after it — "resolve:ssm:golden-ami:" — is malformed rather than "no
// selector": every published form either omits the colon or follows it with a version or label.
// So is an ARN whose resource is not "parameter/" followed by a name, such as "parameterfoo":
// SSM's parameter ARN is arn:aws:ssm:region:account:parameter/name.
func ec2ParseSSMImageReference(imageID string) (ec2SSMImageReference, bool) {
	rest, ok := strings.CutPrefix(imageID, ec2SSMImagePrefix)
	if !ok || rest == "" {
		return ec2SSMImageReference{}, false
	}
	if strings.HasPrefix(rest, "arn:") {
		parts := strings.SplitN(rest, ":", 7)
		if len(parts) < 6 || parts[2] != "ssm" {
			return ec2SSMImageReference{}, false
		}
		name, isParam := strings.CutPrefix(parts[5], "parameter/")
		if !isParam || name == "" {
			return ec2SSMImageReference{}, false
		}
		ref := ec2SSMImageReference{name: ec2NormalizeSSMName(name), region: parts[3], accountID: parts[4], isARN: true}
		if len(parts) == 7 {
			if parts[6] == "" {
				return ec2SSMImageReference{}, false
			}
			ref.selector = parts[6]
		}
		return ref, true
	}
	name, selector, hasSelector := strings.Cut(rest, ":")
	if name == "" || (hasSelector && selector == "") {
		return ec2SSMImageReference{}, false
	}
	return ec2SSMImageReference{name: ec2NormalizeSSMName(name), selector: selector}, true
}

// ec2NormalizeSSMName gives a parameter name the leading slash SSM's PutParameter and
// GetParameter add here, so "golden-ami" and "/golden-ami" name one parameter.
func ec2NormalizeSSMName(name string) string {
	if !strings.HasPrefix(name, "/") {
		return "/" + name
	}
	return name
}

// ec2ResolveSSMImageID returns the AMI ID an ImageId names: imageID itself when it does not
// begin resolve:ssm:, and otherwise the value of the Systems Manager parameter it names.
//
// It is the one implementation of the resolution. The launch reaches it through
// [EC2Plugin.resolveLaunchImage], which every launch path calls once; Auto Scaling's
// attribute-based selection reaches it the same way, and the authorization decision, which runs
// before the handler and holds only the StateManager, calls it directly. A Spot request left open
// by a seeded shortfall reuses the launch's answer rather than resolving again.
//
// The error is an *AWSError for a reference that does not resolve, and a wrapped Go error when
// SSM's state cannot be read or a stored record cannot be decoded. A store failure is never
// reported as an absent parameter, and never falls back to the bundled public parameters.
func ec2ResolveSSMImageID(goCtx context.Context, state StateManager, reqCtx *RequestContext, imageID string) (string, error) {
	if !ec2IsSSMImageID(imageID) {
		return imageID, nil
	}
	ref, ok := ec2ParseSSMImageReference(imageID)
	if !ok {
		return "", ec2SSMImageUnresolved(imageID, "is not a Systems Manager parameter reference")
	}
	if ref.isARN && (ref.region != reqCtx.Region || (ref.accountID != "" && ref.accountID != reqCtx.AccountID)) {
		return "", ec2SSMImageUnresolved(imageID, "names a parameter in another account or Region, and substrate models no parameter sharing")
	}

	data, err := state.Get(goCtx, ssmNamespace, ssmParameterStateKey(reqCtx.AccountID, reqCtx.Region, ref.name))
	if err != nil {
		return "", fmt.Errorf("ec2 resolve %s: read SSM parameter %s: %w", imageID, ref.name, err)
	}
	if data != nil {
		var param SSMParameter
		if err := json.Unmarshal(data, &param); err != nil {
			return "", fmt.Errorf("ec2 resolve %s: decode SSM parameter %s: %w", imageID, ref.name, err)
		}
		if param.Type != "String" {
			return "", ec2SSMImageUnresolved(imageID, "names a parameter of type "+param.Type+"; an AMI parameter must be of type String")
		}
		if awsErr := ec2CheckSSMSelector(imageID, ref.selector, param.Version); awsErr != nil {
			return "", awsErr
		}
		return param.Value, nil
	}

	// A public parameter AWS publishes; every one of them is at version 1 here, as GetParameter
	// reports it ([resolveManagedParameter]).
	if img, found := bundledImageForParameter(ref.name); found && (!ref.isARN || ref.accountID == "") {
		if awsErr := ec2CheckSSMSelector(imageID, ref.selector, 1); awsErr != nil {
			return "", awsErr
		}
		return img.idIn(reqCtx.Region), nil
	}
	return "", ec2SSMImageUnresolved(imageID, "names a parameter that does not exist")
}

// ec2CheckSSMSelector accepts an empty selector or one naming the parameter's current version,
// and refuses anything else; see the file comment for why a label and an older version cannot
// resolve.
func ec2CheckSSMSelector(imageID, selector string, current int64) *AWSError {
	if selector == "" {
		return nil
	}
	n, err := strconv.ParseInt(selector, 10, 64)
	if err != nil {
		return ec2SSMImageUnresolved(imageID, "names a parameter label, and substrate records no parameter labels")
	}
	if n != current {
		return ec2SSMImageUnresolved(imageID, fmt.Sprintf("names version %d of a parameter at version %d, and substrate keeps no parameter history", n, current))
	}
	return nil
}

// resolveLaunchImage turns the ImageId a launch names into the AMI ID it names and that AMI's
// record: a resolve:ssm: reference is resolved ([ec2ResolveSSMImageID]) and the result looked up
// ([EC2Plugin.resolveImage]). found reports whether the AMI exists; the ID is returned either way,
// so a caller can refuse it by the ID's own shape and name.
//
// It is the single point a launch's ImageId passes through, so every reader past it — the
// existence and architecture checks, the instance record, a Spot request's launch specification
// and product description, and Auto Scaling's architecture narrowing — sees the AMI, never the
// reference.
func (p *EC2Plugin) resolveLaunchImage(reqCtx *RequestContext, imageID string) (string, EC2Image, bool, error) {
	resolved, err := ec2ResolveSSMImageID(context.Background(), p.state, reqCtx, imageID)
	if err != nil {
		return "", EC2Image{}, false, err
	}
	img, found := p.resolveImage(reqCtx, resolved)
	return resolved, img, found, nil
}

// ec2SSMImageUnresolved is the refusal for a resolve:ssm: ImageId that does not resolve to an
// AMI. The code is EC2's common InvalidParameterValue; the message is substrate's own, since no
// published or observed wording exists (see the file comment).
func ec2SSMImageUnresolved(imageID, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("The ImageId %s %s.", imageID, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
