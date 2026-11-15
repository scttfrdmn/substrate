package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// ec2CheckLaunchArchitecture refuses a launch whose AMI architecture the instance type cannot
// run (#1505), or returns nil.
//
// Both halves exist since #1497: the catalog reports processorInfo.supportedArchitectures per
// family, and every AMI reports architecture. Until this check a launch of an arm64 AMI on an
// x86_64 type (or the reverse) succeeded here and failed on AWS.
//
// The code and message are observed, not published — neither is on the API reference, and the
// error table's nearest entry (UnsupportedOperation, "you might be trying to launch an instance
// type that is not supported by the specified AMI") is not what real EC2 answers. Real
// RunInstances refusals quoted verbatim, each with HTTP 400 and InvalidParameterValue:
//
//   - chgasparoto/curso-aws-com-terraform discussion #43 (Terraform, status code 400): "The
//     architecture 'x86_64' of the specified instance type does not match the architecture
//     'arm64' of the specified AMI. Specify an instance type and an AMI that have matching
//     architectures, and try again. You can use 'describe-instance-types' or 'describe-images'
//     to discover the architecture of the instance type or AMI."
//   - Stack Overflow 75975304 (Terraform): the same sentence for t2.micro, whose type side reads
//     'i386,x86_64' — the type's supportedArchitectures joined with a comma, which is how the
//     type half is rendered here.
//   - rancher/rancher#38176 (a Spot request): the arm64-type / x86_64-AMI direction.
//
// The Auto Scaling user guide's AMI troubleshooting page quotes the same sentence inside a
// group's "Launching EC2 instance failed." activity, which is how a group's launch reports it.
//
// What is **not** refused, so the check cannot fail a launch that has no basis for failing:
//
//   - an instance type the catalog does not carry — RunInstances validates no type today, and
//     refusing here would be that wider change under another name;
//   - an AMI whose architecture is empty, i386 (RegisterImage's published default, reported by
//     an AMI registered without Architecture), or one of the mac values — none of which any
//     catalog type reports, so refusing would refuse every launch of such an AMI. AWS would
//     refuse an i386 AMI on most current types, but substrate cannot tell an AMI that really is
//     i386 from one whose architecture was simply never stated.
func ec2CheckLaunchArchitecture(instanceType, imageArch string) *AWSError {
	if imageArch != ec2ArchX86 && imageArch != ec2ArchARM64 {
		return nil
	}
	info, ok := ec2InstanceTypeIndex[instanceType]
	if !ok || len(info.SupportedArchs) == 0 || containsStr(info.SupportedArchs, imageArch) {
		return nil
	}
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf("The architecture '%s' of the specified instance type does not match the "+
			"architecture '%s' of the specified AMI. Specify an instance type and an AMI that have matching "+
			"architectures, and try again. You can use 'describe-instance-types' or 'describe-images' to "+
			"discover the architecture of the instance type or AMI.",
			strings.Join(info.SupportedArchs, ","), imageArch),
		HTTPStatus: http.StatusBadRequest,
	}
}
