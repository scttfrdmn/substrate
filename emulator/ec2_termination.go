package emulator

import (
	"fmt"
	"net/http"
)

// ec2TerminationProtectionBlocked groups instances by Availability Zone and
// returns, for each zone holding at least one protected instance, the ID of the
// protected instance that blocks it. A zone absent from the result has no
// protected instance among those named and may be terminated.
//
// The grouping is the whole behavior of a mixed TerminateInstances request, and
// it is neither per-request nor per-instance. Quoting the TerminateInstances
// reference, which is the only place this is stated:
//
//	If you terminate multiple instances across multiple Availability Zones, and
//	one or more of the specified instances are enabled for termination
//	protection, the request fails with the following results:
//	  - The specified instances that are in the same Availability Zone as the
//	    protected instance are not terminated.
//	  - The specified instances that are in different Availability Zones, where
//	    no other specified instances are protected, are successfully terminated.
//
// So an unprotected instance sharing a zone with a protected one survives, while
// an unprotected instance in another zone does not. Refusing the whole request
// and terminating every unprotected instance are both wrong, in opposite
// directions.
//
// When two protected instances share a zone the first in request order is
// reported, so the error names a stable instance across replays.
func ec2TerminationProtectionBlocked(instances []EC2Instance) map[string]string {
	blocked := make(map[string]string)
	for _, inst := range instances {
		if !inst.DisableAPITermination {
			continue
		}
		if _, seen := blocked[inst.AvailabilityZone]; !seen {
			blocked[inst.AvailabilityZone] = inst.InstanceID
		}
	}
	return blocked
}

// ec2TerminationProtectedError reports that instanceID may not be terminated
// because termination protection is enabled.
//
// The code is a doc citation: the EC2 client-error table lists
// OperationNotPermitted as "The specified operation is not allowed", naming this
// case first among its examples — "you might be trying to terminate an instance
// that has termination protection enabled". The message text is substrate's own.
// #489 supplied a remembered console wording, which no capture corroborates, so
// per docs/fidelity.md the text is not dressed up as observed; it interpolates
// the instance ID and names the attribute to clear, which is what a caller acts
// on.
func ec2TerminationProtectedError(instanceID string) *AWSError {
	return &AWSError{
		Code: "OperationNotPermitted",
		Message: fmt.Sprintf(
			"The instance '%s' may not be terminated. "+
				"Modify its 'disableApiTermination' instance attribute and try again.",
			instanceID),
		HTTPStatus: http.StatusBadRequest,
	}
}
