package emulator

import (
	"fmt"
	"net/http"
	"strconv"
)

// ec2DefaultInstanceCount is the instance count RunInstances assumes when neither
// MinCount nor MaxCount is on the wire.
//
// Absence keeps defaulting rather than erroring, deliberately. AWS marks both as
// "Required: Yes", but in every typed SDK they are required members that fail
// client-side — MaxCount == nil is smithy.NewErrParamRequired("MaxCount") in
// aws-sdk-go-v2's validateOpRunInstancesInput, and botocore rejects the missing
// kwarg before serializing — so a consumer bug there cannot reach the wire.
// Requiring presence here would validate an unreachable bug class while breaking
// every hand-built form-encoded request, which substrate's own tests and
// cfn_deployer.go's CFN path are (#431, declined half of #412).
const ec2DefaultInstanceCount = 1

// resolveInstanceCounts returns the MinCount/MaxCount a RunInstances request asks
// for, or the error EC2 raises when a value is present and invalid.
//
// A value that is present but invalid *is* reachable, which is why this validates
// where presence does not: the query protocol carries these as strings, and neither
// SDK checks their range. botocore's ParamValidator accepts MinCount=0 and
// MinCount=3/MaxCount=1 unchanged, and aws-sdk-go-v2 checks only for nil — so both
// reach the wire from ordinary typed code. Substrate previously took
// `strconv.Atoi(...)` with the error discarded and clamped anything <= 0 up to 1, so
// MinCount=0 launched an instance instead of failing: a silently wrong result rather
// than a missing one.
//
// Error codes are the common-error entries, because the RunInstances reference
// documents no action-specific error for these ("For information about the errors
// that are common to all actions, see Common Error Types") and the EC2 error
// reference lists nothing for instance counts either. InvalidParameterValue is
// documented as "A value that you provided for a parameter isn't valid. Check the
// parameter constraints and try again." (HTTP 400) — which is exactly an
// out-of-range or unparseable count, given the documented constraint "Between 1 and
// the quota for the specified instance type for your account for this Region".
//
// min > max uses the same code rather than InvalidParameterCombination, which the
// same reference defines as "Parameters that must not be used together were used
// together. Remove one of the conflicting parameters and try again." MinCount and
// MaxCount are documented as used together, so nothing about that description fits;
// the defect is the *value* of maxCount relative to minCount. The plausible guess
// was InvalidParameterCombination, and #413 is why a plausible guess about an AWS
// error code is not good enough to ship: SDK callers match the literal string.
//
// Messages name the parameters in the lowercase form EC2's value-error messages use
// ("Invalid value 'x' for parameter minCount"), matching the reference's own
// examples for this error family.
func resolveInstanceCounts(params map[string]string) (minCount, maxCount int, err error) {
	minCount, err = ec2InstanceCount(params, "MinCount", "minCount", ec2DefaultInstanceCount)
	if err != nil {
		return 0, 0, err
	}
	// An absent MaxCount defaults to MinCount rather than to 1: a request naming
	// only MinCount=3 asks for three instances, and defaulting to 1 would silently
	// cap it below the minimum the caller demanded.
	maxCount, err = ec2InstanceCount(params, "MaxCount", "maxCount", minCount)
	if err != nil {
		return 0, 0, err
	}

	if minCount > maxCount {
		return 0, 0, &AWSError{
			Code: "InvalidParameterValue",
			Message: fmt.Sprintf(
				"Invalid value '%d' for parameter maxCount. The maxCount must be equal to or greater than the minCount '%d'.",
				maxCount, minCount),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return minCount, maxCount, nil
}

// ec2InstanceCount reads one instance-count parameter, returning def when it is
// absent and an InvalidParameterValue error when it is present but unparseable or
// below 1. wireName is the lowercase spelling used in the error message.
func ec2InstanceCount(params map[string]string, param, wireName string, def int) (int, error) {
	raw, ok := params[param]
	if !ok || raw == "" {
		return def, nil
	}
	n, convErr := strconv.Atoi(raw)
	if convErr != nil {
		return 0, ec2InvalidInstanceCount(raw, wireName, "It must be an integer.")
	}
	if n < 1 {
		// The documented constraint is "Between 1 and the quota ...", so 0 is out of
		// range, not a request for nothing. The upper bound is a per-account,
		// per-instance-type quota substrate does not model, so it is not enforced.
		return 0, ec2InvalidInstanceCount(raw, wireName, "It must be at least 1.")
	}
	return n, nil
}

// ec2InvalidInstanceCount builds the InvalidParameterValue error for a bad
// instance-count value.
func ec2InvalidInstanceCount(value, wireName, reason string) error {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("Invalid value '%s' for parameter %s. %s", value, wireName, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
