package emulator

// stepfunctions_execution_members.go holds the response members `StartSyncExecution` and
// `DescribeExecution` publish in common, which neither operation reported before #1071.
//
// `StartSyncExecution` answered five of the fourteen members its Response Syntax publishes and
// `DescribeExecution` seven of its twenty-one, and four of the gaps are the same four on both
// pages: `error`, `cause`, `inputDetails` and `outputDetails`. The first two mattered most on
// `StartSyncExecution`, because AWS publishes that operation's failure contract as
//
//	StartSyncExecution will return a 200 OK response, even if your execution fails, because the
//	status code in the API response doesn't reflect function errors.
//
// so the body is the *only* place a failure can be reported — and a consumer testing a failure
// path could see `"status":"FAILED"` and nothing about why. `DescribeExecution` had the same blind
// spot on the operation a consumer actually polls.

// sfnExecutionFailure returns the `error` and `cause` members of a failed execution.
//
// Both are empty for an execution that has not failed, and the caller omits them rather than
// sending empty strings: AWS gives no meaning to an empty `error`, and `output` is published as
// the member that carries a *successful* execution's result ("This field is set only if the
// execution succeeds. If the execution fails, this field is null.").
//
// The fallback exists because [ExecutionState.ErrorDetails] was the one joined string these two
// members used to be recorded in, and a record persisted before #1071 — or replayed from an older
// event log — still holds it. Splitting on the first ": " recovers the pair for every failure
// substrate itself generates, but it is best-effort rather than exact: a `Fail` state's `Error` is
// caller-supplied and may contain ": " of its own, in which case the tail of the caller's error
// code is reported as the head of the cause. That unfixable ambiguity in the old shape is why the
// two members are now recorded separately instead of derived.
func sfnExecutionFailure(exec *ExecutionState) (errorCode, cause string) {
	if exec.ErrorCode != "" || exec.ErrorCause != "" {
		return exec.ErrorCode, exec.ErrorCause
	}
	if exec.ErrorDetails == "" {
		return "", ""
	}
	for i := 0; i+1 < len(exec.ErrorDetails); i++ {
		if exec.ErrorDetails[i] == ':' && exec.ErrorDetails[i+1] == ' ' {
			return exec.ErrorDetails[:i], exec.ErrorDetails[i+2:]
		}
	}
	return exec.ErrorDetails, ""
}

// sfnDataDetails is the `CloudWatchEventsExecutionDataDetails` value both `inputDetails` and
// `outputDetails` carry.
//
// `included` is not a derivation from substrate never truncating a payload — AWS publishes the
// value itself: "Indicates whether input or output was included in the response. Always true for
// API calls." An API caller is the only kind of caller substrate has, so `true` is the page's own
// answer. The type publishes no other member.
func sfnDataDetails() map[string]interface{} {
	return map[string]interface{}{"included": true}
}

// sfnAddExecutionResultMembers adds the four members `StartSyncExecution` and `DescribeExecution`
// publish identically and neither reported, to a response body that already carries `input` and
// `output`.
//
// `inputDetails` accompanies `input` and `outputDetails` accompanies `output`, rather than either
// appearing unconditionally. **That coupling is substrate's reading**: neither page states when
// the details members are present, and details *about* a payload the body does not carry would
// describe nothing. It also keeps the pair consistent with `output`'s own published rule that it
// is set only when the execution succeeded.
func sfnAddExecutionResultMembers(out map[string]interface{}, exec *ExecutionState) {
	if _, ok := out["input"]; ok {
		out["inputDetails"] = sfnDataDetails()
	}
	if _, ok := out["output"]; ok {
		out["outputDetails"] = sfnDataDetails()
	}
	if errorCode, cause := sfnExecutionFailure(exec); errorCode != "" || cause != "" {
		if errorCode != "" {
			out["error"] = errorCode
		}
		if cause != "" {
			out["cause"] = cause
		}
	}
}
