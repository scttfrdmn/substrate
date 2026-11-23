package emulator

// TracingConfig (#1519).
//
// CreateFunction and UpdateFunctionConfiguration take `TracingConfig: {Mode}`, and every
// FunctionConfiguration answers it back as TracingConfigResponse. Substrate dropped it, so a
// function Terraform created with `tracing_config { mode = "Active" }` read back as PassThrough and
// the plan after the apply showed a change. The mode is recorded intent: substrate emits no trace.

// The two values TracingConfig.Mode publishes.
const (
	lambdaTracingActive      = "Active"
	lambdaTracingPassThrough = "PassThrough"
)

// lambdaTracingConfig is the TracingConfig member of a CreateFunction or
// UpdateFunctionConfiguration request.
type lambdaTracingConfig struct {
	// Mode is Active or PassThrough; empty when the request carries no TracingConfig.
	Mode string `json:"Mode"`
}

// validate refuses a Mode outside the published enum with InvalidParameterValueException. The
// message is substrate's own.
func (c lambdaTracingConfig) validate() *AWSError {
	switch c.Mode {
	case "", lambdaTracingActive, lambdaTracingPassThrough:
		return nil
	}
	return lambdaInvalidParameterValue("TracingConfig.Mode must be Active or PassThrough, not " + c.Mode)
}
