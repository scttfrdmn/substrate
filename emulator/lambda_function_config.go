package emulator

import (
	"cmp"
	"slices"
	"strconv"
)

// ImageConfig, EphemeralStorage and LoggingConfig (#1659).
//
// CreateFunction and UpdateFunctionConfiguration take all three, and every FunctionConfiguration
// answers them back: ImageConfig as `ImageConfigResponse{ImageConfig}` for an image-packaged
// function, EphemeralStorage as `{Size}`, and LoggingConfig with its defaults filled in. Each is
// recorded intent: substrate runs no container, mounts no /tmp and writes no log.
//
// A function record keeps each field only where it differs from the default, so a record an older
// build wrote, which carries none of them, renders the defaults, and an explicit default is the same
// record as an omitted one. [lambdaFunctionFingerprint] relies on that: a function whose three
// fields are all defaults fingerprints as it did before they existed.

// The bounds and default of EphemeralStorage.Size, in MB. API_EphemeralStorage: "The default value
// is 512, but can be any whole number between 512 and 10,240 MB." The page's higher maximum for a
// function using AcceleratorConfig does not apply, since substrate models no AcceleratorConfig.
const (
	lambdaEphemeralStorageDefault = 512
	lambdaEphemeralStorageMax     = 10240
)

// The values LoggingConfig.LogFormat publishes, and the level both log levels default to under
// JSON. The Lambda developer guide's log-level page: "The default application log level for log
// filtering is INFO" and "By default, Lambda sets the system log level to INFO".
const (
	lambdaLogFormatText   = "Text"
	lambdaLogFormatJSON   = "JSON"
	lambdaLogLevelDefault = "INFO"
)

// The enums ApplicationLogLevel and SystemLogLevel publish.
var (
	lambdaApplicationLogLevels = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"} //nolint:gochecknoglobals // immutable enum
	lambdaSystemLogLevels      = []string{"DEBUG", "INFO", "WARN"}                            //nolint:gochecknoglobals // immutable enum
)

// LambdaImageConfig is a function's ImageConfig: the values that override its container image's
// ENTRYPOINT, CMD and WORKDIR.
type LambdaImageConfig struct {
	// EntryPoint overrides the image's ENTRYPOINT.
	EntryPoint []string `json:"EntryPoint,omitempty"`

	// Command overrides the image's CMD.
	Command []string `json:"Command,omitempty"`

	// WorkingDirectory overrides the image's WORKDIR.
	WorkingDirectory string `json:"WorkingDirectory,omitempty"`
}

// empty reports whether the configuration overrides nothing.
func (c *LambdaImageConfig) empty() bool {
	return c == nil || (len(c.EntryPoint) == 0 && len(c.Command) == 0 && c.WorkingDirectory == "")
}

// LambdaLoggingConfig is a function's LoggingConfig as the record keeps it: each member is empty
// where the function uses the default.
type LambdaLoggingConfig struct {
	// LogFormat is JSON, or empty for the default Text.
	LogFormat string `json:"LogFormat,omitempty"`

	// ApplicationLogLevel filters application logs; only valid with JSON.
	ApplicationLogLevel string `json:"ApplicationLogLevel,omitempty"`

	// SystemLogLevel filters system logs; only valid with JSON.
	SystemLogLevel string `json:"SystemLogLevel,omitempty"`

	// LogGroup is the log group the function logs to, or empty for `/aws/lambda/<name>`.
	LogGroup string `json:"LogGroup,omitempty"`
}

// lambdaEphemeralStorage is the EphemeralStorage member of a request.
type lambdaEphemeralStorage struct {
	// Size is the /tmp size in MB.
	Size int `json:"Size"`
}

// lambdaDefaultLogGroup is the log group a function logs to when its LoggingConfig names none.
func lambdaDefaultLogGroup(functionName string) string {
	return "/aws/lambda/" + functionName
}

// lambdaResolveEphemeralStorage validates a request's EphemeralStorage and returns the size the
// record keeps: zero for the default, current when the request carries none.
//
// A size outside 512–10240 is InvalidParameterValueException, the code CreateFunction and
// UpdateFunctionConfiguration publish for "one of the parameters in the request is not valid"; the
// message is substrate's own.
func lambdaResolveEphemeralStorage(current int, req *lambdaEphemeralStorage) (int, *AWSError) {
	if req == nil {
		return current, nil
	}
	if req.Size < lambdaEphemeralStorageDefault || req.Size > lambdaEphemeralStorageMax {
		return 0, lambdaInvalidParameterValue("EphemeralStorage.Size must be between " +
			strconv.Itoa(lambdaEphemeralStorageDefault) + " and " + strconv.Itoa(lambdaEphemeralStorageMax) +
			" MB, not " + strconv.Itoa(req.Size))
	}
	if req.Size == lambdaEphemeralStorageDefault {
		return 0, nil
	}
	return req.Size, nil
}

// lambdaResolveLoggingConfig merges a request's LoggingConfig into the function's current one and
// returns what the record keeps, nil when every member is the default.
//
// A member the request names replaces the current one, and one it leaves empty keeps it. A request
// that sets LogFormat to Text and names no level drops the stored levels, which apply only under
// JSON.
//
// A value outside a member's enum is InvalidParameterValueException, with substrate's own message.
// A log level under Text — named, or the default when no format is named — is refused with the
// message Lambda answers, which is observed rather than published: floci-io/floci#5920 measured
// it on CreateFunction and UpdateFunctionConfiguration (eu-west-1, 2026-10-18), and
// aws-cloudformation/resource-provider-enhanced-schemas#81 reproduced the refusal.
func lambdaResolveLoggingConfig(current, req *LambdaLoggingConfig, functionName string) (*LambdaLoggingConfig, *AWSError) {
	if req == nil {
		return current, nil
	}
	if req.LogFormat != "" && req.LogFormat != lambdaLogFormatText && req.LogFormat != lambdaLogFormatJSON {
		return nil, lambdaInvalidParameterValue("LoggingConfig.LogFormat must be JSON or Text, not " + req.LogFormat)
	}
	if req.ApplicationLogLevel != "" && !slices.Contains(lambdaApplicationLogLevels, req.ApplicationLogLevel) {
		return nil, lambdaInvalidParameterValue("LoggingConfig.ApplicationLogLevel must be one of TRACE, DEBUG, INFO, WARN, ERROR or FATAL, not " +
			req.ApplicationLogLevel)
	}
	if req.SystemLogLevel != "" && !slices.Contains(lambdaSystemLogLevels, req.SystemLogLevel) {
		return nil, lambdaInvalidParameterValue("LoggingConfig.SystemLogLevel must be one of DEBUG, INFO or WARN, not " + req.SystemLogLevel)
	}

	var merged LambdaLoggingConfig
	if current != nil {
		merged = *current
	}
	if req.LogFormat != "" {
		merged.LogFormat = req.LogFormat
		if req.LogFormat == lambdaLogFormatText && req.ApplicationLogLevel == "" && req.SystemLogLevel == "" {
			merged.ApplicationLogLevel, merged.SystemLogLevel = "", ""
		}
	}
	if req.ApplicationLogLevel != "" {
		merged.ApplicationLogLevel = req.ApplicationLogLevel
	}
	if req.SystemLogLevel != "" {
		merged.SystemLogLevel = req.SystemLogLevel
	}
	if req.LogGroup != "" {
		merged.LogGroup = req.LogGroup
	}

	if merged.LogFormat == lambdaLogFormatText {
		merged.LogFormat = ""
	}
	if merged.LogFormat == "" && (merged.ApplicationLogLevel != "" || merged.SystemLogLevel != "") {
		// The wording Lambda answers; see the function comment for the source.
		return nil, lambdaInvalidParameterValue(`LogLevel is not supported when LogFormat is set to "Text". ` +
			`Remove LogLevel from your request or change the LogFormat to "JSON" and try again.`)
	}
	// A level equal to JSON's default is the default, so naming it is the same record as omitting it.
	if merged.ApplicationLogLevel == lambdaLogLevelDefault {
		merged.ApplicationLogLevel = ""
	}
	if merged.SystemLogLevel == lambdaLogLevelDefault {
		merged.SystemLogLevel = ""
	}
	if merged.LogGroup == lambdaDefaultLogGroup(functionName) {
		merged.LogGroup = ""
	}
	if merged == (LambdaLoggingConfig{}) {
		return nil, nil
	}
	return &merged, nil
}

// lambdaImageConfigOnZip refuses an ImageConfig on a function whose package is a .zip archive. The
// message is the one Lambda answers, observed rather than published: CreateFunction's refusal quoted
// in Stack Overflow question 76862775 from the Terraform AWS provider.
func lambdaImageConfigOnZip() *AWSError {
	return lambdaInvalidParameterValue("The ImageConfig parameter is not supported for functions created with deployment packages.")
}

// lambdaResolveImageConfig returns the ImageConfig the record keeps: the request's, which replaces
// the current one whole, nil when it overrides nothing, and current when the request carries none.
func lambdaResolveImageConfig(current, req *LambdaImageConfig) *LambdaImageConfig {
	if req == nil {
		return current
	}
	if req.empty() {
		return nil
	}
	c := *req
	return &c
}

// renderLambdaFunctionFields adds EphemeralStorage, LoggingConfig and, for an image-packaged
// function with an ImageConfig, ImageConfigResponse to a rendered FunctionConfiguration.
func renderLambdaFunctionFields(cfg map[string]interface{}, fn LambdaFunction) {
	cfg["EphemeralStorage"] = map[string]int{"Size": cmp.Or(fn.EphemeralStorageSize, lambdaEphemeralStorageDefault)}

	var stored LambdaLoggingConfig
	if fn.LoggingConfig != nil {
		stored = *fn.LoggingConfig
	}
	logging := map[string]string{
		"LogFormat": cmp.Or(stored.LogFormat, lambdaLogFormatText),
		"LogGroup":  cmp.Or(stored.LogGroup, lambdaDefaultLogGroup(fn.FunctionName)),
	}
	if logging["LogFormat"] == lambdaLogFormatJSON {
		logging["ApplicationLogLevel"] = cmp.Or(stored.ApplicationLogLevel, lambdaLogLevelDefault)
		logging["SystemLogLevel"] = cmp.Or(stored.SystemLogLevel, lambdaLogLevelDefault)
	}
	cfg["LoggingConfig"] = logging

	if fn.PackageType == "Image" && !fn.ImageConfig.empty() {
		cfg["ImageConfigResponse"] = map[string]interface{}{"ImageConfig": *fn.ImageConfig}
	}
}
