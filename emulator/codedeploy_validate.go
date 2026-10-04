package emulator

import (
	"net/http"
	"regexp"
)

// What a CodeDeploy request must carry, and what each omission reports (#1197, #1198).
//
// CodeDeploy publishes one refusal per required member rather than one generic code, and each
// operation's page lists exactly the ones that apply to it. So the codes below are per member, not
// per service, and a handler answers the one its own page publishes:
//
//   - applicationName: ApplicationNameRequiredException when absent, InvalidApplicationNameException
//     when it breaks the published Length Constraints (1–100) or Pattern ([A-Za-z0-9+=,.@_-]*). Both
//     are on every page that takes the member: CreateApplication, GetApplication, DeleteApplication,
//     CreateDeploymentGroup, GetDeploymentGroup, DeleteDeploymentGroup and CreateDeployment.
//   - deploymentGroupName: DeploymentGroupNameRequiredException and InvalidDeploymentGroupNameException,
//     the same constraints, on every page that takes it.
//   - serviceRoleArn: RoleRequiredException when absent, InvalidRoleException when it is not a role
//     ARN (CreateDeploymentGroup).
//   - computePlatform: InvalidComputePlatformException for a value outside Server | Lambda | ECS |
//     Kubernetes (CreateApplication).
//   - deploymentId: DeploymentIdRequiredException when absent (GetDeployment).
//
// InvalidInputException, which every one of these sites used to answer, is published on only two
// pages (CreateDeploymentGroup and CreateDeployment) and is not on CodeDeploy's Common Errors page,
// so a caller catching the published per-member exception caught nothing (#1198). It is no longer
// answered for a missing member anywhere.
//
// All of them are 400, which is the only status any CodeDeploy page publishes for a refusal.

// codedeployNamePattern is the published Pattern shared by applicationName, deploymentGroupName and
// deploymentConfigName. The pattern is a character class with `*`, so it admits the empty string;
// the separate Length Constraints minimum of 1 is what refuses that, and the absent case is the
// member-required refusal rather than this one.
var codedeployNamePattern = regexp.MustCompile(`^[A-Za-z0-9+=,.@_-]*$`)

// codedeployNameMaxLen is the published maximum length of the three names above.
const codedeployNameMaxLen = 100

// codedeployRoleARNPattern is what substrate accepts as a service role ARN.
//
// API_CreateDeploymentGroup types serviceRoleArn as String with no pattern and glosses
// InvalidRoleException as "The service role ARN was specified in an invalid format". The page gives no
// format, so this is substrate's reading of "an IAM role ARN": the arn prefix, any partition, the iam
// service, an empty Region, a twelve-digit account and a role/ resource with a path and a name. It is
// deliberately no narrower than that — the role's existence and its trust policy are not checked,
// because nothing in CodeDeploy runs as it.
var codedeployRoleARNPattern = regexp.MustCompile(`^arn:aws[a-zA-Z-]*:iam::\d{12}:role/.+$`)

// codedeployComputePlatforms is API_CreateApplication's published Valid Values for computePlatform.
var codedeployComputePlatforms = map[string]bool{"Server": true, "Lambda": true, "ECS": true, "Kubernetes": true}

// codedeployDefaultDeploymentConfig is the configuration API_CreateDeploymentGroup names as the
// default, in the page's words: "CodeDeployDefault.OneAtATime is the default deployment configuration.
// It is used if a configuration isn't specified for the deployment or deployment group." That sentence
// is the whole of its provenance.
const codedeployDefaultDeploymentConfig = "CodeDeployDefault.OneAtATime"

// codedeployErr builds a CodeDeploy refusal. Every code CodeDeploy publishes for a caller error is 400.
func codedeployErr(code, message string) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: http.StatusBadRequest}
}

// codedeployCheckApplicationName refuses an absent or malformed applicationName.
func codedeployCheckApplicationName(name string) *AWSError {
	if name == "" {
		return codedeployErr("ApplicationNameRequiredException", "The minimum number of required application names was not specified.")
	}
	if len(name) > codedeployNameMaxLen || !codedeployNamePattern.MatchString(name) {
		return codedeployErr("InvalidApplicationNameException", "The application name was specified in an invalid format.")
	}
	return nil
}

// codedeployCheckGroupName refuses an absent or malformed deploymentGroupName.
func codedeployCheckGroupName(name string) *AWSError {
	if name == "" {
		return codedeployErr("DeploymentGroupNameRequiredException", "The deployment group name was not specified.")
	}
	if len(name) > codedeployNameMaxLen || !codedeployNamePattern.MatchString(name) {
		return codedeployErr("InvalidDeploymentGroupNameException", "The deployment group name was specified in an invalid format.")
	}
	return nil
}

// codedeployCheckConfigName refuses a deploymentConfigName that breaks the published constraints. An
// absent one is not refused: the member is Required: No, and the default applies.
//
// The name is not checked against the configurations that exist: substrate models no deployment
// configuration, so DeploymentConfigDoesNotExistException has no site, and refusing a custom
// configuration name a caller created elsewhere would be worse than accepting it.
func codedeployCheckConfigName(name string) *AWSError {
	if name == "" {
		return nil
	}
	if len(name) > codedeployNameMaxLen || !codedeployNamePattern.MatchString(name) {
		return codedeployErr("InvalidDeploymentConfigNameException", "The deployment configuration name was specified in an invalid format.")
	}
	return nil
}
