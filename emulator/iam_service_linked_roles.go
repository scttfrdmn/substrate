package emulator

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// iamSLRPathPrefix is the reserved path a service-linked role lives under.
//
// AWS's own policy templates for service-linked roles name it in every Resource
// they scope — "arn:aws:iam::*:role/aws-service-role/{{SERVICE-NAME}}.amazonaws.com/…"
// — and five of substrate's bundled managed policies do the same, which is what
// makes the path load-bearing rather than cosmetic: a role minted anywhere else
// matches none of those statements.
const iamSLRPathPrefix = "/aws-service-role/"

// iamSLRNamePrefix is the prefix AWS puts on every service-linked role's name.
//
// AWS calls it "the service-provided prefix" and never publishes the mapping from a
// service principal to the rest of the name; see [iamSLRRoleName].
const iamSLRNamePrefix = "AWSServiceRoleFor"

// iamSLRServiceSuffix is the domain every service principal ends with.
const iamSLRServiceSuffix = ".amazonaws.com"

// iamSLRAWSServiceNameCondKey is the condition key AWS publishes for
// CreateServiceLinkedRole's own parameter.
//
// Eleven of the 32 condition blocks in substrate's bundled managed policies condition
// on it, which made it the largest group of statements that could not be evaluated at
// all before #747 — the operation carrying the parameter did not exist.
const iamSLRAWSServiceNameCondKey = "iam:AWSServiceName"

// Deletion-task statuses, which are AWS's four documented values for
// GetServiceLinkedRoleDeletionStatus: "SUCCEEDED | IN_PROGRESS | FAILED |
// NOT_STARTED".
const (
	iamSLRDeletionSucceeded  = "SUCCEEDED"
	iamSLRDeletionInProgress = "IN_PROGRESS"
	iamSLRDeletionFailed     = "FAILED"
	iamSLRDeletionNotStarted = "NOT_STARTED"
)

// iamSLRDeletionStatuses is the set the seed endpoint accepts, so a typo is refused
// where it is written rather than reported later as a status no SDK models.
var iamSLRDeletionStatuses = map[string]bool{
	iamSLRDeletionSucceeded:  true,
	iamSLRDeletionInProgress: true,
	iamSLRDeletionFailed:     true,
	iamSLRDeletionNotStarted: true,
}

// iamSLRRoleNames maps a service principal to the role name AWS mints for it.
//
// Every row is read off a Resource element in substrate's own bundled AWS managed
// policies, which spell the name AWS uses — `AWSServiceRoleForElastiCache`,
// `AWSServiceRoleForAmazonSSM`, `AWSServiceRoleForCloudWatchEvents`. That is the only
// source substrate can cite for the mapping: AWS publishes no rule deriving a role name
// from a service principal, and the IAM User Guide warns against inferring the principal
// itself — "Do not try to guess the service principal, because it is case sensitive
// and the format can vary across AWS services."
//
// So the table is exactly the six principals a statement in this repository names, and
// nothing more. It is deliberately not padded out with the other well-known names
// (`AWSServiceRoleForRDS`, `AWSServiceRoleForEC2Spot`, …): a guessed row would sit in the
// slot the real name needed and be indistinguishable from a cited one, which is the #561
// failure — a plausible-looking identifier hiding the absence of a verified one.
// [iamSLRDerivedRoleName] answers for every other principal and is documented as
// substrate's convention, so a name it produces cannot be mistaken for AWS's.
//
// The six rows are exactly the principals a bundled statement names inside a
// service-linked-role Resource, and two of those Resources are an exact ARN with no
// trailing `*` — Lambda's and ElastiCache's — so they would match nothing without a row
// here. That is what makes those statements evaluate rather than deny (#747).
//
// One bundled statement scopes Resource around a principal the table does not cover:
// AWSBatchFullAccess pairs `batch.amazonaws.com` with `arn:aws:iam::*:role/*Batch*`. A
// derived name satisfies it, because the pattern is a contains-glob rather than a path —
// but that is luck, not a rule, and it is the reason [iamSLRDerivedRoleName] title-cases
// rather than lower-casing.
var iamSLRRoleNames = map[string]string{
	"lambda.amazonaws.com":            "AWSServiceRoleForLambda",
	"elasticache.amazonaws.com":       "AWSServiceRoleForElastiCache",
	"events.amazonaws.com":            "AWSServiceRoleForCloudWatchEvents",
	"ssm.amazonaws.com":               "AWSServiceRoleForAmazonSSM",
	"cognito-idp.amazonaws.com":       "AWSServiceRoleForAmazonCognitoIdp",
	"email.cognito-idp.amazonaws.com": "AWSServiceRoleForAmazonCognitoIdpEmail",
}

// iamSLRPath returns the reserved path a service's linked role lives under.
func iamSLRPath(serviceName string) string {
	return iamSLRPathPrefix + serviceName + "/"
}

// iamIsSLRPath reports whether path is the reserved service-linked-role path.
func iamIsSLRPath(path string) bool {
	return strings.HasPrefix(path, iamSLRPathPrefix)
}

// iamSLRServiceFromPath returns the service principal a reserved path names, or "" when
// path is not one.
func iamSLRServiceFromPath(path string) string {
	if !iamIsSLRPath(path) {
		return ""
	}
	return strings.Trim(strings.TrimPrefix(path, iamSLRPathPrefix), "/")
}

// iamSLRRoleName returns the role name CreateServiceLinkedRole mints for a service
// principal and an optional custom suffix.
//
// AWS: "A string that you provide, which is combined with the service-provided prefix
// to form the complete role name." It does not say how the two are joined; AWS's own
// role names use "_", which is what substrate uses — the separator is observed
// behavior rather than the API model, and it matters because the bundled statements
// that scope Resource end in `*` and so admit any suffix at all.
func iamSLRRoleName(serviceName, customSuffix string) string {
	name, ok := iamSLRRoleNames[serviceName]
	if !ok {
		name = iamSLRDerivedRoleName(serviceName)
	}
	if customSuffix != "" {
		name += "_" + customSuffix
	}
	return name
}

// iamSLRDerivedRoleName is substrate's convention for a service principal
// [iamSLRRoleNames] holds no row for.
//
// It strips the `.amazonaws.com` suffix and title-cases each remaining segment, so
// `foo.application-autoscaling.amazonaws.com` yields
// `AWSServiceRoleForFooApplicationAutoscaling`. That is a *convention*, not AWS's rule:
// AWS's real names are not derivable — `spot.amazonaws.com` becomes
// `AWSServiceRoleForEC2Spot`, which no transformation of "spot" produces — so the
// honest thing is a stable, well-formed name plus a table for every principal substrate
// has a citation for.
//
// A name derived here still satisfies every bundled statement that names a principal
// outside [iamSLRRoleNames]: all but one of those statements scope Resource to "*", and
// the exception — AWSBatchFullAccess's `arn:aws:iam::*:role/*Batch*` — is matched by the
// title-cased `AWSServiceRoleForBatch` this produces (#747).
func iamSLRDerivedRoleName(serviceName string) string {
	trimmed := strings.TrimSuffix(serviceName, iamSLRServiceSuffix)
	var b strings.Builder
	b.WriteString(iamSLRNamePrefix)
	for _, segment := range strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == '.' || r == '-' || r == '_'
	}) {
		b.WriteString(strings.ToUpper(segment[:1]))
		b.WriteString(segment[1:])
	}
	return b.String()
}

// iamSLRTrustPolicy is the trust policy a service-linked role carries.
//
// AWS: "The defined permissions include the trust policy and the permissions policy",
// and the linked service is the only principal that may assume the role — "unless
// defined otherwise, only that service can assume the roles". Substrate writes exactly
// that one statement, which is what makes the role usable by the STS trust-policy gate
// (#593) without inventing permissions the service would grant itself.
func iamSLRTrustPolicy(serviceName string) PolicyDocument {
	return PolicyDocument{
		Version: policyVarVersion,
		Statement: []PolicyStatement{{
			Effect:    IAMEffectAllow,
			Principal: &PolicyPrincipal{Service: StringOrSlice{serviceName}},
			Action:    StringOrSlice{"sts:AssumeRole"},
		}},
	}
}

// IAMSLRDeletionTask records one DeleteServiceLinkedRole submission.
//
// It exists because the operation is asynchronous on AWS: it "Submits a
// service-linked role deletion request and returns a DeletionTaskId, which you can use
// to check the status of the deletion." The record is what
// GetServiceLinkedRoleDeletionStatus answers from, and its Status is seedable so a
// consumer's poll loop can be driven to a FAILED outcome without wall-clock time —
// which is the only way the failure AWS documents ("If you submit a deletion request
// for a service-linked role whose linked service is still accessing a resource, then
// the deletion task fails") is reachable in an emulator that runs no linked service.
type IAMSLRDeletionTask struct {
	// DeletionTaskID is the identifier the submission returned, in AWS's documented
	// format `task/aws-service-role/<service-principal-name>/<role-name>/<task-uuid>`.
	DeletionTaskID string `json:"DeletionTaskId"`

	// RoleName is the service-linked role the task is deleting.
	RoleName string `json:"RoleName"`

	// ServiceName is the role's linked service principal.
	ServiceName string `json:"ServiceName"`

	// Status is one of AWS's four documented values.
	Status string `json:"Status"`

	// Reason is the failure reason, set only for a FAILED task.
	Reason string `json:"Reason,omitempty"`

	// RoleUsageList names the resources still using the role, which AWS reports
	// alongside a failure reason "usually including the resources that must be
	// deleted".
	RoleUsageList []IAMSLRRoleUsage `json:"RoleUsageList,omitempty"`
}

// IAMSLRRoleUsage is one region's worth of resources still using a service-linked
// role, as reported inside a failed deletion task's reason.
type IAMSLRRoleUsage struct {
	// Region is the region the resources are in.
	Region string `json:"Region"`

	// Resources are the ARNs that must be deleted before the role can be.
	Resources []string `json:"Resources"`
}

// iamSLRDeletionTaskID builds the identifier DeleteServiceLinkedRole returns.
//
// AWS documents the format exactly — `task/aws-service-role/<service-principal-name>/
// <role-name>/<task-uuid>` — and it matters beyond cosmetics: the service and the role
// are *in* the ID, which is what lets [iamAuthzResources] name the resource a
// GetServiceLinkedRoleDeletionStatus request is about without reading state.
func iamSLRDeletionTaskID(serviceName, roleName, uuid string) string {
	return "task/aws-service-role/" + serviceName + "/" + roleName + "/" + uuid
}

// iamSLRRoleFromDeletionTaskID returns the service principal and role name a deletion
// task ID names, and whether it parsed.
func iamSLRRoleFromDeletionTaskID(taskID string) (serviceName, roleName string, ok bool) {
	rest, found := strings.CutPrefix(taskID, "task/aws-service-role/")
	if !found {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// iamSLRTaskUUID mints the trailing component of a deletion task ID.
//
// The shape is a UUID because AWS's own example is one
// (`ec720f7a-c0ba-4838-be33-f72e1873dd52`) and an SDK or a consumer's regex may read
// it as such. It is random for the same reason [generateIAMID] is: an identifier AWS
// generates is not derivable from the request, and deriving one would make two
// deletions of the same role collide on a single ID.
func iamSLRTaskUUID() string {
	raw := make([]byte, 16)
	if _, err := cryptorand.Read(raw); err != nil {
		panic(fmt.Sprintf("iamSLRTaskUUID: crypto/rand read: %v", err))
	}
	// Version 4, variant 1, so the value is a well-formed random UUID rather than 16
	// hex-encoded bytes that merely look like one.
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	h := hex.EncodeToString(raw)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
