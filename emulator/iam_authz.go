package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// iamAuthzRequestContext publishes the condition keys an IAM request carries about
// itself rather than about any resource it names.
//
// Today that is one key, `iam:AWSServiceName`. Eleven of the condition blocks in
// substrate's bundled managed policies condition on it — the single largest group in the
// bundle, and all eleven on `iam:CreateServiceLinkedRole`. Before #747 none of them could
// be satisfied, because the operation that carries the parameter did not exist and
// nothing published the value.
//
// It is published for CreateServiceLinkedRole and DeleteServiceLinkedRole, the two
// actions the Service Authorization Reference lists it on, and **not** for
// GetServiceLinkedRoleDeletionStatus. That asymmetry is deliberate: publishing a key AWS
// does not would let a `StringEquals` on it succeed here and fail there, which is the
// permissive direction and the one a privilege boundary must not drift in. A key that is
// absent is what [condKeyPresent] reads as absent, so a positive condition fails and a
// negated one holds — exactly AWS's answer for a key the request does not carry.
//
// It is a *request* key, not a resource key: it describes what the caller asked for, so
// it is read once per request and merged under every resource, the same treatment
// [addRequestTags] gets and for the same reason.
func iamAuthzRequestContext(condCtx map[string]string, req *AWSRequest) {
	if req.Service != "iam" {
		return
	}
	if name := iamAWSServiceNameParam(req); name != "" {
		condCtx[iamSLRAWSServiceNameCondKey] = name
	}
}

// iamAWSServiceNameParam returns the service principal the request is about, or "".
func iamAWSServiceNameParam(req *AWSRequest) string {
	switch req.Operation {
	case "CreateServiceLinkedRole":
		return iamAuthzParam(req, "AWSServiceName")
	case "DeleteServiceLinkedRole":
		// The reference lists the key on this operation too, but the operation names the
		// role rather than the service — RoleName is the only thing on the wire. The
		// service is recovered from the name substrate minted ([iamSLRServiceFromRoleName]),
		// which resolves for every principal the table covers and yields "" for the rest,
		// leaving the key absent rather than guessed.
		return iamSLRServiceFromRoleName(iamAuthzParam(req, "RoleName"))
	default:
		return ""
	}
}

// iamAuthzParam reads one named request parameter, from whichever of the two forms an
// IAM request arrives in.
//
// Params is the query form a real SDK or the CLI sends, and it is what `server.go`
// marshals into Body — so for a live request the two hold the same data and Params is
// authoritative. A caller that speaks the JSON protocol directly, which substrate accepts
// and its own tests use, populates only Body. Reading both is what keeps an authorization
// decision from depending on which of the two a caller chose; [addRequestTags]' iam arm
// reads the body for the same reason, from the other side.
func iamAuthzParam(req *AWSRequest, name string) string {
	if v := req.Params[name]; v != "" {
		return v
	}
	if len(req.Body) == 0 {
		return ""
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return ""
	}
	var v string
	if err := json.Unmarshal(body[name], &v); err != nil {
		return ""
	}
	return v
}

// iamAuthzResources returns the resources an IAM request names, when substrate can
// resolve them, or nil to leave the request on the single-resource path.
//
// [AuthController.buildResourceARN]'s iam arm answers `arn:aws:iam::<acct>:*` for every
// IAM operation, which is a literal `*` in the resource position that no pattern with a
// path in it can match. Seven of the bundled service-linked-role statements scope their
// Resource to `arn:aws:iam::*:role/aws-service-role/…`, two of them to an exact ARN
// with no trailing wildcard, so all seven denied a request they are written to allow.
//
// Those statements are also the citation for resolving all three operations rather than
// only the create. Two of them — the SSM policy's and the Cognito policy's — name
// `iam:DeleteServiceLinkedRole` and `iam:GetServiceLinkedRoleDeletionStatus` together and
// scope Resource to a service-linked-role ARN, so a status poll's request resource has to
// be that ARN for AWS's own managed policy to grant what it plainly intends. That is
// provenance from a policy shipping inside substrate rather than from the API model — the
// Service Authorization Reference's resource-type column for these actions could not be
// read, because the page renders client-side.
//
// This resolves the resource **only for the service-linked-role operations**. Deriving a
// per-operation resource for the rest of IAM is its own piece of work (#770) and
// deliberately not attempted here: a half-done version would move the wrong-answer
// boundary rather than remove it.
func iamAuthzResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	arn := iamAuthzSLRResourceARN(state, reqCtx, req)
	if arn == "" {
		return nil
	}
	return []authzResource{{ARN: arn}}
}

// iamAuthzSLRResourceARN returns the service-linked-role ARN an SLR operation is about,
// or "" when the request is not one or does not name enough to resolve it.
func iamAuthzSLRResourceARN(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	switch req.Operation {
	case "CreateServiceLinkedRole":
		service := iamAuthzParam(req, "AWSServiceName")
		if service == "" {
			return ""
		}
		name := iamSLRRoleName(service, iamAuthzParam(req, "CustomSuffix"))
		return iamRoleARN(reqCtx.AccountID, iamSLRPath(service), name)

	case "DeleteServiceLinkedRole":
		name := iamAuthzParam(req, "RoleName")
		if name == "" {
			return ""
		}
		// The role's own record holds the reserved path, and the path holds the
		// service. Reading it is what makes the ARN the same string CreateServiceLinkedRole
		// built — deriving the service from the *name* would have to invert
		// [iamSLRRoleName], which is not invertible for a name outside the table.
		path := iamAuthzRolePath(state, reqCtx.AccountID, name)
		if !iamIsSLRPath(path) {
			// Not a service-linked role, or no such role. Either way this is not the
			// resource shape those statements are written about, so leave the request
			// on the general path rather than mint an ARN that only looks specific.
			return ""
		}
		return iamRoleARN(reqCtx.AccountID, path, name)

	case "GetServiceLinkedRoleDeletionStatus":
		// AWS's documented task-ID format embeds both the service principal and the
		// role name, so this one resolves with no state read at all — which also means
		// it resolves for a task whose role has already been deleted, the normal case
		// for a status poll that reports SUCCEEDED.
		service, name, ok := iamSLRRoleFromDeletionTaskID(iamAuthzParam(req, "DeletionTaskId"))
		if !ok {
			return ""
		}
		return iamRoleARN(reqCtx.AccountID, iamSLRPath(service), name)

	default:
		return ""
	}
}

// iamAuthzRolePath returns the stored path of a role, or "" when there is no such role.
//
// It reads through the raw [StateManager] rather than through IAMPlugin, for the reason
// the other authz resolvers do: [AuthController] holds no plugin, and a decision must
// not depend on one being registered.
func iamAuthzRolePath(state StateManager, accountID, roleName string) string {
	if state == nil {
		return ""
	}
	raw, err := state.Get(context.Background(), iamNamespace, iamRoleKey(accountID, roleName))
	if err != nil || raw == nil {
		return ""
	}
	var role IAMRole
	if err := json.Unmarshal(raw, &role); err != nil {
		return ""
	}
	return role.Path
}

// iamSLRServiceFromRoleName recovers a service principal from a service-linked role's
// name, using the same table [iamSLRRoleName] mints from.
//
// It exists for the second authorization door, which sees a DeleteServiceLinkedRole
// request and has to publish `iam:AWSServiceName` for it without the state read
// [iamAuthzRolePath] performs. A name minted with a custom suffix still resolves,
// because the suffix is appended after the table's value; a name outside the table does
// not, and the key is then absent rather than guessed — which [condKeyPresent] reads as
// absent, so a positive condition fails and a negated one holds, exactly as it would on
// AWS for a key the request does not carry.
func iamSLRServiceFromRoleName(roleName string) string {
	if !strings.HasPrefix(roleName, iamSLRNamePrefix) {
		return ""
	}
	best, bestLen := "", 0
	for service, name := range iamSLRRoleNames {
		if roleName != name && !strings.HasPrefix(roleName, name+"_") {
			continue
		}
		// Longest match wins. No two table values can both match one name today — the
		// "_" separator prevents it — but map iteration order is unspecified, so
		// picking by length rather than by whichever row came first is what keeps a
		// future table addition from making the answer depend on the runtime.
		if len(name) > bestLen {
			best, bestLen = service, len(name)
		}
	}
	return best
}
