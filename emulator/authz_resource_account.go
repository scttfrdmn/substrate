package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// authz_resource_account.go holds the authorization inputs the CDK's bootstrap roles depend on
// (#1606). `cdk deploy` runs its asset upload as the bootstrap stack's file-publishing role and its
// deployment as the deploy role, and substrate enforces both roles' policies. Three inputs those
// policies are written against were missing, and each denied a call the policy allows:
//
//   - aws:ResourceAccount. The file-publishing role's only S3 statement is conditioned on
//     `StringEquals aws:ResourceAccount ${AWS::AccountId}`, and the deploy role's cross-account
//     statement on `StringNotEquals s3:ResourceAccount`. The key was never written, so the first
//     never matched.
//   - An SSM parameter's ARN. ssm:GetParameter was authorized against "*", so the deploy role's
//     ReadVersion statement — scoped to the bootstrap-version parameter's ARN — never matched.
//   - A bucket-level S3 request's ARN. A path-style ListObjectsV2 arrives as "/bucket/", and the
//     trailing slash made its resource "arn:aws:s3:::bucket/", which no statement naming the
//     bucket's ARN matches.

// authzResourceAccount returns the account that owns a request's resource: the account an ARN
// names; for an S3 bucket, whose ARN names none, the account that created the bucket; and
// otherwise the requesting account, which is where substrate keeps every resource a request can
// reach without naming another account. AWS documents aws:ResourceAccount as "the resource owner's
// AWS account ID" and includes it "in the request context for most service actions".
//
// An SSM public parameter is the exception, and answers "" so the key is left unset. Its ARN names
// no account (see [ssmAuthzResourceARN]): it is AWS-owned, AWS publishes no account ID for it, and
// the key's own page warns that "some AWS services require access to AWS owned resources that are
// hosted in another AWS account" which an aws:ResourceAccount condition may block. Reporting the
// caller's account would claim the caller owns AWS's parameters; leaving the key unset is
// substrate's reading of an answer AWS does not publish.
func (a *AuthController) authzResourceAccount(reqCtx *RequestContext, resourceARN string) string {
	parts := strings.SplitN(resourceARN, ":", 6)
	if len(parts) == 6 && parts[0] == "arn" {
		if acct := parts[4]; acct != "" && !strings.ContainsAny(acct, "*?") {
			return acct
		}
		switch {
		case parts[2] == "s3" && parts[4] == "":
			if owner := a.s3BucketOwner(parts[5]); owner != "" {
				return owner
			}
		case parts[2] == "ssm" && parts[4] == "":
			return ""
		}
	}
	return reqCtx.AccountID
}

// s3BucketOwner returns the account that created the bucket an S3 ARN's resource ("bucket" or
// "bucket/key") names, or "" when the bucket has no record.
func (a *AuthController) s3BucketOwner(resource string) string {
	if a.state == nil {
		return ""
	}
	bucket, _, _ := strings.Cut(resource, "/")
	if bucket == "" || strings.ContainsAny(bucket, "*?") {
		return ""
	}
	data, err := a.state.Get(context.Background(), s3Namespace, "bucket:"+bucket)
	if err != nil || data == nil {
		return ""
	}
	var rec S3Bucket
	if json.Unmarshal(data, &rec) != nil {
		return ""
	}
	return rec.AccountID
}

// authzResourceAccountContext writes aws:ResourceAccount, and S3's own s3:ResourceAccount — "the
// resource owner AWS account ID" in the S3 condition-key table — for one resource, unless the
// resource's own context already carries it.
func (a *AuthController) authzResourceAccountContext(condCtx map[string]string, reqCtx *RequestContext,
	req *AWSRequest, resourceARN string,
) {
	if _, set := condCtx["aws:ResourceAccount"]; set {
		return
	}
	acct := a.authzResourceAccount(reqCtx, resourceARN)
	if acct == "" {
		return
	}
	condCtx["aws:ResourceAccount"] = acct
	if req.Service == "s3" {
		condCtx["s3:ResourceAccount"] = acct
	}
}

// ssmAuthzResourceARN returns the parameter an SSM request names, as its ARN, or "*" for a
// request naming none. A parameter ARN is "arn:aws:ssm:region:account:parameter/name"; a
// hierarchical name already begins with "/", which the ARN does not double. A Name that is
// itself an ARN is used as given.
//
// A public parameter AWS manages — one in the set the plugin answers without a stored record,
// [resolveManagedParameter]'s — has an ARN with an empty account, as AWS's own policies write it: the AWSPremiumSupport-
// TroubleshootEKSCluster runbook reference grants ssm:GetParameter on
// "arn:aws:ssm:::parameter/aws/service/eks/optimized-ami/...". Building it with the caller's
// account matched no such statement. The form is chosen from that managed set and never from the
// name's prefix alone: a stored parameter is the caller's whatever its name, and a name-only test
// would let a caller-created parameter shed its account — and every account-scoped Deny with it.
// PutParameter also refuses the reserved "aws" and "ssm" prefixes, as AWS does
// ([ssmReservedParameterName]).
func ssmAuthzResourceARN(req *AWSRequest, region, acct string) string {
	name := req.Params["Name"]
	if name == "" && len(req.Body) > 0 {
		var body struct {
			Name string `json:"Name"`
		}
		if json.Unmarshal(req.Body, &body) == nil {
			name = body.Name
		}
	}
	switch {
	case name == "":
		return "*"
	case strings.HasPrefix(name, "arn:"):
		return name
	}
	// Normalized as the plugin normalizes it before storing or reading, so "flat" and "/flat" name
	// one parameter and one ARN.
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	if _, managed := bundledImageForParameter(name); managed {
		return "arn:aws:ssm:" + region + "::parameter" + name
	}
	return "arn:aws:ssm:" + region + ":" + acct + ":parameter" + name
}
