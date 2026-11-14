package emulator

import (
	"net/http"
	"strings"
)

// ecrRepositoryARNResourcePrefix is the resource segment every ECR repository ARN carries before
// the repository name: arn:{partition}:ecr:{region}:{account}:repository/{name}.
const ecrRepositoryARNResourcePrefix = "repository/"

// ecrRepositoryTarget is a repository ARN taken apart: the registry it names, and the repository in
// it.
type ecrRepositoryTarget struct {
	AccountID string
	Region    string
	Name      string
}

// ecrParseRepositoryARN splits an ECR repository ARN into its account, Region and repository name,
// refusing a string that is not one with InvalidParameterException/400.
//
// **Everything after `repository/` is the name (#1465).** A repository name may contain `/` — the
// published repositoryName pattern (API_PutImage, API_BatchGetImage) is
// `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*`, and every pull-through
// cache repository is `<prefix>/<upstream-repo>` — so a parser that took the last path segment
// answered `ns/sub/repo`'s ARN as if it named `repo`, and ListTagsForResource,
// TagResource and UntagResource all refused a repository DescribeRepositories could find by name.
//
// **The account and Region are the ARN's, not the caller's (#1113).** The parser this replaces took
// a suffix and threw the rest away, and its caller built the state key from the request context, so
// an ARN naming another registry was served from the caller's own: the wrong repository tagged when
// the caller had one of that name, RepositoryNotFoundException when it did not. The parse keeps the
// ARN's scope so [ecrRequireLocalRepository] can refuse an ARN outside the caller's registry rather
// than silently re-scoping it.
//
// InvalidParameterException is kept for a string that is not an ECR repository ARN at all; the code
// for a well-formed ARN outside the caller's registry is argued at [ecrRequireLocalRepository].
//
// Refused as not an ECR repository ARN (InvalidParameterException): fewer than six `:`-separated
// segments, a first segment other than `arn`, a partition that is not an `aws` one, a service other
// than `ecr`, an empty Region, an account that is not twelve digits, a resource that does not begin
// `repository/`, and `repository/` with no name. ListTagsForResource publishes that "the only
// supported resource is an Amazon ECR repository", which is what makes a well-formed ARN of another
// type — a Lambda function's, say — a parameter error rather than a missing repository.
func ecrParseRepositoryARN(arn string) (ecrRepositoryTarget, *AWSError) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || !strings.HasPrefix(parts[1], "aws") || parts[2] != "ecr" {
		return ecrRepositoryTarget{}, ecrInvalidRepositoryARN(arn)
	}
	region, account, resource := parts[3], parts[4], parts[5]
	if region == "" || len(account) != 12 || strings.TrimLeft(account, "0123456789") != "" {
		return ecrRepositoryTarget{}, ecrInvalidRepositoryARN(arn)
	}
	name, ok := strings.CutPrefix(resource, ecrRepositoryARNResourcePrefix)
	if !ok || name == "" {
		return ecrRepositoryTarget{}, ecrInvalidRepositoryARN(arn)
	}
	return ecrRepositoryTarget{AccountID: account, Region: region, Name: name}, nil
}

// ecrRequireLocalRepository refuses a repository ARN that names a registry other than the caller's:
// another account, or the caller's account in another Region.
//
// **Another account.** ECR reaches across accounts only through a repository policy — "Resource-based
// policies let you grant usage permission to other accounts on a per-resource basis", and "when the
// principal and the resource are in different AWS accounts, you must also grant the principal entity
// permission" through an identity-based policy (ECR user guide, "How Amazon ECR works with IAM").
// Substrate stores a repository policy's text and evaluates none of it, so it cannot tell a
// cross-account TagResource AWS would allow from one it would deny, and no ECR page confirms that the
// three tagging operations are reachable cross-account at all. Serving the foreign repository would
// let any caller read and rewrite another account's tags with no permission check; refusing is the
// side that never grants what AWS would not. This is the arrangement [kmsRequireLocal] has for KMS.
//
// **Another Region.** An ECR endpoint is Regional and serves its own Region's registry; no page says a
// Regional endpoint resolves a repository ARN from a different Region, so this is refused on the same
// footing, which is also what #1113's acceptance criteria asked for.
//
// The code is RepositoryNotFoundException/400, published on all three tagging operations — "The
// specified repository could not be found. … ensure that you are performing operations on the
// correct registry". From the caller's side a repository it may not address is indistinguishable
// from one that is not there. The mapping is substrate's reading: no ECR page joins the case to a
// code.
func ecrRequireLocalRepository(ctx *RequestContext, target ecrRepositoryTarget) *AWSError {
	if target.AccountID == ctx.AccountID && target.Region == ctx.Region {
		return nil
	}
	return &AWSError{
		Code: "RepositoryNotFoundException",
		Message: "The repository with name '" + target.Name + "' does not exist in the registry with id '" + ctx.AccountID +
			"' in " + ctx.Region + "; the ARN names registry " + target.AccountID + " in " + target.Region,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrInvalidRepositoryARN reports a resourceArn that is not an ECR repository ARN.
func ecrInvalidRepositoryARN(arn string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "Invalid parameter at 'resourceArn' failed to satisfy constraint: '" + arn + "' is not an Amazon ECR repository ARN",
		HTTPStatus: http.StatusBadRequest,
	}
}
