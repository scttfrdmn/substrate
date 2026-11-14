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
// the caller had one of that name, RepositoryNotFoundException when it did not. An ARN is a fully
// qualified name, so the record it addresses is looked up where the ARN says it lives.
//
// What a well-formed ARN for a registry holding no such repository answers is substrate's reading,
// because no ECR page publishes a code for it: **RepositoryNotFoundException/400**, which all three
// tagging operations publish — "The specified repository could not be found … ensure that you are
// performing operations on the correct registry" — and which is the literal truth: the repository
// that ARN names does not exist. InvalidParameterException, the other published candidate, is kept
// for a string that is not an ECR repository ARN at all.
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

// ecrInvalidRepositoryARN reports a resourceArn that is not an ECR repository ARN.
func ecrInvalidRepositoryARN(arn string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "Invalid parameter at 'resourceArn' failed to satisfy constraint: '" + arn + "' is not an Amazon ECR repository ARN",
		HTTPStatus: http.StatusBadRequest,
	}
}
