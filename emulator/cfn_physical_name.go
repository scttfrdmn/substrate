package emulator

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// cfnNameConstraint describes what a service will accept as a physical name, so
// a generated one can be trimmed to fit before the create call is made rather
// than failing validation at the plugin. Only two things vary between the
// services in cfnGeneratedNameTypes: how long the name may be, and whether it
// must be lowercase. The character set does not vary, because the set
// CloudFormation documents for a generated name — "must begin with a letter;
// contain only ASCII letters, digits, and hyphens; and not end with a hyphen or
// contain two consecutive hyphens" — is a subset of what every one of those
// services allows.
type cfnNameConstraint struct {
	// maxLen is the longest name the service accepts, from that service's own
	// API reference rather than from CloudFormation's.
	maxLen int

	// lower forces the whole name to lowercase, for a service whose names are
	// case-sensitive and lowercase-only (S3).
	lower bool
}

// cfnGeneratedNameTypes maps a resource type to the constraint on the physical
// name substrate generates for it when the template does not supply one.
//
// A type belongs here only when its physical name is unique across an account
// (or an account and Region) and the service invents one when the template
// omits it. That is deliberately narrower than "every property that falls back
// to the logical ID": most such properties are not account-unique names at all.
// AWS::ApiGateway::Resource's PathPart is a URL segment unique only within its
// parent, and Domain, SecretId, ClusterId and ReplicationGroupId are
// identifiers a template legitimately controls — generating those would change
// the URLs and identifiers a consumer wrote the template to get. A type absent
// from this table keeps the older behavior of using the logical ID verbatim.
var cfnGeneratedNameTypes = map[string]cfnNameConstraint{
	// IAM names: "[\w+=,.@-]+", 64 for a role, 128 for a policy or an instance
	// profile (CreateRole, CreatePolicy, CreateInstanceProfile).
	"AWS::IAM::Role":            {maxLen: 64},
	"AWS::IAM::InstanceProfile": {maxLen: 128},
	// AWS::IAM::Policy declares PolicyName as required, so a template that omits
	// it is already invalid — but substrate deploys the type through CreatePolicy,
	// which mints an account-unique managed policy, so the omitted case still has
	// to generate rather than collide.
	"AWS::IAM::Policy": {maxLen: 128},

	// A bucket name is globally unique, lowercase, and 3–63 characters
	// (validateBucketName enforces the same range).
	"AWS::S3::Bucket": {maxLen: 63, lower: true},

	// A DynamoDB table name is unique within a Region. CreateTable's own limit is
	// 1–1024 characters because the parameter also accepts a table ARN; the
	// name-only limit is the 3–255 in the DynamoDB service quotas, which is the
	// tighter of the two and so the one to fit.
	"AWS::DynamoDB::Table": {maxLen: 255},

	// A queue name may be up to 80 characters (CreateQueue); a topic name up to
	// 256 (CreateTopic). Both creates are idempotent, so without a generated
	// name two stacks silently share one resource rather than colliding — see
	// #560.
	"AWS::SQS::Queue": {maxLen: 80},
	"AWS::SNS::Topic": {maxLen: 256},

	// A log group name is unique within a Region and may be up to 512
	// characters (CreateLogGroup).
	"AWS::Logs::LogGroup": {maxLen: 512},

	// A bare Lambda function name "is limited to 64 characters in length"
	// (CreateFunction).
	"AWS::Lambda::Function": {maxLen: 64},
}

// cfnGeneratedNameSuffixLen is the length of the derived suffix, matching the
// twelve characters in CloudFormation's own documented example of a generated
// physical ID (MyStack-MyBucket-abcdefghijk1).
const cfnGeneratedNameSuffixLen = 12

// cfnGeneratedName returns the physical name to use for a resource of resType
// whose template omitted its name property, or logicalID unchanged when resType
// has no entry in cfnGeneratedNameTypes.
//
// The name is {stackName}-{logicalID}-{suffix}, the shape CloudFormation
// documents. The suffix is *derived* — FNV-64a over the account, Region, stack
// name and logical ID — where CloudFormation uses a random one. That divergence
// is deliberate and load-bearing: UpdateStack in substrate is a re-Deploy of the
// whole template (see clearUnchangedRedeploys), so a name regenerated per
// deploy would mint a fresh resource on every update and leak the one it
// replaced. Deriving it makes the name stable for a given (account, Region,
// stack, logical ID) while still differing between stacks, which is the property
// #560 needs, and keeps the emulator deterministic by construction.
//
// The account and Region are in the hash because two same-named stacks in
// different Regions are different stacks, which sameScope already treats them
// as.
func cfnGeneratedName(cctx *cfnContext, resType, logicalID string) string {
	c, ok := cfnGeneratedNameTypes[resType]
	if !ok {
		return logicalID
	}

	stackName, region, accountID := "", "", ""
	if cctx != nil {
		stackName, region, accountID = cctx.stackName, cctx.region, cctx.accountID
	}

	suffix := cfnNameSuffix(accountID, region, stackName, logicalID)

	// A generated name must begin with a letter. The stack segment leads, so the
	// guarantee is applied there, before the length budget is spent — prefixing
	// afterwards would either overrun maxLen or eat into the suffix.
	stack := cfnNameSegment(stackName)
	if stack == "" || !isASCIILetter(stack[0]) {
		stack = "s" + stack
	}
	logical := cfnNameSegment(logicalID)
	if logical == "" {
		logical = "r"
	}

	// Two hyphens plus the suffix are reserved, and the suffix is never
	// truncated: it is the only part of the name that makes it unique.
	budget := c.maxLen - len(suffix) - 2
	if budget < 2 {
		// No service in the table is this tight; keep the suffix rather than the
		// segments if one ever is, because uniqueness matters more than legibility.
		return maybeLower(suffix[:min(len(suffix), c.maxLen)], c.lower)
	}
	stack, logical = fitSegments(stack, logical, budget)

	return maybeLower(stack+"-"+logical+"-"+suffix, c.lower)
}

// cfnNameSuffix derives the opaque, stable tail of a generated physical name.
// The value is taken modulo 36^cfnGeneratedNameSuffixLen and zero-padded so
// every suffix is the same width, which keeps the length budget in
// cfnGeneratedName exact rather than approximate.
func cfnNameSuffix(accountID, region, stackName, logicalID string) string {
	h := fnv.New64a()
	// Errors are impossible: hash.Hash.Write never returns one.
	_, _ = h.Write([]byte(accountID + "/" + region + "/" + stackName + "/" + logicalID))

	mod := uint64(1)
	for i := 0; i < cfnGeneratedNameSuffixLen; i++ {
		mod *= 36
	}
	s := strconv.FormatUint(h.Sum64()%mod, 36)
	if pad := cfnGeneratedNameSuffixLen - len(s); pad > 0 {
		s = strings.Repeat("0", pad) + s
	}
	return s
}

// cfnNameSegment reduces one component of a generated name to the documented
// character set: ASCII letters, digits and hyphens, with no leading, trailing or
// doubled hyphen. Every other byte becomes a hyphen, so a logical ID and a stack
// name remain recognizable in the result.
func cfnNameSegment(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case isASCIILetter(ch) || (ch >= '0' && ch <= '9'):
			b.WriteByte(ch)
		default:
			// Collapse a run of unusable bytes into a single hyphen.
			if cur := b.String(); cur != "" && !strings.HasSuffix(cur, "-") {
				b.WriteByte('-')
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// fitSegments shrinks stack and logical so their combined length is at most
// budget, dividing the cut between them in proportion to their lengths rather
// than sacrificing one entirely — both are what makes a generated name
// recognizable. Each keeps at least one character.
func fitSegments(stack, logical string, budget int) (string, string) {
	total := len(stack) + len(logical)
	if total <= budget {
		return stack, logical
	}

	stackWidth := budget * len(stack) / total
	stackWidth = max(1, min(stackWidth, budget-1))
	logicalWidth := budget - stackWidth

	stack = strings.Trim(stack[:min(stackWidth, len(stack))], "-")
	logical = strings.Trim(logical[:min(logicalWidth, len(logical))], "-")
	if stack == "" {
		stack = "s"
	}
	if logical == "" {
		logical = "r"
	}
	return stack, logical
}

// isASCIILetter reports whether ch is an unaccented A–Z or a–z.
func isASCIILetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

// maybeLower lowercases s when the constraint demands it.
func maybeLower(s string, lower bool) string {
	if lower {
		return strings.ToLower(s)
	}
	return s
}
