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
// services allows, and of what CloudFormation itself accepts for an identifier
// no service ever sees.
type cfnNameConstraint struct {
	// maxLen is the longest name the service accepts, from that service's own
	// API reference rather than from CloudFormation's — except for a type whose
	// generated identifier is never sent to a service at all, where there is no
	// such limit to read and the bound comes from CloudFormation's own published
	// example instead (see the AWS::ApiGateway::Method entry).
	maxLen int

	// lower forces the whole name to lowercase, for a service whose names are
	// case-sensitive and lowercase-only (S3).
	lower bool

	// nameProp is the property a template names the resource with, whose absence is what makes
	// the deployer generate a name; empty for a type with no name property, whose identifier is
	// always generated (AWS::ApiGateway::Method).
	nameProp string
}

// cfnUsesGeneratedName reports whether a resource of resType declared with props is named by
// [cfnGeneratedName]: its type generates a name, and the template declares none, or declares
// one that resolves to nothing. It is what decides whether a replacement needs a fresh name
// ([cfnContext.mintReplacementName]); a template-named resource keeps its declared name, and a
// type outside cfnGeneratedNameTypes is named by its logical ID.
func cfnUsesGeneratedName(resType string, props map[string]interface{}, cctx *cfnContext) bool {
	c, ok := cfnGeneratedNameTypes[resType]
	if !ok {
		return false
	}
	if c.nameProp == "" {
		return true
	}
	return resolveStringProp(props, c.nameProp, "", cctx) == ""
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
//
// One entry is admitted on a different warrant: a type with no name property at
// all, whose identifier exists only in the CloudFormation layer and whose
// Template Reference publishes a generated one in this table's own shape. There
// the alternative is not "a name the template controls" but "an identifier that
// does not distinguish two resources in the same stack", which is what
// AWS::ApiGateway::Method had.
var cfnGeneratedNameTypes = map[string]cfnNameConstraint{
	// IAM names: "[\w+=,.@-]+", 64 for a role, 128 for a policy or an instance
	// profile (CreateRole, CreatePolicy, CreateInstanceProfile).
	"AWS::IAM::Role":            {maxLen: 64, nameProp: "RoleName"},
	"AWS::IAM::InstanceProfile": {maxLen: 128, nameProp: "InstanceProfileName"},
	"AWS::IAM::ManagedPolicy":   {maxLen: 128, nameProp: "ManagedPolicyName"},
	"AWS::IAM::User":            {maxLen: 64, nameProp: "UserName"},
	"AWS::IAM::Group":           {maxLen: 128, nameProp: "GroupName"},
	// AWS::IAM::Policy's PolicyName is required, so the generated value is not a name: it is the
	// physical ID of the inline policy the type embeds, which has no identifier of its own for the
	// stack to record (#1562). Every property is No interruption, so it is never replaced.
	"AWS::IAM::Policy": {maxLen: 128, nameProp: "PolicyName"},
	// CreateSAMLProvider's Name: "[\w._-]+", 1 to 128 characters.
	"AWS::IAM::SAMLProvider": {maxLen: 128, nameProp: "Name"},

	// A bucket name is globally unique, lowercase, and 3–63 characters
	// (validateBucketName enforces the same range).
	"AWS::S3::Bucket": {maxLen: 63, lower: true, nameProp: "BucketName"},

	// A DynamoDB table name is unique within a Region. CreateTable's own limit is
	// 1–1024 characters because the parameter also accepts a table ARN; the
	// name-only limit is the 3–255 in the DynamoDB service quotas, which is the
	// tighter of the two and so the one to fit.
	"AWS::DynamoDB::Table": {maxLen: 255, nameProp: "TableName"},

	// A queue name may be up to 80 characters (CreateQueue); a topic name up to
	// 256 (CreateTopic). Both creates are idempotent, so without a generated
	// name two stacks silently share one resource rather than colliding — see
	// #560.
	"AWS::SQS::Queue": {maxLen: 80, nameProp: "QueueName"},
	"AWS::SNS::Topic": {maxLen: 256, nameProp: "TopicName"},

	// A launch template name is unique within a Region and may be 3–128 characters of letters,
	// digits and -_./() (API_CreateLaunchTemplate's LaunchTemplateName rule, which EC2 enforces
	// as of #1507). The Template Reference says "If you do not specify a name, CloudFormation
	// creates a name for you"; a generated name's letters, digits and hyphens are inside the rule,
	// and the {stack}-{logical}-{suffix} shape is always longer than 3, so a short logical ID
	// such as LT no longer becomes a name EC2 refuses.
	"AWS::EC2::LaunchTemplate": {maxLen: 128, nameProp: "LaunchTemplateName"},

	// A log group name is unique within a Region and may be up to 512
	// characters (CreateLogGroup).
	"AWS::Logs::LogGroup": {maxLen: 512, nameProp: "LogGroupName"},

	// A dashboard name is unique within an account — dashboards are global — and is at most 255
	// characters of A-Z, a-z, 0-9, "-" and "_" (API_PutDashboard). The Template Reference says "If
	// you do not specify a name, one will be generated automatically"; a generated name's letters,
	// digits and hyphens are inside the rule.
	"AWS::CloudWatch::Dashboard": {maxLen: 255, nameProp: "DashboardName"},

	// Neither declares a name property, so CloudFormation always generates one (#1605). A cache
	// parameter group name is 1–255 letters, digits and hyphens, beginning with a letter, and
	// ElastiCache stores it lowercase (CreateCacheParameterGroup); an EFS creation token is 1–64
	// characters (CreateFileSystem). Named from the logical ID alone, two stacks declaring the
	// same logical ID shared one group or one file system, and deleting either stack deleted the
	// other's.
	"AWS::ElastiCache::ParameterGroup": {maxLen: 255, lower: true},
	"AWS::EFS::FileSystem":             {maxLen: 64},

	// A Lambda permission's statement ID is 1–100 of [a-zA-Z0-9-_] (AddPermission), and the type
	// declares no StatementId, so CloudFormation generates it; a queue policy's ID likewise
	// (#1605). The bound for the queue policy is substrate's: AWS publishes none.
	"AWS::Lambda::Permission": {maxLen: 100},
	"AWS::SQS::QueuePolicy":   {maxLen: 255},

	// A bare Lambda function name "is limited to 64 characters in length"
	// (CreateFunction).
	"AWS::Lambda::Function": {maxLen: 64, nameProp: "FunctionName"},

	// A Config rule name is unique within a Region and may be up to 128 characters
	// (ConfigRuleName, 1-128). CloudFormation documents generating one — the page's
	// own example is mystack-MyConfigRule-12ABCFPXHV4OV, the shape this table
	// produces. Its two sibling types are deliberately absent: AWS itself names a
	// configuration recorder and a delivery channel "default", and only one of each
	// may exist per account per Region, so a generated stack-scoped name would
	// replace a name the service assigns and could not collide with anything anyway.
	"AWS::Config::ConfigRule": {maxLen: 128, nameProp: "ConfigRuleName"},

	// A security group name is unique within its VPC, case-insensitively, and up to 255
	// characters (API_CreateSecurityGroup's GroupName). It joined this table when substrate began
	// enforcing that uniqueness (#1491): with the logical ID as the name, two stacks of one
	// template in one VPC — the default VPC, for a template naming none — collided with
	// InvalidGroup.Duplicate where "If you don't specify a name, CloudFormation generates a
	// unique physical ID to name the resource" (CloudFormation resource custom naming).
	"AWS::EC2::SecurityGroup": {maxLen: 255, nameProp: "GroupName"},

	// AWS::ApiGateway::Method is the one entry with no name property at all, and
	// so the one whose bound does not come from a service limit. API Gateway's
	// REST API has no method identifier — API_Method carries the eleven
	// configuration fields and no id, and API_PutMethod returns none — so nothing
	// is ever sent to a service for validation and there is no service-side
	// maximum to fit. The identifier exists only in the CloudFormation layer, and
	// the Template Reference publishes it: Ref "returns the method ID, such as
	// mysta-metho-01234b567890example". That example is this table's shape, it is
	// 31 characters, and it is lowercase — so 31 and lower are taken from the
	// example, the only evidence AWS publishes, rather than from a limit. Without
	// an entry here every method in a stack is identified by its HTTP verb, so two
	// methods with the same verb on different resources are indistinguishable
	// (#843).
	"AWS::ApiGateway::Method": {maxLen: 31, lower: true},
}

// cfnGeneratedNameSuffixLen is the length of the derived suffix, matching the
// twelve characters in CloudFormation's own documented example of a generated
// physical ID (MyStack-MyBucket-abcdefghijk1).
const cfnGeneratedNameSuffixLen = 12

// cfnNameSuffixAlphabet is the alphabet of a generated name's suffix: base 36 in lowercase, which
// is what the derived suffix renders in, so a minted suffix is indistinguishable in shape and fits
// a lowercase-only name.
const cfnNameSuffixAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

// cfnGeneratedName returns the physical name to use for a resource of resType
// whose template omitted its name property, or logicalID unchanged when resType
// has no entry in cfnGeneratedNameTypes.
//
// The name is {stackName}-{logicalID}-{suffix}, the shape CloudFormation
// documents. The suffix is *derived* — FNV-64a over the account, Region, stack
// name and logical ID — where CloudFormation uses a random one. That divergence
// is deliberate and load-bearing. Through v0.127.0 UpdateStack re-created every
// resource, so a name regenerated per deploy minted a fresh resource on every
// update and leaked the one it replaced. An unchanged resource is now kept (#1427),
// but a changed one whose type has no update path is still re-created, and its
// generated name must come out the same for the re-create to be the same resource
// rather than a replacement. Deriving it makes the name stable for a given (account, Region,
// stack, logical ID) while still differing between stacks, which is the property
// #560 needs, and keeps the emulator deterministic by construction.
//
// A **replacement** is the exception, and the one place the suffix is minted rather
// than derived (#1545). A change to an "Update requires: Replacement" property creates
// a new resource while the old one still exists, so the derived name is taken; the
// deployer mints the replacement a suffix through the request's [IDMint]
// ([cfnContext.mintReplacementName]), which a replay of the request mints again. The
// name a resource is first created under is unchanged.
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
	if cctx != nil {
		if minted, ok := cctx.nameSuffixes[logicalID]; ok {
			suffix = minted
		}
	}

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
