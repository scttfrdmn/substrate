package emulator_test

// ResourceTypeFilters: a resource type is the ARN's own segment, not a prefix of it (#936).
//
// AWS is explicit about both halves of what a filter means. "Specifying a resource type of
// ec2:instance returns *only* EC2 instances", and "[t]he string for each service name and resource
// type is the same as that embedded in a resource's Amazon Resource Name (ARN)" (GetResources). Taken
// together those two sentences say the type is delimited by the ARN itself.
//
// matchesResourceType compared it as an unanchored prefix instead, which broke the first sentence in
// both directions. Too wide: "ecs:task" selected an ARN whose resource portion is
// "task-definition/{family}:{revision}", because that string begins with "task" — so a caller could
// not express "tasks only" at all. Too narrow: "apigateway:restapis" selected nothing, because API
// Gateway's resource portion begins with a slash ("/restapis/{id}"), which a prefix comparison against
// "restapis" cannot see past. The second was known and worked around in a comment rather than fixed.
//
// This is #910's anchored-segment rule applied one layer up. #910 and #918 fixed
// strings.Contains(arn, ":stateMachine:") and strings.LastIndex(arn, "distribution/") in the
// resolvers, on the ground that a resource type must be matched against the ARN's own delimited
// segment; the filter matcher is the one comparison of that kind whose left-hand side comes from the
// caller, and it was not part of either pass.
//
// The cases below are one per ARN shape substrate scans, which is what makes the fix a rule rather
// than an ECS patch: "/" and ":" both delimit a type across AWS's own formats, some ARNs carry a
// leading slash, and S3's carries no type string at all.

import (
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTaggingTypeFilter_ATypeIsTheARNsOwnSegment covers each ARN shape in the scanned set, from both
// sides: the filter that must select the ARN, and a filter that is a strict prefix of its type and
// must not.
func TestTaggingTypeFilter_ATypeIsTheARNsOwnSegment(t *testing.T) {
	for _, tc := range []struct {
		name string
		arn  string
		// match is a filter that must select arn; miss is one that must not.
		match []string
		miss  []string
	}{
		{
			name:  "slash-delimited type",
			arn:   "arn:aws:ec2:us-east-1:123456789012:instance/i-abc123",
			match: []string{"ec2", "ec2:instance"},
			miss:  []string{"ec2:inst", "ec2:instances", "ecs:instance"},
		},
		{
			name:  "colon-delimited type",
			arn:   "arn:aws:states:us-east-1:123456789012:stateMachine:hello",
			match: []string{"states", "states:stateMachine"},
			miss:  []string{"states:state", "states:activity"},
		},
		{
			name: "slash-delimited type whose id carries a colon",
			arn:  "arn:aws:ecs:us-east-1:123456789012:task-definition/sidecar:3",
			// The pair that made the old behavior observable: "task" is a strict prefix of
			// "task-definition", so before #936 asking for tasks answered with this too.
			match: []string{"ecs", "ecs:task-definition"},
			miss:  []string{"ecs:task", "ecs:task-def"},
		},
		{
			name:  "the task the previous row used to shadow",
			arn:   "arn:aws:ecs:us-east-1:123456789012:task/web/0123456789abcdef",
			match: []string{"ecs", "ecs:task"},
			miss:  []string{"ecs:task-definition", "ecs:tas"},
		},
		{
			name: "type behind a leading slash",
			arn:  "arn:aws:apigateway:us-east-1::/restapis/abc123",
			// AWS publishes this ARN as arn:{partition}:apigateway:{region}::/restapis/{api-id}, so
			// the string embedded in it is "restapis" and the slash is a path separator.
			match: []string{"apigateway", "apigateway:restapis"},
			miss:  []string{"apigateway:restapi", "apigateway:/restapis"},
		},
		{
			name: "no type embedded at all",
			arn:  "arn:aws:s3:::bucket-logs",
			// An S3 bucket ARN embeds no type string, so there is nothing for "s3:bucket" to be "the
			// same as" and the service-only filter is the only one that reaches it. It used to match
			// "s3:bucket" by accident of the name's first six characters.
			match: []string{"s3", "s3:bucket-logs"},
			miss:  []string{"s3:bucket", "s3:object"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, f := range tc.match {
				assert.True(t, emulator.ResourceTypeMatchesForTest(tc.arn, []string{f}),
					"filter %q selects %s", f, tc.arn)
			}
			for _, f := range tc.miss {
				assert.False(t, emulator.ResourceTypeMatchesForTest(tc.arn, []string{f}),
					"filter %q does not select %s", f, tc.arn)
			}
		})
	}
}

// TestTaggingTypeFilter_EveryReachableTypeIsSelectedByItsOwnName is the coverage criterion #909 asks
// for: one ARN per resource type the tagging API reaches, rather than one per ARN *shape*.
//
// The shape cases above prove the rule; this proves the rule covers the tree. Each row asserts both
// halves of what a filter means — the `service:type` filter selects the ARN, and the service-only
// filter selects it too, since a service-only filter is the one a caller reaches for when a type
// embeds no name of its own. Everything the twenty-eight `GetResources` scanners produce is here —
// twenty-nine types, because scanIAMEntities yields both a user and a role — plus the four that only
// `TagResources`/`UntagResources` reach, because a filter is applied to whatever ARN the caller was
// given and the two sets are allowed to differ.
//
// A row that is added to a scanner without being added here is the drift this exists to catch: the
// count is asserted, so a twenty-ninth type fails the test rather than going unexercised.
func TestTaggingTypeFilter_EveryReachableTypeIsSelectedByItsOwnName(t *testing.T) {
	const (
		acct = "123456789012"
		reg  = "us-east-1"
	)
	rows := []struct {
		service string
		rtype   string
		arn     string
	}{
		// No type segment at all: the whole resource portion is the type, so only the service-only
		// filter is a caller's realistic reach. The rtype column is what the rule yields.
		{"s3", "my-bucket", "arn:aws:s3:::my-bucket"},
		{"sqs", "my-queue", "arn:aws:sqs:" + reg + ":" + acct + ":my-queue"},
		{"sns", "my-topic", "arn:aws:sns:" + reg + ":" + acct + ":my-topic"},

		// Colon-delimited.
		{"lambda", "function", "arn:aws:lambda:" + reg + ":" + acct + ":function:my-fn"},
		{"states", "stateMachine", "arn:aws:states:" + reg + ":" + acct + ":stateMachine:hello"},
		{"states", "activity", "arn:aws:states:" + reg + ":" + acct + ":activity:worker"},
		{"rds", "db", "arn:aws:rds:" + reg + ":" + acct + ":db:orders"},
		{"rds", "cluster", "arn:aws:rds:" + reg + ":" + acct + ":cluster:orders"},
		{"rds", "subgrp", "arn:aws:rds:" + reg + ":" + acct + ":subgrp:default"},
		{"elasticache", "cluster", "arn:aws:elasticache:" + reg + ":" + acct + ":cluster:cache-1"},
		{"secretsmanager", "secret", "arn:aws:secretsmanager:" + reg + ":" + acct + ":secret:prod/db"},

		// Slash-delimited.
		{"dynamodb", "table", "arn:aws:dynamodb:" + reg + ":" + acct + ":table/orders"},
		{"ec2", "instance", "arn:aws:ec2:" + reg + ":" + acct + ":instance/i-abc123"},
		{"iam", "user", "arn:aws:iam::" + acct + ":user/alice"},
		{"iam", "role", "arn:aws:iam::" + acct + ":role/app"},
		{"ecr", "repository", "arn:aws:ecr:" + reg + ":" + acct + ":repository/app"},
		{"ecs", "cluster", "arn:aws:ecs:" + reg + ":" + acct + ":cluster/web"},
		{"ecs", "service", "arn:aws:ecs:" + reg + ":" + acct + ":service/web/api"},
		{"ecs", "task", "arn:aws:ecs:" + reg + ":" + acct + ":task/web/0123456789abcdef"},
		{"ecs", "task-definition", "arn:aws:ecs:" + reg + ":" + acct + ":task-definition/sidecar:3"},
		{"cognito-idp", "userpool", "arn:aws:cognito-idp:" + reg + ":" + acct + ":userpool/us-east-1_abc"},
		{"apigateway", "restapis", "arn:aws:apigateway:" + reg + "::/restapis/abc123"},
		{"kinesis", "stream", "arn:aws:kinesis:" + reg + ":" + acct + ":stream/orders"},
		{"elasticfilesystem", "file-system", "arn:aws:elasticfilesystem:" + reg + ":" + acct + ":file-system/fs-abc123"},
		{"elasticfilesystem", "access-point", "arn:aws:elasticfilesystem:" + reg + ":" + acct + ":access-point/fsap-abc123"},
		{"glue", "database", "arn:aws:glue:" + reg + ":" + acct + ":database/analytics"},
		{"glue", "job", "arn:aws:glue:" + reg + ":" + acct + ":job/nightly-etl"},
		{"glue", "crawler", "arn:aws:glue:" + reg + ":" + acct + ":crawler/inventory"},
		{"glue", "connection", "arn:aws:glue:" + reg + ":" + acct + ":connection/warehouse"},
		{"kms", "key", "arn:aws:kms:" + reg + ":" + acct + ":key/abcd-1234"},
		{"acm", "certificate", "arn:aws:acm:" + reg + ":" + acct + ":certificate/abc-123"},

		// The parameter name's own leading "/" is the delimiter, so a hierarchical name does not make
		// a second type segment.
		{"ssm", "parameter", "arn:aws:ssm:" + reg + ":" + acct + ":parameter/app/db/password"},

		// No Region segment, because CloudFront is global — which the rule does not care about, since
		// it reads parts[5] either way.
		{"cloudfront", "distribution", "arn:aws:cloudfront::" + acct + ":distribution/E15MNIMTCFKK4C"},
	}

	// Locked so a new scanner or resolver arm has to be added here too: twenty-nine types across the
	// twenty-eight scanners, plus an EFS access point and Glue's job, crawler and connection, which
	// only the named-ARN operations reach.
	require.Len(t, rows, 33, "one row per reachable resource type")

	for _, r := range rows {
		t.Run(r.service+":"+r.rtype, func(t *testing.T) {
			assert.True(t, emulator.ResourceTypeMatchesForTest(r.arn, []string{r.service + ":" + r.rtype}),
				"the type filter selects it")
			assert.True(t, emulator.ResourceTypeMatchesForTest(r.arn, []string{r.service}),
				"and so does the service-only filter")
		})
	}
}

// TestTaggingTypeFilter_ARDSClusterFilterDoesNotSelectAClusterParameterGroup is #909's prospective
// case, asserted on the matcher because neither type is storable.
//
// AWS's RDS ARN table gives `cluster-pg` and `cluster-snapshot` as resource types of their own, both
// of which begin with the string `cluster`. Nothing in substrate can be stored under either — the RDS
// resolver refuses both segments — so a wire assertion is impossible, and a filter is wrong
// independently of whether a record exists. This is the row that fails the moment either type is
// stored, which is the point of pinning it before then.
func TestTaggingTypeFilter_ARDSClusterFilterDoesNotSelectAClusterParameterGroup(t *testing.T) {
	const base = "arn:aws:rds:us-east-1:123456789012:"
	assert.True(t, emulator.ResourceTypeMatchesForTest(base+"cluster:orders", []string{"rds:cluster"}),
		"a DB cluster is selected")
	for _, resource := range []string{"cluster-pg:custom-pg", "cluster-snapshot:nightly", "cluster-endpoint:reader"} {
		assert.False(t, emulator.ResourceTypeMatchesForTest(base+resource, []string{"rds:cluster"}),
			"rds:cluster does not select %s", resource)
	}
}

// TestTaggingTypeFilter_ADifferentServiceNeverMatches keeps the service check ahead of the type check.
// "ecs:cluster" and "elasticache:cluster" share a type string, and only the service tells them apart.
func TestTaggingTypeFilter_ADifferentServiceNeverMatches(t *testing.T) {
	const ecsCluster = "arn:aws:ecs:us-east-1:123456789012:cluster/web"
	assert.True(t, emulator.ResourceTypeMatchesForTest(ecsCluster, []string{"ecs:cluster"}))
	assert.False(t, emulator.ResourceTypeMatchesForTest(ecsCluster, []string{"elasticache:cluster"}))
	assert.False(t, emulator.ResourceTypeMatchesForTest(ecsCluster, []string{"elasticache"}))
}

// TestTaggingTypeFilter_AnyFilterInTheListSelects holds the OR across the list, which is what makes a
// multi-type request one call rather than several.
func TestTaggingTypeFilter_AnyFilterInTheListSelects(t *testing.T) {
	const task = "arn:aws:ecs:us-east-1:123456789012:task/web/0123456789abcdef"
	assert.True(t, emulator.ResourceTypeMatchesForTest(task, []string{"ecs:service", "ecs:task"}),
		"the second filter selects it")
	assert.False(t, emulator.ResourceTypeMatchesForTest(task, []string{"ecs:service", "ecs:cluster"}),
		"neither does")
	assert.False(t, emulator.ResourceTypeMatchesForTest(task, nil),
		"an empty list selects nothing; GetResources treats that as no filter before calling this")
}

// TestTaggingTypeFilter_AMalformedARNSelectsNothing pins the arity guard. An ARN with fewer than six
// colon-separated fields has no resource portion to read, and answering true would report a record
// substrate could not have keyed.
func TestTaggingTypeFilter_AMalformedARNSelectsNothing(t *testing.T) {
	for _, arn := range []string{
		"",
		"not-an-arn",
		"arn:aws:ecs",
		"arn:aws:ecs:us-east-1:123456789012",
	} {
		require.False(t, emulator.ResourceTypeMatchesForTest(arn, []string{"ecs", "ecs:task"}),
			"malformed ARN %q selects nothing", arn)
	}
}
