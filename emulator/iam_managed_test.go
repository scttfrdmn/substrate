package emulator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestListManagedPolicies_Count(t *testing.T) {
	policies := emulator.ListManagedPolicies()
	assert.Len(t, policies, 52, "expected exactly 52 bundled managed policies")
}

// TestListManagedPolicies_ARNFormat asserts each ARN is the policy's Path and PolicyName
// composed, which is the invariant that survived #484's addition of paths.
//
// It previously asserted the ARN suffix was the bare PolicyName, which held only while
// every bundled policy sat at "/". A `service-role/` policy carries its path in the ARN and
// not in PolicyName — the distinction ListPolicies --path-prefix reads.
func TestListManagedPolicies_ARNFormat(t *testing.T) {
	policies := emulator.ListManagedPolicies()
	for _, p := range policies {
		assert.True(t,
			strings.HasPrefix(p.ARN, "arn:aws:iam::aws:policy/"),
			"policy %s has unexpected ARN %s", p.PolicyName, p.ARN,
		)
		suffix := strings.TrimPrefix(p.ARN, "arn:aws:iam::aws:policy/")
		wantSuffix := strings.TrimPrefix(p.Path, "/") + p.PolicyName
		assert.Equal(t, wantSuffix, suffix,
			"the ARN suffix should be Path+PolicyName for %s", p.PolicyName)
		assert.NotContains(t, p.PolicyName, "/",
			"PolicyName must be the bare name; the path is a separate field (%s)", p.PolicyName)
	}
}

func TestListManagedPolicies_UniqueARNs(t *testing.T) {
	policies := emulator.ListManagedPolicies()
	seen := make(map[string]bool, len(policies))
	for _, p := range policies {
		assert.False(t, seen[p.ARN], "duplicate ARN: %s", p.ARN)
		seen[p.ARN] = true
	}
}

func TestListManagedPolicies_UniqueIDs(t *testing.T) {
	policies := emulator.ListManagedPolicies()
	seen := make(map[string]bool, len(policies))
	for _, p := range policies {
		assert.False(t, seen[p.PolicyID], "duplicate PolicyID: %s", p.PolicyID)
		seen[p.PolicyID] = true
	}
}

func TestListManagedPolicies_RequiredFields(t *testing.T) {
	policies := emulator.ListManagedPolicies()
	for _, p := range policies {
		assert.NotEmpty(t, p.PolicyName, "PolicyName missing")
		assert.NotEmpty(t, p.PolicyID, "PolicyID missing for %s", p.PolicyName)
		assert.NotEmpty(t, p.ARN, "ARN missing for %s", p.PolicyName)
		// A path is "/" or "/name/" — the service-role policies added in #484 are the
		// latter. Both forms open and close with a slash, which is what IAM reports.
		assert.True(t, strings.HasPrefix(p.Path, "/") && strings.HasSuffix(p.Path, "/"),
			"Path %q should be slash-delimited for %s", p.Path, p.PolicyName)
		assert.True(t, p.IsAttachable, "IsAttachable should be true for %s", p.PolicyName)
		assert.NotEmpty(t, p.DefaultVersionID, "DefaultVersionId missing for %s", p.PolicyName)
	}
}

func TestGetManagedPolicy_AdministratorAccess(t *testing.T) {
	p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/AdministratorAccess")
	require.True(t, ok)
	assert.Equal(t, "AdministratorAccess", p.PolicyName)
	require.Len(t, p.Document.Statement, 1)
	stmt := p.Document.Statement[0]
	assert.Equal(t, emulator.IAMEffectAllow, stmt.Effect)
	assert.Contains(t, []string(stmt.Action), "*")
	assert.Contains(t, []string(stmt.Resource), "*")
}

func TestGetManagedPolicy_ReadOnlyAccess(t *testing.T) {
	p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/ReadOnlyAccess")
	require.True(t, ok)
	assert.Equal(t, "ReadOnlyAccess", p.PolicyName)
	assert.NotEmpty(t, p.Document.Statement)
}

func TestGetManagedPolicy_PowerUserAccess(t *testing.T) {
	p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/PowerUserAccess")
	require.True(t, ok)
	assert.Equal(t, "PowerUserAccess", p.PolicyName)

	// PowerUserAccess must use NotAction in at least one statement.
	hasNotAction := false
	for _, stmt := range p.Document.Statement {
		if len(stmt.NotAction) > 0 {
			hasNotAction = true
			break
		}
	}
	assert.True(t, hasNotAction, "PowerUserAccess should have at least one NotAction statement")
}

func TestGetManagedPolicy_NotFound(t *testing.T) {
	_, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/NonExistentPolicy")
	assert.False(t, ok)
}

func TestGetManagedPolicy_Concurrent(t *testing.T) {
	// Verify the sync.Once lookup map is safe for concurrent use.
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/AdministratorAccess")
			assert.True(t, ok)
			assert.NotNil(t, p)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// --- Service-role policies (#484) ------------------------------------------

// iamServiceRolePolicies are the five policies #484 asks for: the ones an instance
// profile or an execution role carries, as opposed to the human-operator policies the
// catalog held before. Path is the value IAM reports, not a component of PolicyName.
var iamServiceRolePolicies = []struct {
	arn      string
	name     string
	path     string
	policyID string
	version  string
}{
	{
		arn:      "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
		name:     "AmazonSSMManagedInstanceCore",
		path:     "/",
		policyID: "ANPAIXSHM2BNB2D3AXXRU",
		version:  "v2",
	},
	{
		arn:      "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
		name:     "AmazonEC2ContainerRegistryReadOnly",
		path:     "/",
		policyID: "ANPAIFYZPA37OOHVIH7KQ",
		version:  "v3",
	},
	{
		arn:      "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
		name:     "AmazonECSTaskExecutionRolePolicy",
		path:     "/service-role/",
		policyID: "ANPAJG4T4G4PV56DE72PY",
		version:  "v1",
	},
	{
		arn:      "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
		name:     "AWSLambdaBasicExecutionRole",
		path:     "/service-role/",
		policyID: "ANPAJNCQGXC42545SKXIK",
		version:  "v1",
	},
	{
		arn:      "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
		name:     "AWSLambdaVPCAccessExecutionRole",
		path:     "/service-role/",
		policyID: "ANPAJVTME3YLVNL72YR2K",
		version:  "v3",
	},
}

// TestGetManagedPolicy_ServiceRolePolicies asserts each of the five resolves in process
// with the metadata AWS reports and a non-empty document.
//
// The document is asserted here rather than over the wire because it is not observable
// over the wire at all: GetPolicy returns metadata only, and GetPolicyVersion — the one
// operation that returns a document — is unimplemented (#498). The documents still matter
// in process: they are what the IAM evaluator reads.
func TestGetManagedPolicy_ServiceRolePolicies(t *testing.T) {
	for _, tc := range iamServiceRolePolicies {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := emulator.GetManagedPolicy(tc.arn)
			require.True(t, ok, "%s should resolve", tc.arn)

			assert.Equal(t, tc.name, p.PolicyName)
			assert.Equal(t, tc.path, p.Path)
			assert.Equal(t, tc.policyID, p.PolicyID)
			assert.Equal(t, tc.version, p.DefaultVersionID)
			assert.NotEmpty(t, p.Description, "Description missing for %s", tc.name)

			require.NotEmpty(t, p.Document.Statement, "document should not be empty")
			assert.Equal(t, "2012-10-17", p.Document.Version)
			for i, stmt := range p.Document.Statement {
				assert.Equal(t, emulator.IAMEffectAllow, stmt.Effect,
					"statement %d of %s should Allow", i, tc.name)
				assert.NotEmpty(t, stmt.Action, "statement %d of %s has no Action", i, tc.name)
				assert.NotEmpty(t, stmt.Resource, "statement %d of %s has no Resource", i, tc.name)
			}
		})
	}
}

// TestGetManagedPolicy_ServiceRolePath asserts a service-role policy keeps the path out
// of PolicyName. ListPolicies --path-prefix reads Path; a name carrying "service-role/"
// would be findable by ARN and invisible to a path query.
func TestGetManagedPolicy_ServiceRolePath(t *testing.T) {
	p, ok := emulator.GetManagedPolicy(
		"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
	require.True(t, ok)

	assert.Equal(t, "/service-role/", p.Path)
	assert.Equal(t, "AWSLambdaBasicExecutionRole", p.PolicyName)
	assert.NotContains(t, p.PolicyName, "service-role")
}

// TestGetManagedPolicy_SSMCoreDocument pins the AmazonSSMManagedInstanceCore document
// against its AWS managed-policy reference page: three Allow statements over the ssm,
// ssmmessages and ec2messages prefixes on "*". This is the policy #484 reproduces with,
// and the one an SSM-managed instance profile cannot work without.
func TestGetManagedPolicy_SSMCoreDocument(t *testing.T) {
	p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
	require.True(t, ok)
	require.Len(t, p.Document.Statement, 3)

	byPrefix := map[string]int{}
	for _, stmt := range p.Document.Statement {
		assert.Equal(t, []string{"*"}, []string(stmt.Resource))
		for _, action := range stmt.Action {
			prefix, _, found := strings.Cut(action, ":")
			require.True(t, found, "action %q should be prefix:Name", action)
			byPrefix[prefix]++
		}
	}

	assert.Equal(t, map[string]int{"ssm": 15, "ssmmessages": 4, "ec2messages": 6}, byPrefix)
}

// TestGetManagedPolicy_ServiceRoleEvaluation asserts the seeded documents evaluate, which
// is the reason the catalog carries documents at all.
func TestGetManagedPolicy_ServiceRoleEvaluation(t *testing.T) {
	tests := []struct {
		name   string
		arn    string
		action string
		want   string
	}{
		{
			name:   "SSM core allows UpdateInstanceInformation",
			arn:    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
			action: "ssm:UpdateInstanceInformation",
			want:   emulator.DecisionAllow,
		},
		{
			name:   "SSM core does not allow s3:GetObject",
			arn:    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
			action: "s3:GetObject",
			want:   emulator.DecisionImplicitDeny,
		},
		{
			name:   "Lambda basic execution allows PutLogEvents",
			arn:    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			action: "logs:PutLogEvents",
			want:   emulator.DecisionAllow,
		},
		{
			name:   "Lambda basic execution does not allow ec2:CreateNetworkInterface",
			arn:    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			action: "ec2:CreateNetworkInterface",
			want:   emulator.DecisionImplicitDeny,
		},
		{
			name:   "Lambda VPC access allows ec2:CreateNetworkInterface",
			arn:    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
			action: "ec2:CreateNetworkInterface",
			want:   emulator.DecisionAllow,
		},
		{
			name:   "ECR read-only allows BatchGetImage",
			arn:    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
			action: "ecr:BatchGetImage",
			want:   emulator.DecisionAllow,
		},
		{
			name:   "ECR read-only does not allow PutImage",
			arn:    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
			action: "ecr:PutImage",
			want:   emulator.DecisionImplicitDeny,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := emulator.GetManagedPolicy(tc.arn)
			require.True(t, ok)

			r := emulator.Evaluate([]emulator.PolicyDocument{p.Document},
				emulator.EvaluationRequest{Action: tc.action, Resource: "*"})
			assert.Equal(t, tc.want, r.Decision)
		})
	}
}

func TestManagedPolicy_PowerUserEvaluation(t *testing.T) {
	p, ok := emulator.GetManagedPolicy("arn:aws:iam::aws:policy/PowerUserAccess")
	require.True(t, ok)

	// s3:GetObject should be allowed (not in NotAction list).
	r1 := emulator.Evaluate([]emulator.PolicyDocument{p.Document}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionAllow, r1.Decision)

	// iam:CreateUser should be implicitly denied (blocked by NotAction).
	r2 := emulator.Evaluate([]emulator.PolicyDocument{p.Document}, emulator.EvaluationRequest{
		Action: "iam:CreateUser", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, r2.Decision)
}
