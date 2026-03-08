package substrate_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestListManagedPolicies_Count(t *testing.T) {
	policies := substrate.ListManagedPolicies()
	assert.Len(t, policies, 47, "expected exactly 47 bundled managed policies")
}

func TestListManagedPolicies_ARNFormat(t *testing.T) {
	policies := substrate.ListManagedPolicies()
	for _, p := range policies {
		assert.True(t,
			strings.HasPrefix(p.ARN, "arn:aws:iam::aws:policy/"),
			"policy %s has unexpected ARN %s", p.PolicyName, p.ARN,
		)
		assert.Equal(t, p.PolicyName, strings.TrimPrefix(p.ARN, "arn:aws:iam::aws:policy/"),
			"policy name should match ARN suffix for %s", p.PolicyName)
	}
}

func TestListManagedPolicies_UniqueARNs(t *testing.T) {
	policies := substrate.ListManagedPolicies()
	seen := make(map[string]bool, len(policies))
	for _, p := range policies {
		assert.False(t, seen[p.ARN], "duplicate ARN: %s", p.ARN)
		seen[p.ARN] = true
	}
}

func TestListManagedPolicies_UniqueIDs(t *testing.T) {
	policies := substrate.ListManagedPolicies()
	seen := make(map[string]bool, len(policies))
	for _, p := range policies {
		assert.False(t, seen[p.PolicyID], "duplicate PolicyID: %s", p.PolicyID)
		seen[p.PolicyID] = true
	}
}

func TestListManagedPolicies_RequiredFields(t *testing.T) {
	policies := substrate.ListManagedPolicies()
	for _, p := range policies {
		assert.NotEmpty(t, p.PolicyName, "PolicyName missing")
		assert.NotEmpty(t, p.PolicyID, "PolicyID missing for %s", p.PolicyName)
		assert.NotEmpty(t, p.ARN, "ARN missing for %s", p.PolicyName)
		assert.Equal(t, "/", p.Path, "Path should be '/' for %s", p.PolicyName)
		assert.True(t, p.IsAttachable, "IsAttachable should be true for %s", p.PolicyName)
		assert.NotEmpty(t, p.DefaultVersionID, "DefaultVersionId missing for %s", p.PolicyName)
	}
}

func TestGetManagedPolicy_AdministratorAccess(t *testing.T) {
	p, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/AdministratorAccess")
	require.True(t, ok)
	assert.Equal(t, "AdministratorAccess", p.PolicyName)
	require.Len(t, p.Document.Statement, 1)
	stmt := p.Document.Statement[0]
	assert.Equal(t, substrate.IAMEffectAllow, stmt.Effect)
	assert.Contains(t, []string(stmt.Action), "*")
	assert.Contains(t, []string(stmt.Resource), "*")
}

func TestGetManagedPolicy_ReadOnlyAccess(t *testing.T) {
	p, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/ReadOnlyAccess")
	require.True(t, ok)
	assert.Equal(t, "ReadOnlyAccess", p.PolicyName)
	assert.NotEmpty(t, p.Document.Statement)
}

func TestGetManagedPolicy_PowerUserAccess(t *testing.T) {
	p, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/PowerUserAccess")
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
	_, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/NonExistentPolicy")
	assert.False(t, ok)
}

func TestGetManagedPolicy_Concurrent(t *testing.T) {
	// Verify the sync.Once lookup map is safe for concurrent use.
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			p, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/AdministratorAccess")
			assert.True(t, ok)
			assert.NotNil(t, p)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestManagedPolicy_PowerUserEvaluation(t *testing.T) {
	p, ok := substrate.GetManagedPolicy("arn:aws:iam::aws:policy/PowerUserAccess")
	require.True(t, ok)

	// s3:GetObject should be allowed (not in NotAction list).
	r1 := substrate.Evaluate([]substrate.PolicyDocument{p.Document}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionAllow, r1.Decision)

	// iam:CreateUser should be implicitly denied (blocked by NotAction).
	r2 := substrate.Evaluate([]substrate.PolicyDocument{p.Document}, substrate.EvaluationRequest{
		Action: "iam:CreateUser", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, r2.Decision)
}
