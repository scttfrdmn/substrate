package substrate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	substrate "github.com/scttfrdmn/substrate"
)

func TestEvaluate_AllowWildcard(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, substrate.DecisionAllow, result.Decision)
}

func TestEvaluate_ImplicitDenyNoStatements(t *testing.T) {
	result := substrate.Evaluate(nil, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_ImplicitDenyNoMatch(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:PutObject"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_ExplicitDenyWins(t *testing.T) {
	allow := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	deny := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectDeny, Action: substrate.StringOrSlice{"s3:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	result := substrate.Evaluate([]substrate.PolicyDocument{allow, deny}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, substrate.DecisionDeny, result.Decision)
}

func TestEvaluate_DenyBeforeAllow(t *testing.T) {
	// Deny in doc[0] before Allow in doc[1] — Deny must win.
	deny := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Sid: "DenyS3", Effect: substrate.IAMEffectDeny, Action: substrate.StringOrSlice{"s3:GetObject"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	allow := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Sid: "AllowAll", Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	result := substrate.Evaluate([]substrate.PolicyDocument{deny, allow}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
	})
	assert.Equal(t, substrate.DecisionDeny, result.Decision)
}

func TestEvaluate_ActionGlob(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		pattern  string
		decision string
	}{
		{"exact match", "s3:GetObject", "s3:GetObject", substrate.DecisionAllow},
		{"wildcard service", "s3:GetObject", "s3:*", substrate.DecisionAllow},
		{"wildcard all", "iam:CreateUser", "*", substrate.DecisionAllow},
		{"prefix no match", "s3:GetObject", "ec2:*", substrate.DecisionImplicitDeny},
		{"question mark", "s3:GetObject", "s3:Get?bject", substrate.DecisionAllow},
		{"question mark no match", "s3:GetXX", "s3:Get?", substrate.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := substrate.PolicyDocument{
				Version: "2012-10-17",
				Statement: []substrate.PolicyStatement{
					{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{tt.pattern}, Resource: substrate.StringOrSlice{"*"}},
				},
			}
			result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
				Action:   tt.action,
				Resource: "*",
			})
			assert.Equal(t, tt.decision, result.Decision, "action=%s pattern=%s", tt.action, tt.pattern)
		})
	}
}

func TestEvaluate_NotAction(t *testing.T) {
	// PowerUserAccess style: NotAction iam:* — allows everything except iam.
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, NotAction: substrate.StringOrSlice{"iam:*", "organizations:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}

	tests := []struct {
		action   string
		decision string
	}{
		{"s3:GetObject", substrate.DecisionAllow},
		{"dynamodb:PutItem", substrate.DecisionAllow},
		{"iam:CreateUser", substrate.DecisionImplicitDeny},
		{"organizations:ListAccounts", substrate.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
				Action:   tt.action,
				Resource: "*",
			})
			assert.Equal(t, tt.decision, result.Decision)
		})
	}
}

func TestEvaluate_ResourceGlob(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:GetObject"}, Resource: substrate.StringOrSlice{"arn:aws:s3:::mybucket/*"}},
		},
	}

	tests := []struct {
		resource string
		decision string
	}{
		{"arn:aws:s3:::mybucket/file.txt", substrate.DecisionAllow},
		{"arn:aws:s3:::mybucket/nested/file.txt", substrate.DecisionAllow},
		{"arn:aws:s3:::otherbucket/file.txt", substrate.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: tt.resource,
			})
			assert.Equal(t, tt.decision, result.Decision)
		})
	}
}

func TestEvaluate_NotResource(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:*"}, NotResource: substrate.StringOrSlice{"arn:aws:s3:::protected/*"}},
		},
	}
	result1 := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, substrate.DecisionAllow, result1.Decision)

	result2 := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::protected/secret.txt",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_Condition_StringEquals(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:GetObject"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"StringEquals": {
						"s3:prefix": substrate.StringOrSlice{"home/"},
					},
				},
			},
		},
	}

	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"s3:prefix": "home/"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"s3:prefix": "other/"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_StringLike(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"StringLike": {
						"aws:username": substrate.StringOrSlice{"dev-*"},
					},
				},
			},
		},
	}

	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:username": "dev-alice"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:username": "prod-alice"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_Bool(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"Bool": {
						"aws:MultiFactorAuthPresent": substrate.StringOrSlice{"true"},
					},
				},
			},
		},
	}

	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "true"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "false"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_Null(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectDeny,
				Action:   substrate.StringOrSlice{"*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"Null": {
						"aws:MultiFactorAuthPresent": substrate.StringOrSlice{"true"},
					},
				},
			},
		},
	}

	// Key absent → null=true → condition matches → deny.
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{},
	})
	assert.Equal(t, substrate.DecisionDeny, result.Decision)

	// Key present → null=false → condition doesn't match → implicit deny (no allow).
	result2 := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "true"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_MultipleDocuments(t *testing.T) {
	// Two documents: one allows s3, one allows ec2. Both must be allowed.
	s3Doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	ec2Doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"ec2:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}

	r1 := substrate.Evaluate([]substrate.PolicyDocument{s3Doc, ec2Doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionAllow, r1.Decision)

	r2 := substrate.Evaluate([]substrate.PolicyDocument{s3Doc, ec2Doc}, substrate.EvaluationRequest{
		Action: "ec2:DescribeInstances", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionAllow, r2.Decision)

	r3 := substrate.Evaluate([]substrate.PolicyDocument{s3Doc, ec2Doc}, substrate.EvaluationRequest{
		Action: "iam:CreateUser", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, r3.Decision)
}

func TestEvaluate_Condition_StringNotEquals(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"StringNotEquals": {
						"aws:RequestedRegion": substrate.StringOrSlice{"us-gov-east-1", "us-gov-west-1"},
					},
				},
			},
		},
	}
	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:RequestedRegion": "us-east-1"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:RequestedRegion": "us-gov-east-1"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_StringNotLike(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"StringNotLike": {
						"aws:username": substrate.StringOrSlice{"admin-*"},
					},
				},
			},
		},
	}
	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:username": "dev-alice"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:username": "admin-bob"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_ArnLike(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:GetObject"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"ArnLike": {
						"aws:SourceArn": substrate.StringOrSlice{"arn:aws:iam::123456789012:role/*"},
					},
				},
			},
		},
	}
	allow := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:SourceArn": "arn:aws:iam::123456789012:role/myrole"},
	})
	assert.Equal(t, substrate.DecisionAllow, allow.Decision)

	deny := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:SourceArn": "arn:aws:iam::999999999999:role/otherrole"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_ArnNotEquals(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectDeny,
				Action:   substrate.StringOrSlice{"*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"ArnNotEquals": {
						"aws:PrincipalArn": substrate.StringOrSlice{"arn:aws:iam::123456789012:role/admin"},
					},
				},
			},
		},
	}
	// Non-admin gets denied.
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:PrincipalArn": "arn:aws:iam::123456789012:role/dev"},
	})
	assert.Equal(t, substrate.DecisionDeny, result.Decision)

	// Admin matches the ARN → ArnNotEquals is false → Deny doesn't apply.
	result2 := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:PrincipalArn": "arn:aws:iam::123456789012:role/admin"},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_Condition_UnknownOperator(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:*"},
				Resource: substrate.StringOrSlice{"*"},
				Condition: map[string]map[string]substrate.StringOrSlice{
					"UnknownOperator": {
						"some:key": substrate.StringOrSlice{"value"},
					},
				},
			},
		},
	}
	// Unknown operator → condition fails → implicit deny.
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{},
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_EmptyAction(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	// Empty action doesn't match "s3:*".
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_MatchedStatements(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{Sid: "AllowS3", Effect: substrate.IAMEffectAllow, Action: substrate.StringOrSlice{"s3:*"}, Resource: substrate.StringOrSlice{"*"}},
		},
	}
	result := substrate.Evaluate([]substrate.PolicyDocument{doc}, substrate.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, substrate.DecisionAllow, result.Decision)
	assert.Contains(t, result.MatchedStatements, "AllowS3")
}
