package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestEvaluate_AllowWildcard(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, emulator.DecisionAllow, result.Decision)
}

func TestEvaluate_ImplicitDenyNoStatements(t *testing.T) {
	result := emulator.Evaluate(nil, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_ImplicitDenyNoMatch(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:PutObject"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_ExplicitDenyWins(t *testing.T) {
	allow := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	deny := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectDeny, Action: emulator.StringOrSlice{"s3:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	result := emulator.Evaluate([]emulator.PolicyDocument{allow, deny}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, emulator.DecisionDeny, result.Decision)
}

func TestEvaluate_DenyBeforeAllow(t *testing.T) {
	// Deny in doc[0] before Allow in doc[1] — Deny must win.
	deny := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Sid: "DenyS3", Effect: emulator.IAMEffectDeny, Action: emulator.StringOrSlice{"s3:GetObject"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	allow := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Sid: "AllowAll", Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	result := emulator.Evaluate([]emulator.PolicyDocument{deny, allow}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
	})
	assert.Equal(t, emulator.DecisionDeny, result.Decision)
}

func TestEvaluate_ActionGlob(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		pattern  string
		decision string
	}{
		{"exact match", "s3:GetObject", "s3:GetObject", emulator.DecisionAllow},
		{"wildcard service", "s3:GetObject", "s3:*", emulator.DecisionAllow},
		{"wildcard all", "iam:CreateUser", "*", emulator.DecisionAllow},
		{"prefix no match", "s3:GetObject", "ec2:*", emulator.DecisionImplicitDeny},
		{"question mark", "s3:GetObject", "s3:Get?bject", emulator.DecisionAllow},
		{"question mark no match", "s3:GetXX", "s3:Get?", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := emulator.PolicyDocument{
				Version: "2012-10-17",
				Statement: []emulator.PolicyStatement{
					{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{tt.pattern}, Resource: emulator.StringOrSlice{"*"}},
				},
			}
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action:   tt.action,
				Resource: "*",
			})
			assert.Equal(t, tt.decision, result.Decision, "action=%s pattern=%s", tt.action, tt.pattern)
		})
	}
}

func TestEvaluate_NotAction(t *testing.T) {
	// PowerUserAccess style: NotAction iam:* — allows everything except iam.
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, NotAction: emulator.StringOrSlice{"iam:*", "organizations:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}

	tests := []struct {
		action   string
		decision string
	}{
		{"s3:GetObject", emulator.DecisionAllow},
		{"dynamodb:PutItem", emulator.DecisionAllow},
		{"iam:CreateUser", emulator.DecisionImplicitDeny},
		{"organizations:ListAccounts", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action:   tt.action,
				Resource: "*",
			})
			assert.Equal(t, tt.decision, result.Decision)
		})
	}
}

func TestEvaluate_ResourceGlob(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:GetObject"}, Resource: emulator.StringOrSlice{"arn:aws:s3:::mybucket/*"}},
		},
	}

	tests := []struct {
		resource string
		decision string
	}{
		{"arn:aws:s3:::mybucket/file.txt", emulator.DecisionAllow},
		{"arn:aws:s3:::mybucket/nested/file.txt", emulator.DecisionAllow},
		{"arn:aws:s3:::otherbucket/file.txt", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: tt.resource,
			})
			assert.Equal(t, tt.decision, result.Decision)
		})
	}
}

func TestEvaluate_NotResource(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:*"}, NotResource: emulator.StringOrSlice{"arn:aws:s3:::protected/*"}},
		},
	}
	result1 := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/file.txt",
	})
	assert.Equal(t, emulator.DecisionAllow, result1.Decision)

	result2 := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::protected/secret.txt",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_Condition_StringEquals(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:GetObject"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {
						"s3:prefix": emulator.StringOrSlice{"home/"},
					},
				},
			},
		},
	}

	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"s3:prefix": "home/"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"s3:prefix": "other/"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_StringLike(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringLike": {
						"aws:username": emulator.StringOrSlice{"dev-*"},
					},
				},
			},
		},
	}

	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:username": "dev-alice"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:username": "prod-alice"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_Bool(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"Bool": {
						"aws:MultiFactorAuthPresent": emulator.StringOrSlice{"true"},
					},
				},
			},
		},
	}

	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "true"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "false"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_Null(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectDeny,
				Action:   emulator.StringOrSlice{"*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"Null": {
						"aws:MultiFactorAuthPresent": emulator.StringOrSlice{"true"},
					},
				},
			},
		},
	}

	// Key absent → null=true → condition matches → deny.
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{},
	})
	assert.Equal(t, emulator.DecisionDeny, result.Decision)

	// Key present → null=false → condition doesn't match → implicit deny (no allow).
	result2 := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action:   "s3:GetObject",
		Resource: "*",
		Context:  map[string]string{"aws:MultiFactorAuthPresent": "true"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_MultipleDocuments(t *testing.T) {
	// Two documents: one allows s3, one allows ec2. Both must be allowed.
	s3Doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	ec2Doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"ec2:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}

	r1 := emulator.Evaluate([]emulator.PolicyDocument{s3Doc, ec2Doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionAllow, r1.Decision)

	r2 := emulator.Evaluate([]emulator.PolicyDocument{s3Doc, ec2Doc}, emulator.EvaluationRequest{
		Action: "ec2:DescribeInstances", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionAllow, r2.Decision)

	r3 := emulator.Evaluate([]emulator.PolicyDocument{s3Doc, ec2Doc}, emulator.EvaluationRequest{
		Action: "iam:CreateUser", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, r3.Decision)
}

func TestEvaluate_Condition_StringNotEquals(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringNotEquals": {
						"aws:RequestedRegion": emulator.StringOrSlice{"us-gov-east-1", "us-gov-west-1"},
					},
				},
			},
		},
	}
	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:RequestedRegion": "us-east-1"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:RequestedRegion": "us-gov-east-1"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_StringNotLike(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringNotLike": {
						"aws:username": emulator.StringOrSlice{"admin-*"},
					},
				},
			},
		},
	}
	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:username": "dev-alice"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:username": "admin-bob"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_ArnLike(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:GetObject"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"ArnLike": {
						"aws:SourceArn": emulator.StringOrSlice{"arn:aws:iam::123456789012:role/*"},
					},
				},
			},
		},
	}
	allow := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:SourceArn": "arn:aws:iam::123456789012:role/myrole"},
	})
	assert.Equal(t, emulator.DecisionAllow, allow.Decision)

	deny := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:SourceArn": "arn:aws:iam::999999999999:role/otherrole"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, deny.Decision)
}

func TestEvaluate_Condition_ArnNotEquals(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectDeny,
				Action:   emulator.StringOrSlice{"*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"ArnNotEquals": {
						"aws:PrincipalArn": emulator.StringOrSlice{"arn:aws:iam::123456789012:role/admin"},
					},
				},
			},
		},
	}
	// Non-admin gets denied.
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:PrincipalArn": "arn:aws:iam::123456789012:role/dev"},
	})
	assert.Equal(t, emulator.DecisionDeny, result.Decision)

	// Admin matches the ARN → ArnNotEquals is false → Deny doesn't apply.
	result2 := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{"aws:PrincipalArn": "arn:aws:iam::123456789012:role/admin"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result2.Decision)
}

func TestEvaluate_Condition_UnknownOperator(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"UnknownOperator": {
						"some:key": emulator.StringOrSlice{"value"},
					},
				},
			},
		},
	}
	// Unknown operator → condition fails → implicit deny.
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
		Context: map[string]string{},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_EmptyAction(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	// Empty action doesn't match "s3:*".
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
}

func TestEvaluate_MatchedStatements(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Sid: "AllowS3", Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"s3:*"}, Resource: emulator.StringOrSlice{"*"}},
		},
	}
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "s3:GetObject", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionAllow, result.Decision)
	assert.Contains(t, result.MatchedStatements, "AllowS3")
}
