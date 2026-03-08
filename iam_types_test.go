package substrate_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestStringOrSlice_UnmarshalString(t *testing.T) {
	var s substrate.StringOrSlice
	require.NoError(t, json.Unmarshal([]byte(`"s3:GetObject"`), &s))
	assert.Equal(t, substrate.StringOrSlice{"s3:GetObject"}, s)
}

func TestStringOrSlice_UnmarshalArray(t *testing.T) {
	var s substrate.StringOrSlice
	require.NoError(t, json.Unmarshal([]byte(`["s3:GetObject","s3:PutObject"]`), &s))
	assert.Equal(t, substrate.StringOrSlice{"s3:GetObject", "s3:PutObject"}, s)
}

func TestStringOrSlice_MarshalSingle(t *testing.T) {
	s := substrate.StringOrSlice{"s3:GetObject"}
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, `"s3:GetObject"`, string(b))
}

func TestStringOrSlice_MarshalMultiple(t *testing.T) {
	s := substrate.StringOrSlice{"s3:GetObject", "s3:PutObject"}
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, `["s3:GetObject","s3:PutObject"]`, string(b))
}

func TestPolicyPrincipal_UnmarshalWildcard(t *testing.T) {
	var p substrate.PolicyPrincipal
	require.NoError(t, json.Unmarshal([]byte(`"*"`), &p))
	assert.True(t, p.All)
	assert.Empty(t, p.AWS)
}

func TestPolicyPrincipal_UnmarshalARN(t *testing.T) {
	var p substrate.PolicyPrincipal
	require.NoError(t, json.Unmarshal([]byte(`"arn:aws:iam::123456789012:root"`), &p))
	assert.False(t, p.All)
	assert.Equal(t, []string{"arn:aws:iam::123456789012:root"}, p.AWS)
}

func TestPolicyPrincipal_UnmarshalTypedMap(t *testing.T) {
	raw := `{"AWS":["arn:aws:iam::123456789012:role/myrole"],"Service":["lambda.amazonaws.com"]}`
	var p substrate.PolicyPrincipal
	require.NoError(t, json.Unmarshal([]byte(raw), &p))
	assert.False(t, p.All)
	assert.Equal(t, []string{"arn:aws:iam::123456789012:role/myrole"}, p.AWS)
	assert.Equal(t, []string{"lambda.amazonaws.com"}, p.Service)
}

func TestPolicyPrincipal_MarshalWildcard(t *testing.T) {
	p := substrate.PolicyPrincipal{All: true}
	b, err := json.Marshal(&p)
	require.NoError(t, err)
	assert.Equal(t, `"*"`, string(b))
}

func TestPolicyPrincipal_MarshalSingleAWS(t *testing.T) {
	p := substrate.PolicyPrincipal{AWS: []string{"arn:aws:iam::123456789012:root"}}
	b, err := json.Marshal(&p)
	require.NoError(t, err)
	assert.Equal(t, `"arn:aws:iam::123456789012:root"`, string(b))
}

func TestPolicyPrincipal_MarshalTypedMap(t *testing.T) {
	p := substrate.PolicyPrincipal{
		AWS:     []string{"arn:aws:iam::123456789012:root"},
		Service: []string{"lambda.amazonaws.com"},
	}
	b, err := json.Marshal(&p)
	require.NoError(t, err)
	// Both keys must be present.
	assert.Contains(t, string(b), `"AWS"`)
	assert.Contains(t, string(b), `"Service"`)
}

func TestPolicyDocument_RoundTrip(t *testing.T) {
	doc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{
			{
				Effect:   substrate.IAMEffectAllow,
				Action:   substrate.StringOrSlice{"s3:GetObject"},
				Resource: substrate.StringOrSlice{"arn:aws:s3:::mybucket/*"},
			},
		},
	}
	b, err := json.Marshal(doc)
	require.NoError(t, err)

	var got substrate.PolicyDocument
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, doc.Version, got.Version)
	assert.Len(t, got.Statement, 1)
	assert.Equal(t, substrate.IAMEffectAllow, got.Statement[0].Effect)
}

func TestGenerateIAMID_Format(t *testing.T) {
	tests := []struct {
		prefix string
		length int
	}{
		{"AIDA", 21},
		{"AROA", 21},
		{"AGPA", 21},
		{"ANPA", 21},
		{"AKIA", 21},
	}
	for _, tt := range tests {
		id := substrate.GenerateIAMIDForTest(tt.prefix)
		assert.Len(t, id, tt.length, "prefix %s", tt.prefix)
		assert.True(t, strings.HasPrefix(id, tt.prefix), "id %s should start with %s", id, tt.prefix)
		// All chars must be uppercase alphanumeric.
		for _, c := range id {
			assert.True(t, (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'),
				"unexpected char %c in %s", c, id)
		}
	}
}

func TestIAMARNHelpers(t *testing.T) {
	account := "123456789012"
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"userARN default path", substrate.IAMUserARNForTest(account, "", "alice"), "arn:aws:iam::123456789012:user/alice"},
		{"userARN custom path", substrate.IAMUserARNForTest(account, "/eng/", "bob"), "arn:aws:iam::123456789012:user/eng/bob"},
		{"roleARN", substrate.IAMRoleARNForTest(account, "", "myrole"), "arn:aws:iam::123456789012:role/myrole"},
		{"groupARN", substrate.IAMGroupARNForTest(account, "", "devs"), "arn:aws:iam::123456789012:group/devs"},
		{"policyARN", substrate.IAMPolicyARNForTest(account, "", "mypolicy"), "arn:aws:iam::123456789012:policy/mypolicy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
