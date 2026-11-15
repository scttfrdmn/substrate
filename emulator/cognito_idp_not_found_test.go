package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cognitoNotFoundPoolID reads the pool ID a CreateUserPool response carries.
func cognitoNotFoundPoolID(t *testing.T, resp *emulator.AWSResponse) string {
	t.Helper()
	var out struct {
		UserPool struct {
			ID string `json:"Id"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out), "decode CreateUserPool: %s", resp.Body)
	require.NotEmpty(t, out.UserPool.ID, "CreateUserPool reports no Id: %s", resp.Body)
	return out.UserPool.ID
}

// Every Cognito user pools not-found refusal answers HTTP 400, as the pages publish (#1425).
func TestCognitoIDPNotFound_EveryLoaderAnswers400(t *testing.T) {
	t.Parallel()
	p, ctx := setupCognitoIDPPlugin(t)
	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": "nf"}))
	require.NoError(t, err)
	poolID := cognitoNotFoundPoolID(t, resp)

	cases := []struct {
		name, op string
		body     map[string]any
		wantCode string
	}{
		{"an absent pool", "DescribeUserPool", map[string]any{"UserPoolId": "us-east-1_absent"}, "ResourceNotFoundException"},
		{"an absent client", "DescribeUserPoolClient",
			map[string]any{"UserPoolId": poolID, "ClientId": "absent"}, "ResourceNotFoundException"},
		{"an absent group", "GetGroup", map[string]any{"UserPoolId": poolID, "GroupName": "absent"}, "ResourceNotFoundException"},
		{"an absent user", "AdminGetUser", map[string]any{"UserPoolId": poolID, "Username": "absent"}, "UserNotFoundException"},
		{"SignUp to an absent client", "SignUp",
			map[string]any{"ClientId": "absent", "Username": "u", "Password": "Passw0rd!"}, "ResourceNotFoundException"},
		{"ConfirmSignUp to an absent client", "ConfirmSignUp",
			map[string]any{"ClientId": "absent", "Username": "u", "ConfirmationCode": "123456"}, "ResourceNotFoundException"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.HandleRequest(ctx, cognitoIDPRequest(t, tc.op, tc.body))
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s must be refused, got %v", tc.op, err)
			assert.Equal(t, tc.wantCode, awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus, "the pages publish %s at 400", tc.wantCode)
		})
	}
}
