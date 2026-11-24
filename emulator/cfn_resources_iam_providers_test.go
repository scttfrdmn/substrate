package emulator_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestCFN_IAMOIDCProviderDeploysUpdatesAndDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
	tmpl := func(clientIDs, thumb, tags string) string {
		return `{"Resources":{
			"Gh":{"Type":"AWS::IAM::OIDCProvider","Properties":{"Url":"https://token.actions.githubusercontent.com",
				"ClientIdList":` + clientIDs + `,"ThumbprintList":["` + thumb + `"],"Tags":` + tags + `}},
			"Deploy":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"gh-deploy",
				"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
					"Principal":{"Federated":{"Ref":"Gh"}},"Action":"sts:AssumeRoleWithWebIdentity"}]}}}},
			"Outputs":{"Ref":{"Value":{"Ref":"Gh"}},"Arn":{"Value":{"Fn::GetAtt":["Gh","Arn"]}}}}`
	}
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	result, err := d.Deploy(ctx, tmpl(`["sts.amazonaws.com"]`, a, `[{"Key":"team","Value":"ci"}]`), "oidc", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, arn, result.Outputs["Ref"], "Ref returns the ARN")
	assert.Equal(t, arn, result.Outputs["Arn"])
	assert.Contains(t, string(cfnIAMOK(t, d, "GetRole", map[string]string{"RoleName": "gh-deploy"})),
		url.QueryEscape(arn), "the role's trust policy names the provider")

	updated, err := d.UpdateStack(ctx, tmpl(`["sts.amazonaws.com","app-2"]`, b, `[{"Key":"env","Value":"prod"}]`),
		"oidc", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, arn, resourceByLogicalID(t, updated, "Gh").PhysicalID, "updated in place, not replaced")
	got := cfnIAMOK(t, d, "GetOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	assert.Equal(t, []string{"sts.amazonaws.com", "app-2"}, cfnIAMMembers(t, got, "ClientIDList>member"))
	assert.Equal(t, []string{b}, cfnIAMMembers(t, got, "ThumbprintList>member"))
	assert.Equal(t, []string{"env"}, cfnIAMMembers(t, got, "Tags>member>Key"), "only the declared tag remains")

	updated, err = d.UpdateStack(ctx, tmpl(`["app-2"]`, b, `[]`), "oidc", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	got = cfnIAMOK(t, d, "GetOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	assert.Equal(t, []string{"app-2"}, cfnIAMMembers(t, got, "ClientIDList>member"))
	assert.Empty(t, cfnIAMMembers(t, got, "Tags>member>Key"))

	require.NoError(t, d.DeleteStack(ctx, "oidc"))
	_, err = cfnIAMDo(t, d, "GetOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchEntity")
}

func TestCFN_IAMSAMLProviderDeploysUpdatesAndDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const arn = "arn:aws:iam::123456789012:saml-provider/Corp"
	doc := func(until string) string {
		return strings.ReplaceAll(strings.ReplaceAll(strings.Replace(samlMetadata, "2030-01-01", until, 1), "\n", " "), `"`, `\"`)
	}
	tmpl := func(until, tags string) string {
		return `{"Resources":{"Idp":{"Type":"AWS::IAM::SAMLProvider","Properties":{"Name":"Corp",
			"SamlMetadataDocument":"` + doc(until) + `","Tags":` + tags + `}}},
			"Outputs":{"Ref":{"Value":{"Ref":"Idp"}},"Uuid":{"Value":{"Fn::GetAtt":["Idp","SamlProviderUUID"]}}}}`
	}
	result, err := d.Deploy(ctx, tmpl("2030-01-01", `[{"Key":"team","Value":"id"}]`), "saml", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, arn, result.Outputs["Ref"])
	assert.Len(t, result.Outputs["Uuid"], 22)

	updated, err := d.UpdateStack(ctx, tmpl("2032-02-02", `[]`), "saml", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	got := cfnIAMOK(t, d, "GetSAMLProvider", map[string]string{"SAMLProviderArn": arn})
	assert.Equal(t, []string{"2032-02-02T00:00:00Z"}, cfnIAMMembers(t, got, "ValidUntil"))
	assert.Empty(t, cfnIAMMembers(t, got, "Tags>member>Key"))

	require.NoError(t, d.DeleteStack(ctx, "saml"))
	_, err = cfnIAMDo(t, d, "GetSAMLProvider", map[string]string{"SAMLProviderArn": arn})
	require.Error(t, err)
}
