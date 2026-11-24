package emulator_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1627: CreateOrganization and DeleteOrganization.

// orgCall issues one operation as the management account and returns the status and error code.
func orgCall(t *testing.T, ts *emulator.TestServer, op string, body, out any) (int, string) {
	t.Helper()
	return decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount, op, body), out)
}

// orgRootID returns the management account's root ID.
func orgRootID(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	var roots struct {
		Roots []emulator.OrgRoot `json:"Roots"`
	}
	status, code := orgCall(t, ts, "ListRoots", map[string]any{}, &roots)
	require.Equal(t, http.StatusOK, status, code)
	require.Len(t, roots.Roots, 1)
	return roots.Roots[0].ID
}

// orgRootPolicyTypes returns the root's PolicyTypes as ListRoots reports them.
func orgRootPolicyTypes(t *testing.T, ts *emulator.TestServer) []emulator.OrgPolicyTypeSummary {
	t.Helper()
	var roots struct {
		Roots []emulator.OrgRoot `json:"Roots"`
	}
	status, code := orgCall(t, ts, "ListRoots", map[string]any{}, &roots)
	require.Equal(t, http.StatusOK, status, code)
	require.Len(t, roots.Roots, 1)
	return roots.Roots[0].PolicyTypes
}

func TestOrganizations_CreateOrganizationOnAFreshAccount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, featureSet, want string
	}{
		{"default", "", "ALL"},
		{"all", "ALL", "ALL"},
		{"consolidated billing", "CONSOLIDATED_BILLING", "CONSOLIDATED_BILLING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			body := map[string]any{}
			if tc.featureSet != "" {
				body["FeatureSet"] = tc.featureSet
			}
			var created struct {
				Organization emulator.Organization `json:"Organization"`
			}
			status, code := orgCall(t, ts, "CreateOrganization", body, &created)
			require.Equal(t, http.StatusOK, status, code)
			assert.Equal(t, tc.want, created.Organization.FeatureSet)
			assert.Equal(t, orgManagementAccount, created.Organization.MasterAccountID)
			assert.Regexp(t, `^o-[a-z0-9]{10}$`, created.Organization.ID)

			var described struct {
				Organization emulator.Organization `json:"Organization"`
			}
			status, code = orgCall(t, ts, "DescribeOrganization", map[string]any{}, &described)
			require.Equal(t, http.StatusOK, status, code)
			assert.Equal(t, created.Organization.ID, described.Organization.ID, "the created organization is the one described")
			assert.Equal(t, tc.want, described.Organization.FeatureSet)
			assert.Empty(t, orgRootPolicyTypes(t, ts), "an explicitly created organization enables no policy type")
		})
	}
}

func TestOrganizations_CreateOrganizationRefusals(t *testing.T) {
	t.Parallel()

	t.Run("an invalid feature set", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		status, code := orgCall(t, ts, "CreateOrganization", map[string]any{"FeatureSet": "SOME"}, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidInputException", code)
	})

	t.Run("an organization created explicitly", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		status, code := orgCall(t, ts, "CreateOrganization", map[string]any{}, nil)
		require.Equal(t, http.StatusOK, status, code)
		status, code = orgCall(t, ts, "CreateOrganization", map[string]any{}, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "AlreadyInOrganizationException", code)
	})

	t.Run("an organization created on first observation", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		status, code := orgCall(t, ts, "DescribeOrganization", map[string]any{}, nil)
		require.Equal(t, http.StatusOK, status, code)
		status, code = orgCall(t, ts, "CreateOrganization", map[string]any{}, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "AlreadyInOrganizationException", code)
	})

	t.Run("a member account", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		member := orgVendMember(t, ts, "member", "member@example.com")
		status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member, "CreateOrganization", map[string]any{}), nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "AlreadyInOrganizationException", code)
	})
}

func TestOrganizations_DeleteOrganizationThenEveryCallIsNotInUse(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	var created struct {
		Organization emulator.Organization `json:"Organization"`
	}
	status, code := orgCall(t, ts, "CreateOrganization", map[string]any{}, &created)
	require.Equal(t, http.StatusOK, status, code)
	status, code = orgCall(t, ts, "EnablePolicyType", map[string]any{
		"RootId": orgRootID(t, ts), "PolicyType": "SERVICE_CONTROL_POLICY",
	}, nil)
	require.Equal(t, http.StatusOK, status, code)
	var ou struct {
		OrganizationalUnit emulator.OrgOrganizationalUnit `json:"OrganizationalUnit"`
	}
	status, code = orgCall(t, ts, "CreateOrganizationalUnit", map[string]any{"ParentId": orgRootID(t, ts), "Name": "eng"}, &ou)
	require.Equal(t, http.StatusOK, status, code)
	status, code = orgCall(t, ts, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": "config.amazonaws.com"}, nil)
	require.Equal(t, http.StatusOK, status, code)

	status, code = orgCall(t, ts, "DeleteOrganization", map[string]any{}, nil)
	require.Equal(t, http.StatusOK, status, code)

	// An operation that reaches ensureOrganization and one that reads a record by ID answer alike.
	for _, call := range []struct {
		op   string
		body map[string]any
	}{
		{"DescribeOrganization", map[string]any{}},
		{"ListRoots", map[string]any{}},
		{"ListAccounts", map[string]any{}},
		{"DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ou.OrganizationalUnit.ID}},
		{"ListAWSServiceAccessForOrganization", map[string]any{}},
		{"DeleteOrganization", map[string]any{}},
		{"CreateAccount", map[string]any{"AccountName": "a", "Email": "a@example.com"}},
	} {
		status, code := orgCall(t, ts, call.op, call.body, nil)
		assert.Equal(t, http.StatusBadRequest, status, call.op)
		assert.Equal(t, "AWSOrganizationsNotInUseException", code, call.op)
	}

	// An account that never had an organization still gets one on first observation.
	ts.RegisterAccount(t, orgOutsiderAccount)
	status, code = decodeAWSResponse(t, orgSignedRequest(t, ts, orgOutsiderAccount, "DescribeOrganization", map[string]any{}), nil)
	assert.Equal(t, http.StatusOK, status, code)

	// CreateOrganization ends it, with a new organization holding none of the old one's records.
	var recreated struct {
		Organization emulator.Organization `json:"Organization"`
	}
	status, code = orgCall(t, ts, "CreateOrganization", map[string]any{"FeatureSet": "CONSOLIDATED_BILLING"}, &recreated)
	require.Equal(t, http.StatusOK, status, code)
	assert.NotEqual(t, created.Organization.ID, recreated.Organization.ID)
	status, code = orgCall(t, ts, "DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ou.OrganizationalUnit.ID}, nil)
	assert.Equal(t, "OrganizationalUnitNotFoundException", code, "status %d", status)
	var access struct {
		EnabledServicePrincipals []map[string]any `json:"EnabledServicePrincipals"`
	}
	status, code = orgCall(t, ts, "ListAWSServiceAccessForOrganization", map[string]any{}, &access)
	require.Equal(t, http.StatusOK, status, code)
	assert.Empty(t, access.EnabledServicePrincipals)
	var accounts struct {
		Accounts []emulator.OrgAccount `json:"Accounts"`
	}
	status, code = orgCall(t, ts, "ListAccounts", map[string]any{}, &accounts)
	require.Equal(t, http.StatusOK, status, code)
	require.Len(t, accounts.Accounts, 1)
	assert.Equal(t, orgManagementAccount, accounts.Accounts[0].ID)
}

func TestOrganizations_DeleteOrganizationRefusals(t *testing.T) {
	t.Parallel()

	t.Run("an organization with a member", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		orgVendMember(t, ts, "member", "member@example.com")
		status, code := orgCall(t, ts, "DeleteOrganization", map[string]any{}, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "OrganizationNotEmptyException", code)
		status, code = orgCall(t, ts, "DescribeOrganization", map[string]any{}, nil)
		assert.Equal(t, http.StatusOK, status, "a refused delete leaves the organization: %s", code)
	})

	t.Run("a member account", func(t *testing.T) {
		t.Parallel()
		ts := emulator.StartTestServer(t)
		member := orgVendMember(t, ts, "member", "member@example.com")
		status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member, "DeleteOrganization", map[string]any{}), nil)
		assert.Equal(t, http.StatusForbidden, status)
		assert.Equal(t, "AccessDeniedException", code)
	})
}

// TestOrganizations_DeleteOrganizationLeavesFullAWSAccessToOtherOrganizations guards the one
// record organizations share: p-FullAWSAccess's target list.
func TestOrganizations_DeleteOrganizationLeavesFullAWSAccessToOtherOrganizations(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.RegisterAccount(t, orgOutsiderAccount)
	status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgOutsiderAccount, "DescribeOrganization", map[string]any{}), nil)
	require.Equal(t, http.StatusOK, status, code)
	status, code = orgCall(t, ts, "DescribeOrganization", map[string]any{}, nil)
	require.Equal(t, http.StatusOK, status, code)

	status, code = orgCall(t, ts, "DeleteOrganization", map[string]any{}, nil)
	require.Equal(t, http.StatusOK, status, code)

	var targets struct {
		Targets []map[string]any `json:"Targets"`
	}
	status, code = decodeAWSResponse(t, orgSignedRequest(t, ts, orgOutsiderAccount, "ListTargetsForPolicy",
		map[string]any{"PolicyId": "p-FullAWSAccess"}), &targets)
	require.Equal(t, http.StatusOK, status, code)
	assert.Len(t, targets.Targets, 2, "the other organization's root and account keep their attachment: %v", targets.Targets)
	for _, target := range targets.Targets {
		assert.NotEqual(t, orgManagementAccount, target["TargetId"], "the deleted organization's account is no longer a target")
	}
}

// TestOrganizations_LifecycleStoreFailuresPropagate asserts a store failure in a create or a delete
// is reported as one rather than as an organization created, deleted or absent.
func TestOrganizations_LifecycleStoreFailuresPropagate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, op, prefix string
		get, put, del    bool
		list             bool
	}{
		{"create clears the marker", "CreateOrganization", "org_deleted:", false, false, true, false},
		{"create writes the organization", "CreateOrganization", "org:", false, true, false, false},
		{"delete reads the accounts", "DeleteOrganization", "account_ids:", true, false, false, false},
		{"delete reads the OUs", "DeleteOrganization", "ou_ids:", true, false, false, false},
		{"delete reads the policies", "DeleteOrganization", "policy_ids:", true, false, false, false},
		{"delete reads the requests", "DeleteOrganization", "car_ids:", true, false, false, false},
		{"delete reads the resource policy", "DeleteOrganization", "resource_policy:", true, false, false, false},
		{"delete reads the attachments", "DeleteOrganization", "attachments:", true, false, false, false},
		{"delete removes a record", "DeleteOrganization", "children:", false, false, true, false},
		{"delete writes the marker", "DeleteOrganization", "org_deleted:", false, true, false, false},
		{"service access reads the list", "ListAWSServiceAccessForOrganization", "service_access:", true, false, false, false},
		{"service access writes the list", "EnableAWSServiceAccess", "service_access:", false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &errOrgState{inner: emulator.NewMemoryStateManager(), prefix: tc.prefix,
				err: errors.New("store unavailable"), onGet: tc.get, onPut: tc.put, onDelete: tc.del, onList: tc.list}
			p := newOrgFaultFixture(t, state)
			if tc.op != "CreateOrganization" {
				_, err := p.EnsureOrganizationForTest(t.Context(), orgTestAccount)
				require.NoError(t, err)
			}
			state.armed = true
			_, err := p.HandleRequest(&emulator.RequestContext{AccountID: orgTestAccount, Region: "us-east-1"}, &emulator.AWSRequest{
				Service: "organizations", Operation: tc.op, Body: []byte(`{"ServicePrincipal":"sso.amazonaws.com"}`),
				Headers: map[string]string{}, Params: map[string]string{},
			})
			require.Error(t, err)
			var awsErr *emulator.AWSError
			assert.False(t, errors.As(err, &awsErr), "a store failure is not an AWS answer: %v", err)
		})
	}
}

func TestOrganizations_ServiceAccess(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, principal := range []string{"sso.amazonaws.com", "config.amazonaws.com", "sso.amazonaws.com"} {
		status, code := orgCall(t, ts, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": principal}, nil)
		require.Equal(t, http.StatusOK, status, code)
	}
	list := func(maxResults int) []string {
		t.Helper()
		var names []string
		token := ""
		for {
			body := map[string]any{"MaxResults": maxResults}
			if token != "" {
				body["NextToken"] = token
			}
			var out struct {
				EnabledServicePrincipals []struct {
					ServicePrincipal string  `json:"ServicePrincipal"`
					DateEnabled      float64 `json:"DateEnabled"`
				} `json:"EnabledServicePrincipals"`
				NextToken string `json:"NextToken"`
			}
			status, code := orgCall(t, ts, "ListAWSServiceAccessForOrganization", body, &out)
			require.Equal(t, http.StatusOK, status, code)
			for _, e := range out.EnabledServicePrincipals {
				assert.NotZero(t, e.DateEnabled)
				names = append(names, e.ServicePrincipal)
			}
			if token = out.NextToken; token == "" {
				return names
			}
		}
	}
	assert.Equal(t, []string{"config.amazonaws.com", "sso.amazonaws.com"}, list(1))

	status, code := orgCall(t, ts, "DisableAWSServiceAccess", map[string]any{"ServicePrincipal": "sso.amazonaws.com"}, nil)
	require.Equal(t, http.StatusOK, status, code)
	status, code = orgCall(t, ts, "DisableAWSServiceAccess", map[string]any{"ServicePrincipal": "sso.amazonaws.com"}, nil)
	require.Equal(t, http.StatusOK, status, "disabling a principal not enabled succeeds: %s", code)
	assert.Equal(t, []string{"config.amazonaws.com"}, list(20))

	for _, bad := range []string{"", "has space.amazonaws.com", string(make([]byte, 129))} {
		status, code := orgCall(t, ts, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": bad}, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidInputException", code)
	}

	member := orgVendMember(t, ts, "member", "member@example.com")
	for _, op := range []string{"EnableAWSServiceAccess", "DisableAWSServiceAccess", "ListAWSServiceAccessForOrganization"} {
		status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member, op,
			map[string]any{"ServicePrincipal": "sso.amazonaws.com"}), nil)
		assert.Equal(t, http.StatusForbidden, status, op)
		assert.Equal(t, "AccessDeniedException", code, op)
	}
}
