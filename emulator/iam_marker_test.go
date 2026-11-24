package emulator_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every IAM listing paged on a key Marker refuses a Marker it did not issue with ValidationError/400
// (IAM CommonErrors) rather than answering page one, and resumes strictly after the Marker's key, so
// a key deleted mid-walk resumes in place.

func TestIAMMarker_EveryKeyListingRefusesAnUnissuedMarker(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "mk-user"})
	iamRequireOK(t, srv, "CreateRole", map[string]any{"RoleName": "mk-role",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`})
	iamRequireOK(t, srv, "CreateGroup", map[string]any{"GroupName": "mk-group"})

	ops := []struct {
		op     string
		params map[string]any
	}{
		{"ListUsers", nil},
		{"ListRoles", nil},
		{"ListGroups", nil},
		{"ListPolicies", map[string]any{"Scope": "Local"}},
		{"ListInstanceProfiles", nil},
		{"ListUserPolicies", map[string]any{"UserName": "mk-user"}},
		{"GetGroup", map[string]any{"GroupName": "mk-group"}},
		{"ListGroupsForUser", map[string]any{"UserName": "mk-user"}},
		{"ListInstanceProfilesForRole", map[string]any{"RoleName": "mk-role"}},
		{"GetAccountAuthorizationDetails", nil},
		{"ListUserTags", map[string]any{"UserName": "mk-user"}},
		{"ListRoleTags", map[string]any{"RoleName": "mk-role"}},
	}
	key := base64.StdEncoding.EncodeToString([]byte("mk-user"))
	for _, marker := range []string{
		"not base64!",
		key[:len(key)-1], // unpadded
		"mk-user",        // a raw key
	} {
		for _, tc := range ops {
			body := map[string]any{"Marker": marker}
			for k, v := range tc.params {
				body[k] = v
			}
			resp := iamRequest(t, srv, tc.op, body)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s Marker=%q", tc.op, marker)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "ValidationError", result["__type"], "%s Marker=%q", tc.op, marker)
		}
	}
}

func TestIAMMarker_AWalkSurvivesAMidWalkDelete(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	for _, name := range []string{"u1", "u2", "u3", "u4", "u5"} {
		iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": name})
	}
	list := func(body map[string]any) ([]string, string) {
		resp := iamRequest(t, srv, "ListUsers", body)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result map[string]any
		decodeIAMXML(t, resp, &result)
		var names []string
		users, _ := result["Users"].([]any)
		for _, u := range users {
			names = append(names, u.(map[string]any)["UserName"].(string))
		}
		marker, _ := result["Marker"].(string)
		return names, marker
	}

	first, marker := list(map[string]any{"MaxItems": 2})
	require.Equal(t, []string{"u1", "u2"}, first)
	require.NotEmpty(t, marker)

	// The Marker's own key and the one after it are deleted; one key before and one after are added.
	for _, name := range []string{"u2", "u3"} {
		iamRequireOK(t, srv, "DeleteUser", map[string]any{"UserName": name})
	}
	for _, name := range []string{"u0", "u9"} {
		iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": name})
	}
	walked := first
	for pages := 0; marker != ""; pages++ {
		require.Less(t, pages, 10)
		var names []string
		names, marker = list(map[string]any{"MaxItems": 2, "Marker": marker})
		walked = append(walked, names...)
	}
	assert.Equal(t, []string{"u1", "u2", "u4", "u5", "u9"}, walked)
}
