package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are #530's gate.
//
// Batch could create a compute environment, a job queue and a job definition and
// read none of them back: DescribeComputeEnvironments, DescribeJobQueues and
// DescribeJobDefinitions were unrouted, so each answered an unknown-operation error
// while DescribeJobs — the one read that was routed — worked. A test could perform
// the write and never verify it.
//
// The issue said the state was already there to answer from. It was not: all three
// creates unmarshaled only the name, echoed it back with a hardcoded
// us-east-1:000000000000 ARN, and never touched the state manager. So the creates
// had to start recording first, which is why these tests assert on the whole record
// rather than just the name — the request body was previously discarded in full.
//
// The assertions are on raw wire bytes rather than a Go round-trip. A struct
// marshaled and unmarshaled by its own definition agrees with itself whatever its
// tags say, which is exactly how #528 and #542 stayed green in tests while broken on
// the wire. Batch's API is camelCase throughout and the create responses were
// already correct, so they are the local reference.

// batchPost posts to a Batch path and returns the status code together with the raw
// response body, so an assertion can name the bytes a caller's SDK parses.
func batchPost(t *testing.T, ts *httptest.Server, path string, body interface{}) (int, string) {
	t.Helper()
	resp := batchRequest(t, ts, http.MethodPost, path, body)
	return resp.StatusCode, string(batchBody(t, resp))
}

// batchIdentityRequest posts to a Batch path as a particular caller: the Region comes
// from the host the request is addressed to and the account from the Authorization
// header, which is how the server resolves both. An empty authHeader is an unsigned
// caller.
func batchIdentityRequest(
	t *testing.T, ts *httptest.Server, region, authHeader, path string, body interface{},
) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "batch." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// batchMembers decodes a describe response and returns the named member, for the
// assertions that need a count rather than a substring.
func batchMembers(t *testing.T, body, member string) []map[string]interface{} {
	t.Helper()
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &decoded), "body was %s", body)
	raw, ok := decoded[member]
	require.True(t, ok, "no %s member in %s", member, body)
	var members []map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &members), "body was %s", body)
	return members
}

// TestBatchDescribe_ComputeEnvironmentIsReadableBack is the smallest statement of
// the defect, for the first of the three resources.
func TestBatchDescribe_ComputeEnvironmentIsReadableBack(t *testing.T) {
	ts := newBatchTestServer(t)

	code, body := batchPost(t, ts, "/v1/createcomputeenvironment", map[string]interface{}{
		"computeEnvironmentName": "ce1",
		"type":                   "MANAGED",
		"serviceRole":            "arn:aws:iam::000000000000:role/AWSBatchServiceRole",
		"computeResources": map[string]interface{}{
			"type":     "EC2",
			"maxvCpus": 128,
			"minvCpus": 0,
			"subnets":  []string{"subnet-a"},
		},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"computeEnvironmentName":"ce1"`)

	code, body = batchPost(t, ts, "/v1/describecomputeenvironments", map[string]interface{}{})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// Each member is asserted as a wire-name/value pair, because the whole request
	// body used to be discarded — a describe reporting only the name would look like
	// a fix while every other field stayed lost.
	assert.Contains(t, body, `"computeEnvironments":[`)
	assert.Contains(t, body, `"computeEnvironmentName":"ce1"`)
	assert.Contains(t, body, `"computeEnvironmentArn":"arn:aws:batch:us-east-1:`)
	assert.Contains(t, body, `"type":"MANAGED"`)
	assert.Contains(t, body, `"status":"VALID"`)
	assert.Contains(t, body, `"serviceRole":"arn:aws:iam::000000000000:role/AWSBatchServiceRole"`)
	assert.Contains(t, body, `"maxvCpus":128`)
	assert.Contains(t, body, `"subnets":["subnet-a"]`)
	// "A compute environment must be created in the ENABLED state", so an omitted
	// state is ENABLED rather than absent.
	assert.Contains(t, body, `"state":"ENABLED"`)
	// The operation's own preamble is that an unmanaged caller reads it to find the
	// ECS cluster, so the member has to be there.
	assert.Contains(t, body, `"ecsClusterArn":"arn:aws:ecs:us-east-1:`)
}

// TestBatchDescribe_JobQueueIsReadableBack covers the second resource, including the
// members a compute-environment cross-reference depends on.
func TestBatchDescribe_JobQueueIsReadableBack(t *testing.T) {
	ts := newBatchTestServer(t)

	code, body := batchPost(t, ts, "/v1/createjobqueue", map[string]interface{}{
		"jobQueueName": "q1",
		"priority":     10,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]interface{}{
			{"order": 1, "computeEnvironment": "ce1"},
		},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = batchPost(t, ts, "/v1/describejobqueues", map[string]interface{}{})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"jobQueues":[`)
	assert.Contains(t, body, `"jobQueueName":"q1"`)
	assert.Contains(t, body, `"jobQueueArn":"arn:aws:batch:us-east-1:`)
	assert.Contains(t, body, `"priority":10`)
	assert.Contains(t, body, `"state":"ENABLED"`)
	assert.Contains(t, body, `"status":"VALID"`)
	assert.Contains(t, body, `"computeEnvironment":"ce1"`)
	assert.Contains(t, body, `"order":1`)
}

// TestBatchDescribe_JobDefinitionRevisions is the versioning half.
//
// registerJobDefinition used to answer "revision":1 unconditionally, so two
// registrations of one name were indistinguishable. Each is now its own revision and
// its own record, which is what makes ${name}:${revision} addressable at all.
func TestBatchDescribe_JobDefinitionRevisions(t *testing.T) {
	ts := newBatchTestServer(t)

	register := func(image string) string {
		t.Helper()
		code, body := batchPost(t, ts, "/v1/registerjobdefinition", map[string]interface{}{
			"jobDefinitionName":   "jd1",
			"type":                "container",
			"containerProperties": map[string]interface{}{"image": image},
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		return body
	}

	first := register("busybox")
	assert.Contains(t, first, `"revision":1`)
	assert.Contains(t, first, `:job-definition/jd1:1"`)

	second := register("amazonlinux")
	assert.Contains(t, second, `"revision":2`, "a second registration of one name is revision 2")
	assert.Contains(t, second, `:job-definition/jd1:2"`)

	// A bare jobDefinitionName names every revision of that definition.
	code, body := batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{
		"jobDefinitionName": "jd1",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.Len(t, batchMembers(t, body, "jobDefinitions"), 2)
	assert.Contains(t, body, `"revision":1`)
	assert.Contains(t, body, `"revision":2`)
	// Each revision carries its own properties, so the two are really distinct
	// records rather than one record described twice.
	assert.Contains(t, body, `"image":"busybox"`)
	assert.Contains(t, body, `"image":"amazonlinux"`)
	assert.Contains(t, body, `"status":"ACTIVE"`)
	assert.Contains(t, body, `"type":"container"`)

	// And one revision is addressable on its own, in both documented forms.
	for _, identifier := range []string{
		"jd1:2",
		"arn:aws:batch:us-east-1:000000000000:job-definition/jd1:2",
	} {
		t.Run(identifier, func(t *testing.T) {
			code, body := batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{
				"jobDefinitions": []string{identifier},
			})
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			require.Len(t, batchMembers(t, body, "jobDefinitions"), 1)
			assert.Contains(t, body, `"revision":2`)
			assert.Contains(t, body, `"image":"amazonlinux"`)
			assert.NotContains(t, body, `"image":"busybox"`,
				"a revision-scoped filter reports that revision and no other")
		})
	}
}

// TestBatchDescribe_Filters covers the name-or-ARN filter each describe documents,
// and the absent-filter case the reference specifies as every resource.
func TestBatchDescribe_Filters(t *testing.T) {
	ts := newBatchTestServer(t)

	for _, name := range []string{"ce1", "ce2", "ce3"} {
		code, body := batchPost(t, ts, "/v1/createcomputeenvironment", map[string]interface{}{
			"computeEnvironmentName": name,
			"type":                   "MANAGED",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
	}

	const arnPrefix = "arn:aws:batch:us-east-1:000000000000:compute-environment/"

	cases := []struct {
		name    string
		filter  []string
		wantLen int
		want    []string
		notWant []string
	}{
		{
			name:    "an absent filter reports every environment",
			wantLen: 3,
			want:    []string{`"ce1"`, `"ce2"`, `"ce3"`},
		},
		{
			name:    "a name",
			filter:  []string{"ce2"},
			wantLen: 1,
			want:    []string{`"ce2"`},
			notWant: []string{`"ce1"`, `"ce3"`},
		},
		{
			name:    "a full ARN",
			filter:  []string{arnPrefix + "ce3"},
			wantLen: 1,
			want:    []string{`"ce3"`},
			notWant: []string{`"ce1"`, `"ce2"`},
		},
		{
			// The parameter is documented as "a list of up to 100 compute environment
			// names or full ARN entries", so the two forms mix freely.
			name:    "names and ARNs mixed",
			filter:  []string{"ce1", arnPrefix + "ce3"},
			wantLen: 2,
			want:    []string{`"ce1"`, `"ce3"`},
			notWant: []string{`"ce2"`},
		},
		{
			// The operation describes "one or more of your compute environments" and
			// documents no not-found error, so an absent name is an absent result
			// rather than a refusal.
			name:    "an absent name is skipped, not refused",
			filter:  []string{"ce1", "nope"},
			wantLen: 1,
			want:    []string{`"ce1"`},
		},
		{
			name:    "a filter matching nothing",
			filter:  []string{"nope"},
			wantLen: 0,
			notWant: []string{`"ce1"`, `"ce2"`, `"ce3"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]interface{}{}
			if tc.filter != nil {
				body["computeEnvironments"] = tc.filter
			}
			code, got := batchPost(t, ts, "/v1/describecomputeenvironments", body)
			require.Equal(t, http.StatusOK, code, "body was %s", got)
			assert.Len(t, batchMembers(t, got, "computeEnvironments"), tc.wantLen)
			for _, want := range tc.want {
				assert.Contains(t, got, want)
			}
			for _, notWant := range tc.notWant {
				assert.NotContains(t, got, notWant)
			}
		})
	}
}

// TestBatchDescribe_JobDefinitionStatusFilter covers the documented status filter and
// the name-matches-nothing case.
//
// Nothing in substrate can yet make a job definition INACTIVE — DeregisterJobDefinition
// is unrouted, filed as #555 — so ACTIVE is the case with records in it. The INACTIVE
// case still earns its place: it proves the filter is applied rather than accepted and
// ignored, which is how a status parameter usually rots.
func TestBatchDescribe_JobDefinitionStatusFilter(t *testing.T) {
	ts := newBatchTestServer(t)

	code, body := batchPost(t, ts, "/v1/registerjobdefinition", map[string]interface{}{
		"jobDefinitionName":   "jd1",
		"type":                "container",
		"containerProperties": map[string]interface{}{"image": "busybox"},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"status": "ACTIVE"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Len(t, batchMembers(t, body, "jobDefinitions"), 1)
	assert.Contains(t, body, `"status":"ACTIVE"`)

	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"status": "INACTIVE"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Empty(t, batchMembers(t, body, "jobDefinitions"),
		"the status filter is applied, so an ACTIVE definition is not reported as INACTIVE")

	// A jobDefinitionName matching nothing must not fall through to the unfiltered
	// branch and report every definition in the account.
	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{
		"jobDefinitionName": "absent",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Empty(t, batchMembers(t, body, "jobDefinitions"))
	assert.NotContains(t, body, "jd1")

	// jobDefinitions "can't be used with other parameters", so it wins outright when
	// both are sent. The second definition below is what makes that a real
	// statement: a jobDefinitionName naming something absent would leave a
	// jobDefinitions filter alone under either reading, so it cannot tell
	// precedence from a union.
	code, body = batchPost(t, ts, "/v1/registerjobdefinition", map[string]interface{}{
		"jobDefinitionName":   "jd2",
		"type":                "container",
		"containerProperties": map[string]interface{}{"image": "alpine"},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{
		"jobDefinitions":    []string{"jd1:1"},
		"jobDefinitionName": "jd2",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Len(t, batchMembers(t, body, "jobDefinitions"), 1)
	assert.Contains(t, body, `"image":"busybox"`)
	assert.NotContains(t, body, `"image":"alpine"`,
		"jobDefinitions wins outright; jobDefinitionName does not widen it")

	// And the same pair with a jobDefinitionName that names nothing.
	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{
		"jobDefinitions":    []string{"jd1:1"},
		"jobDefinitionName": "absent",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Len(t, batchMembers(t, body, "jobDefinitions"), 1)
	assert.Contains(t, body, `"revision":1`)
}

// TestBatchDescribe_Pagination covers maxResults and nextToken, which all three
// operations document identically.
func TestBatchDescribe_Pagination(t *testing.T) {
	ts := newBatchTestServer(t)

	for _, name := range []string{"q1", "q2", "q3", "q4", "q5"} {
		code, body := batchPost(t, ts, "/v1/createjobqueue", map[string]interface{}{
			"jobQueueName": name,
			"priority":     1,
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for pages < 5 {
		body := map[string]interface{}{"maxResults": 2}
		if token != "" {
			body["nextToken"] = token
		}
		code, got := batchPost(t, ts, "/v1/describejobqueues", body)
		require.Equal(t, http.StatusOK, code, "body was %s", got)
		pages++

		queues := batchMembers(t, got, "jobQueues")
		require.LessOrEqual(t, len(queues), 2, "a page must not exceed maxResults")
		for _, q := range queues {
			name, _ := q["jobQueueName"].(string)
			require.False(t, seen[name], "%s appeared on two pages", name)
			seen[name] = true
		}

		var decoded struct {
			NextToken string `json:"nextToken"`
		}
		require.NoError(t, json.Unmarshal([]byte(got), &decoded))
		token = decoded.NextToken
		if token == "" {
			break
		}
	}
	assert.Len(t, seen, 5, "paging through reports every queue exactly once")
	assert.Empty(t, token,
		"the last page omits nextToken, which is the only way a caller's paginator stops")
	assert.Equal(t, 3, pages, "five queues at two per page is three pages")

	// An absent maxResults means up to 100 in one page, so five queues arrive
	// together with no token at all.
	code, got := batchPost(t, ts, "/v1/describejobqueues", map[string]interface{}{})
	require.Equal(t, http.StatusOK, code, "body was %s", got)
	assert.Len(t, batchMembers(t, got, "jobQueues"), 5)
	assert.NotContains(t, got, `"nextToken"`,
		`"This value is null when there are no more results to return"`)

	// The boundary worth stating separately: a maxResults that exactly exhausts the
	// results is still exhausted. A token here would send a caller's paginator round
	// again for an empty page — harmless against real AWS, but it means the token is
	// being derived from the page size rather than from whether anything is left.
	code, got = batchPost(t, ts, "/v1/describejobqueues", map[string]interface{}{"maxResults": 5})
	require.Equal(t, http.StatusOK, code, "body was %s", got)
	assert.Len(t, batchMembers(t, got, "jobQueues"), 5)
	assert.NotContains(t, got, `"nextToken"`)
}

// TestBatchDescribe_ScopedToTheCaller is why the creates had to stop minting a
// hardcoded ARN.
//
// The old creates took `_ *RequestContext` and answered
// arn:aws:batch:us-east-1:000000000000:… for every caller. One substrate process
// serves every account and Region a test suite uses, so a resource has to be
// reported to its own caller and not to another — and the ARN it reports has to be
// one that caller's own SubmitJob and computeEnvironmentOrder can name.
func TestBatchDescribe_ScopedToTheCaller(t *testing.T) {
	ts := newBatchTestServer(t)

	// Unsigned resolves to 000000000000; an AKIA-signed request to 123456789012.
	const unsigned = ""
	signed := signedAuthHeader("batch", "us-east-1")
	signedWest := signedAuthHeader("batch", "eu-west-1")

	create := func(auth, region, name string) string {
		t.Helper()
		code, body := batchIdentityRequest(t, ts, region, auth, "/v1/createcomputeenvironment",
			map[string]interface{}{"computeEnvironmentName": name, "type": "MANAGED"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		return body
	}

	zero := create(unsigned, "us-east-1", "zero-owned")
	test := create(signed, "us-east-1", "test-owned")
	west := create(signedWest, "eu-west-1", "west-owned")

	assert.Contains(t, zero, `"computeEnvironmentArn":"arn:aws:batch:us-east-1:000000000000:compute-environment/zero-owned"`)
	assert.Contains(t, test, `:123456789012:compute-environment/test-owned"`,
		"the ARN carries the caller's account, not a hardcoded 000000000000")
	assert.Contains(t, west, `"computeEnvironmentArn":"arn:aws:batch:eu-west-1:`,
		"the ARN carries the caller's Region, not a hardcoded us-east-1")

	// Each caller's unfiltered describe reports its own environment and no other's.
	cases := []struct {
		auth    string
		region  string
		want    string
		notWant []string
	}{
		{unsigned, "us-east-1", "zero-owned", []string{"test-owned", "west-owned"}},
		{signed, "us-east-1", "test-owned", []string{"zero-owned", "west-owned"}},
		{signedWest, "eu-west-1", "west-owned", []string{"zero-owned", "test-owned"}},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			code, body := batchIdentityRequest(t, ts, tc.region, tc.auth,
				"/v1/describecomputeenvironments", map[string]interface{}{})
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, `"computeEnvironmentName":"`+tc.want+`"`)
			for _, other := range tc.notWant {
				assert.NotContains(t, body, other)
			}

			// And a name filter cannot cross the scope either: naming another
			// caller's environment reports nothing, not that caller's record.
			for _, other := range tc.notWant {
				code, body := batchIdentityRequest(t, ts, tc.region, tc.auth,
					"/v1/describecomputeenvironments",
					map[string]interface{}{"computeEnvironments": []string{other}})
				require.Equal(t, http.StatusOK, code, "body was %s", body)
				assert.Empty(t, batchMembers(t, body, "computeEnvironments"),
					"%s must not be readable by this caller", other)
			}
		})
	}

	// A job definition's revision counter is scoped the same way: two callers each
	// registering one definition of the same name both get revision 1.
	for _, auth := range []string{unsigned, signed} {
		code, body := batchIdentityRequest(t, ts, "us-east-1", auth, "/v1/registerjobdefinition",
			map[string]interface{}{"jobDefinitionName": "shared", "type": "container"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, `"revision":1`,
			"one caller's registrations do not advance another's revision counter")
	}

	// The name *index* has to be scoped too, not only the records.
	//
	// This is worth its own assertion because an unscoped index is invisible to the
	// checks above: a foreign name would resolve to no record for this caller and be
	// skipped, so the result set still looks right. What leaks is the enumeration —
	// pagination runs over the index before the records are loaded, so another
	// caller's names consume this caller's page and the count and token come out
	// wrong. The result would be a caller paging forever through pages that are
	// mysteriously short.
	code, body := batchIdentityRequest(t, ts, "us-east-1", signed,
		"/v1/describecomputeenvironments", map[string]interface{}{"maxResults": 1})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Len(t, batchMembers(t, body, "computeEnvironments"), 1,
		"the caller's one environment fills its one-result page")
	assert.NotContains(t, body, `"nextToken"`,
		"the caller has one environment, so the first page is the last — "+
			"another caller's names must not be counted into the enumeration")
}

// TestBatchDescribe_MissingRequiredParameters covers the validation the persistence
// made necessary: a record keyed by an empty name is unreachable, so an omitted name
// has to be refused rather than stored.
//
// Batch documents exactly two errors for every operation, ClientException (400) and
// ServerException (500), so a parameter complaint is a ClientException — not the
// MissingParameter or InvalidParameterValue other services use.
func TestBatchDescribe_MissingRequiredParameters(t *testing.T) {
	ts := newBatchTestServer(t)

	cases := []struct {
		name string
		path string
		body map[string]interface{}
	}{
		{
			"a compute environment with no name", "/v1/createcomputeenvironment",
			map[string]interface{}{"type": "MANAGED"},
		},
		{
			"a compute environment with no type", "/v1/createcomputeenvironment",
			map[string]interface{}{"computeEnvironmentName": "ce1"},
		},
		{
			"a job queue with no name", "/v1/createjobqueue",
			map[string]interface{}{"priority": 1},
		},
		{
			"a job definition with no name", "/v1/registerjobdefinition",
			map[string]interface{}{"type": "container"},
		},
		{
			"a job definition with no type", "/v1/registerjobdefinition",
			map[string]interface{}{"jobDefinitionName": "jd1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := batchPost(t, ts, tc.path, tc.body)
			require.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Contains(t, body, "ClientException")
		})
	}

	// And none of the refused creates recorded anything, which is the point of
	// refusing them: a half-created resource would be worse than none.
	for member, path := range map[string]string{
		"computeEnvironments": "/v1/describecomputeenvironments",
		"jobQueues":           "/v1/describejobqueues",
		"jobDefinitions":      "/v1/describejobdefinitions",
	} {
		code, body := batchPost(t, ts, path, map[string]interface{}{})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Empty(t, batchMembers(t, body, member))
	}
}

// TestBatchDescribe_EmptyBeforeAnyCreate is the case that used to be an
// unknown-operation error: a describe with nothing created is a 200 and an empty
// list.
func TestBatchDescribe_EmptyBeforeAnyCreate(t *testing.T) {
	for member, path := range map[string]string{
		"computeEnvironments": "/v1/describecomputeenvironments",
		"jobQueues":           "/v1/describejobqueues",
		"jobDefinitions":      "/v1/describejobdefinitions",
	} {
		t.Run(strings.TrimPrefix(path, "/v1/"), func(t *testing.T) {
			ts := newBatchTestServer(t)
			code, body := batchPost(t, ts, path, map[string]interface{}{})
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, `"`+member+`":[]`,
				"an empty result is an empty list, not a null and not a missing member")

			// An empty body at all is the other way an SDK sends this, and it is not
			// a malformed request.
			code, body = batchPost(t, ts, path, nil)
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, `"`+member+`":[]`)
		})
	}
}
