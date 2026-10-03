package emulator_test

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFront invalidations keep and answer the batch they were sent (#1360).
//
// API_Invalidation marks CreateTime, Id, InvalidationBatch and Status Required: Yes, and until #1360
// CreateInvalidation and GetInvalidation answered the first, second and fourth: createInvalidation
// never read its body, so there was no batch to answer. These assertions are on the raw XML, because
// a decode into a struct that omits InvalidationBatch is exactly what hid it.

// cfInvalidationBatch is an InvalidationBatch request body in the published shape.
func cfInvalidationBatch(ref string, quantity int, paths ...string) string {
	var b strings.Builder
	b.WriteString(`<InvalidationBatch xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Paths><Quantity>`)
	b.WriteString(strconv.Itoa(quantity))
	b.WriteString(`</Quantity><Items>`)
	for _, path := range paths {
		b.WriteString("<Path>" + path + "</Path>")
	}
	b.WriteString(`</Items></Paths><CallerReference>` + ref + `</CallerReference></InvalidationBatch>`)
	return b.String()
}

// cfInvalidationHarness is a CloudFront plugin holding one distribution, with a call that answers the
// status and body of one request.
type cfInvalidationHarness struct {
	t      *testing.T
	p      *emulator.CloudFrontPlugin
	ctx    *emulator.RequestContext
	distID string
}

func newCFInvalidationHarness(t *testing.T) *cfInvalidationHarness {
	t.Helper()
	h := &cfInvalidationHarness{t: t, p: &emulator.CloudFrontPlugin{}}
	h.ctx, _ = wireSetup(t, h.p, "req-cf-invalidation")
	status, body := h.call(http.MethodPost, "/2020-05-31/distribution",
		`<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><CallerReference>inv</CallerReference><Comment>inv</Comment><Enabled>true</Enabled></DistributionConfig>`)
	require.Equal(t, http.StatusCreated, status, "CreateDistribution: %s", body)
	m := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(body)
	require.NotNil(t, m, "CreateDistribution must report an Id: %s", body)
	h.distID = m[1]
	return h
}

// call issues one request and returns its status and body, or the refusal's status and code.
func (h *cfInvalidationHarness) call(method, path, body string) (int, string) {
	h.t.Helper()
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "cloudfront", HTTPMethod: method, Path: path, Body: []byte(body),
		Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code
	}
	require.NoError(h.t, err, "%s %s", method, path)
	return resp.StatusCode, string(resp.Body)
}

func (h *cfInvalidationHarness) invalidationPath() string {
	return "/2020-05-31/distribution/" + h.distID + "/invalidation"
}

func TestCloudFrontInvalidation_CreateAndGetAnswerTheBatch(t *testing.T) {
	t.Parallel()
	h := newCFInvalidationHarness(t)

	status, created := h.call(http.MethodPost, h.invalidationPath(), cfInvalidationBatch("batch-1", 2, "/index.html", "/img/*"))
	require.Equal(t, http.StatusCreated, status, "CreateInvalidation: %s", created)
	m := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(created)
	require.NotNil(t, m, "CreateInvalidation must report an Id: %s", created)

	status, got := h.call(http.MethodGet, h.invalidationPath()+"/"+m[1], "")
	require.Equal(t, http.StatusOK, status, "GetInvalidation: %s", got)

	const batch = `<InvalidationBatch><CallerReference>batch-1</CallerReference><Paths><Items><Path>/index.html</Path><Path>/img/*</Path></Items><Quantity>2</Quantity></Paths></InvalidationBatch>`
	for op, body := range map[string]string{"CreateInvalidation": created, "GetInvalidation": got} {
		require.Containsf(t, body, batch, "%s must answer the InvalidationBatch it was sent, which API_Invalidation marks Required: %s", op, body)
		require.Containsf(t, body, "<Status>Completed</Status>", "%s: %s", op, body)
	}
}

func TestCloudFrontInvalidation_RefusesTheBodiesThePagePublishes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body string
		code string
	}{
		{"no body", "", "MissingBody"},
		{"a body that does not parse", "<InvalidationBatch><Paths>", "InvalidArgument"},
		{"no CallerReference", cfInvalidationBatch("", 1, "/*"), "InvalidArgument"},
		{"no paths", cfInvalidationBatch("ref", 0), "InvalidArgument"},
		{"a Quantity that disagrees with Items", cfInvalidationBatch("ref", 3, "/a", "/b"), "InconsistentQuantities"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFInvalidationHarness(t)
			status, code := h.call(http.MethodPost, h.invalidationPath(), tc.body)
			require.Equal(t, http.StatusBadRequest, status, "%s: %s", tc.name, code)
			require.Equal(t, tc.code, code, "%s", tc.name)

			status, list := h.call(http.MethodGet, h.invalidationPath(), "")
			require.Equal(t, http.StatusOK, status, "ListInvalidations: %s", list)
			require.Contains(t, list, "<Quantity>0</Quantity>", "a refused batch must create no invalidation: %s", list)
		})
	}
}

// API_InvalidationBatch: the same CallerReference with the same paths "doesn't create a new
// invalidation request" and returns the one created before.
func TestCloudFrontInvalidation_AResubmittedBatchReturnsTheFirstInvalidation(t *testing.T) {
	t.Parallel()
	h := newCFInvalidationHarness(t)
	body := cfInvalidationBatch("same-ref", 1, "/*")

	_, first := h.call(http.MethodPost, h.invalidationPath(), body)
	status, second := h.call(http.MethodPost, h.invalidationPath(), body)
	require.Equal(t, http.StatusCreated, status, "resubmitted CreateInvalidation: %s", second)
	id := regexp.MustCompile(`<Id>([^<]+)</Id>`)
	require.Equal(t, id.FindStringSubmatch(first)[1], id.FindStringSubmatch(second)[1],
		"a resubmitted batch must answer the first invalidation's Id: %s / %s", first, second)

	_, list := h.call(http.MethodGet, h.invalidationPath(), "")
	require.Contains(t, list, "<Quantity>1</Quantity>", "a resubmitted batch must not add an invalidation: %s", list)

	// A different batch under a new CallerReference is a new invalidation.
	_, third := h.call(http.MethodPost, h.invalidationPath(), cfInvalidationBatch("other-ref", 1, "/*"))
	require.NotEqual(t, id.FindStringSubmatch(first)[1], id.FindStringSubmatch(third)[1], "%s", third)
}
