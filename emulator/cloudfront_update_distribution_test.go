package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// UpdateDistribution replaces the configuration and refuses one CloudFront refuses, and
// DeleteDistribution requires a disabled distribution and the current version (#1271).
//
// Every assertion is on the raw XML or the error code, because the defect was a configuration a
// typed fake accepted: a decode into a struct that omits a member cannot tell whether it was sent.

// cfRMWConfig is a configuration a caller writes from scratch: one S3 origin, a default and one
// path cache behavior, a CallerReference and a PriceClass. It omits the three members CloudFront
// defaults on create and then requires on update — Aliases, the origin's CustomHeaders, and each
// behavior's SmoothStreaming — which is the trap #1271 reports.
const cfRMWConfig = `<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
	`<CallerReference>rmw-1</CallerReference>` +
	`<Origins><Quantity>1</Quantity><Items><Origin><Id>s3-origin</Id><DomainName>bucket.s3.amazonaws.com</DomainName>` +
	`<S3OriginConfig><OriginAccessIdentity></OriginAccessIdentity></S3OriginConfig></Origin></Items></Origins>` +
	`<DefaultCacheBehavior><TargetOriginId>s3-origin</TargetOriginId><ViewerProtocolPolicy>redirect-to-https</ViewerProtocolPolicy></DefaultCacheBehavior>` +
	`<CacheBehaviors><Quantity>1</Quantity><Items><CacheBehavior><PathPattern>/img/*</PathPattern><TargetOriginId>s3-origin</TargetOriginId>` +
	`<ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy></CacheBehavior></Items></CacheBehaviors>` +
	`<Comment>rmw</Comment><Enabled>true</Enabled><PriceClass>PriceClass_100</PriceClass></DistributionConfig>`

// cfDistHarness drives the CloudFront plugin directly, keeping the response headers a test needs
// for the ETag.
type cfDistHarness struct {
	t   *testing.T
	p   *emulator.CloudFrontPlugin
	ctx *emulator.RequestContext
}

func newCFDistHarness(t *testing.T) *cfDistHarness {
	t.Helper()
	h := &cfDistHarness{t: t, p: &emulator.CloudFrontPlugin{}}
	h.ctx, _ = wireSetup(t, h.p, "req-cf-update")
	return h
}

// do issues one request. A refusal answers its status and code as the body; a store fault is
// returned as err.
func (h *cfDistHarness) do(method, path, ifMatch, body string) (status int, out string, headers map[string]string, err error) {
	h.t.Helper()
	reqHeaders := map[string]string{"Content-Type": "application/xml"}
	if ifMatch != "" {
		reqHeaders["If-Match"] = ifMatch
	}
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "cloudfront", HTTPMethod: method, Path: path, Body: []byte(body),
		Headers: reqHeaders, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code + ": " + awsErr.Message, nil, nil
	}
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, string(resp.Body), resp.Headers, nil
}

// must issues one request and requires a 2xx.
func (h *cfDistHarness) must(method, path, ifMatch, body string) (string, map[string]string) {
	h.t.Helper()
	status, out, headers, err := h.do(method, path, ifMatch, body)
	require.NoError(h.t, err, "%s %s", method, path)
	require.Truef(h.t, status >= 200 && status < 300, "%s %s answered %d: %s", method, path, status, out)
	return out, headers
}

// create creates a distribution from config and returns its ID and the ETag the create answered.
func (h *cfDistHarness) create(config string) (id, etag string) {
	h.t.Helper()
	body, headers := h.must(http.MethodPost, "/2020-05-31/distribution", "", config)
	m := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(body)
	require.NotNil(h.t, m, "CreateDistribution must report an Id: %s", body)
	require.NotEmpty(h.t, headers["ETag"], "CreateDistribution must answer an ETag")
	return m[1], headers["ETag"]
}

// config reads a distribution's configuration and ETag.
func (h *cfDistHarness) config(id string) (body, etag string) {
	h.t.Helper()
	body, headers := h.must(http.MethodGet, "/2020-05-31/distribution/"+id+"/config", "", "")
	require.NotEmpty(h.t, headers["ETag"], "GetDistributionConfig must answer an ETag")
	return strings.TrimPrefix(body, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"), headers["ETag"]
}

func TestCloudFrontUpdate_AReadModifyWriteRoundTrips(t *testing.T) {
	t.Parallel()
	h := newCFDistHarness(t)
	id, createdETag := h.create(cfRMWConfig)

	read, etag := h.config(id)
	require.Equal(t, createdETag, etag, "GetDistributionConfig answers the version the create handed out")
	// What the caller sent comes back…
	for _, sent := range []string{
		"<CallerReference>rmw-1</CallerReference>", "<DomainName>bucket.s3.amazonaws.com</DomainName>",
		"<PathPattern>/img/*</PathPattern>", "<PriceClass>PriceClass_100</PriceClass>", "<Comment>rmw</Comment>",
		"<Enabled>true</Enabled>",
	} {
		require.Containsf(t, read, sent, "GetDistributionConfig must answer back what the create sent: %s", read)
	}
	// …with what CloudFront defaults filled in, so the read can be sent back.
	require.Contains(t, read, "<Aliases><Quantity>0</Quantity></Aliases>", "%s", read)
	require.Contains(t, read, "<CustomHeaders><Quantity>0</Quantity></CustomHeaders>", "%s", read)
	require.Equal(t, 2, strings.Count(read, "<SmoothStreaming>false</SmoothStreaming>"),
		"both cache behaviors default SmoothStreaming: %s", read)
	require.NotContains(t, read, "xmlns", "the stored document carries no namespace declarations: %s", read)

	changed := strings.Replace(read, "<Comment>rmw</Comment>", "<Comment>rmw-2</Comment>", 1)
	updated, headers := h.must(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, changed)
	require.Contains(t, updated, "<Comment>rmw-2</Comment>", "UpdateDistribution answers the updated distribution")
	require.NotEmpty(t, headers["ETag"])
	require.NotEqual(t, etag, headers["ETag"], "an update gives the distribution a new version")

	reread, newETag := h.config(id)
	require.Equal(t, headers["ETag"], newETag, "the version the update answered is the current one")
	require.Equal(t, changed, reread, "the configuration is exactly what the update sent")
	_, distHeaders := h.must(http.MethodGet, "/2020-05-31/distribution/"+id, "", "")
	require.Equal(t, newETag, distHeaders["ETag"], "GetDistribution answers the same version")
}

// CreateDistribution keeps its leniency about its own body (#1197's class): a body that does not
// parse, or names no DistributionConfig, still creates a distribution, and the configuration it
// answers is what a create of nothing records — enabled, an empty comment, the defaulted Aliases.
// A create's Enabled false is recorded as such.
func TestCloudFrontCreate_RecordsWhatTheBodyHolds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"a wrapped configuration", "<CreateDistributionRequest><DistributionConfig><Comment>w</Comment><Enabled>false</Enabled></DistributionConfig></CreateDistributionRequest>",
			"<DistributionConfig><Comment>w</Comment><Enabled>false</Enabled><Aliases><Quantity>0</Quantity></Aliases></DistributionConfig>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFDistHarness(t)
			id, _ := h.create(tc.body)
			read, _ := h.config(id)
			require.Equal(t, tc.want, read)
		})
	}
}

// TestCloudFrontCreate_RefusesABodyItCannotRead pins #1133: both creates refuse a body holding no
// readable DistributionConfig with a code both pages publish, where they once created a default,
// enabled distribution and answered 201. Nothing is left behind.
func TestCloudFrontCreate_RefusesABodyItCannotRead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		query string
		body  string
		code  string
	}{
		{"an empty body", "", "", "MissingBody"},
		{"a body that does not parse", "", "<DistributionConfig><Comment>", "InvalidArgument"},
		{"a body naming no DistributionConfig", "", "<Other><Comment>x</Comment></Other>", "InvalidArgument"},
		{"WithTags: an empty body", "?WithTags", "", "MissingBody"},
		{"WithTags: no DistributionConfig child", "?WithTags", "<DistributionConfigWithTags><Tags><Items/></Tags></DistributionConfigWithTags>", "InvalidArgument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFDistHarness(t)
			params := map[string]string{}
			if tc.query != "" {
				params["WithTags"] = "1"
			}
			_, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
				Service: "cloudfront", HTTPMethod: http.MethodPost, Path: "/2020-05-31/distribution", Body: []byte(tc.body),
				Headers: map[string]string{"Content-Type": "application/xml"}, Params: params,
			})
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			require.Equal(t, tc.code, awsErr.Code)
			require.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)

			list, _ := h.must(http.MethodGet, "/2020-05-31/distribution", "", "")
			require.Contains(t, list, "<Quantity>0</Quantity>", "a refused create leaves no distribution behind")
		})
	}
}

// The update replaces the configuration: a member it leaves out is gone, not kept.
func TestCloudFrontUpdate_ReplacesRatherThanMerges(t *testing.T) {
	t.Parallel()
	h := newCFDistHarness(t)
	id, _ := h.create(cfRMWConfig)
	read, etag := h.config(id)

	without := strings.Replace(read, "<PriceClass>PriceClass_100</PriceClass>", "", 1)
	h.must(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, without)
	reread, _ := h.config(id)
	require.NotContains(t, reread, "PriceClass", "a member the update omitted is not merged back in: %s", reread)
}

func TestCloudFrontUpdate_RefusesWhatCloudFrontRefuses(t *testing.T) {
	t.Parallel()
	drop := func(s, element string) string {
		return regexp.MustCompile(`<`+element+`>(?:[^<]|<[^/]|</[^`+element[:1]+`])*?</`+element+`>`).ReplaceAllString(s, "")
	}
	for _, tc := range []struct {
		name   string
		mutate func(read string) string
		status int
		code   string
	}{
		{"no Aliases", func(r string) string { return strings.Replace(r, "<Aliases><Quantity>0</Quantity></Aliases>", "", 1) },
			400, "IllegalUpdate: Aliases are missing for the resource"},
		{"an origin with no CustomHeaders", func(r string) string {
			return strings.Replace(r, "<CustomHeaders><Quantity>0</Quantity></CustomHeaders>", "", 1)
		}, 400, "IllegalUpdate: The 'OriginCustomHeaders' field is missing"},
		{"the default behavior with no SmoothStreaming", func(r string) string {
			return strings.Replace(r, "<SmoothStreaming>false</SmoothStreaming>", "", 1)
		}, 400, "InvalidArgument: The parameter SmoothStreaming flag is missing"},
		{"a path behavior with no SmoothStreaming", func(r string) string {
			i := strings.LastIndex(r, "<SmoothStreaming>false</SmoothStreaming>")
			return r[:i] + r[i+len("<SmoothStreaming>false</SmoothStreaming>"):]
		}, 400, "InvalidArgument: The parameter SmoothStreaming flag is missing"},
		// The order a real account answered them in: with all three missing, Aliases is reported first,
		// and with the other two, CustomHeaders.
		{"all three missing reports Aliases", func(r string) string {
			r = strings.Replace(r, "<Aliases><Quantity>0</Quantity></Aliases>", "", 1)
			r = strings.Replace(r, "<CustomHeaders><Quantity>0</Quantity></CustomHeaders>", "", 1)
			return strings.ReplaceAll(r, "<SmoothStreaming>false</SmoothStreaming>", "")
		}, 400, "IllegalUpdate: Aliases are missing for the resource"},
		{"CustomHeaders and SmoothStreaming missing reports CustomHeaders", func(r string) string {
			r = strings.Replace(r, "<CustomHeaders><Quantity>0</Quantity></CustomHeaders>", "", 1)
			return strings.ReplaceAll(r, "<SmoothStreaming>false</SmoothStreaming>", "")
		}, 400, "IllegalUpdate: The 'OriginCustomHeaders' field is missing"},
		{"no Comment", func(r string) string { return drop(r, "Comment") }, 400, "InvalidArgument: The parameter Comment is required."},
		{"no Enabled", func(r string) string { return strings.Replace(r, "<Enabled>true</Enabled>", "", 1) },
			400, "InvalidArgument: The parameter Enabled is required."},
		{"an Enabled that is not a boolean", func(r string) string {
			return strings.Replace(r, "<Enabled>true</Enabled>", "<Enabled>yes</Enabled>", 1)
		}, 400, "InvalidArgument: The parameter Enabled must be true or false."},
		{"a changed CallerReference", func(r string) string {
			return strings.Replace(r, "<CallerReference>rmw-1</CallerReference>", "<CallerReference>rmw-2</CallerReference>", 1)
		}, 400, "IllegalUpdate: The CallerReference of a distribution cannot be changed."},
		{"a dropped CallerReference", func(r string) string {
			return strings.Replace(r, "<CallerReference>rmw-1</CallerReference>", "", 1)
		}, 400, "IllegalUpdate: The CallerReference of a distribution cannot be changed."},
		{"no body", func(string) string { return "" },
			400, "MissingBody: This operation requires a body. Ensure that the body is present and the Content-Type header is set."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFDistHarness(t)
			id, _ := h.create(cfRMWConfig)
			read, etag := h.config(id)

			status, out, _, err := h.do(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, tc.mutate(read))
			require.NoError(t, err)
			require.Equal(t, tc.status, status, "%s: %s", tc.name, out)
			require.Equal(t, tc.code, out, "%s", tc.name)

			reread, afterETag := h.config(id)
			require.Equal(t, read, reread, "a refused update changes nothing")
			require.Equal(t, etag, afterETag, "a refused update keeps the version")
		})
	}

	t.Run("a body that does not parse", func(t *testing.T) {
		t.Parallel()
		h := newCFDistHarness(t)
		id, etag := h.create(cfRMWConfig)
		status, out, _, err := h.do(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, "<DistributionConfig><Comment>")
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, status, "%s", out)
		require.True(t, strings.HasPrefix(out, "InvalidArgument: "), "%s", out)
	})
}

func TestCloudFrontUpdate_RequiresTheCurrentVersion(t *testing.T) {
	t.Parallel()
	h := newCFDistHarness(t)
	id, _ := h.create(cfRMWConfig)
	read, etag := h.config(id)
	path := "/2020-05-31/distribution/" + id + "/config"

	for _, tc := range []struct {
		name    string
		ifMatch string
		status  int
		code    string
	}{
		{"no If-Match", "", http.StatusBadRequest, "InvalidIfMatchVersion"},
		{"an If-Match substrate could not have issued", "not-a-version", http.StatusBadRequest, "InvalidIfMatchVersion"},
		{"a well-formed version that is not the current one", "EAAAAAAAAAAAAA", http.StatusPreconditionFailed, "PreconditionFailed"},
	} {
		status, out, _, err := h.do(http.MethodPut, path, tc.ifMatch, read)
		require.NoError(t, err)
		require.Equal(t, tc.status, status, "%s: %s", tc.name, out)
		require.True(t, strings.HasPrefix(out, tc.code+": "), "%s: %s", tc.name, out)
	}

	// The version a successful update replaces is stale from then on.
	h.must(http.MethodPut, path, `"`+etag+`"`, read) // a quoted ETag means the version it quotes
	status, out, _, err := h.do(http.MethodPut, path, etag, read)
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, status, "the replaced version is stale: %s", out)
}

func TestCloudFrontDelete_RequiresADisabledDistributionAndTheCurrentVersion(t *testing.T) {
	t.Parallel()
	h := newCFDistHarness(t)
	id, etag := h.create(cfRMWConfig)
	path := "/2020-05-31/distribution/" + id

	for _, tc := range []struct {
		name    string
		ifMatch string
		status  int
		code    string
	}{
		{"no If-Match", "", http.StatusBadRequest, "InvalidIfMatchVersion"},
		{"a stale version", "EAAAAAAAAAAAAA", http.StatusPreconditionFailed, "PreconditionFailed"},
		{"an enabled distribution", etag, http.StatusConflict,
			"DistributionNotDisabled: The specified CloudFront distribution is not disabled. You must disable the distribution before you can delete it."},
	} {
		status, out, _, err := h.do(http.MethodDelete, path, tc.ifMatch, "")
		require.NoError(t, err)
		require.Equal(t, tc.status, status, "%s: %s", tc.name, out)
		require.True(t, strings.HasPrefix(out, tc.code), "%s: %s", tc.name, out)
	}
	h.must(http.MethodGet, path, "", "") // every refusal left it in place

	read, etag := h.config(id)
	_, headers := h.must(http.MethodPut, path+"/config", etag,
		strings.Replace(read, "<Enabled>true</Enabled>", "<Enabled>false</Enabled>", 1))
	status, out, _, err := h.do(http.MethodDelete, path, etag, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusPreconditionFailed, status, "the If-Match is the ETag the disable answered, not the one before it: %s", out)

	status, out, _, err = h.do(http.MethodDelete, path, headers["ETag"], "")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, status, "%s", out)
	status, _, _, err = h.do(http.MethodGet, path, "", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, status)
}

// A record written before #1271 has no stored configuration and no ETag. It answers the
// configuration a create of its Comment and Enabled would have recorded, under its own ID as its
// version, and a read-modify-write of it works.
func TestCloudFrontUpdate_ARecordWrittenBeforeTheFixStillUpdates(t *testing.T) {
	t.Parallel()
	p := &emulator.CloudFrontPlugin{}
	ctx, state := wireSetup(t, p, "req-cf-legacy")
	h := &cfDistHarness{t: t, p: p, ctx: ctx}
	const id = "ELEGACY0000001"
	legacy, err := json.Marshal(map[string]any{
		"Id": id, "ARN": "arn:aws:cloudfront::123456789012:distribution/" + id, "Status": "Deployed",
		"DomainName": id + ".cloudfront.net", "Comment": "old", "Enabled": true,
		"CreatedTime": "2023-11-14T22:13:20Z", "LastModifiedTime": "2023-11-14T22:13:20Z", "AccountID": "123456789012",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "cloudfront", "cfdist:123456789012/"+id, legacy))

	read, etag := h.config(id)
	require.Equal(t, id, etag, "a record with no ETag answers its own ID as its version")
	require.Equal(t, "<DistributionConfig><Aliases><Quantity>0</Quantity></Aliases><Comment>old</Comment><Enabled>true</Enabled></DistributionConfig>", read)

	h.must(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag,
		strings.Replace(read, "<Comment>old</Comment>", "<Comment>new</Comment>", 1))
	reread, newETag := h.config(id)
	require.Contains(t, reread, "<Comment>new</Comment>")
	require.NotEqual(t, id, newETag, "the first update gives it a minted version")
}

// A store fault, or a stored configuration that no longer parses, is an error, never an empty
// configuration a caller could send back over the real one.
func TestCloudFrontUpdate_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		method string
		suffix string
		body   bool
	}{
		{"GetDistributionConfig", http.MethodGet, "/config", false},
		{"UpdateDistribution", http.MethodPut, "/config", true},
	} {
		t.Run(tc.name+", a stored configuration that does not parse", func(t *testing.T) {
			t.Parallel()
			p := &emulator.CloudFrontPlugin{}
			ctx, state := wireSetup(t, p, "req-cf-corrupt")
			h := &cfDistHarness{t: t, p: p, ctx: ctx}
			id, etag := h.create(cfRMWConfig)

			key := "cfdist:123456789012/" + id
			data, err := state.Get(t.Context(), "cloudfront", key)
			require.NoError(t, err)
			var record map[string]any
			require.NoError(t, json.Unmarshal(data, &record))
			record["Config"] = "<DistributionConfig><Comment>"
			data, err = json.Marshal(record)
			require.NoError(t, err)
			require.NoError(t, state.Put(t.Context(), "cloudfront", key, data))

			body := ""
			if tc.body {
				body = cfRMWConfig
			}
			_, _, _, err = h.do(tc.method, "/2020-05-31/distribution/"+id+tc.suffix, etag, body)
			require.Error(t, err, "%s must fail on a stored configuration it cannot read", tc.name)
		})
	}

	t.Run("UpdateDistribution, record write", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		p := cfOACPluginOn(t, fault)
		h := &cfDistHarness{t: t, p: p, ctx: &emulator.RequestContext{
			AccountID: "123456789012", Region: "us-east-1", RequestID: "req-cf-fault", IDs: emulator.NewIDMint("req-cf-fault"),
		}}
		id, _ := h.create(cfRMWConfig)
		read, etag := h.config(id)
		fault.failPut = "cfdist:"
		_, _, _, err := h.do(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", etag, read)
		require.Error(t, err, "a failed record write must not answer the update's success")
	})
}
