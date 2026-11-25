package emulator_test

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Health checks, Route 53's tag operations, ListHostedZonesByName and UpdateHostedZoneComment
// (#1613), over the wire.

var r53HealthCheckIDRe = regexp.MustCompile(`<HealthCheck><Id>([^<]+)</Id>`)

// r53HealthCheckBody is a CreateHealthCheck request for an HTTP check of ip under ref.
func r53HealthCheckBody(ref, ip string) string {
	return `<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>` + ref +
		`</CallerReference><HealthCheckConfig><IPAddress>` + ip + `</IPAddress><Port>80</Port><Type>HTTP</Type>` +
		`<ResourcePath>/health</ResourcePath></HealthCheckConfig></CreateHealthCheckRequest>`
}

// r53CreateHealthCheck creates a health check and returns its ID.
func r53CreateHealthCheck(t *testing.T, ts *emulator.TestServer, body string) string {
	t.Helper()
	code, resp := r53Call(t, ts, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, code, resp)
	m := r53HealthCheckIDRe.FindStringSubmatch(resp)
	require.NotNil(t, m, resp)
	return m[1]
}

// r53Code asserts a request is refused with code and status, and returns the message.
func r53Code(t *testing.T, ts *emulator.TestServer, method, path, body, code string, status int) string {
	t.Helper()
	gotStatus, resp := r53Call(t, ts, method, path, body)
	assert.Equal(t, status, gotStatus, "%s %s: %s", method, path, resp)
	gotCode, msg := r53ErrorOf(resp)
	assert.Equal(t, code, gotCode, "%s %s: %s", method, path, resp)
	return msg
}

func TestRoute53HealthCheck_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	id := r53CreateHealthCheck(t, ts, r53HealthCheckBody("hc-1", "192.0.2.10"))

	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck/"+id, "")
	require.Equal(t, http.StatusOK, code, body)
	for _, want := range []string{"<CallerReference>hc-1</CallerReference>", "<IPAddress>192.0.2.10</IPAddress>",
		"<RequestInterval>30</RequestInterval>", "<FailureThreshold>3</FailureThreshold>", "<HealthCheckVersion>1</HealthCheckVersion>"} {
		assert.Contains(t, body, want, "the published defaults are filled in")
	}

	update := func(version, extra string) (int, string) {
		v := ""
		if version != "" {
			v = "<HealthCheckVersion>" + version + "</HealthCheckVersion>"
		}
		return r53Call(t, ts, http.MethodPost, "/2013-04-01/healthcheck/"+id,
			`<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">`+v+extra+`</UpdateHealthCheckRequest>`)
	}
	code, body = update("1", "<FailureThreshold>5</FailureThreshold><Inverted>true</Inverted><ResetElements><ResettableElementName>ResourcePath</ResettableElementName></ResetElements>")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<HealthCheckVersion>2</HealthCheckVersion>")
	assert.Contains(t, body, "<FailureThreshold>5</FailureThreshold>")
	assert.Contains(t, body, "<Inverted>true</Inverted>")
	assert.NotContains(t, body, "<ResourcePath>", "ResetElements cleared it")

	assert.Equal(t, "The value of HealthCheckVersion in the request doesn't match the value of HealthCheckVersion in the health check.",
		r53Code(t, ts, http.MethodPost, "/2013-04-01/healthcheck/"+id,
			`<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><HealthCheckVersion>1</HealthCheckVersion></UpdateHealthCheckRequest>`,
			"HealthCheckVersionMismatch", http.StatusConflict))
	code, body = update("", "<Disabled>true</Disabled>")
	require.Equal(t, http.StatusOK, code, "an update without a version is applied: %s", body)
	assert.Contains(t, body, "<HealthCheckVersion>3</HealthCheckVersion>")

	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheckcount", "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<HealthCheckCount>1</HealthCheckCount>")

	code, body = r53Call(t, ts, http.MethodDelete, "/2013-04-01/healthcheck/"+id, "")
	require.Equal(t, http.StatusOK, code, body)
	for _, req := range [][2]string{{http.MethodGet, ""}, {http.MethodDelete, ""}, {http.MethodGet, "/status"}} {
		assert.Equal(t, "No health check exists with the specified ID.",
			r53Code(t, ts, req[0], "/2013-04-01/healthcheck/"+id+req[1], "", "NoSuchHealthCheck", http.StatusNotFound))
	}
	r53Code(t, ts, http.MethodPost, "/2013-04-01/healthcheck/"+id,
		`<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"/>`, "NoSuchHealthCheck", http.StatusNotFound)
}

func TestRoute53HealthCheck_CallerReferenceIsIdempotent(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	id := r53CreateHealthCheck(t, ts, r53HealthCheckBody("same", "192.0.2.10"))
	assert.Equal(t, id, r53CreateHealthCheck(t, ts, r53HealthCheckBody("same", "192.0.2.10")),
		"the same caller reference and settings answers the existing health check")
	assert.Equal(t, "The health check you're attempting to create already exists.",
		r53Code(t, ts, http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("same", "192.0.2.11"),
			"HealthCheckAlreadyExists", http.StatusConflict))
	assert.NotEqual(t, id, r53CreateHealthCheck(t, ts, r53HealthCheckBody("other", "192.0.2.10")),
		"a new caller reference with the same settings is a new health check")

	code, body := r53Call(t, ts, http.MethodDelete, "/2013-04-01/healthcheck/"+id, "")
	require.Equal(t, http.StatusOK, code, body)
	r53Code(t, ts, http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("same", "192.0.2.10"),
		"HealthCheckAlreadyExists", http.StatusConflict)
}

func TestRoute53HealthCheck_ListPagesByMarker(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	var ids []string
	for i := range 3 {
		ids = append(ids, r53CreateHealthCheck(t, ts, r53HealthCheckBody(fmt.Sprintf("list-%d", i), "192.0.2.10")))
	}
	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck?maxitems=2", "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<IsTruncated>true</IsTruncated>")
	assert.Contains(t, body, "<NextMarker>"+ids[2]+"</NextMarker>")
	assert.Contains(t, body, "<MaxItems>2</MaxItems>")
	assert.Equal(t, 2, strings.Count(body, "<HealthCheck>"))

	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck?maxitems=2&marker="+ids[2], "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<Marker>"+ids[2]+"</Marker>")
	assert.Contains(t, body, "<IsTruncated>false</IsTruncated>")
	assert.NotContains(t, body, "<NextMarker>")

	r53Code(t, ts, http.MethodGet, "/2013-04-01/healthcheck?marker=nope", "", "InvalidInput", http.StatusBadRequest)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/healthcheck?maxitems=zero", "", "InvalidInput", http.StatusBadRequest)
}

func TestRoute53HealthCheck_TheAccountLimitIsTooManyHealthChecks(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 200 {
		r53CreateHealthCheck(t, ts, r53HealthCheckBody(fmt.Sprintf("limit-%d", i), "192.0.2.10"))
	}
	r53Code(t, ts, http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("limit-200", "192.0.2.10"),
		"TooManyHealthChecks", http.StatusBadRequest)
}

func TestRoute53HealthCheck_StatusIsSeeded(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	id := r53CreateHealthCheck(t, ts, r53HealthCheckBody("status", "192.0.2.10"))
	status := func() string {
		code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck/"+id+"/status", "")
		require.Equal(t, http.StatusOK, code, body)
		assert.Equal(t, 8, strings.Count(body, "<HealthCheckObservation>"), "one observation per default checker Region")
		return regexp.MustCompile(`<Status>([^<]+)</Status>`).FindStringSubmatch(body)[1]
	}
	assert.Equal(t, "Success: HTTP Status Code: 200. OK", status(), "unseeded is healthy")

	seed := func(body string) {
		code, got := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/route53/health-check-status", []byte(body))
		require.Equal(t, http.StatusOK, code, got)
	}
	seed(`{"healthCheckId":"*","healthy":false}`)
	assert.True(t, strings.HasPrefix(status(), "Failure:"), "the wildcard seed governs every health check")
	seed(`{"healthCheckId":"` + id + `","statusReport":"Failure: Connection timed out."}`)
	assert.Equal(t, "Failure: Connection timed out.", status(), "the health check's own seed wins")

	code, got := cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/route53/health-check-status?healthCheckId="+id, nil)
	require.Equal(t, http.StatusOK, code, got)
	assert.True(t, strings.HasPrefix(status(), "Failure:"))
	code, got = cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/route53/health-check-status", nil)
	require.Equal(t, http.StatusOK, code, got)
	assert.Equal(t, "Success: HTTP Status Code: 200. OK", status())

	calc := r53CreateHealthCheck(t, ts, `<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>calc</CallerReference>`+
		`<HealthCheckConfig><Type>CALCULATED</Type><HealthThreshold>1</HealthThreshold><ChildHealthChecks><ChildHealthCheck>`+id+
		`</ChildHealthCheck></ChildHealthChecks></HealthCheckConfig></CreateHealthCheckRequest>`)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/healthcheck/"+calc+"/status", "", "InvalidInput", http.StatusBadRequest)
	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck/"+calc, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.NotContains(t, body, "<RequestInterval>", "a CALCULATED check takes no endpoint defaults")
}

func TestRoute53Tags_HostedZonesAndHealthChecks(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, "tags.example.com", "tags")
	hcID := r53CreateHealthCheck(t, ts, r53HealthCheckBody("tags", "192.0.2.10"))
	change := func(resourceType, id, body string) (int, string) {
		return r53Call(t, ts, http.MethodPost, "/2013-04-01/tags/"+resourceType+"/"+id,
			`<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">`+body+`</ChangeTagsForResourceRequest>`)
	}
	for _, target := range [][2]string{{"hostedzone", zoneID}, {"healthcheck", hcID}} {
		code, body := change(target[0], target[1], `<AddTags><Tag><Key>env</Key><Value>dev</Value></Tag><Tag><Key>team</Key><Value>a</Value></Tag></AddTags>`)
		require.Equal(t, http.StatusOK, code, body)
		code, body = change(target[0], target[1], `<AddTags><Tag><Key>env</Key><Value>prod</Value></Tag></AddTags><RemoveTagKeys><Key>team</Key></RemoveTagKeys>`)
		require.Equal(t, http.StatusOK, code, body)
		code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/tags/"+target[0]+"/"+target[1], "")
		require.Equal(t, http.StatusOK, code, body)
		assert.Contains(t, body, "<ResourceType>"+target[0]+"</ResourceType><ResourceId>"+target[1]+"</ResourceId>"+
			"<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>")
	}

	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/tags/hostedzone",
		`<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ResourceIds><ResourceId>`+zoneID+`</ResourceId></ResourceIds></ListTagsForResourcesRequest>`)
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, 1, strings.Count(body, "<ResourceTagSet>"), body)

	r53Code(t, ts, http.MethodGet, "/2013-04-01/tags/hostedzone/ZNOSUCHZONE", "", "NoSuchHostedZone", http.StatusNotFound)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/tags/healthcheck/nope", "", "NoSuchHealthCheck", http.StatusNotFound)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/tags/healthchecksource/nope", "", "NoSuchHealthCheckSource", http.StatusNotFound)
	r53Code(t, ts, http.MethodPost, "/2013-04-01/tags/hostedzone",
		`<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ResourceIds><ResourceId>ZNOSUCHZONE</ResourceId></ResourceIds></ListTagsForResourcesRequest>`,
		"NoSuchHostedZone", http.StatusNotFound)

	var many strings.Builder
	for i := range 10 {
		fmt.Fprintf(&many, "<Tag><Key>k%d</Key><Value>v</Value></Tag>", i)
	}
	code, body = change("hostedzone", zoneID, "<AddTags>"+many.String()+"</AddTags>")
	assert.Equal(t, http.StatusBadRequest, code, "env plus ten more is over the limit of 10: %s", body)
	gotCode, _ := r53ErrorOf(body)
	assert.Equal(t, "InvalidInput", gotCode)

	// The tags go with the zone.
	code, body = r53Call(t, ts, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, code, body)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/tags/hostedzone/"+zoneID, "", "NoSuchHostedZone", http.StatusNotFound)
}

func TestRoute53Zones_ListHostedZonesByNameSortsByReversedLabels(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	names := []string{"www.example.com", "example.org", "a.example.com", "example.com"}
	ids := map[string]string{}
	for i, n := range names {
		id, _, _ := r53CreateZoneOn(t, ts, n, fmt.Sprintf("byname-%d", i))
		ids[n] = id
	}
	order := regexp.MustCompile(`<Name>([^<]+)</Name>`)
	listed := func(query string) ([]string, string) {
		code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname"+query, "")
		require.Equal(t, http.StatusOK, code, body)
		var out []string
		for _, m := range order.FindAllStringSubmatch(body, -1) {
			out = append(out, m[1])
		}
		return out, body
	}
	got, _ := listed("")
	assert.Equal(t, []string{"example.com.", "a.example.com.", "www.example.com.", "example.org."}, got,
		"com.example. < com.example.a. < com.example.www. < org.example.")

	got, body := listed("?maxitems=2")
	assert.Equal(t, []string{"example.com.", "a.example.com."}, got)
	assert.Contains(t, body, "<IsTruncated>true</IsTruncated>")
	assert.Contains(t, body, "<NextDNSName>www.example.com.</NextDNSName><NextHostedZoneId>"+ids["www.example.com"]+"</NextHostedZoneId>")

	got, body = listed("?dnsname=www.example.com.&hostedzoneid=" + ids["www.example.com"] + "&maxitems=2")
	assert.Equal(t, []string{"www.example.com.", "example.org."}, got)
	assert.Contains(t, body, "<DNSName>www.example.com.</DNSName><HostedZoneId>"+ids["www.example.com"]+"</HostedZoneId>")
	assert.Contains(t, body, "<IsTruncated>false</IsTruncated>")
	assert.NotContains(t, body, "<NextDNSName>")

	got, _ = listed("?dnsname=b.example.com")
	assert.Equal(t, []string{"www.example.com.", "example.org."}, got, "a dnsname no zone has starts at the next name")

	r53Code(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname?hostedzoneid=Z1", "", "InvalidInput", http.StatusBadRequest)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname?dnsname=a..b", "", "InvalidDomainName", http.StatusBadRequest)
	r53Code(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname?dnsname="+strings.Repeat("x", 64)+".com", "", "InvalidDomainName", http.StatusBadRequest)
}

func TestRoute53Zones_UpdateHostedZoneCommentReplacesAndClears(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, "comment.example.com", "comment")
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID,
		`<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Comment>for testing</Comment></UpdateHostedZoneCommentRequest>`)
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<Comment>for testing</Comment>")
	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<Comment>for testing</Comment>")

	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID,
		`<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"></UpdateHostedZoneCommentRequest>`)
	require.Equal(t, http.StatusOK, code, body)
	assert.NotContains(t, body, "<Comment>", "an absent Comment clears it")
	r53Code(t, ts, http.MethodPost, "/2013-04-01/hostedzone/ZNOSUCHZONE",
		`<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"/>`, "NoSuchHostedZone", http.StatusNotFound)
}

func TestCFNRoute53_AHealthCheckDeploysWithItsTagsAndDeletes(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{
		"Resources": {
			"Check": {"Type": "AWS::Route53::HealthCheck", "Properties": {
				"HealthCheckConfig": {"Type": "HTTPS", "FullyQualifiedDomainName": "app.example.com", "Port": 443,
					"ResourcePath": "/health", "Regions": ["us-east-1", "us-west-1", "eu-west-1"], "EnableSNI": true},
				"HealthCheckTags": [{"Key": "Name", "Value": "app"}]}}
		},
		"Outputs": {"Ref": {"Value": {"Ref": "Check"}}, "Id": {"Value": {"Fn::GetAtt": ["Check", "HealthCheckId"]}}}
	}`, "r53-healthcheck", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	check := cfnRoute53ByLogical(result)["Check"]
	require.Empty(t, check.Error)
	require.NotEmpty(t, check.PhysicalID)
	assert.Equal(t, check.PhysicalID, result.Outputs["Ref"])
	assert.Equal(t, check.PhysicalID, result.Outputs["Id"])

	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-healthcheck") {
		assert.Equal(t, "DELETE_COMPLETE", del.Status, "%+v", del)
	}
}
