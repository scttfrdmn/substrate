package emulator_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The edges of the Route 53 operations #1613 added: unroutable paths, unreadable bodies, the checks a
// dispatch the gate never sees reaches, store failures, and the seed endpoints' refusals.

// r53Direct sends one request straight to p and returns its AWS error, or nil.
func r53Direct(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, method, path, body string) (*emulator.AWSResponse, *emulator.AWSError, error) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(method, path, body))
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return resp, awsErr, nil
	}
	return resp, nil, err
}

func TestRoute53Edges_UnroutablePathsAndUnreadableBodies(t *testing.T) {
	t.Parallel()
	p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
	for _, tc := range []struct {
		method, path, body, code string
	}{
		{http.MethodPut, "/2013-04-01/healthcheck/abc", "", "InvalidAction"},
		{http.MethodGet, "/2013-04-01/healthcheck/abc/extra", "", "InvalidAction"},
		{http.MethodGet, "/2013-04-01/healthcheck/", "", "InvalidAction"},
		{http.MethodGet, "/2013-04-01/tags/", "", "InvalidAction"},
		{http.MethodGet, "/2013-04-01/tags/hostedzone", "", "InvalidAction"},
		{http.MethodGet, "/2013-04-01/tags/hostedzone/a/b", "", "InvalidAction"},
		{http.MethodDelete, "/2013-04-01/tags/hostedzone/a", "", "InvalidAction"},
		{http.MethodPost, "/2013-04-01/healthcheck", "<nope", "MalformedXML"},
		{http.MethodPost, "/2013-04-01/healthcheck/abc", "<nope", "MalformedXML"},
		{http.MethodPost, "/2013-04-01/tags/hostedzone/Z1", "<nope", "MalformedXML"},
		{http.MethodPost, "/2013-04-01/tags/hostedzone", "<nope", "MalformedXML"},
		{http.MethodPost, "/2013-04-01/hostedzone/Z1", "<nope", "MalformedXML"},
		{http.MethodPost, "/2013-04-01/tags/bogus/x", "", "InvalidInput"},
		{http.MethodPost, "/2013-04-01/healthcheck", `<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><HealthCheckConfig><Type>TCP</Type></HealthCheckConfig></CreateHealthCheckRequest>`, "InvalidInput"},
		{http.MethodPost, "/2013-04-01/healthcheck", `<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>r</CallerReference><HealthCheckConfig></HealthCheckConfig></CreateHealthCheckRequest>`, "InvalidInput"},
		{http.MethodPost, "/2013-04-01/tags/hostedzone", `<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"/>`, "InvalidInput"},
		{http.MethodGet, "/2013-04-01/change/", "", "NoSuchChange"},
	} {
		_, awsErr, err := r53Direct(t, p, ctx, tc.method, tc.path, tc.body)
		require.NoError(t, err, "%s %s", tc.method, tc.path)
		require.NotNil(t, awsErr, "%s %s must be refused", tc.method, tc.path)
		assert.Equal(t, tc.code, awsErr.Code, "%s %s: %s", tc.method, tc.path, awsErr.Message)
	}
}

// A health check is answered only to the account that created it, and an index entry that outlived
// its record is skipped by the listings.
func TestRoute53Edges_HealthChecksAreAccountScoped(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	resp, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("scoped", "192.0.2.10"))
	require.NoError(t, err)
	require.Nil(t, awsErr)
	id := r53HealthCheckIDRe.FindStringSubmatch(string(resp.Body))[1]

	other := *ctx
	other.AccountID = "210987654321"
	_, awsErr, err = r53Direct(t, p, &other, http.MethodGet, "/2013-04-01/healthcheck/"+id, "")
	require.NoError(t, err)
	require.NotNil(t, awsErr)
	assert.Equal(t, "NoSuchHealthCheck", awsErr.Code)

	require.NoError(t, state.Put(context.Background(), "route53", "healthcheck_ids:123456789012", []byte(`["`+id+`","gone"]`)))
	resp, awsErr, err = r53Direct(t, p, ctx, http.MethodGet, "/2013-04-01/healthcheckcount", "")
	require.NoError(t, err)
	require.Nil(t, awsErr)
	assert.Contains(t, string(resp.Body), "<HealthCheckCount>1</HealthCheckCount>")
}

// r53FailingState refuses every write to a key with the armed prefix, and every read of one with the
// armed read prefix.
type r53FailingState struct {
	emulator.StateManager
	putPrefix, getPrefix string
}

func (s *r53FailingState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if s.putPrefix != "" && strings.HasPrefix(key, s.putPrefix) {
		return errors.New("store refused the write")
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

func (s *r53FailingState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if s.getPrefix != "" && strings.HasPrefix(key, s.getPrefix) {
		return nil, errors.New("store refused the read")
	}
	return s.StateManager.Get(ctx, namespace, key)
}

// A store failure is an error the server answers as a 500, never a success that wrote nothing.
func TestRoute53Edges_StoreFailuresAreErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, putPrefix, getPrefix, method, path, body string
	}{
		{"the change record of a zone create", "change:", "", http.MethodPost, "/2013-04-01/hostedzone",
			`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>f.example.com</Name><CallerReference>f2</CallerReference></CreateHostedZoneRequest>`},
		{"a health check record", "healthcheck:", "", http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("fail", "192.0.2.1")},
		{"a caller reference", "healthcheck_ref:", "", http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("fail", "192.0.2.1")},
		{"a caller reference read", "", "healthcheck_ref:", http.MethodPost, "/2013-04-01/healthcheck", r53HealthCheckBody("fail", "192.0.2.1")},
		{"a change read", "", "change:", http.MethodGet, "/2013-04-01/change/C1", ""},
		{"a health check read", "", "healthcheck:", http.MethodGet, "/2013-04-01/healthcheck/x", ""},
		{"a health check index read", "", "healthcheck_ids:", http.MethodGet, "/2013-04-01/healthcheck", ""},
		{"a zone index read", "", "hostedzone_ids:", http.MethodGet, "/2013-04-01/hostedzonesbyname", ""},
		{"a seed read", "", "status:", http.MethodGet, "/2013-04-01/change/C1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &r53FailingState{StateManager: emulator.NewMemoryStateManager()}
			p, ctx := route53DeleteSetup(t, state)
			// A change to read, so the seed read is reached.
			_, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/hostedzone",
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>s.example.com</Name><CallerReference>s</CallerReference></CreateHostedZoneRequest>`)
			require.NoError(t, err)
			require.Nil(t, awsErr)
			if tc.name == "a seed read" {
				keys, listErr := state.List(context.Background(), "route53", "change:")
				require.NoError(t, listErr)
				require.Len(t, keys, 1)
				tc.path = "/2013-04-01/change/" + strings.TrimPrefix(keys[0], "change:")
			}
			state.putPrefix, state.getPrefix = tc.putPrefix, tc.getPrefix
			_, awsErr, err = r53Direct(t, p, ctx, tc.method, tc.path, tc.body)
			assert.Nil(t, awsErr)
			assert.Error(t, err, "the store failure is reported")
		})
	}
}

func TestRoute53Edges_SeedEndpointsRefuseAnUnreadableBody(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, path := range []string{"/v1/route53/change-status", "/v1/route53/health-check-status"} {
		status, body := cpControlPlaneRequest(t, ts, http.MethodPost, path, []byte(`{not json`))
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", path, body)
	}
	status, body := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/route53/health-check-status", []byte(`{"healthy":true}`))
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"healthCheckId":"status:*"`, "an empty ID is the wildcard")

	// A health check that names its Regions is observed from those alone; a healthy seed reports healthy.
	id := r53CreateHealthCheck(t, ts, `<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>regions</CallerReference>`+
		`<HealthCheckConfig><IPAddress>192.0.2.1</IPAddress><Type>TCP</Type><Port>22</Port><Regions><Region>us-east-1</Region>`+
		`<Region>us-west-1</Region><Region>eu-west-1</Region></Regions></HealthCheckConfig></CreateHealthCheckRequest>`)
	code, got := r53Call(t, ts, http.MethodGet, "/2013-04-01/healthcheck/"+id+"/status", "")
	require.Equal(t, http.StatusOK, code, got)
	assert.Equal(t, 3, strings.Count(got, "<HealthCheckObservation>"))
	assert.Contains(t, got, "<Region>eu-west-1</Region><StatusReport><Status>Success: HTTP Status Code: 200. OK</Status>")
}

func TestRoute53Edges_TheStackDeleteChangeCarriesEveryStoredValue(t *testing.T) {
	t.Parallel()
	weight := int64(5)
	got := emulator.CFNRoute53ChangeForTest("DELETE", emulator.Route53ResourceRecordSet{
		Name: "a&b.example.com.", Type: "A", SetIdentifier: "blue", Weight: &weight, Region: "us-east-1",
		Failover: "PRIMARY", HealthCheckID: "hc",
		AliasTarget: &emulator.Route53AliasTarget{HostedZoneID: "Z2", DNSName: "d.cloudfront.net.", EvaluateTargetHealth: true},
	})
	assert.Equal(t, `<Change><Action>DELETE</Action><ResourceRecordSet><Name>a&amp;b.example.com.</Name><Type>A</Type>`+
		`<SetIdentifier>blue</SetIdentifier><Weight>5</Weight><Region>us-east-1</Region><Failover>PRIMARY</Failover>`+
		`<AliasTarget><HostedZoneId>Z2</HostedZoneId><DNSName>d.cloudfront.net.</DNSName><EvaluateTargetHealth>true</EvaluateTargetHealth></AliasTarget>`+
		`<HealthCheckId>hc</HealthCheckId></ResourceRecordSet></Change>`, got, "an alias record set carries no TTL, and the text is escaped")
}

// A record set group whose record sets are all already gone deletes cleanly.
func TestCFNRoute53_ARecordSetGroupWhoseRecordSetsAreGoneDeletes(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{"Resources": {
		"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "gone.example.com"}}}}`, "r53-gone-zone", nil)
	require.NoError(t, err)
	zoneID := cfnRoute53ByLogical(result)["MyZone"].PhysicalID
	group := `{"Resources": {"Group": {"Type": "AWS::Route53::RecordSetGroup", "Properties": {
		"HostedZoneId": "` + zoneID + `", "RecordSets": [{"Name": "g.gone.example.com", "Type": "A", "TTL": "60", "ResourceRecords": "192.0.2.1"}]}}}}`
	for _, stack := range []string{"r53-gone-a", "r53-gone-b"} {
		res, err := d.Deploy(ctx, group, stack, nil)
		require.NoError(t, err)
		require.Equal(t, "CREATE_COMPLETE", res.Status)
	}
	for _, stack := range []string{"r53-gone-a", "r53-gone-b"} {
		for _, del := range d.DeleteStackResourcesForTest(ctx, stack) {
			assert.Equal(t, "DELETE_COMPLETE", del.Status, "%s: %+v", stack, del)
		}
	}
}
