package emulator_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// No ELB response carried the Query protocol's `ResponseMetadata` — #1149.
//
// Every published sample response on every Elastic Load Balancing page, in both generations, closes
// with `<ResponseMetadata><RequestId>…</RequestId></ResponseMetadata>`, and substrate emitted none of
// it: each handler built its own inline response struct and not one of them declared the member. So a
// consumer's logging or correlation wrapper reading `ResponseMetadata.RequestId` off a successful ELB
// call read the empty string on every operation, silently.
//
// The table below is **exhaustive rather than representative**, which is the point. The defect was
// uniform across 30 response-construction sites precisely because the member was a per-handler
// convention that every handler happened to omit; an assertion on three shapes would pass again the
// first time a new handler builds its own response, which is the same hole under a new name. Asserting
// every routed action is what makes the envelope a rule: a handler that does not answer it fails here
// by name.
//
// The request id itself is asserted twice more, because "an element is present" is the weaker half of
// what #1149 asks for. [TestELBResponseMetadata_TheIDIsTheOneTheEventRecorded] pins that the value is
// the request's own — the id [emulator.Event] recorded, over the wire — and
// [TestELBResponseMetadata_AReplayReproducesTheRecordedRequestID] pins the consequence a
// deterministic emulator needs: a minted-per-call id would make every ELB response body differ from
// its recording, which is #856's debt arriving in a new plugin.

// elbClassicNamespaceURI is the 2012-06-01 document namespace as a bare URI.
//
// [elbClassicNamespace] spells the whole `xmlns="…"` attribute because its own assertion is on raw
// bytes; this is the same value in the form [xml.Name.Space] reports after a decode.
const elbClassicNamespaceURI = "http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/"

// elbRoutedOperationCount is how many actions [emulator.ELBPlugin] routes: 24 ELBv2 actions plus the
// three names the classic API discriminates by `Version`.
//
// Spelled as a number the table is checked against, so adding a handler without adding a row fails
// rather than quietly narrowing the sweep this file exists to be.
const elbRoutedOperationCount = 27

// elbEnvelope is the part of an ELB response every operation shares.
//
// `XMLName` is read rather than tagged so one type decodes all 27 documents, and it carries the
// namespace the root element declared — so the generation is asserted from the same decode.
type elbEnvelope struct {
	XMLName   xml.Name
	RequestID string `xml:"ResponseMetadata>RequestId"`
}

// elbDecodeEnvelope sends one ELB request and decodes the envelope out of its response.
func elbDecodeEnvelope(t *testing.T, baseURL string, params map[string]string) elbEnvelope {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, params["Action"])

	var env elbEnvelope
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&env), params["Action"])
	return env
}

// elbClassicParams is a classic request: the action, the `Version` that routes it, and extra.
func elbClassicParams(action string, extra map[string]string) map[string]string {
	params := map[string]string{"Action": action, "Version": elbClassicVersion}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// TestELBResponseMetadata_EveryRoutedOperationAnswersTheEnvelope walks all 27 routed actions and
// asserts each answers the published root element, its own generation's namespace and a non-empty
// `RequestId`.
//
// The rows run in order against one server rather than in parallel subtests, because five of them
// delete what they are called on: each of those gets a resource of its own, created up front, so the
// shared load balancer, target group, listener and rule the later rows address survive the sweep.
func TestELBResponseMetadata_EveryRoutedOperationAnswersTheEnvelope(t *testing.T) {
	t.Parallel()
	ts := newELBTestServer(t)

	lb := elbCreateLB(t, ts.URL, "meta-alb", nil)
	tg := elbCreateTG(t, ts.URL, "meta-tg", nil)
	listener := elbCreateListener(t, ts.URL, lb, tg, nil)
	rule := elbCreateRule(t, ts.URL, listener, tg, nil)

	doomedLB := elbCreateLB(t, ts.URL, "meta-doomed-alb", nil)
	doomedTG := elbCreateTG(t, ts.URL, "meta-doomed-tg", nil)
	doomedListener := elbCreateListener(t, ts.URL, lb, tg, map[string]string{"Port": "8080"})
	doomedRule := elbCreateRule(t, ts.URL, listener, tg, map[string]string{"Priority": "20"})
	elbClassicCreate(t, ts.URL, "meta-doomed-classic", nil)

	const target = "i-0123456789abcdef0"
	rows := []struct {
		// name distinguishes the two generations' three shared action names.
		name string

		// operation is the action, and therefore the root element's name less `Response`.
		operation string

		// xmlns is the generation's document namespace.
		xmlns string

		// params is the request, less the action the row already names.
		params map[string]string
	}{
		{name: "CreateLoadBalancer", operation: "CreateLoadBalancer", xmlns: elbV2Namespace,
			params: map[string]string{"Name": "meta-row-alb", "Type": "application"}},
		{name: "DescribeLoadBalancers", operation: "DescribeLoadBalancers", xmlns: elbV2Namespace},
		{name: "DescribeLoadBalancerAttributes", operation: "DescribeLoadBalancerAttributes",
			xmlns: elbV2Namespace, params: map[string]string{"LoadBalancerArn": lb}},
		{name: "ModifyLoadBalancerAttributes", operation: "ModifyLoadBalancerAttributes",
			xmlns: elbV2Namespace, params: map[string]string{"LoadBalancerArn": lb}},
		{name: "DeleteLoadBalancer", operation: "DeleteLoadBalancer", xmlns: elbV2Namespace,
			params: map[string]string{"LoadBalancerArn": doomedLB}},

		{name: "CreateTargetGroup", operation: "CreateTargetGroup", xmlns: elbV2Namespace,
			params: map[string]string{"Name": "meta-row-tg", "Protocol": "HTTP", "Port": "80"}},
		{name: "DescribeTargetGroups", operation: "DescribeTargetGroups", xmlns: elbV2Namespace},
		{name: "ModifyTargetGroup", operation: "ModifyTargetGroup", xmlns: elbV2Namespace,
			params: map[string]string{"TargetGroupArn": tg, "HealthCheckPath": "/healthz"}},
		{name: "RegisterTargets", operation: "RegisterTargets", xmlns: elbV2Namespace,
			params: map[string]string{
				"TargetGroupArn":        tg,
				"Targets.member.1.Id":   target,
				"Targets.member.1.Port": "80",
			}},
		{name: "DescribeTargetHealth", operation: "DescribeTargetHealth", xmlns: elbV2Namespace,
			params: map[string]string{"TargetGroupArn": tg}},
		{name: "DeregisterTargets", operation: "DeregisterTargets", xmlns: elbV2Namespace,
			params: map[string]string{"TargetGroupArn": tg, "Targets.member.1.Id": target}},
		{name: "DeleteTargetGroup", operation: "DeleteTargetGroup", xmlns: elbV2Namespace,
			params: map[string]string{"TargetGroupArn": doomedTG}},

		{name: "CreateListener", operation: "CreateListener", xmlns: elbV2Namespace,
			params: map[string]string{
				"LoadBalancerArn":                        lb,
				"Protocol":                               "HTTP",
				"Port":                                   "9090",
				"DefaultActions.member.1.Type":           "forward",
				"DefaultActions.member.1.TargetGroupArn": tg,
			}},
		{name: "DescribeListeners", operation: "DescribeListeners", xmlns: elbV2Namespace,
			params: map[string]string{"LoadBalancerArn": lb}},
		{name: "ModifyListener", operation: "ModifyListener", xmlns: elbV2Namespace,
			params: map[string]string{"ListenerArn": listener, "Port": "81"}},
		{name: "DeleteListener", operation: "DeleteListener", xmlns: elbV2Namespace,
			params: map[string]string{"ListenerArn": doomedListener}},

		{name: "CreateRule", operation: "CreateRule", xmlns: elbV2Namespace,
			params: map[string]string{
				"ListenerArn":                         listener,
				"Priority":                            "30",
				"Conditions.member.1.Field":           "path-pattern",
				"Conditions.member.1.Values.member.1": "/meta/*",
				"Actions.member.1.Type":               "forward",
				"Actions.member.1.TargetGroupArn":     tg,
			}},
		{name: "DescribeRules", operation: "DescribeRules", xmlns: elbV2Namespace,
			params: map[string]string{"ListenerArn": listener}},
		{name: "SetRulePriorities", operation: "SetRulePriorities", xmlns: elbV2Namespace,
			params: map[string]string{
				"RulePriorities.member.1.RuleArn":  rule,
				"RulePriorities.member.1.Priority": "40",
			}},
		{name: "DeleteRule", operation: "DeleteRule", xmlns: elbV2Namespace,
			params: map[string]string{"RuleArn": doomedRule}},

		{name: "AddTags", operation: "AddTags", xmlns: elbV2Namespace,
			params: map[string]string{
				"ResourceArns.member.1": lb,
				"Tags.member.1.Key":     "env",
				"Tags.member.1.Value":   "prod",
			}},
		{name: "DescribeTags", operation: "DescribeTags", xmlns: elbV2Namespace,
			params: map[string]string{"ResourceArns.member.1": lb}},
		{name: "RemoveTags", operation: "RemoveTags", xmlns: elbV2Namespace,
			params: map[string]string{"ResourceArns.member.1": lb, "TagKeys.member.1": "env"}},
		{name: "DescribeAccountLimits", operation: "DescribeAccountLimits", xmlns: elbV2Namespace},

		{name: "classic CreateLoadBalancer", operation: "CreateLoadBalancer",
			xmlns: elbClassicNamespaceURI, params: elbClassicCreateParams("meta-row-classic", nil)},
		{name: "classic DescribeLoadBalancers", operation: "DescribeLoadBalancers",
			xmlns: elbClassicNamespaceURI, params: elbClassicParams("DescribeLoadBalancers", nil)},
		{name: "classic DeleteLoadBalancer", operation: "DeleteLoadBalancer",
			xmlns: elbClassicNamespaceURI, params: elbClassicParams("DeleteLoadBalancer",
				map[string]string{"LoadBalancerName": "meta-doomed-classic"})},
	}
	require.Len(t, rows, elbRoutedOperationCount,
		"every routed ELB action gets a row, or the sweep is not the rule it claims to be")

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			params := map[string]string{"Action": row.operation}
			for k, v := range row.params {
				params[k] = v
			}
			env := elbDecodeEnvelope(t, ts.URL, params)

			assert.Equal(t, row.operation+"Response", env.XMLName.Local, "root element")
			assert.Equal(t, row.xmlns, env.XMLName.Space, "document namespace")
			assert.NotEmpty(t, env.RequestID, "ResponseMetadata carries a RequestId")
		})
	}
}

// TestELBResponseMetadata_TheIDIsTheOneTheEventRecorded ties the rendered value to the request.
//
// A present-but-constant element would satisfy the sweep above, and the value AWS publishes here is
// the id the service assigned to *this* call — the one a consumer quotes in a support case and the
// one [emulator.Event.RequestID] records. Both generations are asserted because they render it
// through the same builder but under different namespaces, and the assertion is on the recorded
// bytes as well as the live ones: an event whose response body carries a different id describes a
// response nobody received.
func TestELBResponseMetadata_TheIDIsTheOneTheEventRecorded(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	elbClassicCreate(t, ts.URL, "recorded-classic", nil)
	lb := elbCreateLB(t, ts.URL, "recorded-alb", nil)
	elbAddTags(t, ts, lb, map[string]string{"env": "prod"})

	events, err := ts.Store().GetStream(t.Context(), "default")
	require.NoError(t, err)
	require.Len(t, events, 3, "three calls record three events")

	for _, event := range events {
		require.NotEmpty(t, event.RequestID, "%s recorded no request id", event.Operation)
		require.NotNil(t, event.Response, "WithRecordedBodies records the response")

		want := "<RequestId>" + event.RequestID + "</RequestId>"
		assert.Contains(t, string(event.Response.Body), want,
			"%s rendered an id other than the one the event carries", event.Operation)
	}
}

// TestELBResponseMetadata_AReplayReproducesTheRecordedRequestID is the determinism half.
//
// `request_id_replay_test.go` pins the rule the builder follows — render `reqCtx.RequestID`, which a
// replay reproduces through `replayRequestID` — and this is that rule observed through ELB's own
// responses: the replay engine compares each replayed body against its recording
// (`replay_body_diff.go`), so a fresh id per call would be reported as a divergence at the
// `ResponseMetadata/RequestId` path of every event in the stream.
//
// The filter is on that path rather than on `Differences` being empty, and the difference matters:
// the recorded classic create mints a DNS-name suffix that a replay mints again, which is #856's
// open divergence and is honestly reported. Asserting zero differences overall would fail on that
// pre-existing debt and say nothing about this one.
func TestELBResponseMetadata_AReplayReproducesTheRecordedRequestID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	elbClassicCreate(t, ts.URL, "replay-classic", nil)
	for _, params := range []map[string]string{
		{"Action": "DescribeAccountLimits"},
		elbClassicParams("DescribeLoadBalancers", nil),
	} {
		require.NotEmpty(t, elbDecodeEnvelope(t, ts.URL, params).RequestID,
			"%s answers a request id live, which is what the replay has to reproduce", params["Action"])
	}

	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Equal(t, 3, results.SuccessEvents, "every recorded event must be re-executed")
	require.Zero(t, results.SkippedEvents)

	for _, diff := range results.Differences {
		assert.False(t, strings.HasSuffix(diff.Field, "/ResponseMetadata/RequestId"),
			"%s replayed a different request id: recorded %v, replayed %v",
			diff.Operation, diff.Expected, diff.Actual)
	}
}
