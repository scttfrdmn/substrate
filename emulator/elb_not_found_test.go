package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// ELBv2 operations that address one resource by ARN refuse an ARN naming nothing (#1313).
//
// Each scanned the caller's records for the ARN and, finding none, fell through to a 200: an empty
// result list for ModifyTargetGroup, ModifyListener, DescribeTargetHealth and SetRulePriorities, and
// an empty success for RegisterTargets, DeregisterTargets, DeleteListener and DeleteRule. Every one of
// those pages publishes the not-found code at HTTP 400 instead, so a consumer acting on a deleted or
// mistyped ARN saw success, and a retry loop keyed on the code never fired.
//
// Each case is driven over the wire twice: once with a well-formed ARN of the right kind that names
// nothing, which must be refused with the published code and sentence, and once with the real ARN,
// which must still succeed with the result it answered before.

// elbWireError decodes an ELB error response's code and message.
func elbWireError(t *testing.T, body []byte) (code, message string) {
	t.Helper()
	var doc struct {
		Error struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "decode ELB error: %s", body)
	return doc.Error.Code, doc.Error.Message
}

// elbWireCall issues one ELBv2 action and returns its status and body.
func elbWireCall(t *testing.T, baseURL string, params map[string]string) (int, []byte) {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s", params["Action"])
	return resp.StatusCode, body
}

// elbWireARN returns the first element named name in body.
func elbWireARN(t *testing.T, body []byte, name string) string {
	t.Helper()
	m := regexp.MustCompile(`<` + name + `>([^<]+)</` + name + `>`).FindSubmatch(body)
	require.NotNilf(t, m, "no <%s> in %s", name, body)
	return string(m[1])
}

func TestELB_AnARNNamingNothingIsRefusedWithThePublishedCode(t *testing.T) {
	ts := newELBTestServer(t)
	ok := func(params map[string]string) []byte {
		t.Helper()
		status, body := elbWireCall(t, ts.URL, params)
		require.Equal(t, http.StatusOK, status, "%s: %s", params["Action"], body)
		return body
	}

	lb := elbWireARN(t, ok(map[string]string{"Action": "CreateLoadBalancer", "Name": "nf-alb", "Type": "application"}), "LoadBalancerArn")
	tg := elbWireARN(t, ok(map[string]string{
		"Action": "CreateTargetGroup", "Name": "nf-tg", "Protocol": "HTTP", "Port": "80", "VpcId": "vpc-12345678",
	}), "TargetGroupArn")
	listener := elbWireARN(t, ok(map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lb, "Protocol": "HTTP", "Port": "80",
		"DefaultActions.member.1.Type": "forward", "DefaultActions.member.1.TargetGroupArn": tg,
	}), "ListenerArn")
	rule := elbWireARN(t, ok(map[string]string{
		"Action": "CreateRule", "ListenerArn": listener, "Priority": "10",
		"Conditions.member.1.Field": "path-pattern", "Conditions.member.1.Values.member.1": "/nf/*",
		"Actions.member.1.Type": "forward", "Actions.member.1.TargetGroupArn": tg,
	}), "RuleArn")

	// A well-formed ARN of each kind that names nothing: the real one with its id changed.
	missing := func(arn string) string { return arn[:len(arn)-4] + "0000" }

	const (
		tgCode, tgMsg             = "TargetGroupNotFound", "The specified target group does not exist."
		listenerCode, listenerMsg = "ListenerNotFound", "The specified listener does not exist."
		ruleCode, ruleMsg         = "RuleNotFound", "The specified rule does not exist."
	)
	for _, tc := range []struct {
		action  string
		params  func(arn string) map[string]string
		arn     string
		code    string
		message string
		// found is asserted on the body the real ARN answers.
		found string
	}{
		{"ModifyTargetGroup", func(a string) map[string]string {
			return map[string]string{"TargetGroupArn": a, "HealthCheckPath": "/nf"}
		}, tg, tgCode, tgMsg, "<HealthCheckPath>/nf</HealthCheckPath>"},
		{"RegisterTargets", func(a string) map[string]string {
			return map[string]string{"TargetGroupArn": a, "Targets.member.1.Id": "i-0123456789abcdef0"}
		}, tg, tgCode, tgMsg, "<RegisterTargetsResult"},
		{"DescribeTargetHealth", func(a string) map[string]string {
			return map[string]string{"TargetGroupArn": a}
		}, tg, tgCode, tgMsg, "<Id>i-0123456789abcdef0</Id>"},
		{"DeregisterTargets", func(a string) map[string]string {
			return map[string]string{"TargetGroupArn": a, "Targets.member.1.Id": "i-0123456789abcdef0"}
		}, tg, tgCode, tgMsg, "<DeregisterTargetsResult"},
		{"ModifyListener", func(a string) map[string]string {
			return map[string]string{"ListenerArn": a, "Port": "8080"}
		}, listener, listenerCode, listenerMsg, "<Port>8080</Port>"},
		{"SetRulePriorities", func(a string) map[string]string {
			return map[string]string{"RulePriorities.member.1.RuleArn": a, "RulePriorities.member.1.Priority": "20"}
		}, rule, ruleCode, ruleMsg, "<Priority>20</Priority>"},
		// The deletes last, rule before listener, since each removes what the cases above address.
		{"DeleteRule", func(a string) map[string]string {
			return map[string]string{"RuleArn": a}
		}, rule, ruleCode, ruleMsg, "<DeleteRuleResult"},
		{"DeleteListener", func(a string) map[string]string {
			return map[string]string{"ListenerArn": a}
		}, listener, listenerCode, listenerMsg, "<DeleteListenerResult"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			params := tc.params(missing(tc.arn))
			params["Action"] = tc.action
			status, body := elbWireCall(t, ts.URL, params)
			require.Equalf(t, http.StatusBadRequest, status, "%s with an ARN naming nothing: %s", tc.action, body)
			code, message := elbWireError(t, body)
			require.Equal(t, tc.code, code, "%s: %s", tc.action, body)
			require.Equal(t, tc.message, message, "%s: %s", tc.action, body)

			params = tc.params(tc.arn)
			params["Action"] = tc.action
			require.Contains(t, string(ok(params)), tc.found, "%s on the real ARN must answer as before", tc.action)
		})
	}

	// SetRulePriorities resolves every pair before writing any: a call naming one real rule and one
	// missing one is refused and leaves the real rule's priority where it was.
	t.Run("SetRulePriorities reprices nothing when any rule is missing", func(t *testing.T) {
		lb2 := elbWireARN(t, ok(map[string]string{"Action": "CreateLoadBalancer", "Name": "nf-alb-2", "Type": "application"}), "LoadBalancerArn")
		l2 := elbWireARN(t, ok(map[string]string{"Action": "CreateListener", "LoadBalancerArn": lb2, "Protocol": "HTTP", "Port": "81"}), "ListenerArn")
		r2 := elbWireARN(t, ok(map[string]string{
			"Action": "CreateRule", "ListenerArn": l2, "Priority": "5",
			"Conditions.member.1.Field": "path-pattern", "Conditions.member.1.Values.member.1": "/a/*",
			"Actions.member.1.Type": "forward", "Actions.member.1.TargetGroupArn": tg,
		}), "RuleArn")
		status, body := elbWireCall(t, ts.URL, map[string]string{
			"Action":                          "SetRulePriorities",
			"RulePriorities.member.1.RuleArn": r2, "RulePriorities.member.1.Priority": "50",
			"RulePriorities.member.2.RuleArn": missing(r2), "RulePriorities.member.2.Priority": "51",
		})
		require.Equal(t, http.StatusBadRequest, status, "%s", body)
		described := ok(map[string]string{"Action": "DescribeRules", "RuleArns.member.1": r2})
		require.Contains(t, string(described), "<Priority>5</Priority>", "a refused call must reprice nothing: %s", described)
	})
}
