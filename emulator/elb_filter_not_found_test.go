package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Three more describe filters refuse a value naming a record the caller does not hold (#1375):
// DescribeTargetGroups' Names and LoadBalancerArn, DescribeListeners' LoadBalancerArn and
// DescribeRules' ListenerArn. Each page publishes the kind's not-found code at 400 with the sentence
// asserted here; until #1375 each value naming nothing matched nothing, so the call answered 200 with
// a shorter or empty list.
func TestELB_ADescribeFilterValueNamingNothingIsRefused(t *testing.T) {
	ts := newELBTestServer(t)
	ok := func(params map[string]string) []byte {
		t.Helper()
		status, body := elbWireCall(t, ts.URL, params)
		require.Equal(t, http.StatusOK, status, "%s: %s", params["Action"], body)
		return body
	}
	lb := elbWireARN(t, ok(map[string]string{"Action": "CreateLoadBalancer", "Name": "fv-alb", "Type": "application"}), "LoadBalancerArn")
	tg := elbWireARN(t, ok(map[string]string{
		"Action": "CreateTargetGroup", "Name": "fv-tg", "Protocol": "HTTP", "Port": "80", "VpcId": "vpc-12345678",
	}), "TargetGroupArn")
	listener := elbWireARN(t, ok(map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lb, "Protocol": "HTTP", "Port": "80",
		"DefaultActions.member.1.Type": "forward", "DefaultActions.member.1.TargetGroupArn": tg,
	}), "ListenerArn")
	rule := elbWireARN(t, ok(map[string]string{
		"Action": "CreateRule", "ListenerArn": listener, "Priority": "10",
		"Conditions.member.1.Field": "path-pattern", "Conditions.member.1.Values.member.1": "/fv/*",
		"Actions.member.1.Type": "forward", "Actions.member.1.TargetGroupArn": tg,
	}), "RuleArn")
	missing := func(arn string) string { return arn[:len(arn)-4] + "0000" }

	const (
		lbCode, lbMsg             = "LoadBalancerNotFound", "The specified load balancer does not exist."
		tgCode, tgMsg             = "TargetGroupNotFound", "The specified target group does not exist."
		listenerCode, listenerMsg = "ListenerNotFound", "The specified listener does not exist."
	)
	for _, tc := range []struct {
		name          string
		refused       map[string]string
		code, message string
		found         map[string]string
		foundContains string
	}{
		{
			"DescribeTargetGroups/Names alone",
			map[string]string{"Action": "DescribeTargetGroups", "Names.member.1": "fv-nothing"}, tgCode, tgMsg,
			map[string]string{"Action": "DescribeTargetGroups", "Names.member.1": "fv-tg"}, "<TargetGroupName>fv-tg</TargetGroupName>",
		},
		{
			"DescribeTargetGroups/Names beside a real one",
			map[string]string{"Action": "DescribeTargetGroups", "Names.member.1": "fv-tg", "Names.member.2": "fv-nothing"}, tgCode, tgMsg,
			nil, "",
		},
		// #1413: API_DescribeLoadBalancers publishes LoadBalancerNotFound for both filters.
		{
			"DescribeLoadBalancers/Names alone",
			map[string]string{"Action": "DescribeLoadBalancers", "Names.member.1": "fv-nothing"}, lbCode, lbMsg,
			map[string]string{"Action": "DescribeLoadBalancers", "Names.member.1": "fv-alb"}, "<LoadBalancerName>fv-alb</LoadBalancerName>",
		},
		{
			"DescribeLoadBalancers/Names beside a real one",
			map[string]string{"Action": "DescribeLoadBalancers", "Names.member.1": "fv-alb", "Names.member.2": "fv-nothing"}, lbCode, lbMsg,
			nil, "",
		},
		{
			"DescribeLoadBalancers/LoadBalancerArns",
			map[string]string{"Action": "DescribeLoadBalancers", "LoadBalancerArns.member.1": missing(lb)}, lbCode, lbMsg,
			map[string]string{"Action": "DescribeLoadBalancers", "LoadBalancerArns.member.1": lb}, "<LoadBalancerArn>" + lb + "</LoadBalancerArn>",
		},
		{
			"DescribeTargetGroups/LoadBalancerArn",
			map[string]string{"Action": "DescribeTargetGroups", "LoadBalancerArn": missing(lb)}, lbCode, lbMsg,
			map[string]string{"Action": "DescribeTargetGroups", "LoadBalancerArn": lb}, "<DescribeTargetGroupsResult>",
		},
		{
			"DescribeListeners/LoadBalancerArn",
			map[string]string{"Action": "DescribeListeners", "LoadBalancerArn": missing(lb)}, lbCode, lbMsg,
			map[string]string{"Action": "DescribeListeners", "LoadBalancerArn": lb}, "<ListenerArn>" + listener + "</ListenerArn>",
		},
		{
			"DescribeRules/ListenerArn",
			map[string]string{"Action": "DescribeRules", "ListenerArn": missing(listener)}, listenerCode, listenerMsg,
			map[string]string{"Action": "DescribeRules", "ListenerArn": listener}, "<RuleArn>" + rule + "</RuleArn>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := elbWireCall(t, ts.URL, tc.refused)
			require.Equalf(t, http.StatusBadRequest, status, "%s naming nothing: %s", tc.name, body)
			code, message := elbWireError(t, body)
			require.Equal(t, tc.code, code, "%s: %s", tc.name, body)
			require.Equal(t, tc.message, message, "%s: %s", tc.name, body)
			if tc.found != nil {
				require.Contains(t, string(ok(tc.found)), tc.foundContains, "%s on a real value must answer as before", tc.name)
			}
		})
	}
}

// The new lookups behind #1375's refusals return a store read error rather than reading it as "not
// held", which would refuse a filter naming a record that may well exist.
func TestELB_ADescribeFilterLookupFaultIsAnError(t *testing.T) {
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	ts := newELBTestServerWithState(t, fault)
	ok := func(params map[string]string) []byte {
		t.Helper()
		status, body := elbWireCall(t, ts.URL, params)
		require.Equal(t, http.StatusOK, status, "%s: %s", params["Action"], body)
		return body
	}
	lb := elbWireARN(t, ok(map[string]string{"Action": "CreateLoadBalancer", "Name": "fl-alb", "Type": "application"}), "LoadBalancerArn")
	tg := elbWireARN(t, ok(map[string]string{
		"Action": "CreateTargetGroup", "Name": "fl-tg", "Protocol": "HTTP", "Port": "80", "VpcId": "vpc-12345678",
	}), "TargetGroupArn")
	listener := elbWireARN(t, ok(map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lb, "Protocol": "HTTP", "Port": "80",
		"DefaultActions.member.1.Type": "forward", "DefaultActions.member.1.TargetGroupArn": tg,
	}), "ListenerArn")

	for _, tc := range []struct {
		prefix string
		params map[string]string
	}{
		{"lb:", map[string]string{"Action": "DescribeLoadBalancers", "Names.member.1": "fl-alb"}},
		{"lb:", map[string]string{"Action": "DescribeTargetGroups", "LoadBalancerArn": lb}},
		{"lb:", map[string]string{"Action": "DescribeListeners", "LoadBalancerArn": lb}},
		{"listener:", map[string]string{"Action": "DescribeRules", "ListenerArn": listener}},
	} {
		t.Run(tc.params["Action"]+"/"+tc.prefix, func(t *testing.T) {
			fault.failGet = tc.prefix
			defer func() { fault.failGet = "" }()
			status, body := elbWireCall(t, ts.URL, tc.params)
			require.GreaterOrEqualf(t, status, http.StatusInternalServerError,
				"%s must fail on a store read fault, not answer %d: %s", tc.params["Action"], status, body)
		})
	}
}
