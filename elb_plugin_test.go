package substrate_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newELBTestServer builds a minimal Server with the ELB plugin registered.
func newELBTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.ELBPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize elb plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// elbRequest sends an ELBv2 query-protocol request and returns the response.
func elbRequest(t *testing.T, ts *httptest.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build elb request: %v", err)
	}
	req.Host = "elasticloadbalancing.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do elb request: %v", err)
	}
	return resp
}

func TestELB_CreateLoadBalancer(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{
		"Action":                  "CreateLoadBalancer",
		"Name":                    "my-alb",
		"Type":                    "application",
		"Scheme":                  "internet-facing",
		"Subnets.member.1":        "subnet-abc123",
		"SecurityGroups.member.1": "sg-def456",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateLoadBalancer: expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			LoadBalancers []struct {
				LoadBalancerArn  string `xml:"LoadBalancerArn"`
				LoadBalancerName string `xml:"LoadBalancerName"`
				DNSName          string `xml:"DNSName"`
				Type             string `xml:"Type"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode CreateLoadBalancer response: %v", err)
	}
	if len(result.Result.LoadBalancers) != 1 {
		t.Fatalf("expected 1 LB, got %d", len(result.Result.LoadBalancers))
	}
	lb := result.Result.LoadBalancers[0]
	if lb.LoadBalancerName != "my-alb" {
		t.Errorf("expected name my-alb, got %q", lb.LoadBalancerName)
	}
	if !strings.HasPrefix(lb.LoadBalancerArn, "arn:aws:elasticloadbalancing:") {
		t.Errorf("unexpected ARN format: %s", lb.LoadBalancerArn)
	}
	if !strings.Contains(lb.DNSName, "us-east-1.elb.amazonaws.com") {
		t.Errorf("unexpected DNS name: %s", lb.DNSName)
	}
	if lb.Type != "application" {
		t.Errorf("expected type application, got %q", lb.Type)
	}
}

func TestELB_DescribeLoadBalancers_Empty(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{"Action": "DescribeLoadBalancers"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeLoadBalancers empty: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.LoadBalancers) != 0 {
		t.Errorf("expected 0 LBs, got %d", len(result.Result.LoadBalancers))
	}
}

func TestELB_DescribeLoadBalancers_ByName(t *testing.T) {
	ts := newELBTestServer(t)
	elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-a", "Type": "application"})
	elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-b", "Type": "network"})

	resp := elbRequest(t, ts, map[string]string{
		"Action":         "DescribeLoadBalancers",
		"Names.member.1": "alb-a",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerName string `xml:"LoadBalancerName"`
			} `xml:"LoadBalancers>member"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.LoadBalancers) != 1 {
		t.Fatalf("expected 1 LB, got %d", len(result.Result.LoadBalancers))
	}
	if result.Result.LoadBalancers[0].LoadBalancerName != "alb-a" {
		t.Errorf("expected alb-a, got %q", result.Result.LoadBalancers[0].LoadBalancerName)
	}
}

func TestELB_DeleteLoadBalancer(t *testing.T) {
	ts := newELBTestServer(t)
	createResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "to-delete", "Type": "application"})
	defer createResp.Body.Close() //nolint:errcheck

	var createResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	arn := createResult.Result.LoadBalancers[0].LoadBalancerArn

	delResp := elbRequest(t, ts, map[string]string{"Action": "DeleteLoadBalancer", "LoadBalancerArn": arn})
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteLoadBalancer: expected 200, got %d", delResp.StatusCode)
	}

	// Verify gone.
	descResp := elbRequest(t, ts, map[string]string{"Action": "DescribeLoadBalancers"})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		Result struct {
			LoadBalancers []struct{} `xml:"LoadBalancers>member"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	if len(descResult.Result.LoadBalancers) != 0 {
		t.Errorf("expected 0 LBs after delete, got %d", len(descResult.Result.LoadBalancers))
	}
}

func TestELB_CreateTargetGroup(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{
		"Action":     "CreateTargetGroup",
		"Name":       "my-tg",
		"Protocol":   "HTTP",
		"Port":       "80",
		"VpcId":      "vpc-abc123",
		"TargetType": "instance",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateTargetGroup: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn  string `xml:"TargetGroupArn"`
				TargetGroupName string `xml:"TargetGroupName"`
				Port            int    `xml:"Port"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.TargetGroups) != 1 {
		t.Fatalf("expected 1 TG, got %d", len(result.Result.TargetGroups))
	}
	tg := result.Result.TargetGroups[0]
	if tg.TargetGroupName != "my-tg" {
		t.Errorf("expected my-tg, got %q", tg.TargetGroupName)
	}
	if !strings.HasPrefix(tg.TargetGroupArn, "arn:aws:elasticloadbalancing:") {
		t.Errorf("unexpected TG ARN: %s", tg.TargetGroupArn)
	}
	if tg.Port != 80 {
		t.Errorf("expected port 80, got %d", tg.Port)
	}
}

func TestELB_DescribeTargetGroups(t *testing.T) {
	ts := newELBTestServer(t)
	elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-1", "Protocol": "HTTP", "Port": "80"})
	elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-2", "Protocol": "HTTPS", "Port": "443"})

	resp := elbRequest(t, ts, map[string]string{"Action": "DescribeTargetGroups"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupName string `xml:"TargetGroupName"`
			} `xml:"TargetGroups>member"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.TargetGroups) != 2 {
		t.Fatalf("expected 2 TGs, got %d", len(result.Result.TargetGroups))
	}
}

func TestELB_DeleteTargetGroup(t *testing.T) {
	ts := newELBTestServer(t)
	cr := elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-del", "Protocol": "HTTP", "Port": "8080"})
	defer cr.Body.Close() //nolint:errcheck
	var crResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&crResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	tgARN := crResult.Result.TargetGroups[0].TargetGroupArn

	delResp := elbRequest(t, ts, map[string]string{"Action": "DeleteTargetGroup", "TargetGroupArn": tgARN})
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteTargetGroup: expected 200, got %d", delResp.StatusCode)
	}
}

func TestELB_RegisterTargets_DescribeTargetHealth(t *testing.T) {
	ts := newELBTestServer(t)
	cr := elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-health", "Protocol": "HTTP", "Port": "80"})
	defer cr.Body.Close() //nolint:errcheck
	var crResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&crResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	tgARN := crResult.Result.TargetGroups[0].TargetGroupArn

	regResp := elbRequest(t, ts, map[string]string{
		"Action":                "RegisterTargets",
		"TargetGroupArn":        tgARN,
		"Targets.member.1.Id":   "i-0123456789abcdef0",
		"Targets.member.1.Port": "80",
		"Targets.member.2.Id":   "i-abcdef0123456789",
	})
	defer regResp.Body.Close() //nolint:errcheck
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("RegisterTargets: expected 200, got %d", regResp.StatusCode)
	}

	healthResp := elbRequest(t, ts, map[string]string{
		"Action":         "DescribeTargetHealth",
		"TargetGroupArn": tgARN,
	})
	defer healthResp.Body.Close() //nolint:errcheck
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeTargetHealth: expected 200, got %d", healthResp.StatusCode)
	}
	var healthResult struct {
		Result struct {
			Descriptions []struct {
				Target struct {
					ID string `xml:"Id"`
				} `xml:"Target"`
				TargetHealth struct {
					State string `xml:"State"`
				} `xml:"TargetHealth"`
			} `xml:"TargetHealthDescriptions>member"`
		} `xml:"DescribeTargetHealthResult"`
	}
	if err := xml.NewDecoder(healthResp.Body).Decode(&healthResult); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if len(healthResult.Result.Descriptions) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(healthResult.Result.Descriptions))
	}
	for _, d := range healthResult.Result.Descriptions {
		if d.TargetHealth.State != "healthy" {
			t.Errorf("expected healthy, got %q", d.TargetHealth.State)
		}
	}
}

func TestELB_DeregisterTargets(t *testing.T) {
	ts := newELBTestServer(t)
	cr := elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-dereg", "Protocol": "HTTP", "Port": "80"})
	defer cr.Body.Close() //nolint:errcheck
	var crResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&crResult); err != nil {
		t.Fatalf("decode: %v", err)
	}
	tgARN := crResult.Result.TargetGroups[0].TargetGroupArn

	regResp := elbRequest(t, ts, map[string]string{
		"Action": "RegisterTargets", "TargetGroupArn": tgARN,
		"Targets.member.1.Id": "i-aaa",
	})
	regResp.Body.Close() //nolint:errcheck

	deregResp := elbRequest(t, ts, map[string]string{
		"Action": "DeregisterTargets", "TargetGroupArn": tgARN,
		"Targets.member.1.Id": "i-aaa",
	})
	defer deregResp.Body.Close() //nolint:errcheck
	if deregResp.StatusCode != http.StatusOK {
		t.Fatalf("DeregisterTargets: expected 200, got %d", deregResp.StatusCode)
	}

	healthResp := elbRequest(t, ts, map[string]string{"Action": "DescribeTargetHealth", "TargetGroupArn": tgARN})
	defer healthResp.Body.Close() //nolint:errcheck
	var healthResult struct {
		Result struct {
			Descriptions []struct{} `xml:"TargetHealthDescriptions>member"`
		} `xml:"DescribeTargetHealthResult"`
	}
	if err := xml.NewDecoder(healthResp.Body).Decode(&healthResult); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if len(healthResult.Result.Descriptions) != 0 {
		t.Errorf("expected 0 targets after deregister, got %d", len(healthResult.Result.Descriptions))
	}
}

func TestELB_CreateListener(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-listener", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	tgResp := elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-listener", "Protocol": "HTTP", "Port": "80"})
	defer tgResp.Body.Close() //nolint:errcheck
	var tgResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(tgResp.Body).Decode(&tgResult); err != nil {
		t.Fatalf("decode tg: %v", err)
	}
	tgARN := tgResult.Result.TargetGroups[0].TargetGroupArn

	lResp := elbRequest(t, ts, map[string]string{
		"Action":                                 "CreateListener",
		"LoadBalancerArn":                        lbARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	})
	defer lResp.Body.Close() //nolint:errcheck
	if lResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateListener: expected 200, got %d", lResp.StatusCode)
	}
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn     string `xml:"ListenerArn"`
				LoadBalancerArn string `xml:"LoadBalancerArn"`
				Protocol        string `xml:"Protocol"`
				Port            int    `xml:"Port"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	if len(lResult.Result.Listeners) != 1 {
		t.Fatalf("expected 1 listener, got %d", len(lResult.Result.Listeners))
	}
	l := lResult.Result.Listeners[0]
	if l.LoadBalancerArn != lbARN {
		t.Errorf("expected lb ARN %s, got %s", lbARN, l.LoadBalancerArn)
	}
	if l.Protocol != "HTTP" {
		t.Errorf("expected HTTP, got %s", l.Protocol)
	}
	if l.Port != 80 {
		t.Errorf("expected port 80, got %d", l.Port)
	}
}

func TestELB_DescribeListeners(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-desc-l", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	for _, port := range []string{"80", "443"} {
		r := elbRequest(t, ts, map[string]string{
			"Action": "CreateListener", "LoadBalancerArn": lbARN,
			"Protocol": "HTTP", "Port": port,
		})
		r.Body.Close() //nolint:errcheck
	}

	resp := elbRequest(t, ts, map[string]string{
		"Action":          "DescribeListeners",
		"LoadBalancerArn": lbARN,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			Listeners []struct{} `xml:"Listeners>member"`
		} `xml:"DescribeListenersResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.Listeners) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(result.Result.Listeners))
	}
}

func TestELB_DeleteListener(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-del-l", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lbARN,
		"Protocol": "HTTP", "Port": "80",
	})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	delResp := elbRequest(t, ts, map[string]string{"Action": "DeleteListener", "ListenerArn": listenerARN})
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteListener: expected 200, got %d", delResp.StatusCode)
	}
}

func TestELB_ModifyListener(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-mod-l", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{
		"Action": "CreateListener", "LoadBalancerArn": lbARN,
		"Protocol": "HTTP", "Port": "80",
	})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	modResp := elbRequest(t, ts, map[string]string{
		"Action":      "ModifyListener",
		"ListenerArn": listenerARN,
		"Port":        "8080",
	})
	defer modResp.Body.Close() //nolint:errcheck
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyListener: expected 200, got %d", modResp.StatusCode)
	}
	var modResult struct {
		Result struct {
			Listeners []struct {
				Port int `xml:"Port"`
			} `xml:"Listeners>member"`
		} `xml:"ModifyListenerResult"`
	}
	if err := xml.NewDecoder(modResp.Body).Decode(&modResult); err != nil {
		t.Fatalf("decode modify: %v", err)
	}
	if len(modResult.Result.Listeners) != 1 || modResult.Result.Listeners[0].Port != 8080 {
		t.Errorf("expected port 8080, got %d", modResult.Result.Listeners[0].Port)
	}
}

func TestELB_CreateRule(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-rule", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{"Action": "CreateListener", "LoadBalancerArn": lbARN, "Protocol": "HTTP", "Port": "80"})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	tgResp := elbRequest(t, ts, map[string]string{"Action": "CreateTargetGroup", "Name": "tg-rule", "Protocol": "HTTP", "Port": "80"})
	defer tgResp.Body.Close() //nolint:errcheck
	var tgResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(tgResp.Body).Decode(&tgResult); err != nil {
		t.Fatalf("decode tg: %v", err)
	}
	tgARN := tgResult.Result.TargetGroups[0].TargetGroupArn

	rResp := elbRequest(t, ts, map[string]string{
		"Action":                              "CreateRule",
		"ListenerArn":                         listenerARN,
		"Priority":                            "10",
		"Conditions.member.1.Field":           "path-pattern",
		"Conditions.member.1.Values.member.1": "/api/*",
		"Actions.member.1.Type":               "forward",
		"Actions.member.1.TargetGroupArn":     tgARN,
	})
	defer rResp.Body.Close() //nolint:errcheck
	if rResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateRule: expected 200, got %d", rResp.StatusCode)
	}
	var rResult struct {
		Result struct {
			Rules []struct {
				RuleArn  string `xml:"RuleArn"`
				Priority string `xml:"Priority"`
			} `xml:"Rules>member"`
		} `xml:"CreateRuleResult"`
	}
	if err := xml.NewDecoder(rResp.Body).Decode(&rResult); err != nil {
		t.Fatalf("decode rule: %v", err)
	}
	if len(rResult.Result.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rResult.Result.Rules))
	}
	if rResult.Result.Rules[0].Priority != "10" {
		t.Errorf("expected priority 10, got %s", rResult.Result.Rules[0].Priority)
	}
}

func TestELB_DescribeRules(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-desc-rules", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{"Action": "CreateListener", "LoadBalancerArn": lbARN, "Protocol": "HTTP", "Port": "80"})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	for _, p := range []string{"5", "10"} {
		r := elbRequest(t, ts, map[string]string{
			"Action": "CreateRule", "ListenerArn": listenerARN, "Priority": p,
			"Conditions.member.1.Field": "path-pattern",
			"Actions.member.1.Type":     "forward",
		})
		r.Body.Close() //nolint:errcheck
	}

	resp := elbRequest(t, ts, map[string]string{"Action": "DescribeRules", "ListenerArn": listenerARN})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			Rules []struct{} `xml:"Rules>member"`
		} `xml:"DescribeRulesResult"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Result.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(result.Result.Rules))
	}
}

func TestELB_DeleteRule(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-del-rule", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{"Action": "CreateListener", "LoadBalancerArn": lbARN, "Protocol": "HTTP", "Port": "80"})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	rResp := elbRequest(t, ts, map[string]string{
		"Action": "CreateRule", "ListenerArn": listenerARN, "Priority": "1",
		"Actions.member.1.Type": "forward",
	})
	defer rResp.Body.Close() //nolint:errcheck
	var rResult struct {
		Result struct {
			Rules []struct {
				RuleArn string `xml:"RuleArn"`
			} `xml:"Rules>member"`
		} `xml:"CreateRuleResult"`
	}
	if err := xml.NewDecoder(rResp.Body).Decode(&rResult); err != nil {
		t.Fatalf("decode rule: %v", err)
	}
	ruleARN := rResult.Result.Rules[0].RuleArn

	delResp := elbRequest(t, ts, map[string]string{"Action": "DeleteRule", "RuleArn": ruleARN})
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteRule: expected 200, got %d", delResp.StatusCode)
	}
}

func TestELB_SetRulePriorities(t *testing.T) {
	ts := newELBTestServer(t)
	lbResp := elbRequest(t, ts, map[string]string{"Action": "CreateLoadBalancer", "Name": "alb-prio", "Type": "application"})
	defer lbResp.Body.Close() //nolint:errcheck
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode lb: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	lResp := elbRequest(t, ts, map[string]string{"Action": "CreateListener", "LoadBalancerArn": lbARN, "Protocol": "HTTP", "Port": "80"})
	defer lResp.Body.Close() //nolint:errcheck
	var lResult struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	if err := xml.NewDecoder(lResp.Body).Decode(&lResult); err != nil {
		t.Fatalf("decode listener: %v", err)
	}
	listenerARN := lResult.Result.Listeners[0].ListenerArn

	rResp := elbRequest(t, ts, map[string]string{
		"Action": "CreateRule", "ListenerArn": listenerARN, "Priority": "5",
		"Actions.member.1.Type": "forward",
	})
	defer rResp.Body.Close() //nolint:errcheck
	var rResult struct {
		Result struct {
			Rules []struct {
				RuleArn string `xml:"RuleArn"`
			} `xml:"Rules>member"`
		} `xml:"CreateRuleResult"`
	}
	if err := xml.NewDecoder(rResp.Body).Decode(&rResult); err != nil {
		t.Fatalf("decode rule: %v", err)
	}
	ruleARN := rResult.Result.Rules[0].RuleArn

	prioResp := elbRequest(t, ts, map[string]string{
		"Action":                           "SetRulePriorities",
		"RulePriorities.member.1.RuleArn":  ruleARN,
		"RulePriorities.member.1.Priority": "100",
	})
	defer prioResp.Body.Close() //nolint:errcheck
	if prioResp.StatusCode != http.StatusOK {
		t.Fatalf("SetRulePriorities: expected 200, got %d", prioResp.StatusCode)
	}
}

func TestELB_UnknownAction(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{"Action": "SomethingUnknown"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown action: expected 400, got %d", resp.StatusCode)
	}
}

func TestELB_DescribeLoadBalancerAttributes(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{
		"Action":          "DescribeLoadBalancerAttributes",
		"LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/test/abc",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeLoadBalancerAttributes: expected 200, got %d", resp.StatusCode)
	}
}

func TestELB_ModifyLoadBalancerAttributes(t *testing.T) {
	ts := newELBTestServer(t)
	// Create a load balancer first.
	lbResp := elbRequest(t, ts, map[string]string{
		"Action": "CreateLoadBalancer",
		"Name":   "test-lb-modify",
		"Type":   "application",
		"Scheme": "internet-facing",
	})
	defer lbResp.Body.Close() //nolint:errcheck
	if lbResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateLoadBalancer: expected 200, got %d", lbResp.StatusCode)
	}
	var lbResult struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	if err := xml.NewDecoder(lbResp.Body).Decode(&lbResult); err != nil {
		t.Fatalf("decode load balancer: %v", err)
	}
	lbARN := lbResult.Result.LoadBalancers[0].LoadBalancerArn

	// Modify load balancer attributes.
	modResp := elbRequest(t, ts, map[string]string{
		"Action":                    "ModifyLoadBalancerAttributes",
		"LoadBalancerArn":           lbARN,
		"Attributes.member.1.Key":   "idle_timeout.timeout_seconds",
		"Attributes.member.1.Value": "120",
	})
	defer modResp.Body.Close() //nolint:errcheck
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyLoadBalancerAttributes: expected 200, got %d", modResp.StatusCode)
	}
}

func TestELB_ModifyTargetGroup(t *testing.T) {
	ts := newELBTestServer(t)
	// Create a target group first.
	tgResp := elbRequest(t, ts, map[string]string{
		"Action":   "CreateTargetGroup",
		"Name":     "test-tg-modify",
		"Protocol": "HTTP",
		"Port":     "80",
		"VpcId":    "vpc-12345678",
	})
	defer tgResp.Body.Close() //nolint:errcheck
	var tgResult struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	if err := xml.NewDecoder(tgResp.Body).Decode(&tgResult); err != nil {
		t.Fatalf("decode target group: %v", err)
	}
	tgARN := tgResult.Result.TargetGroups[0].TargetGroupArn

	// Modify the target group.
	modResp := elbRequest(t, ts, map[string]string{
		"Action":              "ModifyTargetGroup",
		"TargetGroupArn":      tgARN,
		"HealthCheckPath":     "/health",
		"HealthCheckProtocol": "HTTP",
	})
	defer modResp.Body.Close() //nolint:errcheck
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyTargetGroup: expected 200, got %d", modResp.StatusCode)
	}
}
