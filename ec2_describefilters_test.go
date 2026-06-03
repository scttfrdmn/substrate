package substrate_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

// describeQuery issues DescribeInstances with the given params and returns the
// set of instance IDs in the response.
func describeQuery(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances: expected 200, got %d", resp.StatusCode)
	}
	var desc struct {
		Reservations []struct {
			Instances []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&desc); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	var ids []string
	for _, r := range desc.Reservations {
		for _, i := range r.Instances {
			ids = append(ids, i.InstanceID)
		}
	}
	return ids
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// TestEC2_DescribeInstances_MultiFilter is the regression for substrate #305:
// DescribeInstances must apply EVERY Filter.N (AND-combined), not just
// Filter.1, and must honor tag: filters. Real callers (e.g. spawn list
// --state running) lead with a tag filter and put instance-state-name second,
// so reading only Filter.1 silently dropped the state filter and let
// terminated instances leak into a running-only query.
func TestEC2_DescribeInstances_MultiFilter(t *testing.T) {
	ts := newEC2TestServer(t)

	// Launch one instance tagged spawn:managed=true.
	resp := ec2Request(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-12345678",
		"InstanceType":                    "t3.small",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "spawn:managed",
		"TagSpecification.1.Tag.1.Value":  "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", resp.StatusCode)
	}
	var run struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&run); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	if len(run.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(run.Instances))
	}
	id := run.Instances[0].InstanceID

	// Filter as spawn does: tag filter FIRST, state filter SECOND.
	runningFilter := map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:spawn:managed",
		"Filter.1.Value.1": "true",
		"Filter.2.Name":    "instance-state-name",
		"Filter.2.Value.1": "running",
	}

	// Before terminate: the running instance must be returned.
	if ids := describeQuery(t, ts, runningFilter); !contains(ids, id) {
		t.Fatalf("running instance %s not returned by [tag, state=running] filter; got %v", id, ids)
	}

	// Terminate it.
	term := ec2Request(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": id})
	if term.StatusCode != http.StatusOK {
		t.Fatalf("TerminateInstances: expected 200, got %d", term.StatusCode)
	}
	term.Body.Close() //nolint:errcheck

	// After terminate: the SAME [tag, state=running] filter must NOT return it.
	if ids := describeQuery(t, ts, runningFilter); contains(ids, id) {
		t.Errorf("#305: terminated instance %s still returned by state=running filter; got %v", id, ids)
	}

	// A state=terminated filter (state first this time) must return it,
	// proving filter order is irrelevant.
	terminatedFilter := map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "instance-state-name",
		"Filter.1.Value.1": "terminated",
	}
	if ids := describeQuery(t, ts, terminatedFilter); !contains(ids, id) {
		t.Errorf("terminated instance %s not returned by state=terminated filter; got %v", id, ids)
	}

	// A tag filter that doesn't match must exclude it.
	noMatch := map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:spawn:managed",
		"Filter.1.Value.1": "false",
	}
	if ids := describeQuery(t, ts, noMatch); contains(ids, id) {
		t.Errorf("instance %s returned by non-matching tag filter; got %v", id, ids)
	}
}
