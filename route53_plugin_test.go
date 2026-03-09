package substrate_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newR53TestServer builds a minimal Server with the Route53 plugin registered.
func newR53TestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.Route53Plugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
	}); err != nil {
		t.Fatalf("initialize route53 plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// r53Request sends a Route 53 REST request and returns the response.
func r53Request(t *testing.T, ts *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build r53 request: %v", err)
	}
	req.Host = "route53.amazonaws.com"
	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do r53 request: %v", err)
	}
	return resp
}

const r53CreateZoneBody = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference>ref-001</CallerReference>
  <HostedZoneConfig>
    <Comment>test zone</Comment>
    <PrivateZone>false</PrivateZone>
  </HostedZoneConfig>
</CreateHostedZoneRequest>`

func TestRoute53_CreateHostedZone(t *testing.T) {
	ts := newR53TestServer(t)
	resp := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateHostedZone: expected 201, got %d", resp.StatusCode)
	}

	var result struct {
		XMLName    xml.Name `xml:"CreateHostedZoneResponse"`
		HostedZone struct {
			ID   string `xml:"Id"`
			Name string `xml:"Name"`
		} `xml:"HostedZone"`
		ChangeInfo struct {
			Status string `xml:"Status"`
		} `xml:"ChangeInfo"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.HasPrefix(result.HostedZone.ID, "/hostedzone/Z") {
		t.Errorf("unexpected zone ID format: %s", result.HostedZone.ID)
	}
	if result.HostedZone.Name != "example.com." {
		t.Errorf("expected example.com., got %q", result.HostedZone.Name)
	}
	if result.ChangeInfo.Status != "INSYNC" {
		t.Errorf("expected INSYNC, got %q", result.ChangeInfo.Status)
	}
}

func TestRoute53_ListHostedZones_Empty(t *testing.T) {
	ts := newR53TestServer(t)
	resp := r53Request(t, ts, http.MethodGet, "/2013-04-01/hostedzone", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListHostedZones empty: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		XMLName     xml.Name `xml:"ListHostedZonesResponse"`
		HostedZones []struct {
			ID string `xml:"Id"`
		} `xml:"HostedZones>HostedZone"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.HostedZones) != 0 {
		t.Errorf("expected 0 zones, got %d", len(result.HostedZones))
	}
}

func TestRoute53_ListHostedZones_Found(t *testing.T) {
	ts := newR53TestServer(t)
	for _, name := range []string{"alpha.com", "beta.org"} {
		body := `<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>` + name + `</Name><CallerReference>ref-` + name + `</CallerReference></CreateHostedZoneRequest>`
		cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", body)
		cr.Body.Close() //nolint:errcheck
	}

	resp := r53Request(t, ts, http.MethodGet, "/2013-04-01/hostedzone", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		HostedZones []struct {
			ID string `xml:"Id"`
		} `xml:"HostedZones>HostedZone"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.HostedZones) != 2 {
		t.Fatalf("expected 2 zones, got %d", len(result.HostedZones))
	}
}

func TestRoute53_GetHostedZone(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID // e.g., /hostedzone/ZABC123

	resp := r53Request(t, ts, http.MethodGet, "/2013-04-01"+zoneID, "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetHostedZone: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		HostedZone struct {
			ID   string `xml:"Id"`
			Name string `xml:"Name"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.HostedZone.ID != zoneID {
		t.Errorf("expected ID %s, got %s", zoneID, result.HostedZone.ID)
	}
}

func TestRoute53_GetHostedZone_NotFound(t *testing.T) {
	ts := newR53TestServer(t)
	resp := r53Request(t, ts, http.MethodGet, "/2013-04-01/hostedzone/ZNONEXISTENT", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GetHostedZone not found: expected 404, got %d", resp.StatusCode)
	}
}

func TestRoute53_DeleteHostedZone(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID

	delResp := r53Request(t, ts, http.MethodDelete, "/2013-04-01"+zoneID, "")
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteHostedZone: expected 200, got %d", delResp.StatusCode)
	}
	var delResult struct {
		ChangeInfo struct {
			Status string `xml:"Status"`
		} `xml:"ChangeInfo"`
	}
	if err := xml.NewDecoder(delResp.Body).Decode(&delResult); err != nil {
		t.Fatalf("decode delete: %v", err)
	}
	if delResult.ChangeInfo.Status != "INSYNC" {
		t.Errorf("expected INSYNC, got %q", delResult.ChangeInfo.Status)
	}

	// Verify gone.
	listResp := r53Request(t, ts, http.MethodGet, "/2013-04-01/hostedzone", "")
	defer listResp.Body.Close() //nolint:errcheck
	var listResult struct {
		HostedZones []struct{} `xml:"HostedZones>HostedZone"`
	}
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResult.HostedZones) != 0 {
		t.Errorf("expected 0 zones after delete, got %d", len(listResult.HostedZones))
	}
}

func TestRoute53_ChangeResourceRecordSets_Create(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID

	changeBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeResp := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", changeBody)
	defer changeResp.Body.Close() //nolint:errcheck
	if changeResp.StatusCode != http.StatusOK {
		t.Fatalf("ChangeResourceRecordSets CREATE: expected 200, got %d", changeResp.StatusCode)
	}
	var changeResult struct {
		ChangeInfo struct {
			Status string `xml:"Status"`
		} `xml:"ChangeInfo"`
	}
	if err := xml.NewDecoder(changeResp.Body).Decode(&changeResult); err != nil {
		t.Fatalf("decode change: %v", err)
	}
	if changeResult.ChangeInfo.Status != "INSYNC" {
		t.Errorf("expected INSYNC, got %q", changeResult.ChangeInfo.Status)
	}

	// Verify via List.
	listResp := r53Request(t, ts, http.MethodGet, "/2013-04-01"+zoneID+"/rrset", "")
	defer listResp.Body.Close() //nolint:errcheck
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("ListResourceRecordSets: expected 200, got %d", listResp.StatusCode)
	}
	var listResult struct {
		ResourceRecordSets []struct {
			Name string `xml:"Name"`
			Type string `xml:"Type"`
		} `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResult.ResourceRecordSets) != 1 {
		t.Fatalf("expected 1 RRSet, got %d", len(listResult.ResourceRecordSets))
	}
	if listResult.ResourceRecordSets[0].Name != "www.example.com" {
		t.Errorf("expected www.example.com, got %q", listResult.ResourceRecordSets[0].Name)
	}
	if listResult.ResourceRecordSets[0].Type != "A" {
		t.Errorf("expected A, got %q", listResult.ResourceRecordSets[0].Type)
	}
}

func TestRoute53_ChangeResourceRecordSets_Upsert(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID

	createBody := func(action, ip string) string {
		return `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>` + action + `</Action><ResourceRecordSet><Name>api.example.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>` + ip + `</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	}

	// CREATE first.
	r1 := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", createBody("CREATE", "10.0.0.1"))
	r1.Body.Close() //nolint:errcheck

	// UPSERT (update value).
	r2 := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", createBody("UPSERT", "10.0.0.2"))
	defer r2.Body.Close() //nolint:errcheck
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("UPSERT: expected 200, got %d", r2.StatusCode)
	}

	// List and verify only 1 record (upsert, not duplicate).
	listResp := r53Request(t, ts, http.MethodGet, "/2013-04-01"+zoneID+"/rrset", "")
	defer listResp.Body.Close() //nolint:errcheck
	var listResult struct {
		ResourceRecordSets []struct{} `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResult.ResourceRecordSets) != 1 {
		t.Errorf("expected 1 RRSet after upsert, got %d", len(listResult.ResourceRecordSets))
	}
}

func TestRoute53_ChangeResourceRecordSets_Delete(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID

	createRR := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action><ResourceRecordSet><Name>del.example.com</Name><Type>CNAME</Type><TTL>300</TTL><ResourceRecords><ResourceRecord><Value>target.example.com</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	r1 := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", createRR)
	r1.Body.Close() //nolint:errcheck

	deleteRR := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>DELETE</Action><ResourceRecordSet><Name>del.example.com</Name><Type>CNAME</Type><TTL>300</TTL><ResourceRecords><ResourceRecord><Value>target.example.com</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	r2 := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", deleteRR)
	defer r2.Body.Close() //nolint:errcheck
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("DELETE: expected 200, got %d", r2.StatusCode)
	}

	listResp := r53Request(t, ts, http.MethodGet, "/2013-04-01"+zoneID+"/rrset", "")
	defer listResp.Body.Close() //nolint:errcheck
	var listResult struct {
		ResourceRecordSets []struct{} `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResult.ResourceRecordSets) != 0 {
		t.Errorf("expected 0 RRSets after delete, got %d", len(listResult.ResourceRecordSets))
	}
}

func TestRoute53_ListResourceRecordSets(t *testing.T) {
	ts := newR53TestServer(t)
	cr := r53Request(t, ts, http.MethodPost, "/2013-04-01/hostedzone", r53CreateZoneBody)
	defer cr.Body.Close() //nolint:errcheck
	var createResult struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	if err := xml.NewDecoder(cr.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	zoneID := createResult.HostedZone.ID

	for _, rec := range []struct{ name, rtype, value string }{
		{"a.example.com", "A", "1.2.3.4"},
		{"b.example.com", "CNAME", "a.example.com"},
		{"c.example.com", "TXT", `"hello"`},
	} {
		body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action><ResourceRecordSet><Name>` + rec.name + `</Name><Type>` + rec.rtype + `</Type><TTL>300</TTL><ResourceRecords><ResourceRecord><Value>` + rec.value + `</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
		r := r53Request(t, ts, http.MethodPost, "/2013-04-01"+zoneID+"/rrset", body)
		r.Body.Close() //nolint:errcheck
	}

	listResp := r53Request(t, ts, http.MethodGet, "/2013-04-01"+zoneID+"/rrset", "")
	defer listResp.Body.Close() //nolint:errcheck
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", listResp.StatusCode)
	}
	var listResult struct {
		ResourceRecordSets []struct {
			Name string `xml:"Name"`
			Type string `xml:"Type"`
		} `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	if err := xml.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResult.ResourceRecordSets) != 3 {
		t.Fatalf("expected 3 RRSets, got %d", len(listResult.ResourceRecordSets))
	}
}

func TestRoute53_InvalidPath(t *testing.T) {
	ts := newR53TestServer(t)
	resp := r53Request(t, ts, http.MethodGet, "/2013-04-01/unknown/resource", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid path: expected 400, got %d", resp.StatusCode)
	}
}
