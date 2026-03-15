package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Route53Plugin emulates the Amazon Route 53 DNS service.
// It supports hosted zone management and resource record set operations
// using the Route 53 REST/XML API protocol.
type Route53Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "route53".
func (p *Route53Plugin) Name() string { return "route53" }

// Initialize sets up the Route53Plugin with the provided configuration and
// optional TimeController from Options["time_controller"].
func (p *Route53Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"]; ok {
		if typed, ok := tc.(*TimeController); ok {
			p.tc = typed
		}
	}
	return nil
}

// now returns the current time from the TimeController if set, else time.Now().
func (p *Route53Plugin) now() time.Time {
	if p.tc != nil {
		return p.tc.Now()
	}
	return time.Now()
}

// Shutdown is a no-op for Route53Plugin.
func (p *Route53Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Route 53 REST/XML request to the appropriate handler.
// The operation is derived from the HTTP method and URL path.
func (p *Route53Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, zoneID := parseRoute53Operation(req.Operation, req.Path)
	switch op {
	case "CreateHostedZone":
		return p.createHostedZone(ctx, req)
	case "ListHostedZones":
		return p.listHostedZones(ctx, req)
	case "GetHostedZone":
		return p.getHostedZone(ctx, req, zoneID)
	case "DeleteHostedZone":
		return p.deleteHostedZone(ctx, req, zoneID)
	case "ChangeResourceRecordSets":
		return p.changeResourceRecordSets(ctx, req, zoneID)
	case "ListResourceRecordSets":
		return p.listResourceRecordSets(ctx, req, zoneID)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "Route53Plugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Hosted Zone operations ---

func (p *Route53Plugin) createHostedZone(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var xmlReq struct {
		XMLName         xml.Name `xml:"CreateHostedZoneRequest"`
		Name            string   `xml:"Name"`
		CallerReference string   `xml:"CallerReference"`
		Config          struct {
			Comment     string `xml:"Comment"`
			PrivateZone string `xml:"PrivateZone"`
		} `xml:"HostedZoneConfig"`
	}
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
		return nil, &AWSError{Code: "MalformedXML", Message: "could not parse request body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	name := xmlReq.Name
	if name == "" {
		return nil, &AWSError{Code: "InvalidInput", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if !strings.HasSuffix(name, ".") {
		name += "."
	}

	isPrivate := strings.EqualFold(xmlReq.Config.PrivateZone, "true")
	suffix := generateHostedZoneID()
	zoneID := "Z" + suffix
	fullID := "/hostedzone/" + zoneID

	zone := Route53HostedZone{
		ID:              fullID,
		Name:            name,
		Config:          Route53ZoneConfig{Comment: xmlReq.Config.Comment, PrivateZone: isPrivate},
		CallerReference: xmlReq.CallerReference,
		AccountID:       reqCtx.AccountID,
	}
	data, err := json.Marshal(zone)
	if err != nil {
		return nil, fmt.Errorf("route53 createHostedZone marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, "hostedzone:"+zoneID, data); err != nil {
		return nil, fmt.Errorf("route53 createHostedZone state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID, "hostedzone_ids", zoneID); err != nil {
		return nil, err
	}

	changeInfo := Route53ChangeInfo{
		ID:          generateChangeID(),
		Status:      "INSYNC",
		SubmittedAt: p.now().UTC(),
	}

	type xmlZone struct {
		ID                     string `xml:"Id"`
		Name                   string `xml:"Name"`
		CallerReference        string `xml:"CallerReference"`
		ResourceRecordSetCount int64  `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment     string `xml:"Comment,omitempty"`
			PrivateZone bool   `xml:"PrivateZone"`
		} `xml:"Config"`
	}
	type xmlChangeInfo struct {
		ID          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type xmlDelegationSet struct {
		NameServers []string `xml:"NameServers>NameServer"`
	}
	type xmlResponse struct {
		XMLName       xml.Name         `xml:"CreateHostedZoneResponse"`
		XMLNS         string           `xml:"xmlns,attr"`
		HostedZone    xmlZone          `xml:"HostedZone"`
		ChangeInfo    xmlChangeInfo    `xml:"ChangeInfo"`
		DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	}

	resp := xmlResponse{
		XMLNS: r53XMLNS,
		HostedZone: xmlZone{
			ID:              fullID,
			Name:            name,
			CallerReference: xmlReq.CallerReference,
		},
		ChangeInfo: xmlChangeInfo{
			ID:          changeInfo.ID,
			Status:      changeInfo.Status,
			SubmittedAt: changeInfo.SubmittedAt.Format(time.RFC3339),
		},
		DelegationSet: xmlDelegationSet{
			NameServers: []string{
				"ns-1.awsdns-01.com.",
				"ns-2.awsdns-02.net.",
				"ns-3.awsdns-03.org.",
				"ns-4.awsdns-04.co.uk.",
			},
		},
	}
	resp.HostedZone.Config.Comment = xmlReq.Config.Comment
	resp.HostedZone.Config.PrivateZone = isPrivate

	return r53XMLResponse(http.StatusCreated, resp)
}

func (p *Route53Plugin) listHostedZones(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	ids, err := p.loadList(reqCtx.AccountID, "hostedzone_ids")
	if err != nil {
		return nil, fmt.Errorf("route53 listHostedZones list: %w", err)
	}

	type xmlZone struct {
		ID                     string `xml:"Id"`
		Name                   string `xml:"Name"`
		CallerReference        string `xml:"CallerReference"`
		ResourceRecordSetCount int64  `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment     string `xml:"Comment,omitempty"`
			PrivateZone bool   `xml:"PrivateZone"`
		} `xml:"Config"`
	}
	type xmlResponse struct {
		XMLName     xml.Name  `xml:"ListHostedZonesResponse"`
		XMLNS       string    `xml:"xmlns,attr"`
		HostedZones []xmlZone `xml:"HostedZones>HostedZone"`
		IsTruncated bool      `xml:"IsTruncated"`
		MaxItems    string    `xml:"MaxItems"`
	}
	resp := xmlResponse{XMLNS: r53XMLNS, MaxItems: "100"}
	for _, zoneID := range ids {
		data, getErr := p.state.Get(context.Background(), route53Namespace, "hostedzone:"+zoneID)
		if getErr != nil || data == nil {
			continue
		}
		var zone Route53HostedZone
		if json.Unmarshal(data, &zone) != nil {
			continue
		}
		item := xmlZone{
			ID:                     zone.ID,
			Name:                   zone.Name,
			CallerReference:        zone.CallerReference,
			ResourceRecordSetCount: zone.ResourceRecordSetCount,
		}
		item.Config.Comment = zone.Config.Comment
		item.Config.PrivateZone = zone.Config.PrivateZone
		resp.HostedZones = append(resp.HostedZones, item)
	}
	return r53XMLResponse(http.StatusOK, resp)
}

func (p *Route53Plugin) getHostedZone(_ *RequestContext, _ *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	data, err := p.state.Get(context.Background(), route53Namespace, "hostedzone:"+id)
	if err != nil {
		return nil, fmt.Errorf("route53 getHostedZone get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NoSuchHostedZone", Message: "No hosted zone found with ID: " + zoneID, HTTPStatus: http.StatusNotFound}
	}
	var zone Route53HostedZone
	if err := json.Unmarshal(data, &zone); err != nil {
		return nil, fmt.Errorf("route53 getHostedZone unmarshal: %w", err)
	}

	type xmlZone struct {
		ID                     string `xml:"Id"`
		Name                   string `xml:"Name"`
		CallerReference        string `xml:"CallerReference"`
		ResourceRecordSetCount int64  `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment     string `xml:"Comment,omitempty"`
			PrivateZone bool   `xml:"PrivateZone"`
		} `xml:"Config"`
	}
	type xmlDelegationSet struct {
		NameServers []string `xml:"NameServers>NameServer"`
	}
	type xmlResponse struct {
		XMLName       xml.Name         `xml:"GetHostedZoneResponse"`
		XMLNS         string           `xml:"xmlns,attr"`
		HostedZone    xmlZone          `xml:"HostedZone"`
		DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	}
	item := xmlZone{
		ID:                     zone.ID,
		Name:                   zone.Name,
		CallerReference:        zone.CallerReference,
		ResourceRecordSetCount: zone.ResourceRecordSetCount,
	}
	item.Config.Comment = zone.Config.Comment
	item.Config.PrivateZone = zone.Config.PrivateZone
	return r53XMLResponse(http.StatusOK, xmlResponse{
		XMLNS:      r53XMLNS,
		HostedZone: item,
		DelegationSet: xmlDelegationSet{
			NameServers: []string{"ns-1.awsdns-01.com.", "ns-2.awsdns-02.net.", "ns-3.awsdns-03.org.", "ns-4.awsdns-04.co.uk."},
		},
	})
}

func (p *Route53Plugin) deleteHostedZone(reqCtx *RequestContext, _ *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	if err := p.state.Delete(context.Background(), route53Namespace, "hostedzone:"+id); err != nil {
		return nil, fmt.Errorf("route53 deleteHostedZone delete: %w", err)
	}
	p.removeFromList(reqCtx.AccountID, "hostedzone_ids", id)

	changeInfo := Route53ChangeInfo{
		ID:          generateChangeID(),
		Status:      "INSYNC",
		SubmittedAt: p.now().UTC(),
	}
	type xmlChangeInfo struct {
		ID          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type xmlResponse struct {
		XMLName    xml.Name      `xml:"DeleteHostedZoneResponse"`
		XMLNS      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}
	return r53XMLResponse(http.StatusOK, xmlResponse{
		XMLNS: r53XMLNS,
		ChangeInfo: xmlChangeInfo{
			ID:          changeInfo.ID,
			Status:      changeInfo.Status,
			SubmittedAt: changeInfo.SubmittedAt.Format(time.RFC3339),
		},
	})
}

// --- Resource Record Set operations ---

func (p *Route53Plugin) changeResourceRecordSets(_ *RequestContext, req *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)

	// Parse the XML change batch.
	var xmlReq struct {
		XMLName     xml.Name `xml:"ChangeResourceRecordSetsRequest"`
		ChangeBatch struct {
			Comment string `xml:"Comment"`
			Changes []struct {
				Action            string `xml:"Action"`
				ResourceRecordSet struct {
					Name            string `xml:"Name"`
					Type            string `xml:"Type"`
					TTL             string `xml:"TTL"`
					ResourceRecords []struct {
						Value string `xml:"Value"`
					} `xml:"ResourceRecords>ResourceRecord"`
					AliasTarget *struct {
						HostedZoneID         string `xml:"HostedZoneId"`
						DNSName              string `xml:"DNSName"`
						EvaluateTargetHealth string `xml:"EvaluateTargetHealth"`
					} `xml:"AliasTarget"`
				} `xml:"ResourceRecordSet"`
			} `xml:"Changes>Change"`
		} `xml:"ChangeBatch"`
	}
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
		return nil, &AWSError{Code: "MalformedXML", Message: "could not parse change batch: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	for _, change := range xmlReq.ChangeBatch.Changes {
		rrs := change.ResourceRecordSet
		key := r53RRSetKey(rrs.Type, rrs.Name)
		fullKey := "rrset:" + id + ":" + key

		switch strings.ToUpper(change.Action) {
		case "CREATE", "UPSERT":
			ttl, _ := strconv.ParseInt(rrs.TTL, 10, 64)
			rrset := Route53ResourceRecordSet{
				Name:   rrs.Name,
				Type:   rrs.Type,
				TTL:    ttl,
				ZoneID: id,
			}
			for _, rr := range rrs.ResourceRecords {
				rrset.ResourceRecords = append(rrset.ResourceRecords, Route53ResourceRecord{Value: rr.Value})
			}
			if rrs.AliasTarget != nil {
				evalHealth := strings.EqualFold(rrs.AliasTarget.EvaluateTargetHealth, "true")
				rrset.AliasTarget = &Route53AliasTarget{
					HostedZoneID:         rrs.AliasTarget.HostedZoneID,
					DNSName:              rrs.AliasTarget.DNSName,
					EvaluateTargetHealth: evalHealth,
				}
			}
			data, marshalErr := json.Marshal(rrset)
			if marshalErr != nil {
				return nil, fmt.Errorf("route53 changeRRSet marshal: %w", marshalErr)
			}
			if err := p.state.Put(context.Background(), route53Namespace, fullKey, data); err != nil {
				return nil, fmt.Errorf("route53 changeRRSet put: %w", err)
			}
			if err := p.appendToList(id, "rrset_keys", key); err != nil {
				return nil, err
			}
		case "DELETE":
			if err := p.state.Delete(context.Background(), route53Namespace, fullKey); err != nil {
				return nil, fmt.Errorf("route53 changeRRSet delete: %w", err)
			}
			p.removeFromList(id, "rrset_keys", key)
		}
	}

	changeInfo := Route53ChangeInfo{
		ID:          generateChangeID(),
		Status:      "INSYNC",
		SubmittedAt: p.now().UTC(),
		Comment:     xmlReq.ChangeBatch.Comment,
	}
	type xmlChangeInfo struct {
		ID          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
		Comment     string `xml:"Comment,omitempty"`
	}
	type xmlResponse struct {
		XMLName    xml.Name      `xml:"ChangeResourceRecordSetsResponse"`
		XMLNS      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}
	return r53XMLResponse(http.StatusOK, xmlResponse{
		XMLNS: r53XMLNS,
		ChangeInfo: xmlChangeInfo{
			ID:          changeInfo.ID,
			Status:      changeInfo.Status,
			SubmittedAt: changeInfo.SubmittedAt.Format(time.RFC3339),
			Comment:     changeInfo.Comment,
		},
	})
}

func (p *Route53Plugin) listResourceRecordSets(_ *RequestContext, _ *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	keys, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return nil, fmt.Errorf("route53 listResourceRecordSets list: %w", err)
	}

	type xmlRR struct {
		Value string `xml:"Value"`
	}
	type xmlAliasTarget struct {
		HostedZoneID         string `xml:"HostedZoneId"`
		DNSName              string `xml:"DNSName"`
		EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
	}
	type xmlRRSet struct {
		Name            string          `xml:"Name"`
		Type            string          `xml:"Type"`
		TTL             int64           `xml:"TTL,omitempty"`
		ResourceRecords []xmlRR         `xml:"ResourceRecords>ResourceRecord"`
		AliasTarget     *xmlAliasTarget `xml:"AliasTarget,omitempty"`
	}
	type xmlResponse struct {
		XMLName            xml.Name   `xml:"ListResourceRecordSetsResponse"`
		XMLNS              string     `xml:"xmlns,attr"`
		ResourceRecordSets []xmlRRSet `xml:"ResourceRecordSets>ResourceRecordSet"`
		IsTruncated        bool       `xml:"IsTruncated"`
		MaxItems           string     `xml:"MaxItems"`
	}
	resp := xmlResponse{XMLNS: r53XMLNS, MaxItems: "300"}
	for _, k := range keys {
		fullKey := "rrset:" + id + ":" + k
		data, getErr := p.state.Get(context.Background(), route53Namespace, fullKey)
		if getErr != nil || data == nil {
			continue
		}
		var rrset Route53ResourceRecordSet
		if json.Unmarshal(data, &rrset) != nil {
			continue
		}
		item := xmlRRSet{
			Name: rrset.Name,
			Type: rrset.Type,
			TTL:  rrset.TTL,
		}
		for _, rr := range rrset.ResourceRecords {
			item.ResourceRecords = append(item.ResourceRecords, xmlRR{Value: rr.Value}) //nolint:staticcheck
		}
		if rrset.AliasTarget != nil {
			item.AliasTarget = &xmlAliasTarget{
				HostedZoneID:         rrset.AliasTarget.HostedZoneID,
				DNSName:              rrset.AliasTarget.DNSName,
				EvaluateTargetHealth: rrset.AliasTarget.EvaluateTargetHealth,
			}
		}
		resp.ResourceRecordSets = append(resp.ResourceRecordSets, item)
	}
	return r53XMLResponse(http.StatusOK, resp)
}

// --- Helpers ---

// r53XMLNS is the XML namespace for Route 53 API responses.
const r53XMLNS = "https://route53.amazonaws.com/doc/2013-04-01/"

func (p *Route53Plugin) appendToList(scope, listName, id string) error {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), route53Namespace, key)
	if err != nil {
		return fmt.Errorf("route53 appendToList get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	// Dedup.
	for _, existing := range ids {
		if existing == id {
			return nil
		}
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), route53Namespace, key, newData)
}

func (p *Route53Plugin) removeFromList(scope, listName, id string) {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), route53Namespace, key)
	if err != nil || data == nil {
		return
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil {
		return
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, _ := json.Marshal(filtered)
	_ = p.state.Put(context.Background(), route53Namespace, key, newData)
}

func (p *Route53Plugin) loadList(scope, listName string) ([]string, error) {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), route53Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("route53 loadList get %s: %w", key, err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("route53 loadList unmarshal: %w", err)
	}
	return ids, nil
}

// r53XMLResponse serializes v to XML and returns an AWSResponse.
func r53XMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("route53 xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
