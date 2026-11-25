package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// Route53Plugin emulates the Amazon Route 53 DNS service.
// It supports hosted zone management and resource record set operations
// using the Route 53 REST/XML API protocol.
type Route53Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// mu serializes every read-check-write of a record a write operation rewrites: a hosted zone
	// and its record sets (create, delete, change batch, comment), a health check, and a resource's
	// tags (route53_healthcheck.go, route53_tags.go, route53_zones.go).
	mu sync.Mutex
	// seedMu serializes advancing a seeded change-status countdown (route53_change_progression.go).
	seedMu sync.Mutex
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
	op, zoneID := parseRoute53Operation(requestMethod(req), req.Path)
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
	case "ListHostedZonesByName":
		return p.listHostedZonesByName(ctx, req)
	case "UpdateHostedZoneComment":
		return p.updateHostedZoneComment(ctx, req, zoneID)
	case "GetChange":
		return p.getChange(ctx, req, zoneID)
	case "ChangeTagsForResource":
		return p.changeTagsForResource(ctx, req, zoneID)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req, zoneID)
	case "ListTagsForResources":
		return p.listTagsForResources(ctx, req, zoneID)
	case "CreateHealthCheck":
		return p.createHealthCheck(ctx, req)
	case "GetHealthCheck":
		return p.getHealthCheck(ctx, req, zoneID)
	case "ListHealthChecks":
		return p.listHealthChecks(ctx, req)
	case "UpdateHealthCheck":
		return p.updateHealthCheck(ctx, req, zoneID)
	case "DeleteHealthCheck":
		return p.deleteHealthCheck(ctx, req, zoneID)
	case "GetHealthCheckStatus":
		return p.getHealthCheckStatus(ctx, req, zoneID)
	case "GetHealthCheckCount":
		return p.getHealthCheckCount(ctx, req)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
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
		// Request validation refuses a client's request without one first; a dispatch the gate does
		// not see is answered in the same words.
		return nil, wireRequireMember("route53", "CreateHostedZone", req, "Name",
			&AWSError{Code: "InvalidInput", Message: "Name is required", HTTPStatus: http.StatusBadRequest})
	}
	if !strings.HasSuffix(name, ".") {
		name += "."
	}

	// The account's zone index is read and rewritten below, as DeleteHostedZone rewrites it.
	p.mu.Lock()
	defer p.mu.Unlock()
	isPrivate := strings.EqualFold(xmlReq.Config.PrivateZone, "true")
	suffix := generateHostedZoneID(reqCtx.IDs)
	zoneID := "Z" + suffix
	fullID := "/hostedzone/" + zoneID

	// The zone is written first, then its defaults and their index, then the account's zone index, so a
	// failure partway leaves no record set without its zone.
	defaults := r53DefaultRecordSets(zoneID, name)
	zone := Route53HostedZone{
		ID:                     fullID,
		Name:                   name,
		Config:                 Route53ZoneConfig{Comment: xmlReq.Config.Comment, PrivateZone: isPrivate},
		CallerReference:        xmlReq.CallerReference,
		ResourceRecordSetCount: int64(len(defaults)),
		AccountID:              reqCtx.AccountID,
		DefaultRecordSets:      true,
	}
	if err := p.putZone(zoneID, zone); err != nil {
		return nil, fmt.Errorf("route53 createHostedZone: %w", err)
	}
	index := make([]string, 0, len(defaults))
	for _, rrset := range defaults {
		key, err := p.putRecordSet(zoneID, rrset)
		if err != nil {
			return nil, fmt.Errorf("route53 createHostedZone default %s: %w", rrset.Type, err)
		}
		index = append(index, key)
	}
	if err := p.saveList(zoneID, "rrset_keys", index); err != nil {
		return nil, fmt.Errorf("route53 createHostedZone: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID, "hostedzone_ids", zoneID); err != nil {
		return nil, err
	}

	changeInfo, err := p.recordChange(reqCtx, "")
	if err != nil {
		return nil, fmt.Errorf("route53 createHostedZone: %w", err)
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
		XMLName       xml.Name         `xml:"CreateHostedZoneResponse"`
		XMLNS         string           `xml:"xmlns,attr"`
		HostedZone    xmlZone          `xml:"HostedZone"`
		ChangeInfo    r53XMLChangeInfo `xml:"ChangeInfo"`
		DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	}

	resp := xmlResponse{
		XMLNS: r53XMLNS,
		HostedZone: xmlZone{
			ID:                     fullID,
			Name:                   r53WireName(name),
			CallerReference:        xmlReq.CallerReference,
			ResourceRecordSetCount: zone.ResourceRecordSetCount,
		},
		ChangeInfo:    changeInfo,
		DelegationSet: xmlDelegationSet{NameServers: r53DefaultNameServers()},
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
		// A store fault or an undecodable record is an error, not a zone left out of the listing
		// (#1417); only an index entry that outlived its zone is skipped.
		data, getErr := p.state.Get(context.Background(), route53Namespace, "hostedzone:"+zoneID)
		if getErr != nil {
			return nil, fmt.Errorf("route53 listHostedZones get %s: %w", zoneID, getErr)
		}
		if data == nil {
			continue
		}
		var zone Route53HostedZone
		if err := json.Unmarshal(data, &zone); err != nil {
			return nil, fmt.Errorf("route53 listHostedZones unmarshal %s: %w", zoneID, err)
		}
		item := xmlZone{
			ID:                     zone.ID,
			Name:                   r53WireName(zone.Name),
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
		Name:                   r53WireName(zone.Name),
		CallerReference:        zone.CallerReference,
		ResourceRecordSetCount: zone.ResourceRecordSetCount,
	}
	item.Config.Comment = zone.Config.Comment
	item.Config.PrivateZone = zone.Config.PrivateZone
	return r53XMLResponse(http.StatusOK, xmlResponse{
		XMLNS:         r53XMLNS,
		HostedZone:    item,
		DelegationSet: xmlDelegationSet{NameServers: r53DefaultNameServers()},
	})
}

// deleteHostedZone handles DeleteHostedZone (#1416).
//
// API_DeleteHostedZone publishes NoSuchHostedZone (404), "No hosted zone exists with the ID that you
// specified", and HostedZoneNotEmpty (400), "The hosted zone contains resource records that are not
// SOA or NS records": "You can delete a hosted zone only if it contains only the default SOA and NS
// records". CreateHostedZone stores those two record sets (#1537), so every other record set a zone
// holds was written by ChangeResourceRecordSets and blocks the delete. A SOA or NS at the zone's apex
// does not: there is one record set per name and type, so a caller's write there is the default
// record modified, which the page does not count against the delete. A delete that goes ahead removes
// the default record sets and the zone's record-set index with the zone. The HostedZoneNotEmpty
// message is substrate's own; the page publishes none.
func (p *Route53Plugin) deleteHostedZone(reqCtx *RequestContext, _ *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	// The emptiness check and the delete are one step against a concurrent batch, tag or comment write.
	p.mu.Lock()
	defer p.mu.Unlock()
	zone, err := p.loadHostedZone(zoneID)
	if err != nil {
		return nil, err
	}
	blocking, err := p.nonDefaultRecordSet(id, zone.Name)
	if err != nil {
		return nil, err
	}
	if blocking != "" {
		return nil, &AWSError{
			Code:       "HostedZoneNotEmpty",
			Message:    fmt.Sprintf("The hosted zone %s contains the record set %s, which is not its SOA or NS record.", zoneID, blocking),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err := p.deleteZoneRecordSets(id); err != nil {
		return nil, err
	}
	if err := p.state.Delete(context.Background(), route53Namespace, "hostedzone:"+id); err != nil {
		return nil, fmt.Errorf("route53 deleteHostedZone delete: %w", err)
	}
	if err := p.removeFromList(reqCtx.AccountID, "hostedzone_ids", id); err != nil {
		return nil, fmt.Errorf("route53 deleteHostedZone removeFromList: %w", err)
	}

	changeInfo, err := p.recordChange(reqCtx, "")
	if err != nil {
		return nil, fmt.Errorf("route53 deleteHostedZone: %w", err)
	}
	type xmlResponse struct {
		XMLName    xml.Name         `xml:"DeleteHostedZoneResponse"`
		XMLNS      string           `xml:"xmlns,attr"`
		ChangeInfo r53XMLChangeInfo `xml:"ChangeInfo"`
	}
	return r53XMLResponse(http.StatusOK, xmlResponse{XMLNS: r53XMLNS, ChangeInfo: changeInfo})
}

// --- Resource Record Set operations ---

// requireHostedZone refuses a zone ID that names no hosted zone with NoSuchHostedZone (404), the
// code API_ChangeResourceRecordSets and API_ListResourceRecordSets publish for "No hosted zone exists
// with the ID that you specified" (#1410).
func (p *Route53Plugin) requireHostedZone(zoneID string) error {
	_, err := p.loadHostedZone(zoneID)
	return err
}

// loadHostedZone reads the zone a zone ID names, refusing an absent one as [requireHostedZone] does.
func (p *Route53Plugin) loadHostedZone(zoneID string) (Route53HostedZone, error) {
	zone, found, err := r53LoadZone(context.Background(), p.state, zoneID)
	if err != nil {
		return Route53HostedZone{}, err
	}
	if !found {
		return Route53HostedZone{}, &AWSError{Code: "NoSuchHostedZone", Message: "No hosted zone found with ID: " + zoneID, HTTPStatus: http.StatusNotFound}
	}
	return zone, nil
}

// nonDefaultRecordSet returns the index key of the first record set in zone id that is not the zone's
// own apex SOA or NS, or "" when there is none. A read or decode failure is an error, and an index
// entry whose record is gone is skipped, as the listings skip it (#1417).
func (p *Route53Plugin) nonDefaultRecordSet(id, zoneName string) (string, error) {
	keys, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return "", fmt.Errorf("route53 deleteHostedZone list: %w", err)
	}
	for _, k := range keys {
		rrset, found, err := p.loadRecordSet(id, k)
		if err != nil {
			return "", err
		}
		if !found {
			continue
		}
		if r53IsApexDefault(rrset.Type, rrset.Name, zoneName) {
			continue
		}
		return k, nil
	}
	return "", nil
}

// loadRecordSet reads one record set by its index key. found is false when the index entry outlived
// its record, which a caller skips; a read or decode failure is an error (#1417).
func (p *Route53Plugin) loadRecordSet(id, key string) (Route53ResourceRecordSet, bool, error) {
	fullKey := "rrset:" + id + ":" + key
	data, err := p.state.Get(context.Background(), route53Namespace, fullKey)
	if err != nil {
		return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 get record set %s: %w", fullKey, err)
	}
	if data == nil {
		return Route53ResourceRecordSet{}, false, nil
	}
	var rrset Route53ResourceRecordSet
	if err := json.Unmarshal(data, &rrset); err != nil {
		return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 decode record set %s: %w", fullKey, err)
	}
	return rrset, true, nil
}

func (p *Route53Plugin) changeResourceRecordSets(reqCtx *RequestContext, req *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	// The zone record, its record-set index and its record sets are read, checked and written as one,
	// against a tag or comment write to the zone and against another batch.
	p.mu.Lock()
	defer p.mu.Unlock()
	zone, err := p.loadHostedZone(zoneID)
	if err != nil {
		return nil, err
	}

	// Parse the XML change batch.
	var xmlReq struct {
		XMLName     xml.Name `xml:"ChangeResourceRecordSetsRequest"`
		ChangeBatch struct {
			Comment string `xml:"Comment"`
			Changes []struct {
				Action            string          `xml:"Action"`
				ResourceRecordSet r53XMLRecordSet `xml:"ResourceRecordSet"`
			} `xml:"Changes>Change"`
		} `xml:"ChangeBatch"`
	}
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
		return nil, &AWSError{Code: "MalformedXML", Message: "could not parse change batch: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Request validation checks a client's batch before the plugin runs; a batch the gate did not
	// see, a schedule's dispatch or any request with validation off, is checked here through the
	// gate's own walk, so it is answered in the same words with every failure the gate reports.
	if awsErr := wireCheckMember("route53", "ChangeResourceRecordSets", req, "ChangeBatch"); awsErr != nil {
		return nil, awsErr
	}

	index, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets list: %w", err)
	}

	// The page calls a change batch transactional — "Route 53 validates the changes in the request and
	// then either makes all or none of the changes" — so every refusal is found before any change is
	// made.
	changes := make([]r53BatchChange, 0, len(xmlReq.ChangeBatch.Changes))
	apex := make([]r53ApexChange, 0, len(xmlReq.ChangeBatch.Changes))
	for i, change := range xmlReq.ChangeBatch.Changes {
		rrs := change.ResourceRecordSet
		if !slices.Contains(r53RRTypes(), rrs.Type) {
			return nil, r53InvalidType(i, rrs.Type)
		}
		rrset, awsErr := rrs.recordSet(i, id)
		if awsErr != nil {
			return nil, awsErr
		}
		changes = append(changes, r53BatchChange{action: strings.ToUpper(change.Action), rrset: rrset})
		apex = append(apex, r53ApexChange{action: change.Action, rrType: rrs.Type, name: rrs.Name})
	}
	// The per-change check runs first, so a CREATE of the existing apex SOA or NS is answered as a
	// CREATE of any existing record set is; the apex check then reads the batch's end state.
	stored, err := p.loadStoredSets(id, index, changes)
	if err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
	}
	plan, awsErr := planChangeBatch(stored, changes)
	if awsErr != nil {
		return nil, awsErr
	}
	if zone.DefaultRecordSets {
		if awsErr := r53CheckApexDefaults(index, apex, zone.Name); awsErr != nil {
			return nil, awsErr
		}
	}

	// The index is held in memory across the batch and written once.
	for _, rrset := range plan.writes {
		key, err := p.putRecordSet(id, rrset)
		if err != nil {
			return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
		}
		if !slices.Contains(index, key) {
			index = append(index, key)
		}
	}
	if index, err = p.deleteRecordSetKeys(id, index, plan.removes); err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
	}

	if err := p.saveList(id, "rrset_keys", index); err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
	}
	// The count follows the deduplicated index the batch just wrote, so no record set is re-read.
	zone.ResourceRecordSetCount = int64(len(index))
	if err := p.putZone(id, zone); err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
	}

	changeInfo, err := p.recordChange(reqCtx, xmlReq.ChangeBatch.Comment)
	if err != nil {
		return nil, fmt.Errorf("route53 changeResourceRecordSets: %w", err)
	}
	type xmlResponse struct {
		XMLName    xml.Name         `xml:"ChangeResourceRecordSetsResponse"`
		XMLNS      string           `xml:"xmlns,attr"`
		ChangeInfo r53XMLChangeInfo `xml:"ChangeInfo"`
	}
	return r53XMLResponse(http.StatusOK, xmlResponse{XMLNS: r53XMLNS, ChangeInfo: changeInfo})
}
func (p *Route53Plugin) listResourceRecordSets(_ *RequestContext, _ *AWSRequest, zoneID string) (*AWSResponse, error) {
	id := r53ZoneSuffix(zoneID)
	if err := p.requireHostedZone(zoneID); err != nil {
		return nil, err
	}
	keys, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return nil, fmt.Errorf("route53 listResourceRecordSets list: %w", err)
	}

	type xmlRR struct {
		Value string `xml:"Value"`
	}
	// xmlRRs is the ResourceRecords element, absent for an alias record set: API_ResourceRecordSet
	// says "If you're creating an alias resource record set, omit ResourceRecords", and the list's
	// minimum is one, so an empty element read back and sent in a DELETE would be refused.
	type xmlRRs struct {
		Records []xmlRR `xml:"ResourceRecord"`
	}
	type xmlAliasTarget struct {
		HostedZoneID         string `xml:"HostedZoneId"`
		DNSName              string `xml:"DNSName"`
		EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
	}
	type xmlRRSet struct {
		Name            string          `xml:"Name"`
		Type            string          `xml:"Type"`
		SetIdentifier   string          `xml:"SetIdentifier,omitempty"`
		Weight          *int64          `xml:"Weight,omitempty"`
		Region          string          `xml:"Region,omitempty"`
		Failover        string          `xml:"Failover,omitempty"`
		TTL             int64           `xml:"TTL,omitempty"`
		ResourceRecords *xmlRRs         `xml:"ResourceRecords,omitempty"`
		AliasTarget     *xmlAliasTarget `xml:"AliasTarget,omitempty"`
		HealthCheckID   string          `xml:"HealthCheckId,omitempty"`
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
		// A store fault or an undecodable record is an error, not a record set left out of the
		// listing (#1417); only an index entry that outlived its record is skipped.
		rrset, found, loadErr := p.loadRecordSet(id, k)
		if loadErr != nil {
			return nil, fmt.Errorf("route53 listResourceRecordSets: %w", loadErr)
		}
		if !found {
			continue
		}
		item := xmlRRSet{
			Name:          r53WireName(rrset.Name),
			Type:          rrset.Type,
			SetIdentifier: rrset.SetIdentifier,
			Weight:        rrset.Weight,
			Region:        rrset.Region,
			Failover:      rrset.Failover,
			TTL:           rrset.TTL,
			HealthCheckID: rrset.HealthCheckID,
		}
		if len(rrset.ResourceRecords) > 0 {
			item.ResourceRecords = &xmlRRs{}
			for _, rr := range rrset.ResourceRecords {
				item.ResourceRecords.Records = append(item.ResourceRecords.Records, xmlRR{Value: rr.Value}) //nolint:staticcheck
			}
		}
		if rrset.AliasTarget != nil {
			// An alias target's DNSName is a domain name, so it is reported in the form every name is
			// ([r53WireName]). Route 53 is observed to lower-case it: a load balancer whose DNS name has
			// capitals is read back lower-cased (hashicorp/terraform-provider-aws#306), which is why the
			// provider normalizes alias names before comparing them.
			item.AliasTarget = &xmlAliasTarget{
				HostedZoneID:         rrset.AliasTarget.HostedZoneID,
				DNSName:              r53WireName(rrset.AliasTarget.DNSName),
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
	newData, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("route53 appendToList marshal: %w", err)
	}
	return p.state.Put(context.Background(), route53Namespace, key, newData)
}

func (p *Route53Plugin) removeFromList(scope, listName, id string) error {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), route53Namespace, key)
	if err != nil {
		return fmt.Errorf("route53 removeFromList %s state.Get: %w", key, err)
	}
	if data == nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return fmt.Errorf("route53 removeFromList %s unmarshal: %w", key, err)
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("route53 removeFromList %s marshal: %w", key, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, key, newData); err != nil {
		return fmt.Errorf("route53 removeFromList %s state.Put: %w", key, err)
	}
	return nil
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
