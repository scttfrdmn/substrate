package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// r53XMLHostedZone is a HostedZone as ListHostedZonesByName and UpdateHostedZoneComment render it.
type r53XMLHostedZone struct {
	ID              string `xml:"Id"`
	Name            string `xml:"Name"`
	CallerReference string `xml:"CallerReference"`
	Config          struct {
		Comment     string `xml:"Comment,omitempty"`
		PrivateZone bool   `xml:"PrivateZone"`
	} `xml:"Config"`
	ResourceRecordSetCount int64 `xml:"ResourceRecordSetCount"`
}

// r53HostedZoneXML renders a stored zone.
func r53HostedZoneXML(zone Route53HostedZone) r53XMLHostedZone {
	out := r53XMLHostedZone{
		ID: zone.ID, Name: r53WireName(zone.Name), CallerReference: zone.CallerReference,
		ResourceRecordSetCount: zone.ResourceRecordSetCount,
	}
	out.Config.Comment = zone.Config.Comment
	out.Config.PrivateZone = zone.Config.PrivateZone
	return out
}

// r53ListPageMax is the page size API_ListHostedZonesByName and API_ListHealthChecks publish: "Route
// 53 returns up to 100 items in each response", and a larger maxitems is answered with 100.
const r53ListPageMax = 100

// r53PageSize reads a maxitems query value: absent is [r53ListPageMax], above it is capped. A value
// that is not a positive whole number is InvalidInput (400), "The input is not valid.", which both
// list pages publish; the message is substrate's own.
func r53PageSize(raw string) (int, *AWSError) {
	if raw == "" {
		return r53ListPageMax, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, &AWSError{Code: "InvalidInput", Message: fmt.Sprintf("maxitems %q is not a positive whole number.", raw),
			HTTPStatus: http.StatusBadRequest}
	}
	return min(n, r53ListPageMax), nil
}

// r53ReversedName returns a zone name in the order ListHostedZonesByName sorts by. The page: it
// "sorts hosted zones by name with the labels reversed. For example: com.example.www. Note the
// trailing dot". Names are compared lower-cased, since Route 53 treats DNS names case-insensitively.
func r53ReversedName(name string) string {
	labels := strings.Split(r53CanonicalName(name), ".")
	slices.Reverse(labels)
	return strings.Join(labels, ".") + "."
}

// r53ValidDNSName reports whether name is a domain name ListHostedZonesByName can sort from. The
// Developer Guide's "DNS domain name format": "Each label can be up to 63 bytes long. The total length
// of a domain name cannot exceed 255 bytes, including the dots." The bytes are the name's octets, so
// an octal escape such as \052 is one byte, not four ([r53NameOctets]). A single trailing dot is
// allowed, and an empty label is not.
func r53ValidDNSName(name string) bool {
	trimmed := strings.TrimSuffix(name, ".")
	if trimmed == "" {
		return false
	}
	labels := strings.Split(trimmed, ".")
	total := len(labels) - 1
	for _, label := range labels {
		n := r53NameOctets(label)
		if n == 0 || n > 63 {
			return false
		}
		total += n
	}
	return total <= 255
}

// listHostedZonesByName handles ListHostedZonesByName.
//
// The caller's zones are sorted by [r53ReversedName], then by ID, so two zones of one name keep one
// order. dnsname starts the page at the first zone whose name sorts at or after it; dnsname with
// hostedzoneid starts it at that zone, or the first after it. The page publishes the response's
// DNSName, HostedZoneId and MaxItems as the request's own values, and NextDNSName and
// NextHostedZoneId, the first zone of the next page, "only if IsTruncated is true".
//
// API_ListHostedZonesByName publishes InvalidDomainName (400), "The specified domain name is not
// valid.", which answers a dnsname with an empty label or one longer than 63 characters, and
// InvalidInput (400), which answers a hostedzoneid without a dnsname: "For subsequent requests,
// include both dnsname and hostedzoneid parameters". Both messages are substrate's own.
func (p *Route53Plugin) listHostedZonesByName(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	dnsName, startID := req.Params["dnsname"], req.Params["hostedzoneid"]
	if startID != "" && dnsName == "" {
		return nil, &AWSError{Code: "InvalidInput", Message: "The hostedzoneid parameter requires the dnsname parameter.",
			HTTPStatus: http.StatusBadRequest}
	}
	if dnsName != "" && !r53ValidDNSName(dnsName) {
		return nil, &AWSError{Code: "InvalidDomainName", Message: fmt.Sprintf("%s is not a valid domain name.", dnsName),
			HTTPStatus: http.StatusBadRequest}
	}
	size, awsErr := r53PageSize(req.Params["maxitems"])
	if awsErr != nil {
		return nil, awsErr
	}

	zones, err := p.accountZones(reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("route53 listHostedZonesByName: %w", err)
	}
	slices.SortFunc(zones, func(a, b Route53HostedZone) int {
		if c := strings.Compare(r53ReversedName(a.Name), r53ReversedName(b.Name)); c != 0 {
			return c
		}
		return strings.Compare(r53ZoneSuffix(a.ID), r53ZoneSuffix(b.ID))
	})
	start := 0
	if dnsName != "" {
		from, fromID := r53ReversedName(dnsName), r53ZoneSuffix(startID)
		start = len(zones)
		for i, z := range zones {
			c := strings.Compare(r53ReversedName(z.Name), from)
			if c > 0 || (c == 0 && r53ZoneSuffix(z.ID) >= fromID) {
				start = i
				break
			}
		}
	}

	type xmlResponse struct {
		XMLName          xml.Name           `xml:"ListHostedZonesByNameResponse"`
		XMLNS            string             `xml:"xmlns,attr"`
		HostedZones      []r53XMLHostedZone `xml:"HostedZones>HostedZone"`
		DNSName          string             `xml:"DNSName,omitempty"`
		HostedZoneID     string             `xml:"HostedZoneId,omitempty"`
		IsTruncated      bool               `xml:"IsTruncated"`
		NextDNSName      string             `xml:"NextDNSName,omitempty"`
		NextHostedZoneID string             `xml:"NextHostedZoneId,omitempty"`
		MaxItems         string             `xml:"MaxItems"`
	}
	resp := xmlResponse{XMLNS: r53XMLNS, DNSName: dnsName, HostedZoneID: startID, MaxItems: strconv.Itoa(size)}
	if raw := req.Params["maxitems"]; raw != "" {
		resp.MaxItems = raw
	}
	end := min(start+size, len(zones))
	for _, z := range zones[start:end] {
		resp.HostedZones = append(resp.HostedZones, r53HostedZoneXML(z))
	}
	if end < len(zones) {
		resp.IsTruncated = true
		resp.NextDNSName = r53WireName(zones[end].Name)
		resp.NextHostedZoneID = r53ZoneSuffix(zones[end].ID)
	}
	return r53XMLResponse(http.StatusOK, resp)
}

// accountZones reads every hosted zone in account's index, in index order. A store fault or an
// undecodable record is an error; an index entry that outlived its zone is skipped (#1417).
func (p *Route53Plugin) accountZones(account string) ([]Route53HostedZone, error) {
	ids, err := p.loadList(account, "hostedzone_ids")
	if err != nil {
		return nil, err
	}
	zones := make([]Route53HostedZone, 0, len(ids))
	for _, id := range ids {
		zone, found, err := r53LoadZone(context.Background(), p.state, id)
		if err != nil {
			return nil, err
		}
		if found {
			zones = append(zones, zone)
		}
	}
	return zones, nil
}

// updateHostedZoneComment handles UpdateHostedZoneComment: the request's Comment replaces the zone's,
// and an absent one clears it ("If you don't specify a value for Comment, Route 53 deletes the existing
// value of the Comment element, if any", API_UpdateHostedZoneComment). NoSuchHostedZone (404) answers
// an absent zone.
func (p *Route53Plugin) updateHostedZoneComment(_ *RequestContext, req *AWSRequest, zoneID string) (*AWSResponse, error) {
	var xmlReq struct {
		XMLName xml.Name `xml:"UpdateHostedZoneCommentRequest"`
		Comment string   `xml:"Comment"`
	}
	if len(bytes.TrimSpace(req.Body)) > 0 {
		if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
			return nil, &AWSError{Code: "MalformedXML", Message: "could not parse request body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	zone, err := p.loadHostedZone(zoneID)
	if err != nil {
		return nil, err
	}
	zone.Config.Comment = xmlReq.Comment
	if err := p.putZone(r53ZoneSuffix(zoneID), zone); err != nil {
		return nil, fmt.Errorf("route53 updateHostedZoneComment: %w", err)
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName    xml.Name         `xml:"UpdateHostedZoneCommentResponse"`
		XMLNS      string           `xml:"xmlns,attr"`
		HostedZone r53XMLHostedZone `xml:"HostedZone"`
	}{XMLNS: r53XMLNS, HostedZone: r53HostedZoneXML(zone)})
}

// r53DecodeJSON decodes a stored record, naming key in the error.
func r53DecodeJSON(data []byte, key string, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("route53 decode %s: %w", key, err)
	}
	return nil
}
