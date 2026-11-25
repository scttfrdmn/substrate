package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
)

// Route 53 health checks: CreateHealthCheck, GetHealthCheck, ListHealthChecks, UpdateHealthCheck,
// DeleteHealthCheck and GetHealthCheckCount (#1613). GetHealthCheckStatus is seeded, never computed
// (route53_healthcheck_status.go): substrate sends no request to an endpoint, which is the workload
// the scope rule keeps out.
//
// A health check is stored as "healthcheck:{id}" with the account that created it, and listed from
// "healthcheck_ids:{account}". Another account's ID is answered as absent. Its ID is a UUID, the form
// the API pages' examples show (abcdef11-2222-3333-4444-555555fedcba).
//
// CallerReference, API_CreateHealthCheck: "If you send a CreateHealthCheck request with the same
// CallerReference and settings as a previous request, and if the health check doesn't exist, Amazon
// Route 53 creates the health check. If the health check does exist, Route 53 returns the health check
// configuration in the response", while the same CallerReference with different settings, or the
// CallerReference of a deleted health check "regardless of the settings", is HealthCheckAlreadyExists
// (409). "Settings" are the ones the create was sent with: the reference record holds that
// configuration, so a retried create still matches after UpdateHealthCheck has changed the health
// check. A reference whose health check is not there is created again, the first rule's case. A
// caller reference is kept as "healthcheck_ref:{account}:{reference}" after its health check is
// deleted, which is what the second rule needs; the page says Route 53 forgets it "after a number
// of days", and substrate never does, because a duration would make the answer depend on the clock.
//
// HealthCheckVersion, API_UpdateHealthCheck: "A sequential counter that Amazon Route 53 sets to 1
// when you create a health check and increments by 1 each time you update settings". An update whose
// HealthCheckVersion differs from the stored one is HealthCheckVersionMismatch (409); one without it
// updates whatever the version. Type, RequestInterval, MeasureLatency and EndpointType cannot be
// changed, so the update request has no members for them. ResetElements sets FullyQualifiedDomainName,
// ResourcePath and ChildHealthChecks to null and Regions to the default (every region).
//
// HealthCheckConfig's published defaults are applied at create time, for the types that check an
// endpoint (HTTP, HTTPS, HTTP_STR_MATCH, HTTPS_STR_MATCH, TCP): "If you don't specify a value for
// RequestInterval, the default value is 30 seconds" and "if you don't specify a value for
// FailureThreshold, the default value is three health checks" (API_HealthCheckConfig).
//
// The error messages are the sentences the operations' pages give each code: NoSuchHealthCheck "No
// health check exists with the specified ID.", HealthCheckAlreadyExists "The health check you're
// attempting to create already exists.", HealthCheckVersionMismatch "The value of HealthCheckVersion
// in the request doesn't match the value of HealthCheckVersion in the health check." and
// TooManyHealthChecks "This health check can't be created because the current account has reached the
// limit on the number of active health checks." The limit is the Developer Guide's quota on health
// checks, 200 active health checks per account.

// r53MaxHealthChecks is the Developer Guide's quota on active health checks per account.
const r53MaxHealthChecks = 200

// r53EndpointCheckTypes are the health check types that check an endpoint, to which
// HealthCheckConfig's RequestInterval and FailureThreshold defaults apply.
var r53EndpointCheckTypes = []string{"HTTP", "HTTPS", "HTTP_STR_MATCH", "HTTPS_STR_MATCH", "TCP"} //nolint:gochecknoglobals // immutable table

// Route53AlarmIdentifier names the CloudWatch alarm a CLOUDWATCH_METRIC health check follows.
type Route53AlarmIdentifier struct {
	// Region is the alarm's Region.
	Region string `xml:"Region" json:"Region"`
	// Name is the alarm's name.
	Name string `xml:"Name" json:"Name"`
}

// Route53HealthCheckConfig is a health check's HealthCheckConfig, in the element order
// API_HealthCheckConfig gives it. It is both the stored form and the wire form.
type Route53HealthCheckConfig struct {
	// IPAddress is the endpoint's address.
	IPAddress string `xml:"IPAddress,omitempty" json:"IPAddress,omitempty"`
	// Port is the endpoint's port.
	Port *int64 `xml:"Port,omitempty" json:"Port,omitempty"`
	// Type is the kind of check.
	Type string `xml:"Type" json:"Type"`
	// ResourcePath is the path requested.
	ResourcePath string `xml:"ResourcePath,omitempty" json:"ResourcePath,omitempty"`
	// FullyQualifiedDomainName is the endpoint's domain name.
	FullyQualifiedDomainName string `xml:"FullyQualifiedDomainName,omitempty" json:"FullyQualifiedDomainName,omitempty"`
	// SearchString is the string a STR_MATCH check searches for.
	SearchString string `xml:"SearchString,omitempty" json:"SearchString,omitempty"`
	// RequestInterval is the seconds between requests.
	RequestInterval *int64 `xml:"RequestInterval,omitempty" json:"RequestInterval,omitempty"`
	// FailureThreshold is the consecutive results that change the status.
	FailureThreshold *int64 `xml:"FailureThreshold,omitempty" json:"FailureThreshold,omitempty"`
	// MeasureLatency asks for latency graphs.
	MeasureLatency *bool `xml:"MeasureLatency,omitempty" json:"MeasureLatency,omitempty"`
	// Inverted inverts the status.
	Inverted *bool `xml:"Inverted,omitempty" json:"Inverted,omitempty"`
	// Disabled stops the checks.
	Disabled *bool `xml:"Disabled,omitempty" json:"Disabled,omitempty"`
	// HealthThreshold is how many children a CALCULATED check needs healthy.
	HealthThreshold *int64 `xml:"HealthThreshold,omitempty" json:"HealthThreshold,omitempty"`
	// ChildHealthChecks are a CALCULATED check's children.
	ChildHealthChecks []string `xml:"ChildHealthChecks>ChildHealthCheck,omitempty" json:"ChildHealthChecks,omitempty"`
	// EnableSNI sends the domain name in the TLS client hello.
	EnableSNI *bool `xml:"EnableSNI,omitempty" json:"EnableSNI,omitempty"`
	// Regions are the checker Regions; empty means every one.
	Regions []string `xml:"Regions>Region,omitempty" json:"Regions,omitempty"`
	// AlarmIdentifier is a CLOUDWATCH_METRIC check's alarm.
	AlarmIdentifier *Route53AlarmIdentifier `xml:"AlarmIdentifier,omitempty" json:"AlarmIdentifier,omitempty"`
	// InsufficientDataHealthStatus is the status while the alarm lacks data.
	InsufficientDataHealthStatus string `xml:"InsufficientDataHealthStatus,omitempty" json:"InsufficientDataHealthStatus,omitempty"`
	// RoutingControlArn is a RECOVERY_CONTROL check's routing control.
	RoutingControlArn string `xml:"RoutingControlArn,omitempty" json:"RoutingControlArn,omitempty"`
	// EndpointType is PUBLIC or PRIVATE.
	EndpointType string `xml:"EndpointType,omitempty" json:"EndpointType,omitempty"`
	// HealthCheckSourceID is a PRIVATE check's health check source.
	HealthCheckSourceID string `xml:"HealthCheckSourceId,omitempty" json:"HealthCheckSourceId,omitempty"`
}

// Route53HealthCheck is a stored health check.
type Route53HealthCheck struct {
	// ID is the health check's ID.
	ID string `json:"Id"`
	// CallerReference is the create request's caller reference.
	CallerReference string `json:"CallerReference"`
	// Config is the health check's configuration.
	Config Route53HealthCheckConfig `json:"HealthCheckConfig"`
	// Version is the HealthCheckVersion, 1 at create and one more after each update.
	Version int64 `json:"HealthCheckVersion"`
	// AccountID is the account that created the health check.
	AccountID string `json:"AccountID"`
	// Tags holds the health check's tags, written by ChangeTagsForResource.
	Tags map[string]string `json:"Tags,omitempty"`
}

// r53CallerRef is what a health check caller reference is kept as: the health check it created,
// and whether that health check has been deleted.
type r53CallerRef struct {
	ID      string `json:"Id"`
	Deleted bool   `json:"Deleted,omitempty"`
	// Config is the configuration the create was sent with, defaults filled in. A retried create is
	// compared with it rather than with the health check's current configuration, which
	// UpdateHealthCheck changes.
	Config *Route53HealthCheckConfig `json:"HealthCheckConfig,omitempty"`
}

// r53XMLHealthCheck is a HealthCheck as the responses render it.
type r53XMLHealthCheck struct {
	ID                 string                   `xml:"Id"`
	CallerReference    string                   `xml:"CallerReference"`
	HealthCheckConfig  Route53HealthCheckConfig `xml:"HealthCheckConfig"`
	HealthCheckVersion int64                    `xml:"HealthCheckVersion"`
}

func r53HealthCheckXML(hc Route53HealthCheck) r53XMLHealthCheck {
	return r53XMLHealthCheck{ID: hc.ID, CallerReference: hc.CallerReference, HealthCheckConfig: hc.Config, HealthCheckVersion: hc.Version}
}

// r53NoSuchHealthCheck is the NoSuchHealthCheck (404) every health check operation publishes.
func r53NoSuchHealthCheck() *AWSError {
	return &AWSError{Code: "NoSuchHealthCheck", Message: "No health check exists with the specified ID.", HTTPStatus: http.StatusNotFound}
}

// r53IsCode reports whether err is an [AWSError] with code.
func r53IsCode(err error, code string) bool {
	var awsErr *AWSError
	return errors.As(err, &awsErr) && awsErr.Code == code
}

func r53HealthCheckKey(id string) string { return "healthcheck:" + id }

func r53CallerRefKey(account, ref string) string { return "healthcheck_ref:" + account + ":" + ref }

// loadHealthCheck reads the caller's health check id, refusing an absent one, or another account's,
// with NoSuchHealthCheck.
func (p *Route53Plugin) loadHealthCheck(reqCtx *RequestContext, id string) (Route53HealthCheck, error) {
	key := r53HealthCheckKey(id)
	data, err := p.state.Get(context.Background(), route53Namespace, key)
	if err != nil {
		return Route53HealthCheck{}, fmt.Errorf("route53 get health check %s: %w", id, err)
	}
	if data == nil {
		return Route53HealthCheck{}, r53NoSuchHealthCheck()
	}
	var hc Route53HealthCheck
	if err := r53DecodeJSON(data, key, &hc); err != nil {
		return Route53HealthCheck{}, err
	}
	if hc.AccountID != reqCtx.AccountID {
		return Route53HealthCheck{}, r53NoSuchHealthCheck()
	}
	return hc, nil
}

// putHealthCheck stores a health check.
func (p *Route53Plugin) putHealthCheck(hc Route53HealthCheck) error {
	return p.putJSON(r53HealthCheckKey(hc.ID), hc)
}

// putJSON stores v at key in the Route 53 namespace.
func (p *Route53Plugin) putJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("route53 marshal %s: %w", key, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, key, data); err != nil {
		return fmt.Errorf("route53 put %s: %w", key, err)
	}
	return nil
}

// r53DecodeBody decodes a request body into v, answering an unreadable one with MalformedXML.
func r53DecodeBody(req *AWSRequest, v any) *AWSError {
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(v); err != nil {
		return &AWSError{Code: "MalformedXML", Message: "could not parse request body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	return nil
}

// r53WithHealthCheckDefaults returns cfg with HealthCheckConfig's published defaults filled in.
func r53WithHealthCheckDefaults(cfg Route53HealthCheckConfig) Route53HealthCheckConfig {
	if !slices.Contains(r53EndpointCheckTypes, cfg.Type) {
		return cfg
	}
	if cfg.RequestInterval == nil {
		v := int64(30)
		cfg.RequestInterval = &v
	}
	if cfg.FailureThreshold == nil {
		v := int64(3)
		cfg.FailureThreshold = &v
	}
	return cfg
}

// createHealthCheck handles CreateHealthCheck.
func (p *Route53Plugin) createHealthCheck(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var xmlReq struct {
		XMLName           xml.Name                 `xml:"CreateHealthCheckRequest"`
		CallerReference   string                   `xml:"CallerReference"`
		HealthCheckConfig Route53HealthCheckConfig `xml:"HealthCheckConfig"`
	}
	if awsErr := r53DecodeBody(req, &xmlReq); awsErr != nil {
		return nil, awsErr
	}
	if xmlReq.CallerReference == "" {
		return nil, wireRequireMember("route53", "CreateHealthCheck", req, "CallerReference",
			&AWSError{Code: "InvalidInput", Message: "CallerReference is required", HTTPStatus: http.StatusBadRequest})
	}
	if awsErr := wireCheckMember("route53", "CreateHealthCheck", req, "HealthCheckConfig"); awsErr != nil {
		return nil, awsErr
	}
	if xmlReq.HealthCheckConfig.Type == "" {
		return nil, &AWSError{Code: "InvalidInput", Message: "HealthCheckConfig.Type is required", HTTPStatus: http.StatusBadRequest}
	}
	cfg := r53WithHealthCheckDefaults(xmlReq.HealthCheckConfig)

	p.mu.Lock()
	defer p.mu.Unlock()
	refKey := r53CallerRefKey(reqCtx.AccountID, xmlReq.CallerReference)
	refData, err := p.state.Get(context.Background(), route53Namespace, refKey)
	if err != nil {
		return nil, fmt.Errorf("route53 createHealthCheck get caller reference: %w", err)
	}
	if refData != nil {
		var ref r53CallerRef
		if err := r53DecodeJSON(refData, refKey, &ref); err != nil {
			return nil, err
		}
		alreadyExists := &AWSError{Code: "HealthCheckAlreadyExists",
			Message: "The health check you're attempting to create already exists.", HTTPStatus: http.StatusConflict}
		if ref.Deleted {
			return nil, alreadyExists
		}
		existing, err := p.loadHealthCheck(reqCtx, ref.ID)
		switch {
		case err == nil:
			original := existing.Config
			if ref.Config != nil {
				original = *ref.Config
			}
			if !reflect.DeepEqual(original, cfg) {
				return nil, alreadyExists
			}
			return r53HealthCheckResponse(http.StatusCreated, "CreateHealthCheckResponse", existing)
		case !r53IsCode(err, "NoSuchHealthCheck"):
			return nil, err
		}
		// A live reference whose health check is not there: "if the health check doesn't exist, Amazon
		// Route 53 creates the health check".
	}

	ids, err := p.loadList(reqCtx.AccountID, "healthcheck_ids")
	if err != nil {
		return nil, fmt.Errorf("route53 createHealthCheck: %w", err)
	}
	if len(ids) >= r53MaxHealthChecks {
		return nil, &AWSError{Code: "TooManyHealthChecks",
			Message:    "This health check can't be created because the current account has reached the limit on the number of active health checks.",
			HTTPStatus: http.StatusBadRequest}
	}
	hc := Route53HealthCheck{
		ID: reqCtx.IDs.UUID(), CallerReference: xmlReq.CallerReference, Config: cfg, Version: 1, AccountID: reqCtx.AccountID,
	}
	if err := p.putHealthCheck(hc); err != nil {
		return nil, fmt.Errorf("route53 createHealthCheck: %w", err)
	}
	if err := p.putJSON(refKey, r53CallerRef{ID: hc.ID, Config: &cfg}); err != nil {
		return nil, fmt.Errorf("route53 createHealthCheck: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID, "healthcheck_ids", hc.ID); err != nil {
		return nil, fmt.Errorf("route53 createHealthCheck: %w", err)
	}
	return r53HealthCheckResponse(http.StatusCreated, "CreateHealthCheckResponse", hc)
}

// r53HealthCheckResponse renders a response whose body is one HealthCheck element. CreateHealthCheck
// also answers the Location header its page publishes, "The unique URL representing the new health
// check".
func r53HealthCheckResponse(status int, root string, hc Route53HealthCheck) (*AWSResponse, error) {
	resp, err := r53XMLResponse(status, struct {
		XMLName     xml.Name
		XMLNS       string            `xml:"xmlns,attr"`
		HealthCheck r53XMLHealthCheck `xml:"HealthCheck"`
	}{XMLName: xml.Name{Local: root}, XMLNS: r53XMLNS, HealthCheck: r53HealthCheckXML(hc)})
	if err != nil {
		return nil, err
	}
	if root == "CreateHealthCheckResponse" {
		resp.Headers["Location"] = "https://route53.amazonaws.com/2013-04-01/healthcheck/" + hc.ID
	}
	return resp, nil
}

// getHealthCheck handles GetHealthCheck.
func (p *Route53Plugin) getHealthCheck(reqCtx *RequestContext, _ *AWSRequest, id string) (*AWSResponse, error) {
	hc, err := p.loadHealthCheck(reqCtx, id)
	if err != nil {
		return nil, err
	}
	return r53HealthCheckResponse(http.StatusOK, "GetHealthCheckResponse", hc)
}

// accountHealthChecks reads the caller's health checks in creation order, skipping an index entry
// that outlived its record.
func (p *Route53Plugin) accountHealthChecks(reqCtx *RequestContext) ([]Route53HealthCheck, error) {
	ids, err := p.loadList(reqCtx.AccountID, "healthcheck_ids")
	if err != nil {
		return nil, err
	}
	out := make([]Route53HealthCheck, 0, len(ids))
	for _, id := range ids {
		hc, err := p.loadHealthCheck(reqCtx, id)
		if err != nil {
			if r53IsCode(err, "NoSuchHealthCheck") {
				continue
			}
			return nil, err
		}
		out = append(out, hc)
	}
	return out, nil
}

// listHealthChecks handles ListHealthChecks. The page publishes Marker as "the ID of the first health
// check that Amazon Route 53 will return" and NextMarker as the one to send next, present when
// IsTruncated is true; the response echoes the request's Marker and MaxItems. A Marker that names no
// health check of the caller's is InvalidInput (400); that message is substrate's own.
func (p *Route53Plugin) listHealthChecks(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	size, awsErr := r53PageSize(req.Params["maxitems"])
	if awsErr != nil {
		return nil, awsErr
	}
	checks, err := p.accountHealthChecks(reqCtx)
	if err != nil {
		return nil, fmt.Errorf("route53 listHealthChecks: %w", err)
	}
	marker := req.Params["marker"]
	start := 0
	if marker != "" {
		start = slices.IndexFunc(checks, func(hc Route53HealthCheck) bool { return hc.ID == marker })
		if start < 0 {
			return nil, &AWSError{Code: "InvalidInput", Message: fmt.Sprintf("The marker %q names no health check.", marker),
				HTTPStatus: http.StatusBadRequest}
		}
	}
	type xmlResponse struct {
		XMLName      xml.Name            `xml:"ListHealthChecksResponse"`
		XMLNS        string              `xml:"xmlns,attr"`
		HealthChecks []r53XMLHealthCheck `xml:"HealthChecks>HealthCheck"`
		Marker       string              `xml:"Marker"`
		IsTruncated  bool                `xml:"IsTruncated"`
		NextMarker   string              `xml:"NextMarker,omitempty"`
		MaxItems     string              `xml:"MaxItems"`
	}
	resp := xmlResponse{XMLNS: r53XMLNS, Marker: marker, MaxItems: strconv.Itoa(size)}
	if raw := req.Params["maxitems"]; raw != "" {
		resp.MaxItems = raw
	}
	end := min(start+size, len(checks))
	for _, hc := range checks[start:end] {
		resp.HealthChecks = append(resp.HealthChecks, r53HealthCheckXML(hc))
	}
	if end < len(checks) {
		resp.IsTruncated = true
		resp.NextMarker = checks[end].ID
	}
	return r53XMLResponse(http.StatusOK, resp)
}

// updateHealthCheck handles UpdateHealthCheck.
func (p *Route53Plugin) updateHealthCheck(reqCtx *RequestContext, req *AWSRequest, id string) (*AWSResponse, error) {
	var xmlReq struct {
		XMLName                      xml.Name                `xml:"UpdateHealthCheckRequest"`
		HealthCheckVersion           *int64                  `xml:"HealthCheckVersion"`
		IPAddress                    *string                 `xml:"IPAddress"`
		Port                         *int64                  `xml:"Port"`
		ResourcePath                 *string                 `xml:"ResourcePath"`
		FullyQualifiedDomainName     *string                 `xml:"FullyQualifiedDomainName"`
		SearchString                 *string                 `xml:"SearchString"`
		FailureThreshold             *int64                  `xml:"FailureThreshold"`
		Inverted                     *bool                   `xml:"Inverted"`
		Disabled                     *bool                   `xml:"Disabled"`
		HealthThreshold              *int64                  `xml:"HealthThreshold"`
		ChildHealthChecks            *[]string               `xml:"ChildHealthChecks>ChildHealthCheck"`
		EnableSNI                    *bool                   `xml:"EnableSNI"`
		Regions                      *[]string               `xml:"Regions>Region"`
		AlarmIdentifier              *Route53AlarmIdentifier `xml:"AlarmIdentifier"`
		InsufficientDataHealthStatus *string                 `xml:"InsufficientDataHealthStatus"`
		ResetElements                []string                `xml:"ResetElements>ResettableElementName"`
		HealthCheckSourceID          *string                 `xml:"HealthCheckSourceId"`
	}
	if len(bytes.TrimSpace(req.Body)) > 0 {
		if awsErr := r53DecodeBody(req, &xmlReq); awsErr != nil {
			return nil, awsErr
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	hc, err := p.loadHealthCheck(reqCtx, id)
	if err != nil {
		return nil, err
	}
	if xmlReq.HealthCheckVersion != nil && *xmlReq.HealthCheckVersion != hc.Version {
		return nil, &AWSError{Code: "HealthCheckVersionMismatch",
			Message:    "The value of HealthCheckVersion in the request doesn't match the value of HealthCheckVersion in the health check.",
			HTTPStatus: http.StatusConflict}
	}
	cfg := &hc.Config
	setString := func(dst *string, src *string) {
		if src != nil {
			*dst = *src
		}
	}
	setString(&cfg.IPAddress, xmlReq.IPAddress)
	setString(&cfg.ResourcePath, xmlReq.ResourcePath)
	setString(&cfg.FullyQualifiedDomainName, xmlReq.FullyQualifiedDomainName)
	setString(&cfg.SearchString, xmlReq.SearchString)
	setString(&cfg.InsufficientDataHealthStatus, xmlReq.InsufficientDataHealthStatus)
	setString(&cfg.HealthCheckSourceID, xmlReq.HealthCheckSourceID)
	for _, ptr := range []struct{ dst, src **int64 }{
		{&cfg.Port, &xmlReq.Port}, {&cfg.FailureThreshold, &xmlReq.FailureThreshold}, {&cfg.HealthThreshold, &xmlReq.HealthThreshold},
	} {
		if *ptr.src != nil {
			*ptr.dst = *ptr.src
		}
	}
	for _, ptr := range []struct{ dst, src **bool }{
		{&cfg.Inverted, &xmlReq.Inverted}, {&cfg.Disabled, &xmlReq.Disabled}, {&cfg.EnableSNI, &xmlReq.EnableSNI},
	} {
		if *ptr.src != nil {
			*ptr.dst = *ptr.src
		}
	}
	if xmlReq.ChildHealthChecks != nil {
		cfg.ChildHealthChecks = *xmlReq.ChildHealthChecks
	}
	if xmlReq.Regions != nil {
		cfg.Regions = *xmlReq.Regions
	}
	if xmlReq.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = xmlReq.AlarmIdentifier
	}
	for _, el := range xmlReq.ResetElements {
		switch el {
		case "FullyQualifiedDomainName":
			cfg.FullyQualifiedDomainName = ""
		case "Regions":
			cfg.Regions = nil
		case "ResourcePath":
			cfg.ResourcePath = ""
		case "ChildHealthChecks":
			cfg.ChildHealthChecks = nil
		}
	}
	hc.Version++
	if err := p.putHealthCheck(hc); err != nil {
		return nil, fmt.Errorf("route53 updateHealthCheck: %w", err)
	}
	return r53HealthCheckResponse(http.StatusOK, "UpdateHealthCheckResponse", hc)
}

// deleteHealthCheck handles DeleteHealthCheck. Its caller reference is kept, marked deleted, so a
// create that reuses it is HealthCheckAlreadyExists.
func (p *Route53Plugin) deleteHealthCheck(reqCtx *RequestContext, _ *AWSRequest, id string) (*AWSResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hc, err := p.loadHealthCheck(reqCtx, id)
	if err != nil {
		return nil, err
	}
	if err := p.putJSON(r53CallerRefKey(reqCtx.AccountID, hc.CallerReference), r53CallerRef{ID: hc.ID, Deleted: true}); err != nil {
		return nil, fmt.Errorf("route53 deleteHealthCheck: %w", err)
	}
	if err := p.state.Delete(context.Background(), route53Namespace, r53HealthCheckKey(id)); err != nil {
		return nil, fmt.Errorf("route53 deleteHealthCheck: %w", err)
	}
	if err := p.removeFromList(reqCtx.AccountID, "healthcheck_ids", id); err != nil {
		return nil, fmt.Errorf("route53 deleteHealthCheck: %w", err)
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteHealthCheckResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}{XMLNS: r53XMLNS})
}

// getHealthCheckCount handles GetHealthCheckCount: the caller's health checks.
func (p *Route53Plugin) getHealthCheckCount(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	checks, err := p.accountHealthChecks(reqCtx)
	if err != nil {
		return nil, fmt.Errorf("route53 getHealthCheckCount: %w", err)
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName          xml.Name `xml:"GetHealthCheckCountResponse"`
		XMLNS            string   `xml:"xmlns,attr"`
		HealthCheckCount int64    `xml:"HealthCheckCount"`
	}{XMLNS: r53XMLNS, HealthCheckCount: int64(len(checks))})
}
