package emulator

import (
	"context"
	"encoding/xml"
	"net/http"
)

// A bucket's static-website configuration: PutBucketWebsite, GetBucketWebsite and
// DeleteBucketWebsite (#1613, #1658).
//
// Substrate records the configuration and answers it back. It never serves the bucket's website
// endpoint, follows a routing rule or renders an index or error document: those are what a
// browser observes of the endpoint, not what an API call observes of the bucket (CLAUDE.md,
// "Scope").
//
// # Absent is not empty
//
// A bucket with no configuration answers NoSuchWebsiteConfiguration/404, "The specified bucket does
// not have a website configuration". S3's error-code list (API_Error) does not include the code; the
// code and the message are what the AWS CLI prints for get-bucket-website on such a bucket, and
// what the AWS provider for Terraform tolerates on every aws_s3_bucket refresh.
//
// # What is refused
//
//   - MalformedXML/400 for a body that does not decode, a RedirectAllRequestsTo without HostName, an
//     ErrorDocument without Key, a RoutingRule without Redirect, and a Protocol other than http or
//     https: each is Required or an enum in API_RedirectAllRequestsTo, API_ErrorDocument,
//     API_RoutingRule and API_Redirect. A RedirectAllRequestsTo beside any other member is
//     MalformedXML too: PutBucketWebsite's page says "If you specify this property, you can't
//     specify any other property" without naming the code, so the code is substrate's choice.
//   - InvalidArgument/400, "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo
//     is empty", with ArgumentName IndexDocument and ArgumentValue null, for a configuration with
//     neither an index document nor a redirect. That is S3's own answer to an empty
//     WebsiteConfiguration, as reported by versity/versitygw#2260.
//
// Two published limits are not enforced, because no page names the code a breach answers: at most
// 50 routing rules, and an IndexDocument Suffix that must not contain a slash.

// s3WebsiteKeyPrefix is the state-key prefix of a bucket's website configuration. It is listed in
// [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3WebsiteKeyPrefix = "bucket_website:"

// s3WebsiteConfiguration is PutBucketWebsite's request body and GetBucketWebsite's response body,
// with its members in the response syntax's order.
type s3WebsiteConfiguration struct {
	XMLName               xml.Name                `xml:"WebsiteConfiguration" json:"-"`
	Xmlns                 string                  `xml:"xmlns,attr,omitempty" json:"-"`
	RedirectAllRequestsTo *s3WebsiteRedirectAll   `xml:"RedirectAllRequestsTo,omitempty" json:"RedirectAllRequestsTo,omitempty"`
	IndexDocument         *s3WebsiteIndexDocument `xml:"IndexDocument,omitempty" json:"IndexDocument,omitempty"`
	ErrorDocument         *s3WebsiteErrorDocument `xml:"ErrorDocument,omitempty" json:"ErrorDocument,omitempty"`
	RoutingRules          *s3WebsiteRoutingRules  `xml:"RoutingRules,omitempty" json:"RoutingRules,omitempty"`
}

// s3WebsiteRedirectAll is RedirectAllRequestsTo (API_RedirectAllRequestsTo).
type s3WebsiteRedirectAll struct {
	HostName string `xml:"HostName" json:"HostName"`
	Protocol string `xml:"Protocol,omitempty" json:"Protocol,omitempty"`
}

// s3WebsiteIndexDocument is IndexDocument (API_IndexDocument).
type s3WebsiteIndexDocument struct {
	Suffix string `xml:"Suffix" json:"Suffix"`
}

// s3WebsiteErrorDocument is ErrorDocument (API_ErrorDocument).
type s3WebsiteErrorDocument struct {
	Key string `xml:"Key" json:"Key"`
}

// s3WebsiteRoutingRules is the RoutingRules container. It is a pointer member of
// [s3WebsiteConfiguration] rather than a RoutingRules>RoutingRule path, because encoding/xml writes
// an empty parent element for an empty path.
type s3WebsiteRoutingRules struct {
	Rules []s3WebsiteRoutingRule `xml:"RoutingRule" json:"Rules"`
}

// s3WebsiteRoutingRule is one RoutingRule (API_RoutingRule).
type s3WebsiteRoutingRule struct {
	Condition *s3WebsiteCondition `xml:"Condition,omitempty" json:"Condition,omitempty"`
	Redirect  *s3WebsiteRedirect  `xml:"Redirect" json:"Redirect"`
}

// s3WebsiteCondition is a routing rule's Condition (API_Condition).
type s3WebsiteCondition struct {
	HTTPErrorCodeReturnedEquals string `xml:"HttpErrorCodeReturnedEquals,omitempty" json:"HttpErrorCodeReturnedEquals,omitempty"`
	KeyPrefixEquals             string `xml:"KeyPrefixEquals,omitempty" json:"KeyPrefixEquals,omitempty"`
}

// s3WebsiteRedirect is a routing rule's Redirect (API_Redirect).
type s3WebsiteRedirect struct {
	HostName             string `xml:"HostName,omitempty" json:"HostName,omitempty"`
	HTTPRedirectCode     string `xml:"HttpRedirectCode,omitempty" json:"HttpRedirectCode,omitempty"`
	Protocol             string `xml:"Protocol,omitempty" json:"Protocol,omitempty"`
	ReplaceKeyPrefixWith string `xml:"ReplaceKeyPrefixWith,omitempty" json:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith       string `xml:"ReplaceKeyWith,omitempty" json:"ReplaceKeyWith,omitempty"`
}

// routingRules returns cfg's routing rules, none when the container is absent.
func (cfg s3WebsiteConfiguration) routingRules() []s3WebsiteRoutingRule {
	if cfg.RoutingRules == nil {
		return nil
	}
	return cfg.RoutingRules.Rules
}

// s3WebsiteProtocolValid reports whether v is empty or in the published Protocol enum.
func s3WebsiteProtocolValid(v string) bool {
	return v == "" || v == "http" || v == "https"
}

// s3WebsiteRefusal returns the refusal cfg earns, or nil for a configuration PutBucketWebsite
// accepts. See the file comment for each rule and its source.
func s3WebsiteRefusal(cfg s3WebsiteConfiguration) *AWSResponse {
	if r := cfg.RedirectAllRequestsTo; r != nil {
		if r.HostName == "" || !s3WebsiteProtocolValid(r.Protocol) ||
			cfg.IndexDocument != nil || cfg.ErrorDocument != nil || cfg.RoutingRules != nil {
			return s3MalformedXMLResponse()
		}
		return nil
	}
	if cfg.ErrorDocument != nil && cfg.ErrorDocument.Key == "" {
		return s3MalformedXMLResponse()
	}
	for _, rule := range cfg.routingRules() {
		if rule.Redirect == nil || !s3WebsiteProtocolValid(rule.Redirect.Protocol) {
			return s3MalformedXMLResponse()
		}
	}
	if cfg.IndexDocument == nil || cfg.IndexDocument.Suffix == "" {
		return s3ErrorResponseWith(s3Error{
			Code:    "InvalidArgument",
			Message: "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty",
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{
				{Name: "ArgumentName", Value: "IndexDocument"},
				{Name: "ArgumentValue", Value: "null"},
			},
		})
	}
	return nil
}

// putBucketWebsite handles PUT /<bucket>?website. An existing configuration is replaced.
func (p *S3Plugin) putBucketWebsite(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3WebsiteConfiguration
	if !s3DecodeConfigXML(req, &cfg) {
		return s3MalformedXMLResponse(), nil
	}
	if refusal := s3WebsiteRefusal(cfg); refusal != nil {
		return refusal, nil
	}
	if err := p.s3StoreBucketConfig(ctx, s3WebsiteKeyPrefix, bucket, cfg); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// getBucketWebsite handles GET /<bucket>?website.
func (p *S3Plugin) getBucketWebsite(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3WebsiteConfiguration
	found, err := p.s3LoadBucketConfig(ctx, s3WebsiteKeyPrefix, bucket, &cfg)
	if err != nil {
		return nil, err
	}
	if !found {
		return s3ErrorResponse("NoSuchWebsiteConfiguration",
			"The specified bucket does not have a website configuration", http.StatusNotFound), nil
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}

// deleteBucketWebsite handles DELETE /<bucket>?website.
func (p *S3Plugin) deleteBucketWebsite(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	return p.s3DeleteBucketConfig(context.Background(), s3WebsiteKeyPrefix, bucket)
}
