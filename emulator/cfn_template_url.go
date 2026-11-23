package emulator

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// cfn_template_url.go resolves a request's TemplateURL from substrate's own S3 (#1606).
//
// CreateStack, UpdateStack, CreateChangeSet and ValidateTemplate each take the template either
// inline, as TemplateBody, or by reference, as TemplateURL: "The URL must point to a template (max
// size: 1 MB) that's located in an Amazon S3 bucket or a Systems Manager document … The location
// for an Amazon S3 bucket must start with https://. URLs from S3 static websites are not
// supported." (API_CreateChangeSet, TemplateURL). Until #1606 substrate refused TemplateURL
// outright, which blocked every `cdk deploy`: the CDK uploads each stack's template as a file asset
// to the bootstrap bucket and deploys it by URL.
//
// The fetch is not a network read. Substrate holds the only S3 a caller of substrate can have put
// the template in, so a URL naming an S3 bucket is answered from substrate's S3 plugin, through
// [StackDeployer.dispatch] — routed, authorized as the caller and recorded like the resource calls
// a deployment makes. A Systems Manager document URL is not modeled and is refused like any other
// URL that does not name an S3 object.

// cfnTemplateURLMaxBytes is TemplateURL's documented ceiling: "max size: 1 MB".
const cfnTemplateURLMaxBytes = 1 << 20

// cfnTemplateURLUnsupportedMessage is the ValidationError CloudFormation answers for a TemplateURL
// it does not fetch from. The text is observed rather than published: an AWS re:Post question
// ("TemplateURL must be a supported URL..") and aws-cloudformation/cloudformation-coverage-roadmap
// issue #1212 both quote it from a stack operation given a non-S3 URL. An older wording, "TemplateURL
// must be an Amazon S3 URL.", is quoted by aws/aws-cli issues #3200 and #3913; the newer one is used.
const cfnTemplateURLUnsupportedMessage = "TemplateURL must be a supported URL."

// cfnS3TemplateLocation is the bucket and key a TemplateURL names.
type cfnS3TemplateLocation struct {
	bucket string
	key    string
}

// cfnParseTemplateURL reads the S3 object a TemplateURL names, or reports false for a URL that
// names none.
//
// The forms accepted are the ones Amazon S3 publishes for addressing an object over HTTPS, all of
// which a CloudFormation template URL is written in (the AWS::CloudFormation::Stack page's own
// example is the first):
//
//   - path-style on the global endpoint, https://s3.amazonaws.com/bucket/key;
//   - path-style on a regional endpoint, https://s3.region.amazonaws.com/bucket/key, and the legacy
//     dash form https://s3-region.amazonaws.com/bucket/key;
//   - virtual-hosted, https://bucket.s3.amazonaws.com/key, https://bucket.s3.region.amazonaws.com/key
//     and https://bucket.s3-region.amazonaws.com/key.
//
// Each must be https, as the parameter's page requires. s3:// is not accepted: the page says the
// location "must start with https://", and CloudFormation's own API takes no other S3 form.
//
// ownHost is the Host the CloudFormation request itself was sent to. A URL on that host is
// substrate's own endpoint and is read path-style, over http or https. That is the URL the CDK
// actually sends when it is pointed at an emulator: it builds the template's URL from the S3
// client's resolved endpoint (`${endpoint.url.origin}/${bucket}/${key}`, toolkit-lib
// restUrlFromManifest), and with AWS_ENDPOINT_URL set that endpoint is substrate's.
func cfnParseTemplateURL(raw, ownHost string) (cfnS3TemplateLocation, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return cfnS3TemplateLocation{}, false
	}
	host := strings.ToLower(u.Hostname())
	path := strings.TrimPrefix(u.Path, "/")

	if ownHost != "" && strings.EqualFold(u.Host, ownHost) && (u.Scheme == "http" || u.Scheme == "https") {
		return cfnPathStyleLocation(path)
	}
	if u.Scheme != "https" || !strings.HasSuffix(host, ".amazonaws.com") {
		return cfnS3TemplateLocation{}, false
	}
	endpoint := strings.TrimSuffix(host, ".amazonaws.com")
	if cfnS3EndpointLabel(endpoint) {
		return cfnPathStyleLocation(path)
	}
	// Virtual-hosted: the bucket is everything before the S3 endpoint label, and may itself
	// contain dots, so the label is found from the right.
	for _, sep := range []string{".s3.", ".s3-"} {
		if i := strings.LastIndex(endpoint+".", sep); i > 0 && cfnS3EndpointLabel(endpoint[i+1:]) {
			if path == "" {
				return cfnS3TemplateLocation{}, false
			}
			return cfnS3TemplateLocation{bucket: endpoint[:i], key: path}, true
		}
	}
	return cfnS3TemplateLocation{}, false
}

// cfnS3EndpointLabel reports whether label — a host with ".amazonaws.com" removed — is an S3
// endpoint: "s3", "s3.region" or "s3-region".
func cfnS3EndpointLabel(label string) bool {
	if label == "s3" {
		return true
	}
	for _, prefix := range []string{"s3.", "s3-"} {
		if region, ok := strings.CutPrefix(label, prefix); ok {
			return region != "" && !strings.Contains(region, ".")
		}
	}
	return false
}

// cfnPathStyleLocation splits a path-style object path into its bucket and key.
func cfnPathStyleLocation(path string) (cfnS3TemplateLocation, bool) {
	bucket, key, ok := strings.Cut(path, "/")
	if !ok || bucket == "" || key == "" {
		return cfnS3TemplateLocation{}, false
	}
	return cfnS3TemplateLocation{bucket: bucket, key: key}, true
}

// cfnTemplateFromRequest returns the template a stack request carries: its TemplateBody, or the
// object its TemplateURL names, read from substrate's S3 as the requesting caller. It returns ""
// with no error when the request carries neither, so each operation keeps its own rule for that
// case — UpdateStack and CreateChangeSet fall back to the stack's template, CreateStack requires
// one. ValidateTemplate is expected to call it the same way.
//
// Both together are refused: each operation's page says "You must specify only one of" them. The
// message is substrate's own; the page publishes the rule, not the text.
func (p *CloudFormationPlugin) cfnTemplateFromRequest(reqCtx *RequestContext, req *AWSRequest) (string, *AWSError) {
	body, templateURL := req.Params["TemplateBody"], req.Params["TemplateURL"]
	switch {
	case body != "" && templateURL != "":
		return "", &AWSError{
			Code:       "ValidationError",
			Message:    "Specify only one of TemplateBody and TemplateURL",
			HTTPStatus: http.StatusBadRequest,
		}
	case templateURL == "":
		return body, nil
	}
	return p.fetchTemplateURL(reqCtx, templateURL, req.Headers["Host"])
}

// fetchTemplateURL reads the template a TemplateURL names.
//
// The read is a GetObject through [StackDeployer.dispatch] with the requesting caller as the
// principal: CloudFormation reads a template URL with the credentials of whoever named it, so a
// caller who may not read the object cannot deploy it either. It is recorded, as every request a
// stack operation makes on the caller's behalf is.
//
// A refusal is a ValidationError, as CloudFormation answers one. Its text is CloudFormation's
// observed "S3 error: " prefix — aws-quickstart/quickstart-boomi-atom issue #24 quotes "S3 error:
// The specified key does not exist." from a stack create given a URL to a missing object —
// followed by the message S3 itself answered, so a missing bucket, a missing key and a denial each
// read as what they are.
func (p *CloudFormationPlugin) fetchTemplateURL(reqCtx *RequestContext, templateURL, ownHost string) (string, *AWSError) {
	loc, ok := cfnParseTemplateURL(templateURL, ownHost)
	if !ok {
		return "", &AWSError{Code: "ValidationError", Message: cfnTemplateURLUnsupportedMessage,
			HTTPStatus: http.StatusBadRequest}
	}
	d := p.deployerFor(reqCtx, cfnAttribution{creator: reqCtx.Principal})
	resp, _, err := d.dispatch(context.Background(), &AWSRequest{
		Service:   "s3",
		Operation: "GET",
		Path:      "/" + loc.bucket + "/" + loc.key,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}, reqCtx.RequestID)
	if err != nil {
		msg := err.Error()
		if resp != nil {
			if m := extractXMLField(resp.Body, "Message"); m != "" {
				msg = m
			}
		} else if awsErr := (*AWSError)(nil); errors.As(err, &awsErr) && awsErr.Message != "" {
			msg = awsErr.Message
		}
		return "", &AWSError{Code: "ValidationError", Message: "S3 error: " + msg,
			HTTPStatus: http.StatusBadRequest}
	}
	if resp == nil || len(resp.Body) == 0 {
		return "", &AWSError{Code: "ValidationError", Message: "S3 error: the template object is empty",
			HTTPStatus: http.StatusBadRequest}
	}
	if len(resp.Body) > cfnTemplateURLMaxBytes {
		// The limit is the parameter's page's; the message is substrate's own.
		return "", &AWSError{Code: "ValidationError",
			Message:    "The template at TemplateURL exceeds the 1 MB maximum size",
			HTTPStatus: http.StatusBadRequest}
	}
	return string(resp.Body), nil
}
