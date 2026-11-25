package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// Shared plumbing for the singleton bucket configurations that substrate records and answers back
// without acting on: website, logging, transfer acceleration, request payment and replication
// (#1613, #1658). Each is stored as JSON under its own key prefix, keyed by bucket name alone, and
// each prefix is listed in [s3BucketSubresourcePrefixes] so DeleteBucket clears it.
//
// What these configurations would do is out of scope (CLAUDE.md, "Scope"): substrate never serves
// a website endpoint, writes an access log, accelerates a transfer, bills a requester or replicates
// an object. A Get answers what the last Put recorded, which is every observation the API makes.

// s3MalformedXMLResponse is MalformedXML/400 with the message S3's error-code list publishes for it
// (API_Error: "The XML you provided was not well-formed or did not validate against our published
// schema.").
func s3MalformedXMLResponse() *AWSResponse {
	return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest)
}

// s3DecodeConfigXML decodes req's body, aws-chunked framing removed, into v. It reports false for a
// body that is empty or is not well-formed XML, which every caller answers as MalformedXML.
func s3DecodeConfigXML(req *AWSRequest, v any) bool {
	body := decodeAWSChunked(req.Headers, req.Body)
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	return xml.NewDecoder(bytes.NewReader(body)).Decode(v) == nil
}

// s3LoadBucketConfig reads the configuration stored at prefix+bucket into v, reporting false when
// none is stored.
func (p *S3Plugin) s3LoadBucketConfig(ctx context.Context, prefix, bucket string, v any) (bool, error) {
	data, err := p.state.Get(ctx, s3Namespace, s3BucketSubresourceKey(prefix, bucket))
	if err != nil {
		return false, fmt.Errorf("get %s%s: %w", prefix, bucket, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("decode %s%s: %w", prefix, bucket, err)
	}
	return true, nil
}

// s3StoreBucketConfig writes v as the configuration at prefix+bucket.
func (p *S3Plugin) s3StoreBucketConfig(ctx context.Context, prefix, bucket string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s%s: %w", prefix, bucket, err)
	}
	if err := p.state.Put(ctx, s3Namespace, s3BucketSubresourceKey(prefix, bucket), data); err != nil {
		return fmt.Errorf("save %s%s: %w", prefix, bucket, err)
	}
	return nil
}

// s3DeleteBucketConfig removes the configuration at prefix+bucket and answers 204, the status each
// of these Delete operations' Response Syntax shows. Removing a configuration the bucket does not
// have still answers 204: DeleteBucketWebsite's page says so ("You will get a 200 OK response if the
// website configuration you are trying to delete does not exist"), and no page documents an error.
func (p *S3Plugin) s3DeleteBucketConfig(ctx context.Context, prefix, bucket string) (*AWSResponse, error) {
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	if err := p.state.Delete(ctx, s3Namespace, s3BucketSubresourceKey(prefix, bucket)); err != nil {
		return nil, fmt.Errorf("delete %s%s: %w", prefix, bucket, err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// s3EmptyOK is the 200 with an empty body each of these Put operations answers ("If the action is
// successful, the service sends back an HTTP 200 response with an empty HTTP body").
func s3EmptyOK() *AWSResponse {
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}
}
