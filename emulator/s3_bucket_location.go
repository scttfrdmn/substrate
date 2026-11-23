package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// s3LocationConstraint is GetBucketLocation's response body.
//
// API_GetBucketLocation: "Specifies the Region where the bucket resides … Buckets in Region
// us-east-1 have a LocationConstraint of null", which on the wire is an empty element. A bucket's
// account and creation time are not members, so the record's bookkeeping does not reach it.
type s3LocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Region  string   `xml:",chardata"`
}

// getBucketLocation handles GET /<bucket>?location (#1606).
//
// It was refused as NotImplemented (#1349) until `cdk deploy` needed it: the CDK's asset publisher
// asks for the staging bucket's Region before it uploads a stack's template, and a refusal fails
// the deploy before CloudFormation is called.
func (p *S3Plugin) getBucketLocation(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	data, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("get bucket location: %w", err)
	}
	if data == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}
	var rec S3Bucket
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("get bucket location: unmarshal bucket: %w", err)
	}
	region := rec.Region
	if region == "us-east-1" {
		region = ""
	}
	body, err := xml.Marshal(s3LocationConstraint{Xmlns: s3XMLNamespace, Region: region})
	if err != nil {
		return nil, fmt.Errorf("get bucket location: marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/xml"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
