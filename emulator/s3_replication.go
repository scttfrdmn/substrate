package emulator

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"
)

// A bucket's replication configuration: PutBucketReplication, GetBucketReplication and
// DeleteBucketReplication (#1613, #1658).
//
// Substrate records the configuration and answers it back, every member included. It never copies
// an object to a destination bucket, assumes the Role, or reports a replication status on an
// object: replication is asynchronous work S3 does after the request returns, which is outside
// what an API call observes of the bucket (CLAUDE.md, "Scope").
//
// # Absent is not empty
//
// A bucket with no configuration answers ReplicationConfigurationNotFoundError/404, "The
// replication configuration was not found". API_Error does not list the code; it and the message
// are what the AWS CLI prints for get-bucket-replication on such a bucket and what the AWS provider
// for Terraform reports (hashicorp/terraform-provider-aws#30646).
//
// # What is refused, in the order it is checked
//
//  1. MalformedXML/400 for a body that does not decode, a missing Role, no Rule, a rule without
//     Status or Destination.Bucket, a Status outside its enum, a rule with both or neither of
//     Prefix and Filter, and a Filter naming more than one of Prefix, Tag and And. Each is Required
//     or an enum in API_PutBucketReplication, API_ReplicationRule and API_Destination, or "A Filter
//     must specify exactly one Prefix, Tag, or an And child element" (API_ReplicationRule).
//  2. InvalidRequest/400, "Versioning must be 'Enabled' on the bucket to apply a replication
//     configuration", when the source bucket's versioning is not Enabled. The text is S3's answer
//     as the AWS CLI prints it (crossplane-contrib/provider-aws#553 quotes it).
//  3. InvalidRequest/400, "Destination bucket must exist." and "Destination bucket must have
//     versioning enabled.", for a destination that is missing or not versioned. Both messages are
//     S3's observed answers (ministackorg/ministack#2311); that issue gives the second one's code,
//     and the first is given the same.
//  4. InvalidRequest/400 for the schema-version rules: a rule with a Filter must carry Priority and
//     DeleteMarkerReplication, and a rule with the deprecated rule-level Prefix must carry neither.
//     Each message is "{Member} must be specified for" or "{Member} cannot be used for this version
//     of Cross Region Replication configuration schema. Please refer to S3 Developer Guide for more
//     information." Those four texts are S3's in full, as ministackorg/ministack#2311 reproduced
//     them; hashicorp/terraform-provider-aws#19323 quotes the two DeleteMarkerReplication ones up to
//     "schema".
//
// Not enforced: the rule-count ceiling of 1,000; the KMS, Replication Time Control and Metrics
// cross-checks ministack#2311 lists; a rule mixing the two schemas; and the rule that a Filter with
// a Tag needs DeleteMarkerReplication Disabled (API_ReplicationRule), whose code no source
// publishes.
//
// While a configuration is recorded, PutBucketVersioning refuses to suspend the bucket's
// versioning; see [S3Plugin.s3VersioningSuspendRefusal].

// s3ReplicationKeyPrefix is the state-key prefix of a bucket's replication configuration. It is
// listed in [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3ReplicationKeyPrefix = "bucket_replication:"

// s3ReplicationConfiguration is PutBucketReplication's request body and GetBucketReplication's
// response body.
type s3ReplicationConfiguration struct {
	XMLName xml.Name            `xml:"ReplicationConfiguration" json:"-"`
	Xmlns   string              `xml:"xmlns,attr,omitempty" json:"-"`
	Role    string              `xml:"Role" json:"Role"`
	Rules   []s3ReplicationRule `xml:"Rule" json:"Rules"`
}

// s3ReplicationRule is one ReplicationRule (API_ReplicationRule).
type s3ReplicationRule struct {
	ID                        string                       `xml:"ID,omitempty" json:"ID,omitempty"`
	Priority                  *int                         `xml:"Priority,omitempty" json:"Priority,omitempty"`
	Prefix                    *string                      `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
	Filter                    *s3ReplicationFilter         `xml:"Filter,omitempty" json:"Filter,omitempty"`
	Status                    string                       `xml:"Status" json:"Status"`
	SourceSelectionCriteria   *s3ReplicationSourceCriteria `xml:"SourceSelectionCriteria,omitempty" json:"SourceSelectionCriteria,omitempty"`
	ExistingObjectReplication *s3ReplicationStatus         `xml:"ExistingObjectReplication,omitempty" json:"ExistingObjectReplication,omitempty"`
	Destination               *s3ReplicationDestination    `xml:"Destination" json:"Destination"`
	DeleteMarkerReplication   *s3ReplicationStatus         `xml:"DeleteMarkerReplication,omitempty" json:"DeleteMarkerReplication,omitempty"`
}

// s3ReplicationFilter is ReplicationRuleFilter: exactly one of Prefix, Tag or And.
type s3ReplicationFilter struct {
	Prefix *string             `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
	Tag    *s3ReplicationTag   `xml:"Tag,omitempty" json:"Tag,omitempty"`
	And    *s3ReplicationAndOp `xml:"And,omitempty" json:"And,omitempty"`
}

// s3ReplicationAndOp is ReplicationRuleAndOperator.
type s3ReplicationAndOp struct {
	Prefix *string            `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
	Tags   []s3ReplicationTag `xml:"Tag,omitempty" json:"Tags,omitempty"`
}

// s3ReplicationTag is one Tag in a filter.
type s3ReplicationTag struct {
	Key   string `xml:"Key" json:"Key"`
	Value string `xml:"Value" json:"Value"`
}

// s3ReplicationStatus is the {Status} container DeleteMarkerReplication, ExistingObjectReplication,
// SseKmsEncryptedObjects and ReplicaModifications share.
type s3ReplicationStatus struct {
	Status string `xml:"Status" json:"Status"`
}

// s3ReplicationSourceCriteria is SourceSelectionCriteria.
type s3ReplicationSourceCriteria struct {
	ReplicaModifications   *s3ReplicationStatus `xml:"ReplicaModifications,omitempty" json:"ReplicaModifications,omitempty"`
	SseKmsEncryptedObjects *s3ReplicationStatus `xml:"SseKmsEncryptedObjects,omitempty" json:"SseKmsEncryptedObjects,omitempty"`
}

// s3ReplicationDestination is Destination (API_Destination).
type s3ReplicationDestination struct {
	Bucket                   string                     `xml:"Bucket" json:"Bucket"`
	Account                  string                     `xml:"Account,omitempty" json:"Account,omitempty"`
	StorageClass             string                     `xml:"StorageClass,omitempty" json:"StorageClass,omitempty"`
	AccessControlTranslation *s3ReplicationACLTranslate `xml:"AccessControlTranslation,omitempty" json:"AccessControlTranslation,omitempty"`
	EncryptionConfiguration  *s3ReplicationEncryption   `xml:"EncryptionConfiguration,omitempty" json:"EncryptionConfiguration,omitempty"`
	ReplicationTime          *s3ReplicationTime         `xml:"ReplicationTime,omitempty" json:"ReplicationTime,omitempty"`
	Metrics                  *s3ReplicationMetrics      `xml:"Metrics,omitempty" json:"Metrics,omitempty"`
}

// s3ReplicationACLTranslate is AccessControlTranslation.
type s3ReplicationACLTranslate struct {
	Owner string `xml:"Owner" json:"Owner"`
}

// s3ReplicationEncryption is a destination's EncryptionConfiguration.
type s3ReplicationEncryption struct {
	ReplicaKmsKeyID string `xml:"ReplicaKmsKeyID,omitempty" json:"ReplicaKmsKeyID,omitempty"`
}

// s3ReplicationMinutes is the {Minutes} container ReplicationTime.Time and Metrics.EventThreshold
// share.
type s3ReplicationMinutes struct {
	Minutes *int `xml:"Minutes,omitempty" json:"Minutes,omitempty"`
}

// s3ReplicationTime is ReplicationTime (S3 Replication Time Control).
type s3ReplicationTime struct {
	Status string                `xml:"Status" json:"Status"`
	Time   *s3ReplicationMinutes `xml:"Time,omitempty" json:"Time,omitempty"`
}

// s3ReplicationMetrics is a destination's Metrics.
type s3ReplicationMetrics struct {
	Status         string                `xml:"Status" json:"Status"`
	EventThreshold *s3ReplicationMinutes `xml:"EventThreshold,omitempty" json:"EventThreshold,omitempty"`
}

// s3ReplicationStatusValid reports whether s is present and in the Enabled/Disabled enum every
// replication Status member uses.
func s3ReplicationStatusValid(s string) bool { return s == "Enabled" || s == "Disabled" }

// s3OptionalStatusValid reports whether an optional {Status} container is absent or carries a valid
// Status.
func s3OptionalStatusValid(c *s3ReplicationStatus) bool {
	return c == nil || s3ReplicationStatusValid(c.Status)
}

// s3ReplicationSchemaValid reports whether cfg passes check 1 of the file comment.
func s3ReplicationSchemaValid(cfg s3ReplicationConfiguration) bool {
	if cfg.Role == "" || len(cfg.Rules) == 0 {
		return false
	}
	for _, r := range cfg.Rules {
		if !s3ReplicationStatusValid(r.Status) || r.Destination == nil || r.Destination.Bucket == "" {
			return false
		}
		if (r.Prefix == nil) == (r.Filter == nil) {
			return false
		}
		if f := r.Filter; f != nil {
			n := 0
			for _, set := range []bool{f.Prefix != nil, f.Tag != nil, f.And != nil} {
				if set {
					n++
				}
			}
			if n > 1 {
				return false
			}
		}
		if !s3OptionalStatusValid(r.DeleteMarkerReplication) || !s3OptionalStatusValid(r.ExistingObjectReplication) {
			return false
		}
		if sc := r.SourceSelectionCriteria; sc != nil &&
			(!s3OptionalStatusValid(sc.ReplicaModifications) || !s3OptionalStatusValid(sc.SseKmsEncryptedObjects)) {
			return false
		}
		d := r.Destination
		if (d.ReplicationTime != nil && !s3ReplicationStatusValid(d.ReplicationTime.Status)) ||
			(d.Metrics != nil && !s3ReplicationStatusValid(d.Metrics.Status)) {
			return false
		}
	}
	return true
}

// s3ReplicationSchemaVersionRefusal returns check 4's refusal for cfg, or nil.
func s3ReplicationSchemaVersionRefusal(cfg s3ReplicationConfiguration) *AWSResponse {
	refuse := func(member, verb string) *AWSResponse {
		return s3ErrorResponse("InvalidRequest", member+" "+verb+" for this version of Cross Region "+
			"Replication configuration schema. Please refer to S3 Developer Guide for more information.",
			http.StatusBadRequest)
	}
	for _, r := range cfg.Rules {
		if r.Filter != nil {
			if r.Priority == nil {
				return refuse("Priority", "must be specified")
			}
			if r.DeleteMarkerReplication == nil {
				return refuse("DeleteMarkerReplication", "must be specified")
			}
			continue
		}
		if r.Priority != nil {
			return refuse("Priority", "cannot be used")
		}
		if r.DeleteMarkerReplication != nil {
			return refuse("DeleteMarkerReplication", "cannot be used")
		}
	}
	return nil
}

// s3ReplicationDestinationRefusal returns check 3's refusal for cfg, or nil. A destination is named
// by its bucket ARN, arn:aws:s3:::name.
func (p *S3Plugin) s3ReplicationDestinationRefusal(ctx context.Context, cfg s3ReplicationConfiguration) (*AWSResponse, error) {
	for _, r := range cfg.Rules {
		name := strings.TrimPrefix(r.Destination.Bucket, s3ResourceARN(""))
		if _, found, err := p.s3LoadBucketRecord(ctx, name); err != nil || !found {
			if err != nil {
				return nil, err
			}
			return s3ErrorResponse("InvalidRequest", "Destination bucket must exist.", http.StatusBadRequest), nil
		}
		if p.getBucketVersioningStatus(ctx, name) != "Enabled" {
			return s3ErrorResponse("InvalidRequest", "Destination bucket must have versioning enabled.", http.StatusBadRequest), nil
		}
	}
	return nil, nil
}

// putBucketReplication handles PUT /<bucket>?replication. An existing configuration is replaced.
func (p *S3Plugin) putBucketReplication(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3ReplicationConfiguration
	if !s3DecodeConfigXML(req, &cfg) || !s3ReplicationSchemaValid(cfg) {
		return s3MalformedXMLResponse(), nil
	}
	if p.getBucketVersioningStatus(ctx, bucket) != "Enabled" {
		return s3ErrorResponse("InvalidRequest",
			"Versioning must be 'Enabled' on the bucket to apply a replication configuration", http.StatusBadRequest), nil
	}
	if refusal, err := p.s3ReplicationDestinationRefusal(ctx, cfg); refusal != nil || err != nil {
		return refusal, err
	}
	if refusal := s3ReplicationSchemaVersionRefusal(cfg); refusal != nil {
		return refusal, nil
	}
	cfg.Xmlns = ""
	if err := p.s3StoreBucketConfig(ctx, s3ReplicationKeyPrefix, bucket, cfg); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// getBucketReplication handles GET /<bucket>?replication.
func (p *S3Plugin) getBucketReplication(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3ReplicationConfiguration
	found, err := p.s3LoadBucketConfig(ctx, s3ReplicationKeyPrefix, bucket, &cfg)
	if err != nil {
		return nil, err
	}
	if !found {
		return s3ErrorResponse("ReplicationConfigurationNotFoundError",
			"The replication configuration was not found", http.StatusNotFound), nil
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}

// deleteBucketReplication handles DELETE /<bucket>?replication.
func (p *S3Plugin) deleteBucketReplication(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	return p.s3DeleteBucketConfig(context.Background(), s3ReplicationKeyPrefix, bucket)
}
