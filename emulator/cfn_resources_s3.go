package emulator

// StackDeployer helpers for CloudFormation's S3 bucket configuration and in-place update. The rules
// every in-place updater follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"encoding/xml"
	"fmt"
	"slices"
	"strconv"
)

// ----- AWS::S3::Bucket ------------------------------------------------------------------

// cfnS3BucketConfigProperties are the AWS::S3::Bucket properties the deploy applies after
// CreateBucket, each through its own bucket subresource.
//
// LifecycleConfiguration and PublicAccessBlockConfiguration joined the set in v0.131.0, because the
// CDK bootstrap template declares both on its staging bucket (#1606, part of #1573).
//
// WebsiteConfiguration, LoggingConfiguration, AccelerateConfiguration and ReplicationConfiguration
// joined it in v0.134.0 (#1613), with ObjectLockEnabled, which only an update applies here: the
// create sends it as CreateBucket's x-amz-bucket-object-lock-enabled. ObjectLockEnabled and
// ReplicationConfiguration come after VersioningConfiguration, because PutObjectLockConfiguration
// and PutBucketReplication each refuse a bucket whose versioning is not yet Enabled.
var cfnS3BucketConfigProperties = []string{"VersioningConfiguration", "BucketEncryption", "CorsConfiguration",
	"LifecycleConfiguration", "PublicAccessBlockConfiguration", "WebsiteConfiguration", "LoggingConfiguration",
	"AccelerateConfiguration", "ObjectLockEnabled", "ReplicationConfiguration"}

// cfnS3SSEConfiguration is PutBucketEncryption's body, built from the template's BucketEncryption.
type cfnS3SSEConfiguration struct {
	XMLName xml.Name        `xml:"ServerSideEncryptionConfiguration"`
	Rules   []cfnS3SSERuleX `xml:"Rule"`
}

// cfnS3SSERuleX is one ServerSideEncryptionRule of [cfnS3SSEConfiguration].
type cfnS3SSERuleX struct {
	ByDefault        *cfnS3SSEByDefaultX `xml:"ApplyServerSideEncryptionByDefault,omitempty"`
	BucketKeyEnabled *bool               `xml:"BucketKeyEnabled,omitempty"`
}

// cfnS3SSEByDefaultX is ApplyServerSideEncryptionByDefault.
type cfnS3SSEByDefaultX struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty"`
}

// cfnS3EncryptionXML renders the template's BucketEncryption — a ServerSideEncryptionConfiguration
// list of rules, each with ServerSideEncryptionByDefault and BucketKeyEnabled — as the XML
// PutBucketEncryption takes, whose rule member is ApplyServerSideEncryptionByDefault.
func cfnS3EncryptionXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	enc, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	rules, _ := enc["ServerSideEncryptionConfiguration"].([]interface{})
	var cfg cfnS3SSEConfiguration
	for _, raw := range rules {
		r, _ := raw.(map[string]interface{})
		var rule cfnS3SSERuleX
		if def, ok := r["ServerSideEncryptionByDefault"].(map[string]interface{}); ok {
			alg, _ := def["SSEAlgorithm"].(string)
			key, _ := def["KMSMasterKeyID"].(string)
			rule.ByDefault = &cfnS3SSEByDefaultX{SSEAlgorithm: alg, KMSMasterKeyID: key}
		}
		if b, ok := cfnBoolValue(r["BucketKeyEnabled"]); ok {
			rule.BucketKeyEnabled = &b
		}
		cfg.Rules = append(cfg.Rules, rule)
	}
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket encryption marshal: %w", err)
	}
	return out, nil
}

// cfnS3CORSXML renders the template's CorsConfiguration — a CorsRules list whose members are
// AllowedHeaders, AllowedMethods, AllowedOrigins, ExposedHeaders, Id and MaxAge — as the
// CORSConfiguration PutBucketCors takes, through the S3 plugin's own type.
func cfnS3CORSXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	corsCfg, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	rules, _ := corsCfg["CorsRules"].([]interface{})
	var cfg s3CORSConfiguration
	for _, raw := range rules {
		r, _ := raw.(map[string]interface{})
		rule := s3CORSRule{
			AllowedHeaders: resolveStringList(r["AllowedHeaders"], cctx),
			AllowedMethods: resolveStringList(r["AllowedMethods"], cctx),
			AllowedOrigins: resolveStringList(r["AllowedOrigins"], cctx),
			ExposeHeaders:  resolveStringList(r["ExposedHeaders"], cctx),
			ID:             resolveStringProp(r, "Id", "", cctx),
		}
		if maxAge := resolveStringProp(r, "MaxAge", "", cctx); maxAge != "" {
			if n, convErr := strconv.Atoi(maxAge); convErr == nil {
				rule.MaxAgeSeconds = &n
			}
		}
		cfg.Rules = append(cfg.Rules, rule)
	}
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket cors marshal: %w", err)
	}
	return out, nil
}

// cfnS3LifecycleConfiguration is PutBucketLifecycleConfiguration's body.
type cfnS3LifecycleConfiguration struct {
	XMLName xml.Name              `xml:"LifecycleConfiguration"`
	Rules   []cfnS3LifecycleRuleX `xml:"Rule"`
}

// cfnS3LifecycleRuleX is one LifecycleRule, member for member with API_LifecycleRule.
type cfnS3LifecycleRuleX struct {
	ID                             string                     `xml:"ID,omitempty"`
	Filter                         cfnS3LifecycleFilterX      `xml:"Filter"`
	Status                         string                     `xml:"Status"`
	Expiration                     *cfnS3LifecycleExpirationX `xml:"Expiration,omitempty"`
	Transitions                    []cfnS3LifecycleTransition `xml:"Transition,omitempty"`
	NoncurrentVersionTransitions   []cfnS3NoncurrentTransX    `xml:"NoncurrentVersionTransition,omitempty"`
	NoncurrentVersionExpiration    *cfnS3NoncurrentExpX       `xml:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *cfnS3AbortMultipartX      `xml:"AbortIncompleteMultipartUpload,omitempty"`
}

// cfnS3LifecycleFilterX is a LifecycleRuleFilter: one criterion directly, or several under And.
type cfnS3LifecycleFilterX struct {
	Prefix                *string               `xml:"Prefix,omitempty"`
	Tag                   *cfnS3TagX            `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan string                `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    string                `xml:"ObjectSizeLessThan,omitempty"`
	And                   *cfnS3LifecycleFilter `xml:"And,omitempty"`
}

// cfnS3LifecycleFilter is LifecycleRuleAndOperator.
type cfnS3LifecycleFilter struct {
	Prefix                string      `xml:"Prefix,omitempty"`
	Tags                  []cfnS3TagX `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan string      `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    string      `xml:"ObjectSizeLessThan,omitempty"`
}

// cfnS3TagX is an S3 Tag.
type cfnS3TagX struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// cfnS3LifecycleExpirationX is LifecycleExpiration.
type cfnS3LifecycleExpirationX struct {
	Date                      string `xml:"Date,omitempty"`
	Days                      string `xml:"Days,omitempty"`
	ExpiredObjectDeleteMarker string `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

// cfnS3LifecycleTransition is Transition.
type cfnS3LifecycleTransition struct {
	Date         string `xml:"Date,omitempty"`
	Days         string `xml:"Days,omitempty"`
	StorageClass string `xml:"StorageClass,omitempty"`
}

// cfnS3NoncurrentTransX is NoncurrentVersionTransition.
type cfnS3NoncurrentTransX struct {
	NoncurrentDays          string `xml:"NoncurrentDays,omitempty"`
	StorageClass            string `xml:"StorageClass,omitempty"`
	NewerNoncurrentVersions string `xml:"NewerNoncurrentVersions,omitempty"`
}

// cfnS3NoncurrentExpX is NoncurrentVersionExpiration.
type cfnS3NoncurrentExpX struct {
	NoncurrentDays          string `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions string `xml:"NewerNoncurrentVersions,omitempty"`
}

// cfnS3AbortMultipartX is AbortIncompleteMultipartUpload.
type cfnS3AbortMultipartX struct {
	DaysAfterInitiation string `xml:"DaysAfterInitiation"`
}

// cfnS3LifecycleXML renders the template's LifecycleConfiguration as the XML
// PutBucketLifecycleConfiguration takes (part of #1573). The two shapes differ, which is the whole
// of the translation, each member from the AWS::S3::Bucket Rule page and API_LifecycleRule:
//
//   - ExpirationInDays, ExpirationDate and ExpiredObjectDeleteMarker become Expiration's Days, Date
//     and ExpiredObjectDeleteMarker.
//   - Transitions (and the deprecated singular Transition) become Transition elements, their
//     TransitionInDays and TransitionDate becoming Days and Date.
//   - NoncurrentVersionTransitions (and the deprecated singular) become NoncurrentVersionTransition
//     elements, TransitionInDays becoming NoncurrentDays.
//   - NoncurrentVersionExpiration carries over, and the deprecated NoncurrentVersionExpirationInDays
//     becomes its NoncurrentDays.
//   - Prefix, TagFilters and the two ObjectSize bounds become the rule's Filter: one criterion
//     directly, more than one under And, and none as an empty Prefix, which applies the rule to
//     every object.
//   - Id becomes ID; Status and AbortIncompleteMultipartUpload carry over.
func cfnS3LifecycleXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	lc, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	rules, _ := lc["Rules"].([]interface{})
	var cfg cfnS3LifecycleConfiguration
	for _, raw := range rules {
		r, _ := raw.(map[string]interface{})
		rule := cfnS3LifecycleRuleX{
			ID:     cfnScalarString(r["Id"]),
			Status: cfnScalarString(r["Status"]),
			Filter: cfnS3LifecycleFilterXML(r),
		}
		exp := cfnS3LifecycleExpirationX{
			Days:                      cfnScalarString(r["ExpirationInDays"]),
			Date:                      cfnScalarString(r["ExpirationDate"]),
			ExpiredObjectDeleteMarker: cfnScalarString(r["ExpiredObjectDeleteMarker"]),
		}
		if exp != (cfnS3LifecycleExpirationX{}) {
			rule.Expiration = &exp
		}
		for _, t := range cfnListOrOne(r["Transitions"], r["Transition"]) {
			rule.Transitions = append(rule.Transitions, cfnS3LifecycleTransition{
				Days:         cfnScalarString(t["TransitionInDays"]),
				Date:         cfnScalarString(t["TransitionDate"]),
				StorageClass: cfnScalarString(t["StorageClass"]),
			})
		}
		for _, t := range cfnListOrOne(r["NoncurrentVersionTransitions"], r["NoncurrentVersionTransition"]) {
			rule.NoncurrentVersionTransitions = append(rule.NoncurrentVersionTransitions, cfnS3NoncurrentTransX{
				NoncurrentDays:          cfnScalarString(t["TransitionInDays"]),
				StorageClass:            cfnScalarString(t["StorageClass"]),
				NewerNoncurrentVersions: cfnScalarString(t["NewerNoncurrentVersions"]),
			})
		}
		if nve, ok := r["NoncurrentVersionExpiration"].(map[string]interface{}); ok {
			rule.NoncurrentVersionExpiration = &cfnS3NoncurrentExpX{
				NoncurrentDays:          cfnScalarString(nve["NoncurrentDays"]),
				NewerNoncurrentVersions: cfnScalarString(nve["NewerNoncurrentVersions"]),
			}
		} else if days := cfnScalarString(r["NoncurrentVersionExpirationInDays"]); days != "" {
			rule.NoncurrentVersionExpiration = &cfnS3NoncurrentExpX{NoncurrentDays: days}
		}
		if abort, ok := r["AbortIncompleteMultipartUpload"].(map[string]interface{}); ok {
			rule.AbortIncompleteMultipartUpload = &cfnS3AbortMultipartX{
				DaysAfterInitiation: cfnScalarString(abort["DaysAfterInitiation"]),
			}
		}
		cfg.Rules = append(cfg.Rules, rule)
	}
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket lifecycle marshal: %w", err)
	}
	return out, nil
}

// cfnS3LifecycleFilterXML builds a rule's Filter from its Prefix, TagFilters and ObjectSize bounds.
func cfnS3LifecycleFilterXML(r map[string]interface{}) cfnS3LifecycleFilterX {
	prefix := cfnScalarString(r["Prefix"])
	greater, less := cfnScalarString(r["ObjectSizeGreaterThan"]), cfnScalarString(r["ObjectSizeLessThan"])
	var tags []cfnS3TagX
	for _, t := range cfnListOrOne(r["TagFilters"], nil) {
		tags = append(tags, cfnS3TagX{Key: cfnScalarString(t["Key"]), Value: cfnScalarString(t["Value"])})
	}
	criteria := len(tags)
	for _, c := range []string{prefix, greater, less} {
		if c != "" {
			criteria++
		}
	}
	switch {
	case criteria > 1:
		return cfnS3LifecycleFilterX{And: &cfnS3LifecycleFilter{Prefix: prefix, Tags: tags,
			ObjectSizeGreaterThan: greater, ObjectSizeLessThan: less}}
	case len(tags) == 1:
		return cfnS3LifecycleFilterX{Tag: &tags[0]}
	case greater != "" || less != "":
		return cfnS3LifecycleFilterX{ObjectSizeGreaterThan: greater, ObjectSizeLessThan: less}
	}
	return cfnS3LifecycleFilterX{Prefix: &prefix}
}

// cfnListOrOne returns a resolved list-of-structures property, or the deprecated singular property
// beside it as a one-element list when the list is absent.
func cfnListOrOne(list, one interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	if items, ok := list.([]interface{}); ok {
		for _, item := range items {
			if m, isMap := item.(map[string]interface{}); isMap {
				out = append(out, m)
			}
		}
		return out
	}
	if m, ok := one.(map[string]interface{}); ok {
		out = append(out, m)
	}
	return out
}

// cfnScalarString renders a resolved template scalar as the text an XML element carries: a string
// as it is, a number without a fractional part when it has none, a boolean as true or false, and
// anything else as the empty string.
func cfnScalarString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case bool:
		return strconv.FormatBool(val)
	}
	return ""
}

// cfnS3PublicAccessBlockXML renders the template's PublicAccessBlockConfiguration — four booleans,
// each "Required: No" and false when omitted — as the PublicAccessBlockConfiguration
// PutPublicAccessBlock takes, through the S3 plugin's own type.
func cfnS3PublicAccessBlockXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	pab, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	var cfg S3PublicAccessBlockConfiguration
	cfg.BlockPublicAcls, _ = cfnBoolValue(pab["BlockPublicAcls"])
	cfg.IgnorePublicAcls, _ = cfnBoolValue(pab["IgnorePublicAcls"])
	cfg.BlockPublicPolicy, _ = cfnBoolValue(pab["BlockPublicPolicy"])
	cfg.RestrictPublicBuckets, _ = cfnBoolValue(pab["RestrictPublicBuckets"])
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket public access block marshal: %w", err)
	}
	return out, nil
}

// configureS3Bucket applies the bucket subresources the template declares — versioning, default
// encryption, CORS, lifecycle, Block Public Access, website, logging, Transfer Acceleration and
// replication — for the properties in apply, and returns the first refusal.
//
// The create calls it for every declared property; the update for the changed ones. A declared
// VersioningConfiguration sends its Status; a create skips Suspended, which a new bucket already
// is in effect. A removed BucketEncryption, CorsConfiguration, LifecycleConfiguration or
// PublicAccessBlockConfiguration is deleted, which returns the bucket to the state a bucket created
// without it has (SSE-S3 default encryption, no CORS, no lifecycle rules, no Block Public Access
// configuration — substrate does not apply S3's account-level default, see putPublicAccessBlock). A
// removed VersioningConfiguration is left as it is: a bucket that has been versioned cannot return
// to unversioned, and the page does not say what CloudFormation does instead. A removed
// WebsiteConfiguration or ReplicationConfiguration is deleted, a removed LoggingConfiguration
// disables logging, and a removed AccelerateConfiguration, like an ObjectLockEnabled turned off, is
// left as it is, for the reason its case gives.
func (d *StackDeployer) configureS3Bucket(
	ctx context.Context, bucket string, props map[string]interface{}, apply []string, creating bool,
	streamID string, cctx *cfnContext,
) (float64, error) {
	var cost float64
	send := func(method, sub string, body []byte) error {
		headers := map[string]string{}
		if body != nil {
			headers["Content-Type"] = "application/xml"
		}
		return d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "s3", Operation: method, Path: "/" + bucket,
			Params: map[string]string{sub: "1"}, Headers: headers, Body: body}, streamID, &cost)
	}
	for _, prop := range apply {
		raw, declared := props[prop]
		// A property whose Fn::If chose AWS::NoValue is not declared — the bootstrap template's
		// PublicAccessBlockConfiguration is one, under its UsePublicAccessBlockConfiguration
		// condition — and a create has nothing to undo for one.
		if _, chosen := cfnChooseIf(raw, cctx); declared && !chosen {
			declared = false
		}
		if creating && !declared {
			continue
		}
		var err error
		switch prop {
		case "VersioningConfiguration":
			vc, _ := cfnResolveStructured(raw, cctx).(map[string]interface{})
			status, _ := vc["Status"].(string)
			if !declared || status == "" || (creating && status != "Enabled") {
				continue
			}
			err = send("PUT", "versioning",
				[]byte("<VersioningConfiguration><Status>"+status+"</Status></VersioningConfiguration>"))
		case "BucketEncryption":
			if !declared {
				err = send("DELETE", "encryption", nil)
				break
			}
			body, xmlErr := cfnS3EncryptionXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "encryption", body)
		case "CorsConfiguration":
			if !declared {
				err = send("DELETE", "cors", nil)
				break
			}
			body, xmlErr := cfnS3CORSXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "cors", body)
		case "LifecycleConfiguration":
			// "A removed LifecycleConfiguration should DELETE ?lifecycle" (#1573): a bucket created
			// without one has no lifecycle configuration.
			if !declared {
				err = send("DELETE", "lifecycle", nil)
				break
			}
			body, xmlErr := cfnS3LifecycleXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "lifecycle", body)
		case "PublicAccessBlockConfiguration":
			if !declared {
				err = send("DELETE", "publicAccessBlock", nil)
				break
			}
			body, xmlErr := cfnS3PublicAccessBlockXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "publicAccessBlock", body)
		case "WebsiteConfiguration":
			if !declared {
				err = send("DELETE", "website", nil)
				break
			}
			body, xmlErr := cfnS3WebsiteXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "website", body)
		case "LoggingConfiguration":
			// A removed LoggingConfiguration disables logging, which PutBucketLogging spells as an
			// empty BucketLoggingStatus.
			body := []byte("<BucketLoggingStatus/>")
			if declared {
				var xmlErr error
				if body, xmlErr = cfnS3LoggingXML(raw, bucket, cctx); xmlErr != nil {
					return cost, xmlErr
				}
			}
			err = send("PUT", "logging", body)
		case "ObjectLockEnabled":
			// The create sent it to CreateBucket. On an update, true enables Object Lock on the
			// existing bucket; false or a removal is left as it is, because "you can't disable
			// Object Lock" once it is enabled (userguide/object-lock-configure.html).
			enabled, _ := cfnBoolValue(cfnResolveStructured(raw, cctx))
			if creating || !declared || !enabled {
				continue
			}
			err = send("PUT", "object-lock",
				[]byte("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"))
		case "AccelerateConfiguration":
			// There is no DeleteBucketAccelerateConfiguration, and a bucket whose state was once set
			// cannot return to having none, so a removed AccelerateConfiguration is left as it is.
			if !declared {
				continue
			}
			body, xmlErr := cfnS3AccelerateXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "accelerate", body)
		case "ReplicationConfiguration":
			if !declared {
				err = send("DELETE", "replication", nil)
				break
			}
			body, xmlErr := cfnS3ReplicationXML(raw, cctx)
			if xmlErr != nil {
				return cost, xmlErr
			}
			err = send("PUT", "replication", body)
		}
		if err != nil {
			return cost, err
		}
	}
	return cost, nil
}

// updateS3Bucket applies a changed AWS::S3::Bucket's configuration properties
// ([cfnS3BucketConfigProperties]) through their bucket subresources (see [StackDeployer.configureS3Bucket]).
// BucketName, BucketNamePrefix and BucketNamespace are Replacement.
func (d *StackDeployer) updateS3Bucket(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	var apply []string
	for _, p := range cfnS3BucketConfigProperties {
		if slices.Contains(changed, p) {
			apply = append(apply, p)
		}
	}
	cost, err := d.configureS3Bucket(ctx, prior.PhysicalID, props, apply, false, streamID, cctx)
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnS3WebsiteXML renders the template's WebsiteConfiguration — IndexDocument and ErrorDocument as
// plain strings, RedirectAllRequestsTo, and RoutingRules whose members are RoutingRuleCondition and
// RedirectRule — as the WebsiteConfiguration PutBucketWebsite takes.
func cfnS3WebsiteXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	web, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	var cfg s3WebsiteConfiguration
	if idx := cfnScalarString(web["IndexDocument"]); idx != "" {
		cfg.IndexDocument = &s3WebsiteIndexDocument{Suffix: idx}
	}
	if errDoc := cfnScalarString(web["ErrorDocument"]); errDoc != "" {
		cfg.ErrorDocument = &s3WebsiteErrorDocument{Key: errDoc}
	}
	if r, ok := web["RedirectAllRequestsTo"].(map[string]interface{}); ok {
		cfg.RedirectAllRequestsTo = &s3WebsiteRedirectAll{
			HostName: cfnScalarString(r["HostName"]), Protocol: cfnScalarString(r["Protocol"]),
		}
	}
	for _, rule := range cfnListOrOne(web["RoutingRules"], nil) {
		var out s3WebsiteRoutingRule
		if c, ok := rule["RoutingRuleCondition"].(map[string]interface{}); ok {
			out.Condition = &s3WebsiteCondition{
				HTTPErrorCodeReturnedEquals: cfnScalarString(c["HttpErrorCodeReturnedEquals"]),
				KeyPrefixEquals:             cfnScalarString(c["KeyPrefixEquals"]),
			}
		}
		if r, ok := rule["RedirectRule"].(map[string]interface{}); ok {
			out.Redirect = &s3WebsiteRedirect{
				HostName:             cfnScalarString(r["HostName"]),
				HTTPRedirectCode:     cfnScalarString(r["HttpRedirectCode"]),
				Protocol:             cfnScalarString(r["Protocol"]),
				ReplaceKeyPrefixWith: cfnScalarString(r["ReplaceKeyPrefixWith"]),
				ReplaceKeyWith:       cfnScalarString(r["ReplaceKeyWith"]),
			}
		}
		if cfg.RoutingRules == nil {
			cfg.RoutingRules = &s3WebsiteRoutingRules{}
		}
		cfg.RoutingRules.Rules = append(cfg.RoutingRules.Rules, out)
	}
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket website marshal: %w", err)
	}
	return out, nil
}

// cfnS3LoggingXML renders the template's LoggingConfiguration — DestinationBucketName,
// LogFilePrefix and TargetObjectKeyFormat — as the BucketLoggingStatus PutBucketLogging takes. The
// AWS::S3::Bucket page says logs are stored in the bucket itself when DestinationBucketName is
// absent, and TargetPrefix, which the API requires, is the empty prefix when LogFilePrefix is.
func cfnS3LoggingXML(v interface{}, bucket string, cctx *cfnContext) ([]byte, error) {
	lc, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	target := cfnScalarString(lc["DestinationBucketName"])
	if target == "" {
		target = bucket
	}
	prefix := cfnScalarString(lc["LogFilePrefix"])
	enabled := &s3LoggingEnabled{TargetBucket: target, TargetPrefix: &prefix}
	if f, ok := lc["TargetObjectKeyFormat"].(map[string]interface{}); ok {
		enabled.TargetObjectKeyFormat = &s3TargetObjectKeyFormat{}
		if pp, isMap := f["PartitionedPrefix"].(map[string]interface{}); isMap {
			enabled.TargetObjectKeyFormat.PartitionedPrefix = &s3PartitionedPrefix{
				PartitionDateSource: cfnScalarString(pp["PartitionDateSource"]),
			}
		}
		if _, isMap := f["SimplePrefix"].(map[string]interface{}); isMap {
			enabled.TargetObjectKeyFormat.SimplePrefix = &struct{}{}
		}
	}
	out, err := xml.Marshal(s3BucketLoggingStatus{LoggingEnabled: enabled})
	if err != nil {
		return nil, fmt.Errorf("cfn bucket logging marshal: %w", err)
	}
	return out, nil
}

// cfnS3AccelerateXML renders the template's AccelerateConfiguration, whose one member is
// AccelerationStatus, as the AccelerateConfiguration PutBucketAccelerateConfiguration takes.
func cfnS3AccelerateXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	acc, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	out, err := xml.Marshal(s3AccelerateConfiguration{Status: cfnScalarString(acc["AccelerationStatus"])})
	if err != nil {
		return nil, fmt.Errorf("cfn bucket accelerate marshal: %w", err)
	}
	return out, nil
}

// cfnS3ReplicationXML renders the template's ReplicationConfiguration — Role and Rules, whose
// Filter spells its tags TagFilter and TagFilters and whose ID is Id — as the
// ReplicationConfiguration PutBucketReplication takes.
func cfnS3ReplicationXML(v interface{}, cctx *cfnContext) ([]byte, error) {
	rc, _ := cfnResolveStructured(v, cctx).(map[string]interface{})
	cfg := s3ReplicationConfiguration{Role: cfnScalarString(rc["Role"])}
	for _, r := range cfnListOrOne(rc["Rules"], nil) {
		rule := s3ReplicationRule{
			ID:                      cfnScalarString(r["Id"]),
			Status:                  cfnScalarString(r["Status"]),
			Priority:                cfnIntPtr(r["Priority"]),
			DeleteMarkerReplication: cfnS3ReplicationStatus(r, "DeleteMarkerReplication"),
		}
		if p, ok := r["Prefix"]; ok {
			prefix := cfnScalarString(p)
			rule.Prefix = &prefix
		}
		if f, ok := r["Filter"].(map[string]interface{}); ok {
			rule.Filter = cfnS3ReplicationFilter(f)
		}
		if sc, ok := r["SourceSelectionCriteria"].(map[string]interface{}); ok {
			rule.SourceSelectionCriteria = &s3ReplicationSourceCriteria{
				ReplicaModifications:   cfnS3ReplicationStatus(sc, "ReplicaModifications"),
				SseKmsEncryptedObjects: cfnS3ReplicationStatus(sc, "SseKmsEncryptedObjects"),
			}
		}
		if d, ok := r["Destination"].(map[string]interface{}); ok {
			rule.Destination = cfnS3ReplicationDestination(d)
		}
		cfg.Rules = append(cfg.Rules, rule)
	}
	out, err := xml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cfn bucket replication marshal: %w", err)
	}
	return out, nil
}

// cfnS3ReplicationFilter renders a replication rule's Filter: Prefix, TagFilter, or And with a
// Prefix and TagFilters.
func cfnS3ReplicationFilter(f map[string]interface{}) *s3ReplicationFilter {
	out := &s3ReplicationFilter{Prefix: cfnStringPtr(f, "Prefix")}
	if t, ok := f["TagFilter"].(map[string]interface{}); ok {
		tag := cfnS3ReplicationTag(t)
		out.Tag = &tag
	}
	if and, ok := f["And"].(map[string]interface{}); ok {
		out.And = &s3ReplicationAndOp{Prefix: cfnStringPtr(and, "Prefix")}
		for _, t := range cfnListOrOne(and["TagFilters"], nil) {
			out.And.Tags = append(out.And.Tags, cfnS3ReplicationTag(t))
		}
	}
	return out
}

// cfnS3ReplicationDestination renders a replication rule's Destination.
func cfnS3ReplicationDestination(d map[string]interface{}) *s3ReplicationDestination {
	dest := &s3ReplicationDestination{
		Bucket:       cfnScalarString(d["Bucket"]),
		Account:      cfnScalarString(d["Account"]),
		StorageClass: cfnScalarString(d["StorageClass"]),
	}
	if a, ok := d["AccessControlTranslation"].(map[string]interface{}); ok {
		dest.AccessControlTranslation = &s3ReplicationACLTranslate{Owner: cfnScalarString(a["Owner"])}
	}
	if e, ok := d["EncryptionConfiguration"].(map[string]interface{}); ok {
		dest.EncryptionConfiguration = &s3ReplicationEncryption{ReplicaKmsKeyID: cfnScalarString(e["ReplicaKmsKeyID"])}
	}
	if rt, ok := d["ReplicationTime"].(map[string]interface{}); ok {
		dest.ReplicationTime = &s3ReplicationTime{Status: cfnScalarString(rt["Status"]), Time: cfnS3ReplicationMinutes(rt, "Time")}
	}
	if m, ok := d["Metrics"].(map[string]interface{}); ok {
		dest.Metrics = &s3ReplicationMetrics{Status: cfnScalarString(m["Status"]), EventThreshold: cfnS3ReplicationMinutes(m, "EventThreshold")}
	}
	return dest
}

// cfnS3ReplicationStatus renders the {Status} container at m[key], or nil when it is absent.
func cfnS3ReplicationStatus(m map[string]interface{}, key string) *s3ReplicationStatus {
	if c, ok := m[key].(map[string]interface{}); ok {
		return &s3ReplicationStatus{Status: cfnScalarString(c["Status"])}
	}
	return nil
}

// cfnS3ReplicationMinutes renders the {Minutes} container at m[key], or nil when it is absent.
func cfnS3ReplicationMinutes(m map[string]interface{}, key string) *s3ReplicationMinutes {
	c, ok := m[key].(map[string]interface{})
	if !ok {
		return nil
	}
	return &s3ReplicationMinutes{Minutes: cfnIntPtr(c["Minutes"])}
}

// cfnS3ReplicationTag renders one TagFilter.
func cfnS3ReplicationTag(m map[string]interface{}) s3ReplicationTag {
	return s3ReplicationTag{Key: cfnScalarString(m["Key"]), Value: cfnScalarString(m["Value"])}
}

// cfnIntPtr returns a resolved template scalar as an integer, or nil when it is absent or is not
// one.
func cfnIntPtr(v interface{}) *int {
	n, err := strconv.Atoi(cfnScalarString(v))
	if err != nil {
		return nil
	}
	return &n
}

// cfnStringPtr returns m[key] as a string when the key is present, or nil when it is absent, so an
// empty prefix stays distinct from none.
func cfnStringPtr(m map[string]interface{}, key string) *string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	s := cfnScalarString(v)
	return &s
}
