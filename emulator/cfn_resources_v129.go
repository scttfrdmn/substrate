package emulator

// cfn_resources_v129.go holds the in-place updates v0.129.0 adds to cfnResourceUpdaters (#1545,
// #1550): AWS::Lambda::Function, AWS::DynamoDB::Table, AWS::SQS::Queue, AWS::SNS::Topic,
// AWS::S3::Bucket, AWS::KMS::Key, AWS::EC2::SecurityGroup and AWS::ECS::Service, and the
// create-side configuration each shares with its update. The name records the release rather
// than the services, as the other cfn_resources_v*.go files do.
//
// Three rules hold for every updater here, so each doc comment states only its exceptions:
//
//   - **What the update applies is what the create applies.** An updater sends the properties its
//     type's deploy helper forwards, through the service's own update operation; a property the
//     create does not forward either (most types' Tags, a Lambda function's VpcConfig, a queue's
//     RedrivePolicy) is left alone and makes no call, which is what re-creating the resource used
//     to do with it too. A property the create forwards and substrate has no routed operation to
//     update is in cfnUpdateUnmodelledProperties instead, and a change to it fails the resource.
//   - **A property removed from the template resets** where the resource page publishes the value
//     an undeclared property has — SQS's attribute defaults, a Lambda function's Timeout and
//     MemorySize — or where the create itself sends a value for an undeclared property, and the
//     service's API can express it. Otherwise the removal is left as it is and the updater's doc
//     comment says so.
//   - **The physical ID stands**: an updater returns the prior record, so a Ref to the resource
//     resolves as it did, and a refusal is reported on the resource exactly as a create's is.
//
// Template-level Tags are not applied by any of these creates, and so not by their updates. The
// stack's own tags and CloudFormation's three aws:cloudformation:* tags are written on the same
// records, so applying a template's Tags through a replace-the-set call (S3's PutBucketTagging)
// would drop them; that reconciliation is its own change.

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// cfnUpdateUnmodelledProperties maps a type that has an updater to the properties its create
// forwards and its update cannot apply, each with the reason. A change to one fails the resource
// with the reason and the update rolls back, rather than leaving a resource whose live
// configuration no longer matches its template. The reasons are substrate's own.
var cfnUpdateUnmodelledProperties = map[string]map[string]string{
	// The DynamoDB page gives KeySchema "Some interruptions" and the index and stream properties
	// "No interruption"; substrate's UpdateTable takes BillingMode and ProvisionedThroughput only
	// (#1574).
	"AWS::DynamoDB::Table": {
		"KeySchema":              "UpdateTable takes no KeySchema",
		"AttributeDefinitions":   "substrate's UpdateTable takes no AttributeDefinitions",
		"GlobalSecondaryIndexes": "substrate's UpdateTable takes no GlobalSecondaryIndexUpdates",
		"LocalSecondaryIndexes":  "UpdateTable cannot change a table's local secondary indexes",
		"StreamSpecification":    "substrate's UpdateTable takes no StreamSpecification",
	},
	// LaunchType is "Some interruptions" on the ECS::Service page; UpdateService has no launch type
	// member, and how CloudFormation applies the change is not published.
	"AWS::ECS::Service": {
		"LaunchType": "UpdateService takes no launch type",
	},
}

// cfnChangedAny reports whether changed names any of props.
func cfnChangedAny(changed []string, props ...string) bool {
	for _, p := range props {
		if slices.Contains(changed, p) {
			return true
		}
	}
	return false
}

// cfnUpdateDispatch sends one update request and folds its cost and refusal into the running
// totals, so an updater making several calls stops at the first refusal and reports it.
func (d *StackDeployer) cfnUpdateDispatch(
	ctx context.Context, req *AWSRequest, streamID string, cost *float64,
) error {
	_, c, err := d.dispatch(ctx, req, streamID)
	*cost += c
	return err
}

// cfnJSONRequest builds a JSON-protocol request to service with body marshaled.
func cfnJSONRequest(service, operation string, body interface{}) (*AWSRequest, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("cfn %s %s marshal: %w", service, operation, err)
	}
	return &AWSRequest{
		Service: service, Operation: operation, Body: data,
		Headers: map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	}, nil
}

// ----- AWS::Lambda::Function ------------------------------------------------------------

// cfnLambdaConfigProperties are the AWS::Lambda::Function properties the deploy forwards as
// configuration, which UpdateFunctionConfiguration takes.
var cfnLambdaConfigProperties = []string{
	"Runtime", "Role", "Handler", "Description", "Timeout", "MemorySize", "Environment",
}

// The defaults AWS::Lambda::Function publishes for two of them: "The default is 3 seconds"
// (Timeout) and "The default value is 128 MB." (MemorySize).
const (
	cfnLambdaDefaultTimeout    = 3
	cfnLambdaDefaultMemorySize = 128
)

// cfnLambdaConfiguration renders a function's configuration members, the same for CreateFunction
// and UpdateFunctionConfiguration. Runtime and Handler take the defaults the create has always
// sent. Timeout and MemorySize are numeric in the Lambda API, so the resolved string is converted.
//
// Environment's Variables are rendered as strings: the Lambda API's map is string to string, and a
// YAML template's `PORT: 8080` decodes as a number.
func cfnLambdaConfiguration(props map[string]interface{}, cctx *cfnContext) map[string]interface{} {
	body := map[string]interface{}{
		"Runtime":     resolveStringProp(props, "Runtime", "python3.12", cctx),
		"Role":        resolveStringProp(props, "Role", "", cctx),
		"Handler":     resolveStringProp(props, "Handler", "index.handler", cctx),
		"Description": resolveStringProp(props, "Description", "", cctx),
	}
	if timeout := resolveStringProp(props, "Timeout", "", cctx); timeout != "" {
		if n, convErr := strconv.Atoi(timeout); convErr == nil {
			body["Timeout"] = n
		}
	}
	if memory := resolveStringProp(props, "MemorySize", "", cctx); memory != "" {
		if n, convErr := strconv.Atoi(memory); convErr == nil {
			body["MemorySize"] = n
		}
	}
	if env, ok := props["Environment"].(map[string]interface{}); ok {
		vars := map[string]string{}
		if declared, ok := env["Variables"].(map[string]interface{}); ok {
			for k, v := range declared {
				vars[k] = resolveValue(v, cctx)
			}
		}
		body["Environment"] = map[string]interface{}{"Variables": vars}
	}
	return body
}

// updateLambdaFunction applies a changed AWS::Lambda::Function through UpdateFunctionConfiguration
// and UpdateFunctionCode, in that order — the page: "When you update a AWS::Lambda::Function
// resource, CloudFormation calls the UpdateFunctionConfiguration and UpdateFunctionCodeLambda APIs".
// FunctionName, PackageType, DurableConfig and TenancyConfig are Replacement and never reach here.
//
// A removed Timeout or MemorySize resets to the page's default, and a removed Environment to no
// variables. A removed Description is left as it was: substrate's UpdateFunctionConfiguration
// treats an empty Description as not sent.
func (d *StackDeployer) updateLambdaFunction(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	var cost float64
	base := "/2015-03-31/functions/" + prior.PhysicalID
	cfg := cfnLambdaConfiguration(props, cctx)
	if cfnChangedAny(changed, cfnLambdaConfigProperties...) {
		if _, ok := cfg["Timeout"]; !ok {
			cfg["Timeout"] = cfnLambdaDefaultTimeout
		}
		if _, ok := cfg["MemorySize"]; !ok {
			cfg["MemorySize"] = cfnLambdaDefaultMemorySize
		}
		if _, ok := cfg["Environment"]; !ok {
			cfg["Environment"] = map[string]interface{}{"Variables": map[string]string{}}
		}
		req, err := cfnJSONRequest("lambda", "PUT", cfg)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		req.Path = base + "/configuration"
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if slices.Contains(changed, "Code") {
		body := map[string]interface{}{"Runtime": cfg["Runtime"]}
		cfnLambdaCode(body, props, cctx)
		code, _ := body["Code"].(map[string]interface{})
		if code == nil {
			code = map[string]interface{}{}
		}
		req, err := cfnJSONRequest("lambda", "PUT", code)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		req.Path = base + "/code"
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	return dr, cost, nil
}

// ----- AWS::DynamoDB::Table -------------------------------------------------------------

// updateDynamoDBTable applies a changed AWS::DynamoDB::Table through UpdateTable (BillingMode and
// ProvisionedThroughput) and UpdateTimeToLive (TimeToLiveSpecification). TableName and
// ImportSourceSpecification are Replacement; the key schema, index and stream properties are in
// cfnUpdateUnmodelledProperties.
//
// A removed BillingMode resets to PROVISIONED — "If not specified, the default is PROVISIONED." A
// removed TimeToLiveSpecification disables TTL on the attribute the previous declaration named.
func (d *StackDeployer) updateDynamoDBTable(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	var cost float64
	if cfnChangedAny(changed, "BillingMode", "ProvisionedThroughput") {
		body := map[string]interface{}{
			"TableName":   prior.PhysicalID,
			"BillingMode": resolveStringProp(props, "BillingMode", "PROVISIONED", cctx),
		}
		if pt, ok := props["ProvisionedThroughput"]; ok {
			body["ProvisionedThroughput"] = resolveNested(pt, cctx)
		}
		req, err := cfnJSONRequest("dynamodb", "UpdateTable", body)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if slices.Contains(changed, "TimeToLiveSpecification") {
		spec, declared := props["TimeToLiveSpecification"]
		var ttl interface{}
		if declared {
			ttl = resolveNested(spec, cctx)
		} else {
			old, _ := cctx.priorProps["TimeToLiveSpecification"].(map[string]interface{})
			attr, _ := old["AttributeName"].(string)
			ttl = map[string]interface{}{"AttributeName": attr, "Enabled": false}
		}
		req, err := cfnJSONRequest("dynamodb", "UpdateTimeToLive",
			map[string]interface{}{"TableName": prior.PhysicalID, "TimeToLiveSpecification": ttl})
		if err != nil {
			return DeployedResource{}, 0, err
		}
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	return dr, cost, nil
}

// ----- AWS::SQS::Queue ------------------------------------------------------------------

// updateSQSQueue applies a changed AWS::SQS::Queue's attributes through SetQueueAttributes, for the
// five the create forwards (sqsDriftAttributes). QueueName and FifoQueue are Replacement.
//
// A removed attribute resets to its default, which the page publishes for each — VisibilityTimeout
// "If you don't specify a value, AWS CloudFormation uses the default value of 30 seconds",
// DelaySeconds "The default value is 0", MessageRetentionPeriod 345,600 seconds,
// MaximumMessageSize 1 MiB, ReceiveMessageWaitTimeSeconds short polling (0). The values are SQS's
// own (sqsAttributeDefaults), so the queue reads back what a queue created without the attribute
// reports.
func (d *StackDeployer) updateSQSQueue(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	params := map[string]string{
		"Action":   "SetQueueAttributes",
		"QueueUrl": sqsQueueURL(cctx.region, cctx.accountID, prior.PhysicalID),
	}
	n := 0
	for _, name := range sqsDriftAttributes {
		if !slices.Contains(changed, name) {
			continue
		}
		value := sqsAttributeDefaults[name]
		if _, declared := props[name]; declared {
			value = resolveValue(props[name], cctx)
		}
		n++
		params[fmt.Sprintf("Attribute.%d.Name", n)] = name
		params[fmt.Sprintf("Attribute.%d.Value", n)] = value
	}
	if n == 0 {
		return dr, 0, nil
	}
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sqs", Operation: "SetQueueAttributes",
		Headers: map[string]string{}, Params: params}, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- AWS::SNS::Topic ------------------------------------------------------------------

// updateSNSTopic applies a changed AWS::SNS::Topic's DisplayName, the one attribute the create
// forwards, through SetTopicAttributes. TopicName and FifoTopic are Replacement.
//
// A removed DisplayName is set to the empty string, which is what a topic created without one
// reports.
func (d *StackDeployer) updateSNSTopic(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !slices.Contains(changed, "DisplayName") {
		return dr, 0, nil
	}
	arn := prior.PhysicalID
	if !strings.HasPrefix(arn, "arn:") {
		arn = fmt.Sprintf("arn:aws:sns:%s:%s:%s", cctx.region, cctx.accountID, arn)
	}
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sns", Operation: "SetTopicAttributes",
		Headers: map[string]string{},
		Params: map[string]string{
			"Action": "SetTopicAttributes", "TopicArn": arn, "AttributeName": "DisplayName",
			"AttributeValue": resolveStringProp(props, "DisplayName", "", cctx),
		}}, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- AWS::S3::Bucket ------------------------------------------------------------------

// cfnS3BucketConfigProperties are the AWS::S3::Bucket properties the deploy applies after
// CreateBucket, each through its own bucket subresource.
var cfnS3BucketConfigProperties = []string{"VersioningConfiguration", "BucketEncryption", "CorsConfiguration"}

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
	enc, _ := resolveNested(v, cctx).(map[string]interface{})
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
	corsCfg, _ := resolveNested(v, cctx).(map[string]interface{})
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

// cfnBoolValue reads a template boolean, which a JSON template spells true and a YAML or
// parameter-fed one may spell "true".
func cfnBoolValue(v interface{}) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	case string:
		parsed, err := strconv.ParseBool(b)
		return parsed, err == nil
	}
	return false, false
}

// configureS3Bucket applies the bucket subresources the template declares — versioning, default
// encryption and CORS — for the properties in apply, and returns the first refusal.
//
// The create calls it for every declared property; the update for the changed ones. A declared
// VersioningConfiguration sends its Status; a create skips Suspended, which a new bucket already
// is in effect. A removed BucketEncryption or CorsConfiguration is deleted, which returns the
// bucket to the state a bucket created without it has (SSE-S3 default encryption, no CORS). A
// removed VersioningConfiguration is left as it is: a bucket that has been versioned cannot return
// to unversioned, and the page does not say what CloudFormation does instead.
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
		var err error
		switch prop {
		case "VersioningConfiguration":
			vc, _ := resolveNested(raw, cctx).(map[string]interface{})
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
		}
		if err != nil {
			return cost, err
		}
	}
	return cost, nil
}

// updateS3Bucket applies a changed AWS::S3::Bucket's versioning, default encryption and CORS
// through their bucket subresources (see [StackDeployer.configureS3Bucket]). BucketName,
// BucketNamePrefix and BucketNamespace are Replacement.
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

// ----- AWS::KMS::Key --------------------------------------------------------------------

// configureKMSKey applies a key's Enabled, EnableKeyRotation/RotationPeriodInDays, KeyPolicy and
// Description for the properties in apply, and returns the first refusal.
//
// The create applies the declared ones its CreateKey does not take — rotation and Enabled — and the
// update every changed one. The order keeps each call legal: a key being enabled is enabled first,
// and a key being disabled is disabled last, because EnableKeyRotation and DisableKeyRotation
// refuse a disabled key.
//
// A removed property resets to the page's default: Enabled "The default value is true",
// EnableKeyRotation "By default, automatic key rotation is not enabled", RotationPeriodInDays "the
// default value is 365 days", and KeyPolicy "If you do not provide a key policy, AWS KMS attaches
// a default key policy" — KMS's default document, which PutKeyPolicy writes back. A removed
// Description is set to the empty string the create sends for an undeclared one.
func (d *StackDeployer) configureKMSKey(
	ctx context.Context, keyID string, props map[string]interface{}, apply []string, creating bool,
	streamID string, cctx *cfnContext,
) (float64, error) {
	var cost float64
	call := func(operation string, body map[string]interface{}) error {
		body["KeyId"] = keyID
		req, err := cfnJSONRequest("kms", operation, body)
		if err != nil {
			return err
		}
		return d.cfnUpdateDispatch(ctx, req, streamID, &cost)
	}
	enabled := true
	if v, ok := cfnBoolValue(resolveNested(props["Enabled"], cctx)); ok {
		enabled = v
	}
	if enabled && !creating && slices.Contains(apply, "Enabled") {
		if err := call("EnableKey", map[string]interface{}{}); err != nil {
			return cost, err
		}
	}
	if cfnChangedAny(apply, "EnableKeyRotation", "RotationPeriodInDays") {
		rotate, _ := cfnBoolValue(resolveNested(props["EnableKeyRotation"], cctx))
		var err error
		switch {
		case rotate:
			body := map[string]interface{}{}
			if p := resolveStringProp(props, "RotationPeriodInDays", "", cctx); p != "" {
				if n, convErr := strconv.Atoi(p); convErr == nil {
					body["RotationPeriodInDays"] = n
				}
			}
			err = call("EnableKeyRotation", body)
		case !creating:
			err = call("DisableKeyRotation", map[string]interface{}{})
		}
		if err != nil {
			return cost, err
		}
	}
	if !creating && slices.Contains(apply, "KeyPolicy") {
		policy := cfnJSONDocument(props["KeyPolicy"], cctx)
		if policy == "" {
			policy = kmsDefaultKeyPolicy(cctx.accountID)
		}
		if err := call("PutKeyPolicy", map[string]interface{}{"PolicyName": kmsDefaultPolicyName, "Policy": policy}); err != nil {
			return cost, err
		}
	}
	if !creating && slices.Contains(apply, "Description") {
		if err := call("UpdateKeyDescription", map[string]interface{}{
			"Description": resolveStringProp(props, "Description", "", cctx),
		}); err != nil {
			return cost, err
		}
	}
	if !enabled && slices.Contains(apply, "Enabled") {
		if err := call("DisableKey", map[string]interface{}{}); err != nil {
			return cost, err
		}
	}
	return cost, nil
}

// updateKMSKey applies a changed AWS::KMS::Key through UpdateKeyDescription, PutKeyPolicy,
// EnableKeyRotation/DisableKeyRotation and EnableKey/DisableKey (see
// [StackDeployer.configureKMSKey]). KeySpec, KeyUsage, MultiRegion and Origin cannot change and
// fail the update (cfnCreateOnlyProperties). PendingWindowInDays and BypassPolicyLockoutSafetyCheck
// make no call: the first governs the delete, and the second only how a policy is checked.
func (d *StackDeployer) updateKMSKey(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	cost, err := d.configureKMSKey(ctx, prior.PhysicalID, props, changed, false, streamID, cctx)
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// ----- AWS::EC2::SecurityGroup (#1550) --------------------------------------------------

// updateEC2SecurityGroup applies a changed AWS::EC2::SecurityGroup's inline SecurityGroupIngress
// and SecurityGroupEgress to the existing group, keeping its ID (#1550). Both are "Some
// interruptions"; GroupName, GroupDescription and VpcId are Replacement and never reach here.
//
// The rules are reconciled against the group's live rules rather than against the previous
// template, so a rollback re-applying the previous declaration restores what the failed update
// changed. For each direction whose property changed: every live rule the declaration does not
// hold is revoked, then every declared rule the group does not hold is authorized — the order the
// page gives: "If you modify a rule, CloudFormation removes the existing rule and then adds a new
// rule." A rule is one source under one protocol and port range, the granularity EC2 itself
// compares at ([ec2AuthorizeDuplicates]), with protocols and ports in [ec2CanonicalPortRange]'s
// form and CIDRs compared in canonical network form, so `10.0.0.1/16` and `10.0.0.0/16` are one
// rule.
//
// A removed SecurityGroupEgress revokes every egress rule and adds none back. The page: "After the
// security group is created, if you remove all egress rules that you added, we do not add egress
// rules, so no outbound traffic is allowed." A removed SecurityGroupIngress likewise leaves no
// inbound rule. A rule whose description alone changed makes no call: substrate does not route
// UpdateSecurityGroupRuleDescriptions*. Tags make no call, as the file comment says.
func (d *StackDeployer) updateEC2SecurityGroup(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !cfnChangedAny(changed, "SecurityGroupIngress", "SecurityGroupEgress") {
		return dr, 0, nil
	}
	group, err := d.loadCFNSecurityGroup(ctx, prior.PhysicalID)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	if group == nil {
		dr.Error = fmt.Sprintf("InvalidGroup.NotFound: the security group %s does not exist", prior.PhysicalID)
		return dr, 0, nil
	}
	var cost float64
	for _, dir := range []struct {
		prop, revoke, authorize string
		live                    []EC2IPPermission
	}{
		{"SecurityGroupIngress", "RevokeSecurityGroupIngress", "AuthorizeSecurityGroupIngress", group.IngressRules},
		{"SecurityGroupEgress", "RevokeSecurityGroupEgress", "AuthorizeSecurityGroupEgress", group.EgressRules},
	} {
		if !slices.Contains(changed, dir.prop) {
			continue
		}
		desired := cfnSGDeclaredPermissions(props[dir.prop], cctx)
		revoke, authorize := cfnSGRuleDiff(dir.live, desired)
		for _, step := range []struct {
			action string
			perms  []EC2IPPermission
		}{{dir.revoke, revoke}, {dir.authorize, authorize}} {
			if len(step.perms) == 0 {
				continue
			}
			params := cfnSGPermissionParams(step.perms)
			params["Action"] = step.action
			params["GroupId"] = prior.PhysicalID
			if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "ec2", Operation: step.action,
				Params: params, Headers: map[string]string{}}, streamID, &cost); routeErr != nil {
				dr.Error = routeErr.Error()
				return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
			}
		}
	}
	return dr, cost, nil
}

// loadCFNSecurityGroup reads a security group's record in the deployer's account and Region, or
// nil when there is none.
func (d *StackDeployer) loadCFNSecurityGroup(ctx context.Context, groupID string) (*EC2SecurityGroup, error) {
	if d.state == nil {
		return nil, nil
	}
	data, err := d.state.Get(ctx, ec2Namespace,
		"sg:"+d.identity.accountID+"/"+d.identity.region+"/"+groupID)
	if err != nil {
		return nil, fmt.Errorf("cfn load security group %s: %w", groupID, err)
	}
	if data == nil {
		return nil, nil
	}
	var sg EC2SecurityGroup
	if err := json.Unmarshal(data, &sg); err != nil {
		return nil, fmt.Errorf("cfn unmarshal security group %s: %w", groupID, err)
	}
	return &sg, nil
}

// cfnSGDeclaredPermissions parses a SecurityGroupIngress or SecurityGroupEgress list into the
// permissions an Authorize request carrying it would store, through the same two halves the
// create uses: [sgRuleParams] renders the request, and EC2's own parseSGPermissions reads it back.
func cfnSGDeclaredPermissions(v interface{}, cctx *cfnContext) []EC2IPPermission {
	rules, _ := v.([]interface{})
	params := map[string]string{}
	n := 0
	for _, raw := range rules {
		rule, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		n++
		for k, val := range sgRuleParams(rule, "IpPermissions."+strconv.Itoa(n)+".", cctx) {
			params[k] = val
		}
	}
	if n == 0 {
		return nil
	}
	return parseSGPermissions(params, cctx.accountID)
}

// cfnSGAtoms splits permissions into one permission per source, the unit EC2 stores a rule at.
// A permission naming no source stays whole.
func cfnSGAtoms(perms []EC2IPPermission) []EC2IPPermission {
	var out []EC2IPPermission
	for _, perm := range perms {
		port := EC2IPPermission{IPProtocol: perm.IPProtocol, FromPort: perm.FromPort, ToPort: perm.ToPort}
		if !ec2PermHasSource(perm) {
			out = append(out, port)
			continue
		}
		for _, c := range perm.IPRanges {
			one := port
			one.IPRanges = []string{c}
			out = append(out, one)
		}
		for _, c := range perm.IPv6Ranges {
			one := port
			one.IPv6Ranges = []string{c}
			out = append(out, one)
		}
		for _, id := range perm.PrefixListIDs {
			one := port
			one.PrefixListIDs = []string{id}
			out = append(out, one)
		}
		for _, pair := range perm.UserIDGroupPairs {
			one := port
			one.UserIDGroupPairs = []EC2UserIDGroupPair{pair}
			out = append(out, one)
		}
	}
	return out
}

// cfnCanonicalCIDR returns a CIDR in canonical network form — host bits cleared, IPv6 compressed —
// or the input unchanged when it does not parse, so an unparseable value still compares against
// itself.
func cfnCanonicalCIDR(cidr string) string {
	p, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil {
		return cidr
	}
	return p.Masked().String()
}

// cfnCanonicalAtoms returns a copy of atoms with every CIDR in [cfnCanonicalCIDR]'s form, for
// comparison only: what is sent keeps the spelling it was stored or declared with.
func cfnCanonicalAtoms(atoms []EC2IPPermission) []EC2IPPermission {
	out := make([]EC2IPPermission, len(atoms))
	for i, a := range atoms {
		out[i] = a
		out[i].IPRanges = make([]string, len(a.IPRanges))
		for j, c := range a.IPRanges {
			out[i].IPRanges[j] = cfnCanonicalCIDR(c)
		}
		out[i].IPv6Ranges = make([]string, len(a.IPv6Ranges))
		for j, c := range a.IPv6Ranges {
			out[i].IPv6Ranges[j] = cfnCanonicalCIDR(c)
		}
	}
	return out
}

// cfnSGRuleDiff returns the live rules the declaration does not hold, which are revoked, and the
// declared rules the group does not hold, which are authorized — each as one permission per source.
// "Holds" is EC2's own duplicate test, [ec2AuthorizeDuplicates], over canonical CIDRs.
func cfnSGRuleDiff(live, desired []EC2IPPermission) (revoke, authorize []EC2IPPermission) {
	liveAtoms, desiredAtoms := cfnSGAtoms(live), cfnSGAtoms(desired)
	liveCanon, desiredCanon := cfnCanonicalAtoms(liveAtoms), cfnCanonicalAtoms(desiredAtoms)
	for i, atom := range liveCanon {
		if !ec2AuthorizeDuplicates(desiredCanon, []EC2IPPermission{atom}) {
			revoke = append(revoke, liveAtoms[i])
		}
	}
	for i, atom := range desiredCanon {
		if !ec2AuthorizeDuplicates(liveCanon, []EC2IPPermission{atom}) {
			authorize = append(authorize, desiredAtoms[i])
		}
	}
	return revoke, authorize
}

// cfnSGPermissionParams renders one-source permissions as the IpPermissions.N.* members an
// Authorize or Revoke request takes.
func cfnSGPermissionParams(perms []EC2IPPermission) map[string]string {
	params := map[string]string{}
	for i, perm := range perms {
		prefix := "IpPermissions." + strconv.Itoa(i+1) + "."
		params[prefix+"IpProtocol"] = perm.IPProtocol
		params[prefix+"FromPort"] = strconv.Itoa(perm.FromPort)
		params[prefix+"ToPort"] = strconv.Itoa(perm.ToPort)
		for j, c := range perm.IPRanges {
			params[fmt.Sprintf("%sIpRanges.%d.CidrIp", prefix, j+1)] = c
		}
		for j, c := range perm.IPv6Ranges {
			params[fmt.Sprintf("%sIpv6Ranges.%d.CidrIpv6", prefix, j+1)] = c
		}
		for j, id := range perm.PrefixListIDs {
			params[fmt.Sprintf("%sPrefixListIds.%d.PrefixListId", prefix, j+1)] = id
		}
		for j, pair := range perm.UserIDGroupPairs {
			gp := fmt.Sprintf("%sGroups.%d.", prefix, j+1)
			if pair.GroupID != "" {
				params[gp+"GroupId"] = pair.GroupID
			} else if pair.GroupName != "" {
				params[gp+"GroupName"] = pair.GroupName
			}
			if pair.UserID != "" {
				params[gp+"UserId"] = pair.UserID
			}
			if pair.Description != "" {
				params[gp+"Description"] = pair.Description
			}
		}
	}
	return params
}

// ----- AWS::ECS::Service ----------------------------------------------------------------

// cfnECSDesiredCount is the service's DesiredCount, or the 1 the create has always sent for an
// undeclared one.
func cfnECSDesiredCount(props map[string]interface{}, cctx *cfnContext) int {
	if v := resolveStringProp(props, "DesiredCount", "", cctx); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1
}

// updateECSService applies a changed AWS::ECS::Service's TaskDefinition and DesiredCount through
// UpdateService, keeping the service. Cluster, ServiceName, Role and SchedulingStrategy are
// Replacement; LaunchType is in cfnUpdateUnmodelledProperties. A removed DesiredCount returns to
// the create's 1.
func (d *StackDeployer) updateECSService(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !cfnChangedAny(changed, "TaskDefinition", "DesiredCount") {
		return dr, 0, nil
	}
	body := map[string]interface{}{
		"cluster": resolveStringProp(props, "Cluster", "default", cctx),
		"service": prior.PhysicalID,
	}
	if slices.Contains(changed, "TaskDefinition") {
		body["taskDefinition"] = resolveStringProp(props, "TaskDefinition", "", cctx)
	}
	if slices.Contains(changed, "DesiredCount") {
		body["desiredCount"] = cfnECSDesiredCount(props, cctx)
	}
	req, err := cfnJSONRequest("ecs", "UpdateService", body)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	req.Headers = map[string]string{"x-amz-target": "AmazonEC2ContainerServiceV20141113.UpdateService"}
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}
