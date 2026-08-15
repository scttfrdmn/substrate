package emulator

// cfn_resources_v101.go holds the StackDeployer.deployResource helpers for AWS
// Config. The name records the substrate release that added them (v0.101.0) rather
// than the service, because several releases touched overlapping services; the
// helpers here follow the same pattern as those in cfn_resources_v77.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// This file replaces the AWS::Config::ConfigRule and
// AWS::Config::ConfigurationRecorder stubs that shipped in v0.32.0, and wires
// AWS::Config::DeliveryChannel, which fell through to deployGenericStub. All three
// reported CREATE_COMPLETE while creating nothing, and nothing observable
// contradicted them — the #388 class, fixable only now that the Config API handlers
// exist (#580).
//
// AWS::Config::DeliveryChannel is here despite the release plan scoping it out. The
// plan's reason for excluding it — "a stub is the defect this lane exists to remove"
// — is exactly the argument for including it: the type was *already* a de-facto stub
// through deployGenericStub, so leaving it out would have preserved the defect for
// one of the three siblings. It is also the only way the recorder's documented
// CloudFormation behavior is reachable at all: "AWS CloudFormation starts the
// recorder as soon as the delivery channel is available", and a recorder cannot be
// started without a channel.
//
// Two divergences from the Config API model are deliberate and load-bearing:
//
//   - **CloudFormation requires properties the API model marks optional.** RoleARN on
//     the recorder and S3BucketName on the channel are *Required: Yes* on their
//     CloudFormation pages while the API model requires neither. Real CloudFormation
//     refuses such a template before it calls Config at all, so substrate refuses at
//     the CloudFormation layer too rather than letting the plugin answer
//     InvalidRoleException — the caller's stack event then names the property
//     CloudFormation would have named.
//   - **CloudFormation spells the recorder's and channel's nested members
//     UpperCamel; the API spells them lowerCamel.** RecordingGroup.AllSupported is
//     recordingGroup.allSupported on the wire, and so on through RecordingMode and
//     the whole DeliveryChannel shape. Real Config is case-sensitive and would
//     silently ignore the UpperCamel members: the deploy would succeed, the recorder
//     would record AWS's default group, and nothing would report that the template's
//     group had been discarded. Substrate itself would *not* show that, because its
//     handlers decode with encoding/json, whose field matching is case-insensitive —
//     which is precisely why the keys are translated here rather than forwarded. The
//     request body substrate records is what an exported event log replays against
//     AWS, so a body that only works against a lenient decoder is a fixture that
//     passes here and fails there. Pinned by
//     TestCFN_ConfigRecorderWireKeysAreLowerCamel, which asserts the emitted keys
//     directly for that reason. The ConfigRule shape is UpperCamel in both and needs
//     no translation — only a whitelist, because CloudFormation defines a Compliance
//     property that PutConfigRule's input does not have.

// cfgsvcJSONContentType is the content type Config's JSON 1.1 protocol uses. It is
// set on every dispatched request so a refusal is encoded the way the service would
// encode it rather than falling back to a protocol default.
const cfgsvcJSONContentType = "application/x-amz-json-1.1"

// deployConfigConfigurationRecorder creates the account's AWS Config configuration
// recorder.
//
// Ref returns the recorder name — "the configuration recorder name, such as
// default" — and the Fn::GetAtt section of the resource's page is empty, so no
// attributes are exposed. Name is optional because "AWS Config automatically assigns
// the name of 'default'", which is also why the type is absent from
// cfnGeneratedNameTypes: a generated stack-scoped name would replace a name AWS
// itself assigns, and only one recorder may exist per account per Region anyway.
//
// The recorder is created stopped. Starting it is the delivery channel's job, per
// the page's own "AWS CloudFormation starts the recorder as soon as the delivery
// channel is available" — see deployConfigDeliveryChannel.
func (d *StackDeployer) deployConfigConfigurationRecorder(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	const resType = "AWS::Config::ConfigurationRecorder"
	name := resolveStringProp(props, "Name", cfgsvcDefaultName, cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: resType, PhysicalID: name}

	roleARN := resolveStringProp(props, "RoleARN", "", cctx)
	if roleARN == "" {
		// CloudFormation's own requirement, not Config's — see the file header.
		dr.Error = "RoleARN is required by AWS::Config::ConfigurationRecorder"
		return dr, 0, nil
	}

	recorder := map[string]interface{}{"name": name, "roleARN": roleARN}
	if group := cfgsvcCFNRecordingGroup(props["RecordingGroup"], cctx); group != nil {
		recorder["recordingGroup"] = group
	}
	if mode := cfgsvcCFNRecordingMode(props["RecordingMode"], cctx); mode != nil {
		recorder["recordingMode"] = mode
	}

	body, err := json.Marshal(map[string]interface{}{"ConfigurationRecorder": recorder})
	if err != nil {
		return dr, 0, fmt.Errorf("marshal configuration recorder body: %w", err)
	}

	_, cost, routeErr := d.dispatch(ctx, cfgsvcCFNRequest("PutConfigurationRecorder", body), streamID)
	if routeErr != nil {
		// Recorded on the resource, not returned — a returned error aborts the stack.
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}

	// The ARN is read back rather than rebuilt here. Its second component is a
	// RecorderId that no member of the API model carries, so substrate mints it
	// (cfgsvcMintRecorderID); recomputing that in the CloudFormation layer would give
	// the derivation two homes that could drift apart. The v0.32.0 stub's
	// recorder/<name> was wrong on both counts — the segment is
	// configuration-recorder and it takes a name *and* an ID.
	arnCost, arn := d.cfgsvcCFNRecorderARN(ctx, name, streamID)
	dr.ARN = arn
	return dr, cost + arnCost, nil
}

// cfgsvcCFNRecorderARN reads the recorder's ARN back through
// DescribeConfigurationRecorders. An unreadable response leaves the ARN empty rather
// than synthesizing one, because an ARN substrate guessed is worse than none: a
// policy written against the real form would not match it, so a test asserting a
// denial would pass for the wrong reason.
func (d *StackDeployer) cfgsvcCFNRecorderARN(
	ctx context.Context, name, streamID string,
) (float64, string) {
	body, err := json.Marshal(map[string]interface{}{"ConfigurationRecorderNames": []string{name}})
	if err != nil {
		return 0, ""
	}
	resp, cost, routeErr := d.dispatch(ctx,
		cfgsvcCFNRequest("DescribeConfigurationRecorders", body), streamID)
	if routeErr != nil || resp == nil {
		return cost, ""
	}
	var out struct {
		ConfigurationRecorders []struct {
			ARN string `json:"arn"`
		} `json:"ConfigurationRecorders"`
	}
	if jsonErr := json.Unmarshal(resp.Body, &out); jsonErr != nil || len(out.ConfigurationRecorders) == 0 {
		return cost, ""
	}
	return cost, out.ConfigurationRecorders[0].ARN
}

// deployConfigDeliveryChannel creates the account's AWS Config delivery channel, and
// starts the stack's configuration recorder once it exists.
//
// Ref returns the channel name and the Fn::GetAtt section of the page is empty, so
// no attributes are exposed. The resource's ARN is left **empty**, which is not an
// omission: the Service Authorization Reference defines ten Config resource types and
// none is a delivery channel, the DeliveryChannel shape has no arn member to carry
// one, and TagResource does not accept a channel. Synthesizing one would be an
// invention with nothing behind it.
//
// Two ordering facts from the page are modeled by typePriority rather than by
// DependsOn — which substrate parses and does not act on — so a template need not
// declare them: "Before you can create a delivery channel, you must create a
// configuration recorder" (recorder 2, channel 3), and the channel must exist before
// a rule is put (rule 4).
//
// A caveat a fixture author needs: a real PutDeliveryChannel refuses a bucket whose
// policy does not admit Config (InsufficientDeliveryPolicyException), and substrate
// has no AWS::S3::BucketPolicy resource type, so a template *cannot* express the
// policy the check wants. Seeding the outcome — POST /v1/config/delivery-policy/{bucket}
// with {"outcome":"ok"} — is the only way a CloudFormation fixture passes it.
func (d *StackDeployer) deployConfigDeliveryChannel(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	const resType = "AWS::Config::DeliveryChannel"
	name := resolveStringProp(props, "Name", cfgsvcDefaultName, cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: resType, PhysicalID: name}

	bucket := resolveStringProp(props, "S3BucketName", "", cctx)
	if bucket == "" {
		// CloudFormation's own requirement, not Config's — see the file header.
		dr.Error = "S3BucketName is required by AWS::Config::DeliveryChannel"
		return dr, 0, nil
	}

	channel := map[string]interface{}{"name": name, "s3BucketName": bucket}
	for cfnKey, wireKey := range map[string]string{
		"S3KeyPrefix": "s3KeyPrefix",
		"S3KmsKeyArn": "s3KmsKeyArn",
		"SnsTopicARN": "snsTopicARN",
	} {
		if v := resolveStringProp(props, cfnKey, "", cctx); v != "" {
			channel[wireKey] = v
		}
	}
	if snapshot, ok := resolveNested(props["ConfigSnapshotDeliveryProperties"], cctx).(map[string]interface{}); ok {
		if freq := cfgsvcCFNString(snapshot, "DeliveryFrequency"); freq != "" {
			channel["configSnapshotDeliveryProperties"] = map[string]interface{}{"deliveryFrequency": freq}
		}
	}

	body, err := json.Marshal(map[string]interface{}{"DeliveryChannel": channel})
	if err != nil {
		return dr, 0, fmt.Errorf("marshal delivery channel body: %w", err)
	}

	_, cost, routeErr := d.dispatch(ctx, cfgsvcCFNRequest("PutDeliveryChannel", body), streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}

	startCost, startErr := d.cfgsvcCFNStartRecorder(ctx, cctx, streamID)
	if startErr != "" && dr.Error == "" {
		dr.Error = startErr
	}
	return dr, cost + startCost, nil
}

// cfgsvcCFNStartRecorder starts the recorder declared in the same stack, which is
// what makes DescribeConfigurationRecorderStatus report recording: true for a
// template carrying both a recorder and a channel, and false for one carrying only a
// recorder.
//
// Only a *sibling* recorder is started. A recorder that some other stack or a direct
// API call created is deliberately left alone: real CloudFormation starts the
// recorder it manages, and reaching outside the stack to change the state of a
// resource this stack does not own would make a delivery channel a covert switch on
// somebody else's recorder — and the teardown could not undo it, since the sweep
// finds the recorder to stop the same way.
func (d *StackDeployer) cfgsvcCFNStartRecorder(
	ctx context.Context, cctx *cfnContext, streamID string,
) (float64, string) {
	name, ok := cfgsvcCFNRecorderSibling(cctx)
	if !ok {
		return 0, ""
	}
	body, err := json.Marshal(map[string]interface{}{"ConfigurationRecorderName": name})
	if err != nil {
		return 0, ""
	}
	_, cost, routeErr := d.dispatch(ctx,
		cfgsvcCFNRequest("StartConfigurationRecorder", body), streamID)
	if routeErr != nil {
		return cost, routeErr.Error()
	}
	return cost, ""
}

// cfgsvcCFNRecorderSibling returns the physical name of the configuration recorder
// deployed by the same stack, if it deployed successfully.
//
// The logical IDs are sorted before the scan so the answer does not depend on Go's
// map iteration order. Only one recorder can exist per account and Region, so a
// second one in the same template was refused and carries an Error — but sorting
// costs nothing and keeps a replay identical to the run it replays, which is the
// property the whole emulator rests on.
func cfgsvcCFNRecorderSibling(cctx *cfnContext) (string, bool) {
	if cctx == nil {
		return "", false
	}
	ids := make([]string, 0, len(cctx.resources))
	for id := range cctx.resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		dr := cctx.resources[id]
		if dr.Type == "AWS::Config::ConfigurationRecorder" && dr.Error == "" && dr.PhysicalID != "" {
			return dr.PhysicalID, true
		}
	}
	return "", false
}

// deployConfigConfigRule creates an AWS Config rule.
//
// Ref returns the rule name, "such as mystack-MyConfigRule-12ABCFPXHV4OV" — the
// shape cfnGeneratedName produces — because "if you don't specify a name,
// CloudFormation generates a unique physical ID and uses that ID for the rule name".
// That is why AWS::Config::ConfigRule is in cfnGeneratedNameTypes where the recorder
// and the channel are not: those two are named "default" by AWS itself.
//
// Unlike the recorder and the channel, this type does expose Fn::GetAtt attributes —
// Arn, ConfigRuleId and Compliance.Type — and all three are read back from the
// service rather than derived here, so the ARN and the ID cannot disagree with what
// DescribeConfigRules reports.
func (d *StackDeployer) deployConfigConfigRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	const resType = "AWS::Config::ConfigRule"
	name := resolveStringProp(props, "ConfigRuleName",
		cfnGeneratedName(cctx, resType, logicalID), cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: resType, PhysicalID: name}

	rule := map[string]interface{}{"ConfigRuleName": name}
	// Source is the only Required: Yes property on the resource's page, and it is the
	// only required member of the API model's ConfigRule shape too — the one place the
	// two agree on a requirement.
	source, ok := resolveNested(props["Source"], cctx).(map[string]interface{})
	if !ok || len(source) == 0 {
		dr.Error = "Source is required by AWS::Config::ConfigRule"
		return dr, 0, nil
	}
	rule["Source"] = source

	// A whitelist rather than a pass-through. The ConfigRule shape's members are
	// UpperCamel in both the template and the API, so forwarding *looks* safe — but
	// CloudFormation defines a Compliance property that PutConfigRule's input has no
	// member for, and the model's ConfigRuleArn/ConfigRuleId are refused outright on a
	// create ("These values are generated by Config for new rules"), so a template
	// echoing a described rule back would be refused rather than deployed.
	for _, key := range []string{"Description", "MaximumExecutionFrequency"} {
		if v := resolveStringProp(props, key, "", cctx); v != "" {
			rule[key] = v
		}
	}
	for _, key := range []string{"Scope", "EvaluationModes"} {
		if v := resolveNested(props[key], cctx); v != nil {
			rule[key] = v
		}
	}
	if params := cfgsvcCFNInputParameters(props["InputParameters"], cctx); params != "" {
		rule["InputParameters"] = params
	}

	body, err := json.Marshal(map[string]interface{}{"ConfigRule": rule})
	if err != nil {
		return dr, 0, fmt.Errorf("marshal config rule body: %w", err)
	}

	_, cost, routeErr := d.dispatch(ctx, cfgsvcCFNRequest("PutConfigRule", body), streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}

	attrCost := d.cfgsvcCFNRuleAttributes(ctx, &dr, name, streamID)
	return dr, cost + attrCost, nil
}

// cfgsvcCFNInputParameters renders the rule's InputParameters as the JSON *string*
// the API model wants.
//
// The resource's page shows both spellings: its JSON example supplies a JSON object
// and its YAML example a string. Both are accepted here, because a template author
// following either example is following the documentation.
func cfgsvcCFNInputParameters(v interface{}, cctx *cfnContext) string {
	resolved := resolveNested(v, cctx)
	switch typed := resolved.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(encoded)
	}
}

// cfgsvcCFNRuleAttributes fills in the rule's ARN and its two Metadata-borne GetAtt
// attributes from the service's own answers.
//
// Compliance.Type comes from DescribeComplianceByConfigRule, so a compliance value
// seeded before the deploy is what an Output reading it reports — which is the point:
// compliance is seed-only, and a stack whose Output asserts a rule's compliance is
// asserting against the seed the fixture set. An unreadable answer leaves the
// attribute unset rather than defaulting to a verdict, because INSUFFICIENT_DATA
// invented here would be indistinguishable from INSUFFICIENT_DATA observed.
func (d *StackDeployer) cfgsvcCFNRuleAttributes(
	ctx context.Context, dr *DeployedResource, name, streamID string,
) float64 {
	var total float64

	if body, err := json.Marshal(map[string]interface{}{"ConfigRuleNames": []string{name}}); err == nil {
		resp, cost, routeErr := d.dispatch(ctx, cfgsvcCFNRequest("DescribeConfigRules", body), streamID)
		total += cost
		if routeErr == nil && resp != nil {
			var out struct {
				ConfigRules []struct {
					ConfigRuleArn string `json:"ConfigRuleArn"`
					ConfigRuleID  string `json:"ConfigRuleId"`
				} `json:"ConfigRules"`
			}
			if json.Unmarshal(resp.Body, &out) == nil && len(out.ConfigRules) > 0 {
				dr.ARN = out.ConfigRules[0].ConfigRuleArn
				cfnSetMetadata(dr, "ConfigRuleId", out.ConfigRules[0].ConfigRuleID)
			}
		}
	}

	if body, err := json.Marshal(map[string]interface{}{"ConfigRuleNames": []string{name}}); err == nil {
		resp, cost, routeErr := d.dispatch(ctx,
			cfgsvcCFNRequest("DescribeComplianceByConfigRule", body), streamID)
		total += cost
		if routeErr == nil && resp != nil {
			var out struct {
				ComplianceByConfigRules []struct {
					Compliance struct {
						ComplianceType string `json:"ComplianceType"`
					} `json:"Compliance"`
				} `json:"ComplianceByConfigRules"`
			}
			if json.Unmarshal(resp.Body, &out) == nil && len(out.ComplianceByConfigRules) > 0 {
				cfnSetMetadata(dr, "Compliance.Type",
					out.ComplianceByConfigRules[0].Compliance.ComplianceType)
			}
		}
	}

	return total
}

// cfnSetMetadata records a GetAtt-resolvable attribute on a resource, allocating the
// map on first use and ignoring an empty value so an absent attribute stays absent
// rather than resolving to "".
func cfnSetMetadata(dr *DeployedResource, key, value string) {
	if value == "" {
		return
	}
	if dr.Metadata == nil {
		dr.Metadata = map[string]interface{}{}
	}
	dr.Metadata[key] = value
}

// cfgsvcCFNRequest builds a Config API request for a dispatched CloudFormation
// resource.
func cfgsvcCFNRequest(operation string, body []byte) *AWSRequest {
	return &AWSRequest{
		Service:   configServiceNamespace,
		Operation: operation,
		Body:      body,
		Headers:   map[string]string{"Content-Type": cfgsvcJSONContentType},
		Params:    map[string]string{},
	}
}

// cfgsvcCFNRecordingGroup translates a template's RecordingGroup into the API
// model's recordingGroup.
//
// Every member is renamed, and that is the whole reason this function exists — see
// the file header on the case asymmetry. A nil result means the template supplied no
// group, which PutConfigurationRecorder answers with AWS's default group; an empty
// map would instead be a group with every parameter unset, which the reference
// enumerates as an InvalidRecordingGroupException case.
func cfgsvcCFNRecordingGroup(v interface{}, cctx *cfnContext) map[string]interface{} {
	src, ok := resolveNested(v, cctx).(map[string]interface{})
	if !ok || len(src) == 0 {
		return nil
	}
	group := map[string]interface{}{}
	for cfnKey, wireKey := range map[string]string{
		"AllSupported":               "allSupported",
		"IncludeGlobalResourceTypes": "includeGlobalResourceTypes",
	} {
		if b, set := cfgsvcCFNBool(src, cfnKey); set {
			group[wireKey] = b
		}
	}
	if types := cfgsvcCFNStringList(src["ResourceTypes"]); len(types) > 0 {
		group["resourceTypes"] = types
	}
	if excl, isMap := src["ExclusionByResourceTypes"].(map[string]interface{}); isMap {
		if types := cfgsvcCFNStringList(excl["ResourceTypes"]); len(types) > 0 {
			group["exclusionByResourceTypes"] = map[string]interface{}{"resourceTypes": types}
		}
	}
	if strategy, isMap := src["RecordingStrategy"].(map[string]interface{}); isMap {
		if useOnly := cfgsvcCFNString(strategy, "UseOnly"); useOnly != "" {
			group["recordingStrategy"] = map[string]interface{}{"useOnly": useOnly}
		}
	}
	if len(group) == 0 {
		return nil
	}
	return group
}

// cfgsvcCFNRecordingMode translates a template's RecordingMode into the API model's
// recordingMode.
//
// RecordingFrequency is Required: Yes within RecordingMode, so a mode without one is
// dropped rather than sent: the request would be refused, and dropping it leaves the
// recorder on the default frequency, which is what a template that failed to say
// otherwise asked for. RecordingModeOverrides accepts at most one member, and each
// override requires both ResourceTypes and RecordingFrequency.
func cfgsvcCFNRecordingMode(v interface{}, cctx *cfnContext) map[string]interface{} {
	src, ok := resolveNested(v, cctx).(map[string]interface{})
	if !ok {
		return nil
	}
	frequency := cfgsvcCFNString(src, "RecordingFrequency")
	if frequency == "" {
		return nil
	}
	mode := map[string]interface{}{"recordingFrequency": frequency}

	rawOverrides, isList := src["RecordingModeOverrides"].([]interface{})
	if !isList {
		return mode
	}
	overrides := make([]map[string]interface{}, 0, len(rawOverrides))
	for _, raw := range rawOverrides {
		item, isMap := raw.(map[string]interface{})
		if !isMap {
			continue
		}
		types := cfgsvcCFNStringList(item["ResourceTypes"])
		itemFrequency := cfgsvcCFNString(item, "RecordingFrequency")
		if len(types) == 0 || itemFrequency == "" {
			continue
		}
		override := map[string]interface{}{
			"resourceTypes":      types,
			"recordingFrequency": itemFrequency,
		}
		if desc := cfgsvcCFNString(item, "Description"); desc != "" {
			override["description"] = desc
		}
		overrides = append(overrides, override)
	}
	if len(overrides) > 0 {
		mode["recordingModeOverrides"] = overrides
	}
	return mode
}

// cfgsvcCFNString reads a string member out of an already-resolved property map.
func cfgsvcCFNString(src map[string]interface{}, key string) string {
	s, _ := src[key].(string)
	return s
}

// cfgsvcCFNStringList reads a list-of-strings member out of an already-resolved
// property map, dropping non-string and empty members.
func cfgsvcCFNStringList(v interface{}) []string {
	items, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, isString := item.(string); isString && s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cfgsvcCFNBool reads a boolean member out of an already-resolved property map,
// reporting whether the template set it at all.
//
// Both spellings are accepted because both reach here: a JSON template's `true` is a
// Go bool, while a YAML template's and an Fn::If's or Ref's resolution is the string
// "true". Sending allSupported: false for a member the template never mentioned would
// be substrate asserting a choice the author did not make, which the recording-group
// validation treats as a distinct — and refusable — configuration.
func cfgsvcCFNBool(src map[string]interface{}, key string) (value, set bool) {
	switch typed := src[key].(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(typed)
		if err != nil {
			return false, false
		}
		return parsed, true
	}
	return false, false
}
