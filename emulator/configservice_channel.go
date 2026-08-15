package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// The delivery-channel cluster, and the S3 bucket-policy check (#580).
//
// The channel ships alongside the recorder rather than after it, because
// StartConfigurationRecorder refuses without a channel: a recorder-only release
// could never reach `recording: true`, which is the behavior the whole cluster
// exists to make observable.
//
// # The delivery-policy check
//
// PutDeliveryChannel is the one Config operation whose success depends on state in
// *another* service: the bucket must exist and its policy must let Config write.
// AWS reports the two failures separately, as NoSuchBucketException and
// InsufficientDeliveryPolicyException, and substrate computes both from real S3
// state rather than accepting unconditionally.
//
// The matcher is deliberately **permissive**. The two ways to get this wrong are not
// symmetric:
//
//   - Always accepting would make a consumer's bucket-policy bug invisible here and
//     fatal at AWS — the emulator would have said yes to the one thing it was asked
//     to check.
//   - Demanding the exact policy from the AWS documentation would refuse policies
//     AWS accepts, and a wrong refusal breaks working consumer code. That is the
//     worse failure, and it is the one a strict matcher produces every time a
//     consumer writes a correct policy in a form substrate's parser did not expect.
//
// So: a bucket with no policy at all is refused, since that is unambiguous and is
// the mistake consumers actually make. A bucket *with* a policy passes if any Allow
// statement plausibly admits Config — and an unparseable policy passes too, because
// refusing on substrate's own parser would be substrate blaming the consumer for its
// own limitation. Resource ARNs are not matched at all; getting the prefix wrong is
// a real bug, but it is not one this check can distinguish from a valid variant.
//
// A consumer that has no S3 fixture, or that needs the refusal on demand, seeds the
// outcome instead — see configservice_control.go.

// cfgsvcMaxChannels is the number of delivery channels AWS permits per account per
// Region, from PutDeliveryChannel's own "you can have only one" note rather than
// from the service-limits page, which lists no channel maximum.
const cfgsvcMaxChannels = 1

// cfgsvcConfigServicePrincipal is the service principal a bucket policy must admit
// for Config to deliver to it.
const cfgsvcConfigServicePrincipal = "config.amazonaws.com"

// ConfigDeliveryChannel is a stored delivery channel — the DeliveryChannel shape.
// Its members are lowerCamel on the wire, like ConfigurationRecorder's.
type ConfigDeliveryChannel struct {
	// Name is the channel name, "default" when the request omitted one.
	Name string `json:"name"`

	// S3BucketName is the bucket configuration snapshots are delivered to.
	S3BucketName string `json:"s3BucketName,omitempty"`

	// S3KeyPrefix is the prefix within that bucket.
	S3KeyPrefix string `json:"s3KeyPrefix,omitempty"`

	// S3KmsKeyArn is the KMS key used to encrypt delivered objects.
	S3KmsKeyArn string `json:"s3KmsKeyArn,omitempty"`

	// SnsTopicARN is the topic configuration change notifications go to.
	SnsTopicARN string `json:"snsTopicARN,omitempty"`

	// ConfigSnapshotDeliveryProperties is the snapshot delivery frequency.
	ConfigSnapshotDeliveryProperties json.RawMessage `json:"configSnapshotDeliveryProperties,omitempty"`
}

// cfgsvcPutChannelRequest is PutDeliveryChannelRequest.
type cfgsvcPutChannelRequest struct {
	// DeliveryChannel is the channel to create or update, required.
	DeliveryChannel *ConfigDeliveryChannel `json:"DeliveryChannel"`
}

// cfgsvcDescribeChannelsRequest is the input shared by DescribeDeliveryChannels and
// DescribeDeliveryChannelStatus: a name list, and no pagination.
type cfgsvcDescribeChannelsRequest struct {
	// DeliveryChannelNames selects channels by name.
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}

// cfgsvcChannelNameRequest is the input to DeleteDeliveryChannel.
type cfgsvcChannelNameRequest struct {
	// DeliveryChannelName names the channel, required.
	DeliveryChannelName string `json:"DeliveryChannelName"`
}

// channelOperation claims the delivery-channel operations.
func (p *ConfigServicePlugin) channelOperation(op string) (cfgsvcHandler, bool) {
	switch op {
	case "PutDeliveryChannel":
		return p.putDeliveryChannel, true
	case "DescribeDeliveryChannels":
		return p.describeDeliveryChannels, true
	case "DescribeDeliveryChannelStatus":
		return p.describeDeliveryChannelStatus, true
	case "DeleteDeliveryChannel":
		return p.deleteDeliveryChannel, true
	}
	return nil, false
}

// putDeliveryChannel creates or updates the account's delivery channel.
//
// The checks run in the order AWS documents them, and the order is observable: a
// consumer with neither a recorder nor a bucket gets
// NoAvailableConfigurationRecorderException, not NoSuchBucketException, so the first
// thing it is told to fix is the first thing it must fix.
func (p *ConfigServicePlugin) putDeliveryChannel(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPutChannelRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.DeliveryChannel == nil {
		return nil, cfgsvcValidation("DeliveryChannel is required.")
	}

	channel := *in.DeliveryChannel
	if channel.Name == "" {
		channel.Name = cfgsvcDefaultName
	}
	if len(channel.Name) > cfgsvcMaxNameLen {
		return nil, cfgsvcValidation("The delivery channel name must be between 1 and 256 characters long.")
	}

	goCtx := context.Background()
	var recorder ConfigRecorder
	hasRecorder, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return nil, err
	}
	if !hasRecorder {
		return nil, cfgsvcNoAvailableRecorder()
	}

	if err := p.cfgsvcCheckDeliveryBucket(goCtx, channel.S3BucketName); err != nil {
		return nil, err
	}

	key := cfgsvcChannelKey(ctx.AccountID, ctx.Region)
	var existing ConfigDeliveryChannel
	found, err := p.cfgsvcGetJSON(goCtx, key, &existing)
	if err != nil {
		return nil, err
	}
	if found && existing.Name != channel.Name {
		return nil, cfgsvcErr("MaxNumberOfDeliveryChannelsExceededException",
			"You have reached the limit of the number of delivery channels you can create.")
	}
	if err := p.cfgsvcPutJSON(goCtx, key, channel); err != nil {
		return nil, err
	}
	return cfgsvcEmptyResponse(), nil
}

// cfgsvcNoAvailableRecorder is NoAvailableConfigurationRecorderException, which both
// PutDeliveryChannel and PutConfigRule report. #580 omitted it; the model and the
// reference both carry it, and it is what makes a consumer's ordering bug visible.
func cfgsvcNoAvailableRecorder() *AWSError {
	return cfgsvcErr("NoAvailableConfigurationRecorderException",
		"There are no customer managed configuration recorders available to record your "+
			"resources. Use the PutConfigurationRecorder operation to create the customer "+
			"managed configuration recorder.")
}

// cfgsvcCheckDeliveryBucket checks the delivery bucket against real S3 state, or
// against a seeded outcome if one is set.
//
// A channel with no bucket at all is accepted: DeliveryS3Bucket is min-length 0 in
// the model and the operation does not require it, so refusing one would refuse a
// request AWS accepts.
func (p *ConfigServicePlugin) cfgsvcCheckDeliveryBucket(goCtx context.Context, bucket string) error {
	if bucket == "" {
		return nil
	}

	// The seed is consulted first and is absolute in both directions: a consumer with
	// no S3 fixture needs "insufficient" without a bucket, and a consumer whose policy
	// substrate's matcher cannot read needs "ok" without arguing with it.
	outcome, seeded, err := p.seededDeliveryPolicy(goCtx, bucket)
	if err != nil {
		return err
	}
	if seeded {
		if outcome == cfgsvcDeliveryOutcomeInsufficient {
			return cfgsvcInsufficientDeliveryPolicy()
		}
		return nil
	}

	// S3 state is read straight out of the s3 namespace. authz.go already reads five
	// namespaces from one controller, so a plugin reading another service's state is
	// established practice rather than a new coupling — and the alternative, an
	// internal S3 API call, would put an authorization decision in the middle of a
	// validity check.
	bucketData, err := p.state.Get(goCtx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return cfgsvcValidation("could not read bucket state: " + err.Error())
	}
	if bucketData == nil {
		return cfgsvcErr("NoSuchBucketException", "The specified Amazon S3 bucket does not exist.")
	}

	policyData, err := p.state.Get(goCtx, s3Namespace, "bucket_policy:"+bucket)
	if err != nil {
		return cfgsvcValidation("could not read bucket policy: " + err.Error())
	}
	if policyData == nil {
		return cfgsvcInsufficientDeliveryPolicy()
	}
	if !cfgsvcPolicyAdmitsConfig(policyData) {
		return cfgsvcInsufficientDeliveryPolicy()
	}
	return nil
}

// cfgsvcInsufficientDeliveryPolicy is InsufficientDeliveryPolicyException.
func cfgsvcInsufficientDeliveryPolicy() *AWSError {
	return cfgsvcErr("InsufficientDeliveryPolicyException",
		"Your Amazon S3 bucket policy does not allow Config to write to it.")
}

// cfgsvcPolicyAdmitsConfig reports whether a bucket policy plausibly lets Config
// deliver to the bucket.
//
// Permissive by design, per the file header. Anything it cannot read passes, because
// a refusal traceable to substrate's parser rather than to the policy would be
// substrate blaming the consumer for its own limitation. What it does check is the
// pairing a broken policy actually gets wrong: an Allow whose principal covers
// Config and whose action covers PutObject.
func cfgsvcPolicyAdmitsConfig(raw []byte) bool {
	// The stored value is an S3BucketPolicy wrapper holding the document as a string;
	// a document written directly into state is accepted too, so a fixture that seeded
	// one form does not silently fail the check.
	document := raw
	var wrapper S3BucketPolicy
	if err := json.Unmarshal(raw, &wrapper); err == nil && wrapper.Policy != "" {
		document = []byte(wrapper.Policy)
	}

	var policy struct {
		Statement []struct {
			Effect    string          `json:"Effect"`
			Principal json.RawMessage `json:"Principal"`
			Action    json.RawMessage `json:"Action"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal(document, &policy); err != nil {
		return true
	}
	// A document that parsed but carries no statements is not a policy that admits
	// anything, and an empty Statement array is a mistake rather than a parser
	// limitation — so unlike an unreadable document, this one is refused.
	if len(policy.Statement) == 0 {
		return false
	}

	for _, stmt := range policy.Statement {
		if !strings.EqualFold(stmt.Effect, "Allow") {
			continue
		}
		if cfgsvcPrincipalCoversConfig(stmt.Principal) && cfgsvcActionCoversPutObject(stmt.Action) {
			return true
		}
	}
	return false
}

// cfgsvcPrincipalCoversConfig reports whether a statement's Principal covers the
// Config service principal.
//
// A Principal is "*", a map of principal types to one-or-many values, or (in a
// hand-written fixture) a bare string. All three are accepted; anything unreadable
// is treated as covering, per the permissive rule.
func cfgsvcPrincipalCoversConfig(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return str == "*" || strings.Contains(str, cfgsvcConfigServicePrincipal)
	}
	var typed map[string]json.RawMessage
	if err := json.Unmarshal(raw, &typed); err != nil {
		return true
	}
	for _, value := range typed {
		for _, entry := range cfgsvcStringOrSlice(value) {
			if entry == "*" || strings.Contains(entry, cfgsvcConfigServicePrincipal) {
				return true
			}
		}
	}
	return false
}

// cfgsvcActionCoversPutObject reports whether a statement's Action covers writing an
// object: s3:PutObject exactly, or any of the wildcards that subsume it.
func cfgsvcActionCoversPutObject(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	for _, action := range cfgsvcStringOrSlice(raw) {
		lower := strings.ToLower(strings.TrimSpace(action))
		if lower == "*" || lower == "s3:*" || lower == "s3:putobject" {
			return true
		}
		// A prefix wildcard such as s3:Put* covers PutObject. Matching the prefix rather
		// than expanding the wildcard keeps this readable and errs toward accepting.
		if strings.HasSuffix(lower, "*") && strings.HasPrefix("s3:putobject", strings.TrimSuffix(lower, "*")) {
			return true
		}
	}
	return false
}

// cfgsvcStringOrSlice decodes an IAM policy field that may be a single string or a
// list of them, which is the shape of nearly every member of a policy document.
func cfgsvcStringOrSlice(raw json.RawMessage) []string {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many
	}
	return nil
}

// describeDeliveryChannels reports the account's delivery channel.
func (p *ConfigServicePlugin) describeDeliveryChannels(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	channel, found, err := p.cfgsvcResolveDescribedChannel(ctx, req)
	if err != nil {
		return nil, err
	}
	channels := []ConfigDeliveryChannel{}
	if found {
		channels = append(channels, *channel)
	}
	return cfgsvcJSONResponse(map[string]interface{}{"DeliveryChannels": channels},
		"describeDeliveryChannels")
}

// describeDeliveryChannelStatus reports how delivery is going.
//
// Before the recorder has ever started there is nothing to deliver, so every stream
// reports Not_Applicable — note the underscore and the capital A, which is how the
// DeliveryStatus enum spells this member and only this member. After a start they
// report Success, and a seed can pin Failure with an error code and message so a
// consumer's delivery-failure branch is reachable.
func (p *ConfigServicePlugin) describeDeliveryChannelStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	channel, found, err := p.cfgsvcResolveDescribedChannel(ctx, req)
	if err != nil {
		return nil, err
	}
	statuses := []map[string]interface{}{}
	if found {
		status, err := p.cfgsvcChannelStatus(ctx, channel)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return cfgsvcJSONResponse(map[string]interface{}{"DeliveryChannelsStatus": statuses},
		"describeDeliveryChannelStatus")
}

// cfgsvcChannelStatus builds one DeliveryChannelStatus.
//
// The three streams are reported separately rather than folded into one, and they do
// not carry the same members. The two S3 deliveries — configuration history and
// configuration snapshot — are ConfigExportDeliveryInfo, which has lastAttemptTime,
// lastSuccessfulTime and nextDeliveryTime; the SNS notification is
// ConfigStreamDeliveryInfo, which has none of those and carries lastStatusChangeTime
// instead. Emitting one shape for all three would put members in a response the
// caller's own SDK cannot decode, so each is built by its own function.
func (p *ConfigServicePlugin) cfgsvcChannelStatus(ctx *RequestContext, channel *ConfigDeliveryChannel) (
	map[string]interface{}, error) {
	goCtx := context.Background()
	var recorderStatus ConfigRecorderStatus
	if _, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), &recorderStatus); err != nil {
		return nil, err
	}

	// Not_Applicable until the recorder has started at least once: nothing has been
	// delivered, and reporting Success would tell a consumer its pipeline works when
	// nothing has gone through it.
	status := cfgsvcDeliveryNotApplicable
	if !recorderStatus.LastStartTime.IsZero() {
		status = cfgsvcDeliverySuccess
	}
	var errorCode, errorMessage string

	seed, seeded, err := p.seededDeliveryStatus(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	if seeded {
		status = seed.Status
		errorCode = seed.LastErrorCode
		errorMessage = seed.LastErrorMessage
	}

	return map[string]interface{}{
		"name":                       channel.Name,
		"configHistoryDeliveryInfo":  p.cfgsvcExportDeliveryInfo(status, errorCode, errorMessage),
		"configSnapshotDeliveryInfo": p.cfgsvcExportDeliveryInfo(status, errorCode, errorMessage),
		"configStreamDeliveryInfo":   p.cfgsvcStreamDeliveryInfo(channel, status, errorCode, errorMessage),
	}, nil
}

// cfgsvcExportDeliveryInfo builds a ConfigExportDeliveryInfo — the shape both S3
// deliveries use.
//
// A time member is emitted only where it describes something that happened:
// lastSuccessfulTime on a Success and lastAttemptTime on a Failure. nextDeliveryTime
// is left out entirely, because substrate schedules no delivery and a timestamp
// naming one would be a promise nothing keeps.
func (p *ConfigServicePlugin) cfgsvcExportDeliveryInfo(status, errorCode, errorMessage string) map[string]interface{} {
	info := cfgsvcDeliveryInfoBase(status, errorCode, errorMessage)
	switch status {
	case cfgsvcDeliverySuccess:
		info["lastSuccessfulTime"] = EpochSeconds(p.tc.Now())
	case cfgsvcDeliveryFailure:
		info["lastAttemptTime"] = EpochSeconds(p.tc.Now())
	}
	return info
}

// cfgsvcStreamDeliveryInfo builds a ConfigStreamDeliveryInfo — the SNS notification
// stream.
//
// A channel with no SNS topic reports Not_Applicable regardless of the S3 streams,
// and a seed does not override it: the shape's own documentation says "Providing an
// SNS topic on a DeliveryChannel for Config is optional. If the SNS delivery is
// turned off, the last status will be Not_Applicable." Reporting Success for a
// notification stream that was never configured would tell a consumer its alerting
// works when no topic exists to alert on, which is a misconfiguration this operation
// is one of the few places to observe.
func (p *ConfigServicePlugin) cfgsvcStreamDeliveryInfo(channel *ConfigDeliveryChannel,
	status, errorCode, errorMessage string) map[string]interface{} {
	if channel.SnsTopicARN == "" {
		return map[string]interface{}{"lastStatus": cfgsvcDeliveryNotApplicable}
	}
	info := cfgsvcDeliveryInfoBase(status, errorCode, errorMessage)
	// lastStatusChangeTime, not lastSuccessfulTime: this shape carries neither of the
	// export shape's time members.
	info["lastStatusChangeTime"] = EpochSeconds(p.tc.Now())
	return info
}

// cfgsvcDeliveryInfoBase builds the three members both delivery-info shapes share,
// omitting an empty error code or message rather than emitting a blank one.
func cfgsvcDeliveryInfoBase(status, errorCode, errorMessage string) map[string]interface{} {
	info := map[string]interface{}{"lastStatus": status}
	if errorCode != "" {
		info["lastErrorCode"] = errorCode
	}
	if errorMessage != "" {
		info["lastErrorMessage"] = errorMessage
	}
	return info
}

// cfgsvcResolveDescribedChannel decodes and applies the name filter both channel
// describe operations share.
func (p *ConfigServicePlugin) cfgsvcResolveDescribedChannel(ctx *RequestContext, req *AWSRequest) (
	*ConfigDeliveryChannel, bool, error) {
	var in cfgsvcDescribeChannelsRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, false, err
	}
	if len(in.DeliveryChannelNames) > cfgsvcMaxChannels {
		return nil, false, cfgsvcValidation("You have specified more than one delivery channel.")
	}

	var channel ConfigDeliveryChannel
	found, err := p.cfgsvcGetJSON(context.Background(),
		cfgsvcChannelKey(ctx.AccountID, ctx.Region), &channel)
	if err != nil {
		return nil, false, err
	}
	if len(in.DeliveryChannelNames) == 1 {
		if !found || channel.Name != in.DeliveryChannelNames[0] {
			return nil, false, cfgsvcNoSuchChannel()
		}
	}
	return &channel, found, nil
}

// cfgsvcNoSuchChannel is NoSuchDeliveryChannelException.
func cfgsvcNoSuchChannel() *AWSError {
	return cfgsvcErr("NoSuchDeliveryChannelException",
		"You have specified a delivery channel that does not exist.")
}

// deleteDeliveryChannel deletes the account's delivery channel.
//
// It refuses while the recorder is recording: "Before you can delete the delivery
// channel, you must stop the customer managed configuration recorder." That refusal
// is the reason the deletes are in scope at all — it is what makes a
// teardown-and-rebuild fixture, i.e. the same test run twice, express an ordering
// requirement rather than silently succeed on a sequence AWS would reject.
func (p *ConfigServicePlugin) deleteDeliveryChannel(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcChannelNameRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	var channel ConfigDeliveryChannel
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcChannelKey(ctx.AccountID, ctx.Region), &channel)
	if err != nil {
		return nil, err
	}
	if !found || channel.Name != in.DeliveryChannelName {
		return nil, cfgsvcNoSuchChannel()
	}

	var recorderStatus ConfigRecorderStatus
	if _, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), &recorderStatus); err != nil {
		return nil, err
	}
	if recorderStatus.Recording {
		return nil, cfgsvcErr("LastDeliveryChannelDeleteFailedException",
			"You cannot delete the delivery channel you specified because the customer "+
				"managed configuration recorder is running.")
	}

	if err := p.cfgsvcDeleteKey(goCtx, cfgsvcChannelKey(ctx.AccountID, ctx.Region)); err != nil {
		return nil, err
	}
	return cfgsvcEmptyResponse(), nil
}
