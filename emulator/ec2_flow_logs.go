package emulator

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// VPC Flow Logs (#1614).
//
// A flow log is recorded intent: CreateFlowLogs stores the configuration, DescribeFlowLogs
// reports it, and nothing is ever delivered — substrate carries no traffic, so there is no
// record to write to CloudWatch Logs, S3 or Firehose, and no log group, bucket or stream is
// created or required. That is the scope rule in CLAUDE.md: the API observation is the flow
// log's configuration and status, not the records a real one would publish. deliverLogsStatus
// therefore reads SUCCESS and flowLogStatus ACTIVE, the values AWS's DescribeFlowLogs sample
// shows for a working flow log.
//
// A flow log is stored under flowlog:{account}/{region}/{fl-id} as an [EC2FlowLog]. DeleteVpc
// removes the VPC's flow logs with it ([EC2Plugin.vpcNetworkControlDependents]).

// ec2DefaultFlowLogFormat is the format a flow log created without LogFormat records — the
// default format's fields, as AWS's DescribeFlowLogs sample renders a flow log created without
// one.
const ec2DefaultFlowLogFormat = "${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} " +
	"${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}"

// ec2FlowLogTimeFormat is how a flow log's creationTime renders: AWS's DescribeFlowLogs sample
// shows millisecond precision, "2020-02-04T11:46:13.831Z".
const ec2FlowLogTimeFormat = "2006-01-02T15:04:05.000Z"

// ec2MaxFlowLogResources is CreateFlowLogs' "Maximum of 300 for the other resource types" — the
// VPC, subnet and network-interface types substrate accepts.
const ec2MaxFlowLogResources = 300

// EC2FlowLog is a stored flow log.
type EC2FlowLog struct {
	// FlowLogID is the fl- ID.
	FlowLogID string `json:"flow_log_id"`

	// ResourceID is the monitored VPC, subnet or network interface.
	ResourceID string `json:"resource_id"`

	// TrafficType is ACCEPT, REJECT or ALL.
	TrafficType string `json:"traffic_type"`

	// LogDestinationType is cloud-watch-logs, s3 or kinesis-data-firehose.
	LogDestinationType string `json:"log_destination_type"`

	// LogDestination is the destination ARN, empty when the flow log names a log group by name.
	LogDestination string `json:"log_destination,omitempty"`

	// LogGroupName is the CloudWatch Logs log group, for a cloud-watch-logs flow log.
	LogGroupName string `json:"log_group_name,omitempty"`

	// DeliverLogsPermissionArn is the IAM role EC2 would publish with.
	DeliverLogsPermissionArn string `json:"deliver_logs_permission_arn,omitempty"`

	// DeliverCrossAccountRole is the IAM role for cross-account delivery.
	DeliverCrossAccountRole string `json:"deliver_cross_account_role,omitempty"`

	// LogFormat is the record format.
	LogFormat string `json:"log_format"`

	// MaxAggregationInterval is 60 or 600 seconds.
	MaxAggregationInterval int `json:"max_aggregation_interval"`

	// DestinationOptions are an S3 destination's options, nil when none were given.
	DestinationOptions *EC2FlowLogDestinationOptions `json:"destination_options,omitempty"`

	// CreationTime is when the flow log was created, on the simulated clock, as AWS renders it.
	CreationTime string `json:"creation_time"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`
}

// EC2FlowLogDestinationOptions are a flow log's S3 destination options.
type EC2FlowLogDestinationOptions struct {
	// FileFormat is plain-text or parquet.
	FileFormat string `json:"file_format"`

	// HiveCompatiblePartitions reports Hive-compatible S3 prefixes.
	HiveCompatiblePartitions bool `json:"hive_compatible_partitions"`

	// PerHourPartition reports hourly partitions.
	PerHourPartition bool `json:"per_hour_partition"`
}

// ec2FlowLogKey is the state key of a flow log.
func ec2FlowLogKey(accountID, region, id string) string {
	return ec2StatePrefix("flowlog", accountID, region) + id
}

// ec2FlowLogNotFound is InvalidFlowLogId.NotFound, with the error reference's gloss as its
// message.
func ec2FlowLogNotFound() *AWSError {
	return &AWSError{
		Code:       "InvalidFlowLogId.NotFound",
		Message:    "The specified flow log does not exist.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2FlowLogEnum refuses value with InvalidParameterValue unless it is one of allowed.
func ec2FlowLogEnum(param, value string, allowed ...string) *AWSError {
	if containsStr(allowed, value) {
		return nil
	}
	return ec2InvalidParameterValue(param, value, "It must be one of "+strings.Join(allowed, ", ")+".")
}

// ec2LogGroupNameFromARN returns the log group an ARN of the form
// arn:aws:logs:region:account:log-group:NAME names, with any trailing ":*" dropped, or "".
func ec2LogGroupNameFromARN(arn string) string {
	_, name, ok := strings.Cut(arn, ":log-group:")
	if !ok {
		return ""
	}
	return strings.TrimSuffix(name, ":*")
}

// ec2ParseFlowLog reads the configuration CreateFlowLogs carries, everything but the resources.
//
// The rules are the parameter documentation's. TrafficType is "required for the other resource
// types" than transit gateway and Cloud WAN, which are all substrate accepts. LogDestinationType
// defaults to cloud-watch-logs. A cloud-watch-logs flow log names its group by LogDestination or
// by LogGroupName — "Alternatively, use the LogGroupName parameter" — and needs
// DeliverLogsPermissionArn ("required if the destination type is cloud-watch-logs"); s3 and
// kinesis-data-firehose need LogDestination, and LogGroupName "is valid only if the destination
// type is cloud-watch-logs". MaxAggregationInterval is 60 or 600, "Default: 600". The messages
// are substrate's own.
func ec2ParseFlowLog(params map[string]string) (EC2FlowLog, *AWSError) {
	fl := EC2FlowLog{
		TrafficType:              params["TrafficType"],
		LogDestinationType:       params["LogDestinationType"],
		LogDestination:           params["LogDestination"],
		LogGroupName:             params["LogGroupName"],
		DeliverLogsPermissionArn: params["DeliverLogsPermissionArn"],
		DeliverCrossAccountRole:  params["DeliverCrossAccountRole"],
		LogFormat:                params["LogFormat"],
		MaxAggregationInterval:   600,
	}
	if fl.TrafficType == "" {
		return fl, ec2MissingParameter("TrafficType")
	}
	if awsErr := ec2FlowLogEnum("TrafficType", fl.TrafficType, "ACCEPT", "REJECT", "ALL"); awsErr != nil {
		return fl, awsErr
	}
	if fl.LogDestinationType == "" {
		fl.LogDestinationType = "cloud-watch-logs"
	}
	if awsErr := ec2FlowLogEnum("LogDestinationType", fl.LogDestinationType, "cloud-watch-logs", "s3", "kinesis-data-firehose"); awsErr != nil {
		return fl, awsErr
	}
	switch {
	case fl.LogDestinationType != "cloud-watch-logs" && fl.LogGroupName != "":
		return fl, &AWSError{Code: "InvalidParameterCombination",
			Message: "LogGroupName is valid only if the destination type is cloud-watch-logs.", HTTPStatus: http.StatusBadRequest}
	case fl.LogDestination != "" && fl.LogGroupName != "":
		return fl, &AWSError{Code: "InvalidParameterCombination",
			Message: "Specify either LogDestination or LogGroupName, not both.", HTTPStatus: http.StatusBadRequest}
	case fl.LogDestination == "" && fl.LogGroupName == "":
		return fl, ec2MissingParameter("LogDestination")
	case fl.LogDestinationType == "cloud-watch-logs" && fl.DeliverLogsPermissionArn == "":
		return fl, ec2MissingParameter("DeliverLogsPermissionArn")
	}
	if fl.LogDestinationType == "cloud-watch-logs" && fl.LogGroupName == "" {
		fl.LogGroupName = ec2LogGroupNameFromARN(fl.LogDestination)
	}
	if fl.LogFormat == "" {
		fl.LogFormat = ec2DefaultFlowLogFormat
	}
	if raw := params["MaxAggregationInterval"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || (n != 60 && n != 600) {
			return fl, ec2InvalidParameterValue("MaxAggregationInterval", raw, "It must be 60 or 600.")
		}
		fl.MaxAggregationInterval = n
	}
	if opts, awsErr := ec2ParseFlowLogDestinationOptions(params); awsErr != nil {
		return fl, awsErr
	} else if opts != nil {
		fl.DestinationOptions = opts
	}
	return fl, nil
}

// ec2ParseFlowLogDestinationOptions reads DestinationOptions, or returns nil when the request
// carries none. FileFormat defaults to plain-text, and the two booleans to false.
func ec2ParseFlowLogDestinationOptions(params map[string]string) (*EC2FlowLogDestinationOptions, *AWSError) {
	format, hasFormat := params["DestinationOptions.FileFormat"]
	hive, hasHive := params["DestinationOptions.HiveCompatiblePartitions"]
	hour, hasHour := params["DestinationOptions.PerHourPartition"]
	if !hasFormat && !hasHive && !hasHour {
		return nil, nil
	}
	opts := &EC2FlowLogDestinationOptions{FileFormat: "plain-text"}
	if format != "" {
		if awsErr := ec2FlowLogEnum("DestinationOptions.FileFormat", format, "plain-text", "parquet"); awsErr != nil {
			return nil, awsErr
		}
		opts.FileFormat = format
	}
	for _, b := range []struct {
		param, raw string
		set        bool
		dst        *bool
	}{
		{"DestinationOptions.HiveCompatiblePartitions", hive, hasHive, &opts.HiveCompatiblePartitions},
		{"DestinationOptions.PerHourPartition", hour, hasHour, &opts.PerHourPartition},
	} {
		if !b.set || b.raw == "" {
			continue
		}
		v, err := strconv.ParseBool(b.raw)
		if err != nil {
			return nil, ec2InvalidParameterValue(b.param, b.raw, "It must be true or false.")
		}
		*b.dst = v
	}
	return opts, nil
}

// flowLogResourceExists reports whether the resource a flow log monitors exists, and the ID
// kind its absence is reported through. A network interface is resolved through
// DescribeNetworkInterfaces, so whichever interfaces that operation reports are the ones a flow
// log can monitor.
func (p *EC2Plugin) flowLogResourceExists(reqCtx *RequestContext, resourceType, id string) (bool, ec2IDKind, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region + "/"
	switch resourceType {
	case "VPC":
		data, err := p.state.Get(context.Background(), ec2Namespace, "vpc:"+scope+id)
		return data != nil, ec2VPCIDKind, err
	case "Subnet":
		data, err := p.state.Get(context.Background(), ec2Namespace, "subnet:"+scope+id)
		return data != nil, ec2SubnetIDKind, err
	default:
		_, err := p.describeNetworkInterfaces(reqCtx, &AWSRequest{Params: map[string]string{"NetworkInterfaceId.1": id}})
		if err == nil {
			return true, ec2NetworkInterfaceIDKind, nil
		}
		var awsErr *AWSError
		if errors.As(err, &awsErr) && awsErr.Code == ec2NetworkInterfaceIDKind.NotFound {
			return false, ec2NetworkInterfaceIDKind, nil
		}
		return false, ec2NetworkInterfaceIDKind, err
	}
}

// ec2FlowLogUnsuccessfulItem renders one UnsuccessfulItem.
type ec2FlowLogUnsuccessfulItem struct {
	// Code is the error code.
	Code string `xml:"error>code"`

	// Message is the error message.
	Message string `xml:"error>message"`

	// ResourceID is the resource the item is about.
	ResourceID string `xml:"resourceId"`
}

// ec2FlowLogUnsuccessfulSet is the present-even-when-empty unsuccessful element, as both
// CreateFlowLogs' and DeleteFlowLogs' samples render it (`<unsuccessful/>`).
type ec2FlowLogUnsuccessfulSet struct {
	// Items are the unsuccessful items.
	Items []ec2FlowLogUnsuccessfulItem `xml:"item"`
}

// createFlowLogs handles CreateFlowLogs: one flow log per ResourceId.N, each with the request's
// configuration and tags (TagSpecification.N scoped to vpc-flow-log).
//
// ResourceType is VPC, Subnet or NetworkInterface; the transit gateway, regional NAT gateway and
// Cloud WAN types name resources substrate does not model and are refused with
// InvalidParameterValue. Every resource is checked before anything is written, and one that
// does not exist fails the whole request with its kind's NotFound — InvalidVpcID.NotFound,
// InvalidSubnetID.NotFound or InvalidNetworkInterfaceID.NotFound — as a top-level error. No
// source publishes which shape CreateFlowLogs uses for an absent resource; this is substrate's
// reading, matching what DeleteFlowLogs is observed to do with an absent flow log (see
// [EC2Plugin.deleteFlowLogs]).
//
// A resource already holding a flow log with the same traffic type and destination is reported
// per resource, as an Unsuccessful item carrying FlowLogAlreadyExists and the error reference's
// gloss — "A flow log with the specified configuration already exists." — while the request's
// other resources get theirs. A ClientToken is echoed, not used to deduplicate.
func (p *EC2Plugin) createFlowLogs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceIDs := extractIndexedParams(req.Params, "ResourceId")
	if len(resourceIDs) == 0 {
		return nil, ec2MissingParameter("ResourceId")
	}
	if len(resourceIDs) > ec2MaxFlowLogResources {
		return nil, ec2InvalidParameterValue("ResourceId", strconv.Itoa(len(resourceIDs))+" IDs", "Specify at most 300.")
	}
	resourceType := req.Params["ResourceType"]
	if resourceType == "" {
		return nil, ec2MissingParameter("ResourceType")
	}
	if awsErr := ec2FlowLogEnum("ResourceType", resourceType, "VPC", "Subnet", "NetworkInterface"); awsErr != nil {
		return nil, awsErr
	}
	template, awsErr := ec2ParseFlowLog(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	tags := ec2TagSpecificationTags(req.Params, "", "vpc-flow-log")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}
	// Held from the resource checks through the writes, so two identical requests cannot both
	// pass the duplicate check, and DeleteVpc — which holds it too — cannot remove a VPC between
	// its check here and its flow log's write.
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	for _, id := range resourceIDs {
		found, kind, err := p.flowLogResourceExists(reqCtx, resourceType, id)
		if err != nil {
			return nil, err
		}
		if lookupErr := ec2RequireResource(kind, id, found); lookupErr != nil {
			return nil, lookupErr
		}
	}
	ctx := context.Background()
	existing, err := p.loadFlowLogs(ctx, reqCtx)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name                  `xml:"CreateFlowLogsResponse"`
		XMLNS        string                    `xml:"xmlns,attr"`
		ClientToken  string                    `xml:"clientToken,omitempty"`
		Unsuccessful ec2FlowLogUnsuccessfulSet `xml:"unsuccessful"`
		FlowLogIDs   []string                  `xml:"flowLogIdSet>item"`
	}
	resp := response{XMLNS: ec2XMLNS, ClientToken: req.Params["ClientToken"]}
	created := p.tc.Now().UTC().Format(ec2FlowLogTimeFormat)
	for _, id := range resourceIDs {
		if ec2FlowLogDuplicates(existing, template, id) {
			resp.Unsuccessful.Items = append(resp.Unsuccessful.Items, ec2FlowLogUnsuccessfulItem{
				Code: "FlowLogAlreadyExists", Message: "A flow log with the specified configuration already exists.", ResourceID: id,
			})
			continue
		}
		fl := template
		fl.FlowLogID = "fl-" + reqCtx.IDs.Hex(8)
		fl.ResourceID = id
		fl.CreationTime = created
		fl.Tags = tags
		if err := p.putEC2Record(ctx, ec2FlowLogKey(reqCtx.AccountID, reqCtx.Region, fl.FlowLogID), fl); err != nil {
			return nil, fmt.Errorf("ec2 createFlowLogs: %w", err)
		}
		existing = append(existing, fl)
		resp.FlowLogIDs = append(resp.FlowLogIDs, fl.FlowLogID)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2FlowLogDuplicates reports whether resourceID already holds a flow log with template's
// traffic type, destination type and destination.
func ec2FlowLogDuplicates(existing []EC2FlowLog, template EC2FlowLog, resourceID string) bool {
	for _, fl := range existing {
		if fl.ResourceID == resourceID && fl.TrafficType == template.TrafficType &&
			fl.LogDestinationType == template.LogDestinationType &&
			fl.LogDestination == template.LogDestination && fl.LogGroupName == template.LogGroupName {
			return true
		}
	}
	return false
}

// loadFlowLogs returns the account and Region's flow logs, ordered by ID.
func (p *EC2Plugin) loadFlowLogs(ctx context.Context, reqCtx *RequestContext) ([]EC2FlowLog, error) {
	var out []EC2FlowLog
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("flowlog", reqCtx.AccountID, reqCtx.Region), func(_ string, fl EC2FlowLog) {
		out = append(out, fl)
	}); err != nil {
		return nil, fmt.Errorf("ec2 flow logs: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FlowLogID < out[j].FlowLogID })
	return out, nil
}

// deleteFlowLogs handles DeleteFlowLogs.
//
// An ID naming no flow log fails the whole call with a top-level InvalidFlowLogId.NotFound and
// deletes nothing, including the request's other, existing flow logs: that is EC2's observed
// behavior, reported from a live account in aws-controllers-k8s/ec2-controller#402 ("[real,
// fake]: top-level InvalidFlowLogId.NotFound. The whole call is rejected and the real flow log
// is kept"), where no delete was ever seen to come back as an Unsuccessful item. The response's
// unsuccessful element is therefore always empty.
func (p *EC2Plugin) deleteFlowLogs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "FlowLogId")
	if len(ids) == 0 {
		return nil, ec2MissingParameter("FlowLogId")
	}
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	for _, id := range ids {
		data, err := p.state.Get(ctx, ec2Namespace, ec2FlowLogKey(reqCtx.AccountID, reqCtx.Region, id))
		if err != nil {
			return nil, fmt.Errorf("ec2 deleteFlowLogs get: %w", err)
		}
		if data == nil {
			return nil, ec2FlowLogNotFound()
		}
	}
	for _, id := range ids {
		if err := p.state.Delete(ctx, ec2Namespace, ec2FlowLogKey(reqCtx.AccountID, reqCtx.Region, id)); err != nil {
			return nil, fmt.Errorf("ec2 deleteFlowLogs: %w", err)
		}
	}
	type response struct {
		XMLName      xml.Name                  `xml:"DeleteFlowLogsResponse"`
		XMLNS        string                    `xml:"xmlns,attr"`
		Unsuccessful ec2FlowLogUnsuccessfulSet `xml:"unsuccessful"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: ec2XMLNS})
}

// ec2FlowLogItem renders a FlowLog, in the member order AWS's DescribeFlowLogs sample shows.
type ec2FlowLogItem struct {
	// LogDestination is the destination ARN.
	LogDestination string `xml:"logDestination,omitempty"`

	// ResourceID is the monitored resource.
	ResourceID string `xml:"resourceId"`

	// LogDestinationType is the destination type.
	LogDestinationType string `xml:"logDestinationType"`

	// DeliverLogsPermissionArn is the delivery role.
	DeliverLogsPermissionArn string `xml:"deliverLogsPermissionArn,omitempty"`

	// DeliverCrossAccountRole is the cross-account delivery role.
	DeliverCrossAccountRole string `xml:"deliverCrossAccountRole,omitempty"`

	// CreationTime is when the flow log was created.
	CreationTime string `xml:"creationTime"`

	// TrafficType is the traffic captured.
	TrafficType string `xml:"trafficType"`

	// DeliverLogsStatus is the delivery status.
	DeliverLogsStatus string `xml:"deliverLogsStatus"`

	// LogFormat is the record format.
	LogFormat string `xml:"logFormat"`

	// FlowLogStatus is the flow log's status.
	FlowLogStatus string `xml:"flowLogStatus"`

	// LogGroupName is the CloudWatch Logs log group.
	LogGroupName string `xml:"logGroupName,omitempty"`

	// FlowLogID is the flow log's ID.
	FlowLogID string `xml:"flowLogId"`

	// MaxAggregationInterval is the aggregation interval in seconds.
	MaxAggregationInterval int `xml:"maxAggregationInterval"`

	// DestinationOptions are the S3 destination options.
	DestinationOptions *ec2FlowLogDestinationOptionsItem `xml:"destinationOptions,omitempty"`

	// Tags are the flow log's tags.
	Tags *ec2TagSetXML `xml:"tagSet,omitempty"`
}

// ec2FlowLogDestinationOptionsItem renders a DestinationOptionsResponse.
type ec2FlowLogDestinationOptionsItem struct {
	// FileFormat is plain-text or parquet.
	FileFormat string `xml:"fileFormat"`

	// HiveCompatiblePartitions reports Hive-compatible prefixes.
	HiveCompatiblePartitions bool `xml:"hiveCompatiblePartitions"`

	// PerHourPartition reports hourly partitions.
	PerHourPartition bool `xml:"perHourPartition"`
}

// ec2FlowLogXML renders fl.
func ec2FlowLogXML(fl EC2FlowLog) ec2FlowLogItem {
	item := ec2FlowLogItem{
		LogDestination:           fl.LogDestination,
		ResourceID:               fl.ResourceID,
		LogDestinationType:       fl.LogDestinationType,
		DeliverLogsPermissionArn: fl.DeliverLogsPermissionArn,
		DeliverCrossAccountRole:  fl.DeliverCrossAccountRole,
		CreationTime:             fl.CreationTime,
		TrafficType:              fl.TrafficType,
		DeliverLogsStatus:        "SUCCESS",
		LogFormat:                fl.LogFormat,
		FlowLogStatus:            "ACTIVE",
		LogGroupName:             fl.LogGroupName,
		FlowLogID:                fl.FlowLogID,
		MaxAggregationInterval:   fl.MaxAggregationInterval,
		Tags:                     ec2TagSet(fl.Tags),
	}
	if o := fl.DestinationOptions; o != nil {
		item.DestinationOptions = &ec2FlowLogDestinationOptionsItem{
			FileFormat: o.FileFormat, HiveCompatiblePartitions: o.HiveCompatiblePartitions, PerHourPartition: o.PerHourPartition,
		}
	}
	return item
}

// ec2FlowLogFilterSpec is DescribeFlowLogs' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeFlowLogs.html. Every
// documented name is evaluated.
func ec2FlowLogFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"deliver-log-status", "log-destination-type", "flow-log-id", "log-group-name",
			"resource-id", "traffic-type", "tag-key",
		},
	}
}

// ec2FlowLogMatchesFilter evaluates one DescribeFlowLogs filter.
func ec2FlowLogMatchesFilter(fl EC2FlowLog, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(fl.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "deliver-log-status":
		return ec2FilterAccepts(values, "SUCCESS")
	case "log-destination-type":
		return ec2FilterAccepts(values, fl.LogDestinationType)
	case "flow-log-id":
		return ec2FilterAccepts(values, fl.FlowLogID)
	case "log-group-name":
		return ec2FilterAccepts(values, fl.LogGroupName)
	case "resource-id":
		return ec2FilterAccepts(values, fl.ResourceID)
	case "traffic-type":
		return ec2FilterAccepts(values, fl.TrafficType)
	default:
		return true
	}
}

// describeFlowLogs handles DescribeFlowLogs: the account's flow logs in the Region, narrowed by
// FlowLogId.N and Filter.N, ordered by ID, one page at a time.
//
// A FlowLogId.N naming no flow log narrows the answer rather than failing it: the Terraform AWS
// provider's findFlowLogs treats an empty answer, not an error, as "gone", and handles no
// NotFound code here as it does for the sibling describes. The page publishes no MaxResults
// range, so the bounds are [ec2MinUnpublishedMaxResults] with no ceiling.
func (p *EC2Plugin) describeFlowLogs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "FlowLogId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "FlowLogId", ids); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinUnpublishedMaxResults, ec2NoMaxResultsCeiling)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	if err := ec2FlowLogFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	all, err := p.loadFlowLogs(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName   xml.Name         `xml:"DescribeFlowLogsResponse"`
		XMLNS     string           `xml:"xmlns,attr"`
		FlowLogs  []ec2FlowLogItem `xml:"flowLogSet>item"`
		NextToken string           `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: ec2XMLNS}
	for _, fl := range all {
		if len(ids) > 0 && !containsStr(ids, fl.FlowLogID) {
			continue
		}
		selected := true
		for name, values := range filters {
			if !ec2FlowLogMatchesFilter(fl, name, values) {
				selected = false
				break
			}
		}
		if selected {
			resp.FlowLogs = append(resp.FlowLogs, ec2FlowLogXML(fl))
		}
	}
	resp.FlowLogs, resp.NextToken = ec2Page(resp.FlowLogs, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}
