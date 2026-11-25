package emulator_test

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// VPC Flow Logs (#1614): recorded intent, reported with a delivery status of SUCCESS, never
// delivered.

// flTestFlowLog is a FlowLog as a caller decodes it.
type flTestFlowLog struct {
	ID              string         `xml:"flowLogId"`
	ResourceID      string         `xml:"resourceId"`
	TrafficType     string         `xml:"trafficType"`
	DestinationType string         `xml:"logDestinationType"`
	Destination     string         `xml:"logDestination"`
	LogGroupName    string         `xml:"logGroupName"`
	Role            string         `xml:"deliverLogsPermissionArn"`
	DeliverStatus   string         `xml:"deliverLogsStatus"`
	Status          string         `xml:"flowLogStatus"`
	Format          string         `xml:"logFormat"`
	Interval        int            `xml:"maxAggregationInterval"`
	CreationTime    string         `xml:"creationTime"`
	Tags            []describedTag `xml:"tagSet>item"`
	Options         *struct {
		FileFormat string `xml:"fileFormat"`
		Hive       bool   `xml:"hiveCompatiblePartitions"`
		PerHour    bool   `xml:"perHourPartition"`
	} `xml:"destinationOptions"`
}

// flCreateResult is CreateFlowLogs' answer.
type flCreateResult struct {
	IDs          []string `xml:"flowLogIdSet>item"`
	Unsuccessful []struct {
		Code       string `xml:"error>code"`
		Message    string `xml:"error>message"`
		ResourceID string `xml:"resourceId"`
	} `xml:"unsuccessful>item"`
}

// flCreate runs CreateFlowLogs.
func flCreate(t *testing.T, call func(map[string]string) []byte, params map[string]string) flCreateResult {
	t.Helper()
	p := map[string]string{"Action": "CreateFlowLogs"}
	for k, v := range params {
		p[k] = v
	}
	var out flCreateResult
	require.NoError(t, xml.Unmarshal(call(p), &out))
	return out
}

// flList runs DescribeFlowLogs.
func flList(t *testing.T, call func(map[string]string) []byte, params map[string]string) ([]flTestFlowLog, string) {
	t.Helper()
	p := map[string]string{"Action": "DescribeFlowLogs"}
	for k, v := range params {
		p[k] = v
	}
	var doc struct {
		XMLName xml.Name        `xml:"DescribeFlowLogsResponse"`
		Items   []flTestFlowLog `xml:"flowLogSet>item"`
		Next    string          `xml:"nextToken"`
	}
	require.NoError(t, xml.Unmarshal(call(p), &doc))
	return doc.Items, doc.Next
}

// flCWL is a CloudWatch Logs flow log request for one VPC.
func flCWL(vpcID string) map[string]string {
	return map[string]string{
		"ResourceType": "VPC", "ResourceId.1": vpcID, "TrafficType": "ALL",
		"LogDestinationType":       "cloud-watch-logs",
		"LogDestination":           "arn:aws:logs:us-east-1:123456789012:log-group:vpc-flows:*",
		"DeliverLogsPermissionArn": "arn:aws:iam::123456789012:role/flow-logs",
	}
}

func TestEC2FlowLogs_CreateDescribeDelete(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.74.0.0/16")
	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.74.1.0/24")

	params := flCWL(vpcID)
	params["TagSpecification.1.ResourceType"] = "vpc-flow-log"
	params["TagSpecification.1.Tag.1.Key"] = "Name"
	params["TagSpecification.1.Tag.1.Value"] = "vpc"
	params["ClientToken"] = "tok-1"
	raw := string(call(map[string]string{"Action": "CreateFlowLogs", "ResourceType": "Subnet", "ResourceId.1": subnetID,
		"TrafficType": "REJECT", "LogDestinationType": "s3", "LogDestination": "arn:aws:s3:::flows/subnet/",
		"MaxAggregationInterval": "60", "LogFormat": "${version} ${srcaddr}",
		"DestinationOptions.FileFormat": "parquet", "DestinationOptions.PerHourPartition": "true"}))
	assert.Contains(t, raw, "<unsuccessful></unsuccessful>", "present when empty, as AWS's sample renders it")
	created := flCreate(t, call, params)
	require.Len(t, created.IDs, 1)
	assert.True(t, strings.HasPrefix(created.IDs[0], "fl-"))
	assert.Empty(t, created.Unsuccessful)

	logs, _ := flList(t, call, map[string]string{"FlowLogId.1": created.IDs[0]})
	require.Len(t, logs, 1)
	fl := logs[0]
	assert.Equal(t, vpcID, fl.ResourceID)
	assert.Equal(t, "ALL", fl.TrafficType)
	assert.Equal(t, "cloud-watch-logs", fl.DestinationType)
	assert.Equal(t, "vpc-flows", fl.LogGroupName, "the group named by the destination ARN")
	assert.Equal(t, "SUCCESS", fl.DeliverStatus)
	assert.Equal(t, "ACTIVE", fl.Status)
	assert.Equal(t, 600, fl.Interval, "Default: 600")
	assert.Contains(t, fl.Format, "${log-status}", "the default format")
	assert.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`, fl.CreationTime)
	require.Len(t, fl.Tags, 1)
	assert.Nil(t, fl.Options)

	s3, _ := flList(t, call, ec2OneFilter("DescribeFlowLogs", "resource-id", subnetID))
	require.Len(t, s3, 1)
	assert.Equal(t, 60, s3[0].Interval)
	assert.Equal(t, "${version} ${srcaddr}", s3[0].Format)
	assert.Empty(t, s3[0].LogGroupName)
	require.NotNil(t, s3[0].Options)
	assert.Equal(t, "parquet", s3[0].Options.FileFormat)
	assert.True(t, s3[0].Options.PerHour)

	// The same configuration again is an Unsuccessful item, not an error.
	again := flCreate(t, call, flCWL(vpcID))
	assert.Empty(t, again.IDs)
	require.Len(t, again.Unsuccessful, 1)
	assert.Equal(t, "FlowLogAlreadyExists", again.Unsuccessful[0].Code)
	assert.Equal(t, vpcID, again.Unsuccessful[0].ResourceID)
	// A LogGroupName flow log reports no destination ARN.
	byName := flCreate(t, call, map[string]string{"ResourceType": "VPC", "ResourceId.1": vpcID, "TrafficType": "ACCEPT",
		"LogGroupName": "named", "DeliverLogsPermissionArn": "arn:aws:iam::123456789012:role/r"})
	require.Len(t, byName.IDs, 1)
	named, _ := flList(t, call, ec2OneFilter("DescribeFlowLogs", "log-group-name", "named"))
	require.Len(t, named, 1)
	assert.Empty(t, named[0].Destination)

	for _, tc := range []struct {
		name, value string
		want        int
	}{
		{"deliver-log-status", "SUCCESS", 3}, {"deliver-log-status", "FAILED", 0},
		{"log-destination-type", "s3", 1}, {"flow-log-id", created.IDs[0], 1},
		{"traffic-type", "REJECT", 1}, {"tag:Name", "vpc", 1}, {"tag-key", "Name", 1},
		{"resource-id", vpcID, 2},
	} {
		got, _ := flList(t, call, ec2OneFilter("DescribeFlowLogs", tc.name, tc.value))
		assert.Len(t, got, tc.want, "%s=%s", tc.name, tc.value)
	}
	page, next := flList(t, call, map[string]string{"MaxResults": "1"})
	assert.Len(t, page, 1)
	assert.NotEmpty(t, next)
	none, _ := flList(t, call, map[string]string{"FlowLogId.1": "fl-0000000000000000"})
	assert.Empty(t, none, "an unknown ID narrows the answer")

	// A delete naming an unknown ID fails whole, keeping the real one.
	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{"Action": "DeleteFlowLogs",
		"FlowLogId.1": created.IDs[0], "FlowLogId.2": "fl-0000000000000000"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidFlowLogId.NotFound", code)
	kept, _ := flList(t, call, map[string]string{"FlowLogId.1": created.IDs[0]})
	assert.Len(t, kept, 1)
	deleted := string(call(map[string]string{"Action": "DeleteFlowLogs", "FlowLogId.1": created.IDs[0]}))
	assert.Contains(t, deleted, "<unsuccessful></unsuccessful>")
	gone, _ := flList(t, call, map[string]string{"FlowLogId.1": created.IDs[0]})
	assert.Empty(t, gone)
}

func TestEC2FlowLogs_Refusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.75.0.0/16")
	with := func(changes map[string]string) map[string]string {
		p := flCWL(vpcID)
		p["Action"] = "CreateFlowLogs"
		for k, v := range changes {
			if v == "" {
				delete(p, k)
				continue
			}
			p[k] = v
		}
		return p
	}
	for _, tc := range []struct {
		name    string
		changes map[string]string
		code    string
	}{
		{"no resource", map[string]string{"ResourceId.1": ""}, "MissingParameter"},
		{"no resource type", map[string]string{"ResourceType": ""}, "MissingParameter"},
		{"a transit gateway", map[string]string{"ResourceType": "TransitGateway"}, "InvalidParameterValue"},
		{"an unknown VPC", map[string]string{"ResourceId.1": "vpc-0000000000000000"}, "InvalidVpcID.NotFound"},
		{"an unknown subnet", map[string]string{"ResourceType": "Subnet", "ResourceId.1": "subnet-0000000000000000"}, "InvalidSubnetID.NotFound"},
		{"an unknown interface", map[string]string{"ResourceType": "NetworkInterface", "ResourceId.1": "eni-0000000000000000"},
			"InvalidNetworkInterfaceID.NotFound"},
		{"a malformed interface", map[string]string{"ResourceType": "NetworkInterface", "ResourceId.1": "nic-1"},
			"InvalidNetworkInterfaceID.Malformed"},
		{"no traffic type", map[string]string{"TrafficType": ""}, "MissingParameter"},
		{"an unknown traffic type", map[string]string{"TrafficType": "SOME"}, "InvalidParameterValue"},
		{"an unknown destination type", map[string]string{"LogDestinationType": "kafka"}, "InvalidParameterValue"},
		{"no destination", map[string]string{"LogDestination": ""}, "MissingParameter"},
		{"both a destination and a group", map[string]string{"LogGroupName": "g"}, "InvalidParameterCombination"},
		{"a group with an s3 destination", map[string]string{"LogDestinationType": "s3", "LogDestination": "", "LogGroupName": "g"},
			"InvalidParameterCombination"},
		{"no delivery role for CloudWatch Logs", map[string]string{"DeliverLogsPermissionArn": ""}, "MissingParameter"},
		{"an interval of 300", map[string]string{"MaxAggregationInterval": "300"}, "InvalidParameterValue"},
		{"an unknown file format", map[string]string{"DestinationOptions.FileFormat": "csv"}, "InvalidParameterValue"},
		{"a partition flag that is not a boolean", map[string]string{"DestinationOptions.HiveCompatiblePartitions": "yes"},
			"InvalidParameterValue"},
		{"a reserved tag", map[string]string{"TagSpecification.1.ResourceType": "vpc-flow-log",
			"TagSpecification.1.Tag.1.Key": "aws:x", "TagSpecification.1.Tag.1.Value": "y"}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, ts, with(tc.changes), tc.code)
		})
	}
	many := with(nil)
	for i := 1; i <= 301; i++ {
		many["ResourceId."+strconv.Itoa(i)] = vpcID
	}
	ec2RequireRefusal(t, ts, many, "InvalidParameterValue")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteFlowLogs"}, "MissingParameter")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeFlowLogs", "MaxResults": "0"}, "InvalidParameterValue")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeFlowLogs", "MaxResults": "5", "FlowLogId.1": "fl-1"},
		"InvalidParameterCombination")
	ec2RequireRefusal(t, ts, ec2OneFilter("DescribeFlowLogs", "vpc-id", vpcID), "InvalidParameterValue")
}

func TestEC2FlowLogs_ANetworkInterfaceDescribeNetworkInterfacesReports(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.76.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpcID, "10.76.1.0/24")
	ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": subnet,
	})
	var enis struct {
		IDs []string `xml:"networkInterfaceSet>item>networkInterfaceId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "DescribeNetworkInterfaces"}), &enis))
	require.Len(t, enis.IDs, 1)
	created := flCreate(t, call, map[string]string{"ResourceType": "NetworkInterface", "ResourceId.1": enis.IDs[0],
		"TrafficType": "ALL", "LogDestinationType": "kinesis-data-firehose",
		"LogDestination": "arn:aws:firehose:us-east-1:123456789012:deliverystream:flows"})
	require.Len(t, created.IDs, 1)
	ec2TagResource(t, ts, created.IDs[0], "Owner", "net")
	var tags struct {
		Types []string `xml:"tagSet>item>resourceType"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeTags", "resource-id", created.IDs[0])), &tags))
	assert.Equal(t, []string{"vpc-flow-log"}, tags.Types)
}
