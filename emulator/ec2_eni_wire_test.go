package emulator_test

import "testing"

// TestEC2Wire_NetworkInterfaceResponsesCarryNoBookkeepingMember is the network interface record's
// raw-bytes proof (#1522, #756): the record persists account_id, region and ever_tagged, and no
// response that renders an interface carries any of them.
func TestEC2Wire_NetworkInterfaceResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)
	subnetID := ec2WireSubnet(t, p, ctx, vpcID)

	created := ec2Wire(t, p, ctx, "CreateNetworkInterface", map[string]string{"SubnetId": subnetID})
	eniID := ec2WireElement(t, "CreateNetworkInterface", created, "networkInterfaceId")
	key := ec2WireKey("eni", eniID)
	ec2WireRequireScoped(t, state, key, "account_id")
	ec2WireStampEverTagged(t, p, ctx, state, key, eniID)

	run := ec2Wire(t, p, ctx, "RunInstances", map[string]string{
		"ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnetID,
	})
	instanceID := ec2WireElement(t, "RunInstances", run, "instanceId")
	attached := ec2Wire(t, p, ctx, "AttachNetworkInterface", map[string]string{
		"NetworkInterfaceId": eniID, "InstanceId": instanceID, "DeviceIndex": "1",
	})

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateNetworkInterface", held: created, anchor: "<networkInterfaceId>" + eniID + "</networkInterfaceId>"},
		{action: "RunInstances", held: run, anchor: "<networkInterfaceSet>"},
		{action: "AttachNetworkInterface", held: attached, anchor: "<attachmentId>eni-attach-"},
		{action: "DescribeNetworkInterfaces", params: map[string]string{"NetworkInterfaceId.1": eniID},
			anchor: "<networkInterfaceId>" + eniID + "</networkInterfaceId>"},
		{action: "DescribeInstances", params: map[string]string{"InstanceId.1": instanceID},
			anchor: "<networkInterfaceId>" + eniID + "</networkInterfaceId>"},
	})
}
