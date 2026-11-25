package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnSDTemplate is a VPC with a private DNS namespace, an HTTP namespace, a service in each and an
// instance of the DNS one, with the given namespace description and TTL.
func cfnSDTemplate(description, ttl string) string {
	return `{"Parameters":{"Ttl":{"Type":"Number","Default":"` + ttl + `"}},"Resources":{
		"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.20.0.0/16"}},
		"Private":{"Type":"AWS::ServiceDiscovery::PrivateDnsNamespace","Properties":{"Name":"cfn.internal",
			"Vpc":{"Ref":"Vpc"},"Description":"` + description + `",
			"Properties":{"DnsProperties":{"SOA":{"TTL":{"Ref":"Ttl"}}}},"Tags":[{"Key":"k","Value":"v"}]}},
		"Http":{"Type":"AWS::ServiceDiscovery::HttpNamespace","Properties":{"Name":"cfn-http"}},
		"Public":{"Type":"AWS::ServiceDiscovery::PublicDnsNamespace","Properties":{"Name":"cfn.example.com"}},
		"Api":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"Name":"api","NamespaceId":{"Ref":"Private"},
			"Description":"` + description + `",
			"DnsConfig":{"RoutingPolicy":"MULTIVALUE","DnsRecords":[{"Type":"A","TTL":{"Ref":"Ttl"}}]},
			"HealthCheckCustomConfig":{"FailureThreshold":1}}},
		"Jobs":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"Name":"jobs","NamespaceId":{"Fn::GetAtt":["Http","Id"]}}},
		"Node":{"Type":"AWS::ServiceDiscovery::Instance","Properties":{"ServiceId":{"Ref":"Api"},"InstanceId":"node-1",
			"InstanceAttributes":{"AWS_INSTANCE_IPV4":"10.20.0.9","build":"` + description + `"}}}},
		"Outputs":{"NsId":{"Value":{"Ref":"Private"}},"NsArn":{"Value":{"Fn::GetAtt":["Private","Arn"]}},
			"Zone":{"Value":{"Fn::GetAtt":["Private","HostedZoneId"]}},"SvcId":{"Value":{"Ref":"Api"}},
			"SvcArn":{"Value":{"Fn::GetAtt":["Api","Arn"]}},"SvcName":{"Value":{"Fn::GetAtt":["Api","Name"]}},
			"Node":{"Value":{"Ref":"Node"}}}}`
}

// TestCFN_ServiceDiscoveryTypes deploys every Cloud Map type, updates in place, and deletes.
func TestCFN_ServiceDiscoveryTypes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "map", "TemplateBody": cfnSDTemplate("one", "30")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "map")
	require.Equal(t, "CREATE_COMPLETE", status, reason)

	nsID := cfnStackOutput(t, ts, "map", "NsId")
	assert.True(t, strings.HasPrefix(nsID, "ns-"), nsID)
	assert.True(t, strings.HasSuffix(cfnStackOutput(t, ts, "map", "NsArn"), ":namespace/"+nsID))
	zone := cfnStackOutput(t, ts, "map", "Zone")
	assert.True(t, strings.HasPrefix(zone, "Z"), zone)
	svcID := cfnStackOutput(t, ts, "map", "SvcId")
	assert.True(t, strings.HasSuffix(cfnStackOutput(t, ts, "map", "SvcArn"), ":service/"+svcID))
	assert.Equal(t, "api", cfnStackOutput(t, ts, "map", "SvcName"))
	assert.Equal(t, "node-1", cfnStackOutput(t, ts, "map", "Node"))

	ns := sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	assert.EqualValues(t, 30, ns["Properties"].(map[string]any)["DnsProperties"].(map[string]any)["SOA"].(map[string]any)["TTL"],
		"a Ref to a Number parameter is sent as a number")
	assert.Equal(t, []any{map[string]any{"Key": "k", "Value": "v"}}, sdOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": ns["Arn"]})["Tags"])
	inst := sdOK(t, ts, "GetInstance", map[string]string{"ServiceId": svcID, "InstanceId": "node-1"})["Instance"].(map[string]any)
	assert.Equal(t, "one", inst["Attributes"].(map[string]any)["build"])
	assert.Len(t, sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"], 3)

	// Description, Properties, a service's DnsConfig and InstanceAttributes are No interruption.
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "map", "TemplateBody": cfnSDTemplate("two", "45")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "map")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	assert.Equal(t, nsID, cfnStackOutput(t, ts, "map", "NsId"))
	assert.Equal(t, svcID, cfnStackOutput(t, ts, "map", "SvcId"))
	ns = sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	assert.Equal(t, "two", ns["Description"])
	assert.EqualValues(t, 45, ns["Properties"].(map[string]any)["DnsProperties"].(map[string]any)["SOA"].(map[string]any)["TTL"])
	svc := sdOK(t, ts, "GetService", map[string]string{"Id": svcID})["Service"].(map[string]any)
	assert.Equal(t, "two", svc["Description"])
	assert.EqualValues(t, 45, svc["DnsConfig"].(map[string]any)["DnsRecords"].([]any)[0].(map[string]any)["TTL"])
	inst = sdOK(t, ts, "GetInstance", map[string]string{"ServiceId": svcID, "InstanceId": "node-1"})["Instance"].(map[string]any)
	assert.Equal(t, "two", inst["Attributes"].(map[string]any)["build"])

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "map"})
	require.Equal(t, http.StatusOK, code, body)
	code, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "map"})
	assert.Equal(t, http.StatusBadRequest, code, "the stack deleted cleanly: %s", body)
	assert.Empty(t, sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"])
	assert.Empty(t, sdOK(t, ts, "ListServices", map[string]any{})["Services"])
	code, zoneBody := sjREST53(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zone)
	assert.Equal(t, http.StatusNotFound, code, zoneBody)
}

// TestCFN_ServiceDiscoveryFailures pins a seeded FAIL failing the stack, and a service whose
// ServiceAttributes substrate cannot apply.
func TestCFN_ServiceDiscoveryFailures(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sdSeed(t, ts, http.MethodPost, `{"operationId":"*","submittedObservations":2,"status":"FAIL","errorCode":"CANNOT_CREATE_HOSTED_ZONE","errorMessage":"no"}`, "")
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "failing",
		"TemplateBody": `{"Resources":{"N":{"Type":"AWS::ServiceDiscovery::HttpNamespace","Properties":{"Name":"f"}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "failing")
	assert.Equal(t, "ROLLBACK_COMPLETE", status)
	assert.Contains(t, reason, "CANNOT_CREATE_HOSTED_ZONE")
	sdSeed(t, ts, http.MethodDelete, "", "")

	code, body = cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "attrs",
		"TemplateBody": `{"Resources":{"N":{"Type":"AWS::ServiceDiscovery::HttpNamespace","Properties":{"Name":"a"}},
			"S":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"NamespaceId":{"Ref":"N"},"ServiceAttributes":{"Port":"80"}}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "attrs")
	assert.Equal(t, "ROLLBACK_COMPLETE", status)
	assert.Contains(t, reason, "ServiceAttributes")
}
