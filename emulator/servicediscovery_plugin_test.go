package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdCall sends one Cloud Map request the way boto3 and the Terraform AWS provider (servicediscovery
// v1.49) do: application/x-amz-json-1.1 with the Route53AutoNaming_v20170314 target, to the
// emulator's own host, so the target is the only routing signal.
func sdCall(t *testing.T, ts *emulator.TestServer, op string, body any) (int, map[string]any, []byte) {
	t.Helper()
	return sdTargetCall(t, ts, "", "Route53AutoNaming_v20170314."+op, body)
}

// sdTargetCall posts a JSON 1.1 body with an X-Amz-Target, to host when it is set.
func sdTargetCall(t *testing.T, ts *emulator.TestServer, host, target string, body any) (int, map[string]any, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	if host != "" {
		req.Host = host
	}
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var doc map[string]any
	if len(got) > 0 {
		require.NoError(t, json.Unmarshal(got, &doc), "%s", got)
	}
	return resp.StatusCode, doc, got
}

// sdOK asserts a 200 and returns the decoded body.
func sdOK(t *testing.T, ts *emulator.TestServer, op string, body any) map[string]any {
	t.Helper()
	code, doc, raw := sdCall(t, ts, op, body)
	require.Equal(t, http.StatusOK, code, "%s: %s", op, raw)
	return doc
}

// sdErrCode returns the __type of a JSON error body.
func sdErrCode(doc map[string]any) string {
	s, _ := doc["__type"].(string)
	if i := strings.LastIndexByte(s, '#'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// sdWait polls GetOperation, as the Terraform provider's waitOperationSucceeded does, until the
// operation settles, and returns the settled operation and every status it reported.
func sdWait(t *testing.T, ts *emulator.TestServer, opID string) (map[string]any, []string) {
	t.Helper()
	var seen []string
	for range 20 {
		op := sdOK(t, ts, "GetOperation", map[string]string{"OperationId": opID})["Operation"].(map[string]any)
		status := op["Status"].(string)
		seen = append(seen, status)
		if status == "SUCCESS" || status == "FAIL" {
			return op, seen
		}
	}
	t.Fatalf("operation %s did not settle: %v", opID, seen)
	return nil, nil
}

// sdSeed posts or deletes an operation-status seed.
func sdSeed(t *testing.T, ts *emulator.TestServer, method, body, query string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+"/v1/servicediscovery/operation-status"+query, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", raw)
}

// sdDefaultVPC returns the Region's default VPC ID.
func sdDefaultVPC(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	_, body := singleAccountQuery(t, ts, "ec2", "", map[string]string{"Action": "DescribeVpcs", "Version": "2016-11-15"})
	i := strings.Index(body, "<vpcId>")
	require.Positive(t, i, body)
	rest := body[i+len("<vpcId>"):]
	return rest[:strings.Index(rest, "</vpcId>")]
}

// TestServiceDiscovery_TerraformPrivateDNSJourney walks the calls the Terraform AWS provider's
// aws_service_discovery_private_dns_namespace, aws_service_discovery_service and
// aws_service_discovery_instance make: create, wait on the operation, read back, update, destroy.
func TestServiceDiscovery_TerraformPrivateDNSJourney(t *testing.T) {
	ts := emulator.StartTestServer(t)
	vpc := sdDefaultVPC(t, ts)

	opID := sdOK(t, ts, "CreatePrivateDnsNamespace", map[string]any{
		"Name": "corp.internal", "Vpc": vpc, "CreatorRequestId": "tf-1", "Description": "internal",
		"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
	})["OperationId"].(string)
	op, seen := sdWait(t, ts, opID)
	assert.Equal(t, []string{"SUCCESS"}, seen, "an unseeded operation settles on its first observation")
	assert.Equal(t, "CREATE_NAMESPACE", op["Type"])
	nsID := op["Targets"].(map[string]any)["NAMESPACE"].(string)
	assert.Regexp(t, `^ns-[a-z0-9]{16}$`, nsID)

	ns := sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	assert.Equal(t, "DNS_PRIVATE", ns["Type"])
	assert.Equal(t, "corp.internal", ns["Name"])
	assert.Equal(t, "internal", ns["Description"])
	assert.Equal(t, "tf-1", ns["CreatorRequestId"])
	assert.Regexp(t, `^arn:aws:servicediscovery:us-east-1:\d{12}:namespace/`+nsID+`$`, ns["Arn"])
	props := ns["Properties"].(map[string]any)
	zone := props["DnsProperties"].(map[string]any)["HostedZoneId"].(string)
	assert.True(t, strings.HasPrefix(zone, "Z"), zone)
	assert.Equal(t, "corp.internal", props["HttpProperties"].(map[string]any)["HttpName"])

	// The namespace's hosted zone is a private Route 53 zone of the same name.
	code, zoneBody := sjREST53(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zone)
	require.Equal(t, http.StatusOK, code, zoneBody)
	assert.Contains(t, zoneBody, "<Name>corp.internal.</Name>")
	assert.Contains(t, zoneBody, "<PrivateZone>true</PrivateZone>")

	// A retry with the same CreatorRequestId answers the same operation; another is refused.
	retry := sdOK(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "corp.internal", "Vpc": vpc, "CreatorRequestId": "tf-1"})
	assert.Equal(t, opID, retry["OperationId"])
	code, doc, _ := sdCall(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "corp.internal", "Vpc": vpc, "CreatorRequestId": "tf-2"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "NamespaceAlreadyExists", sdErrCode(doc))

	// aws_service_discovery_service.
	svc := sdOK(t, ts, "CreateService", map[string]any{
		"Name": "api", "CreatorRequestId": "tf-svc",
		"DnsConfig": map[string]any{
			"NamespaceId": nsID, "RoutingPolicy": "MULTIVALUE",
			"DnsRecords": []map[string]any{{"Type": "A", "TTL": 10}},
		},
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
		"Tags":                    []map[string]string{{"Key": "team", "Value": "core"}},
	})["Service"].(map[string]any)
	svcID := svc["Id"].(string)
	assert.Regexp(t, `^srv-[a-z0-9]{16}$`, svcID)
	assert.Equal(t, "DNS_HTTP", svc["Type"])
	assert.Equal(t, nsID, svc["NamespaceId"])
	got := sdOK(t, ts, "GetService", map[string]string{"Id": svcID})["Service"].(map[string]any)
	assert.Equal(t, nsID, got["DnsConfig"].(map[string]any)["NamespaceId"])
	assert.EqualValues(t, 0, got["InstanceCount"])
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "core"}},
		sdOK(t, ts, "ListTagsForResource", map[string]string{"ResourceARN": got["Arn"].(string)})["Tags"])

	// aws_service_discovery_instance.
	regOp := sdOK(t, ts, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-1", "CreatorRequestId": "tf-i",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.5", "AWS_INIT_HEALTH_STATUS": "UNHEALTHY", "color": "blue"},
	})["OperationId"].(string)
	op, _ = sdWait(t, ts, regOp)
	assert.Equal(t, map[string]any{"INSTANCE": "i-1", "SERVICE": svcID}, op["Targets"])
	inst := sdOK(t, ts, "GetInstance", map[string]string{"ServiceId": svcID, "InstanceId": "i-1"})["Instance"].(map[string]any)
	assert.Equal(t, "blue", inst["Attributes"].(map[string]any)["color"])
	health := sdOK(t, ts, "GetInstancesHealthStatus", map[string]any{"ServiceId": svcID})["Status"]
	assert.Equal(t, map[string]any{"i-1": "UNHEALTHY"}, health)

	// DiscoverInstances on the data host: HEALTHY by default for a health-checked service.
	discover := func(extra map[string]any) map[string]any {
		body := map[string]any{"NamespaceName": "corp.internal", "ServiceName": "api"}
		for k, v := range extra {
			body[k] = v
		}
		code, doc, raw := sdTargetCall(t, ts, "data-servicediscovery.us-east-1.amazonaws.com", "Route53AutoNaming_v20170314.DiscoverInstances", body)
		require.Equal(t, http.StatusOK, code, "%s", raw)
		return doc
	}
	assert.Empty(t, discover(nil)["Instances"])
	assert.Len(t, discover(map[string]any{"HealthStatus": "HEALTHY_OR_ELSE_ALL"})["Instances"], 1)
	sdOK(t, ts, "UpdateInstanceCustomHealthStatus", map[string]string{"ServiceId": svcID, "InstanceId": "i-1", "Status": "HEALTHY"})
	found := discover(nil)
	require.Len(t, found["Instances"], 1)
	first := found["Instances"].([]any)[0].(map[string]any)
	assert.Equal(t, "HEALTHY", first["HealthStatus"])
	assert.Equal(t, "corp.internal", first["NamespaceName"])
	assert.EqualValues(t, 1, found["InstancesRevision"])
	assert.Empty(t, discover(map[string]any{"QueryParameters": map[string]string{"color": "red"}})["Instances"])
	assert.Len(t, discover(map[string]any{"OptionalParameters": map[string]string{"color": "red"}})["Instances"], 1,
		"an optional filter nothing matches is ignored")

	// Update the namespace and the service.
	upd := sdOK(t, ts, "UpdatePrivateDnsNamespace", map[string]any{
		"Id": nsID, "UpdaterRequestId": "tf-u",
		"Namespace": map[string]any{"Description": "changed", "Properties": map[string]any{"DnsProperties": map[string]any{"SOA": map[string]any{"TTL": 60}}}},
	})["OperationId"].(string)
	op, _ = sdWait(t, ts, upd)
	assert.Equal(t, "UPDATE_NAMESPACE", op["Type"])
	ns = sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	assert.Equal(t, "changed", ns["Description"])
	assert.EqualValues(t, 60, ns["Properties"].(map[string]any)["DnsProperties"].(map[string]any)["SOA"].(map[string]any)["TTL"])
	updSvc := sdOK(t, ts, "UpdateService", map[string]any{"Id": svcID, "Service": map[string]any{
		"Description": "the api", "DnsConfig": map[string]any{"DnsRecords": []map[string]any{{"Type": "A", "TTL": 30}}},
	}})["OperationId"].(string)
	sdWait(t, ts, updSvc)
	got = sdOK(t, ts, "GetService", map[string]string{"Id": svcID})["Service"].(map[string]any)
	assert.Equal(t, "the api", got["Description"])
	assert.EqualValues(t, 30, got["DnsConfig"].(map[string]any)["DnsRecords"].([]any)[0].(map[string]any)["TTL"])
	assert.EqualValues(t, 1, got["InstanceCount"])

	// Destroy in Terraform's order; each delete is refused while it still holds children.
	code, doc, _ = sdCall(t, ts, "DeleteService", map[string]string{"Id": svcID})
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "ResourceInUse", sdErrCode(doc))
	dereg := sdOK(t, ts, "DeregisterInstance", map[string]string{"ServiceId": svcID, "InstanceId": "i-1"})["OperationId"].(string)
	sdWait(t, ts, dereg)
	code, doc, _ = sdCall(t, ts, "GetInstance", map[string]string{"ServiceId": svcID, "InstanceId": "i-1"})
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "InstanceNotFound", sdErrCode(doc))
	assert.EqualValues(t, 2, discover(map[string]any{"HealthStatus": "ALL"})["InstancesRevision"])
	code, doc, _ = sdCall(t, ts, "DeleteNamespace", map[string]string{"Id": nsID})
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "ResourceInUse", sdErrCode(doc))
	sdOK(t, ts, "DeleteService", map[string]string{"Id": got["Arn"].(string)})
	del := sdOK(t, ts, "DeleteNamespace", map[string]string{"Id": nsID})["OperationId"].(string)
	op, _ = sdWait(t, ts, del)
	assert.Equal(t, "DELETE_NAMESPACE", op["Type"])
	code, doc, _ = sdCall(t, ts, "GetNamespace", map[string]string{"Id": nsID})
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "NamespaceNotFound", sdErrCode(doc))
	code, zoneBody = sjREST53(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zone)
	assert.Equal(t, http.StatusNotFound, code, "the hosted zone goes with the namespace: %s", zoneBody)

	ops := sdOK(t, ts, "ListOperations", map[string]any{
		"Filters": []map[string]any{{"Name": "TYPE", "Condition": "IN", "Values": []string{"CREATE_NAMESPACE", "DELETE_NAMESPACE"}}},
	})["Operations"].([]any)
	assert.Equal(t, []any{
		map[string]any{"Id": opID, "Status": "SUCCESS"}, map[string]any{"Id": del, "Status": "SUCCESS"},
	}, ops)
}

// sjREST53 sends a Route 53 REST request and returns its status and body.
func sjREST53(t *testing.T, ts *emulator.TestServer, method, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, http.NoBody)
	require.NoError(t, err)
	req.Host = "route53.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// TestServiceDiscovery_SeededOperations pins the operation-status seed: a countdown of transient
// observations, and a FAIL that withholds the operation's effect.
func TestServiceDiscovery_SeededOperations(t *testing.T) {
	ts := emulator.StartTestServer(t)

	sdSeed(t, ts, http.MethodPost, `{"operationId":"*","submittedObservations":2}`, "")
	opID := sdOK(t, ts, "CreateHttpNamespace", map[string]any{"Name": "apps"})["OperationId"].(string)
	_, seen := sdWait(t, ts, opID)
	assert.Equal(t, []string{"SUBMITTED", "SUBMITTED", "SUCCESS"}, seen)
	listed := sdOK(t, ts, "ListOperations", map[string]any{
		"Filters": []map[string]any{{"Name": "STATUS", "Values": []string{"SUCCESS"}}},
	})["Operations"].([]any)
	require.Len(t, listed, 1, "ListOperations peeks and reports the settled status")

	sdSeed(t, ts, http.MethodPost, `{"operationId":"*","submittedObservations":1,"transientStatus":"PENDING",
		"status":"FAIL","errorCode":"CANNOT_CREATE_HOSTED_ZONE","errorMessage":"seeded"}`, "")
	failed := sdOK(t, ts, "CreateHttpNamespace", map[string]any{"Name": "never"})["OperationId"].(string)
	op, seen := sdWait(t, ts, failed)
	assert.Equal(t, []string{"PENDING", "FAIL"}, seen)
	assert.Equal(t, "CANNOT_CREATE_HOSTED_ZONE", op["ErrorCode"])
	assert.Equal(t, "seeded", op["ErrorMessage"])
	nsID := op["Targets"].(map[string]any)["NAMESPACE"].(string)
	code, doc, _ := sdCall(t, ts, "GetNamespace", map[string]string{"Id": nsID})
	assert.Equal(t, http.StatusNotFound, code, "a failed create creates nothing")
	assert.Equal(t, "NamespaceNotFound", sdErrCode(doc))

	// The first namespace's delete fails too, and leaves it.
	ns := sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"].([]any)
	require.Len(t, ns, 1)
	appsID := ns[0].(map[string]any)["Id"].(string)
	delOp := sdOK(t, ts, "DeleteNamespace", map[string]string{"Id": appsID})["OperationId"].(string)
	op, _ = sdWait(t, ts, delOp)
	assert.Equal(t, "FAIL", op["Status"])
	sdOK(t, ts, "GetNamespace", map[string]string{"Id": appsID})

	// Cleared, an operation settles at once and succeeds; one started under the FAIL seed keeps it.
	sdSeed(t, ts, http.MethodDelete, "", "")
	_, seen = sdWait(t, ts, failed)
	assert.Equal(t, []string{"FAIL"}, seen)
	delOp = sdOK(t, ts, "DeleteNamespace", map[string]string{"Id": appsID})["OperationId"].(string)
	_, seen = sdWait(t, ts, delOp)
	assert.Equal(t, []string{"SUCCESS"}, seen)
	assert.Empty(t, sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"])

	// An ID-scoped seed governs that operation's countdown alone.
	created := sdOK(t, ts, "CreateHttpNamespace", map[string]any{"Name": "scoped"})["OperationId"].(string)
	sdSeed(t, ts, http.MethodPost, `{"operationId":"`+created+`","submittedObservations":1}`, "")
	_, seen = sdWait(t, ts, created)
	assert.Equal(t, []string{"SUBMITTED", "SUCCESS"}, seen)
	sdSeed(t, ts, http.MethodDelete, "", "?operationId="+created)
	_, seen = sdWait(t, ts, created)
	assert.Equal(t, []string{"SUCCESS"}, seen)

	for _, bad := range []string{
		`{"operationId":"*","status":"SUBMITTED"}`, `{"operationId":"*","transientStatus":"SUCCESS"}`,
		`{"operationId":"*","errorCode":"X"}`, `{"operationId":"*","submittedObservations":-1}`,
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/servicediscovery/operation-status", strings.NewReader(bad))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, bad)
	}
}

// TestServiceDiscovery_HTTPNamespaceAndListing pins an HTTP namespace and service, the listing
// filters and paging, and the tagging API's view of both.
func TestServiceDiscovery_HTTPNamespaceAndListing(t *testing.T) {
	ts := emulator.StartTestServer(t)
	var ids []string
	for _, name := range []string{"beta", "alpha", "gamma"} {
		op := sdOK(t, ts, "CreateHttpNamespace", map[string]any{"Name": name})["OperationId"].(string)
		done, _ := sdWait(t, ts, op)
		ids = append(ids, done["Targets"].(map[string]any)["NAMESPACE"].(string))
	}
	first := sdOK(t, ts, "ListNamespaces", map[string]any{"MaxResults": 2})
	page := first["Namespaces"].([]any)
	require.Len(t, page, 2)
	assert.Equal(t, "alpha", page[0].(map[string]any)["Name"])
	assert.NotContains(t, page[0], "CreatorRequestId", "a summary has no CreatorRequestId")
	assert.Nil(t, page[0].(map[string]any)["Properties"].(map[string]any)["DnsProperties"])
	rest := sdOK(t, ts, "ListNamespaces", map[string]any{"MaxResults": 2, "NextToken": first["NextToken"]})
	require.Len(t, rest["Namespaces"], 1)
	assert.Equal(t, "gamma", rest["Namespaces"].([]any)[0].(map[string]any)["Name"])

	byName := sdOK(t, ts, "ListNamespaces", map[string]any{"Filters": []map[string]any{
		{"Name": "NAME", "Condition": "BEGINS_WITH", "Values": []string{"g"}},
		{"Name": "TYPE", "Values": []string{"HTTP"}},
	}})["Namespaces"].([]any)
	require.Len(t, byName, 1)
	assert.Equal(t, "gamma", byName[0].(map[string]any)["Name"])

	svc := sdOK(t, ts, "CreateService", map[string]any{"Name": "worker", "NamespaceId": ids[0]})["Service"].(map[string]any)
	assert.Equal(t, "HTTP", svc["Type"])
	sdOK(t, ts, "CreateService", map[string]any{"Name": "other", "NamespaceId": ids[1]})
	listed := sdOK(t, ts, "ListServices", map[string]any{"Filters": []map[string]any{
		{"Name": "NAMESPACE_ID", "Values": []string{ids[0]}},
	}})["Services"].([]any)
	require.Len(t, listed, 1)
	assert.Equal(t, "worker", listed[0].(map[string]any)["Name"])
	assert.NotContains(t, listed[0], "NamespaceId", "a ServiceSummary has no NamespaceId")
	ns := sdOK(t, ts, "GetNamespace", map[string]string{"Id": ids[0]})["Namespace"].(map[string]any)
	assert.EqualValues(t, 1, ns["ServiceCount"])

	for i := range 3 {
		op := sdOK(t, ts, "RegisterInstance", map[string]any{"ServiceId": svc["Id"], "InstanceId": "w-" + string(rune('a'+i)),
			"Attributes": map[string]string{"slot": string(rune('0' + i))}})["OperationId"].(string)
		sdWait(t, ts, op)
	}
	instances := sdOK(t, ts, "ListInstances", map[string]any{"ServiceId": svc["Id"], "MaxResults": 2})
	require.Len(t, instances["Instances"], 2)
	more := sdOK(t, ts, "ListInstances", map[string]any{"ServiceId": svc["Id"], "MaxResults": 2, "NextToken": instances["NextToken"]})
	require.Len(t, more["Instances"], 1)
	health := sdOK(t, ts, "GetInstancesHealthStatus", map[string]any{"ServiceId": svc["Id"], "Instances": []string{"w-a"}})["Status"]
	assert.Equal(t, map[string]any{"w-a": "UNKNOWN"}, health, "no health check is UNKNOWN")
	code, doc, raw := sdTargetCall(t, ts, "", "Route53AutoNaming_v20170314.DiscoverInstances",
		map[string]any{"NamespaceName": "beta", "ServiceName": "worker", "MaxResults": 2, "HealthStatus": "UNHEALTHY"})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.Len(t, doc["Instances"], 2, "HealthStatus is ignored without a health check, and MaxResults caps the list")

	// Tags, through the service and through the tagging API.
	arn := ns["Arn"].(string)
	sdOK(t, ts, "TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}}})
	sdOK(t, ts, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"a"}})
	assert.Equal(t, []any{map[string]any{"Key": "b", "Value": "2"}}, sdOK(t, ts, "ListTagsForResource", map[string]string{"ResourceARN": arn})["Tags"])
	code, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{arn, svc["Arn"].(string)}, "Tags": map[string]string{"cost": "x"},
	})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.Equal(t, []any{map[string]any{"Key": "cost", "Value": "x"}}, sdOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": svc["Arn"]})["Tags"])
	_, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.GetResources", map[string]any{})
	assert.Contains(t, string(raw), arn)
	assert.Contains(t, string(raw), svc["Arn"].(string))
	_, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:servicediscovery:us-east-1:123456789012:namespace/ns-missing"}, "Tags": map[string]string{"a": "b"},
	})
	assert.Contains(t, string(raw), "FailedResourcesMap")
	assert.Contains(t, string(raw), "ns-missing")
}

// TestServiceDiscovery_ResponsesCarryNoBookkeepingMember asserts on the raw bytes of every response
// that renders a namespace or a service, once both carry the previously-tagged flag.
func TestServiceDiscovery_ResponsesCarryNoBookkeepingMember(t *testing.T) {
	ts := emulator.StartTestServer(t)
	op := sdOK(t, ts, "CreatePublicDnsNamespace", map[string]any{"Name": "example.com",
		"Properties": map[string]any{"DnsProperties": map[string]any{"SOA": map[string]any{"TTL": 100}}}})["OperationId"].(string)
	done, _ := sdWait(t, ts, op)
	nsID := done["Targets"].(map[string]any)["NAMESPACE"].(string)
	svc := sdOK(t, ts, "CreateService", map[string]any{"Name": "www", "NamespaceId": nsID,
		"DnsConfig": map[string]any{"RoutingPolicy": "WEIGHTED", "DnsRecords": []map[string]any{{"Type": "CNAME", "TTL": 60}}},
		"Tags":      []map[string]string{{"Key": "a", "Value": "b"}},
	})["Service"].(map[string]any)
	ns := sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	for _, arn := range []string{ns["Arn"].(string), svc["Arn"].(string)} {
		sdOK(t, ts, "TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "t", "Value": "v"}}})
		sdOK(t, ts, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"t", "a"}})
	}
	for op, body := range map[string]any{
		"GetNamespace":   map[string]string{"Id": nsID},
		"ListNamespaces": map[string]any{},
		"GetService":     map[string]string{"Id": svc["Id"].(string)},
		"ListServices":   map[string]any{},
	} {
		_, _, raw := sdCall(t, ts, op, body)
		require.Contains(t, string(raw), `"Arn":"`, "%s answered a record: %s", op, raw)
		for _, member := range []string{"ever_tagged", "Tags", "Revision", "CreateOperationId", "Vpc", "SOATTL", "HostedZoneID"} {
			assert.NotContains(t, string(raw), `"`+member+`"`, "%s: %s", op, raw)
		}
	}
	_, _, raw := sdCall(t, ts, "GetNamespace", map[string]string{"Id": nsID})
	assert.Contains(t, string(raw), `"SOA":{"TTL":100}`)
	_, _, raw = sdCall(t, ts, "ListTagsForResource", map[string]string{"ResourceARN": ns["Arn"].(string)})
	assert.JSONEq(t, `{"Tags":[]}`, string(raw))
}

// TestServiceDiscovery_Refusals pins each refusal's code and status.
func TestServiceDiscovery_Refusals(t *testing.T) {
	ts := emulator.StartTestServer(t)
	vpc := sdDefaultVPC(t, ts)
	httpOp := sdOK(t, ts, "CreateHttpNamespace", map[string]any{"Name": "h"})["OperationId"].(string)
	done, _ := sdWait(t, ts, httpOp)
	httpNS := done["Targets"].(map[string]any)["NAMESPACE"].(string)
	dnsOp := sdOK(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "d.internal", "Vpc": vpc})["OperationId"].(string)
	done, _ = sdWait(t, ts, dnsOp)
	dnsNS := done["Targets"].(map[string]any)["NAMESPACE"].(string)
	plain := sdOK(t, ts, "CreateService", map[string]any{"Name": "plain", "NamespaceId": httpNS})["Service"].(map[string]any)["Id"].(string)
	dnsSvc := sdOK(t, ts, "CreateService", map[string]any{"Name": "srv", "NamespaceId": dnsNS,
		"DnsConfig": map[string]any{"DnsRecords": []map[string]any{{"Type": "SRV", "TTL": 5}}}, "HealthCheckConfig": map[string]any{"Type": "TCP"},
	})["Service"].(map[string]any)["Id"].(string)
	rec := func(typ string) []map[string]any { return []map[string]any{{"Type": typ, "TTL": 5}} }

	cases := []struct {
		name, op string
		body     any
		code     string
		status   int
	}{
		{"no namespace name", "CreateHttpNamespace", map[string]any{}, "InvalidInput", 400},
		{"arn namespace name", "CreateHttpNamespace", map[string]any{"Name": "arn:x"}, "InvalidInput", 400},
		{"bad public name", "CreatePublicDnsNamespace", map[string]any{"Name": "not_a_domain"}, "InvalidInput", 400},
		{"private without vpc", "CreatePrivateDnsNamespace", map[string]any{"Name": "x.internal"}, "InvalidInput", 400},
		{"reserved tag", "CreateHttpNamespace", map[string]any{"Name": "t", "Tags": []map[string]string{{"Key": "aws:x", "Value": "y"}}}, "InvalidInput", 400},
		{"unknown namespace", "GetNamespace", map[string]string{"Id": "ns-nope"}, "NamespaceNotFound", 404},
		{"no namespace id", "GetNamespace", map[string]string{}, "InvalidInput", 400},
		{"update another type", "UpdateHttpNamespace", map[string]any{"Id": dnsNS, "Namespace": map[string]string{"Description": "x"}}, "InvalidInput", 400},
		{"update without change", "UpdatePrivateDnsNamespace", map[string]any{"Id": dnsNS}, "InvalidInput", 400},
		{"update http without description", "UpdateHttpNamespace", map[string]any{"Id": httpNS, "Namespace": map[string]any{}}, "InvalidInput", 400},
		{"service without namespace", "CreateService", map[string]any{"Name": "x"}, "InvalidInput", 400},
		{"service in a missing namespace", "CreateService", map[string]any{"Name": "x", "NamespaceId": "ns-nope"}, "NamespaceNotFound", 404},
		{"bad service name", "CreateService", map[string]any{"Name": "-bad-", "NamespaceId": httpNS}, "InvalidInput", 400},
		{"service type", "CreateService", map[string]any{"Name": "x", "NamespaceId": httpNS, "Type": "DNS"}, "InvalidInput", 400},
		{"dns config in an http namespace", "CreateService", map[string]any{"Name": "x", "NamespaceId": httpNS, "DnsConfig": map[string]any{"DnsRecords": rec("A")}}, "InvalidInput", 400},
		{"dns namespace without dns config", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS}, "InvalidInput", 400},
		{"record combination", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": []map[string]any{{"Type": "A", "TTL": 1}, {"Type": "SRV", "TTL": 1}}}}, "InvalidInput", 400},
		{"record type", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": rec("MX")}}, "InvalidInput", 400},
		{"record without TTL", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": []map[string]any{{"Type": "A"}}}}, "InvalidInput", 400},
		{"no records", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": []any{}}}, "InvalidInput", 400},
		{"cname multivalue", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": rec("CNAME")}}, "InvalidInput", 400},
		{"cname health check", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"RoutingPolicy": "WEIGHTED", "DnsRecords": rec("CNAME")}, "HealthCheckConfig": map[string]string{"Type": "HTTP"}}, "InvalidInput", 400},
		{"routing policy", "CreateService", map[string]any{"Name": "x", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"RoutingPolicy": "LATENCY", "DnsRecords": rec("A")}}, "InvalidInput", 400},
		{"both health checks", "CreateService", map[string]any{"Name": "x", "NamespaceId": httpNS, "HealthCheckConfig": map[string]string{"Type": "HTTP"}, "HealthCheckCustomConfig": map[string]any{}}, "InvalidInput", 400},
		{"health check type", "CreateService", map[string]any{"Name": "x", "NamespaceId": httpNS, "HealthCheckConfig": map[string]string{"Type": "ICMP"}}, "InvalidInput", 400},
		{"failure threshold", "CreateService", map[string]any{"Name": "x", "NamespaceId": httpNS, "HealthCheckCustomConfig": map[string]any{"FailureThreshold": 11}}, "InvalidInput", 400},
		{"duplicate service", "CreateService", map[string]any{"Name": "PLAIN", "NamespaceId": httpNS}, "", 200},
		{"duplicate dns service by case", "CreateService", map[string]any{"Name": "SRV", "NamespaceId": dnsNS, "DnsConfig": map[string]any{"DnsRecords": rec("A")}}, "ServiceAlreadyExists", 400},
		{"unknown service", "GetService", map[string]string{"Id": "srv-nope"}, "ServiceNotFound", 404},
		{"update without change", "UpdateService", map[string]any{"Id": plain}, "InvalidInput", 400},
		{"update dns of an http service", "UpdateService", map[string]any{"Id": plain, "Service": map[string]any{"DnsConfig": map[string]any{"DnsRecords": rec("A")}}}, "InvalidInput", 400},
		{"update health check type", "UpdateService", map[string]any{"Id": plain, "Service": map[string]any{"HealthCheckConfig": map[string]string{"Type": "X"}}}, "InvalidInput", 400},
		{"instance id", "RegisterInstance", map[string]any{"ServiceId": plain, "InstanceId": "bad id", "Attributes": map[string]string{}}, "InvalidInput", 400},
		{"no attributes", "RegisterInstance", map[string]any{"ServiceId": plain, "InstanceId": "i"}, "InvalidInput", 400},
		{"srv needs a port", "RegisterInstance", map[string]any{"ServiceId": dnsSvc, "InstanceId": "i", "Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"}}, "InvalidInput", 400},
		{"srv needs an address", "RegisterInstance", map[string]any{"ServiceId": dnsSvc, "InstanceId": "i", "Attributes": map[string]string{"AWS_INSTANCE_PORT": "80"}}, "InvalidInput", 400},
		{"init health", "RegisterInstance", map[string]any{"ServiceId": plain, "InstanceId": "i", "Attributes": map[string]string{"AWS_INIT_HEALTH_STATUS": "MAYBE"}}, "InvalidInput", 400},
		{"ec2 id in a dns namespace", "RegisterInstance", map[string]any{"ServiceId": dnsSvc, "InstanceId": "i", "Attributes": map[string]string{"AWS_EC2_INSTANCE_ID": "i-1"}}, "InvalidInput", 400},
		{"attribute value", "RegisterInstance", map[string]any{"ServiceId": plain, "InstanceId": "i", "Attributes": map[string]string{"k": strings.Repeat("v", 1025)}}, "InvalidInput", 400},
		{"unknown instance", "DeregisterInstance", map[string]string{"ServiceId": plain, "InstanceId": "nope"}, "InstanceNotFound", 404},
		{"health of an unknown instance", "GetInstancesHealthStatus", map[string]any{"ServiceId": plain, "Instances": []string{"nope"}}, "InstanceNotFound", 404},
		{"custom health status value", "UpdateInstanceCustomHealthStatus", map[string]string{"ServiceId": plain, "InstanceId": "x", "Status": "MAYBE"}, "InvalidInput", 400},
		{"unknown operation", "GetOperation", map[string]string{"OperationId": "nope"}, "OperationNotFound", 404},
		{"no operation id", "GetOperation", map[string]string{}, "InvalidInput", 400},
		{"filter name", "ListNamespaces", map[string]any{"Filters": []map[string]any{{"Name": "COLOR", "Values": []string{"x"}}}}, "InvalidInput", 400},
		{"filter condition", "ListServices", map[string]any{"Filters": []map[string]any{{"Name": "NAMESPACE_ID", "Condition": "BETWEEN", "Values": []string{"x"}}}}, "InvalidInput", 400},
		{"filter values", "ListOperations", map[string]any{"Filters": []map[string]any{{"Name": "STATUS", "Values": []string{"A", "B"}}}}, "InvalidInput", 400},
		{"between values", "ListOperations", map[string]any{"Filters": []map[string]any{{"Name": "UPDATE_DATE", "Condition": "BETWEEN", "Values": []string{"1"}}}}, "InvalidInput", 400},
		{"empty filter", "ListOperations", map[string]any{"Filters": []map[string]any{{"Name": "TYPE", "Values": []string{}}}}, "InvalidInput", 400},
		{"page size", "ListNamespaces", map[string]any{"MaxResults": 101}, "InvalidInput", 400},
		{"bad token", "ListServices", map[string]any{"NextToken": "nope"}, "InvalidInput", 400},
		{"discover without names", "DiscoverInstances", map[string]any{}, "InvalidInput", 400},
		{"discover health filter", "DiscoverInstances", map[string]any{"NamespaceName": "h", "ServiceName": "plain", "HealthStatus": "SOME"}, "InvalidInput", 400},
		{"discover unknown namespace", "DiscoverInstances", map[string]any{"NamespaceName": "nope", "ServiceName": "plain"}, "NamespaceNotFound", 404},
		{"discover unknown service", "DiscoverInstances", map[string]any{"NamespaceName": "h", "ServiceName": "nope"}, "ServiceNotFound", 404},
		{"tag a bad ARN", "TagResource", map[string]any{"ResourceARN": "arn:aws:s3:::b", "Tags": []map[string]string{{"Key": "a", "Value": "b"}}}, "InvalidInput", 400},
		{"tag an unknown namespace", "TagResource", map[string]any{"ResourceARN": "arn:aws:servicediscovery:us-east-1:123456789012:namespace/ns-nope", "Tags": []map[string]string{{"Key": "a", "Value": "b"}}}, "ResourceNotFoundException", 404},
		{"tag another region", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:servicediscovery:eu-west-1:123456789012:service/srv-x"}, "ResourceNotFoundException", 404},
		{"tag without tags", "TagResource", map[string]any{"ResourceARN": "arn:aws:servicediscovery:us-east-1:123456789012:namespace/ns-x"}, "InvalidInput", 400},
		{"untag without keys", "UntagResource", map[string]any{"ResourceARN": "arn:aws:servicediscovery:us-east-1:123456789012:namespace/ns-x"}, "InvalidInput", 400},
		{"list tags without an ARN", "ListTagsForResource", map[string]any{}, "InvalidInput", 400},
		{"unrouted operation", "GetServiceAttributes", map[string]any{}, "UnknownOperationException", 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, doc, raw := sdCall(t, ts, tc.op, tc.body)
			assert.Equal(t, tc.status, code, "%s", raw)
			if tc.code != "" {
				assert.Equal(t, tc.code, sdErrCode(doc), "%s", raw)
			}
		})
	}

	// A custom health status needs a custom health check.
	op := sdOK(t, ts, "RegisterInstance", map[string]any{"ServiceId": plain, "InstanceId": "p1", "Attributes": map[string]string{}})["OperationId"].(string)
	sdWait(t, ts, op)
	code, doc, _ := sdCall(t, ts, "UpdateInstanceCustomHealthStatus", map[string]string{"ServiceId": plain, "InstanceId": "p1", "Status": "HEALTHY"})
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "CustomHealthNotFound", sdErrCode(doc))

	// A route 53 health check reports HEALTHY, and its service needs a port.
	op = sdOK(t, ts, "RegisterInstance", map[string]any{"ServiceId": dnsSvc, "InstanceId": "s1",
		"Attributes": map[string]string{"AWS_INSTANCE_PORT": "80", "AWS_INSTANCE_IPV6": "::1"}})["OperationId"].(string)
	sdWait(t, ts, op)
	assert.Equal(t, map[string]any{"s1": "HEALTHY"}, sdOK(t, ts, "GetInstancesHealthStatus", map[string]any{"ServiceId": dnsSvc})["Status"])
}

// TestServiceDiscovery_MalformedBodies pins the refusal of a body that is not JSON.
func TestServiceDiscovery_MalformedBodies(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, op := range []string{
		"CreateHttpNamespace", "GetNamespace", "ListNamespaces", "DeleteNamespace", "UpdateHttpNamespace",
		"CreateService", "GetService", "ListServices", "UpdateService", "DeleteService", "RegisterInstance",
		"DeregisterInstance", "GetInstance", "ListInstances", "DiscoverInstances", "GetInstancesHealthStatus",
		"UpdateInstanceCustomHealthStatus", "GetOperation", "ListOperations", "TagResource", "UntagResource",
		"ListTagsForResource",
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader("{not json"))
		require.NoError(t, err)
		req.Header.Set("X-Amz-Target", "Route53AutoNaming_v20170314."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s: %s", op, raw)
		assert.Contains(t, string(raw), "SerializationException", op)
	}
}

// TestServiceDiscovery_ZoneRefusalFiltersAndTagQuota pins a namespace delete Route 53 refuses, the
// operation filters, and the tag quota through both APIs.
func TestServiceDiscovery_ZoneRefusalFiltersAndTagQuota(t *testing.T) {
	ts := emulator.StartTestServer(t)
	op := sdOK(t, ts, "CreatePublicDnsNamespace", map[string]any{"Name": "busy.example.org"})["OperationId"].(string)
	done, _ := sdWait(t, ts, op)
	nsID := done["Targets"].(map[string]any)["NAMESPACE"].(string)
	ns := sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})["Namespace"].(map[string]any)
	zone := ns["Properties"].(map[string]any)["DnsProperties"].(map[string]any)["HostedZoneId"].(string)

	// A record the caller added keeps the zone, and so the namespace, from being deleted.
	change := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action>` +
		`<ResourceRecordSet><Name>www.busy.example.org.</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>192.0.2.1</Value></ResourceRecord></ResourceRecords></ResourceRecordSet>` +
		`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/2013-04-01/hostedzone/"+zone+"/rrset/", strings.NewReader(change))
	require.NoError(t, err)
	req.Host = "route53.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", raw)

	delOp := sdOK(t, ts, "DeleteNamespace", map[string]string{"Id": ns["Arn"].(string)})["OperationId"].(string)
	failed, _ := sdWait(t, ts, delOp)
	assert.Equal(t, "FAIL", failed["Status"])
	assert.Equal(t, "HostedZoneNotEmpty", failed["ErrorCode"])
	sdOK(t, ts, "GetNamespace", map[string]string{"Id": nsID})

	// The operation filters.
	svc := sdOK(t, ts, "CreateService", map[string]any{"Name": "w", "NamespaceId": ns["Arn"],
		"DnsConfig": map[string]any{"DnsRecords": []map[string]any{{"Type": "AAAA", "TTL": 1}}}})["Service"].(map[string]any)
	reg := sdOK(t, ts, "RegisterInstance", map[string]any{"ServiceId": svc["Arn"], "InstanceId": "x",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV6": "::1"}})["OperationId"].(string)
	list := func(filters ...map[string]any) []any {
		return sdOK(t, ts, "ListOperations", map[string]any{"Filters": filters})["Operations"].([]any)
	}
	assert.Len(t, list(map[string]any{"Name": "NAMESPACE_ID", "Values": []string{nsID}}), 2)
	assert.Equal(t, []any{map[string]any{"Id": reg, "Status": "SUCCESS"}}, list(map[string]any{"Name": "SERVICE_ID", "Values": []string{svc["Id"].(string)}}))
	assert.Len(t, list(map[string]any{"Name": "STATUS", "Values": []string{"FAIL"}}), 1)
	assert.Len(t, list(map[string]any{"Name": "UPDATE_DATE", "Condition": "BETWEEN", "Values": []string{"0", "99999999999"}}), 3)
	assert.Empty(t, list(map[string]any{"Name": "UPDATE_DATE", "Condition": "BETWEEN", "Values": []string{"0", "1"}}))
	page := sdOK(t, ts, "ListOperations", map[string]any{"MaxResults": 2})
	require.Len(t, page["Operations"], 2)
	assert.Len(t, sdOK(t, ts, "ListOperations", map[string]any{"MaxResults": 2, "NextToken": page["NextToken"]})["Operations"], 1)
	assert.Len(t, sdOK(t, ts, "ListServices", map[string]any{"Filters": []map[string]any{
		{"Name": "NAMESPACE_ID", "Condition": "IN", "Values": []string{ns["Arn"].(string), "ns-other"}}}})["Services"], 1)

	// Fifty tags fit; the fifty-first is TooManyTagsException through either API.
	tags := make([]map[string]string, 0, 50)
	for i := range 50 {
		tags = append(tags, map[string]string{"Key": "k" + string(rune('A'+i%26)) + string(rune('a'+i/26)), "Value": "v"})
	}
	sdOK(t, ts, "TagResource", map[string]any{"ResourceARN": svc["Arn"], "Tags": tags})
	code, doc, _ := sdCall(t, ts, "TagResource", map[string]any{"ResourceARN": svc["Arn"], "Tags": []map[string]string{{"Key": "more", "Value": "x"}}})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "TooManyTagsException", sdErrCode(doc))
	_, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{svc["Arn"].(string)}, "Tags": map[string]string{"more": "x"},
	})
	assert.Contains(t, string(raw), "TooManyTagsException")
	_, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{ns["Arn"].(string)}, "Tags": map[string]string{"aws:x": "y"},
	})
	assert.Contains(t, string(raw), "InvalidParameterException")
	_, _, raw = sdTargetCall(t, ts, "", "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:servicediscovery:us-east-1:123456789012:instance/x"}, "Tags": map[string]string{"a": "b"},
	})
	assert.Contains(t, string(raw), "FailedResourcesMap")
}
