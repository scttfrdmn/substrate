package emulator_test

// The request-validation pilots beyond ACM (#1612): SESv2 (REST-JSON), Route 53 (REST-XML),
// ElastiCache (Query) and EC2's key-pair and placement-group operations (EC2). Every request is
// raw HTTP, because an SDK would refuse each invalid one before sending it.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// wirePilotCall is one raw request to a pilot service.
type wirePilotCall struct {
	method, host, path, contentType, body string
}

// sesv2Call builds a SESv2 request.
func sesv2Call(method, path, body string) wirePilotCall {
	return wirePilotCall{method, "email.us-east-1.amazonaws.com", path, "application/json", body}
}

// route53Call builds a Route 53 request.
func route53Call(method, path, body string) wirePilotCall {
	return wirePilotCall{method, "route53.amazonaws.com", path, "application/xml", body}
}

// wirePilotQuery builds a Query or EC2 request for host with the given action, version and parameters.
func wirePilotQuery(host, action, version string, params map[string]string) wirePilotCall {
	form := url.Values{"Action": {action}, "Version": {version}}
	for k, v := range params {
		form.Set(k, v)
	}
	return wirePilotCall{http.MethodPost, host, "/", "application/x-www-form-urlencoded", form.Encode()}
}

func elastiCacheCall(action string, params map[string]string) wirePilotCall {
	return wirePilotQuery("elasticache.us-east-1.amazonaws.com", action, "2015-02-02", params)
}

func ec2Call(action string, params map[string]string) wirePilotCall {
	return wirePilotQuery("ec2.us-east-1.amazonaws.com", action, "2016-11-15", params)
}

func (c wirePilotCall) do(t *testing.T, ts *emulator.TestServer) (int, []byte) {
	t.Helper()
	return replayDo(t, ts, c.method, c.path, c.host, []byte(c.body), map[string]string{"Content-Type": c.contentType})
}

var (
	wireXMLCode    = regexp.MustCompile(`<Code>([^<]*)</Code>`)
	wireXMLMessage = regexp.MustCompile(`<Message>([^<]*)</Message>`)
)

// wireErrorOf reads the code and message from a JSON or an XML error body.
func wireErrorOf(t *testing.T, body []byte) (string, string) {
	t.Helper()
	if strings.HasPrefix(strings.TrimSpace(string(body)), "{") {
		var e struct {
			Code    string `json:"Code"`
			Type    string `json:"__type"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(body, &e), string(body))
		if e.Code == "" {
			e.Code = e.Type
		}
		return e.Code, e.Message
	}
	code, message := wireXMLCode.FindSubmatch(body), wireXMLMessage.FindSubmatch(body)
	require.NotNil(t, code, string(body))
	require.NotNil(t, message, string(body))
	return string(code[1]), strings.ReplaceAll(string(message[1]), "&#39;", "'")
}

// wireHostedZone creates a hosted zone and returns its ID.
func wireHostedZone(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	status, body := route53Call(http.MethodPost, "/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>`+name+
			`</Name><CallerReference>ref-`+name+`</CallerReference></CreateHostedZoneRequest>`).do(t, ts)
	require.Equal(t, http.StatusCreated, status, string(body))
	m := regexp.MustCompile(`<Id>/hostedzone/([^<]+)</Id>`).FindSubmatch(body)
	require.NotNil(t, m, string(body))
	return string(m[1])
}

// wireChangeBatch is a one-change ChangeResourceRecordSets body.
func wireChangeBatch(rrType, ttl string) string {
	return `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch>` +
		`<Changes><Change><Action>CREATE</Action><ResourceRecordSet><Name>www.pilot.example.</Name><Type>` + rrType +
		`</Type><TTL>` + ttl + `</TTL><ResourceRecords><ResourceRecord><Value>192.0.2.1</Value></ResourceRecord>` +
		`</ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
}

func TestWireValidation_PilotsRefuseWhatTheModelForbids(t *testing.T) {
	ts := emulator.StartTestServer(t)
	zone := wireHostedZone(t, ts, "pilot.example")
	enumRR := "[SOA, A, TXT, NS, CNAME, MX, NAPTR, PTR, SRV, SPF, AAAA, CAA, DS, TLSA, SSHFP, SVCB, HTTPS]"

	for _, tc := range []struct {
		name, code, message string
		call                wirePilotCall
	}{
		// SESv2: BadRequestException, value left out.
		{"sesv2: a missing member", "BadRequestException",
			"1 validation error detected: Value null at 'emailIdentity' failed to satisfy constraint: Member must not be null",
			sesv2Call(http.MethodPost, "/v2/email/identities", `{}`)},
		{"sesv2: a string under its minimum", "BadRequestException",
			"1 validation error detected: Value at 'emailIdentity' failed to satisfy constraint: " +
				"Member must have length greater than or equal to 1",
			sesv2Call(http.MethodPost, "/v2/email/identities", `{"EmailIdentity":""}`)},
		{"sesv2: an over-length nested member", "BadRequestException",
			"1 validation error detected: Value at 'dkimSigningAttributes.domainSigningSelector' failed to satisfy " +
				"constraint: Member must have length less than or equal to 63",
			sesv2Call(http.MethodPost, "/v2/email/identities", `{"EmailIdentity":"pilot.example",`+
				`"DkimSigningAttributes":{"DomainSigningSelector":"`+strings.Repeat("s", 64)+`","DomainSigningPrivateKey":"k"}}`)},
		{"sesv2: an enum, which SESv2 opts into", "BadRequestException",
			"1 validation error detected: Value at 'dkimSigningAttributes.nextSigningKeyLength' failed to satisfy " +
				"constraint: Member must satisfy enum value set: [RSA_1024_BIT, RSA_2048_BIT]",
			sesv2Call(http.MethodPost, "/v2/email/identities", `{"EmailIdentity":"pilot.example",`+
				`"DkimSigningAttributes":{"NextSigningKeyLength":"RSA_9_BIT"}}`)},
		{"sesv2: SendEmail without Content", "BadRequestException",
			"1 validation error detected: Value null at 'content' failed to satisfy constraint: Member must not be null",
			sesv2Call(http.MethodPost, "/v2/email/outbound-emails", `{}`)},

		// Route 53: InvalidInput, value quoted.
		{"route53: a missing member", "InvalidInput",
			"1 validation error detected: Value null at 'callerReference' failed to satisfy constraint: " +
				"Member must not be null",
			route53Call(http.MethodPost, "/2013-04-01/hostedzone",
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>a.example</Name>`+
					`</CreateHostedZoneRequest>`)},
		{"route53: an over-length URI label", "InvalidInput",
			"1 validation error detected: Value '" + strings.Repeat("Z", 33) + "' at 'id' failed to satisfy " +
				"constraint: Member must have length less than or equal to 32",
			route53Call(http.MethodGet, "/2013-04-01/hostedzone/"+strings.Repeat("Z", 33), "")},
		{"route53: an out-of-range number, at AWS's path", "InvalidInput",
			"1 validation error detected: Value '3600000000000' at 'changeBatch.changes.1.member.resourceRecordSet.tTL' " +
				"failed to satisfy constraint: Member must have value less than or equal to 2147483647",
			route53Call(http.MethodPost, "/2013-04-01/hostedzone/"+zone+"/rrset/", wireChangeBatch("A", "3600000000000"))},
		{"route53: a query-string enum", "InvalidInput",
			"1 validation error detected: Value 'to' at 'startRecordType' failed to satisfy constraint: " +
				"Member must satisfy enum value set: " + enumRR,
			route53Call(http.MethodGet, "/2013-04-01/hostedzone/"+zone+"/rrset?type=to", "")},
		{"route53: a record type outside the enum", "InvalidInput",
			"1 validation error detected: Value 'a' at 'changeBatch.changes.1.member.resourceRecordSet.type' " +
				"failed to satisfy constraint: Member must satisfy enum value set: " + enumRR,
			route53Call(http.MethodPost, "/2013-04-01/hostedzone/"+zone+"/rrset/", wireChangeBatch("a", "300"))},

		// ElastiCache: InvalidParameterValue, its own sentence for a missing member.
		{"elasticache: a missing member", "InvalidParameterValue",
			"The parameter SubnetIds must be provided and must not be blank.",
			elastiCacheCall("CreateCacheSubnetGroup", map[string]string{
				"CacheSubnetGroupName": "g", "CacheSubnetGroupDescription": "d"})},
		{"elasticache: a blank member, answered by the plugin in the same words", "InvalidParameterValue",
			"The parameter CacheClusterId must be provided and must not be blank.",
			elastiCacheCall("CreateCacheCluster", map[string]string{"CacheClusterId": ""})},
		{"elasticache: an over-length member under a list element", "InvalidParameterValue",
			"1 validation error detected: Value '12345' at 'nodeGroupConfiguration.1.member.nodeGroupId' failed to " +
				"satisfy constraint: Member must have length less than or equal to 4",
			elastiCacheCall("CreateReplicationGroup", map[string]string{
				"ReplicationGroupId": "rg", "ReplicationGroupDescription": "d",
				"NodeGroupConfiguration.NodeGroupConfiguration.1.NodeGroupId": "12345"})},

		// EC2: MissingParameter, naming the parameter as it is sent.
		{"ec2: CreateKeyPair without KeyName", "MissingParameter",
			"The request must contain the parameter KeyName", ec2Call("CreateKeyPair", nil)},
		{"ec2: ImportKeyPair without PublicKeyMaterial", "MissingParameter",
			"The request must contain the parameter publicKeyMaterial",
			ec2Call("ImportKeyPair", map[string]string{"KeyName": "k"})},
		{"ec2: DeletePlacementGroup without GroupName", "MissingParameter",
			"The request must contain the parameter groupName", ec2Call("DeletePlacementGroup", nil)},
		{"ec2: a blank KeyName, answered by the plugin in the same words", "MissingParameter",
			"The request must contain the parameter KeyName", ec2Call("CreateKeyPair", map[string]string{"KeyName": ""})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tc.call.do(t, ts)
			require.Equal(t, http.StatusBadRequest, status, string(body))
			code, message := wireErrorOf(t, body)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.message, message)
		})
	}
}

// TestWireValidation_PilotsAnswerAValidSessionIdentically pins that the gate adds nothing to a
// request it lets through, on every pilot protocol.
func TestWireValidation_PilotsAnswerAValidSessionIdentically(t *testing.T) {
	epoch := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	session := func(ts *emulator.TestServer) []string {
		var out []string
		do := func(c wirePilotCall) {
			status, body := c.do(t, ts)
			require.Less(t, status, 300, "%s %s: %s", c.method, c.path, body)
			out = append(out, string(body))
		}
		do(sesv2Call(http.MethodPost, "/v2/email/identities", `{"EmailIdentity":"pilot.example",`+
			`"Tags":[{"Key":"env","Value":"prod"}]}`))
		do(sesv2Call(http.MethodGet, "/v2/email/identities/pilot.example", ""))
		zone := wireHostedZone(t, ts, "pilot.example")
		do(route53Call(http.MethodGet, "/2013-04-01/hostedzone/"+zone, ""))
		do(route53Call(http.MethodPost, "/2013-04-01/hostedzone/"+zone+"/rrset/", wireChangeBatch("A", "300")))
		do(route53Call(http.MethodGet, "/2013-04-01/hostedzone/"+zone+"/rrset?type=A", ""))
		do(elastiCacheCall("CreateCacheSubnetGroup", map[string]string{"CacheSubnetGroupName": "g",
			"CacheSubnetGroupDescription": "d", "SubnetIds.SubnetIdentifier.1": "subnet-0a1b2c3d"}))
		do(elastiCacheCall("DescribeCacheSubnetGroups", nil))
		do(ec2Call("CreateKeyPair", map[string]string{"KeyName": "k", "KeyType": "ed25519"}))
		do(ec2Call("DescribeKeyPairs", nil))
		do(ec2Call("CreatePlacementGroup", map[string]string{"GroupName": "pg", "Strategy": "spread"}))
		do(ec2Call("DeletePlacementGroup", map[string]string{"GroupName": "pg"}))
		return out
	}
	validated := session(emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch)))
	unvalidated := session(emulator.StartTestServer(t, emulator.WithDeterministicRun(epoch), emulator.WithoutWireValidation()))
	require.Len(t, unvalidated, len(validated))
	for i := range validated {
		assert.Equal(t, unvalidated[i], validated[i], "response %d", i)
	}
}

// TestWireValidation_TheEC2SliceIsOperationGranular pins that only the listed EC2 operations are
// validated: a request to any other EC2 operation is answered exactly as with validation off.
func TestWireValidation_TheEC2SliceIsOperationGranular(t *testing.T) {
	on := emulator.StartTestServer(t)
	off := emulator.StartTestServer(t, emulator.WithoutWireValidation())
	call := ec2Call("CreateVpc", nil) // CidrBlock is required by the model
	onStatus, onBody := call.do(t, on)
	offStatus, offBody := call.do(t, off)
	assert.Equal(t, offStatus, onStatus)
	onCode, onMessage := wireErrorOf(t, onBody)
	offCode, offMessage := wireErrorOf(t, offBody)
	assert.Equal(t, offCode, onCode)
	assert.Equal(t, offMessage, onMessage)
}

// TestWireValidation_AStacksElastiCacheDispatchIsNotValidated pins that a CloudFormation dispatch is
// not validated on a pilot service either: the stack's subnet group is created although the
// deployer sends no SubnetIds, which a client's request must carry.
func TestWireValidation_AStacksElastiCacheDispatchIsNotValidated(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfnCreateStack(t, ts, "wire-ec", `{"Resources":{"G":{"Type":"AWS::ElastiCache::SubnetGroup",`+
		`"Properties":{"CacheSubnetGroupName":"from-stack","Description":"d","SubnetIds":["subnet-0a1b2c3d"]}}}}`)
	status, body := elastiCacheCall("DescribeCacheSubnetGroups", map[string]string{"CacheSubnetGroupName": "from-stack"}).do(t, ts)
	require.Equal(t, http.StatusOK, status, string(body))
	assert.Contains(t, string(body), "from-stack")
}

// TestWireValidation_EC2NamesAnInvalidValue pins EC2's sentence for a value that breaks a
// constraint, which no operation of the slice can reach today.
func TestWireValidation_EC2NamesAnInvalidValue(t *testing.T) {
	code, message := emulator.WireEC2InvalidForTest("dsa", "KeyType")
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, "Value (dsa) for parameter KeyType is invalid.", message)
}

// TestWireValidation_PilotPluginChecksAnswerInTheGatesWords pins that each pilot's own check for
// an absent or empty required member, which a request the gate does not see reaches, answers
// what the gate answers, byte for byte.
func TestWireValidation_PilotPluginChecksAnswerInTheGatesWords(t *testing.T) {
	on := emulator.StartTestServer(t)
	off := emulator.StartTestServer(t, emulator.WithoutWireValidation())
	for _, tc := range []struct {
		name string
		call wirePilotCall
	}{
		{"sesv2: an absent EmailIdentity", sesv2Call(http.MethodPost, "/v2/email/identities", `{}`)},
		{"sesv2: an empty EmailIdentity", sesv2Call(http.MethodPost, "/v2/email/identities", `{"EmailIdentity":""}`)},
		{"route53: an absent Name", route53Call(http.MethodPost, "/2013-04-01/hostedzone",
			`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>r`+
				`</CallerReference></CreateHostedZoneRequest>`)},
		{"elasticache: an absent CacheClusterId", elastiCacheCall("CreateCacheCluster", nil)},
		{"ec2: an absent KeyName", ec2Call("CreateKeyPair", nil)},
		{"ec2: an absent GroupName", ec2Call("DeletePlacementGroup", nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			onStatus, onBody := tc.call.do(t, on)
			offStatus, offBody := tc.call.do(t, off)
			require.Equal(t, http.StatusBadRequest, offStatus, string(offBody))
			assert.Equal(t, onStatus, offStatus)
			onCode, onMessage := wireErrorOf(t, onBody)
			offCode, offMessage := wireErrorOf(t, offBody)
			assert.Equal(t, onCode, offCode)
			assert.Equal(t, onMessage, offMessage)
		})
	}
}
