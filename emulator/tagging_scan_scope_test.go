package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetResources reported another account's and another Region's resources (#937).
//
// Three scanners narrowed their state-key scan by nothing at all, so an S3 bucket, a Lambda function
// or an SQS queue in any account was reported to any caller; fourteen more narrowed by account
// without the Region, so a us-east-1 caller was reported a us-west-2 EC2 instance, API, state
// machine, activity, ECR repository, user pool, Kinesis stream, RDS instance, cluster or subnet
// group, cache cluster, file system, Glue database or DynamoDB table. GetResources "[r]eturns all the
// tagged or previously tagged resources that are located in the specified AWS Region for the
// account", which is the sentence these tests are measured against.
//
// Every resource here is created through its owning service's own operation against the endpoint of
// the Region it is meant to be in, and every assertion is made through a real GetResources call to
// one Region's endpoint as one account. That is what makes the Region and the account come from the
// wire: a helper that wrote the record directly would choose the very fields the scope is read from,
// so it could only prove the test agrees with itself. Four services are covered by an equivalent
// per-Region test already and are not repeated here — ACM and CloudFront in
// tagging_acm_cloudfront_test.go, ECS in tagging_ecs_test.go, SSM in tagging_ssm_test.go.
//
// One dependency worth naming: none of these resources is tagged, because GetResources reports an
// untagged resource today. AWS omits one, which is #938; when that lands every row here needs a tag
// written before the assertion, and the row that fails will say which.

const (
	scanScopeEast = "us-east-1"
	scanScopeWest = "us-west-2"
)

// scanScopeServer starts a server that both taggingTestAccount and taggingForeignAccount can sign
// as, which is the only way to be a second caller.
func scanScopeServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount, taggingForeignAccount))
}

// scanScopeSigned sends one signed request to a Region-bearing host and returns the body and status.
//
// It is the primitive under the three protocol helpers below, because the seventeen services here
// span all three of substrate's request protocols and only the Host and the credential are common to
// them. The Region travels in the Host — extractRegion reads the Host first and the credential scope
// only as a fallback — so signing for the same Region the Host names keeps the two from disagreeing
// about what is being asserted.
func scanScopeSigned(t *testing.T, ts *emulator.TestServer, account, method, host, signingName,
	region, path string, body []byte, contentType string,
) ([]byte, int) {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	require.Truef(t, ok, "no credential registered for account %s", account)

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoErrorf(t, err, "build %s %s", method, path)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(method, path, host, signingName, region,
		sigV4TestDateTime, body, creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	require.NoErrorf(t, err, "%s %s on %s", method, path, host)
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoErrorf(t, err, "read %s %s body", method, path)
	return raw, resp.StatusCode
}

// scanScopeJSON posts a JSON-1.1 operation to one Region's endpoint and decodes a success body.
func scanScopeJSON(t *testing.T, ts *emulator.TestServer, account, service, region, targetPrefix,
	op string, body, out any,
) {
	t.Helper()
	host := service + "." + region + ".amazonaws.com"
	data, err := json.Marshal(body)
	require.NoErrorf(t, err, "marshal %s", op)

	raw, status := scanScopeSignedTarget(t, ts, account, host, service, region, targetPrefix, op, data)
	require.Equalf(t, http.StatusOK, status, "%s on %s: %s", op, host, raw)
	if out != nil {
		require.NoErrorf(t, json.Unmarshal(raw, out), "decode %s: %s", op, raw)
	}
}

// scanScopeSignedTarget adds the X-Amz-Target header a JSON-1.1 request carries its operation in.
func scanScopeSignedTarget(t *testing.T, ts *emulator.TestServer, account, host, signingName,
	region, targetPrefix, op string, body []byte,
) ([]byte, int) {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	require.Truef(t, ok, "no credential registered for account %s", account)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(body))
	require.NoErrorf(t, err, "build %s request", op)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", targetPrefix+"."+op)
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(http.MethodPost, "/", host, signingName, region,
		sigV4TestDateTime, body, creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	require.NoErrorf(t, err, "%s on %s", op, host)
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoErrorf(t, err, "read %s body", op)
	return raw, resp.StatusCode
}

// scanScopeQuery posts a query-protocol action to one Region's endpoint and decodes the XML result.
func scanScopeQuery(t *testing.T, ts *emulator.TestServer, account, service, region string,
	params map[string]string, out any,
) {
	t.Helper()
	host := service + "." + region + ".amazonaws.com"
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	encoded := form.Encode()

	raw, status := scanScopeSigned(t, ts, account, http.MethodPost, host, service, region, "/",
		[]byte(encoded), "application/x-www-form-urlencoded")

	var errDoc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Code    string   `xml:"Error>Code"`
	}
	if unmarshalErr := xml.Unmarshal(raw, &errDoc); unmarshalErr == nil && errDoc.Code != "" {
		t.Fatalf("%s on %s: %s (status %d, body %s)", params["Action"], host, errDoc.Code, status, raw)
	}
	require.Equalf(t, http.StatusOK, status, "%s on %s: %s", params["Action"], host, raw)
	if out != nil {
		require.NoErrorf(t, xml.Unmarshal(raw, out), "decode %s: %s", params["Action"], raw)
	}
}

// scanScopeREST sends a rest-json request to one Region's endpoint and decodes a success body.
func scanScopeREST(t *testing.T, ts *emulator.TestServer, account, service, region, method,
	path string, body, out any,
) {
	t.Helper()
	host := service + "." + region + ".amazonaws.com"

	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		require.NoErrorf(t, err, "marshal %s %s", method, path)
	}
	raw, status := scanScopeSigned(t, ts, account, method, host, service, region, path, data,
		"application/json")
	require.Truef(t, status >= http.StatusOK && status < http.StatusMultipleChoices,
		"%s %s on %s: status %d, body %s", method, path, host, status, raw)
	if out != nil {
		require.NoErrorf(t, json.Unmarshal(raw, out), "decode %s %s: %s", method, path, raw)
	}
}

// scanScopeARNs calls GetResources against one Region's endpoint as one account.
//
// getResourcesARNsIn covers the Region half but is pinned to taggingTestAccount, and the account
// half of the scope cannot be asserted without being a second caller.
func scanScopeARNs(t *testing.T, ts *emulator.TestServer, account, region string) []string {
	t.Helper()
	tgt := signedRequestTarget{
		host:        "tagging." + region + ".amazonaws.com",
		target:      "ResourceGroupsTaggingAPI_20170126",
		signingName: "tagging",
	}
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
		} `json:"ResourceTagMappingList"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, account, "GetResources", map[string]any{}), &out)
	require.Emptyf(t, errCode, "GetResources in %s as %s", region, account)
	require.Equalf(t, http.StatusOK, status, "GetResources in %s as %s", region, account)

	arns := make([]string, 0, len(out.ResourceTagMappingList))
	for _, rm := range out.ResourceTagMappingList {
		arns = append(arns, rm.ResourceARN)
	}
	return arns
}

// scanScopeMatches returns the reported ARNs containing marker.
func scanScopeMatches(arns []string, marker string) []string {
	var out []string
	for _, arn := range arns {
		if strings.Contains(arn, marker) {
			out = append(out, arn)
		}
	}
	return out
}

// scanScopeService is one row of the per-service scope table.
//
// create returns a marker that identifies the resource it made within a listing — the ARN the owning
// operation reported where it reports one, and the caller-chosen name where the ARN is built from it.
// A marker rather than a composed ARN, because composing one here would let the assertion agree with
// a scanner that disagreed with what the owning service hands a caller, which is the divergence #826
// through #932 spent nine issues on.
type scanScopeService struct {
	name   string
	create func(t *testing.T, ts *emulator.TestServer, account, region, id string) string
}

// scanScopeServices is every scanner whose scope #937 changed, plus the three the ARN filter now
// holds on its own.
var scanScopeServices = []scanScopeService{
	{
		name: "s3 bucket",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			raw, status := scanScopeSigned(t, ts, account, http.MethodPut,
				"s3."+region+".amazonaws.com", "s3", region, "/"+id, nil, "")
			require.Equalf(t, http.StatusOK, status, "CreateBucket %s in %s: %s", id, region, raw)
			return id
		},
	},
	{
		name: "lambda function",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				FunctionArn string `json:"FunctionArn"`
			}
			scanScopeREST(t, ts, account, "lambda", region, http.MethodPost,
				"/2015-03-31/functions", map[string]any{
					"FunctionName": id,
					"Runtime":      "python3.12",
					"Role":         "arn:aws:iam::" + account + ":role/lambda-exec",
					"Handler":      "index.handler",
					"Code":         map[string]any{"ZipFile": "ZHVtbXk="},
				}, &out)
			require.NotEmptyf(t, out.FunctionArn, "CreateFunction %s reports an ARN", id)
			return out.FunctionArn
		},
	},
	{
		name: "sqs queue",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				QueueURL string `json:"QueueUrl"`
			}
			scanScopeJSON(t, ts, account, "sqs", region, "AmazonSQS", "CreateQueue",
				map[string]any{"QueueName": id}, &out)
			require.NotEmptyf(t, out.QueueURL, "CreateQueue %s reports a URL", id)
			return id
		},
	},
	{
		name: "dynamodb table",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				TableDescription struct {
					TableArn string `json:"TableArn"`
				} `json:"TableDescription"`
			}
			scanScopeJSON(t, ts, account, "dynamodb", region, "DynamoDB_20120810", "CreateTable",
				map[string]any{
					"TableName":            id,
					"BillingMode":          "PAY_PER_REQUEST",
					"KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
					"AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
				}, &out)
			require.NotEmptyf(t, out.TableDescription.TableArn, "CreateTable %s reports an ARN", id)
			return out.TableDescription.TableArn
		},
	},
	{
		name: "ec2 instance",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				Instances []struct {
					InstanceID string `xml:"instanceId"`
				} `xml:"instancesSet>item"`
			}
			scanScopeQuery(t, ts, account, "ec2", region, map[string]string{
				"Action":  "RunInstances",
				"Version": "2016-11-15",
				// A bundled image ID rather than a literal, because an AMI ID is minted per
				// Region and RunInstances answers InvalidAMIID.NotFound for one this Region
				// does not have — which is the whole reason a Region-scoped launch is
				// assertable here at all.
				"ImageId": emulator.BundledImageID(region,
					"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"),
				"InstanceType": "t3.micro",
				"MinCount":     "1",
				"MaxCount":     "1",
			}, &out)
			require.Lenf(t, out.Instances, 1, "RunInstances reports one instance")
			require.NotEmptyf(t, out.Instances[0].InstanceID, "RunInstances reports an instance ID")
			return out.Instances[0].InstanceID
		},
	},
	{
		name: "apigateway rest api",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				ID string `json:"id"`
			}
			scanScopeREST(t, ts, account, "apigateway", region, http.MethodPost, "/restapis",
				map[string]any{"name": id}, &out)
			require.NotEmptyf(t, out.ID, "CreateRestApi %s reports an ID", id)
			return "/restapis/" + out.ID
		},
	},
	{
		name: "step functions state machine",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				StateMachineArn string `json:"stateMachineArn"`
			}
			scanScopeJSON(t, ts, account, "states", region, "AWSStepFunctions", "CreateStateMachine",
				map[string]any{
					"name":       id,
					"definition": `{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
					"roleArn":    "arn:aws:iam::" + account + ":role/states-exec",
				}, &out)
			require.NotEmptyf(t, out.StateMachineArn, "CreateStateMachine %s reports an ARN", id)
			return out.StateMachineArn
		},
	},
	{
		name: "step functions activity",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				ActivityArn string `json:"activityArn"`
			}
			scanScopeJSON(t, ts, account, "states", region, "AWSStepFunctions", "CreateActivity",
				map[string]any{"name": id}, &out)
			require.NotEmptyf(t, out.ActivityArn, "CreateActivity %s reports an ARN", id)
			return out.ActivityArn
		},
	},
	{
		name: "ecr repository",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				Repository struct {
					RepositoryArn string `json:"repositoryArn"`
				} `json:"repository"`
			}
			scanScopeJSON(t, ts, account, "ecr", region, "AmazonEC2ContainerRegistry_V20150921",
				"CreateRepository", map[string]any{"repositoryName": id}, &out)
			require.NotEmptyf(t, out.Repository.RepositoryArn, "CreateRepository %s reports an ARN", id)
			return out.Repository.RepositoryArn
		},
	},
	{
		name: "cognito user pool",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				UserPool struct {
					Arn string `json:"Arn"`
				} `json:"UserPool"`
			}
			scanScopeJSON(t, ts, account, "cognito-idp", region,
				"AWSCognitoIdentityProviderService", "CreateUserPool",
				map[string]any{"PoolName": id}, &out)
			require.NotEmptyf(t, out.UserPool.Arn, "CreateUserPool %s reports an ARN", id)
			return out.UserPool.Arn
		},
	},
	{
		name: "kinesis stream",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			scanScopeJSON(t, ts, account, "kinesis", region, "Kinesis_20131202", "CreateStream",
				map[string]any{"StreamName": id, "ShardCount": 1}, nil)
			return ":stream/" + id
		},
	},
	{
		name: "rds db instance",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				ARN string `xml:"CreateDBInstanceResult>DBInstance>DBInstanceArn"`
			}
			scanScopeQuery(t, ts, account, "rds", region, map[string]string{
				"Action":               "CreateDBInstance",
				"DBInstanceIdentifier": id,
				"DBInstanceClass":      "db.t3.micro",
				"Engine":               "postgres",
				"MasterUsername":       "admin",
			}, &out)
			require.NotEmptyf(t, out.ARN, "CreateDBInstance %s reports an ARN", id)
			return out.ARN
		},
	},
	{
		name: "rds db cluster",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				ARN string `xml:"CreateDBClusterResult>DBCluster>DBClusterArn"`
			}
			scanScopeQuery(t, ts, account, "rds", region, map[string]string{
				"Action":              "CreateDBCluster",
				"DBClusterIdentifier": id,
				"Engine":              "aurora-postgresql",
				"MasterUsername":      "admin",
			}, &out)
			require.NotEmptyf(t, out.ARN, "CreateDBCluster %s reports an ARN", id)
			return out.ARN
		},
	},
	{
		name: "rds db subnet group",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				ARN string `xml:"CreateDBSubnetGroupResult>DBSubnetGroup>DBSubnetGroupArn"`
			}
			scanScopeQuery(t, ts, account, "rds", region, map[string]string{
				"Action":                   "CreateDBSubnetGroup",
				"DBSubnetGroupName":        id,
				"DBSubnetGroupDescription": "subnets for " + id,
				"VpcId":                    "vpc-0123456789abcdef0",
			}, &out)
			require.NotEmptyf(t, out.ARN, "CreateDBSubnetGroup %s reports an ARN", id)
			return out.ARN
		},
	},
	{
		name: "elasticache cache cluster",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			scanScopeQuery(t, ts, account, "elasticache", region, map[string]string{
				"Action":         "CreateCacheCluster",
				"CacheClusterId": id,
				"CacheNodeType":  "cache.t3.micro",
				"Engine":         "redis",
				"NumCacheNodes":  "1",
			}, nil)
			return ":cluster:" + id
		},
	},
	{
		name: "efs file system",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			var out struct {
				FileSystemID string `json:"FileSystemId"`
			}
			scanScopeREST(t, ts, account, "elasticfilesystem", region, http.MethodPost,
				"/2015-02-01/file-systems", map[string]any{"CreationToken": id}, &out)
			require.NotEmptyf(t, out.FileSystemID, "CreateFileSystem %s reports an ID", id)
			return out.FileSystemID
		},
	},
	{
		name: "glue database",
		create: func(t *testing.T, ts *emulator.TestServer, account, region, id string) string {
			t.Helper()
			scanScopeJSON(t, ts, account, "glue", region, "AWSGlue", "CreateDatabase",
				map[string]any{"DatabaseInput": map[string]any{"Name": id}}, nil)
			return ":database/" + id
		},
	},
}

// TestTaggingScanScope_AResourceIsReportedOnlyInItsOwnRegion is #937's Region half.
//
// Each row creates one resource in us-east-1 and another in us-west-2 through the same operation
// against two endpoints, then asserts each Region's GetResources reports its own and not the other's.
// The positive half matters as much as the negative one: "the other Region's resource is absent"
// would pass against an endpoint that reported nothing at all, so both directions are asserted from
// both Regions.
func TestTaggingScanScope_AResourceIsReportedOnlyInItsOwnRegion(t *testing.T) {
	for _, svc := range scanScopeServices {
		t.Run(svc.name, func(t *testing.T) {
			ts := scanScopeServer(t)

			east := svc.create(t, ts, taggingTestAccount, scanScopeEast, "scope-east-1")
			west := svc.create(t, ts, taggingTestAccount, scanScopeWest, "scope-west-1")
			require.NotEqual(t, east, west, "the two resources are distinguishable in a listing")

			eastARNs := scanScopeARNs(t, ts, taggingTestAccount, scanScopeEast)
			westARNs := scanScopeARNs(t, ts, taggingTestAccount, scanScopeWest)

			assert.Lenf(t, scanScopeMatches(eastARNs, east), 1,
				"us-east-1 reports its own resource: %v", eastARNs)
			assert.Emptyf(t, scanScopeMatches(eastARNs, west),
				"us-east-1 does not report the us-west-2 resource: %v", eastARNs)

			assert.Lenf(t, scanScopeMatches(westARNs, west), 1,
				"us-west-2 reports its own resource: %v", westARNs)
			assert.Emptyf(t, scanScopeMatches(westARNs, east),
				"us-west-2 does not report the us-east-1 resource: %v", westARNs)
		})
	}
}

// TestTaggingScanScope_AResourceIsReportedOnlyToItsOwnAccount is #937's account half.
//
// It runs over every service rather than only the three scanners that narrowed by nothing, because
// the scope is now held by one filter over the reported ARN and the point is that no scanner can opt
// out of it. Two resources are created with different names: an S3 bucket name is globally unique,
// and a Lambda function's state key is "function:{name}" with no account in it, so two accounts
// creating one name would collide in state rather than be reported to the wrong caller. That
// collision is the write-side residue of this issue and is filed as #943.
func TestTaggingScanScope_AResourceIsReportedOnlyToItsOwnAccount(t *testing.T) {
	for _, svc := range scanScopeServices {
		t.Run(svc.name, func(t *testing.T) {
			ts := scanScopeServer(t)

			own := svc.create(t, ts, taggingTestAccount, scanScopeEast, "scope-own-1")
			foreign := svc.create(t, ts, taggingForeignAccount, scanScopeEast, "scope-other-1")
			require.NotEqual(t, own, foreign, "the two resources are distinguishable in a listing")

			ownARNs := scanScopeARNs(t, ts, taggingTestAccount, scanScopeEast)
			foreignARNs := scanScopeARNs(t, ts, taggingForeignAccount, scanScopeEast)

			assert.Lenf(t, scanScopeMatches(ownARNs, own), 1,
				"the owning account is reported its own resource: %v", ownARNs)
			assert.Emptyf(t, scanScopeMatches(ownARNs, foreign),
				"the owning account is not reported the other account's resource: %v", ownARNs)

			assert.Lenf(t, scanScopeMatches(foreignARNs, foreign), 1,
				"the other account is reported its own resource: %v", foreignARNs)
			assert.Emptyf(t, scanScopeMatches(foreignARNs, own),
				"the other account is not reported the owning account's resource: %v", foreignARNs)
		})
	}
}

// TestTaggingScanScope_AGlobalResourceIsReportedInEveryRegion is the exemption, asserted rather than
// assumed.
//
// An IAM ARN is arn:aws:iam::{account}:user/{name} and carries no Region, so the filter's rule that
// an empty segment states no scope is what keeps an IAM user visible to a caller in any Region. Left
// untested, a filter that treated an empty Region as "not my Region" would make every IAM entity
// invisible to GetResources everywhere, which no per-Region test would notice.
func TestTaggingScanScope_AGlobalResourceIsReportedInEveryRegion(t *testing.T) {
	ts := scanScopeServer(t)

	status, code, _ := iamScopeResult(t, ts, taggingTestAccount, "CreateUser",
		map[string]string{"UserName": "scope-global-1"})
	require.Emptyf(t, code, "CreateUser: %d", status)
	require.Equal(t, http.StatusOK, status, "CreateUser")

	const marker = ":user/scope-global-1"
	for _, region := range []string{scanScopeEast, scanScopeWest} {
		arns := scanScopeARNs(t, ts, taggingTestAccount, region)
		assert.Lenf(t, scanScopeMatches(arns, marker), 1,
			"GetResources in %s reports the IAM user: %v", region, arns)
	}

	// The other account is still a different caller, so the account half of the rule applies to a
	// Region-less ARN exactly as it does to a Region-bearing one.
	foreign := scanScopeARNs(t, ts, taggingForeignAccount, scanScopeEast)
	assert.Emptyf(t, scanScopeMatches(foreign, marker),
		"another account is not reported the IAM user: %v", foreign)
}
