package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// elbErrorCode decodes the query-protocol error body ELB answers with and returns its
// Code. It fails the test if the response was not an error.
func elbErrorCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	var body struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode elb error body: %v", err)
	}
	if body.Error.Code == "" {
		t.Fatalf("expected an error body, got status %d with no Code", resp.StatusCode)
	}
	return body.Error.Code
}

// elbCreateLB creates a load balancer and returns its ARN.
func elbCreateLB(t *testing.T, ts *httptest.Server, name string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{"Action": "CreateLoadBalancer", "Name": name, "Type": "application"}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLoadBalancer %s", name)

	var result struct {
		Result struct {
			LoadBalancers []struct {
				LoadBalancerArn string `xml:"LoadBalancerArn"`
			} `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Result.LoadBalancers, 1)
	return result.Result.LoadBalancers[0].LoadBalancerArn
}

// elbCreateTG creates a target group and returns its ARN.
func elbCreateTG(t *testing.T, ts *httptest.Server, name string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{"Action": "CreateTargetGroup", "Name": name, "Protocol": "HTTP", "Port": "80"}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateTargetGroup %s", name)

	var result struct {
		Result struct {
			TargetGroups []struct {
				TargetGroupArn string `xml:"TargetGroupArn"`
			} `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Result.TargetGroups, 1)
	return result.Result.TargetGroups[0].TargetGroupArn
}

// elbCreateListener creates a listener on lbARN and returns its ARN.
func elbCreateListener(t *testing.T, ts *httptest.Server, lbARN, tgARN string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action":                                 "CreateListener",
		"LoadBalancerArn":                        lbARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateListener")

	var result struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Result.Listeners, 1)
	return result.Result.Listeners[0].ListenerArn
}

// elbCreateRule creates a rule on listenerARN and returns its ARN.
func elbCreateRule(t *testing.T, ts *httptest.Server, listenerARN, tgARN string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action":                              "CreateRule",
		"ListenerArn":                         listenerARN,
		"Priority":                            "10",
		"Conditions.member.1.Field":           "path-pattern",
		"Conditions.member.1.Values.member.1": "/api/*",
		"Actions.member.1.Type":               "forward",
		"Actions.member.1.TargetGroupArn":     tgARN,
	}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateRule")

	var result struct {
		Result struct {
			Rules []struct {
				RuleArn string `xml:"RuleArn"`
			} `xml:"Rules>member"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Result.Rules, 1)
	return result.Result.Rules[0].RuleArn
}

// elbDescribeTags reads the tags on the named resources, keyed by ARN.
func elbDescribeTags(t *testing.T, ts *httptest.Server, arns ...string) map[string]map[string]string {
	t.Helper()
	params := map[string]string{"Action": "DescribeTags"}
	for i, arn := range arns {
		params["ResourceArns.member."+strconv.Itoa(i+1)] = arn
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "DescribeTags")

	var result struct {
		Result struct {
			TagDescriptions []struct {
				ResourceArn string `xml:"ResourceArn"`
				Tags        []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"Tags>member"`
			} `xml:"TagDescriptions>member"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))

	out := make(map[string]map[string]string, len(result.Result.TagDescriptions))
	for _, td := range result.Result.TagDescriptions {
		tags := make(map[string]string, len(td.Tags))
		for _, tag := range td.Tags {
			tags[tag.Key] = tag.Value
		}
		out[td.ResourceArn] = tags
	}
	return out
}

func TestELB_AddTags_DescribeTags_RoundTrip(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "tagged-alb", nil)

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": arn,
		"Tags.member.1.Key":     "env",
		"Tags.member.1.Value":   "prod",
		"Tags.member.2.Key":     "team",
		"Tags.member.2.Value":   "",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	tags := elbDescribeTags(t, ts, arn)
	// An empty value is a legal tag, not a terminator: Tag.Value has a documented minimum
	// length of 0, so "team" must be present rather than having ended the walk.
	assert.Equal(t, map[string]string{"env": "prod", "team": ""}, tags[arn])
}

func TestELB_AddTags_UpdatesExistingValue(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "retag-alb", map[string]string{
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "dev",
	})

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": arn,
		"Tags.member.1.Key":     "env",
		"Tags.member.1.Value":   "prod",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// AWS: "If a tag with the same key is already associated with the resource, AddTags
	// updates its value" — one tag, not two.
	assert.Equal(t, map[string]string{"env": "prod"}, elbDescribeTags(t, ts, arn)[arn])
}

func TestELB_AddTags_AcrossSeveralResources(t *testing.T) {
	ts := newELBTestServer(t)
	lbARN := elbCreateLB(t, ts, "multi-alb", nil)
	tgARN := elbCreateTG(t, ts, "multi-tg", nil)

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": lbARN,
		"ResourceArns.member.2": tgARN,
		"Tags.member.1.Key":     "owner",
		"Tags.member.1.Value":   "platform",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	tags := elbDescribeTags(t, ts, lbARN, tgARN)
	assert.Equal(t, map[string]string{"owner": "platform"}, tags[lbARN])
	assert.Equal(t, map[string]string{"owner": "platform"}, tags[tgARN])
}

// TestELB_AddTags_AppliesNothingWhenOneARNIsUnknown pins the resolve-everything pre-pass:
// a request naming one good and one absent resource must leave the good one untouched.
func TestELB_AddTags_AppliesNothingWhenOneARNIsUnknown(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "atomic-alb", nil)

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": arn,
		"ResourceArns.member.2": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/ghost/0abcdef",
		"Tags.member.1.Key":     "env",
		"Tags.member.1.Value":   "prod",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "LoadBalancerNotFound", elbErrorCode(t, resp))

	assert.Empty(t, elbDescribeTags(t, ts, arn)[arn], "the resolvable resource must be untouched")
}

func TestELB_AddTags_Errors(t *testing.T) {
	ts := newELBTestServer(t)
	lbARN := elbCreateLB(t, ts, "err-alb", nil)

	longKey := strings.Repeat("k", 129)
	longValue := strings.Repeat("v", 257)

	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "no ResourceArns",
			params: map[string]string{"Tags.member.1.Key": "env"},
			code:   "ValidationError",
		},
		{
			name:   "no Tags",
			params: map[string]string{"ResourceArns.member.1": lbARN},
			code:   "ValidationError",
		},
		{
			name: "duplicate key",
			params: map[string]string{
				"ResourceArns.member.1": lbARN,
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "a",
				"Tags.member.2.Key": "env", "Tags.member.2.Value": "b",
			},
			code: "DuplicateTagKeys",
		},
		{
			name: "key over 128 characters",
			params: map[string]string{
				"ResourceArns.member.1": lbARN,
				"Tags.member.1.Key":     longKey, "Tags.member.1.Value": "v",
			},
			code: "ValidationError",
		},
		{
			name: "value over 256 characters",
			params: map[string]string{
				"ResourceArns.member.1": lbARN,
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": longValue,
			},
			code: "ValidationError",
		},
		{
			name: "key outside the permitted character set",
			params: map[string]string{
				"ResourceArns.member.1": lbARN,
				"Tags.member.1.Key":     "env!", "Tags.member.1.Value": "v",
			},
			code: "ValidationError",
		},
		{
			name: "value outside the permitted character set",
			params: map[string]string{
				"ResourceArns.member.1": lbARN,
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "pr#od",
			},
			code: "ValidationError",
		},
		{
			name: "ARN naming no ELB resource type",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:s3:::my-bucket",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "ValidationError",
		},
		{
			name: "absent load balancer",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nope/0aaaaaaa",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "LoadBalancerNotFound",
		},
		{
			name: "absent target group",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/nope/0aaaaaaa",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "TargetGroupNotFound",
		},
		{
			name: "absent listener",
			params: map[string]string{
				"ResourceArns.member.1": lbARN + "/listener/0aaaaaaa",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "ListenerNotFound",
		},
		{
			name: "absent rule",
			params: map[string]string{
				"ResourceArns.member.1": lbARN + "/listener/0aaaaaaa/rule/0bbbbbbb",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "RuleNotFound",
		},
		{
			name: "absent listener under AWS's own flat ARN shape",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/nope/0aaaaaaa/0bbbbbbb",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "ListenerNotFound",
		},
		{
			name: "absent rule under AWS's own flat ARN shape",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener-rule/app/nope/0aaaaaaa/0bbbbbbb/0ccccccc",
				"Tags.member.1.Key":     "env", "Tags.member.1.Value": "v",
			},
			code: "RuleNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{"Action": "AddTags"}
			for k, v := range tt.params {
				params[k] = v
			}
			resp := elbRequest(t, ts, params)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, tt.code, elbErrorCode(t, resp))
		})
	}
}

func TestELB_AddTags_TooManyTags(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "limit-alb", nil)

	params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": arn}
	for i := 1; i <= 51; i++ {
		params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "TooManyTags", elbErrorCode(t, resp))
}

// TestELB_AddTags_ReservedPrefixDoesNotCountAgainstTheLimit pins the exclusion the ELB
// restrictions publish — "Tags with this prefix do not count against your tags per resource
// limit" — and the deliberate absence of a refusal for the prefix itself.
func TestELB_AddTags_ReservedPrefixDoesNotCountAgainstTheLimit(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "reserved-alb", nil)

	params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": arn}
	for i := 1; i <= 50; i++ {
		params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	params["Tags.member.51.Key"] = "aws:cloudformation:stack-name"
	params["Tags.member.51.Value"] = "my-stack"

	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "50 user tags plus one aws: tag is within the limit")

	tags := elbDescribeTags(t, ts, arn)[arn]
	assert.Len(t, tags, 51)
	assert.Equal(t, "my-stack", tags["aws:cloudformation:stack-name"])
}

// TestELB_AddTags_ReTaggingAtTheLimitSucceeds pins that the count is over the post-merge key
// set: a resource already holding 50 tags accepts a new value for one of them.
func TestELB_AddTags_ReTaggingAtTheLimitSucceeds(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "full-alb", nil)

	params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": arn}
	for i := 1; i <= 50; i++ {
		params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	first := elbRequest(t, ts, params)
	defer first.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, first.StatusCode)

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": arn,
		"Tags.member.1.Key":     "k7",
		"Tags.member.1.Value":   "changed",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "changed", elbDescribeTags(t, ts, arn)[arn]["k7"])
}

func TestELB_RemoveTags(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "untag-alb", map[string]string{
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
		"Tags.member.2.Key": "team", "Tags.member.2.Value": "platform",
	})

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "RemoveTags",
		"ResourceArns.member.1": arn,
		"TagKeys.member.1":      "env",
		// A key that is not present is silently ignored: RemoveTags publishes no error
		// for one.
		"TagKeys.member.2": "absent",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, map[string]string{"team": "platform"}, elbDescribeTags(t, ts, arn)[arn])
}

func TestELB_RemoveTags_Errors(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "untag-err-alb", nil)

	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "no ResourceArns",
			params: map[string]string{"TagKeys.member.1": "env"},
			code:   "ValidationError",
		},
		{
			name:   "no TagKeys",
			params: map[string]string{"ResourceArns.member.1": arn},
			code:   "ValidationError",
		},
		{
			name: "key outside the permitted character set",
			params: map[string]string{
				"ResourceArns.member.1": arn,
				"TagKeys.member.1":      "env!",
			},
			code: "ValidationError",
		},
		{
			name: "absent load balancer",
			params: map[string]string{
				"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nope/0aaaaaaa",
				"TagKeys.member.1":      "env",
			},
			code: "LoadBalancerNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{"Action": "RemoveTags"}
			for k, v := range tt.params {
				params[k] = v
			}
			resp := elbRequest(t, ts, params)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, tt.code, elbErrorCode(t, resp))
		})
	}
}

// TestELB_RemoveTags_RefusesMoreThan128Keys pins the operation's own array-member maximum,
// which is a per-request cap and not the per-resource tag limit.
func TestELB_RemoveTags_RefusesMoreThan128Keys(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "many-keys-alb", nil)

	params := map[string]string{"Action": "RemoveTags", "ResourceArns.member.1": arn}
	for i := 1; i <= 129; i++ {
		params["TagKeys.member."+strconv.Itoa(i)] = "k" + strconv.Itoa(i)
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
}

// TestELB_RemoveTags_DuplicateKeyIsNotRefused pins the per-operation error set:
// DuplicateTagKeys is listed on AddTags and not on RemoveTags, so naming a key twice
// removes it once rather than being refused.
func TestELB_RemoveTags_DuplicateKeyIsNotRefused(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "dup-remove-alb", map[string]string{
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
	})

	resp := elbRequest(t, ts, map[string]string{
		"Action":                "RemoveTags",
		"ResourceArns.member.1": arn,
		"TagKeys.member.1":      "env",
		"TagKeys.member.2":      "env",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, elbDescribeTags(t, ts, arn)[arn])
}

// TestELB_DescribeTags_ReportsAnUntaggedResource pins that a resource with no tags is still
// reported: omitting it would make "no tags" indistinguishable from "no such resource".
func TestELB_DescribeTags_ReportsAnUntaggedResource(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "bare-alb", nil)

	tags := elbDescribeTags(t, ts, arn)
	require.Contains(t, tags, arn)
	assert.Empty(t, tags[arn])
}

func TestELB_DescribeTags_Errors(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts, "desc-tags-alb", nil)

	t.Run("no ResourceArns", func(t *testing.T) {
		resp := elbRequest(t, ts, map[string]string{"Action": "DescribeTags"})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
	})

	// The operation's ResourceArns carries "Array Members: Maximum number of 20 items",
	// which AddTags and RemoveTags do not.
	t.Run("more than 20 resources", func(t *testing.T) {
		params := map[string]string{"Action": "DescribeTags"}
		for i := 1; i <= 21; i++ {
			params["ResourceArns.member."+strconv.Itoa(i)] = arn
		}
		resp := elbRequest(t, ts, params)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
	})

	t.Run("absent target group", func(t *testing.T) {
		resp := elbRequest(t, ts, map[string]string{
			"Action":                "DescribeTags",
			"ResourceArns.member.1": "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/nope/0aaaaaaa",
		})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "TargetGroupNotFound", elbErrorCode(t, resp))
	})
}

// TestELB_CreatesPersistTags walks the four creates that accept Tags.member.N and asserts
// each one stored what it was given — the gap #748 opened with, where every create silently
// dropped its tags.
func TestELB_CreatesPersistTags(t *testing.T) {
	ts := newELBTestServer(t)
	tagged := map[string]string{
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
	}

	lbARN := elbCreateLB(t, ts, "create-tags-alb", tagged)
	tgARN := elbCreateTG(t, ts, "create-tags-tg", tagged)
	listenerARN := elbCreateListener(t, ts, lbARN, tgARN, tagged)
	ruleARN := elbCreateRule(t, ts, listenerARN, tgARN, tagged)

	for _, arn := range []string{lbARN, tgARN, listenerARN, ruleARN} {
		assert.Equal(t, map[string]string{"env": "prod"}, elbDescribeTags(t, ts, arn)[arn], arn)
	}
}

// TestELB_CreateLoadBalancer_DuplicateTagKeys pins the one create of the four that publishes
// DuplicateTagKeys.
func TestELB_CreateLoadBalancer_DuplicateTagKeys(t *testing.T) {
	ts := newELBTestServer(t)
	resp := elbRequest(t, ts, map[string]string{
		"Action": "CreateLoadBalancer", "Name": "dup-alb", "Type": "application",
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "a",
		"Tags.member.2.Key": "env", "Tags.member.2.Value": "b",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "DuplicateTagKeys", elbErrorCode(t, resp))

	// The refusal happens before the record is written, so no load balancer is left behind
	// carrying a tag set the request was refused for.
	descResp := elbRequest(t, ts, map[string]string{"Action": "DescribeLoadBalancers"})
	defer descResp.Body.Close() //nolint:errcheck
	var result struct {
		Result struct {
			LoadBalancers []struct{} `xml:"LoadBalancers>member"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.NewDecoder(descResp.Body).Decode(&result))
	assert.Empty(t, result.Result.LoadBalancers)
}

// TestELB_OtherCreates_DuplicateTagKeysResolvesLastWins is the other half: the API reference
// lists DuplicateTagKeys on CreateLoadBalancer and on none of the other three creates, so
// substrate does not invent it there — the duplicate resolves last-wins instead.
func TestELB_OtherCreates_DuplicateTagKeysResolvesLastWins(t *testing.T) {
	ts := newELBTestServer(t)
	dup := map[string]string{
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "first",
		"Tags.member.2.Key": "env", "Tags.member.2.Value": "second",
	}

	lbARN := elbCreateLB(t, ts, "lastwins-alb", nil)
	tgARN := elbCreateTG(t, ts, "lastwins-tg", dup)
	listenerARN := elbCreateListener(t, ts, lbARN, tgARN, dup)
	ruleARN := elbCreateRule(t, ts, listenerARN, tgARN, dup)

	for _, arn := range []string{tgARN, listenerARN, ruleARN} {
		assert.Equal(t, map[string]string{"env": "second"}, elbDescribeTags(t, ts, arn)[arn], arn)
	}
}

// TestELB_CreateTargetGroup_TooManyTags pins TooManyTags on a create, which all four
// publish, and the rollback: a refused create leaves nothing behind.
func TestELB_CreateTargetGroup_TooManyTags(t *testing.T) {
	ts := newELBTestServer(t)
	params := map[string]string{
		"Action": "CreateTargetGroup", "Name": "over-limit-tg", "Protocol": "HTTP", "Port": "80",
	}
	for i := 1; i <= 51; i++ {
		params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	resp := elbRequest(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "TooManyTags", elbErrorCode(t, resp))

	descResp := elbRequest(t, ts, map[string]string{"Action": "DescribeTargetGroups"})
	defer descResp.Body.Close() //nolint:errcheck
	var result struct {
		Result struct {
			TargetGroups []struct{} `xml:"TargetGroups>member"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.NewDecoder(descResp.Body).Decode(&result))
	assert.Empty(t, result.Result.TargetGroups)
}

// TestELB_EmptyOutputOperationsCarryTheirResultElement pins the shape every ELBv2 operation
// whose output has no members must answer with.
//
// ELBv2 speaks the Query protocol, where each output shape declares a resultWrapper, and
// botocore looks that wrapper up by name — so a bare <OperationResponse/> makes the AWS CLI
// and boto3 raise KeyError instead of reporting success, while a test that reads the XML
// directly sees nothing wrong. Six of these answered that way before #748, which is exactly
// why the assertion is on the wire bytes rather than on a decoded struct.
func TestELB_EmptyOutputOperationsCarryTheirResultElement(t *testing.T) {
	ts := newELBTestServer(t)
	lbARN := elbCreateLB(t, ts, "wrapper-lb", nil)
	tgARN := elbCreateTG(t, ts, "wrapper-tg", nil)
	listenerARN := elbCreateListener(t, ts, lbARN, tgARN, nil)
	ruleARN := elbCreateRule(t, ts, listenerARN, tgARN, nil)

	tests := []struct {
		name   string
		params map[string]string
	}{
		{"AddTags", map[string]string{
			"Action": "AddTags", "ResourceArns.member.1": lbARN,
			"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
		}},
		{"RemoveTags", map[string]string{
			"Action": "RemoveTags", "ResourceArns.member.1": lbARN, "TagKeys.member.1": "env",
		}},
		{"RegisterTargets", map[string]string{
			"Action": "RegisterTargets", "TargetGroupArn": tgARN,
			"Targets.member.1.Id": "i-1234567890abcdef0",
		}},
		{"DeregisterTargets", map[string]string{
			"Action": "DeregisterTargets", "TargetGroupArn": tgARN,
			"Targets.member.1.Id": "i-1234567890abcdef0",
		}},
		{"DeleteRule", map[string]string{"Action": "DeleteRule", "RuleArn": ruleARN}},
		{"DeleteListener", map[string]string{"Action": "DeleteListener", "ListenerArn": listenerARN}},
		{"DeleteTargetGroup", map[string]string{"Action": "DeleteTargetGroup", "TargetGroupArn": tgARN}},
		{"DeleteLoadBalancer", map[string]string{
			"Action": "DeleteLoadBalancer", "LoadBalancerArn": lbARN,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := elbRequest(t, ts, tt.params)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Contains(t, string(body), "<"+tt.name+"Result></"+tt.name+"Result>")
			assert.Contains(t, string(body), "<"+tt.name+"Response xmlns=")
		})
	}
}
