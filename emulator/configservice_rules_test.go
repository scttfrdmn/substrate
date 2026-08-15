package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Config-rule cluster, seeded compliance, and PutEvaluations (#580).
//
// The load-bearing assertion in this file is that an unevaluated rule reports
// INSUFFICIENT_DATA rather than COMPLIANT. A consumer's compliance gate reads
// COMPLIANT as "pass", so an emulator that answered COMPLIANT by default would let a
// rule that never ran look like a rule that passed — the exact bug a Config test
// exists to catch. Everything else here is in service of that: the seed reaches the
// other verdicts, and PutEvaluations outranks the seed because a rule reporting its
// own result is an observation rather than a fixture default.

// configRulePayload is a minimal valid ConfigRule: a managed rule needs only a name
// and a Source naming its owner and identifier.
func configRulePayload(name string) map[string]any {
	return map[string]any{
		"ConfigRuleName": name,
		"Source": map[string]any{
			"Owner":            "AWS",
			"SourceIdentifier": "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED",
		},
	}
}

// configRuleServer starts a server with a recorder in place, because PutConfigRule
// refuses without one.
func configRuleServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	return ts
}

// configPutRule creates a rule and requires that it succeed.
func configPutRule(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	status, code, message := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": configRulePayload(name)})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

// configPutRuleRaw sends a PutConfigRule and returns the status and error code, for
// the cases whose subject is the refusal.
func configPutRuleRaw(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "PutConfigRule", body)
	return decodeConfigResponse(t, resp, nil)
}

// configDescribeRules returns the rules DescribeConfigRules reports.
func configDescribeRules(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeConfigRules", body)
	var out struct {
		ConfigRules []map[string]any `json:"ConfigRules"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConfigRules
}

// configDescribeCompliance returns the ComplianceByConfigRules
// DescribeComplianceByConfigRule reports.
func configDescribeCompliance(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeComplianceByConfigRule", body)
	var out struct {
		ComplianceByConfigRules []map[string]any `json:"ComplianceByConfigRules"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ComplianceByConfigRules
}

// configComplianceDetails returns the EvaluationResults
// GetComplianceDetailsByConfigRule reports.
func configComplianceDetails(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "GetComplianceDetailsByConfigRule", body)
	var out struct {
		EvaluationResults []map[string]any `json:"EvaluationResults"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.EvaluationResults
}

// configComplianceTypeOf pulls the ComplianceType out of one
// ComplianceByConfigRule entry.
func configComplianceTypeOf(t *testing.T, entry map[string]any) string {
	t.Helper()
	compliance, ok := entry["Compliance"].(map[string]any)
	require.True(t, ok, "the entry carries a Compliance shape: %v", entry)
	verdict, ok := compliance["ComplianceType"].(string)
	require.True(t, ok, "the Compliance shape carries a ComplianceType: %v", compliance)
	return verdict
}

// configSeedRuleCompliance pins a rule's verdict and requires the seed be accepted.
func configSeedRuleCompliance(t *testing.T, ts *emulator.TestServer, rule string, body map[string]any) {
	t.Helper()
	configSeed(t, ts, "/v1/config/rule-compliance/"+rule, body)
}

// configResultToken builds the ResultToken for a rule, in the documented
// substrate-config-rule:<base64url(name)> form.
//
// A real caller receives its token in the rule's invocation event, which substrate
// does not produce — running the rule is workload-internal. A constructible token is
// what makes a custom-rule fixture writable, and this helper is a consumer's own
// fixture code written out.
func configResultToken(rule string) string {
	return "substrate-config-rule:" + base64.RawURLEncoding.EncodeToString([]byte(rule))
}

// configPutEvaluations submits evaluations for a rule and returns the status and
// error code.
func configPutEvaluations(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "PutEvaluations", body)
	return decodeConfigResponse(t, resp, nil)
}

// configEvaluation builds one Evaluation payload.
func configEvaluation(resourceID, verdict string) map[string]any {
	return map[string]any{
		"ComplianceResourceType": "AWS::S3::Bucket",
		"ComplianceResourceId":   resourceID,
		"ComplianceType":         verdict,
		"OrderingTimestamp":      1767225600,
	}
}

func TestConfigRules_AnUnevaluatedRuleIsInsufficientDataNotCompliant(t *testing.T) {
	// The cluster's headline behavior. A rule that has just been created has evaluated
	// nothing, and AWS reports INSUFFICIENT_DATA for it. A consumer whose gate treats
	// COMPLIANT as "pass" must fail closed here, and it can only do so if substrate
	// refuses to fabricate the optimistic answer.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "s3-encrypted", entries[0]["ConfigRuleName"])
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]),
		"a rule that has not evaluated is INSUFFICIENT_DATA — a fabricated COMPLIANT would "+
			"hide a consumer's fail-open bug")

	// And the details operation agrees by reporting nothing, rather than inventing a
	// resource to hang a verdict on.
	results := configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "s3-encrypted"})
	assert.Empty(t, results, "no evaluation has run, so there are no per-resource results")
}

func TestConfigRules_PutMintsIDAndARNAndRefusesThemOnCreate(t *testing.T) {
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	rules := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	ruleID, ok := rules[0]["ConfigRuleId"].(string)
	require.True(t, ok)
	assert.Contains(t, ruleID, "config-rule-", "the minted ID carries the prefix AWS uses")
	assert.LessOrEqual(t, len(ruleID), 64, "ConfigRuleId is bounded at 64 characters")
	assert.Equal(t, "arn:aws:config:us-east-1:123456789012:config-rule/"+ruleID,
		rules[0]["ConfigRuleArn"],
		"the ARN's identifier component is the ConfigRuleId, per the Service Authorization Reference")
	assert.Equal(t, "ACTIVE", rules[0]["ConfigRuleState"])

	// The ID is deterministic: a second identical create mints the same one, which is
	// what makes a replayed event stream produce the same ARNs.
	configPutRule(t, ts, "s3-encrypted")
	again := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, again, 1)
	assert.Equal(t, ruleID, again[0]["ConfigRuleId"], "a minted ID is derived, not random")

	// A create supplying either generated value is refused: "These values are generated
	// by Config for new rules." Echoing back a described rule to create a second one is
	// the mistake this catches.
	rule := configRulePayload("s3-versioned")
	rule["ConfigRuleId"] = ruleID
	status, code, _ := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": rule})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code)

	rule = configRulePayload("s3-versioned")
	rule["ConfigRuleArn"] = "arn:aws:config:us-east-1:123456789012:config-rule/config-rule-abc123"
	status, code, _ = configPutRuleRaw(t, ts, map[string]any{"ConfigRule": rule})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code)
}

func TestConfigRules_UpdateByIDOrARNKeepsTheStoredName(t *testing.T) {
	// "If you are updating a rule that you added previously, you can specify the rule
	// by ConfigRuleName, ConfigRuleId, or ConfigRuleArn." An update addressed by ID must
	// not rename the rule to empty, and must not create a second one.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")
	rules := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	ruleID, _ := rules[0]["ConfigRuleId"].(string)
	ruleARN, _ := rules[0]["ConfigRuleArn"].(string)

	byID := map[string]any{
		"ConfigRuleId": ruleID,
		"Description":  "addressed by id",
		"Source": map[string]any{
			"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED",
		},
	}
	status, code, message := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": byID})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	rules = configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1, "an update by ID updates rather than creating a second rule")
	assert.Equal(t, "s3-encrypted", rules[0]["ConfigRuleName"], "the stored name survives")
	assert.Equal(t, "addressed by id", rules[0]["Description"])

	byARN := map[string]any{
		"ConfigRuleArn": ruleARN,
		"Description":   "addressed by arn",
		"Source": map[string]any{
			"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED",
		},
	}
	status, code, message = configPutRuleRaw(t, ts, map[string]any{"ConfigRule": byARN})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	rules = configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	assert.Equal(t, "addressed by arn", rules[0]["Description"])
}

func TestConfigRules_PutRefusesWithoutARecorder(t *testing.T) {
	// NoAvailableConfigurationRecorderException, which #580 omitted: a rule in an
	// account with no recorder has nothing to evaluate, and a consumer that creates its
	// rules before its recorder finds out here rather than in production.
	ts := emulator.StartTestServer(t)
	status, code, _ := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": configRulePayload("s3-encrypted")})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoAvailableConfigurationRecorderException", code)
}

func TestConfigRules_PutRefusalsAreInvalidParameterValueNotValidation(t *testing.T) {
	// None of the six rule operations declares ValidationException — the recorder
	// cluster's operations do, but these answer InvalidParameterValueException. A
	// caller's error handling is written against the code the operation declares, so
	// the wrong one is a branch it has not got.
	ts := configRuleServer(t)

	type ruleCase struct {
		name string
		rule map[string]any
	}
	cases := []ruleCase{
		{"no source at all", map[string]any{"ConfigRuleName": "r"}},
		{"unknown owner", map[string]any{
			"ConfigRuleName": "r", "Source": map[string]any{"Owner": "CUSTOM_THING"},
		}},
		{"AWS owner with no identifier", map[string]any{
			"ConfigRuleName": "r", "Source": map[string]any{"Owner": "AWS"},
		}},
		{"custom policy with no policy details", map[string]any{
			"ConfigRuleName": "r", "Source": map[string]any{"Owner": "CUSTOM_POLICY"},
		}},
		{"custom policy with the wrong runtime", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "CUSTOM_POLICY",
				"CustomPolicyDetails": map[string]any{
					"PolicyRuntime": "guard-1.0.0", "PolicyText": "rule x {}",
				},
			},
		}},
		{"blank name", map[string]any{
			"ConfigRuleName": "   ",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
			},
		}},
		{"an execution frequency outside the enum", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
			},
			"MaximumExecutionFrequency": "OneHour",
		}},
		{"an evaluation mode outside the enum", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
			},
			"EvaluationModes": []map[string]any{{"Mode": "REACTIVE"}},
		}},
		{"a resource ID with no single resource type", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
			},
			"Scope": map[string]any{"ComplianceResourceId": "b1"},
		}},
		{"a tag value with no tag key", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
			},
			"Scope": map[string]any{"TagValue": "prod"},
		}},
	}

	// The shape bounds, each from the model's own min/max. They are asserted because a
	// bound substrate does not enforce lets a fixture store a value the API would have
	// refused, and the consumer discovers the refusal against real AWS instead.
	longRule := func(member string, value any) map[string]any {
		rule := configRulePayload("r")
		rule[member] = value
		return rule
	}
	cases = append(cases,
		ruleCase{"a name past 128 characters", longRule("ConfigRuleName", strings.Repeat("n", 129))},
		ruleCase{"a Description past 256", longRule("Description", strings.Repeat("d", 257))},
		ruleCase{"InputParameters past 1024", longRule("InputParameters", strings.Repeat("p", 1025))},
		ruleCase{"a SourceIdentifier past 256", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "AWS", "SourceIdentifier": strings.Repeat("S", 257),
			},
		}},
		ruleCase{"PolicyText past 10000", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "CUSTOM_POLICY",
				"CustomPolicyDetails": map[string]any{
					"PolicyRuntime": "guard-2.x.x", "PolicyText": strings.Repeat("x", 10001),
				},
			},
		}},
		ruleCase{"a SourceDetails frequency outside the enum", map[string]any{
			"ConfigRuleName": "r",
			"Source": map[string]any{
				"Owner": "CUSTOM_LAMBDA", "SourceIdentifier": "arn:aws:lambda:us-east-1:123456789012:function:f",
				"SourceDetails": []map[string]any{
					{"EventSource": "aws.config", "MessageType": "ScheduledNotification",
						"MaximumExecutionFrequency": "Every_Hour"},
				},
			},
		}},
		ruleCase{"more than 100 ComplianceResourceTypes", longRule("Scope", map[string]any{
			"ComplianceResourceTypes": slices.Repeat([]string{"AWS::S3::Bucket"}, 101),
		})},
		ruleCase{"a Scope TagKey past 128", longRule("Scope", map[string]any{
			"TagKey": strings.Repeat("k", 129),
		})},
		ruleCase{"a Scope TagValue past 256", longRule("Scope", map[string]any{
			"TagKey": "env", "TagValue": strings.Repeat("v", 257),
		})},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": tc.rule})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValueException", code,
				"the rule cluster declares InvalidParameterValueException, not ValidationException")
		})
	}
}

func TestConfigRules_ACustomPolicyRuleRoundTripsItsPolicyText(t *testing.T) {
	// The Guard policy is recorded intent: substrate stores it and never runs it, so
	// what matters is that a describe hands back exactly what was submitted.
	ts := configRuleServer(t)
	rule := map[string]any{
		"ConfigRuleName": "guard-rule",
		"Source": map[string]any{
			"Owner": "CUSTOM_POLICY",
			"CustomPolicyDetails": map[string]any{
				"PolicyRuntime": "guard-2.x.x",
				"PolicyText":    "let buckets = Resources.*[ Type == 'AWS::S3::Bucket' ]",
			},
		},
	}
	status, code, message := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": rule})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	rules := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	source, ok := rules[0]["Source"].(map[string]any)
	require.True(t, ok)
	details, ok := source["CustomPolicyDetails"].(map[string]any)
	require.True(t, ok, "the policy comes back: %v", source)
	assert.Equal(t, "guard-2.x.x", details["PolicyRuntime"])
	assert.Equal(t, "let buckets = Resources.*[ Type == 'AWS::S3::Bucket' ]", details["PolicyText"])
	assert.Empty(t, rules[0]["CreatedBy"],
		"CreatedBy names the service that created a service-linked rule and is empty for a caller's own")
}

func TestConfigRules_CreatedByIsNotAcceptedFromARequest(t *testing.T) {
	// Substrate models no service-linked rules, so echoing a caller-supplied CreatedBy
	// would report a rule as service-created when it was not.
	ts := configRuleServer(t)
	rule := configRulePayload("s3-encrypted")
	rule["CreatedBy"] = "securityhub.amazonaws.com"
	status, code, message := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": rule})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	rules := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	assert.Empty(t, rules[0]["CreatedBy"])
}

func TestConfigRules_DescribeFiltersByNameAndEvaluationMode(t *testing.T) {
	ts := configRuleServer(t)
	configPutRule(t, ts, "detective-rule")

	proactive := configRulePayload("proactive-rule")
	proactive["EvaluationModes"] = []map[string]any{{"Mode": "PROACTIVE"}}
	status, code, message := configPutRuleRaw(t, ts, map[string]any{"ConfigRule": proactive})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	all := configDescribeRules(t, ts, map[string]any{})
	assert.Len(t, all, 2)

	named := configDescribeRules(t, ts, map[string]any{"ConfigRuleNames": []string{"detective-rule"}})
	require.Len(t, named, 1)
	assert.Equal(t, "detective-rule", named[0]["ConfigRuleName"])

	// A rule created with no EvaluationModes matches DETECTIVE, because that is the
	// service's default. Treating an empty list as matching nothing would hide most
	// rules, since the CLI does not require the member.
	detective := configDescribeRules(t, ts, map[string]any{
		"Filters": map[string]any{"EvaluationMode": "DETECTIVE"},
	})
	require.Len(t, detective, 1)
	assert.Equal(t, "detective-rule", detective[0]["ConfigRuleName"])

	onlyProactive := configDescribeRules(t, ts, map[string]any{
		"Filters": map[string]any{"EvaluationMode": "PROACTIVE"},
	})
	require.Len(t, onlyProactive, 1)
	assert.Equal(t, "proactive-rule", onlyProactive[0]["ConfigRuleName"])
}

func TestConfigRules_DescribeRefusesAnUnknownNameAndAnOversizeFilter(t *testing.T) {
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	resp := configRequest(t, ts, "DescribeConfigRules", map[string]any{
		"ConfigRuleNames": []string{"no-such-rule"},
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchConfigRuleException", code,
		"the caller asserted the name, so a miss is an error rather than an empty list")

	names := make([]string, 26)
	for i := range names {
		names[i] = "r"
	}
	resp = configRequest(t, ts, "DescribeConfigRules", map[string]any{"ConfigRuleNames": names})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code, "ConfigRuleNames is bounded at 25")
}

func TestConfigRules_ABadNextTokenIsInvalidNextToken(t *testing.T) {
	// The rule operations answer InvalidNextTokenException rather than
	// ValidationException, and an unreadable token is an error rather than a silent
	// restart: a paginating caller that restarts sees duplicates, which is the failure
	// mode hardest to notice.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	for _, op := range []string{"DescribeConfigRules", "DescribeComplianceByConfigRule"} {
		t.Run(op, func(t *testing.T) {
			resp := configRequest(t, ts, op, map[string]any{"NextToken": "%%not-base64%%"})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidNextTokenException", code)
		})
	}

	// A well-formed token naming a rule that is not in the listing is equally invalid.
	resp := configRequest(t, ts, "DescribeConfigRules", map[string]any{
		"NextToken": base64.StdEncoding.EncodeToString([]byte("no-such-rule")),
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidNextTokenException", code)

	resp = configRequest(t, ts, "GetComplianceDetailsByConfigRule", map[string]any{
		"ConfigRuleName": "s3-encrypted", "NextToken": "not base64 either",
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidNextTokenException", code)
}

func TestConfigRules_DetailsLimitIsBoundedAt100(t *testing.T) {
	// The bound is asserted from both sides. Refusing 101 is the obvious half; the
	// half that matters as much is accepting 100, because the model's Valid Range is
	// 0-100 and a narrower ceiling would refuse a request AWS permits. A wrong refusal
	// breaks working consumer code, which is worse than a wrong acceptance.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	for _, tc := range []struct {
		name  string
		limit int
		code  string
	}{
		{name: "the model's maximum is accepted", limit: 100},
		{name: "one past it is refused", limit: 101, code: "InvalidParameterValueException"},
		{name: "a negative Limit is refused", limit: -1, code: "InvalidParameterValueException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := configRequest(t, ts, "GetComplianceDetailsByConfigRule", map[string]any{
				"ConfigRuleName": "s3-encrypted", "Limit": tc.limit,
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			if tc.code == "" {
				require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestConfigRules_SeededComplianceIsReportedByBothOperations(t *testing.T) {
	// Both compliance operations read the same seed, so they cannot disagree about a
	// rule — which is what a consumer comparing a summary against details depends on.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	configSeedRuleCompliance(t, ts, "s3-encrypted", map[string]any{
		"complianceType": "NON_COMPLIANT",
		"annotation":     "Bucket is not encrypted",
		"resources": []map[string]any{
			{"resourceType": "AWS::S3::Bucket", "resourceId": "plain-bucket"},
		},
	})

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "NON_COMPLIANT", configComplianceTypeOf(t, entries[0]))
	compliance, _ := entries[0]["Compliance"].(map[string]any)
	count, ok := compliance["ComplianceContributorCount"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), count["CappedCount"],
		"a seeded NON_COMPLIANT asserts at least one contributing resource")
	assert.Equal(t, false, count["CapExceeded"], "substrate has no contributor cap to exceed")

	results := configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "s3-encrypted"})
	require.Len(t, results, 1)
	assert.Equal(t, "NON_COMPLIANT", results[0]["ComplianceType"])
	assert.Equal(t, "Bucket is not encrypted", results[0]["Annotation"])
	identifier, ok := results[0]["EvaluationResultIdentifier"].(map[string]any)
	require.True(t, ok)
	qualifier, ok := identifier["EvaluationResultQualifier"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3-encrypted", qualifier["ConfigRuleName"])
	assert.Equal(t, "AWS::S3::Bucket", qualifier["ResourceType"])
	assert.Equal(t, "plain-bucket", qualifier["ResourceId"])
	assert.Equal(t, "DETECTIVE", qualifier["EvaluationMode"])
	assert.Nil(t, identifier["ResourceEvaluationId"],
		"ResourceEvaluationId identifies a StartResourceEvaluation result, which substrate does not model")

	// Clearing the seed returns the rule to INSUFFICIENT_DATA rather than leaving the
	// seeded value behind, because the seed is applied at read time.
	configClearSeed(t, ts, "/v1/config/rule-compliance/s3-encrypted")
	entries = configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]))
}

func TestConfigRules_ARuleLevelSeedLeavesTheDetailsEmpty(t *testing.T) {
	// A seed naming no resources is a rule-level verdict. It answers the summary and
	// leaves the details empty, because inventing a resource ID would put one in the
	// response that exists nowhere in the account.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")
	configSeedRuleCompliance(t, ts, "s3-encrypted", map[string]any{"complianceType": "COMPLIANT"})

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "COMPLIANT", configComplianceTypeOf(t, entries[0]))

	results := configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "s3-encrypted"})
	assert.Empty(t, results)
}

func TestConfigRules_TheWildcardSeedAppliesToEveryRuleAndTheNamedOneWins(t *testing.T) {
	// A fixture that does not enumerate its stack's rule names still needs to pin a
	// default; a rule it does name must override it.
	ts := configRuleServer(t)
	configPutRule(t, ts, "rule-a")
	configPutRule(t, ts, "rule-b")

	configSeedRuleCompliance(t, ts, "*", map[string]any{"complianceType": "COMPLIANT"})
	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 2)
	for _, entry := range entries {
		assert.Equal(t, "COMPLIANT", configComplianceTypeOf(t, entry), entry["ConfigRuleName"])
	}

	configSeedRuleCompliance(t, ts, "rule-b", map[string]any{"complianceType": "NON_COMPLIANT"})
	byName := map[string]string{}
	for _, entry := range configDescribeCompliance(t, ts, map[string]any{}) {
		name, _ := entry["ConfigRuleName"].(string)
		byName[name] = configComplianceTypeOf(t, entry)
	}
	assert.Equal(t, "COMPLIANT", byName["rule-a"])
	assert.Equal(t, "NON_COMPLIANT", byName["rule-b"], "the named seed outranks the wildcard")
}

func TestConfigRules_TheSeedRefusesValuesTheShapesCannotCarry(t *testing.T) {
	// The three asymmetric ComplianceType restrictions are the fidelity core of this
	// cluster. A seed the response shape cannot carry would be reported as a string no
	// SDK enum member matches, so it is refused rather than stored.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")

	cases := []struct {
		name string
		body map[string]any
		why  string
	}{
		{
			name: "NOT_APPLICABLE for a rule",
			body: map[string]any{"complianceType": "NOT_APPLICABLE"},
			why:  "Config does not support NOT_APPLICABLE for the Compliance data type",
		},
		{
			name: "a value outside the enum",
			body: map[string]any{"complianceType": "MOSTLY_COMPLIANT"},
			why:  "not a ComplianceType at all",
		},
		{
			name: "no compliance type",
			body: map[string]any{"annotation": "why"},
			why:  "complianceType is what the seed is for",
		},
		{
			name: "resources alongside INSUFFICIENT_DATA",
			body: map[string]any{
				"complianceType": "INSUFFICIENT_DATA",
				"resources": []map[string]any{
					{"resourceType": "AWS::S3::Bucket", "resourceId": "b1"},
				},
			},
			why: "the EvaluationResult shape does not support INSUFFICIENT_DATA",
		},
		{
			name: "a resource with no ID",
			body: map[string]any{
				"complianceType": "NON_COMPLIANT",
				"resources":      []map[string]any{{"resourceType": "AWS::S3::Bucket"}},
			},
			why: "a result with no resource ID identifies nothing",
		},
		{
			name: "a body naming a different rule",
			body: map[string]any{"complianceType": "COMPLIANT", "configRuleName": "some-other-rule"},
			why:  "the path already named the rule, so this is a contradiction",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := configSeedRaw(t, ts, "/v1/config/rule-compliance/s3-encrypted", tc.body)
			require.Equal(t, http.StatusBadRequest, status, "%s: %s", tc.why, body)

			// The refusal body must decode. A refusal a caller cannot read is a refusal it
			// reports as a parse error, at the one moment it most needs the reason — and the
			// obvious inline form, fmt.Sprintf over a JSON literal containing %q, produces
			// exactly that, because %q's own quotes close the JSON string early.
			var decoded struct {
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal([]byte(body), &decoded), "refusal body is JSON: %s", body)
			assert.NotEmpty(t, decoded.Error, "the refusal says why: %s", body)
		})
	}

	// And the rule still reports the seedless default, so a refused seed left nothing
	// half-applied.
	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]))
}

func TestConfigRules_ComplianceTypesFilterIsHonouredAndBounded(t *testing.T) {
	ts := configRuleServer(t)
	configPutRule(t, ts, "compliant-rule")
	configPutRule(t, ts, "failing-rule")
	configSeedRuleCompliance(t, ts, "compliant-rule", map[string]any{"complianceType": "COMPLIANT"})
	configSeedRuleCompliance(t, ts, "failing-rule", map[string]any{"complianceType": "NON_COMPLIANT"})

	entries := configDescribeCompliance(t, ts, map[string]any{"ComplianceTypes": []string{"NON_COMPLIANT"}})
	require.Len(t, entries, 1)
	assert.Equal(t, "failing-rule", entries[0]["ConfigRuleName"])

	resp := configRequest(t, ts, "DescribeComplianceByConfigRule", map[string]any{
		"ComplianceTypes": []string{"COMPLIANT", "NON_COMPLIANT", "NOT_APPLICABLE", "INSUFFICIENT_DATA"},
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code, "ComplianceTypes is bounded at 3")

	resp = configRequest(t, ts, "DescribeComplianceByConfigRule", map[string]any{
		"ComplianceTypes": []string{"MOSTLY"},
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code)
}

func TestConfigRules_PutEvaluationsOutranksTheSeed(t *testing.T) {
	// A custom rule reporting its own verdict is an API observation, so it wins over a
	// fixture default. This is what lets a consumer drive its real Lambda handler: the
	// handler calls PutEvaluations, and the assertion goes through
	// DescribeComplianceByConfigRule.
	ts := configRuleServer(t)
	configPutRule(t, ts, "custom-rule")
	configSeedRuleCompliance(t, ts, "custom-rule", map[string]any{"complianceType": "COMPLIANT"})

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	require.Equal(t, "COMPLIANT", configComplianceTypeOf(t, entries[0]))

	status, code, message := configPutEvaluations(t, ts, map[string]any{
		"ResultToken": configResultToken("custom-rule"),
		"Evaluations": []map[string]any{
			configEvaluation("good-bucket", "COMPLIANT"),
			configEvaluation("bad-bucket", "NON_COMPLIANT"),
		},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	entries = configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "NON_COMPLIANT", configComplianceTypeOf(t, entries[0]),
		"one non-compliant resource makes the rule non-compliant, over the seed's COMPLIANT")
	compliance, _ := entries[0]["Compliance"].(map[string]any)
	count, _ := compliance["ComplianceContributorCount"].(map[string]any)
	assert.Equal(t, float64(1), count["CappedCount"], "one of the two resources contributed")

	results := configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "custom-rule"})
	require.Len(t, results, 2)
	byResource := map[string]string{}
	for _, r := range results {
		identifier, _ := r["EvaluationResultIdentifier"].(map[string]any)
		qualifier, _ := identifier["EvaluationResultQualifier"].(map[string]any)
		id, _ := qualifier["ResourceId"].(string)
		verdict, _ := r["ComplianceType"].(string)
		byResource[id] = verdict
	}
	assert.Equal(t, "COMPLIANT", byResource["good-bucket"])
	assert.Equal(t, "NON_COMPLIANT", byResource["bad-bucket"])

	// A filter over the recorded results works the same as over seeded ones.
	filtered := configComplianceDetails(t, ts, map[string]any{
		"ConfigRuleName": "custom-rule", "ComplianceTypes": []string{"NON_COMPLIANT"},
	})
	require.Len(t, filtered, 1)
}

func TestConfigRules_AResubmissionReplacesTheResourcesVerdict(t *testing.T) {
	// A rule reports one current result per resource, so two contradictory verdicts for
	// one resource is a state AWS never reports.
	ts := configRuleServer(t)
	configPutRule(t, ts, "custom-rule")

	token := configResultToken("custom-rule")
	status, code, message := configPutEvaluations(t, ts, map[string]any{
		"ResultToken": token,
		"Evaluations": []map[string]any{configEvaluation("b1", "NON_COMPLIANT")},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	status, code, message = configPutEvaluations(t, ts, map[string]any{
		"ResultToken": token,
		"Evaluations": []map[string]any{configEvaluation("b1", "COMPLIANT")},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	results := configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "custom-rule"})
	require.Len(t, results, 1, "the resubmission replaced rather than accumulating")
	assert.Equal(t, "COMPLIANT", results[0]["ComplianceType"])

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "COMPLIANT", configComplianceTypeOf(t, entries[0]))
}

func TestConfigRules_PutEvaluationsRefusals(t *testing.T) {
	ts := configRuleServer(t)
	configPutRule(t, ts, "custom-rule")

	t.Run("a null result token", func(t *testing.T) {
		// "When TestMode is true, PutEvaluations doesn't require a valid value for the
		// ResultToken parameter, but the value cannot be null." A null token is refused in
		// both modes; only its validity is waived.
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"Evaluations": []map[string]any{configEvaluation("b1", "COMPLIANT")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidResultTokenException", code)
	})

	t.Run("a token substrate did not mint", func(t *testing.T) {
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": "some-opaque-token",
			"Evaluations": []map[string]any{configEvaluation("b1", "COMPLIANT")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidResultTokenException", code)
	})

	t.Run("a token for a rule that does not exist", func(t *testing.T) {
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("no-such-rule"),
			"Evaluations": []map[string]any{configEvaluation("b1", "COMPLIANT")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "NoSuchConfigRuleException", code)
	})

	t.Run("INSUFFICIENT_DATA as a verdict", func(t *testing.T) {
		// "Config does not accept INSUFFICIENT_DATA as the value for ComplianceType from a
		// PutEvaluations request." A rule with no data reports it; a resource cannot.
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{configEvaluation("b1", "INSUFFICIENT_DATA")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("a missing resource id", func(t *testing.T) {
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{{
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "COMPLIANT",
				"OrderingTimestamp":      1767225600,
			}},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("a missing ordering timestamp", func(t *testing.T) {
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{{
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceResourceId":   "b1",
				"ComplianceType":         "COMPLIANT",
			}},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("more than 100 evaluations", func(t *testing.T) {
		evaluations := make([]map[string]any, 101)
		for i := range evaluations {
			evaluations[i] = configEvaluation("b", "COMPLIANT")
		}
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": evaluations,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("a resource type past 256 characters", func(t *testing.T) {
		evaluation := configEvaluation("b1", "COMPLIANT")
		evaluation["ComplianceResourceType"] = strings.Repeat("T", 257)
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{evaluation},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("a resource id past 768 characters", func(t *testing.T) {
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{configEvaluation(strings.Repeat("b", 769), "COMPLIANT")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("an annotation past 256 characters", func(t *testing.T) {
		evaluation := configEvaluation("b1", "COMPLIANT")
		evaluation["Annotation"] = strings.Repeat("a", 257)
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": configResultToken("custom-rule"),
			"Evaluations": []map[string]any{evaluation},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValueException", code)
	})

	t.Run("a token that is not base64", func(t *testing.T) {
		// The envelope's payload is base64url. An unreadable one names no rule, and a
		// token naming no rule is the same refusal as a token naming an absent one.
		status, code, _ := configPutEvaluations(t, ts, map[string]any{
			"ResultToken": "substrate-config-rule:not-valid-base64!!",
			"Evaluations": []map[string]any{configEvaluation("b1", "COMPLIANT")},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidResultTokenException", code)
	})

	// None of the refusals stored anything, so the rule still reports the seedless
	// default.
	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]))
}

func TestConfigRules_TestModeStoresNothingAndTakesAnyToken(t *testing.T) {
	// "No updates occur to your existing evaluations, and evaluation results are not
	// sent to Config." A consumer verifying that its handler can reach the API must not
	// change the account's compliance by doing so.
	ts := configRuleServer(t)
	configPutRule(t, ts, "custom-rule")

	status, code, message := configPutEvaluations(t, ts, map[string]any{
		"ResultToken": "any-non-null-value",
		"TestMode":    true,
		"Evaluations": []map[string]any{configEvaluation("b1", "NON_COMPLIANT")},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]),
		"TestMode stored nothing")
	assert.Empty(t, configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "custom-rule"}))
}

func TestConfigRules_PutEvaluationsWithNoEvaluationsIsAccepted(t *testing.T) {
	// Evaluations is 0-100 and *not* required: a rule that found nothing in scope
	// submits an empty list, and refusing it would refuse a request AWS accepts.
	ts := configRuleServer(t)
	configPutRule(t, ts, "custom-rule")

	status, code, message := configPutEvaluations(t, ts, map[string]any{
		"ResultToken": configResultToken("custom-rule"),
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]),
		"an empty submission recorded no verdict, so the rule still has no data")
}

func TestConfigRules_DeleteRemovesTheRuleItsEvaluationsAndItsSeed(t *testing.T) {
	// "Deletes the specified Config rule and all of its evaluation results." A rebuilt
	// rule of the same name must start clean: inheriting its predecessor's verdict is a
	// state no AWS account is ever in, and it would make a teardown-and-rebuild fixture
	// — a test run repeated twice — report the first run's compliance.
	ts := configRuleServer(t)
	configPutRule(t, ts, "s3-encrypted")
	configSeedRuleCompliance(t, ts, "s3-encrypted", map[string]any{"complianceType": "NON_COMPLIANT"})
	status, code, message := configPutEvaluations(t, ts, map[string]any{
		"ResultToken": configResultToken("s3-encrypted"),
		"Evaluations": []map[string]any{configEvaluation("b1", "NON_COMPLIANT")},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	resp := configRequest(t, ts, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	assert.Empty(t, configDescribeRules(t, ts, map[string]any{}), "the rule is gone")

	configPutRule(t, ts, "s3-encrypted")
	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]),
		"the rebuilt rule inherits neither the seed nor the recorded evaluations")
	assert.Empty(t, configComplianceDetails(t, ts, map[string]any{"ConfigRuleName": "s3-encrypted"}))
}

func TestConfigRules_DeleteLeavesTheWildcardSeedAlone(t *testing.T) {
	// The "*" seed is a fixture-wide default rather than one rule's state, so deleting
	// a rule must not silently change what every other rule reports.
	ts := configRuleServer(t)
	configPutRule(t, ts, "rule-a")
	configPutRule(t, ts, "rule-b")
	configSeedRuleCompliance(t, ts, "*", map[string]any{"complianceType": "COMPLIANT"})

	resp := configRequest(t, ts, "DeleteConfigRule", map[string]any{"ConfigRuleName": "rule-a"})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	entries := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "rule-b", entries[0]["ConfigRuleName"])
	assert.Equal(t, "COMPLIANT", configComplianceTypeOf(t, entries[0]))

	// Clearing the wildcard by name returns the surviving rule to the seedless default.
	configClearSeed(t, ts, "/v1/config/rule-compliance/*")
	entries = configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entries[0]))
}

func TestConfigRules_ClearingEverySeedReturnsEveryRuleToInsufficientData(t *testing.T) {
	// The name-less DELETE is the reset a fixture runs between cases, and it must reach
	// the named seeds and the wildcard alike: a leftover seed would make the next case
	// read the previous one's verdict, which is the failure a shared control plane
	// invites.
	ts := configRuleServer(t)
	configPutRule(t, ts, "rule-a")
	configPutRule(t, ts, "rule-b")
	configSeedRuleCompliance(t, ts, "rule-a", map[string]any{"complianceType": "NON_COMPLIANT"})
	configSeedRuleCompliance(t, ts, "*", map[string]any{"complianceType": "COMPLIANT"})

	configClearSeed(t, ts, "/v1/config/rule-compliance")

	for _, entry := range configDescribeCompliance(t, ts, map[string]any{}) {
		assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, entry),
			"%v kept a seed the reset should have cleared", entry["ConfigRuleName"])
	}
}

func TestConfigRules_AMalformedSeedBodyIsRefusedReadably(t *testing.T) {
	// A seed body that is not JSON at all takes a different path from one that is JSON
	// with a bad value, and it must still answer something a caller can decode.
	ts := configRuleServer(t)

	status, body := configSeedRaw(t, ts, "/v1/config/rule-compliance/s3-encrypted", "not-a-seed-object")
	require.Equal(t, http.StatusBadRequest, status, body)
	var decoded struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &decoded), "refusal body is JSON: %s", body)
	assert.NotEmpty(t, decoded.Error)
}

func TestConfigRules_UnknownRuleRefusals(t *testing.T) {
	ts := configRuleServer(t)

	cases := []struct {
		operation string
		body      map[string]any
	}{
		{"DeleteConfigRule", map[string]any{"ConfigRuleName": "no-such-rule"}},
		{"GetComplianceDetailsByConfigRule", map[string]any{"ConfigRuleName": "no-such-rule"}},
		{"DescribeComplianceByConfigRule", map[string]any{"ConfigRuleNames": []string{"no-such-rule"}}},
	}
	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			resp := configRequest(t, ts, tc.operation, tc.body)
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status,
				"every Config exception is HTTP 400, including the not-found ones")
			assert.Equal(t, "NoSuchConfigRuleException", code)
		})
	}
}

func TestConfigRules_MissingRequiredNamesAreRefused(t *testing.T) {
	ts := configRuleServer(t)

	for _, op := range []string{"DeleteConfigRule", "GetComplianceDetailsByConfigRule"} {
		t.Run(op, func(t *testing.T) {
			resp := configRequest(t, ts, op, map[string]any{})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValueException", code)
		})
	}

	resp := configRequest(t, ts, "PutConfigRule", map[string]any{})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValueException", code)
}

func TestConfigRules_AreIsolatedByRegion(t *testing.T) {
	// Config is regional, and "rules in one Region only" is a common real
	// misconfiguration. A rule created in us-east-1 must be absent in eu-west-1 —
	// including its compliance, which would otherwise report a rule the Region has not
	// got.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutRule(t, ts, "s3-encrypted")

	resp := configRequestIn(t, ts, "eu-west-1", "DescribeConfigRules", map[string]any{})
	var out struct {
		ConfigRules []map[string]any `json:"ConfigRules"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, out.ConfigRules, "the rule belongs to us-east-1 alone")

	resp = configRequestIn(t, ts, "eu-west-1", "DescribeComplianceByConfigRule", map[string]any{})
	var compliance struct {
		ComplianceByConfigRules []map[string]any `json:"ComplianceByConfigRules"`
	}
	status, code, message = decodeConfigResponse(t, resp, &compliance)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, compliance.ComplianceByConfigRules)

	// An empty listing is not on its own proof of isolation: the name index could be
	// regional while the records it points at are not, and every assertion above would
	// still pass. So put a rule of the *same name* in the second Region and require
	// that each Region reports its own — with a shared record the second Put would
	// silently edit the first Region's rule.
	configPutRecorderIn(t, ts, "eu-west-1", "default")
	east := configRulePayload("s3-encrypted")
	east["Description"] = "the us-east-1 rule"
	west := configRulePayload("s3-encrypted")
	west["Description"] = "the eu-west-1 rule"
	for region, payload := range map[string]map[string]any{"us-east-1": east, "eu-west-1": west} {
		resp := configRequestIn(t, ts, region, "PutConfigRule", map[string]any{"ConfigRule": payload})
		status, code, message := decodeConfigResponse(t, resp, nil)
		require.Equal(t, http.StatusOK, status, "%s in %s: %s", code, region, message)
	}
	for region, want := range map[string]string{
		"us-east-1": "the us-east-1 rule",
		"eu-west-1": "the eu-west-1 rule",
	} {
		resp := configRequestIn(t, ts, region, "DescribeConfigRules", map[string]any{})
		var rules struct {
			ConfigRules []map[string]any `json:"ConfigRules"`
		}
		status, code, message := decodeConfigResponse(t, resp, &rules)
		require.Equal(t, http.StatusOK, status, "%s in %s: %s", code, region, message)
		require.Len(t, rules.ConfigRules, 1, "%s reports its own rule", region)
		assert.Equal(t, want, rules.ConfigRules[0]["Description"],
			"%s reports the rule it was given, not the other Region's", region)
	}

	// Recorded evaluations are keyed the same way, and a shared key would report one
	// Region's custom-rule verdict as the other's.
	status, code, message = configPutEvaluations(t, ts, map[string]any{
		"ResultToken": configResultToken("s3-encrypted"),
		"Evaluations": []map[string]any{configEvaluation("east-bucket", "COMPLIANT")},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	resp = configRequestIn(t, ts, "eu-west-1", "GetComplianceDetailsByConfigRule",
		map[string]any{"ConfigRuleName": "s3-encrypted"})
	var details struct {
		EvaluationResults []map[string]any `json:"EvaluationResults"`
	}
	status, code, message = decodeConfigResponse(t, resp, &details)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, details.EvaluationResults,
		"the evaluation was submitted in us-east-1 and belongs to it alone")
}

func TestConfigRules_DescribePaginatesAndTheTokenTerminates(t *testing.T) {
	// DescribeConfigRules has no Limit member, so the page size is substrate's. What a
	// caller depends on is that the loop terminates and every rule appears exactly
	// once, which is what this asserts over a listing large enough to be sorted
	// non-trivially.
	ts := configRuleServer(t)
	for _, name := range []string{"rule-c", "rule-a", "rule-b"} {
		configPutRule(t, ts, name)
	}

	seen := map[string]int{}
	body := map[string]any{}
	for range 5 {
		resp := configRequest(t, ts, "DescribeConfigRules", body)
		var out struct {
			ConfigRules []map[string]any `json:"ConfigRules"`
			NextToken   string           `json:"NextToken"`
		}
		status, code, message := decodeConfigResponse(t, resp, &out)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		for _, rule := range out.ConfigRules {
			name, _ := rule["ConfigRuleName"].(string)
			seen[name]++
		}
		if out.NextToken == "" {
			break
		}
		body = map[string]any{"NextToken": out.NextToken}
	}
	assert.Equal(t, map[string]int{"rule-a": 1, "rule-b": 1, "rule-c": 1}, seen)
}

func TestConfigRules_AreNotReachableWithoutTheOperationBeingClaimed(t *testing.T) {
	// The rule cluster is claimed by the plugin's third claim function; an operation no
	// cluster claims is InvalidAction naming itself, so a consumer discovers which call
	// is missing rather than getting a bare refusal.
	ts := configRuleServer(t)
	resp := configRequest(t, ts, "PutRemediationConfigurations", map[string]any{})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidAction", code)
	assert.Contains(t, message, "PutRemediationConfigurations")
}
