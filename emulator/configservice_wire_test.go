package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertion scripts/wire-bookkeeping-projected.txt cites for AWS Config's
// five control-plane seed records (#756).
//
// Those five records are the odd ones in the baseline inventory. Everything else listed
// there is substrate's own bookkeeping grafted onto a *resource*, where the fix is a wire
// struct — emulator/ecr_wire.go is the worked case. These are seed documents in
// configServiceCtrlNamespace, and their accountId and region are the seed's own scope:
// handleConfigSeedRecorderStatus and its four siblings read them off the control-plane
// POST body and build the state key from them, which is the whole mechanism
// cfgsvcCtrlKeyCandidates resolves. Deleting them would delete the ability to seed one
// account or one Region, so the narrow discharge the baseline offers — a field written and
// never read — is not available here.
//
// What is true instead is that no AWS response ever carries one. Each seeded* getter hands
// the struct back and its caller copies *named* fields onto a published shape, so the
// projection already exists in code and what was missing was this file: the citation the
// projected inventory requires before it will accept a record.
//
// Four more operations consult a seed and are deliberately not listed below, because they
// carry nothing a projection could leak: StartConfigurationRecorder and
// StopConfigurationRecorder answer an empty body, and PutConformancePack and
// DeleteConformancePack consult cfgsvcPackStateView only to decide whether the call is
// allowed.

// configSeedScopeMembers are the two members every Config seed declares and no AWS Config
// shape publishes, spelled as the json tags the seed structs use — those are the names
// that would reach a caller.
//
// Neither carries `omitempty`, so unlike the ever_tagged class of member these cannot be
// absent for free: a seed that was found at all had both populated. That is what makes an
// absence assertion on them worth making.
var configSeedScopeMembers = []string{"accountId", "region"}

// The account and Region the signed test request resolves to. The seeds below are scoped
// to exactly this pair rather than to the "*" wildcard, so cfgsvcCtrlKeyCandidates answers
// from its most specific candidate — the one whose document has both members filled in.
const (
	configWireAccount = "123456789012"
	configWireRegion  = "us-east-1"
)

// configRawBody runs one Config request, requires a 200, and returns the undecoded body.
//
// The raw bytes are the point of this file. Every other helper in the Config suite decodes
// into a struct, which is exactly what hides a member the struct does not declare.
func configRawBody(t *testing.T, ts *emulator.TestServer, operation string, body any) string {
	t.Helper()
	resp := configRequest(t, ts, operation, body)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	var raw json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&raw))
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s answered %d: %s", operation, resp.StatusCode, raw)
	return string(raw)
}

// configSeedScoped posts a Config seed carrying the account and Region of the caller the
// test's signed request resolves to.
//
// Injected centrally rather than spelled into each body, so a case cannot forget it — and
// a case that did would make every assertion in this file pass against an unpopulated
// document.
func configSeedScoped(t *testing.T, ts *emulator.TestServer, path string, body map[string]any) {
	t.Helper()
	scoped := map[string]any{"accountId": configWireAccount, "region": configWireRegion}
	for key, value := range body {
		scoped[key] = value
	}
	configSeed(t, ts, path, scoped)
}

// configAssertNoSeedScopeMember fails if a response names a seed's scope anywhere in its
// document, at any depth.
//
// A member-name walk rather than the substring form emulator/efs_wire_test.go uses,
// because these same bodies legitimately contain the account ID and the Region as
// substrings of a Config ARN — so only an assertion about the member *name* is available,
// and a walk states "nowhere in the document" where a substring check only rules out one
// spelling of one nesting.
func configAssertNoSeedScopeMember(t *testing.T, site, raw string) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc), "%s answered undecodable JSON: %s", site, raw)
	configWalkJSONMembers(t, site, "$", doc)
}

// configWalkJSONMembers recurses through a decoded JSON document asserting on every member
// name it meets, reporting the path so a failure names the member rather than the service.
func configWalkJSONMembers(t *testing.T, site, path string, node any) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			assert.NotContainsf(t, configSeedScopeMembers, key,
				"%s answered %s, which scopes a seed and no AWS Config shape publishes", site, child)
			configWalkJSONMembers(t, site, child, value)
		}
	case []any:
		for i, value := range typed {
			configWalkJSONMembers(t, site, fmt.Sprintf("%s[%d]", path, i), value)
		}
	}
}

// TestConfigWire_SeededResponsesCarryNoSeedScopeMember is the assertion all five of AWS
// Config's projected seed records cite.
//
// It runs over every response that carries a seed-derived value, because a projection is
// only as good as its least projected caller and a site added later has to be added here
// too. Each case asserts a presence anchor before the absences: without one, a response
// that never consulted the seed would satisfy the absence trivially, and the test would
// pass while proving nothing.
//
// The seeds are written after the recorder is started, not before. StartConfigurationRecorder
// persists the status it read, so seeding first would have the fixture's own ordering
// decide what is stored rather than what is projected.
func TestConfigWire_SeededResponsesCarryNoSeedScopeMember(t *testing.T) {
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-wire-logs")
	configStartRecorder(t, ts, "default")
	configPutRule(t, ts, "s3-encrypted")
	configPutPack(t, ts, "ops")

	configSeedScoped(t, ts, "/v1/config/recorder-status", map[string]any{
		"lastStatus":       "Failure",
		"lastErrorCode":    "InsufficientDeliveryPolicy",
		"lastErrorMessage": "Cannot write to the delivery bucket",
	})
	configSeedScoped(t, ts, "/v1/config/delivery-status", map[string]any{
		"status":           "Failure",
		"lastErrorCode":    "AccessDenied",
		"lastErrorMessage": "The bucket policy denies the write",
	})
	configSeedScoped(t, ts, "/v1/config/rule-compliance/s3-encrypted", map[string]any{
		"complianceType": "NON_COMPLIANT",
		"annotation":     "Bucket b1 is unencrypted",
		"resources":      []map[string]any{{"resourceType": "AWS::S3::Bucket", "resourceId": "b1"}},
	})
	configSeedScoped(t, ts, "/v1/config/pack-status/ops", map[string]any{
		"state":        "CREATE_FAILED",
		"statusReason": "The template could not be read",
	})
	configSeedScoped(t, ts, "/v1/config/pack-compliance/ops", map[string]any{
		"rules": []map[string]any{{"configRuleName": "iam-password-policy", "complianceType": "NON_COMPLIANT"}},
	})

	for _, tc := range []struct {
		record    string
		operation string
		request   map[string]any
		anchor    string
	}{
		{
			"cfgsvcSeededRecorderStatus", "DescribeConfigurationRecorderStatus",
			map[string]any{}, `"lastStatus":"Failure"`,
		},
		{
			"cfgsvcSeededDeliveryStatus", "DescribeDeliveryChannelStatus",
			map[string]any{}, `"lastErrorCode":"AccessDenied"`,
		},
		{
			"cfgsvcSeededRuleCompliance", "DescribeComplianceByConfigRule",
			map[string]any{"ConfigRuleNames": []string{"s3-encrypted"}}, `"ComplianceType":"NON_COMPLIANT"`,
		},
		{
			"cfgsvcSeededRuleCompliance", "GetComplianceDetailsByConfigRule",
			map[string]any{"ConfigRuleName": "s3-encrypted"}, `"Annotation":"Bucket b1 is unencrypted"`,
		},
		{
			"cfgsvcSeededPackStatus", "DescribeConformancePackStatus",
			map[string]any{"ConformancePackNames": []string{"ops"}}, `"ConformancePackState":"CREATE_FAILED"`,
		},
		{
			"cfgsvcSeededPackCompliance", "DescribeConformancePackCompliance",
			map[string]any{"ConformancePackName": "ops"}, `"ConfigRuleName":"iam-password-policy"`,
		},
		{
			"cfgsvcSeededPackCompliance", "GetConformancePackComplianceSummary",
			map[string]any{"ConformancePackNames": []string{"ops"}},
			`"ConformancePackComplianceStatus":"NON_COMPLIANT"`,
		},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			raw := configRawBody(t, ts, tc.operation, tc.request)
			require.Containsf(t, raw, tc.anchor,
				"presence anchor: %s has to carry %s's seed for an absence to mean anything",
				tc.operation, tc.record)
			configAssertNoSeedScopeMember(t, tc.operation, raw)
		})
	}
}
