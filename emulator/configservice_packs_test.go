package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/require"
)

// AWS Config conformance-pack tests (#580).
//
// The behavior worth defending here is the waiter: a pack is CREATE_IN_PROGRESS
// until something observes it, then CREATE_COMPLETE forever. Both halves are
// asserted, because a status that never advanced and a status that re-resolved on
// every poll each break a consumer's waiter, in opposite directions.

// configPutPack creates a pack with a template body and returns its ARN.
func configPutPack(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	resp := configRequest(t, ts, "PutConformancePack", map[string]any{
		"ConformancePackName": name,
		"TemplateBody":        `{"Resources":{}}`,
	})
	var out struct {
		ConformancePackArn string `json:"ConformancePackArn"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConformancePackArn
}

// configPutPackRaw sends a PutConformancePack and returns the status and error code,
// for the cases whose subject is the refusal.
func configPutPackRaw(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "PutConformancePack", body)
	return decodeConfigResponse(t, resp, nil)
}

// configDescribePacks returns the ConformancePackDetails.
func configDescribePacks(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeConformancePacks", body)
	var out struct {
		ConformancePackDetails []map[string]any `json:"ConformancePackDetails"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConformancePackDetails
}

// configPackStatus returns the ConformancePackStatusDetails.
func configPackStatus(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	return configPackStatusIn(t, ts, "us-east-1", body)
}

// configPackStatusIn returns the ConformancePackStatusDetails from a named Region.
func configPackStatusIn(t *testing.T, ts *emulator.TestServer, region string,
	body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequestIn(t, ts, region, "DescribeConformancePackStatus", body)
	var out struct {
		ConformancePackStatusDetails []map[string]any `json:"ConformancePackStatusDetails"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConformancePackStatusDetails
}

// configPackStateOf reports the state of one named pack, requiring it be present.
func configPackStateOf(t *testing.T, ts *emulator.TestServer, name string) map[string]any {
	t.Helper()
	details := configPackStatus(t, ts, map[string]any{"ConformancePackNames": []string{name}})
	require.Len(t, details, 1, "the pack has a status")
	return details[0]
}

// configPackCompliance returns the ConformancePackRuleComplianceList.
func configPackCompliance(t *testing.T, ts *emulator.TestServer, body map[string]any) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeConformancePackCompliance", body)
	var out struct {
		ConformancePackRuleComplianceList []map[string]any `json:"ConformancePackRuleComplianceList"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConformancePackRuleComplianceList
}

// configPackSummary returns the ConformancePackComplianceSummaryList.
func configPackSummary(t *testing.T, ts *emulator.TestServer, names []string) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "GetConformancePackComplianceSummary", map[string]any{
		"ConformancePackNames": names,
	})
	var out struct {
		ConformancePackComplianceSummaryList []map[string]any `json:"ConformancePackComplianceSummaryList"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConformancePackComplianceSummaryList
}

// configDeletePack sends a DeleteConformancePack and returns the refusal, if any.
func configDeletePack(t *testing.T, ts *emulator.TestServer, name string) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "DeleteConformancePack", map[string]any{"ConformancePackName": name})
	return decodeConfigResponse(t, resp, nil)
}

// configSeedPackCompliance pins a pack's per-rule verdicts and requires the seed be
// accepted.
func configSeedPackCompliance(t *testing.T, ts *emulator.TestServer, pack string, rules []map[string]any) {
	t.Helper()
	configSeed(t, ts, "/v1/config/pack-compliance/"+pack, map[string]any{"rules": rules})
}

func TestConformancePacks_PutReturnsOnlyTheARN(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConformancePack", map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{}}`,
	})
	var out map[string]any
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	// The response shape carries one member and no state, which is why a consumer has to
	// poll the status operation to learn whether its pack deployed at all.
	require.Equal(t, []string{"ConformancePackArn"}, mapKeys(out))
	arn, ok := out["ConformancePackArn"].(string)
	require.True(t, ok)
	require.Regexp(t,
		`^arn:aws:config:us-east-1:123456789012:conformance-pack/ops/conformance-pack-[a-z0-9_-]{8}$`, arn)
}

func TestConformancePacks_APutDoesNotRequireARecorder(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// PutConfigRule and PutDeliveryChannel both refuse without a recorder and declare
	// NoAvailableConfigurationRecorderException for it. PutConformancePack declares no
	// such error, so a pack can be created in an account that records nothing — the
	// asymmetry a fixture would otherwise have to discover by being refused.
	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

func TestConformancePacks_StatusResolvesOnObservationAndThenStaysPut(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	// The first observation both reports and settles the transition, so a waiter
	// converges in one poll without waiting on any clock.
	first := configPackStateOf(t, ts, "ops")
	require.Equal(t, "CREATE_COMPLETE", first["ConformancePackState"])
	require.NotNil(t, first["LastUpdateCompletedTime"], "a completed pack carries a completion time")

	// The second is the half that matters more: a status that re-resolved would move
	// LastUpdateCompletedTime under a waiter comparing successive polls, and such a
	// waiter would never converge.
	second := configPackStateOf(t, ts, "ops")
	require.Equal(t, first, second, "a resolved status is identical on every later observation")

	third := configPackStateOf(t, ts, "ops")
	require.Equal(t, first, third)
}

func TestConformancePacks_AnUnobservedPackIsStillInProgress(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	// DescribeConformancePacks reports configuration, not state, so it must not resolve
	// the transition: if it did, a consumer that listed its packs before polling would
	// never see CREATE_IN_PROGRESS and the in-progress refusals below would be
	// unreachable.
	details := configDescribePacks(t, ts, nil)
	require.Len(t, details, 1)
	require.NotContains(t, details[0], "ConformancePackState",
		"ConformancePackDetail has no state member — state lives only on the status shape")

	// A second Put before anything observed the first is ResourceInUseException, which is
	// the ordering bug a consumer that creates and immediately updates actually hits.
	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceInUseException", code)
	require.Contains(t, message, "For PutConformancePack and PutOrganizationConformancePack")

	// So is a delete, and its message is the bullet for *its* operation rather than the
	// Put's — the exception's documentation is a list covering seven unrelated cases.
	status, code, message = configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceInUseException", code)
	require.Contains(t, message, "For DeleteConformancePack")
}

func TestConformancePacks_AnUpdateReentersCreateInProgress(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])

	// The enum has no UPDATE_ state, so an update re-enters CREATE_IN_PROGRESS. Reporting
	// CREATE_COMPLETE straight away would let a waiter return before the update it just
	// requested had been deployed at all.
	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{"Updated":{}}}`,
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	details := configDescribePacks(t, ts, nil)
	require.Len(t, details, 1, "an update does not create a second pack")

	// And the update's own transition resolves on observation, like the create's.
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
}

func TestConformancePacks_StatusCarriesEveryRequiredMember(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	detail := configPackStateOf(t, ts, "ops")
	// All six are Required: Yes on ConformancePackStatusDetail. A consumer decoding a
	// required member into a non-pointer field gets a zero value rather than an error
	// when one is missing, so an omission would surface as an empty string in its logs
	// rather than as a failure.
	for _, member := range []string{
		"ConformancePackName", "ConformancePackId", "ConformancePackArn",
		"ConformancePackState", "StackArn", "LastUpdateRequestedTime",
	} {
		require.NotEmpty(t, detail[member], "%s is a required member of ConformancePackStatusDetail", member)
	}

	// StackArn names a CloudFormation stack substrate does not create — nothing backs it
	// — but it is required, so a well-formed synthetic ARN is the honest answer. The
	// awsconfigconforms- prefix is the convention Config uses for pack stacks.
	stackARN, ok := detail["StackArn"].(string)
	require.True(t, ok)
	require.Regexp(t,
		`^arn:aws:cloudformation:us-east-1:123456789012:stack/awsconfigconforms-ops-[a-z0-9_-]{8}/`+
			`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, stackARN)
}

func TestConformancePacks_IdentifiersAreStableAcrossPuts(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	first := configPutPack(t, ts, "ops")
	configPackStateOf(t, ts, "ops")
	second := configPutPack(t, ts, "ops")

	// The ID is derived from account, Region and name rather than drawn from a clock or
	// a random source, so replaying the same event stream mints the same ARN. An update
	// keeps it, which is what lets a fixture assert an ARN it captured earlier.
	require.Equal(t, first, second, "an update does not re-mint the pack's ARN")
}

func TestConformancePacks_DescribeRefusesAnUnknownNameButStatusDoesNot(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	// DescribeConformancePacks declares NoSuchConformancePackException; a name matching
	// nothing refuses the whole call.
	resp := configRequest(t, ts, "DescribeConformancePacks", map[string]any{
		"ConformancePackNames": []string{"absent"},
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConformancePackException", code)

	// DescribeConformancePackStatus does not declare it at all — "If there are no
	// conformance packs then you will see an empty result" — so the same name is an
	// empty list. Answering the two the same way would send a consumer down a branch
	// one of them has not got.
	require.Empty(t, configPackStatus(t, ts, map[string]any{
		"ConformancePackNames": []string{"absent"},
	}))

	// A mixed request refuses on the missing one rather than silently returning the
	// packs that do exist.
	resp = configRequest(t, ts, "DescribeConformancePacks", map[string]any{
		"ConformancePackNames": []string{"ops", "absent"},
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConformancePackException", code)
}

func TestConformancePacks_ExactlyOneTemplateSourceIsRequired(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	cases := []struct {
		name string
		body map[string]any
	}{
		{
			name: "none",
			body: map[string]any{"ConformancePackName": "ops"},
		},
		{
			name: "body and s3 uri",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        `{"Resources":{}}`,
				"TemplateS3Uri":       "s3://templates/ops.yaml",
			},
		},
		{
			name: "body and ssm document",
			body: map[string]any{
				"ConformancePackName":        "ops",
				"TemplateBody":               `{"Resources":{}}`,
				"TemplateSSMDocumentDetails": map[string]any{"DocumentName": "ops-pack"},
			},
		},
		{
			name: "all three",
			body: map[string]any{
				"ConformancePackName":        "ops",
				"TemplateBody":               `{"Resources":{}}`,
				"TemplateS3Uri":              "s3://templates/ops.yaml",
				"TemplateSSMDocumentDetails": map[string]any{"DocumentName": "ops-pack"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, code, message := configPutPackRaw(t, ts, tc.body)
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, "InvalidParameterValueException", code)
			// AWS's own sentence, its "the follow" typo included, so a consumer matching on
			// message text matches.
			require.Contains(t, message, "You must specify only one of the follow parameters")
		})
	}
}

func TestConformancePacks_EachTemplateSourceIsAcceptedOnItsOwn(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body map[string]any
	}{
		{
			name: "template body",
			body: map[string]any{"TemplateBody": `{"Resources":{}}`},
		},
		{
			name: "s3 uri",
			body: map[string]any{"TemplateS3Uri": "s3://templates/ops.yaml"},
		},
		{
			name: "ssm document",
			body: map[string]any{
				"TemplateSSMDocumentDetails": map[string]any{
					"DocumentName":    "ops-pack",
					"DocumentVersion": "$LATEST",
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			body := map[string]any{"ConformancePackName": "ops"}
			for k, v := range tc.body {
				body[k] = v
			}
			// Accepting each source is worth asserting on its own: refusing one AWS permits
			// would break a working template, which is the worse failure of the two.
			status, code, message := configPutPackRaw(t, ts, body)
			require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		})
	}
}

func TestConformancePacks_PutRefusals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		body    map[string]any
		code    string
		message string
	}{
		{
			name: "no name",
			body: map[string]any{"TemplateBody": `{"Resources":{}}`},
			code: "InvalidParameterValueException",
		},
		{
			name: "name starting with a digit",
			body: map[string]any{"ConformancePackName": "1ops", "TemplateBody": `{"Resources":{}}`},
			code: "InvalidParameterValueException",
		},
		{
			name: "name with an underscore",
			body: map[string]any{"ConformancePackName": "ops_pack", "TemplateBody": `{"Resources":{}}`},
			code: "InvalidParameterValueException",
		},
		{
			name: "name over 256 characters",
			body: map[string]any{
				"ConformancePackName": "o" + strings.Repeat("p", 256),
				"TemplateBody":        `{"Resources":{}}`,
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "a template s3 uri that is not an s3 uri",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateS3Uri":       "https://templates.example/ops.yaml",
			},
			// The pattern is the *template's* problem, so the model's own template exception
			// answers rather than the generic input one.
			code:    "ConformancePackTemplateValidationException",
			message: "You have specified a template that is not valid or supported.",
		},
		{
			name: "a template body over 51200 bytes",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        strings.Repeat("x", 51201),
			},
			code: "ConformancePackTemplateValidationException",
		},
		{
			name: "an ssm document name too short for the pattern",
			body: map[string]any{
				"ConformancePackName":        "ops",
				"TemplateSSMDocumentDetails": map[string]any{"DocumentName": "op"},
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "an ssm document version that is neither a keyword nor a number",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateSSMDocumentDetails": map[string]any{
					"DocumentName":    "ops-pack",
					"DocumentVersion": "latest",
				},
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "a delivery bucket over 63 characters",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        `{"Resources":{}}`,
				"DeliveryS3Bucket":    strings.Repeat("b", 64),
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "a delivery key prefix over 1024 characters",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        `{"Resources":{}}`,
				"DeliveryS3KeyPrefix": strings.Repeat("p", 1025),
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "an input parameter with no name",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        `{"Resources":{}}`,
				"ConformancePackInputParameters": []map[string]any{
					{"ParameterValue": "v"},
				},
			},
			code: "InvalidParameterValueException",
		},
		{
			name: "an input parameter value over 4096 characters",
			body: map[string]any{
				"ConformancePackName": "ops",
				"TemplateBody":        `{"Resources":{}}`,
				"ConformancePackInputParameters": []map[string]any{
					{"ParameterName": "n", "ParameterValue": strings.Repeat("v", 4097)},
				},
			},
			code: "InvalidParameterValueException",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			status, code, message := configPutPackRaw(t, ts, tc.body)
			// Every AWS Config exception is HTTP 400, the not-found ones included, so a
			// consumer matches on the code rather than the status.
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, tc.code, code, message)
			if tc.message != "" {
				require.Contains(t, message, tc.message)
			}
		})
	}
}

func TestConformancePacks_InputParametersAreAcceptedUpToSixty(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	params := make([]map[string]any, 0, 61)
	for i := range 60 {
		params = append(params, map[string]any{
			"ParameterName":  fmt.Sprintf("p%02d", i),
			"ParameterValue": "v",
		})
	}
	// Accepting the model's own maximum matters as much as refusing 61: a narrower cap
	// would refuse a request AWS permits, and a fixture with a large parameterised pack
	// would fail for a reason that is substrate's alone.
	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName":            "ops",
		"TemplateBody":                   `{"Resources":{}}`,
		"ConformancePackInputParameters": params,
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	status, code, _ = configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "big",
		"TemplateBody":        `{"Resources":{}}`,
		"ConformancePackInputParameters": append(params, map[string]any{
			"ParameterName": "p60", "ParameterValue": "v",
		}),
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)
}

func TestConformancePacks_DetailReportsWhatWasSubmittedAndNoTemplate(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{"Rule":{}}}`,
		"DeliveryS3Bucket":    "cfg-packs",
		"DeliveryS3KeyPrefix": "ops/",
		"ConformancePackInputParameters": []map[string]any{
			{"ParameterName": "MaxAge", "ParameterValue": "90"},
		},
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	details := configDescribePacks(t, ts, nil)
	require.Len(t, details, 1)
	detail := details[0]
	require.Equal(t, "ops", detail["ConformancePackName"])
	require.Equal(t, "cfg-packs", detail["DeliveryS3Bucket"])
	require.Equal(t, "ops/", detail["DeliveryS3KeyPrefix"])
	require.Len(t, detail["ConformancePackInputParameters"], 1)

	// Neither ConformancePackDetail nor the status shape has a template member, so the
	// template a caller submitted is recorded intent it cannot read back. Emitting it
	// would be an invention no SDK field would receive.
	require.NotContains(t, detail, "TemplateBody")
	require.NotContains(t, detail, "TemplateS3Uri")
	// CreatedBy names the AWS service that created a service-managed pack; substrate
	// models none, so reporting a value would claim a caller's pack was service-created.
	require.NotContains(t, detail, "CreatedBy")
}

func TestConformancePacks_LimitIsBoundedAtTwentyWithInvalidLimitException(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	cases := []struct {
		operation string
		limit     int
		code      string
	}{
		// PageSizeLimit's Valid Range is 0-20 for these three, so 20 must be accepted: a
		// narrower ceiling would refuse a page size AWS serves.
		{"DescribeConformancePacks", 20, ""},
		{"DescribeConformancePacks", 21, "InvalidLimitException"},
		{"DescribeConformancePacks", -1, "InvalidLimitException"},
		{"DescribeConformancePackStatus", 20, ""},
		{"DescribeConformancePackStatus", 21, "InvalidLimitException"},
		// DescribeConformancePackCompliance's own ceiling is 1000, a different bound on the
		// same service — collapsing the two onto one number would let a fixture page in
		// units one of the operations refuses.
		{"DescribeConformancePackCompliance", 1000, ""},
		{"DescribeConformancePackCompliance", 1001, "InvalidLimitException"},
		{"GetConformancePackComplianceSummary", 20, ""},
		{"GetConformancePackComplianceSummary", 21, "InvalidLimitException"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d", tc.operation, tc.limit), func(t *testing.T) {
			t.Parallel()
			body := map[string]any{"Limit": tc.limit}
			switch tc.operation {
			case "DescribeConformancePackCompliance":
				body["ConformancePackName"] = "ops"
			case "GetConformancePackComplianceSummary":
				body["ConformancePackNames"] = []string{"ops"}
			}
			resp := configRequest(t, ts, tc.operation, body)
			status, code, message := decodeConfigResponse(t, resp, nil)
			if tc.code == "" {
				require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
				return
			}
			// The rule cluster answers InvalidParameterValueException for the same complaint
			// because none of its operations declares InvalidLimitException, while all four
			// here do. A caller's handling is written against the codes its operation declares.
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, tc.code, code)
			require.Equal(t, "The specified limit is outside the allowable range.", message)
		})
	}
}

func TestConformancePacks_PaginationWalksEveryPack(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 25 {
		configPutPack(t, ts, fmt.Sprintf("pack-%02d", i))
	}

	seen := map[string]bool{}
	token := ""
	for pages := 0; pages < 10; pages++ {
		body := map[string]any{"Limit": 20}
		if token != "" {
			body["NextToken"] = token
		}
		resp := configRequest(t, ts, "DescribeConformancePacks", body)
		var out struct {
			ConformancePackDetails []map[string]any `json:"ConformancePackDetails"`
			NextToken              string           `json:"NextToken"`
		}
		status, code, message := decodeConfigResponse(t, resp, &out)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		require.LessOrEqual(t, len(out.ConformancePackDetails), 20, "a page never exceeds the cap")
		for _, detail := range out.ConformancePackDetails {
			name, ok := detail["ConformancePackName"].(string)
			require.True(t, ok)
			require.False(t, seen[name], "%s appears on two pages", name)
			seen[name] = true
		}
		token = out.NextToken
		if token == "" {
			break
		}
	}
	require.Equal(t, "", token, "the walk terminates")
	require.Len(t, seen, 25, "every pack is reported exactly once across the pages")
}

func TestConformancePacks_ABadNextTokenIsRefused(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	for _, operation := range []string{"DescribeConformancePacks", "DescribeConformancePackStatus"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			resp := configRequest(t, ts, operation, map[string]any{"NextToken": "not-a-token!!"})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, "InvalidNextTokenException", code)
		})
	}
}

func TestConformancePacks_NameFiltersAreBounded(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	names := make([]string, 0, 26)
	for i := range 26 {
		names = append(names, fmt.Sprintf("pack-%02d", i))
	}
	resp := configRequest(t, ts, "DescribeConformancePacks", map[string]any{
		"ConformancePackNames": names,
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)
}

func TestConformancePacks_TheSummaryRequiresBetweenOneAndFiveNames(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 6 {
		configPutPack(t, ts, fmt.Sprintf("pack-%d", i))
	}

	// ConformancePackNames is Required: Yes with Array Members 1-5 here, unlike the
	// describe operations' optional 0-25 — the summary cannot be asked for every pack.
	resp := configRequest(t, ts, "GetConformancePackComplianceSummary", map[string]any{})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)

	resp = configRequest(t, ts, "GetConformancePackComplianceSummary", map[string]any{
		"ConformancePackNames": []string{"pack-0", "pack-1", "pack-2", "pack-3", "pack-4", "pack-5"},
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)

	// Five is the model's own maximum and must be served.
	summaries := configPackSummary(t, ts, []string{"pack-0", "pack-1", "pack-2", "pack-3", "pack-4"})
	require.Len(t, summaries, 5)
}

func TestConformancePacks_TheSummaryRefusesAnUnknownPack(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	resp := configRequest(t, ts, "GetConformancePackComplianceSummary", map[string]any{
		"ConformancePackNames": []string{"ops", "absent"},
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConformancePackException", code)
}

func TestConformancePacks_ComplianceDefaultsToInsufficientData(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	// Substrate deploys no template and evaluates no rules, so an unseeded pack reports
	// nothing and summarizes as INSUFFICIENT_DATA — the verdict AWS gives a pack that has
	// not evaluated. A fabricated COMPLIANT would make every compliance assertion in
	// every consumer's suite pass for free.
	require.Empty(t, configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"}))

	summaries := configPackSummary(t, ts, []string{"ops"})
	require.Len(t, summaries, 1)
	require.Equal(t, "INSUFFICIENT_DATA", summaries[0]["ConformancePackComplianceStatus"])
}

func TestConformancePacks_SeededComplianceIsReportedAndSummarized(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configSeedPackCompliance(t, ts, "ops", []map[string]any{
		{"configRuleName": "iam-password-policy", "complianceType": "COMPLIANT",
			"controls": []string{"CIS 1.5"}},
		{"configRuleName": "s3-encrypted", "complianceType": "NON_COMPLIANT"},
	})

	list := configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"})
	require.Len(t, list, 2)
	byName := map[string]map[string]any{}
	for _, entry := range list {
		name, ok := entry["ConfigRuleName"].(string)
		require.True(t, ok)
		byName[name] = entry
	}
	require.Equal(t, "COMPLIANT", byName["iam-password-policy"]["ComplianceType"])
	require.Equal(t, []any{"CIS 1.5"}, byName["iam-password-policy"]["Controls"])
	require.Equal(t, "NON_COMPLIANT", byName["s3-encrypted"]["ComplianceType"])
	require.NotContains(t, byName["s3-encrypted"], "Controls", "Controls is optional")

	// The summary is derived from these same verdicts rather than seeded separately, so
	// the two cannot disagree — a consumer that reads the summary and then drills into
	// the rules gets one story.
	summaries := configPackSummary(t, ts, []string{"ops"})
	require.Equal(t, "NON_COMPLIANT", summaries[0]["ConformancePackComplianceStatus"])
}

func TestConformancePacks_TheCumulativeVerdictRanksNonCompliantHighest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		rules   []map[string]any
		verdict string
	}{
		{
			name:    "no rules",
			rules:   []map[string]any{},
			verdict: "INSUFFICIENT_DATA",
		},
		{
			name: "all compliant",
			rules: []map[string]any{
				{"configRuleName": "a", "complianceType": "COMPLIANT"},
				{"configRuleName": "b", "complianceType": "COMPLIANT"},
			},
			verdict: "COMPLIANT",
		},
		{
			name: "one unevaluated",
			rules: []map[string]any{
				{"configRuleName": "a", "complianceType": "COMPLIANT"},
				{"configRuleName": "b", "complianceType": "INSUFFICIENT_DATA"},
			},
			verdict: "INSUFFICIENT_DATA",
		},
		{
			name: "one failure alongside an unevaluated rule",
			rules: []map[string]any{
				{"configRuleName": "a", "complianceType": "INSUFFICIENT_DATA"},
				{"configRuleName": "b", "complianceType": "NON_COMPLIANT"},
			},
			// A known failure outranks an unknown: a pack with something wrong in it is not
			// "we don't know yet", and reporting INSUFFICIENT_DATA would let a consumer
			// treating unknown as "wait and retry" loop past a real problem.
			verdict: "NON_COMPLIANT",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			configPutPack(t, ts, "ops")
			configSeedPackCompliance(t, ts, "ops", tc.rules)
			summaries := configPackSummary(t, ts, []string{"ops"})
			require.Len(t, summaries, 1)
			require.Equal(t, tc.verdict, summaries[0]["ConformancePackComplianceStatus"])
		})
	}
}

func TestConformancePacks_TheComplianceSeedRefusesNotApplicable(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// NOT_APPLICABLE is a rule-level ComplianceType but is absent from
	// ConformancePackComplianceType, so storing one would make this cluster report a
	// value no SDK enum member matches — a caller's switch would fall through to its
	// default and the test would pass while asserting nothing.
	status, body := configSeedRaw(t, ts, "/v1/config/pack-compliance/ops", map[string]any{
		"rules": []map[string]any{
			{"configRuleName": "a", "complianceType": "NOT_APPLICABLE"},
		},
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Contains(t, body, "NOT_APPLICABLE")
}

func TestConformancePacks_TheComplianceSeedRefusesADuplicateRule(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// Two verdicts for one rule name would make the reported one depend on map iteration
	// order, which is precisely the nondeterminism this emulator exists to remove.
	status, body := configSeedRaw(t, ts, "/v1/config/pack-compliance/ops", map[string]any{
		"rules": []map[string]any{
			{"configRuleName": "a", "complianceType": "COMPLIANT"},
			{"configRuleName": "a", "complianceType": "NON_COMPLIANT"},
		},
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Contains(t, body, "appears twice")
}

func TestConformancePacks_TheComplianceSeedRefusalsAreReadable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "a rule with no name",
			body: map[string]any{"rules": []map[string]any{{"complianceType": "COMPLIANT"}}},
			want: "configRuleName",
		},
		{
			name: "a rule name over 128 characters",
			body: map[string]any{"rules": []map[string]any{
				{"configRuleName": strings.Repeat("r", 129), "complianceType": "COMPLIANT"},
			}},
			want: "configRuleName",
		},
		{
			name: "no compliance type",
			body: map[string]any{"rules": []map[string]any{{"configRuleName": "a"}}},
			want: "ConformancePackComplianceType",
		},
		{
			name: "more than twenty controls",
			body: map[string]any{"rules": []map[string]any{
				{"configRuleName": "a", "complianceType": "COMPLIANT",
					"controls": sliceOfN("c", 21)},
			}},
			want: "controls",
		},
		{
			name: "an empty control",
			body: map[string]any{"rules": []map[string]any{
				{"configRuleName": "a", "complianceType": "COMPLIANT", "controls": []string{""}},
			}},
			want: "control",
		},
		{
			name: "a body that is not an object",
			body: nil,
			want: "cannot unmarshal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			var body any = tc.body
			if tc.body == nil {
				body = "not-a-seed-object"
			}
			status, response := configSeedRaw(t, ts, "/v1/config/pack-compliance/ops", body)
			require.Equal(t, http.StatusBadRequest, status)
			// A refusal a fixture author cannot read is a refusal they will work around by
			// guessing, so the message names what was wrong and the body is valid JSON.
			require.Contains(t, response, tc.want)
			require.Contains(t, response, `"error"`)
		})
	}
}

func TestConformancePacks_ASeedContradictingItsPathIsRefused(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for _, path := range []string{"/v1/config/pack-status/ops", "/v1/config/pack-compliance/ops"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			// The path names the pack, so a body naming a different one is a contradiction
			// rather than an override: honoring either would leave the caller unable to tell
			// which pack it seeded.
			status, response := configSeedRaw(t, ts, path, map[string]any{
				"conformancePackName": "other",
				"state":               "CREATE_FAILED",
				"rules":               []map[string]any{},
			})
			require.Equal(t, http.StatusBadRequest, status)
			require.Contains(t, response, "contradicts")
		})
	}
}

func TestConformancePacks_ExactRuleNamesAreRequiredInAFilter(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configSeedPackCompliance(t, ts, "ops", []map[string]any{
		{"configRuleName": "s3-encrypted", "complianceType": "NON_COMPLIANT"},
		{"configRuleName": "iam-password-policy", "complianceType": "COMPLIANT"},
	})

	// "You must provide exact rule names." A filter naming a rule the pack does not
	// report is a refusal rather than an empty list, so a typo in a fixture is caught
	// instead of silently passing as "nothing matched".
	resp := configRequest(t, ts, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "ops",
		"Filters":             map[string]any{"ConfigRuleNames": []string{"s3-encrypted", "typo"}},
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConfigRuleInConformancePackException", code)

	// An exact name filters as expected.
	list := configPackCompliance(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"Filters":             map[string]any{"ConfigRuleNames": []string{"s3-encrypted"}},
	})
	require.Len(t, list, 1)
	require.Equal(t, "s3-encrypted", list[0]["ConfigRuleName"])
}

func TestConformancePacks_TheComplianceFilterExcludesInsufficientData(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configSeedPackCompliance(t, ts, "ops", []map[string]any{
		{"configRuleName": "a", "complianceType": "COMPLIANT"},
		{"configRuleName": "b", "complianceType": "NON_COMPLIANT"},
		{"configRuleName": "c", "complianceType": "INSUFFICIENT_DATA"},
	})

	// The response carries INSUFFICIENT_DATA — it is in the enum the shape declares.
	list := configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"})
	require.Len(t, list, 3)

	// But the *filter* does not: "The allowed values are COMPLIANT and NON_COMPLIANT.
	// INSUFFICIENT_DATA is not supported." Accepting it would answer with an empty list
	// where AWS answers with an error, so a fixture would read "no rules match" instead
	// of "you cannot ask that".
	resp := configRequest(t, ts, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "ops",
		"Filters":             map[string]any{"ComplianceType": "INSUFFICIENT_DATA"},
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)
	require.Contains(t, message, "INSUFFICIENT_DATA is not supported")

	// NOT_APPLICABLE is not in the pack enum at all, so it is refused for that reason
	// rather than as an unsupported filter.
	resp = configRequest(t, ts, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "ops",
		"Filters":             map[string]any{"ComplianceType": "NOT_APPLICABLE"},
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValueException", code)

	// The two values the filter does accept work.
	for _, verdict := range []string{"COMPLIANT", "NON_COMPLIANT"} {
		list = configPackCompliance(t, ts, map[string]any{
			"ConformancePackName": "ops",
			"Filters":             map[string]any{"ComplianceType": verdict},
		})
		require.Len(t, list, 1)
		require.Equal(t, verdict, list[0]["ComplianceType"])
	}
}

func TestConformancePacks_TheRuleNameFilterIsBoundedAtTenAndSixtyFour(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	cases := []struct {
		name  string
		names []string
	}{
		{name: "eleven rule names", names: sliceOfN("rule-", 11)},
		{name: "a name over 64 characters", names: []string{strings.Repeat("r", 65)}},
		{name: "an empty name", names: []string{""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := configRequest(t, ts, "DescribeConformancePackCompliance", map[string]any{
				"ConformancePackName": "ops",
				"Filters":             map[string]any{"ConfigRuleNames": tc.names},
			})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			// The filter's per-name ceiling is 64, not the 128 a ConfigRuleName carries
			// elsewhere: a rule named longer than 64 can be reported by this operation but
			// not filtered for by it. The asymmetry is the model's.
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, "InvalidParameterValueException", code)
		})
	}
}

func TestConformancePacks_ComplianceRefusesAnUnknownPack(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "absent",
	})
	status, code, _ := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConformancePackException", code)
}

func TestConformancePacks_ASeededFailureStatePersistsAndIsClearable(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configSeed(t, ts, "/v1/config/pack-status/ops", map[string]any{
		"state":        "CREATE_FAILED",
		"statusReason": "the template declares an unsupported resource",
	})

	// Substrate deploys no stack, so a pack it created always completes and a consumer's
	// CREATE_FAILED branch — the branch that reports a bad template to its author — is
	// unreachable without this seed.
	first := configPackStateOf(t, ts, "ops")
	require.Equal(t, "CREATE_FAILED", first["ConformancePackState"])
	require.Equal(t, "the template declares an unsupported resource", first["ConformancePackStatusReason"])

	// A seeded state is stable across polls for the same reason a resolved one is.
	require.Equal(t, first, configPackStateOf(t, ts, "ops"))

	// Clearing restores the real state, because the seed is applied when the status is
	// read rather than written into the record. A seed that baked itself in would leave a
	// fixture unable to get back to the nominal path without deleting the pack.
	configClearSeed(t, ts, "/v1/config/pack-status/ops")
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
}

func TestConformancePacks_ASeededFailureIsStillDeletable(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configSeed(t, ts, "/v1/config/pack-status/ops", map[string]any{"state": "CREATE_FAILED"})

	// CREATE_FAILED is terminal, so the delete goes through even though what is *stored*
	// still says CREATE_IN_PROGRESS — the record was never rewritten. A delete that read
	// the stored field instead would leave a failed pack undeletable, which is the state
	// a consumer most needs to clean up.
	status, code, message := configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Empty(t, configDescribePacks(t, ts, nil))
}

func TestConformancePacks_ASeededInProgressStateBlocksAPut(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
	configSeed(t, ts, "/v1/config/pack-status/ops", map[string]any{"state": "DELETE_IN_PROGRESS"})

	// The seed is what makes DELETE_IN_PROGRESS observable at all: substrate's delete
	// completes within the call, so no stored pack ever holds that state. A consumer whose
	// teardown races its rebuild needs the refusal to be reachable.
	status, code, _ := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceInUseException", code)

	status, code, _ = configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceInUseException", code)
}

func TestConformancePacks_TheStatusSeedRefusalsAreReadable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "no state",
			body: map[string]any{},
			want: "state is required",
		},
		{
			name: "a state outside the enum",
			body: map[string]any{"state": "UPDATE_COMPLETE"},
			want: "ConformancePackState",
		},
		{
			name: "delete complete, which the enum does not carry",
			body: map[string]any{"state": "DELETE_COMPLETE"},
			want: "ConformancePackState",
		},
		{
			name: "a status reason on a state that is not a failure",
			body: map[string]any{"state": "CREATE_COMPLETE", "statusReason": "all good"},
			// A reason on a non-failure state would be dropped by every consumer that reads it
			// only on failure, so the seed is refused rather than half-applied.
			want: "CREATE_FAILED or DELETE_FAILED",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			status, response := configSeedRaw(t, ts, "/v1/config/pack-status/ops", tc.body)
			require.Equal(t, http.StatusBadRequest, status)
			require.Contains(t, response, tc.want)
			require.Contains(t, response, `"error"`)
		})
	}
}

func TestConformancePacks_AWildcardSeedCoversEveryPack(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configPutPack(t, ts, "security")
	configSeed(t, ts, "/v1/config/pack-status/*", map[string]any{"state": "CREATE_FAILED"})

	// A fixture that does not enumerate its pack names — because a stack chose them —
	// still needs to assert "nothing deployed", which is what the wildcard is for.
	for _, name := range []string{"ops", "security"} {
		require.Equal(t, "CREATE_FAILED", configPackStateOf(t, ts, name)["ConformancePackState"])
	}

	// A named seed beats the wildcard: pack name is the more specific axis, so a fixture
	// that fails everything but completes one pack gets what it asked for.
	configSeed(t, ts, "/v1/config/pack-status/ops", map[string]any{"state": "CREATE_COMPLETE"})
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
	require.Equal(t, "CREATE_FAILED", configPackStateOf(t, ts, "security")["ConformancePackState"])
}

func TestConformancePacks_DeleteRemovesThePackAndItsSeeds(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configPackStateOf(t, ts, "ops")
	configSeedPackCompliance(t, ts, "ops", []map[string]any{
		{"configRuleName": "s3-encrypted", "complianceType": "NON_COMPLIANT"},
	})
	configSeed(t, ts, "/v1/config/pack-status/ops", map[string]any{"state": "CREATE_FAILED"})

	status, code, message := configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Empty(t, configDescribePacks(t, ts, nil))
	require.Empty(t, configPackStatus(t, ts, nil))

	// A rebuilt pack of the same name starts clean. Inheriting its predecessor's verdict
	// would make a teardown-and-rebuild fixture — a test run repeated twice — report the
	// first run's compliance on the second.
	configPutPack(t, ts, "ops")
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
	require.Empty(t, configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"}))
	summaries := configPackSummary(t, ts, []string{"ops"})
	require.Equal(t, "INSUFFICIENT_DATA", summaries[0]["ConformancePackComplianceStatus"])
}

func TestConformancePacks_DeleteLeavesTheWildcardSeedsAlone(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configPutPack(t, ts, "security")
	configSeed(t, ts, "/v1/config/pack-status/*", map[string]any{"state": "CREATE_FAILED"})

	status, code, message := configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	// A wildcard is a fixture-wide default rather than one pack's state, so deleting a
	// pack must not silently change what every other pack reports.
	require.Equal(t, "CREATE_FAILED", configPackStateOf(t, ts, "security")["ConformancePackState"])
}

func TestConformancePacks_DeleteAnswersAnEmptyBody(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")
	configPackStateOf(t, ts, "ops")

	resp := configRequest(t, ts, "DeleteConformancePack", map[string]any{"ConformancePackName": "ops"})
	var out map[string]any
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	// DeleteConformancePack has no output shape at all: "the service sends back an HTTP
	// 200 response with an empty HTTP body." Inventing a member would give a consumer
	// something to read that AWS never sends.
	require.Empty(t, out)
}

func TestConformancePacks_DeleteRefusesAnUnknownPack(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, code, _ := configDeletePack(t, ts, "absent")
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NoSuchConformancePackException", code)
}

func TestConformancePacks_TheAccountCapIsFifty(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 50 {
		configPutPack(t, ts, fmt.Sprintf("pack-%02d", i))
	}

	// The Service Limits page gives 50 and marks it not adjustable, so the fiftieth must
	// be accepted and the fifty-first refused.
	status, code, message := configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "one-too-many",
		"TemplateBody":        `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "MaxNumberOfConformancePacksExceededException", code)
	require.Contains(t, message, "Service Limits")

	// An update of an existing pack is not a new pack, so it is not refused at the cap —
	// a consumer at its limit can still change what it has.
	configPackStateOf(t, ts, "pack-00")
	status, code, message = configPutPackRaw(t, ts, map[string]any{
		"ConformancePackName": "pack-00",
		"TemplateBody":        `{"Resources":{"Updated":{}}}`,
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

func TestConformancePacks_AreIsolatedByRegion(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutPack(t, ts, "ops")

	// "Recording in one Region only" is the misconfiguration this service exists to make
	// visible, so every key here is regional and a pack created in one Region must be
	// absent from another.
	require.Empty(t, configPackStatusIn(t, ts, "eu-west-1", nil))

	// An empty listing is not on its own proof of isolation: the name index could be
	// regional while the records it points at are not, and the assertion above would
	// still pass. So put a pack of the *same name* in the second Region and require each
	// to report its own — with a shared record the second Put would silently edit the
	// first Region's pack, and its ARN would name the wrong Region.
	resp := configRequestIn(t, ts, "eu-west-1", "PutConformancePack", map[string]any{
		"ConformancePackName": "ops",
		"TemplateBody":        `{"Resources":{}}`,
	})
	var out struct {
		ConformancePackArn string `json:"ConformancePackArn"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Contains(t, out.ConformancePackArn, ":eu-west-1:")

	east := configPackStateOf(t, ts, "ops")
	require.Contains(t, east["ConformancePackArn"], ":us-east-1:")
	west := configPackStatusIn(t, ts, "eu-west-1", map[string]any{
		"ConformancePackNames": []string{"ops"},
	})
	require.Len(t, west, 1)
	require.Contains(t, west[0]["ConformancePackArn"], ":eu-west-1:")
	require.NotEqual(t, east["ConformancePackId"], west[0]["ConformancePackId"],
		"the minted ID is derived from the Region, so the two packs are distinct")

	// Seed keys carry the Region too, so a seed scoped to one Region must not report its
	// verdict as the other's. (An unscoped seed is the "*"/"*" wildcard and would reach
	// both by design, which is what the wildcard test asserts.)
	configSeed(t, ts, "/v1/config/pack-compliance/ops", map[string]any{
		"accountId": "123456789012",
		"region":    "us-east-1",
		"rules": []map[string]any{
			{"configRuleName": "s3-encrypted", "complianceType": "NON_COMPLIANT"},
		},
	})
	require.Len(t, configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"}), 1,
		"the seed reaches the Region it names")
	resp = configRequestIn(t, ts, "eu-west-1", "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "ops",
	})
	var westCompliance struct {
		ConformancePackRuleComplianceList []map[string]any `json:"ConformancePackRuleComplianceList"`
	}
	status, code, message = decodeConfigResponse(t, resp, &westCompliance)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Empty(t, westCompliance.ConformancePackRuleComplianceList,
		"a seed written for us-east-1 does not reach eu-west-1")
}

func TestConformancePacks_AnUnsupportedPackOperationIsRefusedByName(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// Config has 97 operations and substrate implements the detective-controls subset, so
	// the organization-level pack operations are not claimed. Naming the operation is
	// what lets a consumer discover which call is missing rather than reading a bare
	// refusal. Config is JSON-RPC, so the code and status are the
	// UnknownOperationException/404 AWS publishes for that protocol (#716).
	resp := configRequest(t, ts, "PutOrganizationConformancePack", map[string]any{})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "UnknownOperationException", code)
	require.Contains(t, message, "PutOrganizationConformancePack")
}

// sliceOfN builds n values with a numbered prefix, for the bound cases.
func sliceOfN(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, fmt.Sprintf("%s%02d", prefix, i))
	}
	return out
}

// mapKeys returns a map's keys, for asserting a response carries exactly one member.
func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
