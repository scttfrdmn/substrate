package emulator_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnDescribedChange is one Changes member of a DescribeChangeSet response, decoded with every
// ResourceChange member #1446 added.
type cfnDescribedChange struct {
	Action             string   `xml:"ResourceChange>Action"`
	LogicalResourceID  string   `xml:"ResourceChange>LogicalResourceId"`
	PhysicalResourceID string   `xml:"ResourceChange>PhysicalResourceId"`
	ResourceType       string   `xml:"ResourceChange>ResourceType"`
	Replacement        string   `xml:"ResourceChange>Replacement"`
	Scope              []string `xml:"ResourceChange>Scope>member"`
	Details            []struct {
		Attribute          string `xml:"Target>Attribute"`
		Name               string `xml:"Target>Name"`
		RequiresRecreation string `xml:"Target>RequiresRecreation"`
		Evaluation         string `xml:"Evaluation"`
		ChangeSource       string `xml:"ChangeSource"`
		CausingEntity      string `xml:"CausingEntity"`
	} `xml:"ResourceChange>Details>member"`
}

// cfnChangeSetChanges creates a stack from oldTmpl, a change set to newTmpl with params, and
// returns the change set's changes by logical ID.
func cfnChangeSetChanges(t *testing.T, oldTmpl, newTmpl string, params map[string]string) map[string]cfnDescribedChange {
	t.Helper()
	ts := newCFNTestServer(t)
	cfnWire(t, ts, "CreateStack", map[string]string{"StackName": "cs-detail", "TemplateBody": oldTmpl})
	req := map[string]string{"StackName": "cs-detail", "ChangeSetName": "detail", "ChangeSetType": "UPDATE"}
	if newTmpl != "" {
		req["TemplateBody"] = newTmpl
	}
	for k, v := range params {
		req[k] = v
	}
	cfnWire(t, ts, "CreateChangeSet", req)
	body := cfnWire(t, ts, "DescribeChangeSet", map[string]string{"StackName": "cs-detail", "ChangeSetName": "detail"})
	var doc struct {
		Changes []cfnDescribedChange `xml:"DescribeChangeSetResult>Changes>member"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "%s", body)
	out := make(map[string]cfnDescribedChange, len(doc.Changes))
	for _, c := range doc.Changes {
		out[c.LogicalResourceID+"/"+c.Action] = c
	}
	return out
}

const (
	cfnDetailBase = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cs-detail-bucket"}},` +
		`"Runner":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"cs-detail-role",` +
		`"Description":{"Fn::GetAtt":["Data","Arn"]},` +
		`"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`
)

// TestCFNChangeSet_DeletionPolicyOnlyChangeIsAStaticModify is the case #1446 names: a template
// whose only difference is DeletionPolicy and UpdateReplacePolicy answers a Modify with two Static
// details, and nothing else: a dependent that Fn::GetAtts the modified resource is not a change,
// because a policy is not an attribute. Before #1446 the diff compared only Type and Properties, so the change
// set was empty.
func TestCFNChangeSet_DeletionPolicyOnlyChangeIsAStaticModify(t *testing.T) {
	retained := strings.Replace(cfnDetailBase,
		`"Data":{"Type":"AWS::S3::Bucket",`,
		`"Data":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain","UpdateReplacePolicy":"Retain",`, 1)
	changes := cfnChangeSetChanges(t, cfnDetailBase, retained, nil)

	data, ok := changes["Data/Modify"]
	require.True(t, ok, "%v", changes)
	assert.Equal(t, "cs-detail-bucket", data.PhysicalResourceID, "a Modify names the deployed resource")
	assert.Equal(t, "False", data.Replacement)
	assert.Equal(t, []string{"DeletionPolicy", "UpdateReplacePolicy"}, data.Scope)
	require.Len(t, data.Details, 2)
	for i, attr := range []string{"DeletionPolicy", "UpdateReplacePolicy"} {
		d := data.Details[i]
		assert.Equal(t, attr, d.Attribute)
		assert.Empty(t, d.Name, "Name is given only for a Properties target")
		assert.Equal(t, "Never", d.RequiresRecreation)
		assert.Equal(t, "Static", d.Evaluation)
		assert.Equal(t, "DirectModification", d.ChangeSource)
		assert.Empty(t, d.CausingEntity, "no CausingEntity for a DirectModification")
	}

	// A policy change touches nothing Fn::GetAtt can read, so the dependent is not a change at all:
	// a retain-only change set names exactly the resources whose policies changed.
	_, leaked := changes["Runner/Modify"]
	assert.False(t, leaked, "a DeletionPolicy-only change must not propagate to a GetAtt dependent: %v", changes)
	assert.Len(t, changes, 1)
}

// TestCFNChangeSet_PropertyChangePropagatesThroughGetAtt pins the propagation that does happen: a
// Properties or Tags change on a resource makes a dependent's Fn::GetAtt of it a Dynamic
// ResourceAttribute detail, and a Metadata change does not.
func TestCFNChangeSet_PropertyChangePropagatesThroughGetAtt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		properties string
		propagates bool
	}{
		{"Tags", `"Properties":{"BucketName":"cs-detail-bucket","Tags":[{"Key":"k","Value":"v"}]}`, true},
		{"Metadata", `"Metadata":{"Note":"changed"},"Properties":{"BucketName":"cs-detail-bucket"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := strings.Replace(cfnDetailBase, `"Properties":{"BucketName":"cs-detail-bucket"}`, tc.properties, 1)
			changes := cfnChangeSetChanges(t, cfnDetailBase, next, nil)
			require.Contains(t, changes, "Data/Modify")
			runner, ok := changes["Runner/Modify"]
			require.Equal(t, tc.propagates, ok, "%v", changes)
			if !ok {
				return
			}
			assert.Equal(t, "False", runner.Replacement)
			assert.Equal(t, []string{"Properties"}, runner.Scope)
			require.Len(t, runner.Details, 1)
			assert.Equal(t, "Description", runner.Details[0].Name)
			assert.Equal(t, "Dynamic", runner.Details[0].Evaluation)
			assert.Equal(t, "ResourceAttribute", runner.Details[0].ChangeSource)
			assert.Equal(t, "Data.Arn", runner.Details[0].CausingEntity)
		})
	}
}

// TestCFNChangeSet_JSONAndYAMLNumbersAreEqual pins that a number is the same number whichever
// syntax decoded it: encoding/json yields float64 and the YAML decoder int, and comparing the two
// raw reported a change for an identical template.
func TestCFNChangeSet_JSONAndYAMLNumbersAreEqual(t *testing.T) {
	const asJSON = `{"Resources":{"Runner":{"Type":"AWS::IAM::Role",` +
		`"Metadata":{"Retries":3,"Ratio":0.5},` +
		`"CreationPolicy":{"ResourceSignal":{"Count":1}},` +
		`"Properties":{"RoleName":"cs-yaml-role","MaxSessionDuration":3600,` +
		`"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`
	const asYAML = "Resources:\n" +
		"  Runner:\n" +
		"    Type: AWS::IAM::Role\n" +
		"    Metadata:\n      Retries: 3\n      Ratio: 0.5\n" +
		"    CreationPolicy:\n      ResourceSignal:\n        Count: 1\n" +
		"    Properties:\n" +
		"      RoleName: cs-yaml-role\n" +
		"      MaxSessionDuration: 3600\n" +
		"      AssumeRolePolicyDocument:\n        Version: '2012-10-17'\n        Statement: []\n"
	assert.Empty(t, cfnChangeSetChanges(t, asJSON, asYAML, nil), "the same template in the other syntax changes nothing")

	changed := strings.Replace(asYAML, "MaxSessionDuration: 3600", "MaxSessionDuration: 7200", 1)
	got := cfnChangeSetChanges(t, asJSON, changed, nil)
	require.Contains(t, got, "Runner/Modify", "a real numeric change is still reported")
	require.Len(t, got["Runner/Modify"].Details, 1)
	assert.Equal(t, "MaxSessionDuration", got["Runner/Modify"].Details[0].Name)
}

// TestCFNChangeSet_ReplacementFollowsRequiresRecreation pins the Replacement derivation: a
// property whose reference page says "Update requires: Replacement" is Always, a Static Always is
// True, and a Ref to the replaced resource is a Dynamic ResourceReference on the dependent.
func TestCFNChangeSet_ReplacementFollowsRequiresRecreation(t *testing.T) {
	renamed := strings.Replace(cfnDetailBase, `"cs-detail-bucket"`, `"cs-detail-bucket-2"`, 1)
	renamed = strings.Replace(renamed, `"Description":{"Fn::GetAtt":["Data","Arn"]}`, `"Description":{"Ref":"Data"}`, 1)
	base := strings.Replace(cfnDetailBase, `"Description":{"Fn::GetAtt":["Data","Arn"]}`, `"Description":{"Ref":"Data"}`, 1)
	changes := cfnChangeSetChanges(t, base, renamed, nil)

	data := changes["Data/Modify"]
	assert.Equal(t, "True", data.Replacement, "BucketName is Update requires: Replacement")
	require.Len(t, data.Details, 1)
	assert.Equal(t, "Properties", data.Details[0].Attribute)
	assert.Equal(t, "BucketName", data.Details[0].Name)
	assert.Equal(t, "Always", data.Details[0].RequiresRecreation)

	runner, ok := changes["Runner/Modify"]
	require.True(t, ok, "%v", changes)
	require.Len(t, runner.Details, 1)
	assert.Equal(t, "ResourceReference", runner.Details[0].ChangeSource)
	assert.Equal(t, "Data", runner.Details[0].CausingEntity)
	assert.Equal(t, "Dynamic", runner.Details[0].Evaluation)
	assert.Equal(t, "False", runner.Replacement, "Description is not a replacement property")
}

// TestCFNChangeSet_ParameterChangeIsTwoDetails pins the published pairing: a property that Refs a
// changed parameter is a Dynamic DirectModification and a Static ParameterReference naming it.
func TestCFNChangeSet_ParameterChangeIsTwoDetails(t *testing.T) {
	const tmpl = `{"Parameters":{"Suffix":{"Type":"String","Default":"one"}},"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":{"Fn::Sub":"cs-param-${Suffix}"},` +
		`"Tags":[{"Key":"suffix","Value":{"Ref":"Suffix"}}]}}}}`
	changes := cfnChangeSetChanges(t, tmpl, "", map[string]string{
		"Parameters.member.1.ParameterKey":   "Suffix",
		"Parameters.member.1.ParameterValue": "two",
	})

	data, ok := changes["Data/Modify"]
	require.True(t, ok, "a parameter change with an unchanged template is still a Modify: %v", changes)
	assert.Equal(t, "True", data.Replacement, "the Static ParameterReference on BucketName is Always")
	assert.Equal(t, []string{"Properties", "Tags"}, data.Scope)

	type key struct{ attr, name, eval, source, entity string }
	got := map[key]string{}
	for _, d := range data.Details {
		got[key{d.Attribute, d.Name, d.Evaluation, d.ChangeSource, d.CausingEntity}] = d.RequiresRecreation
	}
	assert.Equal(t, map[key]string{
		{"Properties", "BucketName", "Dynamic", "DirectModification", ""}:      "Always",
		{"Properties", "BucketName", "Static", "ParameterReference", "Suffix"}: "Always",
		{"Tags", "", "Dynamic", "DirectModification", ""}:                      "Never",
		{"Tags", "", "Static", "ParameterReference", "Suffix"}:                 "Never",
	}, got)
	assert.Equal(t, "Dynamic", data.Details[0].Evaluation, "Dynamic details are listed first, as the published samples are")
}

// TestCFNChangeSet_AddAndRemoveCarryNoDetail pins that Scope and Details "apply only to modified
// resources", and that only a Remove names a physical ID.
func TestCFNChangeSet_AddAndRemoveCarryNoDetail(t *testing.T) {
	const next = `{"Resources":{"Other":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cs-other"}}}}`
	changes := cfnChangeSetChanges(t, cfnBucketTemplate, next, nil)

	added, removed := changes["Other/Add"], changes["Data/Remove"]
	assert.Empty(t, added.PhysicalResourceID, "an added resource has no physical ID yet")
	assert.Equal(t, "cfn-wire-bucket", removed.PhysicalResourceID)
	for _, c := range []cfnDescribedChange{added, removed} {
		assert.Empty(t, c.Scope)
		assert.Empty(t, c.Details)
		assert.Empty(t, c.Replacement)
	}
}

// TestCFNPlugin_ListStackResources is #1446's first item: the paginated summary form of
// DescribeStackResources, answered from the same derivation.
func TestCFNPlugin_ListStackResources(t *testing.T) {
	ts := newCFNTestServer(t)

	var resources []string
	for i := range 105 {
		resources = append(resources, fmt.Sprintf(`"B%03d":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"lsr-%03d"}}`, i, i))
	}
	tmpl := `{"Resources":{` + strings.Join(resources, ",") + `}}`
	cfnWire(t, ts, "CreateStack", map[string]string{"StackName": "listed", "TemplateBody": tmpl})

	type summary struct {
		LogicalResourceID    string `xml:"LogicalResourceId"`
		PhysicalResourceID   string `xml:"PhysicalResourceId"`
		ResourceType         string `xml:"ResourceType"`
		ResourceStatus       string `xml:"ResourceStatus"`
		LastUpdatedTimestamp string `xml:"LastUpdatedTimestamp"`
	}
	page := func(token string) ([]summary, string) {
		params := map[string]string{"StackName": "listed"}
		if token != "" {
			params["NextToken"] = token
		}
		body := cfnWire(t, ts, "ListStackResources", params)
		var doc struct {
			Summaries []summary `xml:"ListStackResourcesResult>StackResourceSummaries>member"`
			NextToken string    `xml:"ListStackResourcesResult>NextToken"`
		}
		require.NoError(t, xml.Unmarshal(body, &doc), "%s", body)
		return doc.Summaries, doc.NextToken
	}

	first, token := page("")
	require.Len(t, first, 100)
	require.NotEmpty(t, token, "a stack over one page answers a NextToken")
	second, last := page(token)
	require.Len(t, second, 5)
	assert.Empty(t, last)

	seen := map[string]bool{}
	for _, s := range append(first, second...) {
		assert.False(t, seen[s.LogicalResourceID], "no resource is listed twice")
		seen[s.LogicalResourceID] = true
		assert.Equal(t, "AWS::S3::Bucket", s.ResourceType)
		assert.Equal(t, "CREATE_COMPLETE", s.ResourceStatus)
		assert.NotEmpty(t, s.LastUpdatedTimestamp)
	}
	assert.Len(t, seen, 105)

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"missing StackName", map[string]string{}, "MissingParameter"},
		{"unknown stack", map[string]string{"StackName": "absent"}, "ValidationError"},
		{"unissued token", map[string]string{"StackName": "listed", "NextToken": "not-a-token"}, "ValidationError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := cfnAction(t, ts, "ListStackResources", tc.params)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, tc.code, cfnErrorCode(t, body))
		})
	}
}

// TestCFNPlugin_GetTemplateStage is #1446's third item: TemplateStage is validated, Processed is
// Original when the template declares no Transform, and a template that declares one does not
// offer a Processed stage substrate cannot produce.
func TestCFNPlugin_GetTemplateStage(t *testing.T) {
	ts := newCFNTestServer(t)
	cfnWire(t, ts, "CreateStack", map[string]string{"StackName": "plain", "TemplateBody": cfnBucketTemplate})
	const transformed = `{"Transform":"AWS::Serverless-2016-10-31","Resources":{"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-sam-bucket"}}}}`
	cfnWire(t, ts, "CreateStack", map[string]string{"StackName": "sam", "TemplateBody": transformed})

	read := func(stack, stage string) (int, string, []string) {
		params := map[string]string{"StackName": stack}
		if stage != "" {
			params["TemplateStage"] = stage
		}
		status, body := cfnAction(t, ts, "GetTemplate", params)
		var doc struct {
			TemplateBody string   `xml:"GetTemplateResult>TemplateBody"`
			Stages       []string `xml:"GetTemplateResult>StagesAvailable>member"`
		}
		if status == http.StatusOK {
			require.NoError(t, xml.Unmarshal([]byte(body), &doc))
			return status, doc.TemplateBody, doc.Stages
		}
		return status, body, nil
	}

	for _, stage := range []string{"", "Original", "Processed"} {
		status, body, stages := read("plain", stage)
		require.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, cfnBucketTemplate, body, "stage %q", stage)
		assert.Equal(t, []string{"Original", "Processed"}, stages)
	}

	status, body, stages := read("sam", "")
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, transformed, body)
	assert.Equal(t, []string{"Original"}, stages, "no Processed stage is offered for an unexpanded transform")
	status, body, _ = read("sam", "Original")
	require.Equal(t, http.StatusOK, status, body)

	status, body, _ = read("sam", "Processed")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	status, body, _ = read("plain", "Expanded")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body), "a stage outside Original | Processed is refused")
}

// TestCFNPlugin_DetectStackDriftHonorsLogicalResourceIds is #1446's fourth item: the filter
// restricts what is checked, so a resource outside it cannot drift the stack, and a name the stack
// does not declare is refused.
func TestCFNPlugin_DetectStackDriftHonorsLogicalResourceIds(t *testing.T) {
	ts := newCFNTestServer(t)
	const tmpl = `{"Resources":{` +
		`"Kept":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"drift-kept"}},` +
		`"Gone":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"drift-gone"}}}}`
	cfnWire(t, ts, "CreateStack", map[string]string{"StackName": "drifty", "TemplateBody": tmpl})
	// Deleted out of band, which is what drift detection exists to notice.
	require.NoError(t, ts.state.Delete(t.Context(), "s3", "bucket:drift-gone"))

	status := func(params map[string]string) (string, int) {
		t.Helper()
		body := cfnWire(t, ts, "DetectStackDrift", params)
		var id struct {
			ID string `xml:"DetectStackDriftResult>StackDriftDetectionId"`
		}
		require.NoError(t, xml.Unmarshal(body, &id))
		got := cfnWire(t, ts, "DescribeStackDriftDetectionStatus", map[string]string{"StackDriftDetectionId": id.ID})
		var doc struct {
			Status string `xml:"DescribeStackDriftDetectionStatusResult>StackDriftStatus"`
			Count  int    `xml:"DescribeStackDriftDetectionStatusResult>DriftedStackResourceCount"`
		}
		require.NoError(t, xml.Unmarshal(got, &doc))
		return doc.Status, doc.Count
	}

	s, n := status(map[string]string{"StackName": "drifty"})
	assert.Equal(t, "DRIFTED", s)
	assert.Equal(t, 1, n)

	s, n = status(map[string]string{"StackName": "drifty", "LogicalResourceIds.member.1": "Kept"})
	assert.Equal(t, "IN_SYNC", s, "the deleted resource is outside the filter")
	assert.Equal(t, 0, n)

	s, n = status(map[string]string{"StackName": "drifty", "LogicalResourceIds.member.1": "Gone"})
	assert.Equal(t, "DRIFTED", s)
	assert.Equal(t, 1, n)

	code, body := cfnAction(t, ts, "DetectStackDrift", map[string]string{"StackName": "drifty", "LogicalResourceIds.member.1": "Nope"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	tooMany := map[string]string{"StackName": "drifty"}
	for i := 1; i <= 201; i++ {
		tooMany[fmt.Sprintf("LogicalResourceIds.member.%d", i)] = "Kept"
	}
	code, body = cfnAction(t, ts, "DetectStackDrift", tooMany)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
}
