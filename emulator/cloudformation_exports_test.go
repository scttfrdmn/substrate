package emulator_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The exporting/importing pair as a wire caller sends it. Kept separate from the
// in-process constants because the wire tests use the S3 and IAM plugins the
// wire server registers, not the deployer suite's wider set.
const (
	cfnWireExportTemplate = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-wire-export"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"},"Description":"the bucket",` +
		`"Export":{"Name":{"Fn::Sub":"${AWS::StackName}-BucketName"}}}}}`

	cfnWireImportTemplate = `{"Resources":{"Copy":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":{"Fn::Sub":["${n}-copy",` +
		`{"n":{"Fn::ImportValue":"wirenet-BucketName"}}]}}}}}`
)

// cfnExportMembers unmarshals a ListExports response.
type cfnExportMembers struct {
	Exports []struct {
		Name             string `xml:"Name"`
		Value            string `xml:"Value"`
		ExportingStackID string `xml:"ExportingStackId"`
	} `xml:"ListExportsResult>Exports>member"`
}

// TestCFNPlugin_ListExports asserts the wire shape of the export registry.
//
// The member names are the API's, not substrate's: a caller's SDK unmarshals by
// them, so Name/Value/ExportingStackId being right is the whole observable. The
// ARN is asserted against the same builder DescribeStacks uses, because an export
// that named a different stack ARN than the stack itself would be unusable for
// the "which stack do I delete" question ListExports exists to answer.
func TestCFNPlugin_ListExports(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "wirenet", "TemplateBody": cfnWireExportTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "ListExports", nil)
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc cfnExportMembers
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	require.Len(t, doc.Exports, 1)
	assert.Equal(t, "wirenet-BucketName", doc.Exports[0].Name)
	assert.Equal(t, "cfn-wire-export", doc.Exports[0].Value)

	// The ARN has to be the one DescribeStacks reports for the same stack, or the
	// "which stack do I delete" question ListExports exists to answer gets an
	// identifier no other call recognizes.
	code, describeBody := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wirenet"})
	require.Equal(t, http.StatusOK, code, "body was %s", describeBody)
	var describe struct {
		StackID string `xml:"DescribeStacksResult>Stacks>member>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(describeBody), &describe), "body was %s", describeBody)
	require.Contains(t, describe.StackID, ":stack/wirenet/")
	assert.Equal(t, describe.StackID, doc.Exports[0].ExportingStackID,
		"the exporting stack is identified by its ARN, not its name")
}

// TestCFNPlugin_ListImports asserts the read the API points a caller at when a
// delete is refused, including the two edges the reference fixes: ExportName is
// required, and an export nothing imports is an empty list rather than an error.
func TestCFNPlugin_ListImports(t *testing.T) {
	ts := newCFNTestServer(t)

	// Ordered, not a map: the exporting stack has to exist before the importing one
	// resolves against it, which is CloudFormation's own constraint and not an
	// artifact of the test.
	for _, s := range []struct{ name, tmpl string }{
		{"wirenet", cfnWireExportTemplate},
		{"wireapp", cfnWireImportTemplate},
	} {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{
			"StackName": s.name, "TemplateBody": s.tmpl,
		})
		require.Equal(t, http.StatusOK, code, "creating %s: body was %s", s.name, body)
	}

	var doc struct {
		Imports []string `xml:"ListImportsResult>Imports>member"`
	}

	code, body := cfnAction(t, ts, "ListImports", map[string]string{
		"ExportName": "wirenet-BucketName",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	assert.Equal(t, []string{"wireapp"}, doc.Imports,
		"stack names, not ARNs: \"a list of stack names that are importing the "+
			"specified exported output value\"")

	code, body = cfnAction(t, ts, "ListImports", map[string]string{
		"ExportName": "no-such-export",
	})
	assert.Equal(t, http.StatusOK, code,
		"ListImports documents no service-specific error, so an unknown export is empty")
	doc.Imports = nil
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	assert.Empty(t, doc.Imports)

	code, body = cfnAction(t, ts, "ListImports", nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "MissingParameter", cfnErrorCode(t, body))
}

// TestCFNPlugin_DeleteStackRefusedWhileExportIsImported is the wire half of
// #522's second gate: the refusal has to reach the caller as a documented error
// code, not as a 500 or a silent success.
//
// DeleteStack documents only TokenAlreadyExists, so ValidationError at 400 is
// substrate's own choice for this case — the same code the API uses for every
// other "your request cannot be satisfied" refusal on this service.
func TestCFNPlugin_DeleteStackRefusedWhileExportIsImported(t *testing.T) {
	ts := newCFNTestServer(t)

	for _, s := range []struct{ name, tmpl string }{
		{"wirenet", cfnWireExportTemplate},
		{"wireapp", cfnWireImportTemplate},
	} {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{
			"StackName": s.name, "TemplateBody": s.tmpl,
		})
		require.Equal(t, http.StatusOK, code, "creating %s: body was %s", s.name, body)
	}

	code, body := cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "wirenet"})
	assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Contains(t, body, "wirenet-BucketName")
	assert.Contains(t, body, "wireapp", "the caller is told which stack to remove first")

	// The stack survived the refusal, and deleting the importer first releases it.
	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wirenet"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "wireapp"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "wirenet"})
	assert.Equal(t, http.StatusOK, code, "body was %s", body)
}

// TestCFNPlugin_CreateStackRefusesADuplicateExportName pins the uniqueness rule
// over the wire, since a template that would be rejected by the real API must be
// rejected here — otherwise substrate green-lights a deployment that cannot ship.
func TestCFNPlugin_CreateStackRefusesADuplicateExportName(t *testing.T) {
	ts := newCFNTestServer(t)

	const fixed = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-wire-fixed"}}},` +
		`"Outputs":{"V":{"Value":{"Ref":"Data"},"Export":{"Name":"Wire-Shared"}}}}`

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "owner", "TemplateBody": fixed,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "claimant", "TemplateBody": fixed,
	})
	assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Contains(t, body, "Wire-Shared")
	assert.Contains(t, body, "owner", "the conflict names the stack already holding it")
}

// TestCFNPlugin_DescribeStacksReportsExportName pins ExportName beside its output.
//
// The API reports it on the Output structure itself, which saves a caller a second
// ListExports call to learn whether an output is importable — and an output with no
// Export must not grow an empty element, since that would read as "exported under
// the empty name".
func TestCFNPlugin_DescribeStacksReportsExportName(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "wirenet", "TemplateBody": cfnWireExportTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wirenet"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Outputs []struct {
			OutputKey   string `xml:"OutputKey"`
			OutputValue string `xml:"OutputValue"`
			ExportName  string `xml:"ExportName"`
		} `xml:"DescribeStacksResult>Stacks>member>Outputs>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	require.Len(t, doc.Outputs, 1)
	assert.Equal(t, "BucketName", doc.Outputs[0].OutputKey)
	assert.Equal(t, "cfn-wire-export", doc.Outputs[0].OutputValue)
	assert.Equal(t, "wirenet-BucketName", doc.Outputs[0].ExportName)

	// A local output carries no ExportName element at all.
	code, body = cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "wirelocal", "TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "wirelocal"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "<ExportName>",
		"an output without an Export is not importable and says nothing about one")
}

// TestCFNPlugin_ExportsAreScopedToTheCallersPartition pins that the wire path
// consults the caller's identity rather than the plugin's default.
//
// deleteStack in particular had to switch to the caller's deployer for this: the
// stack record is unpartitioned, but the export visibility that decides the refusal
// is not, so the default identity would have consulted the wrong namespace and
// either refused a legal delete or allowed an illegal one.
func TestCFNPlugin_ExportsAreScopedToTheCallersPartition(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "wirenet", "TemplateBody": cfnWireExportTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// A request signed as another account is out of the export's scope, so the export
	// is invisible to it.
	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
		"Action": "ListExports", "Version": "2010-05-15",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var doc cfnExportMembers
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	assert.Empty(t, doc.Exports, "another account must not see the export")

	// And an import from that account fails the resource rather than resolving
	// against an export it is not entitled to see.
	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
		"Action": "CreateStack", "Version": "2010-05-15",
		"StackName": "otheracct", "TemplateBody": cfnWireImportTemplate,
	})
	require.Equal(t, http.StatusOK, code, "the resource fails; the operation does not: %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
		"Action": "DescribeStackResources", "Version": "2010-05-15",
		"StackName": "otheracct",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "CREATE_FAILED")
	assert.Contains(t, body, "no exported output named")

	// The refusal to delete is scoped the same way: the other account's stack
	// imports nothing this account can see, so the exporting stack in the default
	// account is still deletable.
	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "wirenet"})
	assert.Equal(t, http.StatusOK, code,
		"an out-of-scope import must not pin the exporting stack: %s", body)
}

// TestCFNPlugin_DeleteRefusalUsesTheCallersOwnNamespace pins the reason
// deleteStack builds a deployer from the caller's identity rather than using the
// plugin's default one.
//
// The stack record is unpartitioned, so the delete itself works either way. The
// export visibility deciding the refusal is not, so the default identity would
// consult the wrong namespace — and the direction that matters is this one: a
// non-default caller's own import is invisible from the default namespace, so a
// delete that must be refused would be allowed, quietly removing an export another
// of that caller's stacks is holding. A caller in the default account cannot catch
// it, because its identity *is* the default — which is why both stacks here are
// created by a caller signing as another account.
func TestCFNPlugin_DeleteRefusalUsesTheCallersOwnNamespace(t *testing.T) {
	ts := newCFNTestServer(t)

	// Both stacks signed as cfnOtherAccount, so both live outside the default account
	// the plugin's own deployer would consult.
	for _, s := range []struct{ name, tmpl string }{
		{"wirenet", cfnWireExportTemplate},
		{"wireapp", cfnWireImportTemplate},
	} {
		code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
			"Action": "CreateStack", "Version": "2010-05-15",
			"StackName": s.name, "TemplateBody": s.tmpl,
		})
		require.Equal(t, http.StatusOK, code, "creating %s: body was %s", s.name, body)
	}

	// The import resolved, which is what makes the refusal below meaningful: an
	// unresolved import would leave nothing recorded and the delete would be legal.
	code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
		"Action": "DescribeStackResources", "Version": "2010-05-15",
		"StackName": "wireapp",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.Contains(t, body, "CREATE_COMPLETE")
	require.NotContains(t, body, "CREATE_FAILED")

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", cfnOtherAccount, map[string]string{
		"Action": "DeleteStack", "Version": "2010-05-15", "StackName": "wirenet",
	})
	assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Contains(t, body, "wireapp",
		"the caller's own import must be seen, not the default account's")
}
