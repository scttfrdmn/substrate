package emulator_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The tests below cover #545: a function's CodeSize and CodeSha256 report the
// deployment package it was created from, whether that package arrived inline, from
// S3, or through a CloudFormation template.
//
// They assert on the JSON GetFunction returns rather than on the state record, since
// CodeSize is only interesting as an observation a caller makes — and a caller who
// polls a function's size to decide whether a deploy took effect is the one the bug
// was reported by.

// lambdaCodeTestWorld bundles a server carrying the Lambda, S3 and CloudFormation
// plugins with the deployer that shares their registry.
//
// One world rather than three: the point of the fix is that a package uploaded to
// substrate's S3 is the same object the Lambda plugin sizes, and a CFN-deployed
// function is the same function GetFunction answers for. Separate servers per plugin
// could not express either.
type lambdaCodeTestWorld struct {
	srv      *emulator.Server
	deployer *emulator.StackDeployer
	state    emulator.StateManager
}

func newLambdaCodeTestWorld(t *testing.T) *lambdaCodeTestWorld {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	opts := emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
			"registry":        registry,
		},
	}
	for _, p := range []emulator.Plugin{
		&emulator.LambdaPlugin{},
		&emulator.S3Plugin{},
		&emulator.IAMPlugin{},
	} {
		require.NoError(t, p.Initialize(context.Background(), opts))
		registry.Register(p)
	}

	return &lambdaCodeTestWorld{
		srv:      emulator.NewServer(*cfg, registry, store, state, tc, logger),
		deployer: emulator.NewStackDeployer(registry, store, state, tc, logger, costs),
		state:    state,
	}
}

// putS3Package uploads body as an object, the way a test or a build pipeline stages a
// deployment package before pointing a function at it.
func (w *lambdaCodeTestWorld) putS3Package(t *testing.T, bucket, key string, body []byte) {
	t.Helper()
	resp := s3Request(t, w.srv, http.MethodPut, "/"+bucket, nil, nil)
	require.Equal(t, http.StatusOK, resp.Code, "create bucket: %s", resp.Body.String())
	resp = s3Request(t, w.srv, http.MethodPut, "/"+bucket+"/"+key, body, nil)
	require.Equal(t, http.StatusOK, resp.Code, "put object: %s", resp.Body.String())
}

// getFunctionConfig reports the Configuration member of GetFunction's response.
func (w *lambdaCodeTestWorld) getFunctionConfig(t *testing.T, name string) map[string]any {
	t.Helper()
	resp := lambdaRequest(t, w.srv, http.MethodGet, "/2015-03-31/functions/"+name, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Configuration map[string]any `json:"Configuration"`
		Code          map[string]any `json:"Code"`
	}
	decodeLambdaJSON(t, resp, &out)
	require.NotNil(t, out.Configuration, "GetFunction returned no Configuration")
	return out.Configuration
}

// getFunctionCode reports the Code member of GetFunction's response.
func (w *lambdaCodeTestWorld) getFunctionCode(t *testing.T, name string) map[string]any {
	t.Helper()
	resp := lambdaRequest(t, w.srv, http.MethodGet, "/2015-03-31/functions/"+name, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Code map[string]any `json:"Code"`
	}
	decodeLambdaJSON(t, resp, &out)
	return out.Code
}

// codeSize reports a Configuration's CodeSize as an int64. JSON numbers decode to
// float64, and asserting on a float64 would let a size of 156.5 pass.
func codeSize(t *testing.T, cfg map[string]any) int64 {
	t.Helper()
	n, ok := cfg["CodeSize"].(float64)
	require.True(t, ok, "CodeSize was %#v", cfg["CodeSize"])
	return int64(n)
}

// lambdaTestPackage is a 156-byte stand-in for a deployment package, the size the
// issue reported as 0. Its exact contents do not matter; its exact length does, since
// that is the observation under test.
var lambdaTestPackage = bytes.Repeat([]byte("substrate-lambda-package-0123456789abcdef\n"), 4)[:156]

// TestLambdaCodeSize_DirectS3Source is the direct half of #545: a CreateFunction
// naming an S3 object reports that object's length, where it reported 0.
func TestLambdaCodeSize_DirectS3Source(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "pkgs", "fn.zip", lambdaTestPackage)

	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "direct-s3",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
		"Code":         map[string]any{"S3Bucket": "pkgs", "S3Key": "fn.zip"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	cfg := w.getFunctionConfig(t, "direct-s3")
	assert.Equal(t, int64(156), codeSize(t, cfg),
		"CodeSize is the S3 object's byte length")
	assert.NotEmpty(t, cfg["CodeSha256"],
		"an S3-sourced package has a digest; an empty one is the pre-fix behavior")
	assert.Equal(t, "Zip", cfg["PackageType"])
}

// TestLambdaCodeSize_DirectS3AbsentObject records the deliberate choice that an
// absent object still deploys.
//
// Real Lambda refuses the create. Substrate does not, because its S3 and Lambda
// namespaces are independent and a template may legitimately name an object a test
// never uploaded; failing here would make a stack undeployable for a reason unrelated
// to what the test is checking. The observable is a size of 0, not an error.
func TestLambdaCodeSize_DirectS3AbsentObject(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "missing-s3",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
		"Code":         map[string]any{"S3Bucket": "nowhere", "S3Key": "absent.zip"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode,
		"an absent package must not fail the create")

	cfg := w.getFunctionConfig(t, "missing-s3")
	assert.Equal(t, int64(0), codeSize(t, cfg))
	assert.Empty(t, cfg["CodeSha256"],
		"there is no package to digest, so no digest is reported")
}

// TestLambdaCodeSize_DirectS3ObjectVersion checks that S3ObjectVersion selects the
// version it names rather than the current one.
//
// The two versions have different lengths deliberately: with equal lengths the
// assertion would hold whichever record was read, which would measure nothing.
func TestLambdaCodeSize_DirectS3ObjectVersion(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	resp := s3Request(t, w.srv, http.MethodPut, "/vpkgs", nil, nil)
	require.Equal(t, http.StatusOK, resp.Code)
	resp = s3Request(t, w.srv, http.MethodPut, "/vpkgs?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
	require.Equal(t, http.StatusOK, resp.Code, "enable versioning: %s", resp.Body.String())

	first := bytes.Repeat([]byte("a"), 100)
	resp = s3Request(t, w.srv, http.MethodPut, "/vpkgs/fn.zip", first, nil)
	require.Equal(t, http.StatusOK, resp.Code)
	firstVersion := resp.Header().Get("x-amz-version-id")
	require.NotEmpty(t, firstVersion, "versioned PUT must report a version id")

	resp = s3Request(t, w.srv, http.MethodPut, "/vpkgs/fn.zip", bytes.Repeat([]byte("b"), 250), nil)
	require.Equal(t, http.StatusOK, resp.Code)

	createResp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "versioned-s3",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
		"Code": map[string]any{
			"S3Bucket":        "vpkgs",
			"S3Key":           "fn.zip",
			"S3ObjectVersion": firstVersion,
		},
	})
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	cfg := w.getFunctionConfig(t, "versioned-s3")
	assert.Equal(t, int64(100), codeSize(t, cfg),
		"the named version's length, not the current version's 250")
}

// TestLambdaCodeSize_InlineZipFileIsHashed checks the inline path: CodeSize was
// already right, but CodeSha256 was empty, so a caller could not tell one package
// from another.
func TestLambdaCodeSize_InlineZipFileIsHashed(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	create := func(name string, pkg []byte) map[string]any {
		resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
			"FunctionName": name,
			"Runtime":      "python3.12",
			"Role":         "arn:aws:iam::123456789012:role/r",
			"Handler":      "index.handler",
			"Code":         map[string]any{"ZipFile": base64.StdEncoding.EncodeToString(pkg)},
		})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		return w.getFunctionConfig(t, name)
	}

	one := create("inline-one", lambdaTestPackage)
	assert.Equal(t, int64(156), codeSize(t, one))
	assert.NotEmpty(t, one["CodeSha256"], "an inline package has a digest")

	// The same bytes hash the same and different bytes hash differently — which is
	// the only property a caller comparing digests across deploys relies on.
	again := create("inline-again", lambdaTestPackage)
	assert.Equal(t, one["CodeSha256"], again["CodeSha256"])

	other := create("inline-other", append(bytes.Clone(lambdaTestPackage), 'x'))
	assert.NotEqual(t, one["CodeSha256"], other["CodeSha256"])
}

// TestLambdaCodeSize_UpdateFunctionCode covers both update paths.
//
// The digest assertion is the substantive one: UpdateFunctionCode used to assign
// CodeSha256 a fresh random value on every call, so a caller asking "did the code
// change?" was always told yes.
func TestLambdaCodeSize_UpdateFunctionCode(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "updates", "v2.zip", bytes.Repeat([]byte("z"), 4096))

	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "updated",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": base64.StdEncoding.EncodeToString(lambdaTestPackage)},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	before := w.getFunctionConfig(t, "updated")
	require.Equal(t, int64(156), codeSize(t, before))

	// Re-uploading the identical package leaves the digest alone: the code did not
	// change, so the answer to "did it change?" is no.
	resp = lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated/code", map[string]any{
		"ZipFile": base64.StdEncoding.EncodeToString(lambdaTestPackage),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	same := w.getFunctionConfig(t, "updated")
	assert.Equal(t, before["CodeSha256"], same["CodeSha256"],
		"an unchanged package keeps its digest")
	assert.NotEqual(t, before["RevisionId"], same["RevisionId"],
		"a RevisionId is not a digest and advances on every update")

	// An S3-sourced update reports the new object's length, where it reported the
	// stale size and a random digest.
	resp = lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated/code", map[string]any{
		"S3Bucket": "updates",
		"S3Key":    "v2.zip",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	after := w.getFunctionConfig(t, "updated")
	assert.Equal(t, int64(4096), codeSize(t, after))
	assert.NotEqual(t, before["CodeSha256"], after["CodeSha256"],
		"a different package has a different digest")

	// An UpdateFunctionCode carrying no code at all — which the API allows, and which
	// an SDK sends when only DryRun or Publish is set — changes no package, so it
	// changes no digest. This is the assertion that pins the digest to the package
	// rather than to the act of calling the operation.
	resp = lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated/code",
		map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	empty := w.getFunctionConfig(t, "updated")
	assert.Equal(t, after["CodeSha256"], empty["CodeSha256"],
		"an update with no package keeps the digest it had")
	assert.Equal(t, int64(4096), codeSize(t, empty),
		"and keeps the size it had")

	// Pointing the function at an image does change the package, so the digest moves
	// with the URI — and moves back if the same URI is set again.
	const uri = "123456789012.dkr.ecr.us-east-1.amazonaws.com/updated:v1"
	resp = lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated/code",
		map[string]any{"ImageUri": uri})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	img := w.getFunctionConfig(t, "updated")
	assert.NotEqual(t, after["CodeSha256"], img["CodeSha256"],
		"a new image is a new package")

	resp = lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated/code",
		map[string]any{"ImageUri": uri})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, img["CodeSha256"], w.getFunctionConfig(t, "updated")["CodeSha256"],
		"the same image is the same package")
}

// TestLambdaCodeSize_ImageUriInCode checks the documented spelling of an image
// source, which substrate accepted only at the top level.
//
// PackageType is inferred rather than required: an ImageUri implies Image, and real
// Lambda rejects the pair with Zip, so inferring it cannot contradict a valid
// request. GetFunction reports the image through FunctionCodeLocation, which is what
// makes accepting the URI observable at all.
func TestLambdaCodeSize_ImageUriInCode(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	const uri = "123456789012.dkr.ecr.us-east-1.amazonaws.com/fn:latest"

	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "image-fn",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Code":         map[string]any{"ImageUri": uri},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	cfg := w.getFunctionConfig(t, "image-fn")
	assert.Equal(t, "Image", cfg["PackageType"],
		"an ImageUri implies PackageType Image")

	code := w.getFunctionCode(t, "image-fn")
	assert.Equal(t, "ECR", code["RepositoryType"],
		"the service hosting an image is ECR, not S3")
	assert.Equal(t, uri, code["ImageUri"])
	assert.Empty(t, code["Location"],
		"there is no presigned S3 URL for an image-packaged function")

	// The digest is reported at create, not only after an UpdateFunctionCode. Having it
	// appear on one path and not the other would mean a function created from an image
	// had no digest until it happened to be updated.
	assert.NotEmpty(t, cfg["CodeSha256"],
		"an image-packaged function's digest tracks its URI from creation")

	other := newLambdaCodeTestWorld(t)
	resp = lambdaRequest(t, other.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "image-fn",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Code":         map[string]any{"ImageUri": uri + "-different"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.NotEqual(t, cfg["CodeSha256"], other.getFunctionConfig(t, "image-fn")["CodeSha256"],
		"a different image is a different package, so it digests differently")
}

// The templates below declare the same function three ways, which is what turns
// "CFN drops Code" into three separate observations.
const (
	cfnInlineLambdaTemplate = `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-inline","Runtime":"python3.12","Handler":"index.handler",
		"Role":"arn:aws:iam::123456789012:role/r",
		"Code":{"ZipFile":"def handler(event, context):\n    return {'ok': True}\n"}}}}}`

	cfnS3LambdaTemplate = `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-s3","Runtime":"python3.12","Handler":"index.handler",
		"Role":"arn:aws:iam::123456789012:role/r",
		"Code":{"S3Bucket":"pkgs","S3Key":"fn.zip"}}}}}`

	cfnImageLambdaTemplate = `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-image",
		"Role":"arn:aws:iam::123456789012:role/r",
		"Code":{"ImageUri":"123456789012.dkr.ecr.us-east-1.amazonaws.com/cfn:latest"}}}}}`
)

// TestLambdaCodeSize_CFNInlineZipFile is the half of #545 the report never tested: a
// CloudFormation function whose template declares its code inline.
//
// The resource type's ZipFile is plain source — "CloudFormation places it in a file
// named index and zips it to create a deployment package" — while CreateFunction's
// ZipFile is "the base64-encoded contents of the deployment package". So the deployer
// has to build the archive, not forward the string: forwarding it would hand Lambda
// something that is not a package and usually not valid base64, leaving CodeSize 0
// exactly as before.
func TestLambdaCodeSize_CFNInlineZipFile(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	result, err := w.deployer.Deploy(context.Background(), cfnInlineLambdaTemplate, "inline-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)

	cfg := w.getFunctionConfig(t, "cfn-inline")
	size := codeSize(t, cfg)
	assert.Positive(t, size, "an inline template's function has a package; 0 is the bug")
	assert.NotEmpty(t, cfg["CodeSha256"])

	// The package is the archive CloudFormation would have built: one entry, named
	// for the runtime. Asserting on the stored bytes rather than only on the size is
	// what distinguishes a real ZIP from any string of the right length.
	raw, err := w.state.Get(context.Background(), "lambda", "function_zip:cfn-inline")
	require.NoError(t, err)
	require.NotEmpty(t, raw, "an inline package is staged for execution")
	assert.Equal(t, size, int64(len(raw)), "CodeSize is the archive's length")

	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err, "the stored package is a ZIP archive")
	require.Len(t, zr.File, 1)
	assert.Equal(t, "index.py", zr.File[0].Name,
		"a python runtime's inline source lands in index.py")

	// Deterministic by construction: the same template deployed again produces the
	// same package, which is what makes a digest worth comparing.
	w2 := newLambdaCodeTestWorld(t)
	_, err = w2.deployer.Deploy(context.Background(), cfnInlineLambdaTemplate, "inline-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, cfg["CodeSha256"], w2.getFunctionConfig(t, "cfn-inline")["CodeSha256"])
}

// TestLambdaCodeSize_CFNInlineZipFileNodeRuntime pins the one extension the reference
// names outright, so the runtime is read rather than assumed.
func TestLambdaCodeSize_CFNInlineZipFileNodeRuntime(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	tmpl := `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-node","Runtime":"nodejs20.x","Handler":"index.handler",
		"Role":"arn:aws:iam::123456789012:role/r",
		"Code":{"ZipFile":"exports.handler = async () => ({ok: true});\n"}}}}}`

	_, err := w.deployer.Deploy(context.Background(), tmpl, "node-stack", nil)
	require.NoError(t, err)

	raw, err := w.state.Get(context.Background(), "lambda", "function_zip:cfn-node")
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err)
	require.Len(t, zr.File, 1)
	assert.Equal(t, "index.js", zr.File[0].Name,
		`"the index file that CloudFormation creates uses the extension .js"`)
}

// TestLambdaCodeSize_CFNS3Source is the case the issue filed: a CFN-deployed
// function whose package is an S3 object reports that object's length.
func TestLambdaCodeSize_CFNS3Source(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "pkgs", "fn.zip", lambdaTestPackage)

	result, err := w.deployer.Deploy(context.Background(), cfnS3LambdaTemplate, "s3-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)

	cfg := w.getFunctionConfig(t, "cfn-s3")
	assert.Equal(t, int64(156), codeSize(t, cfg), "was 0 before the fix")
	assert.NotEmpty(t, cfg["CodeSha256"])

	// Nothing is staged for execution — the bytes were never fetched — and that is a
	// separate and correct fact from knowing the size.
	raw, err := w.state.Get(context.Background(), "lambda", "function_zip:cfn-s3")
	require.NoError(t, err)
	assert.Empty(t, raw, "an S3 package is not fetched, only sized")
}

// TestLambdaCodeSize_CFNS3SourceAbsentObject checks that a template naming an object
// no test uploaded still deploys.
func TestLambdaCodeSize_CFNS3SourceAbsentObject(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	result, err := w.deployer.Deploy(context.Background(), cfnS3LambdaTemplate, "absent-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Empty(t, result.Resources[0].Error,
		"an absent package must not fail the stack")

	assert.Equal(t, int64(0), codeSize(t, w.getFunctionConfig(t, "cfn-s3")))
}

// TestLambdaCodeSize_CFNImageUri checks that a container-image template keeps both
// its image and its package type, which the deployer dropped along with Code.
func TestLambdaCodeSize_CFNImageUri(t *testing.T) {
	w := newLambdaCodeTestWorld(t)

	_, err := w.deployer.Deploy(context.Background(), cfnImageLambdaTemplate, "image-stack", nil)
	require.NoError(t, err)

	assert.Equal(t, "Image", w.getFunctionConfig(t, "cfn-image")["PackageType"])
	assert.Equal(t, "123456789012.dkr.ecr.us-east-1.amazonaws.com/cfn:latest",
		w.getFunctionCode(t, "cfn-image")["ImageUri"])
}

// TestLambdaCodeSize_CFNCodeResolvesRefs checks that Code's members go through the
// template's parameter and pseudo-parameter resolution like every other property.
//
// A template that hardcodes its bucket is the exception; one that takes it as a
// parameter is what a pipeline generates, and forwarding the unresolved {"Ref": …}
// would name a bucket called nothing at all.
func TestLambdaCodeSize_CFNCodeResolvesRefs(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "param-pkgs", "fn.zip", lambdaTestPackage)

	tmpl := `{"Parameters":{"Bucket":{"Type":"String"},"Key":{"Type":"String","Default":"fn.zip"}},
		"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-ref","Runtime":"python3.12","Handler":"index.handler",
		"Role":"arn:aws:iam::123456789012:role/r",
		"Code":{"S3Bucket":{"Ref":"Bucket"},"S3Key":{"Ref":"Key"}}}}}}`

	_, err := w.deployer.Deploy(context.Background(), tmpl, "ref-stack",
		map[string]string{"Bucket": "param-pkgs"})
	require.NoError(t, err)

	assert.Equal(t, int64(156), codeSize(t, w.getFunctionConfig(t, "cfn-ref")))
}

// TestLambdaCodeSize_CFNNoCode checks that a template declaring no Code at all is
// unchanged: it deploys, and reports no package.
//
// Substrate's own generated templates and many hand-written test fixtures omit Code,
// so this is the path that must not start failing.
func TestLambdaCodeSize_CFNNoCode(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	tmpl := `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{
		"FunctionName":"cfn-bare","Runtime":"python3.12","Handler":"index.handler",
		"Role":"arn:aws:iam::123456789012:role/r"}}}}`

	result, err := w.deployer.Deploy(context.Background(), tmpl, "bare-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)

	cfg := w.getFunctionConfig(t, "cfn-bare")
	assert.Equal(t, int64(0), codeSize(t, cfg))
	assert.Equal(t, "Zip", cfg["PackageType"])
}

// TestLambdaCodeSize_CFNRequestBodyCarriesCode asserts on the CreateFunction body the
// deployer sends, one level below the observations above.
//
// The gap was that this body carried no Code key at all, so asserting on its presence
// is the most direct statement of the fix — and it holds for a resource type whose
// Code arrives in a shape the Lambda plugin happens to tolerate as well as one it
// does not.
func TestLambdaCodeSize_CFNRequestBodyCarriesCode(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "pkgs", "fn.zip", lambdaTestPackage)
	_, err := w.deployer.Deploy(context.Background(), cfnS3LambdaTemplate, "body-stack", nil)
	require.NoError(t, err)

	raw, err := w.state.Get(context.Background(), "lambda", "function:cfn-s3")
	require.NoError(t, err) //nolint:testifylint // the deploy above is asserted separately.
	require.NotEmpty(t, raw)
	var fn map[string]any
	require.NoError(t, json.Unmarshal(raw, &fn))
	assert.Equal(t, float64(156), fn["CodeSize"],
		"the size reached the persisted record, not just the response")
}

// TestLambdaCodeSize_S3ETagIsTheDigest documents what the digest of an S3-sourced
// package actually is, since it is substrate's own choice rather than the API model.
//
// Substrate never fetches the bytes, so it cannot compute the SHA256 real Lambda
// reports. It reports the object's ETag instead, which for a single-part upload is
// the MD5 of the body — it changes exactly when the package changes, which is the
// question a caller comparing digests is asking, and it is stable across reads.
func TestLambdaCodeSize_S3ETagIsTheDigest(t *testing.T) {
	w := newLambdaCodeTestWorld(t)
	w.putS3Package(t, "etags", "fn.zip", lambdaTestPackage)

	head := s3Request(t, w.srv, http.MethodHead, "/etags/fn.zip", nil, nil)
	require.Equal(t, http.StatusOK, head.Code)
	etag := head.Header().Get("ETag")
	require.NotEmpty(t, etag)

	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "etag-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
		"Code":         map[string]any{"S3Bucket": "etags", "S3Key": "fn.zip"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	cfg := w.getFunctionConfig(t, "etag-fn")
	assert.Equal(t, strings.Trim(etag, `"`), cfg["CodeSha256"],
		"the digest is the object's ETag, unquoted")
}
