package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The five property lookups that read nothing (#877), and the identity one of them fed (#859).
//
// resolveStringProp does a single flat map index, so a key spelled with a dot matches nothing and
// the fallback is returned unconditionally. Five call sites passed one, and the result was not a
// missing value but a *wrong* one: a Glue job whose command was "glueetl" because the template's
// "pythonshell" was never read, an ECS task definition registered as EC2 because the template's
// FARGATE was never read, a distribution whose stored comment was its own logical ID, and an origin
// access identity whose physical ID — and therefore its Ref — was its logical ID.
//
// Every assertion here reads the value back through the *owning service's* own API rather than off
// the DeployedResource, per #765: a fallback that reached the service produces a deploy result that
// looks entirely correct, which is how all five survived. The one exception is the OAI, whose deploy
// dispatches nothing at all — there is no service-side record to read, so its identity is asserted
// on the PhysicalID and on the two intrinsics that resolve from it.
//
// None of the five had a test before this file.

// cfnDottedPropsAccount and cfnDottedPropsRegion are the identity newFullTestDeployerWithRegistry
// deploys under, which the OAI's derivation reads.
const (
	cfnDottedPropsAccount = "123456789012"
	cfnDottedPropsRegion  = "us-east-1"
)

// TestCFNProps_NoDottedResolveStringPropKey is the tripwire that keeps a sixth site from appearing.
//
// It parses every non-test file in the package and refuses any resolveStringProp call whose key
// argument is a string literal containing a dot. That is a structural rule rather than a review
// habit for the reason the five sites needed it: the call compiles, the deploy succeeds, the stack
// reports CREATE_COMPLETE, and the only symptom is a value the template did not ask for. Nothing
// short of reading the template back through the service catches it, and four of the five were
// written by copying a neighboring line.
//
// A dotted key is refused rather than made to work, because a nested read and a list-indexed read
// are different operations — [resolveNestedStringProp] and [resolveIndexedStringProp] — and a
// shared dotted-path walk would have turned all five constants into template-controlled values at
// once, as a side effect of a refactor rather than as a decision per site.
func TestCFNProps_NoDottedResolveStringPropKey(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files, "the package's own sources must be readable from the test's cwd")

	// The helper is unexported, so every possible caller is a file in this one directory — no walk
	// is needed, and a flat glob cannot silently stop covering a subdirectory that gains one.
	fset := token.NewFileSet()
	checked := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		checked++

		f, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr)

		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			ident, ok := call.Fun.(*ast.Ident)
			if !ok || ident.Name != "resolveStringProp" || len(call.Args) < 2 {
				return true
			}
			lit, ok := call.Args[1].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				// A computed key cannot be judged here. That is not a gap worth closing: no site
				// builds one, and the defect is a literal written with a dot in it.
				return true
			}
			key, unquoteErr := strconv.Unquote(lit.Value)
			if unquoteErr != nil || !strings.Contains(key, ".") {
				return true
			}
			t.Errorf("%s:%d passes the dotted key %q to resolveStringProp, which indexes props "+
				"flat — the lookup matches nothing and the fallback is returned unconditionally "+
				"(#877). Use resolveNestedStringProp for a nested object member, or "+
				"resolveIndexedStringProp for a list element.",
				path, fset.Position(lit.Pos()).Line, key)
			return true
		})
	}
	require.Greater(t, checked, 100, "the glob must be reaching the package's sources")
}

// cfnDottedGlueJob reads a Glue job back through GetJob and returns its Command.
func cfnDottedGlueJob(t *testing.T, registry *emulator.PluginRegistry, name string) emulator.GlueJobCommand {
	t.Helper()
	body, err := json.Marshal(map[string]any{"JobName": name})
	require.NoError(t, err)

	resp, err := routeJSON(t, registry, "glue", "AWSGlue.GetJob", "GetJob", body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GetJob: %s", resp.Body)

	var out struct {
		Job struct {
			Command emulator.GlueJobCommand `json:"Command"`
		} `json:"Job"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out.Job.Command
}

// TestCFNGlueJob_CommandIsReadFromTheTemplate covers the highest-impact pair of the five.
//
// Both members of Command were indexed flat, so every Glue job created through CloudFormation
// reported an empty ScriptLocation — the one property that says what the job runs — and a
// pythonshell job was created as a Spark ETL job. Neither is visible in the deploy result: the
// fallbacks are a valid command name and a valid empty string, so the stack completes and GetJob
// answers 200 with the wrong job.
func TestCFNGlueJob_CommandIsReadFromTheTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string

		wantName     string
		wantLocation string
	}{
		{
			name: "a pythonshell job with a script",
			command: `"Command": {
				"Name": "pythonshell",
				"ScriptLocation": "s3://scripts/etl.py"
			},`,
			wantName:     "pythonshell",
			wantLocation: "s3://scripts/etl.py",
		},
		{
			name: "a Spark job with a script",
			command: `"Command": {
				"Name": "glueetl",
				"ScriptLocation": "s3://scripts/spark.scala"
			},`,
			wantName: "glueetl",
			// The Name matched the old fallback, so this case is the one that isolates
			// ScriptLocation: it fails on the location alone.
			wantLocation: "s3://scripts/spark.scala",
		},
		{
			name:    "no Command at all",
			command: "",
			// The fallbacks are still the fallbacks. A fix that read a nested value but lost the
			// default would change what a template omitting Command produces, which is a
			// different behavior change from the one #877 asks for.
			wantName:     "glueetl",
			wantLocation: "",
		},
		{
			name:    "a Command carrying neither member",
			command: `"Command": {},`,
			// A present-but-empty object is the case a walk that only checked the outer key would
			// get wrong: props["Command"] exists, so an early return would hand back "".
			wantName:     "glueetl",
			wantLocation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, registry := newFullTestDeployerWithRegistry(t)
			tmpl := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Job": {
						"Type": "AWS::Glue::Job",
						"Properties": {
							"JobName": "nightly",
							"Role": "arn:aws:iam::123456789012:role/glue",
							` + tt.command + `
							"Description": "unused"
						}
					}
				}
			}`
			result, err := d.Deploy(context.Background(), tmpl, "glue-command-stack", nil)
			require.NoError(t, err)
			require.Empty(t, findResource(t, result, "Job").Error)

			cmd := cfnDottedGlueJob(t, registry, "nightly")
			assert.Equal(t, tt.wantName, cmd.Name,
				"the command name decides whether the job is Spark ETL or Python shell")
			assert.Equal(t, tt.wantLocation, cmd.ScriptLocation,
				"ScriptLocation is what the job runs; an empty one is a job that runs nothing")
		})
	}
}

// TestCFNGlueJob_CommandResolvesAnIntrinsic pins that the nested read goes through resolveValue.
//
// A nested walk that returned the raw map member would work for a literal and silently render a
// map for a !Sub — the same class of wrong value, one level deeper. The bucket name arrives as a
// stack parameter so the value cannot be a literal anywhere in the template.
func TestCFNGlueJob_CommandResolvesAnIntrinsic(t *testing.T) {
	t.Parallel()
	d, registry := newFullTestDeployerWithRegistry(t)

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"ScriptBucket": {"Type": "String"}},
		"Resources": {
			"Job": {
				"Type": "AWS::Glue::Job",
				"Properties": {
					"JobName": "nightly",
					"Role": "arn:aws:iam::123456789012:role/glue",
					"Command": {
						"Name": "pythonshell",
						"ScriptLocation": {"Fn::Sub": "s3://${ScriptBucket}/etl.py"}
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "glue-sub-stack",
		map[string]string{"ScriptBucket": "team-scripts"})
	require.NoError(t, err)
	require.Empty(t, findResource(t, result, "Job").Error)

	assert.Equal(t, "s3://team-scripts/etl.py", cfnDottedGlueJob(t, registry, "nightly").ScriptLocation,
		"a nested member is resolved, not carried through raw")
}

// cfnDottedTaskDefinition reads a task definition back and returns its requiresCompatibilities.
func cfnDottedTaskDefinition(t *testing.T, registry *emulator.PluginRegistry, family string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"taskDefinition": family})
	require.NoError(t, err)

	resp, err := routeJSON(t, registry, "ecs",
		"AmazonEC2ContainerServiceV20141113.DescribeTaskDefinition", "DescribeTaskDefinition", body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "DescribeTaskDefinition: %s", resp.Body)

	var out struct {
		TaskDefinition struct {
			RequiresCompatibilities []string `json:"requiresCompatibilities"`
		} `json:"taskDefinition"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out.TaskDefinition.RequiresCompatibilities
}

// TestCFNECSTaskDefinition_RequiresCompatibilitiesIsReadFromTheTemplate is the list-indexed site.
//
// "RequiresCompatibilities.0" was indexed flat, so every task definition deployed through
// CloudFormation registered as EC2 and a Fargate one was never Fargate — a launch type a consumer
// asserts on and a service refuses to run against the wrong one. This is also the site that ruled
// out fixing the class in resolveStringProp: the value is a list element, which a dotted-path map
// walk cannot reach at all.
func TestCFNECSTaskDefinition_RequiresCompatibilitiesIsReadFromTheTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		property    string
		wantElement string
	}{
		{
			name:        "a Fargate task definition",
			property:    `"RequiresCompatibilities": ["FARGATE"],`,
			wantElement: "FARGATE",
		},
		{
			name: "an EC2 task definition states it",
			// Matches the old fallback, so on its own this case proves nothing — it is here
			// because a fix that read the list but dropped a one-element one would pass the
			// Fargate case and fail this one.
			property:    `"RequiresCompatibilities": ["EC2"],`,
			wantElement: "EC2",
		},
		{
			name:        "the first element of several",
			property:    `"RequiresCompatibilities": ["FARGATE", "EC2"],`,
			wantElement: "FARGATE",
		},
		{
			name:     "no RequiresCompatibilities at all",
			property: "",
			// AWS's own default when the property is absent, and unchanged by #877.
			wantElement: "EC2",
		},
		{
			name: "an empty list",
			// The bounds case: the key resolves to a list, so a read that indexed it without
			// checking the length would panic rather than fall back.
			property:    `"RequiresCompatibilities": [],`,
			wantElement: "EC2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, registry := newFullTestDeployerWithRegistry(t)
			tmpl := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Task": {
						"Type": "AWS::ECS::TaskDefinition",
						"Properties": {
							"Family": "web",
							"NetworkMode": "awsvpc",
							` + tt.property + `
							"ContainerDefinitions": [
								{"Name": "app", "Image": "nginx:latest", "Essential": true}
							]
						}
					}
				}
			}`
			result, err := d.Deploy(context.Background(), tmpl, "ecs-compat-stack", nil)
			require.NoError(t, err)
			require.Empty(t, findResource(t, result, "Task").Error)

			assert.Equal(t, []string{tt.wantElement}, cfnDottedTaskDefinition(t, registry, "web"),
				"the launch type a service must match is registered from the template")
		})
	}
}

// cfnDottedDistributionComment reads a distribution back through GetDistribution.
func cfnDottedDistributionComment(t *testing.T, registry *emulator.PluginRegistry, distID string) string {
	t.Helper()
	resp, err := registry.RouteRequest(&emulator.RequestContext{
		RequestID: "test-request",
		AccountID: cfnDottedPropsAccount,
		Region:    cfnDottedPropsRegion,
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}, &emulator.AWSRequest{
		Service: "cloudfront", Operation: "GET",
		Path:    "/2020-05-31/distribution/" + distID,
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GetDistribution: %s", resp.Body)

	// GetDistribution renders Comment directly inside <Distribution> — see
	// marshalDistributionXML, which is also what GetDistributionConfig reports under its own
	// wrapper. Either would answer; this one is the operation a consumer reads a distribution with.
	var out struct {
		XMLName xml.Name `xml:"Distribution"`
		Comment string   `xml:"Comment"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &out))
	return out.Comment
}

// TestCFNCloudFrontDistribution_CommentIsReadFromTheTemplate is the cosmetic one of the five, and
// it is here because "cosmetic" is a judgement about the blast radius rather than about the value.
//
// The distribution's PhysicalID is recovered from CreateDistribution's response Id, so the wrong
// comment corrupted nothing downstream. But what GetDistribution reported was still the template's
// logical ID rather than what the template said — a value from somewhere other than the request,
// which is the whole class.
func TestCFNCloudFrontDistribution_CommentIsReadFromTheTemplate(t *testing.T) {
	t.Parallel()
	d, registry := newFullTestDeployerWithRegistry(t)

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Cdn": {
				"Type": "AWS::CloudFront::Distribution",
				"Properties": {
					"DistributionConfig": {
						"Comment": "public assets",
						"Enabled": true
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cdn-stack", nil)
	require.NoError(t, err)

	dist := findResource(t, result, "Cdn")
	require.Empty(t, dist.Error)
	require.NotEqual(t, "Cdn", dist.PhysicalID,
		"the distribution's ID comes from the response, not from the logical ID")

	comment := cfnDottedDistributionComment(t, registry, dist.PhysicalID)
	assert.Equal(t, "public assets", comment)
	assert.NotEqual(t, "Cdn", comment, "the logical ID is not what the template said")
}

// cfnOAIIDPattern is the shape AWS publishes twice — E15MNIMTCFKK4C via Ref and E74FTE3AJFJ256A via
// Fn::GetAtt Id. Asserted as a pattern rather than as a fixed value so the test states the contract
// (#859) instead of restating one hash's output, which would have to be edited alongside any change
// to the derivation and would then prove nothing about it.
var cfnOAIIDPattern = regexp.MustCompile(`^E[A-Z0-9]{13}$`)

// cfnDeployOAIs deploys count origin access identities into stackName and returns the deploy
// result, with an output per identity for Ref, Fn::GetAtt Id and Fn::GetAtt S3CanonicalUserId.
func cfnDeployOAIs(t *testing.T, stackName string, count int) *emulator.DeployResult {
	t.Helper()
	d, _ := newFullTestDeployerWithRegistry(t)

	resources := make([]string, 0, count)
	outputs := make([]string, 0, count*3)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("Oai%d", i)
		resources = append(resources, `"`+id+`": {
			"Type": "AWS::CloudFront::CloudFrontOriginAccessIdentity",
			"Properties": {
				"CloudFrontOriginAccessIdentityConfig": {"Comment": "for `+id+`"}
			}
		}`)
		outputs = append(outputs,
			`"`+id+`Ref": {"Value": {"Ref": "`+id+`"}}`,
			`"`+id+`Id": {"Value": {"Fn::GetAtt": ["`+id+`", "Id"]}}`,
			`"`+id+`Canonical": {"Value": {"Fn::GetAtt": ["`+id+`", "S3CanonicalUserId"]}}`)
	}

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {` + strings.Join(resources, ",") + `},
		"Outputs": {` + strings.Join(outputs, ",") + `}
	}`
	result, err := d.Deploy(context.Background(), tmpl, stackName, nil)
	require.NoError(t, err)
	return result
}

// TestCFNCloudFrontOAI_MintsAnIdentity is #859.
//
// PhysicalID was the resource's own logical ID — not because a stub was left behind, but because
// the dotted lookup that was meant to read the comment returned its fallback, and the fallback was
// the logical ID. So !Ref Oai handed an S3 bucket policy or an origin-access configuration a value
// AWS would never mint, and the template's comment was never read either.
func TestCFNCloudFrontOAI_MintsAnIdentity(t *testing.T) {
	t.Parallel()
	result := cfnDeployOAIs(t, "oai-stack", 1)

	oai := findResource(t, result, "Oai0")
	require.Empty(t, oai.Error)
	assert.Regexp(t, cfnOAIIDPattern, oai.PhysicalID,
		"AWS publishes E followed by 13 uppercase alphanumerics, twice")
	assert.Len(t, oai.PhysicalID, 14)
	assert.NotEqual(t, "Oai0", oai.PhysicalID, "the logical ID is not an identifier AWS mints")

	// Ref has no cfnRefValue arm and falls through to PhysicalID, which is correct here rather
	// than incidental: AWS's Ref for this type *is* the identity's ID.
	assert.Equal(t, oai.PhysicalID, result.Outputs["Oai0Ref"],
		"Ref returns the origin access identity, such as E15MNIMTCFKK4C")
	assert.Equal(t, oai.PhysicalID, result.Outputs["Oai0Id"],
		"Fn::GetAtt Id and Ref are one identifier under two names, so they cannot disagree")

	// The recorded decision, not an oversight: AWS publishes no format for S3CanonicalUserId —
	// one 96-hex sample — so substrate answers empty rather than inventing a grammar (#827).
	assert.Empty(t, result.Outputs["Oai0Canonical"],
		"S3CanonicalUserId stays unresolved by decision")

	assert.Equal(t, "for Oai0", oai.Metadata["Comment"],
		"the comment the dotted lookup never read is recorded where a reader can see it")
}

// TestCFNCloudFrontOAI_TwoInOneStackDiffer and the redeploy test below are the two halves of the
// derivation's contract, and they pull in opposite directions: the ID must be stable across
// deploys of one resource and distinct between two resources. A constant satisfies the first and
// crypto/rand the second; only a derivation over the resource's own scope satisfies both.
func TestCFNCloudFrontOAI_TwoInOneStackDiffer(t *testing.T) {
	t.Parallel()
	result := cfnDeployOAIs(t, "oai-pair-stack", 2)

	first := findResource(t, result, "Oai0").PhysicalID
	second := findResource(t, result, "Oai1").PhysicalID
	assert.Regexp(t, cfnOAIIDPattern, first)
	assert.Regexp(t, cfnOAIIDPattern, second)
	assert.NotEqual(t, first, second,
		"two identities in one stack are two resources and must not share an ID")
}

// TestCFNCloudFrontOAI_RedeployIsStable is the half that rules out crypto/rand.
//
// generateCloudFrontID already produces exactly this shape, and reusing it would have been the
// shorter change — but UpdateStack in substrate re-deploys the whole template, so a random ID would
// change on every update and every bucket policy naming the old one would silently stop matching.
// Two deployers are used rather than two Deploy calls on one, so nothing carried over in memory can
// account for the agreement.
func TestCFNCloudFrontOAI_RedeployIsStable(t *testing.T) {
	t.Parallel()

	first := findResource(t, cfnDeployOAIs(t, "oai-stable-stack", 1), "Oai0").PhysicalID
	second := findResource(t, cfnDeployOAIs(t, "oai-stable-stack", 1), "Oai0").PhysicalID

	assert.Regexp(t, cfnOAIIDPattern, first)
	assert.Equal(t, first, second,
		"a redeploy of one resource reports the identity it reported before")
}

// TestCFNCloudFrontOAI_ScopeChangesTheIdentity pins each component of the derivation.
//
// A derivation that ignored the stack name would make two stacks' identities collide, and one that
// ignored the logical ID is the previous test's opposite failure. Only the stack name and the
// logical ID are varied here: the account and Region are fixed by the test deployer, and #734's
// single-account rule means a test cannot vary the account without building its own wiring.
func TestCFNCloudFrontOAI_ScopeChangesTheIdentity(t *testing.T) {
	t.Parallel()

	base := findResource(t, cfnDeployOAIs(t, "oai-scope-a", 1), "Oai0").PhysicalID
	otherStack := findResource(t, cfnDeployOAIs(t, "oai-scope-b", 1), "Oai0").PhysicalID
	assert.NotEqual(t, base, otherStack,
		"the same logical ID in two stacks is two identities")

	// Oai1 in a two-resource stack is the same logical ID varied against the same stack name as
	// TestCFNCloudFrontOAI_TwoInOneStackDiffer, which is the logical-ID component.
	assert.NotEqual(t, base, findResource(t, cfnDeployOAIs(t, "oai-scope-a", 2), "Oai1").PhysicalID,
		"two logical IDs in one stack are two identities")
}
