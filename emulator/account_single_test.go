package emulator_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The tests in this file are #734's gate, and between them they state the whole
// contract: there is one account, it is resolved in one place, and every service
// that names an account names that one.
//
// The defect was not two services disagreeing about a constant — it was substrate
// handing out a *second* account for free. ParseAWSRequest branched on the shape of
// the access key: a key beginning with "AKIA" resolved to 123456789012, and
// anything else — substrate's own documented test/test, an unsigned request, an
// ASIA session key — resolved to 000000000000. One server, two accounts, chosen by
// which of two documented credentials the client happened to pick, and nothing on
// the wire to tell a caller which one they got.
//
// So the fix has two halves that need pinning separately. The first is that the
// account comes from one place, which is what the tripwire below asserts
// structurally. The second is that every producer reads it from that place, which is
// what the agreement test asserts observably.

// TestSingleAccount_NoSecondHardcodedAccountInProduction is the tripwire.
//
// It parses every non-test Go file in the repository and refuses any string literal
// carrying an account-shaped run of digits, with exactly one exemption: the
// declaration of defaultAccountID itself. That is a stronger rule than "no
// 000000000000" — a second literal 123456789012 would be just as wrong, because the
// property #734 buys is that the account is resolved in one place and overridable
// from configuration. A literal anywhere else is a service that cannot be moved.
//
// It catches the offender the live run found: listEventBuses returned
// arn:aws:events:us-east-1:000000000000:event-bus/default, ignoring both of its
// request context's fields, so a caller's own default event bus was reported in an
// account and a Region that were not theirs.
func TestSingleAccount_NoSecondHardcodedAccountInProduction(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	type finding struct {
		file    string
		line    int
		account string
		inside  string
	}
	var found []finding

	fset := token.NewFileSet()
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// test/ is the e2e suite and testdata/ is fixtures; neither ships.
			// examples/ is consumer code, and a consumer filling in AccountID on a
			// RequestContext it builds itself is the documented way to choose an
			// account — the opposite of the defect. The rest are not Go at all and
			// walking them only costs time.
			switch d.Name() {
			case ".git", "node_modules", "docs", "examples", "test", "testdata":
				if path != root {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		f, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		// The one exemption, identified structurally rather than by file name: the
		// value of the const named defaultAccountID.
		exempt := map[ast.Expr]bool{}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}

		ast.Inspect(f, func(n ast.Node) bool {
			if spec, ok := n.(*ast.ValueSpec); ok {
				for _, name := range spec.Names {
					if name.Name == "defaultAccountID" {
						for _, v := range spec.Values {
							exempt[v] = true
						}
					}
				}
				return true
			}
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || exempt[lit] {
				return true
			}
			text, unquoteErr := strconv.Unquote(lit.Value)
			if unquoteErr != nil {
				return true
			}
			for _, account := range accountShapedRuns(text) {
				found = append(found, finding{
					file:    rel,
					line:    fset.Position(lit.Pos()).Line,
					account: account,
					inside:  text,
				})
			}
			return true
		})
		return nil
	})
	require.NoError(t, walkErr)

	for _, f := range found {
		t.Errorf("%s:%d hardcodes account %s in %q; read it from RequestContext.AccountID, "+
			"or from defaultAccountID when there is genuinely no request in hand",
			f.file, f.line, f.account, f.inside)
	}
}

// accountShapedRuns returns the account IDs a string literal contains.
//
// An AWS account ID is twelve digits, and the bounding rule is what keeps this from
// firing on identifiers that merely contain twelve zeroes: an account ID appears in
// an ARN between colons, or after one and before a slash, so a run bounded by
// anything else is part of a longer token rather than an account. That excludes
// Kinesis's "shardId-000000000000" and DynamoDB's stream shard IDs, both preceded by
// a hyphen, and MSK's "-0001-0001-0001-000000000001" for the same reason. Requiring
// the run to be maximal excludes the twenty-digit shard sequence and the
// fourteen-digit pricing offer version, which are not twelve digits long even though
// they contain twelve consecutive ones.
func accountShapedRuns(s string) []string {
	const accountIDLen = 12
	var out []string
	for i := 0; i < len(s); {
		if s[i] < '0' || s[i] > '9' {
			i++
			continue
		}
		j := i
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j-i == accountIDLen && accountIDBoundary(s, i-1) && accountIDBoundary(s, j) {
			out = append(out, s[i:j])
		}
		i = j
	}
	return out
}

// accountIDBoundary reports whether index i can bound an account ID: either it is
// off the end of the string, or it holds one of the two characters an ARN uses to
// separate an account from what surrounds it.
func accountIDBoundary(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return true
	}
	return s[i] == ':' || s[i] == '/'
}

// singleAccountQuery posts a query-protocol request to a service in us-east-1,
// signed as account.
//
// An empty account leaves the request unsigned, which still reaches its plugin —
// [emulator.VerifySigV4] passes a request carrying no Authorization header — and
// resolves to the server's default account.
func singleAccountQuery(t *testing.T, ts *emulator.TestServer, service, account string,
	params map[string]string,
) (int, string) {
	t.Helper()

	var creds emulator.CredentialEntry
	if account != "" {
		var ok bool
		creds, ok = ts.CredentialsFor(account)
		require.True(t, ok, "no credential registered for account %s", account)
	}

	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	body := form.Encode()
	host := service + ".us-east-1.amazonaws.com"

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(body))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	signAs(req, creds, service, "us-east-1", []byte(body))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// TestSingleAccount_ServicesAgreeOnTheAccount is the observable half, and it is the
// test that would have caught #734 the way the live run did: by asking three
// different services who the caller is and comparing the answers.
//
// The three are not arbitrary. sts:GetCallerIdentity is the only operation whose
// entire purpose is to answer the question, and it puts the account in all three of
// its members. The CloudFormation deployer mints a stack ARN from an identity it
// carries separately, which is the split #517 was filed for. And the Resource Groups
// Tagging API reaches a resource through an account-prefixed state key, so its
// answer proves the account the write used and the account the read used are the
// same string — which is the failure mode that leaves a resource created and then
// invisible.
//
// It runs for two accounts, because a producer that returns a constant agrees with
// itself. The second account is reachable only by signing, so a disagreement shows
// up as a resource in the wrong account rather than as a resource that is missing.
func TestSingleAccount_ServicesAgreeOnTheAccount(t *testing.T) {
	const secondAccount = "777788889999"
	ts := emulator.StartTestServerWithAccounts(t, secondAccount)

	for _, account := range []string{"123456789012", secondAccount} {
		t.Run("account "+account, func(t *testing.T) {
			// 1. The operation whose whole job is to answer the question. Both
			// members that carry an account carry this one: Arn names it in its
			// fifth field, and a caller comparing the two must see them agree.
			code, body := singleAccountQuery(t, ts, "sts", account, map[string]string{
				"Action": "GetCallerIdentity", "Version": "2011-06-15",
			})
			require.Equal(t, http.StatusOK, code, "GetCallerIdentity: body was %s", body)
			assert.Equal(t, account, cfnXMLValue(t, body, "Account"))
			assert.True(t, strings.HasPrefix(cfnXMLValue(t, body, "Arn"), "arn:aws:iam::"+account+":"),
				"Arn was %s", cfnXMLValue(t, body, "Arn"))

			// 2. The CloudFormation deployer's own identity, which reaches a caller
			// only as the StackId it mints. The stack name carries the account
			// because a stack record is not partitioned by one — that is #517's
			// other half, and it is not what this test is about.
			stack := "agreement-" + account
			code, body = singleAccountQuery(t, ts, "cloudformation", account, map[string]string{
				"Action": "CreateStack", "Version": "2010-05-15",
				"StackName": stack, "TemplateBody": cfnEmptyTemplate,
			})
			require.Equal(t, http.StatusOK, code, "CreateStack: body was %s", body)
			assert.Contains(t, cfnXMLValue(t, body, "StackId"),
				":cloudformation:us-east-1:"+account+":stack/"+stack+"/")

			// 3. An account-prefixed state key, written by one service and read by
			// another. A DynamoDB table is written under "table:{account}/{name}";
			// the tagging API lists that prefix and reports the ARN the table
			// carries. Both halves have to agree with the account above, or the
			// table is created and then unreachable.
			resp := signedRequest(t, ts, signedRequestTarget{
				host:        "dynamodb.us-east-1.amazonaws.com",
				target:      "DynamoDB_20120810",
				signingName: "dynamodb",
			}, account, "CreateTable", map[string]any{
				"TableName":            "agreement",
				"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
				"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
				"Tags":                 []map[string]string{{"Key": "Owner", "Value": "agreement"}},
			})
			status, errCode := decodeAWSResponse(t, resp, nil)
			require.Equal(t, http.StatusOK, status, "CreateTable: %s", errCode)

			var listed struct {
				ResourceTagMappingList []struct {
					ResourceARN string `json:"ResourceARN"`
					Tags        []struct {
						Key   string `json:"Key"`
						Value string `json:"Value"`
					} `json:"Tags"`
				} `json:"ResourceTagMappingList"`
			}
			resp = signedRequest(t, ts, signedRequestTarget{
				host:        "tagging.us-east-1.amazonaws.com",
				target:      "ResourceGroupsTaggingAPI_20170126",
				signingName: "tagging",
			}, account, "GetResources", map[string]any{
				"ResourceTypeFilters": []string{"dynamodb:table"},
			})
			status, errCode = decodeAWSResponse(t, resp, &listed)
			require.Equal(t, http.StatusOK, status, "GetResources: %s", errCode)

			require.Len(t, listed.ResourceTagMappingList, 1,
				"a caller sees its own table and only its own")
			assert.Equal(t, "arn:aws:dynamodb:us-east-1:"+account+":table/agreement",
				listed.ResourceTagMappingList[0].ResourceARN)
			require.Len(t, listed.ResourceTagMappingList[0].Tags, 1)
			assert.Equal(t, "Owner", listed.ResourceTagMappingList[0].Tags[0].Key)
		})
	}

	// The unsigned caller is the case #734 was really about, and it is also the one
	// path on which all three of GetCallerIdentity's members come from the account
	// rather than from a principal. Substrate's own documented credentials, an
	// unsigned request and an ASIA session key used to resolve to 000000000000
	// while AKIAIOSFODNN7EXAMPLE resolved to 123456789012; now there is one answer.
	code, body := singleAccountQuery(t, ts, "sts", "", map[string]string{
		"Action": "GetCallerIdentity", "Version": "2011-06-15",
	})
	require.Equal(t, http.StatusOK, code, "unsigned GetCallerIdentity: body was %s", body)
	assert.Equal(t, "123456789012", cfnXMLValue(t, body, "Account"))
	assert.Equal(t, "123456789012", cfnXMLValue(t, body, "UserId"))
	assert.Equal(t, "arn:aws:iam::123456789012:root", cfnXMLValue(t, body, "Arn"))
}
