package emulator_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestLambdaFunctionFields_DefaultsAreAnswered pins the defaults a function created without
// ImageConfig, EphemeralStorage or LoggingConfig reports (#1659), and that its tags stay out of
// Configuration (#1660).
func TestLambdaFunctionFields_DefaultsAreAnswered(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	created := lambdaCreateZipFunction(t, srv, "plain", "print(1)", map[string]any{
		"Tags": map[string]string{"team": "a"},
	})

	assert.Equal(t, map[string]any{"Size": float64(512)}, created["EphemeralStorage"])
	assert.Equal(t, map[string]any{"LogFormat": "Text", "LogGroup": "/aws/lambda/plain"}, created["LoggingConfig"])
	assert.NotContains(t, created, "ImageConfigResponse", "a zip function has no image to configure")
	assert.NotContains(t, created, "Tags", "FunctionConfiguration has no Tags member")

	status, got, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/plain", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, map[string]any{"team": "a"}, got["Tags"], "GetFunction answers the tags beside Configuration")
	cfg := got["Configuration"].(map[string]any)
	assert.NotContains(t, cfg, "Tags", "and never inside it")
	assert.Equal(t, map[string]any{"Size": float64(512)}, cfg["EphemeralStorage"])
}

// TestLambdaFunctionFields_EveryConfigurationAnswersThem drives an image function carrying all
// three fields through every operation that renders a FunctionConfiguration.
func TestLambdaFunctionFields_EveryConfigurationAnswersThem(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	imageConfig := map[string]any{
		"EntryPoint":       []any{"/lambda-entrypoint.sh"},
		"Command":          []any{"app.handler"},
		"WorkingDirectory": "/var/task",
	}
	status, created, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "img", "Role": "arn:aws:iam::123456789012:role/r",
		"PackageType": "Image", "Architectures": []string{"arm64"},
		"Code":             map[string]any{"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest"},
		"ImageConfig":      imageConfig,
		"EphemeralStorage": map[string]any{"Size": 2048},
		"LoggingConfig":    map[string]any{"LogFormat": "Text", "LogGroup": "/aegc/dev"},
	})
	require.Equal(t, http.StatusCreated, status, raw)

	wantImage := map[string]any{"ImageConfig": imageConfig}
	wantStorage := map[string]any{"Size": float64(2048)}
	wantLogging := map[string]any{"LogFormat": "Text", "LogGroup": "/aegc/dev"}
	check := func(t *testing.T, cfg map[string]any) {
		t.Helper()
		assert.Equal(t, wantImage, cfg["ImageConfigResponse"])
		assert.Equal(t, wantStorage, cfg["EphemeralStorage"])
		assert.Equal(t, wantLogging, cfg["LoggingConfig"])
	}
	check(t, created)

	version := lambdaPublish(t, srv, "img")
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/img/aliases",
		map[string]any{"Name": "live", "FunctionVersion": version})
	require.Equal(t, http.StatusCreated, status, raw)

	for _, tc := range []struct {
		name, path string
		pick       func(map[string]any) map[string]any
	}{
		{"GetFunction", "/2015-03-31/functions/img", func(m map[string]any) map[string]any {
			return m["Configuration"].(map[string]any)
		}},
		{"GetFunctionConfiguration", "/2015-03-31/functions/img/configuration", func(m map[string]any) map[string]any { return m }},
		{"GetFunctionConfiguration of the version", "/2015-03-31/functions/img/configuration?Qualifier=" + version,
			func(m map[string]any) map[string]any { return m }},
		{"GetFunction of the alias", "/2015-03-31/functions/img:live", func(m map[string]any) map[string]any {
			return m["Configuration"].(map[string]any)
		}},
		{"ListFunctions", "/2015-03-31/functions", func(m map[string]any) map[string]any {
			return m["Functions"].([]any)[0].(map[string]any)
		}},
		{"ListVersionsByFunction", "/2015-03-31/functions/img/versions", func(m map[string]any) map[string]any {
			return m["Versions"].([]any)[1].(map[string]any)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, got, raw := lambdaDo(t, srv, http.MethodGet, tc.path, nil)
			require.Equal(t, http.StatusOK, status, raw)
			check(t, tc.pick(got))
		})
	}
}

// TestLambdaFunctionFields_JSONLoggingDefaultsBothLevels pins the levels a JSON function reports:
// INFO for each it does not name, and the named one as given.
func TestLambdaFunctionFields_JSONLoggingDefaultsBothLevels(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	created := lambdaCreateZipFunction(t, srv, "json", "print(1)", map[string]any{
		"LoggingConfig": map[string]any{"LogFormat": "JSON", "SystemLogLevel": "WARN"},
	})
	assert.Equal(t, map[string]any{
		"LogFormat": "JSON", "ApplicationLogLevel": "INFO", "SystemLogLevel": "WARN", "LogGroup": "/aws/lambda/json",
	}, created["LoggingConfig"])
}

// TestLambdaFunctionFields_InvalidValuesAreRefused covers each refusal CreateFunction and
// UpdateFunctionConfiguration make.
func TestLambdaFunctionFields_InvalidValuesAreRefused(t *testing.T) {
	t.Parallel()
	textLevel := "LogLevel is not supported when LogFormat is set to"
	cases := []struct {
		name    string
		fields  map[string]any
		message string
	}{
		{"storage below 512", map[string]any{"EphemeralStorage": map[string]any{"Size": 511}}, "between 512 and 10240"},
		{"storage above 10240", map[string]any{"EphemeralStorage": map[string]any{"Size": 10241}}, "between 512 and 10240"},
		{"unknown format", map[string]any{"LoggingConfig": map[string]any{"LogFormat": "XML"}}, "JSON or Text"},
		{"unknown application level", map[string]any{"LoggingConfig": map[string]any{"LogFormat": "JSON", "ApplicationLogLevel": "LOUD"}}, "ApplicationLogLevel"},
		{"unknown system level", map[string]any{"LoggingConfig": map[string]any{"LogFormat": "JSON", "SystemLogLevel": "TRACE"}}, "SystemLogLevel"},
		{"a level under Text", map[string]any{"LoggingConfig": map[string]any{"LogFormat": "Text", "ApplicationLogLevel": "WARN"}}, textLevel},
		{"a level with no format", map[string]any{"LoggingConfig": map[string]any{"SystemLogLevel": "WARN"}}, textLevel},
	}
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "target", "print(1)", nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"FunctionName": "refused", "Runtime": "python3.12", "Handler": "index.handler",
				"Role": "arn:aws:iam::123456789012:role/r",
			}
			for k, v := range tc.fields {
				body[k] = v
			}
			status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, "InvalidParameterValueException")
			assert.Contains(t, raw, tc.message)

			status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/target/configuration", tc.fields)
			assert.Equal(t, http.StatusBadRequest, status, "UpdateFunctionConfiguration: %s", raw)
			assert.Contains(t, raw, tc.message)
		})
	}
	status, _, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/refused", nil)
	assert.Equal(t, http.StatusNotFound, status, "a refused create stores nothing: %s", raw)
}

// TestLambdaFunctionFields_UpdateMergesAndReplaces pins how UpdateFunctionConfiguration applies
// each field: EphemeralStorage replaces the size, LoggingConfig merges member by member and
// switching to Text drops the levels, and ImageConfig replaces the whole configuration.
func TestLambdaFunctionFields_UpdateMergesAndReplaces(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "upd", "Role": "arn:aws:iam::123456789012:role/r", "PackageType": "Image",
		"Code":        map[string]any{"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/upd:1"},
		"ImageConfig": map[string]any{"Command": []string{"a.handler"}, "WorkingDirectory": "/w"},
	})
	require.Equal(t, http.StatusCreated, status, raw)

	update := func(body map[string]any) map[string]any {
		t.Helper()
		status, out, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/upd/configuration", body)
		require.Equal(t, http.StatusOK, status, raw)
		return out
	}

	got := update(map[string]any{"EphemeralStorage": map[string]any{"Size": 4096}})
	assert.Equal(t, map[string]any{"Size": float64(4096)}, got["EphemeralStorage"])
	assert.Equal(t, map[string]any{"ImageConfig": map[string]any{"Command": []any{"a.handler"}, "WorkingDirectory": "/w"}},
		got["ImageConfigResponse"], "an update naming no ImageConfig keeps it")

	got = update(map[string]any{"LoggingConfig": map[string]any{"LogFormat": "JSON"}})
	assert.Equal(t, map[string]any{"Size": float64(4096)}, got["EphemeralStorage"], "an update naming no EphemeralStorage keeps it")
	got = update(map[string]any{"LoggingConfig": map[string]any{"ApplicationLogLevel": "ERROR"}})
	assert.Equal(t, map[string]any{
		"LogFormat": "JSON", "ApplicationLogLevel": "ERROR", "SystemLogLevel": "INFO", "LogGroup": "/aws/lambda/upd",
	}, got["LoggingConfig"], "a level merges into the stored JSON format")
	got = update(map[string]any{"LoggingConfig": map[string]any{"LogFormat": "Text"}})
	assert.Equal(t, map[string]any{"LogFormat": "Text", "LogGroup": "/aws/lambda/upd"}, got["LoggingConfig"],
		"Text without a level drops the stored levels")

	got = update(map[string]any{"ImageConfig": map[string]any{"EntryPoint": []string{"/e"}}})
	assert.Equal(t, map[string]any{"ImageConfig": map[string]any{"EntryPoint": []any{"/e"}}}, got["ImageConfigResponse"],
		"ImageConfig replaces the whole configuration")
	got = update(map[string]any{"ImageConfig": map[string]any{}})
	assert.NotContains(t, got, "ImageConfigResponse", "an empty ImageConfig clears it")
}

// TestLambdaFunctionFields_AChangePublishesANewVersion pins the fields' place in the version
// fingerprint: a change publishes a new version, and restating a default does not.
func TestLambdaFunctionFields_AChangePublishesANewVersion(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "fp", "print(1)", nil)
	require.Equal(t, "1", lambdaPublish(t, srv, "fp"))

	update := func(body map[string]any) {
		t.Helper()
		status, _, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/fp/configuration", body)
		require.Equal(t, http.StatusOK, status, raw)
	}
	update(map[string]any{
		"EphemeralStorage": map[string]any{"Size": 512},
		"LoggingConfig":    map[string]any{"LogFormat": "Text", "LogGroup": "/aws/lambda/fp"},
	})
	assert.Equal(t, "1", lambdaPublish(t, srv, "fp"), "restating the defaults changes nothing")

	for i, body := range []map[string]any{
		{"EphemeralStorage": map[string]any{"Size": 1024}},
		{"LoggingConfig": map[string]any{"LogGroup": "/custom"}},
		{"LoggingConfig": map[string]any{"LogFormat": "JSON"}},
	} {
		update(body)
		assert.Equal(t, []string{"2", "3", "4"}[i], lambdaPublish(t, srv, "fp"), "%v", body)
	}
	update(map[string]any{"LoggingConfig": map[string]any{"ApplicationLogLevel": "INFO", "SystemLogLevel": "INFO"}})
	assert.Equal(t, "4", lambdaPublish(t, srv, "fp"), "naming JSON's default levels changes nothing")

	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "fp-img", "Role": "arn:aws:iam::123456789012:role/r", "PackageType": "Image",
		"Code": map[string]any{"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/fp:1"},
	})
	require.Equal(t, http.StatusCreated, status, raw)
	require.Equal(t, "1", lambdaPublish(t, srv, "fp-img"))
	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/fp-img/configuration",
		map[string]any{"ImageConfig": map[string]any{"Command": []string{"x"}}})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "2", lambdaPublish(t, srv, "fp-img"), "an ImageConfig change publishes a new version")
}

// TestLambdaFunctionFields_ImageConfigOnAZipFunctionIsRefused pins the refusal Lambda answers for an
// ImageConfig on a function packaged as a .zip archive, on create and on update.
func TestLambdaFunctionFields_ImageConfigOnAZipFunctionIsRefused(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	const message = "The ImageConfig parameter is not supported for functions created with deployment packages."
	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "zip-img", "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/r", "ImageConfig": map[string]any{"Command": []string{"x"}},
	})
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Contains(t, raw, message)

	lambdaCreateZipFunction(t, srv, "zip", "print(1)", nil)
	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/zip/configuration",
		map[string]any{"ImageConfig": map[string]any{"WorkingDirectory": "/w"}})
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Contains(t, raw, message)
	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/zip/configuration",
		map[string]any{"ImageConfig": map[string]any{}})
	assert.Equal(t, http.StatusOK, status, "an empty ImageConfig overrides nothing: %s", raw)
}

// TestLambdaFunctionFields_ARecordWithoutThemRendersTheDefaults reads a function record an older
// build wrote, which carries none of the three fields.
func TestLambdaFunctionFields_ARecordWithoutThemRendersTheDefaults(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newLambdaTestServerWithState(t, state)
	legacy := `{"FunctionName":"old","FunctionArn":"arn:aws:lambda:us-east-1:123456789012:function:old",` +
		`"Runtime":"python3.12","Role":"arn:aws:iam::123456789012:role/r","Handler":"index.handler",` +
		`"Timeout":3,"MemorySize":128,"CodeSize":0,"CodeSha256":"","RevisionId":"r","State":"Active",` +
		`"PackageType":"Zip","LastModified":"2026-01-01T00:00:00Z","CreateDate":"2026-01-01T00:00:00Z"}`
	require.NoError(t, state.Put(context.Background(), "lambda", "function:123456789012/us-east-1/old", []byte(legacy)))

	status, got, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/old/configuration", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, map[string]any{"Size": float64(512)}, got["EphemeralStorage"])
	assert.Equal(t, map[string]any{"LogFormat": "Text", "LogGroup": "/aws/lambda/old"}, got["LoggingConfig"])
}
