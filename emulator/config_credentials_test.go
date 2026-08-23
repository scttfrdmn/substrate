package emulator_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// substrate.yaml's credentials: section is read (#736).
//
// It was documented from the beginning and Config had no field for it, so viper
// read the keys and nothing consumed them: a consumer who enabled it got no
// registry, no verification and no error. These tests cover the parse, the
// refusals Validate owes, and — the part that decides whether the section is
// actually wired — a server built from a config file behaving accordingly.

// credentialsYAML writes a substrate.yaml holding body and returns its path.
func credentialsYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "substrate.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// serverFromConfig builds a server wired the way cmd/substrate does, and returns
// the capture plugin a request lands on. The two ServerOptions fields below are
// the whole of that wiring, which is what makes this the behavior test #736
// asks for rather than a restatement of ToCredentialRegistry.
func serverFromConfig(t *testing.T, cfg *emulator.Config) (*emulator.Server, *principalCapturePlugin) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	capture := &principalCapturePlugin{}
	registry.Register(capture)
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	reg := cfg.Credentials.ToCredentialRegistry(cfg.Account.Default)
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{
		Credentials:      reg,
		VerifySignatures: reg != nil && cfg.Credentials.VerifySignatures,
	})
	return srv, capture
}

// signedDynamoCall issues the same request principalDynamoCall does, but signed
// for real, so a 200 means the signature verified rather than that verification
// was skipped.
func signedDynamoCall(t *testing.T, srv *emulator.Server, accessKey, secretKey string) *http.Response {
	t.Helper()
	const (
		host     = "dynamodb.us-east-1.amazonaws.com"
		dateTime = "20250101T120000Z"
	)
	body := []byte(`{}`)
	authHeader := computeSigV4Signature(t, http.MethodPost, "http://"+host+"/", body,
		accessKey, secretKey, "us-east-1", "dynamodb", dateTime)

	r := httptest.NewRequest(http.MethodPost, "http://"+host+"/", bytes.NewReader(body))
	r.Host = host
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	r.Header.Set("X-Amz-Date", dateTime)
	r.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func TestCredentialsCfg_Defaults(t *testing.T) {
	cfg := emulator.DefaultConfig()
	assert.False(t, cfg.Credentials.Enabled, "the section is off unless asked for")
	assert.True(t, cfg.Credentials.VerifySignatures,
		"enabled: true alone must keep meaning what the section documented")
	assert.Empty(t, cfg.Credentials.Entries)
}

func TestLoadConfig_CredentialsSection(t *testing.T) {
	path := credentialsYAML(t, `
credentials:
  enabled: true
  entries:
    - access_key_id: "AKIAEXAMPLE00000001"
      secret_access_key: "secret-one"
      account_id: "111122223333"
    - access_key_id: "AKIAEXAMPLE00000002"
      secret_access_key: "secret-two"
`)
	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)

	assert.True(t, cfg.Credentials.Enabled)
	assert.True(t, cfg.Credentials.VerifySignatures, "unset means the documented default")
	require.Len(t, cfg.Credentials.Entries, 2)
	assert.Equal(t, "AKIAEXAMPLE00000001", cfg.Credentials.Entries[0].AccessKeyID)
	assert.Equal(t, "secret-one", cfg.Credentials.Entries[0].SecretAccessKey)
	assert.Equal(t, "111122223333", cfg.Credentials.Entries[0].AccountID)
	assert.Empty(t, cfg.Credentials.Entries[1].AccountID, "optional, and adopts account.default")
}

func TestLoadConfig_CredentialsVerificationCanBeTurnedOff(t *testing.T) {
	// The combination #630 opened up. A viper default of true is easy to write in a
	// way that cannot be overridden back to false, so this is worth pinning.
	path := credentialsYAML(t, `
credentials:
  enabled: true
  verify_signatures: false
`)
	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)
	assert.True(t, cfg.Credentials.Enabled)
	assert.False(t, cfg.Credentials.VerifySignatures)
}

func TestCredentialsCfg_ToCredentialRegistry(t *testing.T) {
	t.Run("disabled builds nothing", func(t *testing.T) {
		cfg := emulator.CredentialsCfg{
			Entries: []emulator.CredentialEntryCfg{{AccessKeyID: "AKIAEXAMPLE00000001"}},
		}
		assert.Nil(t, cfg.ToCredentialRegistry("123456789012"),
			"a nil registry is how the server is told to attribute everyone to the default")
	})

	t.Run("entries and built-ins coexist", func(t *testing.T) {
		cfg := emulator.CredentialsCfg{
			Enabled: true,
			Entries: []emulator.CredentialEntryCfg{{
				AccessKeyID:     "AKIAEXAMPLE00000001",
				SecretAccessKey: "secret-one",
				AccountID:       "111122223333",
				SessionToken:    "token-one",
			}},
		}
		reg := cfg.ToCredentialRegistry("123456789012")
		require.NotNil(t, reg)

		entry, ok := reg.Lookup("AKIAEXAMPLE00000001")
		require.True(t, ok)
		assert.Equal(t, "111122223333", entry.AccountID)
		assert.Equal(t, "secret-one", entry.SecretAccessKey)
		assert.Equal(t, "token-one", entry.SessionToken)

		builtin, ok := reg.Lookup("AKIATEST12345678901")
		require.True(t, ok, "the built-ins are not displaced by a configured entry")
		assert.Equal(t, "123456789012", builtin.AccountID)
	})

	t.Run("an empty account_id adopts the default", func(t *testing.T) {
		cfg := emulator.CredentialsCfg{
			Enabled: true,
			Entries: []emulator.CredentialEntryCfg{{AccessKeyID: "AKIAEXAMPLE00000001"}},
		}
		entry, ok := cfg.ToCredentialRegistry("210987654321").Lookup("AKIAEXAMPLE00000001")
		require.True(t, ok)
		assert.Equal(t, "210987654321", entry.AccountID)
	})

	t.Run("a configured entry replaces a built-in", func(t *testing.T) {
		// The route to moving the built-in key into another account, and the reason
		// the built-ins are seeded first rather than last.
		cfg := emulator.CredentialsCfg{
			Enabled: true,
			Entries: []emulator.CredentialEntryCfg{{
				AccessKeyID:     "AKIATEST12345678901",
				SecretAccessKey: "another-secret",
				AccountID:       "111122223333",
			}},
		}
		entry, ok := cfg.ToCredentialRegistry("123456789012").Lookup("AKIATEST12345678901")
		require.True(t, ok)
		assert.Equal(t, "111122223333", entry.AccountID)
		assert.Equal(t, "another-secret", entry.SecretAccessKey)
	})
}

func TestCredentialsCfg_RegisterInto(t *testing.T) {
	// SIGHUP reload cannot swap the registry the server holds, so it adds to it.
	reg := emulator.NewCredentialRegistry()
	cfg := emulator.CredentialsCfg{
		Enabled: true,
		Entries: []emulator.CredentialEntryCfg{{AccessKeyID: "AKIARELOADED00000001"}},
	}
	cfg.RegisterInto(reg, "111122223333")

	entry, ok := reg.Lookup("AKIARELOADED00000001")
	require.True(t, ok)
	assert.Equal(t, "111122223333", entry.AccountID)

	assert.NotPanics(t, func() { cfg.RegisterInto(nil, "111122223333") },
		"the server may hold no registry at all")
}

func TestValidate_CredentialsEntries(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*emulator.Config)
		wantErr string
	}{
		{
			name: "empty access key id",
			mutate: func(c *emulator.Config) {
				c.Credentials.Enabled = true
				c.Credentials.Entries = []emulator.CredentialEntryCfg{
					{SecretAccessKey: "secret"},
				}
			},
			wantErr: "credentials.entries[0].access_key_id",
		},
		{
			name: "account id that is not an account id",
			mutate: func(c *emulator.Config) {
				c.Credentials.Enabled = true
				c.Credentials.Entries = []emulator.CredentialEntryCfg{
					{AccessKeyID: "AKIAEXAMPLE00000001", SecretAccessKey: "s", AccountID: "1234"},
				}
			},
			wantErr: "12 digits",
		},
		{
			name: "duplicate access key id",
			mutate: func(c *emulator.Config) {
				c.Credentials.Enabled = true
				c.Credentials.Entries = []emulator.CredentialEntryCfg{
					{AccessKeyID: "AKIAEXAMPLE00000001", SecretAccessKey: "s"},
					{AccessKeyID: "AKIAEXAMPLE00000001", SecretAccessKey: "t"},
				}
			},
			wantErr: "duplicates entries[0]",
		},
		{
			name: "missing secret while verifying",
			mutate: func(c *emulator.Config) {
				c.Credentials.Enabled = true
				c.Credentials.Entries = []emulator.CredentialEntryCfg{
					{AccessKeyID: "AKIAEXAMPLE00000001"},
				}
			},
			wantErr: "secret_access_key",
		},
		{
			name: "a malformed entry is refused even with the section off",
			mutate: func(c *emulator.Config) {
				c.Credentials.Entries = []emulator.CredentialEntryCfg{
					{AccessKeyID: "AKIAEXAMPLE00000001", AccountID: "not-an-account"},
				}
			},
			wantErr: "12 digits",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := emulator.DefaultConfig()
			tt.mutate(cfg)
			err := emulator.Validate(cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidate_CredentialsSecretOptionalWithoutVerification(t *testing.T) {
	// An entry that exists only to say which account a key belongs to has no
	// signature to satisfy, so requiring a secret would be noise.
	cfg := emulator.DefaultConfig()
	cfg.Credentials.Enabled = true
	cfg.Credentials.VerifySignatures = false
	cfg.Credentials.Entries = []emulator.CredentialEntryCfg{
		{AccessKeyID: "AKIAEXAMPLE00000001", AccountID: "111122223333"},
	}
	assert.NoError(t, emulator.Validate(cfg))
}

func TestConfig_CredentialsEnabled_AttributesAndEnforces(t *testing.T) {
	path := credentialsYAML(t, `
credentials:
  enabled: true
  entries:
    - access_key_id: "AKIAEXAMPLE00000001"
      secret_access_key: "secret-one"
      account_id: "111122223333"
`)
	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)
	srv, capture := serverFromConfig(t, cfg)

	// A key the file names, signed with the secret the file gives it, verifies and
	// resolves to the account the file gives it.
	resp := signedDynamoCall(t, srv, "AKIAEXAMPLE00000001", "secret-one")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, capture.called)
	assert.Equal(t, "111122223333", capture.accountID)

	// The same key with the wrong secret does not, which is what distinguishes
	// verification from a table lookup.
	srvBad, captureBad := serverFromConfig(t, cfg)
	respBad := signedDynamoCall(t, srvBad, "AKIAEXAMPLE00000001", "not-the-secret")
	bodyBad, err := io.ReadAll(respBad.Body)
	require.NoError(t, err)
	require.NoError(t, respBad.Body.Close())
	assert.Equal(t, http.StatusForbidden, respBad.StatusCode)
	assert.Contains(t, string(bodyBad), "SignatureDoesNotMatch")
	assert.False(t, captureBad.called)

	// A key it does not name is refused, which is the enforcement half.
	srv2, capture2 := serverFromConfig(t, cfg)
	resp2 := principalDynamoCall(t, srv2, "AKIANEVERREGISTERED1")
	require.NoError(t, resp2.Body.Close())
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode)
	assert.False(t, capture2.called)
}

func TestConfig_CredentialsEnabled_AcceptsTheDocumentedCredentials(t *testing.T) {
	// The reason NewCredentialRegistry seeds three keys. A consumer who follows
	// README.md, docs/endpoint-configuration.md or docs/testing-guide.md and then
	// enables this section must not start getting InvalidClientTokenId 403s.
	path := credentialsYAML(t, "credentials:\n  enabled: true\n")
	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)

	const exampleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	tests := []struct {
		accessKey, secretKey, source string
	}{
		{"test", "test", "README.md, docs/endpoint-configuration.md"},
		{"AKIAIOSFODNN7EXAMPLE", exampleSecret, "docs/testing-guide.md, test/e2e"},
		{"AKIATEST12345678901", exampleSecret, "NewCredentialRegistry's original entry"},
	}
	for _, tt := range tests {
		t.Run(tt.accessKey, func(t *testing.T) {
			srv, capture := serverFromConfig(t, cfg)
			resp := signedDynamoCall(t, srv, tt.accessKey, tt.secretKey)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusOK, resp.StatusCode, "documented in %s", tt.source)
			assert.True(t, capture.called)
		})
	}
}

func TestConfig_CredentialsWithoutVerification_AcceptsAnyKey(t *testing.T) {
	path := credentialsYAML(t, `
credentials:
  enabled: true
  verify_signatures: false
  entries:
    - access_key_id: "AKIAEXAMPLE00000001"
      account_id: "111122223333"
`)
	cfg, err := emulator.LoadConfig(path)
	require.NoError(t, err)

	srv, capture := serverFromConfig(t, cfg)
	resp := principalDynamoCall(t, srv, "AKIAEXAMPLE00000001")
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, capture.called)
	assert.Equal(t, "111122223333", capture.accountID, "attribution without authentication")

	srv2, capture2 := serverFromConfig(t, cfg)
	resp2 := principalDynamoCall(t, srv2, "AKIANEVERREGISTERED1")
	require.NoError(t, resp2.Body.Close())
	assert.Equal(t, http.StatusOK, resp2.StatusCode, "nothing is being enforced")
	require.True(t, capture2.called)
	assert.Equal(t, cfg.Account.Default, capture2.accountID)
}

func TestConfig_CredentialsDisabled_ChangesNothing(t *testing.T) {
	// The shipped default. Every caller lands in account.default and no signature
	// is checked, which is what every existing deployment sees.
	cfg := emulator.DefaultConfig()
	srv, capture := serverFromConfig(t, cfg)

	resp := principalDynamoCall(t, srv, "AKIANEVERREGISTERED1")
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, capture.called)
	assert.Equal(t, cfg.Account.Default, capture.accountID)
	assert.Nil(t, capture.principal)
}
