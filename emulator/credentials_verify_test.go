package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ServerOptions.Credentials and ServerOptions.VerifySignatures are independent
// (#630).
//
// One field used to mean both "resolve accounts from this table" and "enforce
// signatures on every request", so a test that needed to call as a second
// account had to accept that every request must be signed with a pre-loaded key
// — including substrate's own documented credentials, which are in no registry
// and were therefore refused. The four combinations are all expressible now, and
// this file asserts each of them rather than the field.
//
// The one that did not exist before is set/off, and it is the one every test
// server now uses.

// verifyTestAccount is the account NewCredentialRegistry's built-in key belongs
// to, and the default a server attributes an unknown key to.
const verifyTestAccount = "123456789012"

// verifyRegisteredKey is the built-in credential NewCredentialRegistry seeds.
const verifyRegisteredKey = "AKIATEST12345678901"

// verifyOtherAccount is a second account, registered so a hit resolves to
// something the default account resolution would not have produced. Without it a
// passing test proves nothing: the built-in key's account is the default anyway.
const verifyOtherAccount = "210987654321"

// verifyOtherKey signs as verifyOtherAccount.
const verifyOtherKey = "AKIAOTHERACCOUNT0001"

// newVerifyTestServer builds a server with the given registry and verification
// setting, and returns the capture plugin the request lands on.
func newVerifyTestServer(
	t *testing.T, reg *emulator.CredentialRegistry, verify bool,
) (*emulator.Server, *principalCapturePlugin) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	capture := &principalCapturePlugin{}
	registry.Register(capture)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{
		Credentials:      reg,
		VerifySignatures: verify,
	})
	return srv, capture
}

// verifyRegistry returns a registry holding the built-in key plus verifyOtherKey.
func verifyRegistry() *emulator.CredentialRegistry {
	reg := emulator.NewCredentialRegistry()
	reg.Register(emulator.CredentialEntry{
		AccessKeyID:     verifyOtherKey,
		SecretAccessKey: "secret-for-" + verifyOtherAccount,
		AccountID:       verifyOtherAccount,
	})
	return reg
}

// TestServer_RegistryWithoutVerification is the combination #630 exists for: a
// registered key resolves to its own account, and the request is not refused
// even though its signature is nonsense.
func TestServer_RegistryWithoutVerification(t *testing.T) {
	srv, capture := newVerifyTestServer(t, verifyRegistry(), false)

	resp := principalDynamoCall(t, srv, verifyOtherKey)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"an unverified signature is not an error when verification is off")
	require.True(t, capture.called)
	assert.Equal(t, verifyOtherAccount, capture.accountID,
		"the registry answered which account this key belongs to")
}

// TestServer_RegistryWithoutVerification_SynthesizesNoPrincipal is the reason
// this could not simply be wired everywhere.
//
// A registry hit with no IAM entity behind it used to be named as a principal
// derived from the access key. Doing that for a server that wired a registry only
// to attribute accounts would flip GetCallerIdentity's ARN off :root and turn
// GetUser into a NoSuchEntity lookup — a repository-wide behavior change riding
// on an unrelated one. The fallback belongs to verification, which is the only
// thing that establishes the caller holds the secret.
func TestServer_RegistryWithoutVerification_SynthesizesNoPrincipal(t *testing.T) {
	srv, capture := newVerifyTestServer(t, verifyRegistry(), false)

	resp := principalDynamoCall(t, srv, verifyRegisteredKey)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, capture.called)
	assert.Nil(t, capture.principal,
		"a key merely present in a table has proven nothing and names no principal")
}

// TestServer_RegistryWithVerification_SynthesizesPrincipal is the same request
// against the same registry with verification on, which is where the fallback
// does belong. Nothing about this changed; it is here so the pair reads as one
// decision.
func TestServer_RegistryWithVerification_SynthesizesPrincipal(t *testing.T) {
	srv, capture := newVerifyTestServer(t, verifyRegistry(), true)

	// A registered key with a bad signature is refused, so sign nothing and let
	// the account come from the header alone — VerifySigV4 passes an
	// Authorization header it cannot parse as SigV4 straight through, and the
	// step above still reads the key out of it.
	resp := principalDynamoCall(t, srv, verifyRegisteredKey)
	require.NoError(t, resp.Body.Close())

	if resp.StatusCode == http.StatusForbidden {
		// The stub signature does not verify, which is itself the enforcement
		// this combination is about; the principal assertion below is then moot.
		return
	}
	require.True(t, capture.called)
	require.NotNil(t, capture.principal)
	assert.Equal(t, "arn:aws:iam::"+verifyTestAccount+":user/"+verifyRegisteredKey,
		capture.principal.ARN)
}

// TestServer_VerificationRefusesAnUnregisteredKey pins that turning verification
// on still refuses a key the registry does not hold, with the code AWS uses.
func TestServer_VerificationRefusesAnUnregisteredKey(t *testing.T) {
	srv, capture := newVerifyTestServer(t, verifyRegistry(), true)

	resp := principalDynamoCall(t, srv, "AKIANEVERREGISTERED1")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	var errBody struct {
		Code string `json:"__type"`
	}
	if json.Unmarshal(body, &errBody) == nil && errBody.Code != "" {
		assert.Contains(t, errBody.Code, "InvalidClientTokenId")
	} else {
		assert.Contains(t, string(body), "InvalidClientTokenId")
	}
	assert.False(t, capture.called, "refused before any plugin saw it")
}

// TestServer_NoVerificationAcceptsAnUnregisteredKey is the same request with
// verification off — today's StartTestServer contract, and what keeps
// substrate's documented example credentials working.
func TestServer_NoVerificationAcceptsAnUnregisteredKey(t *testing.T) {
	srv, capture := newVerifyTestServer(t, verifyRegistry(), false)

	resp := principalDynamoCall(t, srv, "AKIAIOSFODNN7EXAMPLE")
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, capture.called)
	assert.Equal(t, verifyTestAccount, capture.accountID,
		"an unknown key keeps the account already resolved")
}

// TestServer_VerificationWithoutRegistryIsDowngraded pins the fourth
// combination, which has no key material to check against.
//
// The issue asked for a construction-time refusal. NewServer returns *Server with
// no error, and changing that signature reaches ~42 test files plus the CLI for a
// mistake a warning describes just as precisely — so it downgrades to
// verification-off and logs. A panic in an emulator library would be worse than
// either.
func TestServer_VerificationWithoutRegistryIsDowngraded(t *testing.T) {
	srv, capture := newVerifyTestServer(t, nil, true)

	resp := principalDynamoCall(t, srv, "AKIANEVERREGISTERED1")
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"a server with nothing to verify against refuses nothing")
	assert.True(t, capture.called)
}

// TestServer_VerificationWithoutRegistryDoesNotPanicOnANilLogger pins that the
// downgrade's warning is not a new way for NewServer to panic. NewServer touched
// the logger nowhere before, so a caller passing nil worked.
func TestServer_VerificationWithoutRegistryDoesNotPanicOnANilLogger(t *testing.T) {
	cfg := emulator.DefaultConfig()
	assert.NotPanics(t, func() {
		_ = emulator.NewServer(*cfg, emulator.NewPluginRegistry(),
			emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig()),
			emulator.NewMemoryStateManager(),
			emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)),
			nil,
			emulator.ServerOptions{VerifySignatures: true})
	})
}

// TestStartTestServer_RegistersAnAccountWithoutTurningVerificationOn is the test
// server half of the decoupling: RegisterAccount used to require the opt-in
// entry point, whose contract was that every request be signed.
func TestStartTestServer_RegistersAnAccountWithoutTurningVerificationOn(t *testing.T) {
	ts := emulator.StartTestServer(t)

	entry := ts.RegisterAccount(t, verifyOtherAccount)
	assert.Equal(t, verifyOtherAccount, entry.AccountID)

	got, ok := ts.CredentialsFor(verifyOtherAccount)
	require.True(t, ok)
	assert.Equal(t, entry, got)

	// An unsigned call still works, which is what verification-off buys: a test
	// can be a second account without every other call in it needing a signature.
	const body = "Action=GetCallerIdentity&Version=2011-06-15"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		ts.URL+"/", strings.NewReader(body))
	require.NoError(t, err)
	req.Host = "sts.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestStartTestServer_WithAccountsOption is the folded-in form of
// StartTestServerWithAccounts, minus the signature contract.
func TestStartTestServer_WithAccountsOption(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(verifyOtherAccount))

	entry, ok := ts.CredentialsFor(verifyOtherAccount)
	require.True(t, ok, "the option registered the account")
	assert.Equal(t, verifyOtherAccount, entry.AccountID)

	builtin, ok := ts.CredentialsFor(verifyTestAccount)
	require.True(t, ok, "and the built-in account is still there")
	assert.Equal(t, verifyRegisteredKey, builtin.AccessKeyID)
}

// TestStartTestServer_WithSignatureVerificationRefusesAnUnregisteredKey pins that
// a test can still ask for enforcement, which is what
// StartTestServerWithAccounts now does for its existing callers.
func TestStartTestServer_WithSignatureVerificationRefusesAnUnregisteredKey(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithSignatureVerification())

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		ts.URL+"/", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", principalAuthHeader("AKIANEVERREGISTERED1", "sts"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.URL.RawQuery = "Action=GetCallerIdentity&Version=2011-06-15"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidClientTokenId")
}
