package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"testing"
	"time"
)

// TestServer is a running Substrate server for use in integration tests.
// Create one with [StartTestServer]; it is automatically shut down when the
// test ends via t.Cleanup.
type TestServer struct {
	// URL is the base URL of the server, e.g. "http://127.0.0.1:54321". The host is
	// the literal address rather than the name "localhost"; see [testServerHost].
	URL string
	// Port is the TCP port the server is listening on.
	Port     int
	tc       *TimeController
	srv      *Server
	state    StateManager
	store    *EventStore
	registry *PluginRegistry
	creds    map[string]CredentialEntry
}

// Store returns the [EventStore] backing the server, for cost summaries
// ([EventStore.GetCostSummary]) and recording/replay. The store is enabled by
// [StartTestServer], so operations issued against [TestServer.URL] are recorded.
func (ts *TestServer) Store() *EventStore { return ts.store }

// StateManager returns the [StateManager] backing the server, for tests that
// need to seed or inspect resource state directly.
func (ts *TestServer) StateManager() StateManager { return ts.state }

// TimeController returns the [TimeController] driving the server's simulated
// clock. Prefer [TestServer.AdvanceTime] / [TestServer.SetTime] for common cases.
func (ts *TestServer) TimeController() *TimeController { return ts.tc }

// Registry returns the [PluginRegistry] of registered service plugins.
func (ts *TestServer) Registry() *PluginRegistry { return ts.registry }

// AuthController returns the [AuthController] the server authorizes requests
// against, so a replay of the server's own stream can be given the same one.
//
// A replay re-decides authorization ([ReplayPipeline]), and it can only reach the
// recorded answer if it is handed the controller the recording used — the
// controller reads policies out of the [StateManager], and pointing it at a
// different one is what would make a replay disagree with its recording for a
// reason that is not a divergence (#833).
func (ts *TestServer) AuthController() *AuthController { return ts.srv.opts.Auth }

// FaultController returns the [FaultController] wired into the server, which is
// disabled until a rule is armed through POST /v1/fault/rules or
// [FaultController.UpdateConfig].
//
// It is exposed both for [FaultController.FaultsFired] — a rule that matched
// nothing looks exactly like a consumer's retry working — and so that a replay of
// the server's stream can be given the same controller, which rewinds itself to
// its armed state at the start of a replay (#833).
func (ts *TestServer) FaultController() *FaultController { return ts.srv.opts.Fault }

// TestServerOption configures a server started by [StartTestServer].
type TestServerOption func(*testServerConfig)

// testServerConfig collects the options a [TestServerOption] sets.
type testServerConfig struct {
	accounts          []string
	verifySignatures  bool
	recordBodies      bool
	recordStateHashes bool
}

// WithAccounts makes the server callable as each of the given accounts, in
// addition to the built-in 123456789012.
//
// Each account gets its own signing credential, readable with
// [TestServer.CredentialsFor]. Because the accounts are named up front, a test
// that needs to sign as an account Organizations vends — whose ID is not known
// until CreateAccount returns — should call [TestServer.RegisterAccount] instead,
// which works on any test server.
func WithAccounts(accounts ...string) TestServerOption {
	return func(c *testServerConfig) { c.accounts = append(c.accounts, accounts...) }
}

// WithSignatureVerification enforces SigV4 on the test server, so a request
// signed with an access key no account here holds is refused with
// InvalidClientTokenId 403.
//
// Off by default, because attributing accounts and enforcing signatures are
// separate questions (#630) and only the first is what a multi-account test
// needs. With verification on, every signed request must use a credential from
// [TestServer.CredentialsFor] — including substrate's own documented example
// keys, which belong to no registry and are therefore refused.
func WithSignatureVerification() TestServerOption {
	return func(c *testServerConfig) { c.verifySignatures = true }
}

// WithRecordedBodies makes the event store capture each request and response body
// on the events it records, so the stream can be replayed.
//
// Off by default, matching [EventStoreConfig.IncludeBodies], because a body is the
// largest thing an event carries and most tests only count events or read costs
// off them. But a recorded event with no request **cannot be re-executed**:
// [ReplayEngine] skips it, and before #833 counted the skip as a successful
// replay — so a test that records against a default test server and then replays
// verifies nothing at all. Any test about replay needs this.
func WithRecordedBodies() TestServerOption {
	return func(c *testServerConfig) { c.recordBodies = true }
}

// WithRecordedStateHashes makes the event store record a hash of emulator state
// before and after each request, which [ReplayConfig.ValidateState] compares a
// replay against.
//
// Off by default, matching [EventStoreConfig.IncludeStateHashes], because it takes
// a full state snapshot twice per request.
//
// A stream that contains a create will report a `state_hash_after` difference on
// replay, and that is the honest answer rather than a defect: substrate mints most
// identifiers from crypto/rand, so the minted value differs between the recording
// and the replay and lands in the state it hashes (#856).
func WithRecordedStateHashes() TestServerOption {
	return func(c *testServerConfig) { c.recordStateHashes = true }
}

// StartTestServer starts an in-process Substrate server on a random port,
// registers all default plugins, and schedules t.Cleanup to shut it down.
// The returned [TestServer] is ready to accept requests when this function
// returns.
//
// SigV4 signatures are not verified unless [WithSignatureVerification] is
// passed, so any access key is accepted — every one of them, and an unsigned
// request too. A key the server holds a credential for resolves to that
// credential's account; every other key, and an unsigned request, resolves to
// 123456789012. Name further accounts with [WithAccounts] or
// [TestServer.RegisterAccount].
func StartTestServer(t testing.TB, opts ...TestServerOption) *TestServer {
	t.Helper()

	var cfg testServerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	ts := startTestServer(t, cfg)
	for _, account := range cfg.accounts {
		ts.RegisterAccount(t, account)
	}
	return ts
}

// StartTestServerWithAccounts starts a test server callable as each of the given
// accounts with SigV4 verification on.
//
// It is a thin wrapper over [StartTestServer] and exists only because it
// predates the options; prefer StartTestServer with [WithAccounts], and add
// [WithSignatureVerification] if the test is about signatures rather than about
// accounts. Verification is on here so this entry point keeps the stricter
// contract its callers were written against.
func StartTestServerWithAccounts(t testing.TB, accounts ...string) *TestServer {
	t.Helper()
	return StartTestServer(t, WithAccounts(accounts...), WithSignatureVerification())
}

// testServerHost is the literal address a test server binds and is dialed on.
//
// The literal, not the name "localhost", because a name has two answers on a
// dual-stack host: net.Listen picks *one* family for the wildcard-free bind while
// the client resolves the name independently, so a probe and the test that follows
// it could disagree about which server they were talking to. The flake in #798
// reported [::1] on both ends, which is only visible at all because the address is
// in the error. It also removes a resolver call from a code path CLAUDE.md forbids
// from depending on the network.
const testServerHost = "127.0.0.1"

// testServerProbeTimeout bounds one health-check attempt.
//
// The probe was untimed, on http.DefaultClient, whose Timeout is 0. Because the
// listener is opened before the serving goroutine starts, an attempt lands in the
// kernel's accept backlog and blocks rather than failing — so a single attempt
// could consume the entire deadline instead of retrying, which is how #798's
// subtest reached 5.19s (#798).
const testServerProbeTimeout = 250 * time.Millisecond

// testServerProbeDeadline bounds the whole startup wait.
const testServerProbeDeadline = 5 * time.Second

// awaitTestServer blocks until the server behind baseURL answers /health, and reports
// why it gave up if it never does.
//
// Reporting the failure at all is the point. The loop this replaces discarded its
// probe's result: it broke out on success and simply fell out of the deadline
// otherwise, returning a *TestServer either way — so a server that never came up
// produced no message, and the first API call reported a confusing transport error from
// somewhere else entirely (#798). Returning the error rather than failing the test here
// keeps testing.TB out of the probe, which is what lets the give-up path be tested.
//
// The client is dedicated and pools nothing. On http.DefaultClient the probe left a
// keep-alive connection in a pool shared by 149 call sites across 54 test files,
// keyed by host and port — and ports are recycled within one `go test` process while
// every test server is shut down at the end of its test.
//
// deadline bounds the whole wait; [testServerProbeDeadline] is what a test server uses.
func awaitTestServer(baseURL string, deadline time.Duration) error {
	client := &http.Client{
		Timeout:   testServerProbeTimeout,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	giveUpAt := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(giveUpAt) {
		resp, pingErr := client.Get(baseURL + "/health") //nolint:noctx
		if pingErr == nil {
			_ = resp.Body.Close()
			return nil
		}
		lastErr = pingErr
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("%s did not answer /health within %s: %w", baseURL, deadline, lastErr)
}

// startTestServer is the shared body of [StartTestServer] and
// [StartTestServerWithAccounts]. A [CredentialRegistry] is always wired, so
// [TestServer.RegisterAccount] works on every test server; whether signatures
// are enforced is the caller's separate choice (#630).
func startTestServer(t testing.TB, tsCfg testServerConfig) *TestServer {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Server.Address = testServerHost + ":0"
	// Enable the in-memory event store so cost summaries and recording/replay
	// work against the server out of the box (see TestServer.Store).
	cfg.EventStore.Enabled = true
	cfg.EventStore.Backend = "memory"
	// Both default to false and are opt-in per test: a body is the largest thing an
	// event carries, and a state hash is a full snapshot taken twice per request.
	// See [WithRecordedBodies] for why a replay test must set the first.
	cfg.EventStore.IncludeBodies = tsCfg.recordBodies
	cfg.EventStore.IncludeStateHashes = tsCfg.recordStateHashes
	cfg.Log.Level = "error"

	state := NewMemoryStateManager()
	tc := NewTimeController(time.Now())
	registry := NewPluginRegistry()
	logger := NewDefaultLogger(slog.LevelError, false)

	store := NewEventStore(cfg.EventStore.ToEventStoreConfig(), WithTimeController(tc))

	// An IAM-enforcing composition test is the reason the authorization chain
	// exists, and TestServer is how a consumer writes one, so a test server
	// authorizes: a call made with a key minted by CreateAccessKey, or with STS
	// session credentials, is evaluated against that principal's policies, and a
	// stack's resource calls are evaluated against its service role or its creator.
	//
	// This changes nothing for a test that does not create an IAM principal.
	// Enforcement keys off whether the caller resolves to an IAM entity present in
	// state, so an unsigned request, substrate's documented credentials, and any
	// key that was never minted here are all unenforced.
	auth := NewAuthController(state, logger, WithAuthTimeController(tc))

	ctx := context.Background()
	if err := RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil,
		WithPluginAuth(auth)); err != nil {
		t.Fatalf("StartTestServer: register plugins: %v", err)
	}

	// Bind to a random port and keep the listener open to avoid the TOCTOU race
	// between port reservation and server bind.
	ln, err := net.Listen("tcp", testServerHost+":0")
	if err != nil {
		t.Fatalf("StartTestServer: listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	// Wire a FaultController (disabled by default) so tests can seed fault rules
	// via POST /v1/fault/rules without extra setup. A fixed seed keeps
	// probabilistic rules deterministic.
	fault := NewFaultController(FaultConfig{Enabled: false}, 1)

	// Wire a CostController so recorded events carry cost data and
	// ts.Store().GetCostSummary returns non-zero costs out of the box.
	costs := NewCostController(CostConfig{Enabled: true})

	// A registry is wired unconditionally so RegisterAccount has somewhere to put
	// an entry, which is what lets one test server serve both the single- and the
	// multi-account case. It changes nothing for a test that does not register an
	// account: the only key in it is the built-in one, whose account is the
	// default anyway, and with verification off a registry hit synthesizes no
	// principal — see step 1.5 in [Server.handleAWSRequest].
	creds := NewCredentialRegistry()

	srv := NewServer(*cfg, registry, store, state, tc, logger, ServerOptions{
		Fault: fault, Costs: costs, Auth: auth,
		Credentials:      creds,
		VerifySignatures: tsCfg.verifySignatures,
		// No connection to this server is ever pooled, so none can be reused after
		// the test that created it tears it down and the port is recycled (#798).
		DisableKeepAlives: true,
	})

	srvCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(srvCtx, ln)
	}()

	baseURL := fmt.Sprintf("http://%s:%d", testServerHost, port)
	if err := awaitTestServer(baseURL, testServerProbeDeadline); err != nil {
		// Defensive: the server is in-process and its listener is already bound, so
		// reaching here means it never began serving. Fail now rather than hand back a
		// *TestServer whose every call fails for a reason that names the transport.
		t.Fatalf("StartTestServer: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Stop(shutCtx)
		<-done
	})

	ts := &TestServer{
		URL: baseURL, Port: port, tc: tc, srv: srv,
		state: state, store: store, registry: registry,
		creds: map[string]CredentialEntry{},
	}
	if entry, ok := creds.Lookup(defaultTestAccessKeyID); ok {
		ts.creds[entry.AccountID] = entry
	}
	return ts
}

// RegisterAccount mints a signing credential for an account and returns it, so a
// test can call as an account whose ID it did not know at startup — an account
// Organizations vended, for one.
//
// This works on any server [StartTestServer] returns, since every one of them
// wires a registry. It does not turn signature verification on; that is
// [WithSignatureVerification] and must be decided before the server starts.
//
// The access key and secret are derived from the account ID rather than generated,
// so a recorded run replays with the same credentials in it. AWS access key IDs are
// 20 characters of uppercase alphanumerics; a 12-digit account ID padded after
// "AKIA" fits exactly, which keeps the key one that [VerifySigV4] and
// resolvePrincipal both accept.
func (ts *TestServer) RegisterAccount(t testing.TB, accountID string) CredentialEntry {
	t.Helper()
	if existing, ok := ts.creds[accountID]; ok {
		return existing
	}
	if len(accountID) != 12 {
		t.Fatalf("RegisterAccount(%q): an AWS account ID is 12 digits", accountID)
	}
	entry := CredentialEntry{
		AccessKeyID:     "AKIA" + accountID + "TEST",
		SecretAccessKey: "substrate-secret-" + accountID,
		AccountID:       accountID,
	}
	ts.srv.opts.Credentials.Register(entry)
	ts.creds[accountID] = entry
	return entry
}

// CredentialsFor returns the signing credential for an account registered with
// this server, and whether one exists.
func (ts *TestServer) CredentialsFor(accountID string) (CredentialEntry, bool) {
	entry, ok := ts.creds[accountID]
	return entry, ok
}

// ResetState wipes all server state. Call this between test cases that share
// a single [TestServer] instance to avoid state leaking across cases.
//
// That covers what a plugin holds on itself as well as the [StateManager]'s
// contents: minted-identifier counters (#886), S3's object payloads (#902), and the
// Lambda event-source-mapping pollers, warm Lambda containers and RDS containers a
// previous case started (#903). A server built by [StartTestServer] has nothing left
// over afterwards; the only thing a reset does not clear is a filesystem an
// in-process embedder injected into the S3 plugin, which substrate does not own.
//
// The parameter is [testing.TB], not *testing.T, so a benchmark can reset
// between iterations — the last of the harness entry points to widen (#605).
func (ts *TestServer) ResetState(tb testing.TB) {
	tb.Helper()
	resp, err := http.Post(ts.URL+"/v1/state/reset", "application/json", nil) //nolint:noctx
	if err != nil {
		tb.Fatalf("ResetState: post: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("ResetState: unexpected status %d", resp.StatusCode)
	}
}

// AdvanceTime moves the simulated clock forward by d. Useful for triggering
// time-dependent logic such as TTL expiry, alert thresholds, or cost accrual
// without waiting for wall time to pass.
func (ts *TestServer) AdvanceTime(d time.Duration) {
	ts.tc.SetTime(ts.tc.Now().Add(d))
}

// SetTime sets the simulated clock to an absolute timestamp. Useful for
// establishing a known starting point before running time-sensitive tests.
func (ts *TestServer) SetTime(t time.Time) {
	ts.tc.SetTime(t)
}

// SetScale sets the time acceleration factor. A scale of 1.0 is real-time;
// 3600.0 makes one real second equal one simulated hour. Use together with
// [TestServer.AdvanceTime] or [TestServer.SetTime] to drive time-dependent
// code paths.
func (ts *TestServer) SetScale(scale float64) {
	ts.tc.SetScale(scale)
}

// FreezeTime stops the simulated clock where it stands, so every value a handler
// renders from it is the same instant however long the test takes.
//
// This is what a test asserting an exact timestamp needs, and the reason it is named
// rather than expressed as a scale of zero: `CLAUDE.md` rules out a test depending on
// wall-clock time, and a clock advancing at any nonzero scale makes a rendered second
// depend on how fast the machine ran (#1217). [TestServer.AdvanceTime] and
// [TestServer.SetTime] still move a frozen clock, which is how a frozen test drives a
// time-dependent path — deliberately, by a known interval.
func (ts *TestServer) FreezeTime() {
	ts.tc.Freeze()
}

// FreezeTimeAt stops the simulated clock at exactly t, so every value rendered from
// the clock is t until [TestServer.UnfreezeTime].
//
// This exists because the obvious spelling — SetTime then FreezeTime — stops the clock
// a few tens of nanoseconds *after* t, since FreezeTime stops it where it currently
// reads. That is invisible until something renders it, and then it is a second
// boundary crossed for no reason a reader of the test could see, which is the class of
// defect #1217 was.
func (ts *TestServer) FreezeTimeAt(t time.Time) {
	ts.tc.Freeze()
	ts.tc.SetTime(t)
}

// UnfreezeTime resumes the simulated clock from the instant it was frozen at.
func (ts *TestServer) UnfreezeTime() {
	ts.tc.Unfreeze()
}

// TimeFrozen reports whether the simulated clock is stopped.
func (ts *TestServer) TimeFrozen() bool {
	return ts.tc.Frozen()
}

// seedSSMRegion is the default AWS region used for seeded SSM parameters.
const seedSSMRegion = "us-east-1"

// SeedSSMParameter pre-populates an SSM Parameter Store path with a string
// value, bypassing the HTTP layer. This is useful for seeding public AWS SSM
// paths used for AMI discovery (e.g. /aws/service/canonical/... or
// /aws/service/ami-amazon-linux-latest/...) so that code under test can resolve
// AMI IDs without requiring a real AWS account or additional client setup.
//
// Parameters are stored under account 123456789012 and region us-east-1, which
// are the defaults used by the built-in test credentials. The value is stored
// as a String parameter at version 1. Call this before running the code under
// test; call [TestServer.ResetState] between test cases to clear seeded values.
func (ts *TestServer) SeedSSMParameter(name, value string) {
	if ts.state == nil {
		return
	}
	// Ensure name starts with /.
	if len(name) > 0 && name[0] != '/' {
		name = "/" + name
	}

	ctx := context.Background()
	param := &SSMParameter{
		Name:             name,
		Type:             "String",
		Value:            value,
		Version:          1,
		LastModifiedDate: ts.tc.Now(),
		AccountID:        defaultAccountID,
		Region:           seedSSMRegion,
		ARN:              ssmParameterARN(seedSSMRegion, defaultAccountID, name),
	}
	data, err := json.Marshal(param)
	if err != nil {
		return
	}
	stateKey := "parameter:" + defaultAccountID + "/" + seedSSMRegion + "/" + name
	_ = ts.state.Put(ctx, ssmNamespace, stateKey, data)

	// Update the paths index.
	pathsKey := "parameter_paths:" + defaultAccountID + "/" + seedSSMRegion
	existing, _ := ts.state.Get(ctx, ssmNamespace, pathsKey)
	var paths []string
	if existing != nil {
		_ = json.Unmarshal(existing, &paths)
	}
	// Add name if not already present.
	found := false
	for _, p := range paths {
		if p == name {
			found = true
			break
		}
	}
	if !found {
		paths = append(paths, name)
		sort.Strings(paths)
		if pathsData, err := json.Marshal(paths); err == nil {
			_ = ts.state.Put(ctx, ssmNamespace, pathsKey, pathsData)
		}
	}
}

// SeedSSMParameters pre-populates multiple SSM Parameter Store paths in a
// single call. It is a convenience wrapper around [TestServer.SeedSSMParameter].
// The params map keys are parameter names and values are their string values.
func (ts *TestServer) SeedSSMParameters(params map[string]string) {
	for name, value := range params {
		ts.SeedSSMParameter(name, value)
	}
}

// SeedEC2Image registers an AMI owned by the seeding account, bypassing the HTTP layer, so a
// test can launch from an image ID it chose.
//
// RunInstances refuses an AMI it cannot resolve (#733), which leaves a test that needs a
// specific ID three options: name a bundled AMI through [BundledImageID], call RegisterImage,
// or call this. It exists for the third case — a caller-owned image, or an ID a fixture
// already hardcodes — and writes the same record RegisterImage would, in state, so
// DescribeImages lists it and an Owners=self describe matches it. A bundled AMI is *not* in
// state, which is the difference between the two.
//
// The image is stored under account 123456789012 and the region the built-in test credentials
// use, in state "available". Call [TestServer.ResetState] between cases to clear it.
func (ts *TestServer) SeedEC2Image(imageID, name string) {
	if ts.state == nil {
		return
	}

	img := EC2Image{
		ImageID:      imageID,
		Name:         name,
		Description:  name,
		State:        "available",
		CreationDate: ts.tc.Now().UTC().Format(time.RFC3339),
		AccountID:    defaultAccountID,
		Region:       seedSSMRegion,
	}
	data, err := json.Marshal(img)
	if err != nil {
		return
	}
	_ = ts.state.Put(context.Background(), ec2Namespace,
		ec2ImageStateKey(defaultAccountID, seedSSMRegion, imageID), data)
}
