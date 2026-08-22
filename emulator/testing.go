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
	// URL is the base URL of the server, e.g. "http://localhost:54321".
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

// StartTestServer starts an in-process Substrate server on a random port,
// registers all default plugins, and schedules t.Cleanup to shut it down.
// The returned [TestServer] is ready to accept requests when this function
// returns.
//
// No [CredentialRegistry] is wired, so SigV4 signatures are not verified and any
// access key is accepted — every one of them, and an unsigned request too,
// resolving to account 123456789012. Use [StartTestServerWithAccounts] when a
// test needs to call as more than one account.
func StartTestServer(t *testing.T) *TestServer {
	t.Helper()
	return startTestServer(t, nil)
}

// StartTestServerWithAccounts starts a test server that can be called as any of
// the given accounts, in addition to the built-in 123456789012.
//
// Each account gets its own signing credential, readable with
// [TestServer.CredentialsFor]. Because the accounts are named up front, a test
// that needs to sign as an account Organizations vends — whose ID is not known
// until CreateAccount returns — should call [TestServer.RegisterAccount] instead.
//
// This is a separate entry point from [StartTestServer] rather than an option on
// it because wiring a registry also switches SigV4 verification on: the server
// verifies signatures exactly when [ServerOptions.Credentials] is non-nil, and an
// access key the registry does not hold is refused with InvalidClientTokenId 403.
// Every request made against the returned server must therefore be signed with one
// of these credentials, which is a stricter contract than [StartTestServer]'s and
// is why it is opt-in. Decoupling the two is #630.
func StartTestServerWithAccounts(t testing.TB, accounts ...string) *TestServer {
	t.Helper()

	registry := NewCredentialRegistry()
	ts := startTestServer(t, registry)
	for _, account := range accounts {
		ts.RegisterAccount(t, account)
	}
	return ts
}

// startTestServer is the shared body of [StartTestServer] and
// [StartTestServerWithAccounts]. A nil registry leaves SigV4 verification off,
// which is [StartTestServer]'s documented behavior.
func startTestServer(t testing.TB, creds *CredentialRegistry) *TestServer {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Server.Address = "localhost:0"
	// Enable the in-memory event store so cost summaries and recording/replay
	// work against the server out of the box (see TestServer.Store).
	cfg.EventStore.Enabled = true
	cfg.EventStore.Backend = "memory"
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
	ln, err := net.Listen("tcp", "localhost:0")
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

	srv := NewServer(*cfg, registry, store, state, tc, logger,
		ServerOptions{Fault: fault, Costs: costs, Auth: auth, Credentials: creds})

	srvCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(srvCtx, ln)
	}()

	// Wait until the health endpoint responds.
	baseURL := fmt.Sprintf("http://localhost:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, pingErr := http.Get(baseURL + "/health") //nolint:noctx
		if pingErr == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Stop(shutCtx)
		<-done
	})

	ts := &TestServer{URL: baseURL, Port: port, tc: tc, srv: srv, state: state, store: store, registry: registry}
	if creds != nil {
		ts.creds = map[string]CredentialEntry{}
		if entry, ok := creds.Lookup(defaultTestAccessKeyID); ok {
			ts.creds[entry.AccountID] = entry
		}
	}
	return ts
}

// RegisterAccount mints a signing credential for an account and returns it, so a
// test can call as an account whose ID it did not know at startup — an account
// Organizations vended, for one.
//
// The server must have been started by [StartTestServerWithAccounts]; a server with
// no credential registry has nowhere to put the entry, and registering one after
// the fact could not turn signature verification on for a server already running.
//
// The access key and secret are derived from the account ID rather than generated,
// so a recorded run replays with the same credentials in it. AWS access key IDs are
// 20 characters of uppercase alphanumerics; a 12-digit account ID padded after
// "AKIA" fits exactly, which keeps the key one that [VerifySigV4] and
// resolvePrincipal both accept.
func (ts *TestServer) RegisterAccount(t testing.TB, accountID string) CredentialEntry {
	t.Helper()
	if ts.creds == nil {
		t.Fatalf("RegisterAccount(%s): this server has no credential registry; start it with StartTestServerWithAccounts", accountID)
	}
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
func (ts *TestServer) ResetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/state/reset", "application/json", nil) //nolint:noctx
	if err != nil {
		t.Fatalf("ResetState: post: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ResetState: unexpected status %d", resp.StatusCode)
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
