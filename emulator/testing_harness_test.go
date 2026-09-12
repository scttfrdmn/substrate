package emulator_test

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What the test harness itself is not allowed to depend on (#798).
//
// A test server flaked once with
//
//	POST PutConfigurationRecorder: Post "http://localhost:59492/":
//	  read tcp [::1]:59530->[::1]:59492: read: connection reset by peer
//
// after a subtest that took 5.19s where its siblings took milliseconds. Substrate's
// premise is that a red test is a real signal rather than timing noise, so the harness
// is held to it: these cases assert the three properties whose absence made that
// failure possible and invisible.
//
// The diagnosis was confirmed while verifying an unrelated change, when the *sibling*
// test in the same file — TestConfigChannel_ThePolicyMatcherRefusesWhatAdmitsNothing —
// failed the same way with the same [::1] address. Nothing in either test is at fault:
// the shape belongs to the shared harness, which is why fixing it there answers the
// issue's "if the same shape exists in sibling tests, they are fixed with it".

func TestStartTestServer_IsReachedByAddressNotByName(t *testing.T) {
	// "localhost" has two answers on a dual-stack host, and net.Listen picks one family
	// for the bind while a client resolves the name for itself. The flake reported
	// [::1] on both ends, so it is not knowable from the message which server the
	// probe had reached. A literal address has one answer, and needs no resolver on a
	// path CLAUDE.md forbids from touching the network.
	ts := emulator.StartTestServer(t)

	require.True(t, strings.HasPrefix(ts.URL, "http://127.0.0.1:"),
		"a test server is dialed by address, not by name: %s", ts.URL)
	assert.Equal(t, "http://127.0.0.1:"+strconv.Itoa(ts.Port), ts.URL,
		"URL and Port describe the same listener")

	// And the listener really is on that family, rather than the URL merely claiming it.
	host, port, err := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	require.NoError(t, err)
	conn, err := net.Dial("tcp4", net.JoinHostPort(host, port))
	require.NoError(t, err, "the bound address must be reachable over IPv4")
	require.NoError(t, conn.Close())
}

func TestStartTestServer_PoolsNoConnectionThatCouldOutliveIt(t *testing.T) {
	// The mechanism behind the reset. A pooled connection to a server that has since
	// been shut down fails on *read* — after the request was written — which is exactly
	// "read: connection reset by peer"; and because ports are recycled inside one `go
	// test` process while every test server is torn down at the end of its test, the
	// failure lands in whichever later test drew the port rather than in the test that
	// leaked the connection. Closing server-side is what makes that impossible for
	// every caller at once, including the 149 that share http.DefaultClient.
	ts := emulator.StartTestServer(t)

	resp, err := http.Get(ts.URL + "/health") //nolint:noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.True(t, resp.Close,
		"the response must close its connection, so no client can pool it")

	// And on the wire, because Response.Close is net/http's own reading of it: Connection
	// is hop-by-hop, so the transport consumes the header and it never reaches
	// Response.Header. A raw socket sees what a client actually acts on — and sees the
	// server hang up, which is the property that makes a stale pooled connection
	// impossible rather than merely unlikely.
	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck
	_, err = conn.Write([]byte("GET /health HTTP/1.1\r\nHost: " +
		strings.TrimPrefix(ts.URL, "http://") + "\r\n\r\n"))
	require.NoError(t, err)

	raw, err := io.ReadAll(conn)
	require.NoError(t, err, "the server must close the connection, not leave it open")
	assert.Contains(t, strings.ToLower(string(raw)), "connection: close")
}

func TestStartTestServer_AnswersHealthBeforeItReturns(t *testing.T) {
	// The contract StartTestServer documents, and the one the discarded probe result
	// silently stopped honoring: the server is ready when the call returns. The probe
	// loop broke out on success and simply fell out of its 5s deadline on failure,
	// handing back a *TestServer either way — so a server that never came up produced
	// no message at all, and the first API call reported a transport error from
	// somewhere else. It now t.Fatalf's, which is defensive enough that there is no
	// way to provoke it from a test: the server is in-process and its listener is
	// already bound.
	ts := emulator.StartTestServer(t)

	resp, err := http.Get(ts.URL + "/health") //nolint:noctx
	require.NoError(t, err, "the first call after StartTestServer must not race the server")
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAwaitTestServer_ReportsWhyItGaveUp(t *testing.T) {
	// The defect itself, at the level it lives: the probe's answer must be an answer. The
	// loop this replaces broke out on success and fell out of its deadline on failure with
	// nothing to show for it, so a server that never came up was indistinguishable from one
	// that did until some later call failed naming the transport.
	//
	// StartTestServer cannot reach this — its server is in-process and its listener is
	// already bound — so the probe is called directly, against a port nothing is listening
	// on. The deadline is a parameter so this costs milliseconds rather than the 5s a test
	// server waits: a refused connection on loopback fails immediately, so the loop spins
	// and gives up at the deadline whatever the machine's speed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedURL := "http://" + ln.Addr().String()
	require.NoError(t, ln.Close())

	err = emulator.AwaitTestServerForTest(closedURL, 30*time.Millisecond)
	require.Error(t, err, "a server that never answers must be reported, not shrugged off")
	assert.Contains(t, err.Error(), closedURL, "the message names which server")
	assert.Contains(t, err.Error(), "30ms", "and how long it waited")
	assert.Contains(t, err.Error(), "/health", "and what it asked for")

	// The negative control, so the case above is not passing for want of a working probe.
	ts := emulator.StartTestServer(t)
	assert.NoError(t, emulator.AwaitTestServerForTest(ts.URL, 5*time.Second))
}

func TestStartTestServer_ManyServersInOneProcessDoNotInterfere(t *testing.T) {
	// The condition the flake needed, reproduced deliberately: many servers started
	// *and torn down* inside one process, so the OS recycles ports among them. Each
	// runs as a subtest so its t.Cleanup shuts the server down before the next starts —
	// which is precisely the sequence that used to leave a pooled connection pointing
	// at a dead listener. Interference surfaces as a transport error rather than a
	// status code, so require.NoError is the assertion that matters here.
	//
	// This cannot prove the absence of a race; it reproduces the *setup* the reported
	// failure needed, and each request goes through http.DefaultClient on purpose,
	// since that shared pool is where the stale connection used to live.
	for i := range 25 {
		t.Run("server "+strconv.Itoa(i), func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp, err := http.Get(ts.URL + "/health") //nolint:noctx
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}
