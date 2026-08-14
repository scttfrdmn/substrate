// This file contains runnable "journey" tests that demonstrate Substrate's
// differentiating features end-to-end with the real AWS SDK v2:
//
//   - journey_throttling_test.go — seed a throttling fault, verify SDK retries
//   - journey_replay_test.go     — record a session and replay it deterministically
//   - journey_cost_test.go       — assert a workload stays within a cost budget
//   - journey_timetravel_test.go — advance the simulated clock and observe state
//
// Each journey starts its own in-process server via emulator.StartTestServer so
// it reads as a self-contained example a user can copy. They double as CI
// coverage that the documented workflows actually compile and run.
package e2e_test

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// journeyConfig builds an AWS SDK config pointed at a per-journey TestServer.
// It uses the built-in test credentials, whose account is journeyAccountID.
func journeyConfig(ts *emulator.TestServer) (aws.Config, error) {
	return config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(
			// An "AKIA…" access key resolves to the well-known test account
			// (journeyAccountID); other keys resolve to the zero account.
			credentials.NewStaticCredentialsProvider("AKIATEST12345678901", "test", ""),
		),
		config.WithHTTPClient(&http.Client{}),
	)
}

// journeyAccountID is the account the built-in AKIA test credentials map to.
const journeyAccountID = "123456789012"

// journeyConfigAs builds an SDK config that signs as one specific account of a
// server started with [emulator.StartTestServerWithAccounts].
//
// Such a server verifies SigV4 — it holds a credential registry, and the server
// verifies signatures exactly when one is present — so the credentials have to be
// the ones the registry vended rather than the built-in test key. That is what
// makes a journey through two identities possible at all: the caller's account is
// resolved from the signing key, so a member account is a genuinely different
// caller rather than a parameter.
func journeyConfigAs(ts *emulator.TestServer, accountID string) (aws.Config, error) {
	entry, ok := ts.CredentialsFor(accountID)
	if !ok {
		return aws.Config{}, fmt.Errorf("account %s is not registered with this test server", accountID)
	}
	return config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(entry.AccessKeyID, entry.SecretAccessKey, ""),
		),
		config.WithHTTPClient(&http.Client{}),
	)
}
