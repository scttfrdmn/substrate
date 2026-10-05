package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Unsubscribe refuses a string that is not a subscription ARN with InvalidParameter/400, and a
// well-formed ARN naming no subscription with NotFound/404, in that order (#1259).
//
// API_Unsubscribe publishes both. Before #1259 the handler built a state key from whatever string
// arrived and reported NotFound for all of them, so a caller who passed a topic ARN (the plausible
// mistake, since it is a perfectly good SNS ARN) was told the subscription did not exist rather than
// that the ARN was not one. The two refusals are distinguishable now, which is the property under test.

// snsUnsubscribe sends one Unsubscribe and returns the status and the error code a refusal carries.
func snsUnsubscribe(t *testing.T, ts *emulator.TestServer, arn string) (int, string) {
	t.Helper()
	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":          "Unsubscribe",
		"SubscriptionArn": arn,
	})
	return status, errCode
}

func TestSNSUnsubscribe_AMalformedARNIsInvalidParameterAndAnAbsentOneIsNotFound(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "unsub-refusals")

	for _, tc := range []struct {
		name   string
		arn    string
		status int
		code   string
	}{
		// The case a consumer actually sends, so it is first.
		{"a topic ARN names no subscription", topicARN, http.StatusBadRequest, "InvalidParameter"},
		{"empty", "", http.StatusBadRequest, "InvalidParameter"},
		{"not an ARN", "unsub-refusals", http.StatusBadRequest, "InvalidParameter"},
		{"another service", "arn:aws:sqs:us-east-1:123456789012:queue:tail", http.StatusBadRequest, "InvalidParameter"},
		{"no subscription id", topicARN + ":", http.StatusBadRequest, "InvalidParameter"},
		{"well-formed, naming no subscription", topicARN + ":00000000000000000000000000000000", http.StatusNotFound, "NotFound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, code := snsUnsubscribe(t, ts, tc.arn)
			assert.Equal(t, tc.status, status, "Unsubscribe %q", tc.arn)
			assert.Equal(t, tc.code, code, "Unsubscribe %q", tc.arn)
		})
	}
}

// A real subscription still unsubscribes, and is then a well-formed ARN naming nothing.
func TestSNSUnsubscribe_ARealSubscriptionIsRemovedThenNotFound(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "unsub-real")
	require.True(t, strings.HasPrefix(subARN, "arn:aws:sns:"), "fixture subscription ARN %q", subARN)

	status, code := snsUnsubscribe(t, ts, subARN)
	require.Equal(t, http.StatusOK, status, "Unsubscribe of a real subscription: %s", code)

	_, getErr := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	assert.Equal(t, "NotFound", getErr, "the subscription is gone")

	status, code = snsUnsubscribe(t, ts, subARN)
	assert.Equal(t, http.StatusNotFound, status, "a second Unsubscribe")
	assert.Equal(t, "NotFound", code, "a second Unsubscribe names a subscription that no longer exists")
}
