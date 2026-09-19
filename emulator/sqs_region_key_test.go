package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// One queue name is one queue per account **per Region** (#1088).
//
// The state key was `queue:<account>/<name>`, built by taking the last two components of a queue URL
// — which skips the Region, because a queue URL carries it in the host. So one name was one record
// across every Region, and the consequence was worse than colliding state: `CreateQueue` for a name
// another Region already held found that record, took its idempotent branch and **answered the other
// Region's URL**, so every later call the caller made addressed the wrong endpoint and nothing
// refused it.
//
// Two further defects had to move in the same commit, because the key change makes them incoherent
// rather than merely wrong. `queue_names` was one flat global key with no account and no Region in
// it, and `listQueues` filtered on `QueueNamePrefix` alone — it read neither `ctx.AccountID` nor
// `ctx.Region` — so `ListQueues` in one account already reported another account's queues, before
// anything about the Region was changed.
//
// Every assertion here goes over the wire through a Region-specific host, because the Region a
// handler sees comes from the endpoint (`extractRegion`) and that is precisely the value the old key
// discarded. A test calling `HandleRequest` with a hand-built context would assert the fix while
// bypassing the path a caller takes to it.

// sqsRegionAccount is the account every request in this file resolves to: the test server has no
// auth configured, so ParseAWSRequest falls back to it.
const sqsRegionAccount = "123456789012"

// sqsRegionRequest sends an SQS query-protocol request to the endpoint for one Region, and returns
// the status and body.
//
// The Region reaches the handler through the Host header alone, which is the whole point: an SQS
// endpoint is one Region's, `API_GetQueueUrl` publishes no Region parameter, and the host is where
// `extractRegion` looks first.
func sqsRegionRequest(t *testing.T, srv *emulator.Server, region, action string,
	params map[string]string,
) (int, string) {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	form.Set("Version", "2012-11-05")
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "sqs." + region + ".amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	return resp.StatusCode, readBody(t, resp)
}

// sqsRegionCreateQueue creates a queue through the given Region's endpoint and returns the URL that
// Region answered with.
func sqsRegionCreateQueue(t *testing.T, srv *emulator.Server, region, name string) string {
	t.Helper()
	status, body := sqsRegionRequest(t, srv, region, "CreateQueue", map[string]string{"QueueName": name})
	require.Equal(t, http.StatusOK, status, "CreateQueue in %s: %s", region, body)
	var out struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	require.NotEmpty(t, out.QueueURL, "CreateQueue returned no URL: %s", body)
	return out.QueueURL
}

// sqsRegionListQueues reports the queue URLs one Region's endpoint lists.
func sqsRegionListQueues(t *testing.T, srv *emulator.Server, region string) []string {
	t.Helper()
	status, body := sqsRegionRequest(t, srv, region, "ListQueues", nil)
	require.Equal(t, http.StatusOK, status, "ListQueues in %s: %s", region, body)
	var out struct {
		QueueURLs []string `xml:"ListQueuesResult>QueueUrl"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	return out.QueueURLs
}

// sqsRegionErrorCode reports the Code an error body carries, or "" for a body that is not one.
//
// JSON rather than XML even though the request was query-protocol: `errorProtocolFor` consults the
// service map before the Content-Type and sqs is registered as errProtoJSONRPC, so an SQS refusal is
// always a JSON document. See [TestSQS_QueueDoesNotExist_AllOperations], which records the same.
func sqsRegionErrorCode(t *testing.T, body string) string {
	t.Helper()
	var out struct {
		Code string `json:"Code"`
	}
	if json.Unmarshal([]byte(body), &out) != nil {
		return ""
	}
	return out.Code
}

// TestSQSRegionKey_OneNameInTwoRegionsIsTwoQueues is #1088's first criterion, and the defect at its
// sharpest: the second create answered the *first* Region's URL.
//
// A caller reading that URL then addressed `sqs.us-east-1...` for a queue it had asked eu-west-1
// for, and every operation on it succeeded against the wrong Region's record — the failure the
// assertion on the URL host is about, rather than the state key it comes from.
func TestSQSRegionKey_OneNameInTwoRegionsIsTwoQueues(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	east := sqsRegionCreateQueue(t, srv, "us-east-1", "orders")
	west := sqsRegionCreateQueue(t, srv, "eu-west-1", "orders")

	assert.NotEqual(t, east, west, "one name in two Regions must be two queues, not one")
	assert.Contains(t, east, "us-east-1", "the us-east-1 endpoint answers a us-east-1 URL")
	assert.Contains(t, west, "eu-west-1",
		"the eu-west-1 endpoint answered us-east-1's URL until #1088, because the existing record "+
			"for the name was found and the idempotent branch taken")
}

// TestSQSRegionKey_TheTwoQueuesHoldSeparateState pins that the two records are genuinely separate
// rather than two URLs over one document.
//
// Asserted through SetQueueAttributes and GetQueueAttributes rather than through state, because a
// shared record is only observable as one Region's write appearing in the other's read — which is
// what a consumer sees and what a state assertion would not distinguish from two records holding
// the same value by coincidence.
func TestSQSRegionKey_TheTwoQueuesHoldSeparateState(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	east := sqsRegionCreateQueue(t, srv, "us-east-1", "orders")
	west := sqsRegionCreateQueue(t, srv, "eu-west-1", "orders")

	status, body := sqsRegionRequest(t, srv, "us-east-1", "SetQueueAttributes", map[string]string{
		"QueueUrl":          east,
		"Attribute.1.Name":  "VisibilityTimeout",
		"Attribute.1.Value": "90",
	})
	require.Equal(t, http.StatusOK, status, "SetQueueAttributes: %s", body)

	readTimeout := func(region, queueURL string) string {
		t.Helper()
		st, b := sqsRegionRequest(t, srv, region, "GetQueueAttributes", map[string]string{
			"QueueUrl":        queueURL,
			"AttributeName.1": "VisibilityTimeout",
		})
		require.Equal(t, http.StatusOK, st, "GetQueueAttributes in %s: %s", region, b)
		var out struct {
			Attrs []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"GetQueueAttributesResult>Attribute"`
		}
		require.NoError(t, xml.Unmarshal([]byte(b), &out))
		for _, a := range out.Attrs {
			if a.Name == "VisibilityTimeout" {
				return a.Value
			}
		}
		return ""
	}

	assert.Equal(t, "90", readTimeout("us-east-1", east), "the Region that wrote it reads it back")
	assert.Equal(t, "30", readTimeout("eu-west-1", west),
		"the other Region keeps its own default, rather than seeing a write it never received")
}

// TestSQSRegionKey_ACrossRegionURLIsRefused is the case a Region parsed out of the URL host would
// get wrong, and the reason the Region comes from the request instead.
//
// Substrate's URL does carry the Region in its host, so a host parse is available — but it is a
// guess at the one value the endpoint already knows for certain, and it would silently serve the
// other Region's queue. Taking the Region from the request gives AWS's answer:
// `QueueDoesNotExist`, because from this endpoint's point of view there is no such queue.
func TestSQSRegionKey_ACrossRegionURLIsRefused(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	west := sqsRegionCreateQueue(t, srv, "eu-west-1", "only-in-west")

	status, body := sqsRegionRequest(t, srv, "us-east-1", "GetQueueAttributes", map[string]string{
		"QueueUrl":        west,
		"AttributeName.1": "All",
	})
	assert.Equal(t, http.StatusBadRequest, status, "body was %s", body)
	assert.Equal(t, "QueueDoesNotExist", sqsRegionErrorCode(t, body),
		"a queue URL from another Region names no queue at this endpoint")
}

// TestSQSRegionKey_ListQueuesIsRegionScoped is the half of the fix the issue does not mention and
// that the key change forces.
//
// `listQueues` read one flat `queue_names` key and filtered on `QueueNamePrefix` alone, so it
// reported every queue substrate had ever created in any Region. Once two Regions can hold one name
// that is not merely over-broad — it answers one endpoint with two URLs for the same name.
func TestSQSRegionKey_ListQueuesIsRegionScoped(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	east := sqsRegionCreateQueue(t, srv, "us-east-1", "orders")
	west := sqsRegionCreateQueue(t, srv, "eu-west-1", "orders")
	sqsRegionCreateQueue(t, srv, "eu-west-1", "shipments")

	assert.Equal(t, []string{east}, sqsRegionListQueues(t, srv, "us-east-1"),
		"us-east-1 lists its own one queue")
	assert.ElementsMatch(t, sqsRegionListQueues(t, srv, "eu-west-1"),
		[]string{west, sqsQueueURLFor("eu-west-1", "shipments")},
		"eu-west-1 lists its own two, and neither is us-east-1's same-named queue")
}

// TestSQSRegionKey_ListQueuesStillFiltersOnPrefixAndDropsADeletedQueue pins the two properties the
// removed `queue_names` index was carrying, now that the list comes from a prefix scan.
//
// The index had to be pruned on delete, and a missed prune left a URL in every caller's list. A
// scan cannot miss one — deleting the record is the whole of the removal — but the property is worth
// an assertion precisely because the code that used to guarantee it is gone.
func TestSQSRegionKey_ListQueuesStillFiltersOnPrefixAndDropsADeletedQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	orders := sqsRegionCreateQueue(t, srv, "us-east-1", "orders-live")
	sqsRegionCreateQueue(t, srv, "us-east-1", "shipments")

	status, body := sqsRegionRequest(t, srv, "us-east-1", "ListQueues",
		map[string]string{"QueueNamePrefix": "orders"})
	require.Equal(t, http.StatusOK, status, "ListQueues: %s", body)
	var out struct {
		QueueURLs []string `xml:"ListQueuesResult>QueueUrl"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	assert.Equal(t, []string{orders}, out.QueueURLs, "the prefix still selects one of the two")

	status, body = sqsRegionRequest(t, srv, "us-east-1", "DeleteQueue",
		map[string]string{"QueueUrl": orders})
	require.Equal(t, http.StatusOK, status, "DeleteQueue: %s", body)

	assert.NotContains(t, sqsRegionListQueues(t, srv, "us-east-1"), orders,
		"a deleted queue leaves the list, which the index needed a prune to achieve")
}

// sqsQueueURLFor spells the URL substrate answers for one queue, for the one assertion that needs a
// URL it did not receive from a create.
func sqsQueueURLFor(region, name string) string {
	return "http://sqs." + region + ".localhost/" + sqsRegionAccount + "/" + name
}
