package substrate_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

// TestSQSFIFO_CreateAndSend creates a FIFO queue and sends a message with a
// MessageGroupId, which is required for FIFO queues.
func TestSQSFIFO_CreateAndSend(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	resp := sqsRequest(t, srv, map[string]string{
		"Action":            "CreateQueue",
		"QueueName":         "test-queue.fifo",
		"Attribute.1.Name":  "FifoQueue",
		"Attribute.1.Value": "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue FIFO: got %d", resp.StatusCode)
	}

	type createQueueResult struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	var cqr createQueueResult
	if err := xml.NewDecoder(resp.Body).Decode(&cqr); err != nil {
		t.Fatalf("decode CreateQueue: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	if cqr.QueueURL == "" {
		t.Fatal("expected non-empty QueueURL")
	}

	// Send WITH MessageGroupId and MessageDeduplicationId → success.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":                 "SendMessage",
		"QueueUrl":               cqr.QueueURL,
		"MessageBody":            "hello-fifo",
		"MessageGroupId":         "group1",
		"MessageDeduplicationId": "unique-id-1",
	})
	if resp2.StatusCode != http.StatusOK {
		body := readBody(t, resp2)
		t.Fatalf("SendMessage FIFO with group: got %d body: %s", resp2.StatusCode, body)
	}
	resp2.Body.Close() //nolint:errcheck
}

// TestSQSFIFO_MissingGroupID verifies that sending to a FIFO queue without a
// MessageGroupId returns an error.
func TestSQSFIFO_MissingGroupID(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	resp := sqsRequest(t, srv, map[string]string{
		"Action":            "CreateQueue",
		"QueueName":         "no-group.fifo",
		"Attribute.1.Name":  "FifoQueue",
		"Attribute.1.Value": "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue: got %d", resp.StatusCode)
	}
	var cqr struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	xml.NewDecoder(resp.Body).Decode(&cqr) //nolint:errcheck
	resp.Body.Close()                      //nolint:errcheck

	// Send without MessageGroupId → error.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    cqr.QueueURL,
		"MessageBody": "missing-group",
	})
	if resp2.StatusCode == http.StatusOK {
		t.Fatal("expected error for FIFO send without MessageGroupId, got 200")
	}
	resp2.Body.Close() //nolint:errcheck
}

// TestSQSFIFO_Deduplication verifies that sending the same
// MessageDeduplicationId twice within the deduplication window returns the
// same MessageId without enqueuing a second message.
func TestSQSFIFO_Deduplication(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	resp := sqsRequest(t, srv, map[string]string{
		"Action":            "CreateQueue",
		"QueueName":         "dedup-queue.fifo",
		"Attribute.1.Name":  "FifoQueue",
		"Attribute.1.Value": "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue: got %d", resp.StatusCode)
	}
	var cqr struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	xml.NewDecoder(resp.Body).Decode(&cqr) //nolint:errcheck
	resp.Body.Close()                      //nolint:errcheck

	send := func(body, dedupID string) string {
		r := sqsRequest(t, srv, map[string]string{
			"Action":                 "SendMessage",
			"QueueUrl":               cqr.QueueURL,
			"MessageBody":            body,
			"MessageGroupId":         "g1",
			"MessageDeduplicationId": dedupID,
		})
		if r.StatusCode != http.StatusOK {
			t.Fatalf("SendMessage: got %d body: %s", r.StatusCode, readBody(t, r))
		}
		var sm struct {
			MessageID string `xml:"SendMessageResult>MessageId"`
		}
		xml.NewDecoder(r.Body).Decode(&sm) //nolint:errcheck
		r.Body.Close()                     //nolint:errcheck
		return sm.MessageID
	}

	id1 := send("first-body", "dup-token")
	id2 := send("second-body", "dup-token")

	if id1 == "" {
		t.Fatal("expected non-empty MessageId from first send")
	}
	if id2 != id1 {
		t.Errorf("expected same MessageId on duplicate, first=%s second=%s", id1, id2)
	}

	// Only one message should be in the queue.
	rr := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            cqr.QueueURL,
		"MaxNumberOfMessages": "10",
	})
	if rr.StatusCode != http.StatusOK {
		t.Fatalf("ReceiveMessage: got %d", rr.StatusCode)
	}
	body := readBody(t, rr)
	count := strings.Count(body, "<Message>")
	if count != 1 {
		t.Errorf("expected 1 message in queue after dedup, found %d <Message> elements in: %s", count, body)
	}
}
