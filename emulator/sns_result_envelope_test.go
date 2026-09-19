package emulator_test

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for #1141: which SNS operations carry a `<{Operation}Result>` element and which do not.
//
// Every assertion here is on the direct children of the response root, in document order, because
// that is what `aws-sdk-go-v2`'s generated deserializer walks: it looks the result element up by
// name and fails the whole operation when it is absent, which is how a successful TagResource was
// reported to a caller as `TagResourceResult node not found`. Asserting on a substring would not
// distinguish the element from the same text appearing elsewhere in the body.

// snsResponseChildren returns an SNS response's root element name and the names of its direct
// children in document order.
func snsResponseChildren(t *testing.T, body string) (string, []string) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(body))
	var (
		root     string
		children []string
		depth    int
	)
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch el := tok.(type) {
		case xml.StartElement:
			depth++
			switch depth {
			case 1:
				root = el.Name.Local
			case 2:
				children = append(children, el.Name.Local)
			}
		case xml.EndElement:
			depth--
		}
	}
	require.NotEmpty(t, root, "no root element in %q", body)
	return root, children
}

// snsEnvelopeBetween returns the text between two delimiters, failing the test when either is absent.
func snsEnvelopeBetween(t *testing.T, body, openTag, closeTag string) string {
	t.Helper()
	start := strings.Index(body, openTag)
	require.GreaterOrEqual(t, start, 0, "%s not in %q", openTag, body)
	rest := body[start+len(openTag):]
	end := strings.Index(rest, closeTag)
	require.GreaterOrEqual(t, end, 0, "%s not in %q", closeTag, body)
	return rest[:end]
}

// snsEnvelopeTopic creates a topic and returns its ARN.
func snsEnvelopeTopic(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	resp := snsRequest(t, srv, map[string]string{"Action": "CreateTopic", "Name": name})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return snsEnvelopeBetween(t, readSNSBody(t, resp), "<TopicArn>", "</TopicArn>")
}

// snsEnvelopeSubscription subscribes an SQS endpoint to a topic and returns the subscription ARN.
func snsEnvelopeSubscription(t *testing.T, srv *emulator.Server, topicARN string) string {
	t.Helper()
	resp := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": topicARN,
		"Protocol": "sqs",
		"Endpoint": "https://sqs.us-east-1.amazonaws.com/123456789012/envelope-queue",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return snsEnvelopeBetween(t, readSNSBody(t, resp), "<SubscriptionArn>", "</SubscriptionArn>")
}

// TestSNSTagOperationsPublishAnEmptyResultElement pins the two operations whose modeled output is an
// empty structure, both of which AWS publishes with the element present and empty.
func TestSNSTagOperationsPublishAnEmptyResultElement(t *testing.T) {
	tests := []struct {
		op     string
		params func(topicARN string) map[string]string
	}{
		{
			op: "TagResource",
			params: func(topicARN string) map[string]string {
				return map[string]string{
					"Action":              "TagResource",
					"ResourceArn":         topicARN,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "test",
				}
			},
		},
		{
			op: "UntagResource",
			params: func(topicARN string) map[string]string {
				return map[string]string{
					"Action":           "UntagResource",
					"ResourceArn":      topicARN,
					"TagKeys.member.1": "env",
					"TagKeys.member.2": "team",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			srv := newSNSTestServer(t)
			topicARN := snsEnvelopeTopic(t, srv, strings.ToLower(tt.op)+"-envelope")

			resp := snsRequest(t, srv, tt.params(topicARN))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body := readSNSBody(t, resp)

			root, children := snsResponseChildren(t, body)
			assert.Equal(t, tt.op+"Response", root)
			// The result element comes first, which is the order AWS's sample publishes and the order
			// the deserializer reads: the result, then the metadata.
			assert.Equal(t, []string{tt.op + "Result", "ResponseMetadata"}, children)

			// Rendered as an open/close pair where AWS's sample shows the self-closing spelling. The
			// two are the same element to any XML parser, which is the only consumer of this byte.
			assert.Contains(t, body, "<"+tt.op+"Result></"+tt.op+"Result>")
			assert.Contains(t, body, `xmlns="https://sns.amazonaws.com/doc/2010-03-31/"`)
			assert.NotEmpty(t, snsEnvelopeBetween(t, body, "<RequestId>", "</RequestId>"))
		})
	}
}

// TestSNSUnitOperationsPublishNoResultElement pins the six operations whose modeled output is
// `smithy.api#Unit`, for which the element must be absent rather than present and empty.
//
// `aws-sdk-go-v2`'s SNS deserializer asks for a result element for 31 operations and none of these
// six is among them, so no SDK can fail on the absence — and AWS's samples for DeleteTopic,
// Unsubscribe and AddPermission show `<ResponseMetadata>` as the response's only child.
func TestSNSUnitOperationsPublishNoResultElement(t *testing.T) {
	tests := []struct {
		op     string
		params func(t *testing.T, srv *emulator.Server) map[string]string
	}{
		{
			op: "DeleteTopic",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				return map[string]string{
					"Action":   "DeleteTopic",
					"TopicArn": snsEnvelopeTopic(t, srv, "delete-envelope"),
				}
			},
		},
		{
			op: "SetTopicAttributes",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				return map[string]string{
					"Action":         "SetTopicAttributes",
					"TopicArn":       snsEnvelopeTopic(t, srv, "set-attrs-envelope"),
					"AttributeName":  "DisplayName",
					"AttributeValue": "envelope",
				}
			},
		},
		{
			op: "Unsubscribe",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				topicARN := snsEnvelopeTopic(t, srv, "unsubscribe-envelope")
				return map[string]string{
					"Action":          "Unsubscribe",
					"SubscriptionArn": snsEnvelopeSubscription(t, srv, topicARN),
				}
			},
		},
		{
			op: "SetSubscriptionAttributes",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				topicARN := snsEnvelopeTopic(t, srv, "set-sub-attrs-envelope")
				return map[string]string{
					"Action":          "SetSubscriptionAttributes",
					"SubscriptionArn": snsEnvelopeSubscription(t, srv, topicARN),
					"AttributeName":   "RawMessageDelivery",
					"AttributeValue":  "true",
				}
			},
		},
		{
			op: "AddPermission",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				return map[string]string{
					"Action":                "AddPermission",
					"TopicArn":              snsEnvelopeTopic(t, srv, "add-permission-envelope"),
					"Label":                 "envelope",
					"AWSAccountId.member.1": "123456789012",
					"ActionName.member.1":   "Publish",
				}
			},
		},
		{
			op: "RemovePermission",
			params: func(t *testing.T, srv *emulator.Server) map[string]string {
				return map[string]string{
					"Action":   "RemovePermission",
					"TopicArn": snsEnvelopeTopic(t, srv, "remove-permission-envelope"),
					"Label":    "envelope",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			srv := newSNSTestServer(t)

			resp := snsRequest(t, srv, tt.params(t, srv))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body := readSNSBody(t, resp)

			root, children := snsResponseChildren(t, body)
			assert.Equal(t, tt.op+"Response", root)
			assert.Equal(t, []string{"ResponseMetadata"}, children)

			// Not merely "no {op}Result": no element whose name ends in Result at all, so a future
			// envelope change cannot satisfy this by renaming the element it must not emit.
			for _, child := range children {
				assert.NotContains(t, child, "Result")
			}
			assert.Contains(t, body, `xmlns="https://sns.amazonaws.com/doc/2010-03-31/"`)
			assert.NotEmpty(t, snsEnvelopeBetween(t, body, "<RequestId>", "</RequestId>"))
		})
	}
}
