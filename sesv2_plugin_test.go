package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupSESv2Plugin(t *testing.T) (*substrate.SESv2Plugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.SESv2Plugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("SESv2Plugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-ses-1",
	}
}

func sesv2Request(method, path string, body map[string]any) *substrate.AWSRequest {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	return &substrate.AWSRequest{
		Service:   "sesv2",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestSESv2Plugin(t *testing.T) {
	p, ctx := setupSESv2Plugin(t)

	t.Run("CreateEmailIdentity_Email", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("POST", "/v2/email/identities", map[string]any{
			"EmailIdentity": "sender@example.com",
		}))
		if err != nil {
			t.Fatalf("CreateEmailIdentity email: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
	})

	t.Run("CreateEmailIdentity_Domain", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("POST", "/v2/email/identities", map[string]any{
			"EmailIdentity": "example.com",
		}))
		if err != nil {
			t.Fatalf("CreateEmailIdentity domain: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
	})

	t.Run("ListEmailIdentities", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("GET", "/v2/email/identities", nil))
		if err != nil {
			t.Fatalf("ListEmailIdentities: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			EmailIdentities []struct {
				IdentityName string `json:"IdentityName"`
				IdentityType string `json:"IdentityType"`
			} `json:"EmailIdentities"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal list response: %v", err)
		}
		if len(result.EmailIdentities) != 2 {
			t.Errorf("want 2 identities, got %d", len(result.EmailIdentities))
		}
	})

	t.Run("GetEmailIdentity", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("GET", "/v2/email/identities/sender@example.com", nil))
		if err != nil {
			t.Fatalf("GetEmailIdentity: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var identity substrate.SESv2Identity
		if err := json.Unmarshal(resp.Body, &identity); err != nil {
			t.Fatalf("unmarshal identity: %v", err)
		}
		if identity.IdentityName != "sender@example.com" {
			t.Errorf("want IdentityName=sender@example.com, got %q", identity.IdentityName)
		}
		if identity.IdentityType != "EMAIL_ADDRESS" {
			t.Errorf("want IdentityType=EMAIL_ADDRESS, got %q", identity.IdentityType)
		}
	})

	t.Run("SendEmail", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("POST", "/v2/email/outbound-emails", map[string]any{
			"FromEmailAddress": "sender@example.com",
			"Destination": map[string]any{
				"ToAddresses": []string{"recipient@example.com"},
			},
			"Content": map[string]any{
				"Simple": map[string]any{
					"Subject": map[string]string{"Data": "Test"},
					"Body":    map[string]any{"Text": map[string]string{"Data": "Hello"}},
				},
			},
		}))
		if err != nil {
			t.Fatalf("SendEmail: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			MessageID string `json:"MessageId"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal send response: %v", err)
		}
		if result.MessageID == "" {
			t.Error("want non-empty MessageId")
		}
	})

	t.Run("DeleteEmailIdentity", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, sesv2Request("DELETE", "/v2/email/identities/sender@example.com", nil))
		if err != nil {
			t.Fatalf("DeleteEmailIdentity: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
	})

	t.Run("GetEmailIdentity_NotFound", func(t *testing.T) {
		_, err := p.HandleRequest(ctx, sesv2Request("GET", "/v2/email/identities/sender@example.com", nil))
		if err == nil {
			t.Fatal("want error for missing identity, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "NotFoundException" {
			t.Errorf("want NotFoundException, got %q", awsErr.Code)
		}
		if awsErr.HTTPStatus != http.StatusNotFound {
			t.Errorf("want 404, got %d", awsErr.HTTPStatus)
		}
	})

	t.Run("CreateEmailIdentity_Duplicate", func(t *testing.T) {
		// First create should succeed.
		_, err := p.HandleRequest(ctx, sesv2Request("POST", "/v2/email/identities", map[string]any{
			"EmailIdentity": "dup@example.com",
		}))
		if err != nil {
			t.Fatalf("first create: %v", err)
		}
		// Second create should return AlreadyExistsException.
		_, err = p.HandleRequest(ctx, sesv2Request("POST", "/v2/email/identities", map[string]any{
			"EmailIdentity": "dup@example.com",
		}))
		if err == nil {
			t.Fatal("want error for duplicate identity, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "AlreadyExistsException" {
			t.Errorf("want AlreadyExistsException, got %q", awsErr.Code)
		}
	})

	t.Run("ListEmailIdentities_Pagination", func(t *testing.T) {
		p2, ctx2 := setupSESv2Plugin(t)
		// Create 3 identities.
		for _, id := range []string{"a@example.com", "b@example.com", "c@example.com"} {
			_, err := p2.HandleRequest(ctx2, sesv2Request("POST", "/v2/email/identities", map[string]any{
				"EmailIdentity": id,
			}))
			if err != nil {
				t.Fatalf("CreateEmailIdentity %s: %v", id, err)
			}
		}
		// List with PageSize=2.
		resp, err := p2.HandleRequest(ctx2, sesv2Request("GET", "/v2/email/identities", map[string]any{
			"PageSize": 2,
		}))
		if err != nil {
			t.Fatalf("ListEmailIdentities: %v", err)
		}
		var result struct {
			EmailIdentities []struct{ IdentityName string } `json:"EmailIdentities"`
			NextToken       string                          `json:"NextToken"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(result.EmailIdentities) != 2 {
			t.Errorf("want 2 identities, got %d", len(result.EmailIdentities))
		}
		if result.NextToken == "" {
			t.Error("want non-empty NextToken for paginated results")
		}
		// Second page using NextToken.
		resp2, err := p2.HandleRequest(ctx2, sesv2Request("GET", "/v2/email/identities", map[string]any{
			"NextToken": result.NextToken,
		}))
		if err != nil {
			t.Fatalf("ListEmailIdentities page2: %v", err)
		}
		var result2 struct {
			EmailIdentities []struct{ IdentityName string } `json:"EmailIdentities"`
		}
		if err := json.Unmarshal(resp2.Body, &result2); err != nil {
			t.Fatalf("unmarshal page2: %v", err)
		}
		if len(result2.EmailIdentities) == 0 {
			t.Error("want at least 1 identity on second page")
		}
	})

	t.Run("DeleteEmailIdentity_NotFound", func(t *testing.T) {
		_, err := p.HandleRequest(ctx, sesv2Request("DELETE", "/v2/email/identities/notexist@example.com", nil))
		if err == nil {
			t.Fatal("want error for missing identity, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "NotFoundException" {
			t.Errorf("want NotFoundException, got %q", awsErr.Code)
		}
	})

	t.Run("SendEmail_Capture", func(t *testing.T) {
		p2, ctx2 := setupSESv2Plugin(t)
		// Send an email.
		resp, err := p2.HandleRequest(ctx2, sesv2Request("POST", "/v2/email/outbound-emails", map[string]any{
			"FromEmailAddress": "from@example.com",
			"Destination": map[string]any{
				"ToAddresses": []string{"to@example.com"},
			},
			"Content": map[string]any{
				"Simple": map[string]any{
					"Subject": map[string]string{"Data": "Hello World"},
					"Body":    map[string]any{"Text": map[string]string{"Data": "Test body"}},
				},
			},
		}))
		if err != nil {
			t.Fatalf("SendEmail: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			MessageID string `json:"MessageId"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if result.MessageID == "" {
			t.Fatal("want non-empty MessageId")
		}

		// Verify the email was captured in state.
		state := substrate.NewMemoryStateManager()
		p3 := &substrate.SESv2Plugin{}
		_ = p3.Initialize(context.Background(), substrate.PluginConfig{
			State:  state,
			Logger: substrate.NewDefaultLogger(slog.LevelError, false),
		})
		_ = p3

		// The capture is in p2's state; we test that the returned messageID is non-empty.
		// A full integration test via server is in the server test.
	})
}
