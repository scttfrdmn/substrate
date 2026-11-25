package emulator_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// sesFaultPlugins builds SES v1 and SES v2 over one faulting store, as the registry wires them,
// and seeds a domain and an address carrying every v1 attribute, plus one recorded message.
func sesFaultPlugins(t *testing.T) (*emulator.SESPlugin, *emulator.SESv2Plugin, *cfFaultStateManager, *emulator.RequestContext) {
	t.Helper()
	state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	opts := map[string]any{"time_controller": emulator.NewTimeController(time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC))}
	v1 := &emulator.SESPlugin{}
	v2 := &emulator.SESv2Plugin{}
	for _, p := range []emulator.Plugin{v1, v2} {
		require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
			State: state, Logger: emulator.NewDefaultLogger(slog.LevelError, false), Options: opts,
		}))
	}
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ses-fault",
		IDs: emulator.NewIDMint("req-ses-fault")}
	for _, call := range []map[string]string{
		{"Action": "VerifyDomainIdentity", "Domain": "example.com"},
		{"Action": "VerifyDomainDkim", "Domain": "example.com"},
		{"Action": "SetIdentityMailFromDomain", "Identity": "example.com", "MailFromDomain": "m.example.com"},
		{"Action": "VerifyEmailIdentity", "EmailAddress": "a@example.com"},
		{"Action": "SendEmail", "Source": "a@example.com", "Destination.ToAddresses.member.1": "b@example.com", "Message.Subject.Data": "s"},
	} {
		_, err := v1.HandleRequest(ctx, &emulator.AWSRequest{Service: "ses", Operation: call["Action"], Params: call})
		require.NoError(t, err, call["Action"])
	}
	return v1, v2, state, ctx
}

// TestSES_StoreFaultsSurfaceAsInternalErrors drives each operation against a store that fails
// the key kind it reads or writes, and asserts the failure comes back as an error carrying no
// AWS code — never as a published refusal, and never swallowed into a success.
func TestSES_StoreFaultsSurfaceAsInternalErrors(t *testing.T) {
	ids := map[string]string{"Identities.member.1": "example.com", "Identities.member.2": "a@example.com"}
	tests := []struct {
		name   string
		fault  func(*cfFaultStateManager)
		action string
		params map[string]string
	}{
		{"verify reads the record", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "VerifyDomainIdentity", map[string]string{"Domain": "new.example"}},
		{"verify writes the record", func(s *cfFaultStateManager) { s.failPut = "identity:" }, "VerifyEmailIdentity", map[string]string{"EmailAddress": "n@example.com"}},
		{"verify writes the index", func(s *cfFaultStateManager) { s.failPut = "identity_names:" }, "VerifyDomainIdentity", map[string]string{"Domain": "new.example"}},
		{"dkim writes the record", func(s *cfFaultStateManager) { s.failPut = "identity:" }, "VerifyDomainDkim", map[string]string{"Domain": "example.com"}},
		{"a corrupt record", func(s *cfFaultStateManager) { s.corruptGet = "identity:" }, "GetIdentityVerificationAttributes", ids},
		{"verification reads the seed", func(s *cfFaultStateManager) { s.failGet = "verification-seed:" }, "GetIdentityVerificationAttributes", ids},
		{"dkim reads the seed", func(s *cfFaultStateManager) { s.failGet = "verification-seed:" }, "GetIdentityDkimAttributes", ids},
		{"dkim reads the record", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "GetIdentityDkimAttributes", ids},
		{"mail from reads the seed", func(s *cfFaultStateManager) { s.failGet = "verification-seed:" }, "GetIdentityMailFromDomainAttributes", ids},
		{"mail from reads the record", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "GetIdentityMailFromDomainAttributes", ids},
		{"notifications read the record", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "GetIdentityNotificationAttributes", ids},
		{"list reads the index", func(s *cfFaultStateManager) { s.failGet = "identity_names:" }, "ListIdentities", nil},
		{"verified list reads the index", func(s *cfFaultStateManager) { s.failGet = "identity_names:" }, "ListVerifiedEmailAddresses", nil},
		{"verified list reads the seed", func(s *cfFaultStateManager) { s.failGet = "verification-seed:" }, "ListVerifiedEmailAddresses", nil},
		{"delete removes the record", func(s *cfFaultStateManager) { s.failDelete = "identity:" }, "DeleteIdentity", map[string]string{"Identity": "example.com"}},
		{"delete updates the index", func(s *cfFaultStateManager) { s.failPut = "identity_names:" }, "DeleteIdentity", map[string]string{"Identity": "example.com"}},
		{"a set reads the record", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "SetIdentityFeedbackForwardingEnabled", map[string]string{"Identity": "example.com", "ForwardingEnabled": "true"}},
		{"a set writes the record", func(s *cfFaultStateManager) { s.failPut = "identity:" }, "SetIdentityDkimEnabled", map[string]string{"Identity": "example.com", "DkimEnabled": "true"}},
		{"an address's DKIM reads its domain", func(s *cfFaultStateManager) { s.failGet = "identity:" }, "SetIdentityDkimEnabled", map[string]string{"Identity": "a@example.com", "DkimEnabled": "true"}},
		{"send writes the message", func(s *cfFaultStateManager) { s.failPut = "captured_email:" }, "SendRawEmail", map[string]string{"RawMessage.Data": "eA=="}},
		{"send writes the message index", func(s *cfFaultStateManager) { s.failPut = "captured_email_ids:" }, "SendEmail",
			map[string]string{"Source": "a@example.com", "Destination.ToAddresses.member.1": "b@example.com", "Message.Subject.Data": "s"}},
		{"quota reads the message index", func(s *cfFaultStateManager) { s.failGet = "captured_email_ids:" }, "GetSendQuota", nil},
		{"statistics read a message", func(s *cfFaultStateManager) { s.failGet = "captured_email:" }, "GetSendStatistics", nil},
		{"statistics read a corrupt message", func(s *cfFaultStateManager) { s.corruptGet = "captured_email:" }, "GetSendStatistics", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v1, _, state, ctx := sesFaultPlugins(t)
			tc.fault(state)
			params := map[string]string{"Action": tc.action}
			for k, v := range tc.params {
				params[k] = v
			}
			_, err := v1.HandleRequest(ctx, &emulator.AWSRequest{Service: "ses", Operation: tc.action, Params: params})
			require.Error(t, err)
			var awsErr *emulator.AWSError
			assert.False(t, errors.As(err, &awsErr), "a store fault must not read as an AWS refusal: %v", err)
			if state.corruptGet == "" {
				assert.ErrorIs(t, err, errCFFaultStore)
			}
		})
	}

	for name, fault := range map[string]func(*cfFaultStateManager){
		"SES v2 SendEmail writes the message":       func(s *cfFaultStateManager) { s.failPut = "captured_email:" },
		"SES v2 SendEmail writes the message index": func(s *cfFaultStateManager) { s.failPut = "captured_email_ids:" },
	} {
		t.Run(name, func(t *testing.T) {
			_, v2, state, ctx := sesFaultPlugins(t)
			fault(state)
			_, err := v2.HandleRequest(ctx, &emulator.AWSRequest{Service: "sesv2", HTTPMethod: "POST", Operation: "POST",
				Path: "/v2/email/outbound-emails", Params: map[string]string{},
				Body: []byte(`{"FromEmailAddress":"a@example.com","Destination":{"ToAddresses":["b@example.com"]},"Content":{"Simple":{"Subject":{"Data":"s"},"Body":{"Text":{"Data":"x"}}}}}`)})
			assert.ErrorIs(t, err, errCFFaultStore, "a failed capture is an error, not a logged success")
		})
	}

	t.Run("SES v2 GetEmailIdentity reads the shared seed", func(t *testing.T) {
		_, v2, state, ctx := sesFaultPlugins(t)
		state.failGet = "verification-seed:"
		_, err := v2.HandleRequest(ctx, &emulator.AWSRequest{Service: "sesv2", HTTPMethod: "GET", Operation: "GET",
			Path: "/v2/email/identities/example.com", Params: map[string]string{}})
		assert.ErrorIs(t, err, errCFFaultStore)
	})

	t.Run("Shutdown", func(t *testing.T) {
		v1, _, _, _ := sesFaultPlugins(t)
		assert.NoError(t, v1.Shutdown(context.Background()))
	})
}
