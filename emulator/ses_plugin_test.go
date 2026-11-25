package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// sesAuth signs a request with the SES signing name, which is what boto3, the AWS CLI and the
// Terraform provider put in the credential scope when pointed at a custom endpoint.
const sesAuth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20261124/us-east-1/ses/aws4_request, " +
	"SignedHeaders=host;x-amz-date, Signature=0000"

// sesCall sends one SES v1 Query request as the SDKs do — a form POST to / — and returns the
// status and body.
func sesCall(t *testing.T, ts *emulator.TestServer, action string, params map[string]string) (int, []byte) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2010-12-01"}}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Authorization", sesAuth)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// sesOK is sesCall for a request that must succeed, decoding the result into out when it is
// non-nil.
func sesOK(t *testing.T, ts *emulator.TestServer, action string, params map[string]string, out any) []byte {
	t.Helper()
	status, body := sesCall(t, ts, action, params)
	require.Equal(t, http.StatusOK, status, "%s: %s", action, body)
	if out != nil {
		require.NoError(t, xml.Unmarshal(body, out), "%s: %s", action, body)
	}
	return body
}

// sesErrorCode asserts a request fails with status and returns its Query error code.
func sesErrorCode(t *testing.T, ts *emulator.TestServer, action string, params map[string]string, status int) string {
	t.Helper()
	got, body := sesCall(t, ts, action, params)
	require.Equal(t, status, got, "%s: %s", action, body)
	var doc struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), "%s: %s", action, body)
	return doc.Error.Code
}

// sesSeed posts or deletes an identity-verification seed.
func sesSeed(t *testing.T, ts *emulator.TestServer, method, query, body string) int {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+"/v1/ses/identity-verification"+query, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// sesV2Get reads an identity through SES v2's GetEmailIdentity.
func sesV2Get(t *testing.T, ts *emulator.TestServer, identity string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v2/email/identities/"+identity, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", sesAuth)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(body, &doc), "%s", body)
	return resp.StatusCode, doc
}

type sesVerificationResp struct {
	Entries []struct {
		Key   string `xml:"key"`
		Value struct {
			VerificationStatus string `xml:"VerificationStatus"`
			VerificationToken  string `xml:"VerificationToken"`
		} `xml:"value"`
	} `xml:"GetIdentityVerificationAttributesResult>VerificationAttributes>entry"`
}

func sesVerification(t *testing.T, ts *emulator.TestServer, identities ...string) sesVerificationResp {
	t.Helper()
	params := map[string]string{}
	for i, id := range identities {
		params["Identities.member."+string(rune('1'+i))] = id
	}
	var out sesVerificationResp
	sesOK(t, ts, "GetIdentityVerificationAttributes", params, &out)
	return out
}

// TestSES_TerraformDomainIdentity mirrors aws_ses_domain_identity and
// aws_ses_domain_identity_verification: VerifyDomainIdentity, then a poll of
// GetIdentityVerificationAttributes until the status is Success, then DeleteIdentity.
func TestSES_TerraformDomainIdentity(t *testing.T) {
	ts := emulator.StartTestServer(t)

	var created struct {
		Token     string `xml:"VerifyDomainIdentityResult>VerificationToken"`
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}
	body := sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, &created)
	assert.Contains(t, string(body), `xmlns="http://ses.amazonaws.com/doc/2010-12-01/"`)
	assert.Len(t, created.Token, 44, "a 32-byte base64 token, the TXT value of _amazonses.example.com")
	assert.NotEmpty(t, created.RequestID)

	var again struct {
		Token string `xml:"VerifyDomainIdentityResult>VerificationToken"`
	}
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, &again)
	assert.Equal(t, created.Token, again.Token, "verifying a held domain again answers the same token")

	got := sesVerification(t, ts, "example.com", "absent.example.org")
	require.Len(t, got.Entries, 1, "an identity the account does not hold is left out of the map")
	assert.Equal(t, "example.com", got.Entries[0].Key)
	assert.Equal(t, "Success", got.Entries[0].Value.VerificationStatus, "verification defaults to Success")
	assert.Equal(t, created.Token, got.Entries[0].Value.VerificationToken)

	// The verification resource's wait loop, driven through the seed.
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"example.com":"Pending"}`))
	assert.Equal(t, "Pending", sesVerification(t, ts, "example.com").Entries[0].Value.VerificationStatus)
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodDelete, "?identity=example.com", ""))
	assert.Equal(t, "Success", sesVerification(t, ts, "example.com").Entries[0].Value.VerificationStatus)

	sesOK(t, ts, "DeleteIdentity", map[string]string{"Identity": "example.com"}, nil)
	assert.Empty(t, sesVerification(t, ts, "example.com").Entries, "a deleted identity is gone from the map")
	// Deleting it again succeeds: the page publishes no error for an absent identity.
	sesOK(t, ts, "DeleteIdentity", map[string]string{"Identity": "example.com"}, nil)
}

// TestSES_TerraformEmailIdentity mirrors aws_ses_email_identity.
func TestSES_TerraformEmailIdentity(t *testing.T) {
	ts := emulator.StartTestServer(t)

	body := sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "sender@example.com"}, nil)
	assert.Contains(t, string(body), "<VerifyEmailIdentityResult></VerifyEmailIdentityResult>")
	got := sesVerification(t, ts, "sender@example.com")
	require.Len(t, got.Entries, 1)
	assert.Equal(t, "Success", got.Entries[0].Value.VerificationStatus)
	assert.Empty(t, got.Entries[0].Value.VerificationToken, "only a domain carries a token")

	// VerifyEmailAddress's model declares no output, so its response has no result element.
	body = sesOK(t, ts, "VerifyEmailAddress", map[string]string{"EmailAddress": "old@example.com"}, nil)
	assert.NotContains(t, string(body), "VerifyEmailAddressResult")

	var listed struct {
		Members []string `xml:"ListVerifiedEmailAddressesResult>VerifiedEmailAddresses>member"`
	}
	sesOK(t, ts, "ListVerifiedEmailAddresses", nil, &listed)
	assert.Equal(t, []string{"old@example.com", "sender@example.com"}, listed.Members)

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"old@example.com":"Failed"}`))
	listed.Members = nil
	sesOK(t, ts, "ListVerifiedEmailAddresses", nil, &listed)
	assert.Equal(t, []string{"sender@example.com"}, listed.Members, "only a Success address is verified")

	body = sesOK(t, ts, "DeleteVerifiedEmailAddress", map[string]string{"EmailAddress": "old@example.com"}, nil)
	assert.NotContains(t, string(body), "DeleteVerifiedEmailAddressResult")
	assert.Empty(t, sesVerification(t, ts, "old@example.com").Entries)
}

// TestSES_TerraformDomainDkim mirrors aws_ses_domain_dkim: VerifyDomainDkim, then
// GetIdentityDkimAttributes for the tokens.
func TestSES_TerraformDomainDkim(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)

	type dkim struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value struct {
				Enabled bool     `xml:"DkimEnabled"`
				Status  string   `xml:"DkimVerificationStatus"`
				Tokens  []string `xml:"DkimTokens>member"`
			} `xml:"value"`
		} `xml:"GetIdentityDkimAttributesResult>DkimAttributes>entry"`
	}
	var before dkim
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "example.com"}, &before)
	require.Len(t, before.Entries, 1)
	assert.False(t, before.Entries[0].Value.Enabled)
	assert.Equal(t, "NotStarted", before.Entries[0].Value.Status)
	assert.Empty(t, before.Entries[0].Value.Tokens)

	var issued struct {
		Tokens []string `xml:"VerifyDomainDkimResult>DkimTokens>member"`
	}
	sesOK(t, ts, "VerifyDomainDkim", map[string]string{"Domain": "example.com"}, &issued)
	require.Len(t, issued.Tokens, 3)
	for _, tok := range issued.Tokens {
		assert.Regexp(t, `^[a-z0-9]{32}$`, tok)
	}

	var after dkim
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "example.com"}, &after)
	require.Len(t, after.Entries, 1)
	assert.True(t, after.Entries[0].Value.Enabled)
	assert.Equal(t, "Success", after.Entries[0].Value.Status)
	assert.Equal(t, issued.Tokens, after.Entries[0].Value.Tokens)

	sesOK(t, ts, "SetIdentityDkimEnabled", map[string]string{"Identity": "example.com", "DkimEnabled": "false"}, nil)
	after = dkim{}
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "example.com"}, &after)
	assert.False(t, after.Entries[0].Value.Enabled)

	// DKIM cannot be enabled before VerifyDomainDkim set it up.
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "other.example"}, nil)
	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityDkimEnabled",
		map[string]string{"Identity": "other.example", "DkimEnabled": "true"}, http.StatusBadRequest))
	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityDkimEnabled",
		map[string]string{"Identity": "other.example", "DkimEnabled": "maybe"}, http.StatusBadRequest))
}

// TestSES_TerraformDomainMailFrom mirrors aws_ses_domain_mail_from: SetIdentityMailFromDomain to
// create and update, GetIdentityMailFromDomainAttributes to read, and SetIdentityMailFromDomain
// without a domain to delete.
func TestSES_TerraformDomainMailFrom(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)

	type mailFrom struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value struct {
				Domain   string `xml:"MailFromDomain"`
				Status   string `xml:"MailFromDomainStatus"`
				Behavior string `xml:"BehaviorOnMXFailure"`
			} `xml:"value"`
		} `xml:"GetIdentityMailFromDomainAttributesResult>MailFromDomainAttributes>entry"`
	}
	read := func() mailFrom {
		var out mailFrom
		sesOK(t, ts, "GetIdentityMailFromDomainAttributes", map[string]string{"Identities.member.1": "example.com"}, &out)
		require.Len(t, out.Entries, 1)
		return out
	}

	assert.Equal(t, "UseDefaultValue", read().Entries[0].Value.Behavior)
	assert.Empty(t, read().Entries[0].Value.Domain)

	sesOK(t, ts, "SetIdentityMailFromDomain", map[string]string{
		"Identity": "example.com", "MailFromDomain": "bounce.example.com", "BehaviorOnMXFailure": "RejectMessage",
	}, nil)
	got := read().Entries[0].Value
	assert.Equal(t, "bounce.example.com", got.Domain)
	assert.Equal(t, "Success", got.Status)
	assert.Equal(t, "RejectMessage", got.Behavior)

	// NotStarted has no CustomMailFromStatus counterpart; a set domain is being checked.
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"*":"NotStarted"}`))
	assert.Equal(t, "Pending", read().Entries[0].Value.Status)
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodDelete, "", ""))

	sesOK(t, ts, "SetIdentityMailFromDomain", map[string]string{"Identity": "example.com"}, nil)
	got = read().Entries[0].Value
	assert.Empty(t, got.Domain, "an absent MailFromDomain disables the custom domain")
	assert.Equal(t, "UseDefaultValue", got.Behavior)

	// An address identity's MAIL FROM domain is a subdomain of the address's domain.
	sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "sender@example.com"}, nil)
	sesOK(t, ts, "SetIdentityMailFromDomain", map[string]string{"Identity": "sender@example.com", "MailFromDomain": "m.example.com"}, nil)

	for name, params := range map[string]map[string]string{
		"not a subdomain":  {"Identity": "example.com", "MailFromDomain": "bounce.other.org"},
		"bad behavior":     {"Identity": "example.com", "MailFromDomain": "m.example.com", "BehaviorOnMXFailure": "Bounce"},
		"unknown identity": {"Identity": "nobody.example", "MailFromDomain": "m.nobody.example"},
	} {
		assert.Equal(t, "InvalidParameterValue",
			sesErrorCode(t, ts, "SetIdentityMailFromDomain", params, http.StatusBadRequest), name)
	}
}

// TestSES_NotificationAttributes covers SetIdentityNotificationTopic,
// SetIdentityFeedbackForwardingEnabled and SetIdentityHeadersInNotificationsEnabled, read back
// through GetIdentityNotificationAttributes, including the rule that forwarding can be off only
// while bounce and complaint topics are both set.
func TestSES_NotificationAttributes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "sender@example.com"}, nil)

	type attrs struct {
		Entries []struct {
			Value struct {
				Bounce     string `xml:"BounceTopic"`
				Complaint  string `xml:"ComplaintTopic"`
				Delivery   string `xml:"DeliveryTopic"`
				Forwarding bool   `xml:"ForwardingEnabled"`
				HeadersB   bool   `xml:"HeadersInBounceNotificationsEnabled"`
				HeadersC   bool   `xml:"HeadersInComplaintNotificationsEnabled"`
				HeadersD   bool   `xml:"HeadersInDeliveryNotificationsEnabled"`
			} `xml:"value"`
		} `xml:"GetIdentityNotificationAttributesResult>NotificationAttributes>entry"`
	}
	read := func() attrs {
		var out attrs
		sesOK(t, ts, "GetIdentityNotificationAttributes", map[string]string{"Identities.member.1": "sender@example.com"}, &out)
		require.Len(t, out.Entries, 1)
		return out
	}
	assert.True(t, read().Entries[0].Value.Forwarding, "feedback forwarding is on by default")

	const topic = "arn:aws:sns:us-east-1:123456789012:bounces"
	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityFeedbackForwardingEnabled",
		map[string]string{"Identity": "sender@example.com", "ForwardingEnabled": "false"}, http.StatusBadRequest),
		"forwarding cannot be disabled before both feedback topics are set")

	for _, kind := range []string{"Bounce", "Complaint", "Delivery"} {
		sesOK(t, ts, "SetIdentityNotificationTopic", map[string]string{
			"Identity": "sender@example.com", "NotificationType": kind, "SnsTopic": topic,
		}, nil)
		sesOK(t, ts, "SetIdentityHeadersInNotificationsEnabled", map[string]string{
			"Identity": "sender@example.com", "NotificationType": kind, "Enabled": "true",
		}, nil)
	}
	// The identity may be named by its ARN.
	sesOK(t, ts, "SetIdentityFeedbackForwardingEnabled", map[string]string{
		"Identity": "arn:aws:ses:us-east-1:123456789012:identity/sender@example.com", "ForwardingEnabled": "false",
	}, nil)
	got := read().Entries[0].Value
	assert.Equal(t, topic, got.Bounce)
	assert.Equal(t, topic, got.Complaint)
	assert.Equal(t, topic, got.Delivery)
	assert.False(t, got.Forwarding)
	assert.True(t, got.HeadersB && got.HeadersC && got.HeadersD)

	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityNotificationTopic",
		map[string]string{"Identity": "sender@example.com", "NotificationType": "Bounce"}, http.StatusBadRequest),
		"the bounce topic must stay set while forwarding is off")
	sesOK(t, ts, "SetIdentityNotificationTopic", map[string]string{"Identity": "sender@example.com", "NotificationType": "Delivery"}, nil)
	assert.Empty(t, read().Entries[0].Value.Delivery, "an absent SnsTopic clears the delivery topic")

	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityNotificationTopic",
		map[string]string{"Identity": "sender@example.com", "NotificationType": "Open"}, http.StatusBadRequest))
	assert.Equal(t, "MissingParameter", sesErrorCode(t, ts, "SetIdentityHeadersInNotificationsEnabled",
		map[string]string{"Identity": "sender@example.com", "NotificationType": "Bounce"}, http.StatusBadRequest))
}

// TestSES_ListIdentities covers the IdentityType filter, MaxItems paging and token refusal.
func TestSES_ListIdentities(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, d := range []string{"b.example", "a.example", "c.example"} {
		sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": d}, nil)
	}
	sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "x@a.example"}, nil)

	type page struct {
		Identities []string `xml:"ListIdentitiesResult>Identities>member"`
		NextToken  string   `xml:"ListIdentitiesResult>NextToken"`
	}
	var all page
	sesOK(t, ts, "ListIdentities", nil, &all)
	assert.Equal(t, []string{"a.example", "b.example", "c.example", "x@a.example"}, all.Identities)
	assert.Empty(t, all.NextToken)

	var emails page
	sesOK(t, ts, "ListIdentities", map[string]string{"IdentityType": "EmailAddress"}, &emails)
	assert.Equal(t, []string{"x@a.example"}, emails.Identities)

	var collected []string
	token := ""
	for range 5 {
		params := map[string]string{"IdentityType": "Domain", "MaxItems": "2"}
		if token != "" {
			params["NextToken"] = token
		}
		var p page
		sesOK(t, ts, "ListIdentities", params, &p)
		collected = append(collected, p.Identities...)
		if token = p.NextToken; token == "" {
			break
		}
	}
	assert.Equal(t, []string{"a.example", "b.example", "c.example"}, collected)

	for name, params := range map[string]map[string]string{
		"bad token":    {"NextToken": "!!not-base64"},
		"bad type":     {"IdentityType": "Phone"},
		"MaxItems 0":   {"MaxItems": "0"},
		"MaxItems big": {"MaxItems": "1001"},
	} {
		assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "ListIdentities", params, http.StatusBadRequest), name)
	}
}

// TestSES_SharedWithSESv2 asserts the two SES APIs hold one set of identities: one verified
// through v1 is SES v2's identity, with the same seeded status, and one SES v2 created is listed
// and verified through v1.
func TestSES_SharedWithSESv2(t *testing.T) {
	ts := emulator.StartTestServer(t)

	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)
	sesOK(t, ts, "VerifyDomainDkim", map[string]string{"Domain": "example.com"}, nil)
	sesOK(t, ts, "SetIdentityMailFromDomain", map[string]string{"Identity": "example.com", "MailFromDomain": "m.example.com"}, nil)

	status, doc := sesV2Get(t, ts, "example.com")
	require.Equal(t, http.StatusOK, status, "%v", doc)
	assert.Equal(t, "DOMAIN", doc["IdentityType"])
	assert.Equal(t, "SUCCESS", doc["VerificationStatus"])
	assert.Equal(t, true, doc["VerifiedForSendingStatus"])
	assert.Equal(t, true, doc["FeedbackForwardingStatus"])
	dkim, ok := doc["DkimAttributes"].(map[string]any)
	require.True(t, ok, "%v", doc)
	assert.Equal(t, true, dkim["SigningEnabled"])
	assert.Equal(t, "SUCCESS", dkim["Status"])
	assert.Len(t, dkim["Tokens"], 3)
	mailFrom, ok := doc["MailFromAttributes"].(map[string]any)
	require.True(t, ok, "%v", doc)
	assert.Equal(t, "m.example.com", mailFrom["MailFromDomain"])
	assert.Equal(t, "USE_DEFAULT_VALUE", mailFrom["BehaviorOnMxFailure"])

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"*":"TemporaryFailure"}`))
	_, doc = sesV2Get(t, ts, "example.com")
	assert.Equal(t, "TEMPORARY_FAILURE", doc["VerificationStatus"], "one seed governs both APIs")
	assert.Equal(t, false, doc["VerifiedForSendingStatus"])
	assert.Equal(t, "TemporaryFailure", sesVerification(t, ts, "example.com").Entries[0].Value.VerificationStatus)
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodDelete, "", ""))

	// The reverse: an identity SES v2 created.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v2/email/identities", strings.NewReader(`{"EmailIdentity":"v2.example"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", sesAuth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var listed struct {
		Identities []string `xml:"ListIdentitiesResult>Identities>member"`
	}
	sesOK(t, ts, "ListIdentities", nil, &listed)
	assert.Equal(t, []string{"example.com", "v2.example"}, listed.Identities)
	got := sesVerification(t, ts, "v2.example")
	require.Len(t, got.Entries, 1)
	assert.Equal(t, "Success", got.Entries[0].Value.VerificationStatus)
	assert.Len(t, got.Entries[0].Value.VerificationToken, 44, "a domain SES v2 created reports its token through v1")

	// Deleting through v1 removes it from v2.
	sesOK(t, ts, "DeleteIdentity", map[string]string{"Identity": "v2.example"}, nil)
	status, _ = sesV2Get(t, ts, "v2.example")
	assert.Equal(t, http.StatusNotFound, status)
}

// TestSES_Send covers SendEmail and SendRawEmail — recorded, never sent — and the quota and
// statistics computed from what was recorded.
func TestSES_Send(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(time.Date(2026, 11, 24, 12, 7, 0, 0, time.UTC))

	var sent struct {
		MessageID string `xml:"SendEmailResult>MessageId"`
	}
	sesOK(t, ts, "SendEmail", map[string]string{
		"Source":                             "sender@example.com",
		"Destination.ToAddresses.member.1":   "a@example.com",
		"Destination.CcAddresses.member.1":   "b@example.com",
		"Destination.BccAddresses.member.1":  "c@example.com",
		"Message.Subject.Data":               "hello",
		"Message.Body.Html.Data":             "<p>hi</p>",
		"Message.Body.Text.Data":             "",
		"ReplyToAddresses.member.1":          "reply@example.com",
		"Destination.ToAddresses.member.999": "ignored@example.com",
	}, &sent)
	assert.Regexp(t, `^[0-9a-f]{16}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-000000$`, sent.MessageID)

	var raw struct {
		MessageID string `xml:"SendRawEmailResult>MessageId"`
	}
	sesOK(t, ts, "SendRawEmail", map[string]string{
		"Destinations.member.1": "d@example.com",
		"RawMessage.Data":       "RnJvbTogc2VuZGVyQGV4YW1wbGUuY29tCg==",
	}, &raw)
	assert.NotEqual(t, sent.MessageID, raw.MessageID)

	resp, err := http.Get(ts.URL + "/v1/emails?subject=hello")
	require.NoError(t, err)
	var emails struct {
		Emails []emulator.SESv2CapturedEmail `json:"Emails"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&emails))
	_ = resp.Body.Close()
	require.Len(t, emails.Emails, 1)
	assert.Equal(t, sent.MessageID, emails.Emails[0].MessageID)
	assert.Equal(t, []string{"a@example.com"}, emails.Emails[0].To)
	assert.Equal(t, []string{"b@example.com"}, emails.Emails[0].Cc)
	assert.Equal(t, "<p>hi</p>", emails.Emails[0].Body)

	var quota struct {
		Max  float64 `xml:"GetSendQuotaResult>Max24HourSend"`
		Rate float64 `xml:"GetSendQuotaResult>MaxSendRate"`
		Sent float64 `xml:"GetSendQuotaResult>SentLast24Hours"`
	}
	sesOK(t, ts, "GetSendQuota", nil, &quota)
	assert.Equal(t, 200.0, quota.Max)
	assert.Equal(t, 1.0, quota.Rate)
	assert.Equal(t, 4.0, quota.Sent, "three recipients and one, counted per recipient")

	type stats struct {
		Points []struct {
			Timestamp string `xml:"Timestamp"`
			Attempts  int64  `xml:"DeliveryAttempts"`
		} `xml:"GetSendStatisticsResult>SendDataPoints>member"`
	}
	var s stats
	sesOK(t, ts, "GetSendStatistics", nil, &s)
	require.Len(t, s.Points, 1)
	assert.Equal(t, "2026-11-24T12:00:00Z", s.Points[0].Timestamp, "a 15-minute period")
	assert.Equal(t, int64(4), s.Points[0].Attempts)

	// A day later the quota window is empty; the two-week statistics still hold the point.
	ts.FreezeTimeAt(time.Date(2026, 11, 25, 12, 8, 0, 0, time.UTC))
	sesOK(t, ts, "GetSendQuota", nil, &quota)
	assert.Equal(t, 0.0, quota.Sent)
	s = stats{}
	sesOK(t, ts, "GetSendStatistics", nil, &s)
	assert.Len(t, s.Points, 1)
	ts.FreezeTimeAt(time.Date(2026, 12, 9, 12, 8, 0, 0, time.UTC))
	s = stats{}
	sesOK(t, ts, "GetSendStatistics", nil, &s)
	assert.Empty(t, s.Points)

	for name, tc := range map[string]struct {
		action string
		params map[string]string
	}{
		"no source":     {"SendEmail", map[string]string{"Destination.ToAddresses.member.1": "a@example.com", "Message.Subject.Data": "s"}},
		"no recipients": {"SendEmail", map[string]string{"Source": "s@example.com", "Message.Subject.Data": "s"}},
		"no subject":    {"SendEmail", map[string]string{"Source": "s@example.com", "Destination.ToAddresses.member.1": "a@example.com"}},
		"no raw data":   {"SendRawEmail", map[string]string{"Source": "s@example.com"}},
	} {
		assert.Equal(t, "MissingParameter", sesErrorCode(t, ts, tc.action, tc.params, http.StatusBadRequest), name)
	}
}

// TestSES_Refusals covers the shared refusals: a missing parameter, a malformed identity, an
// unimplemented action, and an unusable seed.
func TestSES_Refusals(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for name, tc := range map[string]struct {
		action string
		params map[string]string
		status int
		code   string
	}{
		"VerifyDomainIdentity without Domain":     {"VerifyDomainIdentity", nil, http.StatusBadRequest, "MissingParameter"},
		"VerifyDomainIdentity with an address":    {"VerifyDomainIdentity", map[string]string{"Domain": "a@b.example"}, http.StatusBadRequest, "InvalidParameterValue"},
		"VerifyDomainDkim with an address":        {"VerifyDomainDkim", map[string]string{"Domain": "a@b.example"}, http.StatusBadRequest, "InvalidParameterValue"},
		"VerifyDomainDkim without Domain":         {"VerifyDomainDkim", nil, http.StatusBadRequest, "MissingParameter"},
		"VerifyEmailIdentity with a domain":       {"VerifyEmailIdentity", map[string]string{"EmailAddress": "b.example"}, http.StatusBadRequest, "InvalidParameterValue"},
		"VerifyEmailIdentity without an address":  {"VerifyEmailIdentity", nil, http.StatusBadRequest, "MissingParameter"},
		"GetIdentityVerificationAttributes empty": {"GetIdentityVerificationAttributes", nil, http.StatusBadRequest, "MissingParameter"},
		"GetIdentityDkimAttributes empty":         {"GetIdentityDkimAttributes", nil, http.StatusBadRequest, "MissingParameter"},
		"GetIdentityMailFrom empty":               {"GetIdentityMailFromDomainAttributes", nil, http.StatusBadRequest, "MissingParameter"},
		"GetIdentityNotification empty":           {"GetIdentityNotificationAttributes", nil, http.StatusBadRequest, "MissingParameter"},
		"DeleteIdentity without Identity":         {"DeleteIdentity", nil, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityMailFromDomain no Identity":   {"SetIdentityMailFromDomain", nil, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityNotificationTopic no type":    {"SetIdentityNotificationTopic", map[string]string{"Identity": "x"}, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityFeedbackForwarding no flag":   {"SetIdentityFeedbackForwardingEnabled", map[string]string{"Identity": "x"}, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityFeedbackForwarding unknown":   {"SetIdentityFeedbackForwardingEnabled", map[string]string{"Identity": "x", "ForwardingEnabled": "true"}, http.StatusBadRequest, "InvalidParameterValue"},
		"SetIdentityDkimEnabled no Identity":      {"SetIdentityDkimEnabled", nil, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityHeaders no Identity":          {"SetIdentityHeadersInNotificationsEnabled", nil, http.StatusBadRequest, "MissingParameter"},
		"SetIdentityHeaders bad type":             {"SetIdentityHeadersInNotificationsEnabled", map[string]string{"Identity": "x", "NotificationType": "Open"}, http.StatusBadRequest, "InvalidParameterValue"},
		"DeleteVerifiedEmailAddress no address":   {"DeleteVerifiedEmailAddress", nil, http.StatusBadRequest, "MissingParameter"},
		"an unimplemented action":                 {"CreateReceiptRule", nil, http.StatusBadRequest, "InvalidAction"},
	} {
		assert.Equal(t, tc.code, sesErrorCode(t, ts, tc.action, tc.params, tc.status), name)
	}

	for name, body := range map[string]string{
		"not JSON":       `{`,
		"unknown status": `{"example.com":"Verified"}`,
		"empty identity": `{"":"Success"}`,
	} {
		assert.Equal(t, http.StatusBadRequest, sesSeed(t, ts, http.MethodPost, "", body), name)
	}
}

// TestSES_AddressDkimFollowsItsDomain covers Easy DKIM for an address identity, which uses its
// domain's setup: SetIdentityDkimEnabled's page allows enabling an address only "if the
// corresponding domain ... has been set up to use Easy DKIM", and GetIdentityDkimAttributes' says
// an address's tokens "represent the domain of that address".
func TestSES_AddressDkimFollowsItsDomain(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "user@example.com"}, nil)
	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityDkimEnabled",
		map[string]string{"Identity": "user@example.com", "DkimEnabled": "true"}, http.StatusBadRequest),
		"an address cannot enable DKIM before its domain is set up")

	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)
	var issued struct {
		Tokens []string `xml:"VerifyDomainDkimResult>DkimTokens>member"`
	}
	sesOK(t, ts, "VerifyDomainDkim", map[string]string{"Domain": "example.com"}, &issued)
	sesOK(t, ts, "SetIdentityDkimEnabled", map[string]string{"Identity": "user@example.com", "DkimEnabled": "true"}, nil)

	type dkim struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value struct {
				Enabled bool     `xml:"DkimEnabled"`
				Status  string   `xml:"DkimVerificationStatus"`
				Tokens  []string `xml:"DkimTokens>member"`
			} `xml:"value"`
		} `xml:"GetIdentityDkimAttributesResult>DkimAttributes>entry"`
	}
	var got dkim
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "user@example.com"}, &got)
	require.Len(t, got.Entries, 1)
	assert.True(t, got.Entries[0].Value.Enabled)
	assert.Equal(t, "Success", got.Entries[0].Value.Status)
	assert.Equal(t, issued.Tokens, got.Entries[0].Value.Tokens, "an address reports its domain's tokens")

	// The status is the domain's, so a seed on the domain moves the address too.
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"example.com":"Pending"}`))
	got = dkim{}
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "user@example.com"}, &got)
	assert.Equal(t, "Pending", got.Entries[0].Value.Status)

	status, doc := sesV2Get(t, ts, "user@example.com")
	require.Equal(t, http.StatusOK, status)
	attrs, ok := doc["DkimAttributes"].(map[string]any)
	require.True(t, ok, "SES v2 reports the address's DKIM through its domain: %v", doc)
	assert.Equal(t, true, attrs["SigningEnabled"])
	assert.Equal(t, "PENDING", attrs["Status"])

	sesOK(t, ts, "SetIdentityDkimEnabled", map[string]string{"Identity": "user@example.com", "DkimEnabled": "false"}, nil)
	got = dkim{}
	sesOK(t, ts, "GetIdentityDkimAttributes", map[string]string{"Identities.member.1": "user@example.com"}, &got)
	assert.False(t, got.Entries[0].Value.Enabled, "the address's own setting decides signing")
}

// TestSES_VerificationSeedScope covers the seed's account and Region scope and its precedence.
func TestSES_VerificationSeedScope(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)
	status := func() string { return sesVerification(t, ts, "example.com").Entries[0].Value.VerificationStatus }

	// The test server's account is 123456789012 and its Region us-east-1.
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "",
		`{"accountId":"111111111111","identities":{"example.com":"Failed"}}`))
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "",
		`{"region":"us-west-2","identities":{"*":"Failed"}}`))
	assert.Equal(t, "Success", status(), "a seed scoped to another account or Region does not apply")

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "",
		`{"accountId":"123456789012","region":"us-east-1","identities":{"*":"TemporaryFailure"}}`))
	assert.Equal(t, "TemporaryFailure", status(), "a scoped wildcard applies to its account and Region")

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "", `{"example.com":"Pending"}`))
	assert.Equal(t, "Pending", status(), "a seed naming the identity outranks a wildcard one")

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodPost, "",
		`{"accountId":"123456789012","identities":{"example.com":"NotStarted"}}`))
	assert.Equal(t, "NotStarted", status(), "an exact account outranks the wildcard account")

	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodDelete, "?identity=example.com&accountId=123456789012", ""))
	assert.Equal(t, "Pending", status())
	require.Equal(t, http.StatusOK, sesSeed(t, ts, http.MethodDelete, "", ""))
	assert.Equal(t, "Success", status())

	// A refused body writes nothing, and names its first bad entry in identity order.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/ses/identity-verification",
		strings.NewReader(`{"a.example":"Pending","c.example":"Bogus","b.example":"Wrong","example.com":"Failed"}`))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), `b.example`)
	assert.Equal(t, "Success", status(), "a refused seed body writes no entry")

	for name, body := range map[string]string{
		"identities not a map":        `{"identities":"nope"}`,
		"an unknown scoped member":    `{"identities":{"example.com":"Pending"},"acount":"123456789012"}`,
		"a scope without identities":  `{"accountId":"123456789012"}`,
		"a region without identities": `{"region":"us-east-1","example.com":"Pending"}`,
		"a slash in accountId":        `{"accountId":"123/456","identities":{"*":"Pending"}}`,
		"a star in region":            `{"region":"*","identities":{"*":"Pending"}}`,
	} {
		assert.Equal(t, http.StatusBadRequest, sesSeed(t, ts, http.MethodPost, "", body), name)
	}
	assert.Equal(t, "Success", status(), "no refused body wrote a seed")
}

// TestSES_ConcurrentSendsKeepEveryMessage sends through both SES APIs at once. Both update one
// captured-email index read-append-write, so without the shared lock concurrent sends lose IDs
// and the quota undercounts.
func TestSES_ConcurrentSendsKeepEveryMessage(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const each = 25
	var wg sync.WaitGroup
	for i := range each {
		wg.Add(2)
		go func() {
			defer wg.Done()
			// require must not run off the test goroutine, so this posts the form itself.
			form := url.Values{"Action": {"SendEmail"}, "Version": {"2010-12-01"}, "Source": {"s@example.com"},
				"Destination.ToAddresses.member.1": {"to" + strconv.Itoa(i) + "@example.com"},
				"Message.Subject.Data":             {"v1"}}
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
			if !assert.NoError(t, err) {
				return
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
			req.Header.Set("Authorization", sesAuth)
			resp, err := http.DefaultClient.Do(req)
			if !assert.NoError(t, err) {
				return
			}
			_ = resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}()
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/v2/email/outbound-emails", strings.NewReader(
				`{"FromEmailAddress":"s@example.com","Destination":{"ToAddresses":["v2@example.com"]},`+
					`"Content":{"Simple":{"Subject":{"Data":"v2"},"Body":{"Text":{"Data":"x"}}}}}`))
			if !assert.NoError(t, err) {
				return
			}
			req.Header.Set("Authorization", sesAuth)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if !assert.NoError(t, err) {
				return
			}
			_ = resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}()
	}
	wg.Wait()

	var quota struct {
		Sent float64 `xml:"GetSendQuotaResult>SentLast24Hours"`
	}
	sesOK(t, ts, "GetSendQuota", nil, &quota)
	assert.Equal(t, float64(2*each), quota.Sent, "every message from both APIs is in the index")
}

// TestSES_EmailsFilterMatchesEveryRecipient asserts GET /v1/emails?to= matches Cc and Bcc as well
// as To.
func TestSES_EmailsFilterMatchesEveryRecipient(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "SendEmail", map[string]string{
		"Source":                            "s@example.com",
		"Destination.ToAddresses.member.1":  "to@example.com",
		"Destination.CcAddresses.member.1":  "cc@example.com",
		"Destination.BccAddresses.member.1": "bcc@example.com",
		"Message.Subject.Data":              "x",
	}, nil)
	for _, addr := range []string{"to@", "cc@", "bcc@"} {
		resp, err := http.Get(ts.URL + "/v1/emails?to=" + addr)
		require.NoError(t, err)
		var out struct {
			Count int `json:"Count"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		_ = resp.Body.Close()
		assert.Equal(t, 1, out.Count, addr)
	}
}

// TestSES_MailFromAcceptsAnIdentityARN asserts SetIdentityMailFromDomain checks the subdomain rule
// against the identity's name when the identity is given as its ARN.
func TestSES_MailFromAcceptsAnIdentityARN(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)
	sesOK(t, ts, "SetIdentityMailFromDomain", map[string]string{
		"Identity":       "arn:aws:ses:us-east-1:123456789012:identity/example.com",
		"MailFromDomain": "m.example.com",
	}, nil)
	var out struct {
		Domain string `xml:"GetIdentityMailFromDomainAttributesResult>MailFromDomainAttributes>entry>value>MailFromDomain"`
	}
	sesOK(t, ts, "GetIdentityMailFromDomainAttributes", map[string]string{"Identities.member.1": "example.com"}, &out)
	assert.Equal(t, "m.example.com", out.Domain)
}

// TestSES_Routing pins the split refineSESVersion makes: SES v1 and SES v2 share the email host
// and the ses signing name, and only the /v2/ path tells them apart.
func TestSES_Routing(t *testing.T) {
	tests := []struct {
		name   string
		host   string
		path   string
		auth   bool
		method string
		want   string
	}{
		{"v1 by host", "email.us-east-1.amazonaws.com", "/", false, http.MethodPost, "ses"},
		{"v1 by signing name", "localhost:4566", "/", true, http.MethodPost, "ses"},
		{"v2 by host", "email.us-east-1.amazonaws.com", "/v2/email/identities", false, http.MethodGet, "sesv2"},
		{"v2 by signing name", "localhost:4566", "/v2/email/outbound-emails", true, http.MethodPost, "sesv2"},
		{"a /v2/ path for another service is not SES", "localhost:4566", "/v2/apis", false, http.MethodGet, "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(tc.method, "http://"+tc.host+tc.path, nil)
			require.NoError(t, err)
			r.Host = tc.host
			if tc.auth {
				r.Header.Set("Authorization", sesAuth)
			}
			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, req.Service)
		})
	}
}

// TestSES_IdentityARNs covers an identity named by its ARN in every operation that takes one: the
// Get*Attributes maps are keyed by the name the caller gave, DeleteIdentity deletes, and an ARN
// naming another account or Region is an identity the caller does not hold.
func TestSES_IdentityARNs(t *testing.T) {
	ts := emulator.StartTestServer(t)
	sesOK(t, ts, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"}, nil)
	const arn = "arn:aws:ses:us-east-1:123456789012:identity/example.com"
	const otherAccount = "arn:aws:ses:us-east-1:111111111111:identity/example.com"
	const otherRegion = "arn:aws:ses:us-west-2:123456789012:identity/example.com"

	got := sesVerification(t, ts, arn, otherAccount, otherRegion, "arn:aws:sns:us-east-1:123456789012:identity/x")
	require.Len(t, got.Entries, 1, "only the ARN in the caller's account and Region is held")
	assert.Equal(t, arn, got.Entries[0].Key, "the map is keyed by the identity as the caller named it")

	for _, action := range []string{"GetIdentityDkimAttributes", "GetIdentityMailFromDomainAttributes", "GetIdentityNotificationAttributes"} {
		body := sesOK(t, ts, action, map[string]string{"Identities.member.1": arn, "Identities.member.2": otherAccount}, nil)
		assert.Equal(t, 1, strings.Count(string(body), "<key>"), "%s: %s", action, body)
		assert.Contains(t, string(body), "<key>"+arn+"</key>", action)
	}

	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityMailFromDomain",
		map[string]string{"Identity": otherAccount, "MailFromDomain": "m.example.com"}, http.StatusBadRequest),
		"an ARN in another account does not reach the caller's same-named identity")
	assert.Equal(t, "InvalidParameterValue", sesErrorCode(t, ts, "SetIdentityFeedbackForwardingEnabled",
		map[string]string{"Identity": otherRegion, "ForwardingEnabled": "true"}, http.StatusBadRequest))

	sesOK(t, ts, "DeleteIdentity", map[string]string{"Identity": otherAccount}, nil)
	require.Len(t, sesVerification(t, ts, "example.com").Entries, 1, "deleting another account's ARN deletes nothing")
	sesOK(t, ts, "DeleteIdentity", map[string]string{"Identity": arn}, nil)
	assert.Empty(t, sesVerification(t, ts, "example.com").Entries, "deleting by ARN deletes the identity")
}

// TestSES_V2DomainHasEasyDkim asserts a domain SES v2 created carries Easy DKIM tokens, as
// API_CreateEmailIdentity provides by default, so an address under it can enable DKIM.
func TestSES_V2DomainHasEasyDkim(t *testing.T) {
	ts := emulator.StartTestServer(t)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v2/email/identities", strings.NewReader(`{"EmailIdentity":"v2.example"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", sesAuth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, doc := sesV2Get(t, ts, "v2.example")
	attrs, ok := doc["DkimAttributes"].(map[string]any)
	require.True(t, ok, "%v", doc)
	assert.Len(t, attrs["Tokens"], 3)

	sesOK(t, ts, "VerifyEmailIdentity", map[string]string{"EmailAddress": "user@v2.example"}, nil)
	sesOK(t, ts, "SetIdentityDkimEnabled", map[string]string{"Identity": "user@v2.example", "DkimEnabled": "true"}, nil)
}

// TestSES_V2SendRecordsEveryRecipient asserts SES v2's SendEmail records Cc and Bcc, so
// GET /v1/emails?to= finds a v2 message by any recipient.
func TestSES_V2SendRecordsEveryRecipient(t *testing.T) {
	ts := emulator.StartTestServer(t)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v2/email/outbound-emails", strings.NewReader(
		`{"FromEmailAddress":"s@example.com","Destination":{"ToAddresses":["to@example.com"],`+
			`"CcAddresses":["cc@example.com"],"BccAddresses":["bcc@example.com"]},`+
			`"Content":{"Simple":{"Subject":{"Data":"v2"},"Body":{"Text":{"Data":"x"}}}}}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", sesAuth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	for _, addr := range []string{"to@", "cc@", "bcc@"} {
		r, err := http.Get(ts.URL + "/v1/emails?to=" + addr)
		require.NoError(t, err)
		var out struct {
			Count int `json:"Count"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&out))
		_ = r.Body.Close()
		assert.Equal(t, 1, out.Count, addr)
	}
}
