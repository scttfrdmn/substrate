package emulator

import (
	"context"
	"strings"
)

// The four Get*Attributes operations and the Set* operations that feed them.
//
// Each Get* takes Identities.member.N and answers a map keyed by identity. An identity the
// account does not hold is left out of the map rather than refused, which is what a consumer
// reading "is this identity still there" from the map depends on: Terraform's aws_ses_* read
// functions treat a missing key as a resource deleted out of band.

// sesEntry is one entry of a Query map: `<entry><key>…</key><value>…</value></entry>`, the
// layout the reference pages give (VerificationAttributes.entry.N.key and .value).
type sesEntry[T any] struct {
	Key   string `xml:"key"`
	Value T      `xml:"value"`
}

// sesMap is a Query map element holding entries in the order the caller named the identities.
type sesMap[T any] struct {
	Entries []sesEntry[T] `xml:"entry"`
}

// sesIdentitiesParam reads Identities.member.N, refusing an empty list: the member is required
// on all four operations.
func sesIdentitiesParam(req *AWSRequest) ([]string, *AWSError) {
	ids := iamMemberList(req.Params, "Identities")
	if len(ids) == 0 {
		return nil, sesMissingParameter("Identities")
	}
	return ids, nil
}

// sesHeld is one identity a Get*Attributes call named and the account holds: the record, and
// the key the response map files it under, which is the identity as the caller wrote it (a
// name or an ARN).
type sesHeld struct {
	key string
	rec *SESv2Identity
}

// heldIdentities returns the named identities the account holds, in the order named, skipping
// the rest. An identity may be named by its ARN ([sesResolveIdentity]); one naming another
// account or Region is not held.
func (p *SESPlugin) heldIdentities(reqCtx *RequestContext, names []string) ([]sesHeld, error) {
	var out []sesHeld
	seen := map[string]bool{}
	for _, given := range names {
		if seen[given] {
			continue
		}
		seen[given] = true
		name, held := sesResolveIdentity(reqCtx, given)
		if !held {
			continue
		}
		rec, err := p.loadIdentity(reqCtx, name)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			out = append(out, sesHeld{key: given, rec: rec})
		}
	}
	return out, nil
}

// sesVerificationAttrs is IdentityVerificationAttributes. A token is reported for a domain
// only, as the operation's page states.
type sesVerificationAttrs struct {
	VerificationStatus string `xml:"VerificationStatus"`
	VerificationToken  string `xml:"VerificationToken,omitempty"`
}

func (p *SESPlugin) getIdentityVerificationAttributes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := sesIdentitiesParam(req)
	if awsErr != nil {
		return nil, awsErr
	}
	recs, err := p.heldIdentities(reqCtx, names)
	if err != nil {
		return nil, err
	}
	var m sesMap[sesVerificationAttrs]
	for _, h := range recs {
		rec := h.rec
		status, err := sesVerificationStatus(context.Background(), p.state, reqCtx.AccountID, reqCtx.Region, rec.IdentityName)
		if err != nil {
			return nil, err
		}
		attrs := sesVerificationAttrs{VerificationStatus: status}
		if rec.IdentityType == "DOMAIN" {
			attrs.VerificationToken = sesVerificationToken(reqCtx.AccountID, reqCtx.Region, rec.IdentityName)
		}
		m.Entries = append(m.Entries, sesEntry[sesVerificationAttrs]{Key: h.key, Value: attrs})
	}
	type result struct {
		VerificationAttributes sesMap[sesVerificationAttrs] `xml:"VerificationAttributes"`
	}
	return queryOKResponse(reqCtx, "GetIdentityVerificationAttributes", sesXMLNS, result{VerificationAttributes: m})
}

// sesDkimAttrs is IdentityDkimAttributes, rendered from an [sesDkimView].
type sesDkimAttrs struct {
	DkimEnabled            bool          `xml:"DkimEnabled"`
	DkimVerificationStatus string        `xml:"DkimVerificationStatus"`
	DkimTokens             *sesTokenList `xml:"DkimTokens,omitempty"`
}

func (p *SESPlugin) getIdentityDkimAttributes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := sesIdentitiesParam(req)
	if awsErr != nil {
		return nil, awsErr
	}
	recs, err := p.heldIdentities(reqCtx, names)
	if err != nil {
		return nil, err
	}
	var m sesMap[sesDkimAttrs]
	for _, h := range recs {
		rec := h.rec
		view, err := sesDkimFor(context.Background(), p.state, reqCtx.AccountID, reqCtx.Region, rec)
		if err != nil {
			return nil, err
		}
		attrs := sesDkimAttrs{DkimEnabled: view.Enabled, DkimVerificationStatus: view.Status}
		if len(view.Tokens) > 0 {
			attrs.DkimTokens = &sesTokenList{Members: view.Tokens}
		}
		m.Entries = append(m.Entries, sesEntry[sesDkimAttrs]{Key: h.key, Value: attrs})
	}
	type result struct {
		DkimAttributes sesMap[sesDkimAttrs] `xml:"DkimAttributes"`
	}
	return queryOKResponse(reqCtx, "GetIdentityDkimAttributes", sesXMLNS, result{DkimAttributes: m})
}

// sesMailFromAttrs is IdentityMailFromDomainAttributes. The domain and its status are reported
// only while one is set. BehaviorOnMXFailure reads UseDefaultValue for an identity that never set
// one, which is substrate's reading: SetIdentityMailFromDomain's page publishes no default, and
// UseDefaultValue is the value whose behavior (sending from amazonses.com) is what an identity
// without a custom MAIL FROM domain gets.
type sesMailFromAttrs struct {
	MailFromDomain       string `xml:"MailFromDomain,omitempty"`
	MailFromDomainStatus string `xml:"MailFromDomainStatus,omitempty"`
	BehaviorOnMXFailure  string `xml:"BehaviorOnMXFailure"`
}

func (p *SESPlugin) getIdentityMailFromDomainAttributes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := sesIdentitiesParam(req)
	if awsErr != nil {
		return nil, awsErr
	}
	recs, err := p.heldIdentities(reqCtx, names)
	if err != nil {
		return nil, err
	}
	var m sesMap[sesMailFromAttrs]
	for _, h := range recs {
		rec := h.rec
		attrs := sesMailFromAttrs{BehaviorOnMXFailure: rec.BehaviorOnMXFailure}
		if attrs.BehaviorOnMXFailure == "" {
			attrs.BehaviorOnMXFailure = "UseDefaultValue"
		}
		if rec.MailFromDomain != "" {
			status, err := sesVerificationStatus(context.Background(), p.state, reqCtx.AccountID, reqCtx.Region, rec.IdentityName)
			if err != nil {
				return nil, err
			}
			attrs.MailFromDomain = rec.MailFromDomain
			attrs.MailFromDomainStatus = sesMailFromStatus(status)
		}
		m.Entries = append(m.Entries, sesEntry[sesMailFromAttrs]{Key: h.key, Value: attrs})
	}
	type result struct {
		MailFromDomainAttributes sesMap[sesMailFromAttrs] `xml:"MailFromDomainAttributes"`
	}
	return queryOKResponse(reqCtx, "GetIdentityMailFromDomainAttributes", sesXMLNS, result{MailFromDomainAttributes: m})
}

// sesNotificationAttrs is IdentityNotificationAttributes. Feedback forwarding is on by default;
// a topic is reported only while one is set.
type sesNotificationAttrs struct {
	BounceTopic                            string `xml:"BounceTopic,omitempty"`
	ComplaintTopic                         string `xml:"ComplaintTopic,omitempty"`
	DeliveryTopic                          string `xml:"DeliveryTopic,omitempty"`
	ForwardingEnabled                      bool   `xml:"ForwardingEnabled"`
	HeadersInBounceNotificationsEnabled    bool   `xml:"HeadersInBounceNotificationsEnabled"`
	HeadersInComplaintNotificationsEnabled bool   `xml:"HeadersInComplaintNotificationsEnabled"`
	HeadersInDeliveryNotificationsEnabled  bool   `xml:"HeadersInDeliveryNotificationsEnabled"`
}

func (p *SESPlugin) getIdentityNotificationAttributes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := sesIdentitiesParam(req)
	if awsErr != nil {
		return nil, awsErr
	}
	recs, err := p.heldIdentities(reqCtx, names)
	if err != nil {
		return nil, err
	}
	var m sesMap[sesNotificationAttrs]
	for _, h := range recs {
		rec := h.rec
		m.Entries = append(m.Entries, sesEntry[sesNotificationAttrs]{Key: h.key, Value: sesNotificationAttrs{
			BounceTopic:                            rec.BounceTopic,
			ComplaintTopic:                         rec.ComplaintTopic,
			DeliveryTopic:                          rec.DeliveryTopic,
			ForwardingEnabled:                      !rec.ForwardingDisabled,
			HeadersInBounceNotificationsEnabled:    rec.HeadersInBounce,
			HeadersInComplaintNotificationsEnabled: rec.HeadersInComplaint,
			HeadersInDeliveryNotificationsEnabled:  rec.HeadersInDelivery,
		}})
	}
	type result struct {
		NotificationAttributes sesMap[sesNotificationAttrs] `xml:"NotificationAttributes"`
	}
	return queryOKResponse(reqCtx, "GetIdentityNotificationAttributes", sesXMLNS, result{NotificationAttributes: m})
}

// sesDkimView is an identity's Easy DKIM state, as both SES APIs report it. Status is in SES
// v1's spelling.
type sesDkimView struct {
	SetUp   bool
	Enabled bool
	Status  string
	Tokens  []string
}

// sesDkimFor is rec's Easy DKIM state. A domain's is its own: set up once VerifyDomainDkim ran,
// with its tokens and the domain's seeded verification status, since substrate has no DNS to
// find the CNAMEs in either. An address's is its domain's: "If the identity is an email address,
// the tokens represent the domain of that address" (GetIdentityDkimAttributes' page), while
// whether signing is enabled is the address's own setting (SetIdentityDkimEnabled). The page
// adds that the verification status "is only returned for domain name identities", but
// DkimVerificationStatus is a required member, so an address reports its domain's. Not set up,
// an identity reports signing disabled, NotStarted and no tokens.
func sesDkimFor(ctx context.Context, state StateManager, accountID, region string, rec *SESv2Identity) (sesDkimView, error) {
	source := rec
	if rec.IdentityType == "EMAIL_ADDRESS" {
		domain, err := sesLoadIdentity(ctx, state, accountID, region, sesIdentityDomain(rec.IdentityName))
		if err != nil {
			return sesDkimView{}, err
		}
		source = domain
	}
	if source == nil || len(source.DkimTokens) == 0 {
		return sesDkimView{Status: "NotStarted"}, nil
	}
	status, err := sesVerificationStatus(ctx, state, accountID, region, source.IdentityName)
	if err != nil {
		return sesDkimView{}, err
	}
	return sesDkimView{SetUp: true, Enabled: !rec.DkimDisabled, Status: status, Tokens: source.DkimTokens}, nil
}

// sesIdentityDomain is the domain an identity sends from: the domain itself, or an address's
// part after the "@".
func sesIdentityDomain(identity string) string {
	if at := strings.LastIndexByte(identity, '@'); at >= 0 {
		return identity[at+1:]
	}
	return identity
}

// setIdentityMailFromDomain serves SetIdentityMailFromDomain. An absent MailFromDomain disables
// the custom MAIL FROM domain ("A value of null disables the custom MAIL FROM setting"), which is how
// Terraform's aws_ses_domain_mail_from deletes one. A domain that is not a subdomain of the
// identity's own is refused: the page requires the MAIL FROM domain to be "a subdomain of the
// verified identity".
func (p *SESPlugin) setIdentityMailFromDomain(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, "Identity")
	if awsErr != nil {
		return nil, awsErr
	}
	name, held := sesResolveIdentity(reqCtx, identity)
	if !held {
		return nil, sesInvalidParameter("Identity %s does not exist.", identity)
	}
	mailFrom := strings.TrimSpace(req.Params["MailFromDomain"])
	behavior := req.Params["BehaviorOnMXFailure"]
	switch behavior {
	case "", "UseDefaultValue", "RejectMessage":
	default:
		return nil, sesInvalidParameter("Invalid BehaviorOnMXFailure %s; valid values are UseDefaultValue and RejectMessage.", behavior)
	}
	if mailFrom != "" && !strings.HasSuffix(mailFrom, "."+sesIdentityDomain(name)) {
		return nil, sesInvalidParameter("MAIL FROM domain %s must be a subdomain of %s.", mailFrom, sesIdentityDomain(name))
	}
	if err := p.updateIdentity(reqCtx, identity, func(rec *SESv2Identity) error {
		rec.MailFromDomain = mailFrom
		if behavior == "UseDefaultValue" {
			behavior = ""
		}
		rec.BehaviorOnMXFailure = behavior
		return nil
	}); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SetIdentityMailFromDomain", sesXMLNS, struct{}{})
}

// sesNotificationType validates a NotificationType parameter.
func sesNotificationType(req *AWSRequest) (string, *AWSError) {
	t, awsErr := sesRequired(req, "NotificationType")
	if awsErr != nil {
		return "", awsErr
	}
	switch t {
	case "Bounce", "Complaint", "Delivery":
		return t, nil
	}
	return "", sesInvalidParameter("Invalid NotificationType %s; valid values are Bounce, Complaint and Delivery.", t)
}

// setIdentityNotificationTopic serves SetIdentityNotificationTopic. An absent SnsTopic "is
// cleared and publishing is disabled" for that notification type, per the page. Turning off a bounce or complaint
// topic while feedback forwarding is off is refused, because feedback forwarding can be off
// only while both are published to SNS (SetIdentityFeedbackForwardingEnabled's page).
func (p *SESPlugin) setIdentityNotificationTopic(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, "Identity")
	if awsErr != nil {
		return nil, awsErr
	}
	kind, awsErr := sesNotificationType(req)
	if awsErr != nil {
		return nil, awsErr
	}
	topic := strings.TrimSpace(req.Params["SnsTopic"])
	if err := p.updateIdentity(reqCtx, identity, func(rec *SESv2Identity) error {
		if topic == "" && rec.ForwardingDisabled && kind != "Delivery" {
			return sesInvalidParameter("Feedback forwarding is disabled for %s, so its %s topic must stay set.", identity, kind)
		}
		switch kind {
		case "Bounce":
			rec.BounceTopic = topic
		case "Complaint":
			rec.ComplaintTopic = topic
		default:
			rec.DeliveryTopic = topic
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SetIdentityNotificationTopic", sesXMLNS, struct{}{})
}

// setIdentityFeedbackForwardingEnabled serves SetIdentityFeedbackForwardingEnabled. Forwarding
// can be disabled only "when Amazon Simple Notification Service (Amazon SNS) topics are
// specified for both bounces and complaints", per the operation's page.
func (p *SESPlugin) setIdentityFeedbackForwardingEnabled(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, "Identity")
	if awsErr != nil {
		return nil, awsErr
	}
	enabled, awsErr := sesRequiredBool(req, "ForwardingEnabled")
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.updateIdentity(reqCtx, identity, func(rec *SESv2Identity) error {
		if !enabled && (rec.BounceTopic == "" || rec.ComplaintTopic == "") {
			return sesInvalidParameter("Feedback forwarding for %s can be disabled only while bounce and complaint notifications are both published to SNS topics.", identity)
		}
		rec.ForwardingDisabled = !enabled
		return nil
	}); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SetIdentityFeedbackForwardingEnabled", sesXMLNS, struct{}{})
}

// setIdentityHeadersInNotificationsEnabled serves SetIdentityHeadersInNotificationsEnabled,
// which Terraform's aws_ses_identity_notification_topic calls for include_original_headers.
func (p *SESPlugin) setIdentityHeadersInNotificationsEnabled(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, "Identity")
	if awsErr != nil {
		return nil, awsErr
	}
	kind, awsErr := sesNotificationType(req)
	if awsErr != nil {
		return nil, awsErr
	}
	enabled, awsErr := sesRequiredBool(req, "Enabled")
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.updateIdentity(reqCtx, identity, func(rec *SESv2Identity) error {
		switch kind {
		case "Bounce":
			rec.HeadersInBounce = enabled
		case "Complaint":
			rec.HeadersInComplaint = enabled
		default:
			rec.HeadersInDelivery = enabled
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SetIdentityHeadersInNotificationsEnabled", sesXMLNS, struct{}{})
}
