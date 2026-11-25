package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SESPlugin emulates Amazon SES v1, the Query API (Version 2010-12-01) the AWS CLI's `aws ses`,
// boto3's "ses" client and Terraform's aws_ses_* resources call.
//
// It shares its identities with [SESv2Plugin]: both read and write the same records in the
// sesv2 namespace, so a domain verified through VerifyDomainIdentity is the identity SES v2's
// GetEmailIdentity reports, and one SES v2 created is listed by ListIdentities — one account's
// identities, as AWS holds them. Verification itself happens outside the API, so its status is
// seeded (ses_verification.go). Email is recorded, never sent.
type SESPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// recordMu serializes every read-modify-write of a shared SES record: an identity, the
	// identity index, and the captured-email index. It is the lock [SESv2Plugin] holds for
	// the same records when the two are registered together, so a v1 Set* and a v2 delete of
	// one identity cannot interleave, and neither can two sends' index updates.
	recordMu *sync.Mutex
}

// sesXMLNS is the SES v1 model's xmlNamespace.
const sesXMLNS = "http://ses.amazonaws.com/doc/2010-12-01/"

// sesRecordLockOption is the plugin option carrying the *sync.Mutex SES v1 and SES v2 share
// for their identity records and captured-email index.
const sesRecordLockOption = "ses_record_lock"

// Name returns the service name "ses".
func (p *SESPlugin) Name() string { return "ses" }

// Initialize sets up the SESPlugin with the provided configuration.
func (p *SESPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.recordMu = sesRecordLock(cfg.Options)
	return nil
}

// sesRecordLock returns the shared record lock from options, or a lock of the plugin's own
// when it was registered alone.
func sesRecordLock(options map[string]any) *sync.Mutex {
	if mu, ok := options[sesRecordLockOption].(*sync.Mutex); ok && mu != nil {
		return mu
	}
	return &sync.Mutex{}
}

// Shutdown is a no-op for SESPlugin.
func (p *SESPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an SES v1 Query request on its Action.
func (p *SESPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	case "VerifyDomainIdentity":
		return p.verifyDomainIdentity(ctx, req)
	case "VerifyEmailIdentity":
		return p.verifyEmailIdentity(ctx, req, "VerifyEmailIdentity")
	case "VerifyEmailAddress":
		return p.verifyEmailIdentity(ctx, req, "VerifyEmailAddress")
	case "VerifyDomainDkim":
		return p.verifyDomainDkim(ctx, req)
	case "SetIdentityDkimEnabled":
		return p.setIdentityDkimEnabled(ctx, req)
	case "GetIdentityVerificationAttributes":
		return p.getIdentityVerificationAttributes(ctx, req)
	case "GetIdentityDkimAttributes":
		return p.getIdentityDkimAttributes(ctx, req)
	case "GetIdentityMailFromDomainAttributes":
		return p.getIdentityMailFromDomainAttributes(ctx, req)
	case "GetIdentityNotificationAttributes":
		return p.getIdentityNotificationAttributes(ctx, req)
	case "ListIdentities":
		return p.listIdentities(ctx, req)
	case "ListVerifiedEmailAddresses":
		return p.listVerifiedEmailAddresses(ctx)
	case "DeleteIdentity":
		return p.deleteIdentity(ctx, req, "DeleteIdentity", "Identity")
	case "DeleteVerifiedEmailAddress":
		return p.deleteIdentity(ctx, req, "DeleteVerifiedEmailAddress", "EmailAddress")
	case "SetIdentityMailFromDomain":
		return p.setIdentityMailFromDomain(ctx, req)
	case "SetIdentityNotificationTopic":
		return p.setIdentityNotificationTopic(ctx, req)
	case "SetIdentityFeedbackForwardingEnabled":
		return p.setIdentityFeedbackForwardingEnabled(ctx, req)
	case "SetIdentityHeadersInNotificationsEnabled":
		return p.setIdentityHeadersInNotificationsEnabled(ctx, req)
	case "SendEmail":
		return p.sendEmail(ctx, req)
	case "SendRawEmail":
		return p.sendRawEmail(ctx, req)
	case "GetSendQuota":
		return p.getSendQuota(ctx)
	case "GetSendStatistics":
		return p.getSendStatistics(ctx)
	default:
		return nil, unknownActionError(p.Name(), action)
	}
}

// SES v1's refusals. MissingParameter and InvalidParameterValue are both on SES's Common Error
// Types page at HTTP 400 (docs.aws.amazon.com/ses/latest/APIReference/CommonErrors.html), which
// is where an error belonging to no single operation is published; none of the identity
// operations publishes an error of its own. The messages are substrate's own.

// sesMissingParameter refuses a request that omits a required parameter.
func sesMissingParameter(name string) *AWSError {
	return &AWSError{Code: "MissingParameter", Message: "The request must contain the parameter " + name + ".", HTTPStatus: http.StatusBadRequest}
}

// sesInvalidParameter refuses a parameter whose value cannot be used.
func sesInvalidParameter(format string, args ...any) *AWSError {
	return &AWSError{Code: "InvalidParameterValue", Message: fmt.Sprintf(format, args...), HTTPStatus: http.StatusBadRequest}
}

// sesRequired returns the named parameter, or MissingParameter when it is absent or blank.
func sesRequired(req *AWSRequest, name string) (string, *AWSError) {
	v := strings.TrimSpace(req.Params[name])
	if v == "" {
		return "", sesMissingParameter(name)
	}
	return v, nil
}

// sesIdentityKey is an identity record's state key, the one [SESv2Plugin] uses.
func sesIdentityKey(reqCtx *RequestContext, identity string) string {
	return "identity:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + identity
}

// sesIdentityIndexKey is the key of the sorted index of a Region's identity names, the one
// [SESv2Plugin] maintains.
func sesIdentityIndexKey(reqCtx *RequestContext) string {
	return "identity_names:" + reqCtx.AccountID + "/" + reqCtx.Region
}

// sesIdentityType is SES v2's IdentityType for an identity name: an address carries an "@".
func sesIdentityType(identity string) string {
	if strings.Contains(identity, "@") {
		return "EMAIL_ADDRESS"
	}
	return "DOMAIN"
}

// loadIdentity reads an identity record, returning nil when there is none.
func (p *SESPlugin) loadIdentity(reqCtx *RequestContext, identity string) (*SESv2Identity, error) {
	return sesLoadIdentity(context.Background(), p.state, reqCtx.AccountID, reqCtx.Region, identity)
}

// sesLoadIdentity reads an identity record from state, returning nil when there is none. It
// is the read both SES plugins share.
func sesLoadIdentity(ctx context.Context, state StateManager, accountID, region, identity string) (*SESv2Identity, error) {
	data, err := state.Get(ctx, sesv2Namespace, "identity:"+accountID+"/"+region+"/"+identity)
	if err != nil {
		return nil, fmt.Errorf("ses load identity %s: %w", identity, err)
	}
	if data == nil {
		return nil, nil
	}
	var rec SESv2Identity
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("ses decode identity %s: %w", identity, err)
	}
	return &rec, nil
}

// saveIdentity writes an identity record and adds it to the Region's index.
func (p *SESPlugin) saveIdentity(reqCtx *RequestContext, rec *SESv2Identity) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ses encode identity %s: %w", rec.IdentityName, err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, sesv2Namespace, sesIdentityKey(reqCtx, rec.IdentityName), data); err != nil {
		return fmt.Errorf("ses put identity %s: %w", rec.IdentityName, err)
	}
	if err := updateStringIndex(goCtx, p.state, sesv2Namespace, sesIdentityIndexKey(reqCtx), rec.IdentityName); err != nil {
		return fmt.Errorf("ses index identity %s: %w", rec.IdentityName, err)
	}
	return nil
}

// ensureIdentity returns identity's record, creating it when there is none. A Verify* call on an
// identity the account already holds starts verification again and answers as it did the first
// time, so it is not an error.
func (p *SESPlugin) ensureIdentity(reqCtx *RequestContext, identity string) (*SESv2Identity, error) {
	rec, err := p.loadIdentity(reqCtx, identity)
	if err != nil || rec != nil {
		return rec, err
	}
	rec = &SESv2Identity{
		IdentityName: identity,
		IdentityType: sesIdentityType(identity),
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
		CreatedAt:    p.tc.Now(),
	}
	if err := p.saveIdentity(reqCtx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// updateIdentity applies change to an existing identity's record under the record lock. An
// identity the account does not hold is refused with InvalidParameterValue, and so is an ARN
// naming another account or Region ([sesResolveIdentity]). change may read other records,
// which it does under the same lock; an error it returns is returned as is and nothing is
// written.
func (p *SESPlugin) updateIdentity(reqCtx *RequestContext, identity string, change func(*SESv2Identity) error) error {
	name, held := sesResolveIdentity(reqCtx, identity)
	if !held {
		return sesInvalidParameter("Identity %s does not exist.", identity)
	}
	p.recordMu.Lock()
	defer p.recordMu.Unlock()
	rec, err := p.loadIdentity(reqCtx, name)
	if err != nil {
		return err
	}
	if rec == nil {
		return sesInvalidParameter("Identity %s does not exist.", identity)
	}
	if err := change(rec); err != nil {
		return err
	}
	return p.saveIdentity(reqCtx, rec)
}

func (p *SESPlugin) verifyDomainIdentity(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	domain, awsErr := sesRequired(req, "Domain")
	if awsErr != nil {
		return nil, awsErr
	}
	if strings.Contains(domain, "@") {
		return nil, sesInvalidParameter("Invalid domain name %s.", domain)
	}
	p.recordMu.Lock()
	_, err := p.ensureIdentity(reqCtx, domain)
	p.recordMu.Unlock()
	if err != nil {
		return nil, err
	}
	type result struct {
		VerificationToken string `xml:"VerificationToken"`
	}
	return queryOKResponse(reqCtx, "VerifyDomainIdentity", sesXMLNS,
		result{VerificationToken: sesVerificationToken(reqCtx.AccountID, reqCtx.Region, domain)})
}

// verifyEmailIdentity serves VerifyEmailIdentity and its predecessor VerifyEmailAddress, which
// take the same EmailAddress. VerifyEmailIdentity answers an empty result element; the older
// operation's model declares no output at all, so its response carries none.
func (p *SESPlugin) verifyEmailIdentity(reqCtx *RequestContext, req *AWSRequest, op string) (*AWSResponse, error) {
	address, awsErr := sesRequired(req, "EmailAddress")
	if awsErr != nil {
		return nil, awsErr
	}
	if !strings.Contains(address, "@") {
		return nil, sesInvalidParameter("Invalid email address %s.", address)
	}
	p.recordMu.Lock()
	_, err := p.ensureIdentity(reqCtx, address)
	p.recordMu.Unlock()
	if err != nil {
		return nil, err
	}
	if op == "VerifyEmailAddress" {
		return queryOKResponse(reqCtx, op, sesXMLNS, nil)
	}
	return queryOKResponse(reqCtx, op, sesXMLNS, struct{}{})
}

// sesTokenList is a VerificationTokenList: a Query list of member elements.
type sesTokenList struct {
	Members []string `xml:"member"`
}

func (p *SESPlugin) verifyDomainDkim(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	domain, awsErr := sesRequired(req, "Domain")
	if awsErr != nil {
		return nil, awsErr
	}
	if strings.Contains(domain, "@") {
		return nil, sesInvalidParameter("Invalid domain name %s.", domain)
	}
	tokens := sesDkimTokens(reqCtx.AccountID, reqCtx.Region, domain)
	p.recordMu.Lock()
	rec, err := p.ensureIdentity(reqCtx, domain)
	if err == nil {
		rec.DkimTokens = tokens
		rec.DkimDisabled = false
		err = p.saveIdentity(reqCtx, rec)
	}
	p.recordMu.Unlock()
	if err != nil {
		return nil, err
	}
	type result struct {
		DkimTokens sesTokenList `xml:"DkimTokens"`
	}
	return queryOKResponse(reqCtx, "VerifyDomainDkim", sesXMLNS, result{DkimTokens: sesTokenList{Members: tokens}})
}

func (p *SESPlugin) setIdentityDkimEnabled(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, "Identity")
	if awsErr != nil {
		return nil, awsErr
	}
	enabled, awsErr := sesRequiredBool(req, "DkimEnabled")
	if awsErr != nil {
		return nil, awsErr
	}
	// An address can be enabled only "if the corresponding domain ... has been set up to use
	// Easy DKIM" (the operation's page), so its tokens are the domain's. The domain is read
	// inside the change, under the record lock updateIdentity holds, so a concurrent delete or
	// VerifyDomainDkim of the domain is ordered against this call the same way live and on
	// replay.
	if err := p.updateIdentity(reqCtx, identity, func(rec *SESv2Identity) error {
		setUp := len(rec.DkimTokens) > 0
		if rec.IdentityType == "EMAIL_ADDRESS" {
			domain, err := p.loadIdentity(reqCtx, sesIdentityDomain(rec.IdentityName))
			if err != nil {
				return err
			}
			setUp = domain != nil && len(domain.DkimTokens) > 0
		}
		if enabled && !setUp {
			return sesInvalidParameter("Easy DKIM is not set up for %s; call VerifyDomainDkim for its domain first.", rec.IdentityName)
		}
		rec.DkimDisabled = !enabled
		return nil
	}); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SetIdentityDkimEnabled", sesXMLNS, struct{}{})
}

// sesResolveIdentity resolves an identity parameter to the name of an identity in the caller's
// account and Region. It may be the name itself or the identity's ARN,
// `arn:{partition}:ses:{region}:{account}:identity/{name}`, which SetIdentityNotificationTopic's
// page lists among an identity's valid forms ("arn:aws:ses:us-east-1:123456789012:identity/
// example.com"). held is false for an ARN that is malformed, names another service, or names
// another account or Region: no published source says what SES answers for one, so it is read
// as an identity the caller does not hold, which every operation already answers.
func sesResolveIdentity(reqCtx *RequestContext, identity string) (name string, held bool) {
	if !strings.HasPrefix(identity, "arn:") {
		return identity, true
	}
	parts := strings.SplitN(identity, ":", 6)
	if len(parts) != 6 || parts[2] != "ses" || !strings.HasPrefix(parts[5], "identity/") {
		return identity, false
	}
	name = strings.TrimPrefix(parts[5], "identity/")
	return name, name != "" && parts[3] == reqCtx.Region && parts[4] == reqCtx.AccountID
}

// sesRequiredBool reads a required boolean parameter, which the Query protocol sends as
// "true" or "false".
func sesRequiredBool(req *AWSRequest, name string) (bool, *AWSError) {
	v, awsErr := sesRequired(req, name)
	if awsErr != nil {
		return false, awsErr
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, sesInvalidParameter("Value %s for parameter %s is not a boolean.", v, name)
	}
	return b, nil
}

// listIdentities serves ListIdentities: every identity in the Region, in name order, of the
// IdentityType asked for. MaxItems is 1-1000 inclusive, as its page states, and defaults to the
// maximum; NextToken is a base64 cursor naming the last identity of the previous page.
func (p *SESPlugin) listIdentities(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	wantType := ""
	switch t := req.Params["IdentityType"]; t {
	case "":
	case "EmailAddress":
		wantType = "EMAIL_ADDRESS"
	case "Domain":
		wantType = "DOMAIN"
	default:
		return nil, sesInvalidParameter("Invalid IdentityType %s; valid values are EmailAddress and Domain.", t)
	}
	maxItems := 1000
	if v := req.Params["MaxItems"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			return nil, sesInvalidParameter("MaxItems must be between 1 and 1000 inclusive.")
		}
		maxItems = n
	}
	cursor := ""
	if tok := req.Params["NextToken"]; tok != "" {
		raw, err := base64.StdEncoding.DecodeString(tok)
		if err != nil || len(raw) == 0 {
			return nil, sesInvalidParameter("The NextToken %s is not valid.", tok)
		}
		cursor = string(raw)
	}

	names, err := loadStringIndex(context.Background(), p.state, sesv2Namespace, sesIdentityIndexKey(reqCtx))
	if err != nil {
		return nil, fmt.Errorf("ses listIdentities: %w", err)
	}
	var page []string
	next := ""
	for _, name := range names {
		if cursor != "" && name <= cursor {
			continue
		}
		if wantType != "" && sesIdentityType(name) != wantType {
			continue
		}
		if len(page) == maxItems {
			next = base64.StdEncoding.EncodeToString([]byte(page[len(page)-1]))
			break
		}
		page = append(page, name)
	}
	type result struct {
		Identities sesTokenList `xml:"Identities"`
		NextToken  string       `xml:"NextToken,omitempty"`
	}
	return queryOKResponse(reqCtx, "ListIdentities", sesXMLNS, result{Identities: sesTokenList{Members: page}, NextToken: next})
}

// listVerifiedEmailAddresses serves the deprecated ListVerifiedEmailAddresses: the email
// address identities whose status is Success.
func (p *SESPlugin) listVerifiedEmailAddresses(reqCtx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, sesv2Namespace, sesIdentityIndexKey(reqCtx))
	if err != nil {
		return nil, fmt.Errorf("ses listVerifiedEmailAddresses: %w", err)
	}
	var verified []string
	for _, name := range names {
		if sesIdentityType(name) != "EMAIL_ADDRESS" {
			continue
		}
		status, err := sesVerificationStatus(goCtx, p.state, reqCtx.AccountID, reqCtx.Region, name)
		if err != nil {
			return nil, err
		}
		if status == "Success" {
			verified = append(verified, name)
		}
	}
	type result struct {
		VerifiedEmailAddresses sesTokenList `xml:"VerifiedEmailAddresses"`
	}
	return queryOKResponse(reqCtx, "ListVerifiedEmailAddresses", sesXMLNS, result{VerifiedEmailAddresses: sesTokenList{Members: verified}})
}

// deleteIdentity serves DeleteIdentity and the deprecated DeleteVerifiedEmailAddress. Deleting
// an identity the account does not hold succeeds, as both pages publish no error for it.
// DeleteVerifiedEmailAddress's model declares no output, so its response carries no result.
func (p *SESPlugin) deleteIdentity(reqCtx *RequestContext, req *AWSRequest, op, param string) (*AWSResponse, error) {
	identity, awsErr := sesRequired(req, param)
	if awsErr != nil {
		return nil, awsErr
	}
	// An identity the caller does not hold, an ARN naming another account or Region included,
	// is deleted by doing nothing.
	if name, held := sesResolveIdentity(reqCtx, identity); held {
		goCtx := context.Background()
		p.recordMu.Lock()
		err := p.state.Delete(goCtx, sesv2Namespace, sesIdentityKey(reqCtx, name))
		if err == nil {
			err = removeFromStringIndex(goCtx, p.state, sesv2Namespace, sesIdentityIndexKey(reqCtx), name)
		}
		p.recordMu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("ses %s %s: %w", op, identity, err)
		}
	}
	if op == "DeleteVerifiedEmailAddress" {
		return queryOKResponse(reqCtx, op, sesXMLNS, nil)
	}
	return queryOKResponse(reqCtx, op, sesXMLNS, struct{}{})
}
