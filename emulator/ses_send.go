package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// SES v1 sending: SendEmail and SendRawEmail record the message and answer a MessageId; nothing
// is delivered. A sender is not refused for being unverified, because SES v2's SendEmail does
// not model the sandbox either and the two APIs answer one account's sending the same way.
// GetSendQuota and GetSendStatistics are computed from the recorded messages.

// sesSandboxMax24HourSend and sesSandboxMaxSendRate are the sandbox's sending quota: "you can
// send a maximum of 200 messages per 24-hour period" and "a maximum of 1 message per second"
// (SES developer guide, "Request production access (Moving out of the Amazon SES sandbox)").
const (
	sesSandboxMax24HourSend = 200
	sesSandboxMaxSendRate   = 1
)

// sesMessageID mints a MessageId in the shape SES issues, a 16-digit hex prefix, a UUID and a
// "-000000" suffix ("0000014a8a2b8b8f-…-000000").
func sesMessageID(reqCtx *RequestContext) string {
	return reqCtx.IDs.Hex(8) + "-" + reqCtx.IDs.UUID() + "-000000"
}

// captureEmail records email under the keys SES v2's SendEmail uses, so GET /v1/emails lists it.
func (p *SESPlugin) captureEmail(reqCtx *RequestContext, email SESv2CapturedEmail) error {
	data, err := json.Marshal(email)
	if err != nil {
		return fmt.Errorf("ses encode email: %w", err)
	}
	goCtx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Put(goCtx, sesv2Namespace, "captured_email:"+scope+"/"+email.MessageID, data); err != nil {
		return fmt.Errorf("ses put email: %w", err)
	}
	// The index is read, appended to and written back, and SES v2's SendEmail does the same to
	// the same index, so both hold the shared record lock or concurrent sends lose IDs.
	p.recordMu.Lock()
	err = updateStringIndex(goCtx, p.state, sesv2Namespace, "captured_email_ids:"+scope, email.MessageID)
	p.recordMu.Unlock()
	if err != nil {
		return fmt.Errorf("ses index email: %w", err)
	}
	return nil
}

// sesMessageIDResult is SendEmailResult and SendRawEmailResult.
type sesMessageIDResult struct {
	MessageID string `xml:"MessageId"`
}

func (p *SESPlugin) sendEmail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	source, awsErr := sesRequired(req, "Source")
	if awsErr != nil {
		return nil, awsErr
	}
	to := iamMemberList(req.Params, "Destination.ToAddresses")
	cc := iamMemberList(req.Params, "Destination.CcAddresses")
	bcc := iamMemberList(req.Params, "Destination.BccAddresses")
	if len(to)+len(cc)+len(bcc) == 0 {
		return nil, sesMissingParameter("Destination")
	}
	subject, awsErr := sesRequired(req, "Message.Subject.Data")
	if awsErr != nil {
		return nil, awsErr
	}
	body := req.Params["Message.Body.Text.Data"]
	if body == "" {
		body = req.Params["Message.Body.Html.Data"]
	}
	email := SESv2CapturedEmail{
		MessageID: sesMessageID(reqCtx),
		To:        to,
		Cc:        cc,
		Bcc:       bcc,
		From:      source,
		Subject:   subject,
		Body:      body,
		SentAt:    p.tc.Now(),
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
	}
	if err := p.captureEmail(reqCtx, email); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SendEmail", sesXMLNS, sesMessageIDResult{MessageID: email.MessageID})
}

// sendRawEmail records a raw MIME message. Its recipients are Destinations.member.N; substrate
// does not parse the MIME headers for more, since what the message says is the workload, and
// only what the call carried is observable.
func (p *SESPlugin) sendRawEmail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	raw, awsErr := sesRequired(req, "RawMessage.Data")
	if awsErr != nil {
		return nil, awsErr
	}
	email := SESv2CapturedEmail{
		MessageID:  sesMessageID(reqCtx),
		To:         iamMemberList(req.Params, "Destinations"),
		From:       req.Params["Source"],
		RawMessage: raw,
		SentAt:     p.tc.Now(),
		AccountID:  reqCtx.AccountID,
		Region:     reqCtx.Region,
	}
	if err := p.captureEmail(reqCtx, email); err != nil {
		return nil, err
	}
	return queryOKResponse(reqCtx, "SendRawEmail", sesXMLNS, sesMessageIDResult{MessageID: email.MessageID})
}

// capturedEmails returns the Region's recorded messages, from either SES API.
func (p *SESPlugin) capturedEmails(reqCtx *RequestContext) ([]SESv2CapturedEmail, error) {
	goCtx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	ids, err := loadStringIndex(goCtx, p.state, sesv2Namespace, "captured_email_ids:"+scope)
	if err != nil {
		return nil, fmt.Errorf("ses load emails: %w", err)
	}
	var out []SESv2CapturedEmail
	for _, id := range ids {
		data, err := p.state.Get(goCtx, sesv2Namespace, "captured_email:"+scope+"/"+id)
		if err != nil {
			return nil, fmt.Errorf("ses load email %s: %w", id, err)
		}
		if data == nil {
			continue
		}
		var email SESv2CapturedEmail
		if err := json.Unmarshal(data, &email); err != nil {
			return nil, fmt.Errorf("ses decode email %s: %w", id, err)
		}
		out = append(out, email)
	}
	return out, nil
}

// sesRecipients is the number of recipients a message counts against the quota: "the total
// number of recipients (including each recipient in the To:, CC: and BCC: fields)", per
// SendEmail's page.
func sesRecipients(email SESv2CapturedEmail) int {
	return len(email.To) + len(email.Cc) + len(email.Bcc)
}

// getSendQuota serves GetSendQuota: the sandbox quota, and the recipients of the messages
// recorded in the 24 hours before the simulated clock's now.
func (p *SESPlugin) getSendQuota(reqCtx *RequestContext) (*AWSResponse, error) {
	emails, err := p.capturedEmails(reqCtx)
	if err != nil {
		return nil, err
	}
	now := p.tc.Now()
	sent := 0
	for _, email := range emails {
		if !email.SentAt.After(now) && now.Sub(email.SentAt) < 24*time.Hour {
			sent += sesRecipients(email)
		}
	}
	type result struct {
		Max24HourSend   float64 `xml:"Max24HourSend"`
		MaxSendRate     float64 `xml:"MaxSendRate"`
		SentLast24Hours float64 `xml:"SentLast24Hours"`
	}
	return queryOKResponse(reqCtx, "GetSendQuota", sesXMLNS, result{
		Max24HourSend:   sesSandboxMax24HourSend,
		MaxSendRate:     sesSandboxMaxSendRate,
		SentLast24Hours: float64(sent),
	})
}

// sesSendDataPoint is a SendDataPoint. Bounces, complaints and rejects are zero: nothing is
// delivered, so nothing bounces.
type sesSendDataPoint struct {
	Timestamp        string `xml:"Timestamp"`
	DeliveryAttempts int64  `xml:"DeliveryAttempts"`
	Bounces          int64  `xml:"Bounces"`
	Complaints       int64  `xml:"Complaints"`
	Rejects          int64  `xml:"Rejects"`
}

// getSendStatistics serves GetSendStatistics: data points "representing the last two weeks of
// sending activity", where "Each data point in the list contains statistics for a 15-minute
// period of time" (the operation's page). A period with no messages has no data point, and
// points are in time order; the page says neither, so both are substrate's reading.
func (p *SESPlugin) getSendStatistics(reqCtx *RequestContext) (*AWSResponse, error) {
	emails, err := p.capturedEmails(reqCtx)
	if err != nil {
		return nil, err
	}
	now := p.tc.Now()
	buckets := map[time.Time]int64{}
	for _, email := range emails {
		if email.SentAt.After(now) || now.Sub(email.SentAt) >= 14*24*time.Hour {
			continue
		}
		buckets[email.SentAt.UTC().Truncate(15*time.Minute)] += int64(sesRecipients(email))
	}
	starts := make([]time.Time, 0, len(buckets))
	for t := range buckets {
		starts = append(starts, t)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	type pointList struct {
		Members []sesSendDataPoint `xml:"member"`
	}
	var points pointList
	for _, t := range starts {
		points.Members = append(points.Members, sesSendDataPoint{
			Timestamp:        t.Format(time.RFC3339),
			DeliveryAttempts: buckets[t],
		})
	}
	type result struct {
		SendDataPoints pointList `xml:"SendDataPoints"`
	}
	return queryOKResponse(reqCtx, "GetSendStatistics", sesXMLNS, result{SendDataPoints: points})
}
