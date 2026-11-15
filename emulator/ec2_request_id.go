package emulator

import (
	"bytes"
	"encoding/xml"
)

// ec2StampRequestID puts ctx.RequestID into an EC2 success document as its first child,
// `<requestId>…</requestId>`, the element every EC2 operation publishes among its Response
// Elements — "requestId: The ID of the request" — and that every sample response opens with
// (#1189).
//
// # Why at the boundary rather than in each response struct
//
// EC2 renders about seventy response structs, all through [ec2XMLResponse], which has no request
// context to read the ID from. Threading the ID through every struct literal would put the
// element under type checking, but it is seventy edits that each have to remember a field whose
// value is the same on every one, and a struct added later could forget it. Stamping once, here,
// at [EC2Plugin.HandleRequest], is the trade [iamStampRequestID] made for IAM (#1413): one place
// that cannot be forgotten, at the cost of operating on bytes rather than types. The cost is
// bounded by what is stamped — only a 2xx body whose root element is an `…Response` document
// — and by what the stamp does, which is to insert one escaped element after the root's start
// tag.
//
// The value is the request's own ID, the one [Event.RequestID] records and a replay is
// dispatched with, never a freshly minted one, so a replayed EC2 call renders the body it
// recorded. An internal dispatch carrying no request ID renders no element, rather than an empty
// one. Error documents are not stamped here: the Query error writer already renders the request's
// ID as `<RequestID>`, which is EC2's published error shape (errors-overview.html, "Example error
// response").
//
// A document whose first child is already a requestId — a handler that rendered its own — is
// left as it is.
func ec2StampRequestID(resp *AWSResponse, ctx *RequestContext) *AWSResponse {
	if resp == nil || ctx == nil || ctx.RequestID == "" || resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp
	}
	body := resp.Body
	start := 0
	if bytes.HasPrefix(body, []byte(xml.Header)) {
		start = len(xml.Header)
	}
	rest := body[start:]
	if len(rest) == 0 || rest[0] != '<' {
		return resp
	}
	end := bytes.IndexByte(rest, '>')
	if end < 1 || rest[end-1] == '/' {
		return resp
	}
	nameEnd := bytes.IndexAny(rest[1:end], " \t\r\n")
	if nameEnd < 0 {
		nameEnd = end - 1
	}
	if !bytes.HasSuffix(rest[1:1+nameEnd], []byte("Response")) {
		return resp
	}
	at := start + end + 1
	if bytes.HasPrefix(body[at:], []byte("<requestId>")) {
		return resp
	}
	element := "<requestId>" + xmlEsc(ctx.RequestID) + "</requestId>"
	stamped := make([]byte, 0, len(body)+len(element))
	stamped = append(stamped, body[:at]...)
	stamped = append(stamped, element...)
	stamped = append(stamped, body[at:]...)
	resp.Body = stamped
	return resp
}
