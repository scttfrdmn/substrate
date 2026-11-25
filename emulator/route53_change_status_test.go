package emulator_test

import (
	"html"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetChange and its seeded PENDING→INSYNC window (#1613). Every assertion reads the raw XML a
// waiter reads: the Status of the ChangeInfo a mint answers and the one GetChange answers.

// r53Call issues one signed Route 53 request against ts and returns the status and body.
func r53Call(t *testing.T, ts *emulator.TestServer, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Host = "route53.amazonaws.com"
	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1"+
		"/route53/aws4_request, SignedHeaders=host, Signature=fake")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // drained below.
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

var (
	r53ChangeIDRe     = regexp.MustCompile(`<ChangeInfo><Id>([^<]+)</Id><Status>([^<]+)</Status>`)
	r53ZoneIDRe       = regexp.MustCompile(`<Id>/hostedzone/([^<]+)</Id>`)
	r53ErrorCodeRe    = regexp.MustCompile(`<Code>([^<]+)</Code>`)
	r53ErrorMessageRe = regexp.MustCompile(`<Message>([^<]*)</Message>`)
)

// r53ErrorOf returns the code and message of an error body.
func r53ErrorOf(body string) (code, message string) {
	if m := r53ErrorCodeRe.FindStringSubmatch(body); m != nil {
		code = m[1]
	}
	if m := r53ErrorMessageRe.FindStringSubmatch(body); m != nil {
		message = html.UnescapeString(m[1])
	}
	return code, message
}

// r53CreateZoneOn creates a zone named name and returns its bare ID and the change it minted.
func r53CreateZoneOn(t *testing.T, ts *emulator.TestServer, name, ref string) (zoneID, changeID, status string) {
	t.Helper()
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>`+name+
			`</Name><CallerReference>`+ref+`</CallerReference></CreateHostedZoneRequest>`)
	require.Equal(t, http.StatusCreated, code, body)
	change := r53ChangeIDRe.FindStringSubmatch(body)
	require.NotNil(t, change, body)
	return r53ZoneIDRe.FindStringSubmatch(body)[1], change[1], change[2]
}

// r53ChangeBody wraps changes in a ChangeResourceRecordSets request.
func r53ChangeBody(changes ...string) string {
	return `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Comment>c</Comment><Changes>` +
		strings.Join(changes, "") + `</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
}

// r53A renders one change of an A record set with a TTL of 60 and the given values; extra is
// inserted after the Type, for a SetIdentifier and Weight.
func r53A(action, name, extra string, values ...string) string {
	var rr strings.Builder
	for _, v := range values {
		rr.WriteString(`<ResourceRecord><Value>` + v + `</Value></ResourceRecord>`)
	}
	return `<Change><Action>` + action + `</Action><ResourceRecordSet><Name>` + name + `</Name><Type>A</Type>` + extra +
		`<TTL>60</TTL><ResourceRecords>` + rr.String() + `</ResourceRecords></ResourceRecordSet></Change>`
}

// r53GetChangeStatus answers the Status one GetChange of id reports.
func r53GetChangeStatus(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/change/"+id, "")
	require.Equal(t, http.StatusOK, code, body)
	m := regexp.MustCompile(`<Status>([^<]+)</Status>`).FindStringSubmatch(body)
	require.NotNil(t, m, body)
	return m[1]
}

func TestRoute53Change_AnUnseededChangeIsInSyncAndCanBeRead(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, createChange, status := r53CreateZoneOn(t, ts, "change.example.com", "c-1")
	assert.Equal(t, "INSYNC", status)
	assert.True(t, strings.HasPrefix(createChange, "/change/C"), createChange)

	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("CREATE", "www.change.example.com", "", "192.0.2.1")))
	require.Equal(t, http.StatusOK, code, body)
	batch := r53ChangeIDRe.FindStringSubmatch(body)
	require.NotNil(t, batch, body)
	assert.Equal(t, "INSYNC", batch[2])

	for _, id := range []string{createChange, batch[1]} {
		bare := strings.TrimPrefix(id, "/change/")
		assert.Equal(t, "INSYNC", r53GetChangeStatus(t, ts, bare), "the bare ID %s", bare)
	}
	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/change/"+strings.TrimPrefix(batch[1], "/"), "")
	require.Equal(t, http.StatusOK, code, "the ID with its change/ prefix: %s", body)
	assert.Contains(t, body, "<Comment>c</Comment>", "the batch's Comment is stored with the change")
	assert.Contains(t, body, "<Id>"+batch[1]+"</Id>")

	// DeleteHostedZone mints a change too.
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("DELETE", "www.change.example.com", "", "192.0.2.1")))
	require.Equal(t, http.StatusOK, code, body)
	code, body = r53Call(t, ts, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, code, body)
	del := r53ChangeIDRe.FindStringSubmatch(body)
	require.NotNil(t, del, body)
	assert.Equal(t, "INSYNC", r53GetChangeStatus(t, ts, strings.TrimPrefix(del[1], "/change/")))
}

func TestRoute53Change_AnUnknownChangeIsNoSuchChange(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/change/CNOSUCHCHANGE", "")
	assert.Equal(t, http.StatusNotFound, code)
	gotCode, msg := r53ErrorOf(body)
	assert.Equal(t, "NoSuchChange", gotCode)
	assert.Equal(t, "A change with the specified change ID does not exist.", msg)
}

func TestRoute53Change_ASeededWindowReportsPendingThenInSync(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, got := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/route53/change-status",
		[]byte(`{"changeId":"*","pendingObservations":2}`))
	require.Equal(t, http.StatusOK, status, got)

	_, first, created := r53CreateZoneOn(t, ts, "pending.example.com", "p-1")
	assert.Equal(t, "PENDING", created, "the create's response peeks at the first observation")
	_, second, _ := r53CreateZoneOn(t, ts, "pending2.example.com", "p-2")

	firstID := strings.TrimPrefix(first, "/change/")
	assert.Equal(t, "PENDING", r53GetChangeStatus(t, ts, firstID))
	assert.Equal(t, "PENDING", r53GetChangeStatus(t, ts, firstID))
	assert.Equal(t, "INSYNC", r53GetChangeStatus(t, ts, firstID))
	assert.Equal(t, "INSYNC", r53GetChangeStatus(t, ts, firstID), "a settled change stays settled")
	assert.Equal(t, "PENDING", r53GetChangeStatus(t, ts, strings.TrimPrefix(second, "/change/")),
		"each change has its own countdown under the wildcard")

	// A seed for one change, named with its prefix, governs that change alone.
	status, got = cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/route53/change-status", nil)
	require.Equal(t, http.StatusOK, status, got)
	_, third, created := r53CreateZoneOn(t, ts, "pending3.example.com", "p-3")
	assert.Equal(t, "INSYNC", created, "clearing every seed makes every change INSYNC again")
	status, got = cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/route53/change-status",
		[]byte(`{"changeId":"`+third+`","pendingObservations":1}`))
	require.Equal(t, http.StatusOK, status, got)
	thirdID := strings.TrimPrefix(third, "/change/")
	assert.Equal(t, "PENDING", r53GetChangeStatus(t, ts, thirdID))
	assert.Equal(t, "INSYNC", r53GetChangeStatus(t, ts, thirdID))

	status, got = cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/route53/change-status?changeId="+third, nil)
	require.Equal(t, http.StatusOK, status, got)
	status, got = cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/route53/change-status",
		[]byte(`{"changeId":"x","pendingObservations":-1}`))
	assert.Equal(t, http.StatusBadRequest, status, "a negative count is refused: %s", got)
}
