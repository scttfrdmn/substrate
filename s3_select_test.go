package substrate_test

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"testing"
)

// decodeEventFrames parses a binary AWS event stream into a slice of frames,
// each represented as a map of header name → value plus the raw payload bytes.
type eventFrame struct {
	headers map[string]string
	payload []byte
}

func decodeEventFrames(t *testing.T, data []byte) []eventFrame {
	t.Helper()
	var frames []eventFrame
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		if r.Len() < 16 {
			t.Fatalf("truncated event stream: %d bytes remaining", r.Len())
		}
		var totalLen, headerLen uint32
		if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
			t.Fatalf("read totalLen: %v", err)
		}
		if err := binary.Read(r, binary.BigEndian, &headerLen); err != nil {
			t.Fatalf("read headerLen: %v", err)
		}
		// Skip prelude CRC (4 bytes).
		var preludeCRC uint32
		if err := binary.Read(r, binary.BigEndian, &preludeCRC); err != nil {
			t.Fatalf("read preludeCRC: %v", err)
		}
		// Read headers.
		hdrData := make([]byte, headerLen)
		if _, err := r.Read(hdrData); err != nil {
			t.Fatalf("read headers: %v", err)
		}
		headers := parseEventHeaders(t, hdrData)
		// Payload = totalLen - 16 (prelude+preludeCRC+msgCRC) - headerLen.
		payloadLen := int(totalLen) - 16 - int(headerLen)
		if payloadLen < 0 {
			t.Fatalf("negative payload length: %d", payloadLen)
		}
		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := r.Read(payload); err != nil {
				t.Fatalf("read payload: %v", err)
			}
		}
		// Skip message CRC (4 bytes).
		var msgCRC uint32
		if err := binary.Read(r, binary.BigEndian, &msgCRC); err != nil {
			t.Fatalf("read msgCRC: %v", err)
		}
		frames = append(frames, eventFrame{headers: headers, payload: payload})
	}
	return frames
}

func parseEventHeaders(t *testing.T, data []byte) map[string]string {
	t.Helper()
	headers := make(map[string]string)
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		nameLen, err := r.ReadByte()
		if err != nil {
			t.Fatalf("read header name len: %v", err)
		}
		name := make([]byte, nameLen)
		if _, err := r.Read(name); err != nil {
			t.Fatalf("read header name: %v", err)
		}
		// Skip type byte.
		if _, err := r.ReadByte(); err != nil {
			t.Fatalf("read header type: %v", err)
		}
		var valLen uint16
		if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
			t.Fatalf("read header value len: %v", err)
		}
		val := make([]byte, valLen)
		if valLen > 0 {
			if _, err := r.Read(val); err != nil {
				t.Fatalf("read header value: %v", err)
			}
		}
		headers[string(name)] = string(val)
	}
	return headers
}

// s3SelectXMLRequest builds the XML request body for SelectObjectContent.
func s3SelectXMLRequest(expression, inputFormat string) []byte {
	type csvInput struct {
		FileHeaderInfo string `xml:"FileHeaderInfo"`
	}
	type jsonInput struct {
		Type string `xml:"Type"`
	}
	type inputSerialization struct {
		CSV  *csvInput  `xml:"CSV"`
		JSON *jsonInput `xml:"JSON"`
	}
	type jsonOutput struct {
		RecordDelimiter string `xml:"RecordDelimiter"`
	}
	type outputSerialization struct {
		JSON *jsonOutput `xml:"JSON"`
	}
	type request struct {
		XMLName            xml.Name            `xml:"SelectObjectContentRequest"`
		Expression         string              `xml:"Expression"`
		ExpressionType     string              `xml:"ExpressionType"`
		InputSerialization inputSerialization  `xml:"InputSerialization"`
		OutputSerialization outputSerialization `xml:"OutputSerialization"`
	}
	req := request{
		Expression:     expression,
		ExpressionType: "SQL",
		OutputSerialization: outputSerialization{
			JSON: &jsonOutput{RecordDelimiter: "\n"},
		},
	}
	switch inputFormat {
	case "CSV":
		req.InputSerialization = inputSerialization{CSV: &csvInput{FileHeaderInfo: "USE"}}
	default:
		req.InputSerialization = inputSerialization{JSON: &jsonInput{Type: "LINES"}}
	}
	b, err := xml.Marshal(req)
	if err != nil {
		panic(fmt.Sprintf("marshal select request: %v", err))
	}
	return b
}

// TestS3Select_CSV_AllRows verifies SelectObjectContent on a CSV object with no filter.
func TestS3Select_CSV_AllRows(t *testing.T) {
	srv, _ := newS3TestServer(t)

	// Create bucket and put CSV object.
	s3Request(t, srv, "PUT", "/sel-bucket", nil, nil)
	csvData := []byte("name,age\nAlice,30\nBob,25\n")
	s3Request(t, srv, "PUT", "/sel-bucket/data.csv", csvData, map[string]string{
		"Content-Type": "text/csv",
	})

	// Issue SelectObjectContent.
	reqBody := s3SelectXMLRequest("SELECT * FROM S3Object", "CSV")
	w := s3Request(t, srv, "POST", "/sel-bucket/data.csv?select&select-type=2", reqBody, map[string]string{
		"Content-Type": "application/xml",
	})
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	frames := decodeEventFrames(t, w.Body.Bytes())
	// Expect at least Records + Stats + End.
	if len(frames) < 3 {
		t.Fatalf("expected at least 3 frames, got %d", len(frames))
	}

	recordsFrame := frames[0]
	if recordsFrame.headers[":event-type"] != "Records" {
		t.Errorf("first frame event-type: %q", recordsFrame.headers[":event-type"])
	}
	payload := string(recordsFrame.payload)
	if !bytes.Contains([]byte(payload), []byte("Alice")) {
		t.Errorf("payload missing Alice: %q", payload)
	}
	if !bytes.Contains([]byte(payload), []byte("Bob")) {
		t.Errorf("payload missing Bob: %q", payload)
	}

	statsFrame := frames[len(frames)-2]
	if statsFrame.headers[":event-type"] != "Stats" {
		t.Errorf("stats frame event-type: %q", statsFrame.headers[":event-type"])
	}

	endFrame := frames[len(frames)-1]
	if endFrame.headers[":event-type"] != "End" {
		t.Errorf("end frame event-type: %q", endFrame.headers[":event-type"])
	}
}

// TestS3Select_CSV_WhereFilter verifies that WHERE col = 'val' filtering works.
func TestS3Select_CSV_WhereFilter(t *testing.T) {
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, "PUT", "/filter-bucket", nil, nil)
	csvData := []byte("name,age\nAlice,30\nBob,25\nCharlie,30\n")
	s3Request(t, srv, "PUT", "/filter-bucket/people.csv", csvData, nil)

	reqBody := s3SelectXMLRequest("SELECT * FROM S3Object WHERE age = '30'", "CSV")
	w := s3Request(t, srv, "POST", "/filter-bucket/people.csv?select&select-type=2", reqBody, nil)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	frames := decodeEventFrames(t, w.Body.Bytes())
	var recordsPayload []byte
	for _, f := range frames {
		if f.headers[":event-type"] == "Records" {
			recordsPayload = f.payload
			break
		}
	}
	if len(recordsPayload) == 0 {
		t.Fatal("no Records frame in response")
	}
	if !bytes.Contains(recordsPayload, []byte("Alice")) {
		t.Errorf("expected Alice in result")
	}
	if bytes.Contains(recordsPayload, []byte("Bob")) {
		t.Errorf("Bob should be filtered out")
	}
	if !bytes.Contains(recordsPayload, []byte("Charlie")) {
		t.Errorf("expected Charlie in result")
	}
}

// TestS3Select_JSONLines_AllRows verifies SelectObjectContent on a JSON Lines object.
func TestS3Select_JSONLines_AllRows(t *testing.T) {
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, "PUT", "/json-bucket", nil, nil)
	jsonData := []byte("{\"id\":1,\"val\":\"foo\"}\n{\"id\":2,\"val\":\"bar\"}\n")
	s3Request(t, srv, "PUT", "/json-bucket/data.jsonl", jsonData, nil)

	reqBody := s3SelectXMLRequest("SELECT * FROM S3Object", "JSON")
	w := s3Request(t, srv, "POST", "/json-bucket/data.jsonl?select&select-type=2", reqBody, nil)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	frames := decodeEventFrames(t, w.Body.Bytes())
	var recordsPayload []byte
	for _, f := range frames {
		if f.headers[":event-type"] == "Records" {
			recordsPayload = f.payload
			break
		}
	}
	if len(recordsPayload) == 0 {
		t.Fatal("no Records frame in response")
	}
	if !bytes.Contains(recordsPayload, []byte("foo")) {
		t.Errorf("expected foo in result")
	}
	if !bytes.Contains(recordsPayload, []byte("bar")) {
		t.Errorf("expected bar in result")
	}
}

// TestS3Select_MissingKey verifies a 404 for a non-existent object.
func TestS3Select_MissingKey(t *testing.T) {
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, "PUT", "/bucket-404", nil, nil)

	reqBody := s3SelectXMLRequest("SELECT * FROM S3Object", "JSON")
	w := s3Request(t, srv, "POST", "/bucket-404/nonexistent.json?select&select-type=2", reqBody, nil)
	if w.Code != 404 {
		t.Fatalf("expected 404, got %d; body: %s", w.Code, w.Body.String())
	}
}

// TestS3Select_Limit verifies LIMIT n truncates the result set.
func TestS3Select_Limit(t *testing.T) {
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, "PUT", "/limit-bucket", nil, nil)
	csvData := []byte("name\nA\nB\nC\nD\nE\n")
	s3Request(t, srv, "PUT", "/limit-bucket/names.csv", csvData, nil)

	reqBody := s3SelectXMLRequest("SELECT * FROM S3Object LIMIT 2", "CSV")
	w := s3Request(t, srv, "POST", "/limit-bucket/names.csv?select&select-type=2", reqBody, nil)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	frames := decodeEventFrames(t, w.Body.Bytes())
	var recordsPayload []byte
	for _, f := range frames {
		if f.headers[":event-type"] == "Records" {
			recordsPayload = f.payload
			break
		}
	}
	// Count newlines in payload = number of rows returned.
	rowCount := bytes.Count(recordsPayload, []byte("\n"))
	if rowCount != 2 {
		t.Errorf("expected 2 rows with LIMIT 2, got %d rows (payload: %q)", rowCount, recordsPayload)
	}
}
