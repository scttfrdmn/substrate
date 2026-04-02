package substrate

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"

	"github.com/spf13/afero"
)

// selectObjectContentRequest is the XML request body for S3 SelectObjectContent.
type selectObjectContentRequest struct {
	Expression         string `xml:"Expression"`
	ExpressionType     string `xml:"ExpressionType"`
	InputSerialization struct {
		CSV *struct {
			FileHeaderInfo string `xml:"FileHeaderInfo"`
		} `xml:"CSV"`
		JSON *struct {
			Type string `xml:"Type"`
		} `xml:"JSON"`
	} `xml:"InputSerialization"`
	OutputSerialization struct {
		JSON *struct {
			RecordDelimiter string `xml:"RecordDelimiter"`
		} `xml:"JSON"`
		CSV *struct {
			FieldDelimiter string `xml:"FieldDelimiter"`
		} `xml:"CSV"`
	} `xml:"OutputSerialization"`
}

// s3SelectQuery holds the parsed SQL expression used for filtering.
type s3SelectQuery struct {
	whereCol string
	whereVal string
	limit    int
}

// parseS3SelectExpression extracts a WHERE column/value filter and LIMIT from
// a simplified S3 SQL expression. Only SELECT * is supported; WHERE col = 'val'
// and LIMIT n are optional.
func parseS3SelectExpression(expr string) s3SelectQuery {
	q := s3SelectQuery{limit: -1}
	upper := strings.ToUpper(strings.TrimSpace(expr))

	whereIdx := strings.Index(upper, " WHERE ")
	limitIdx := strings.Index(upper, " LIMIT ")

	if whereIdx >= 0 {
		whereClause := expr[whereIdx+7:]
		if limitIdx > whereIdx {
			whereClause = whereClause[:limitIdx-whereIdx-7]
		}
		whereClause = strings.TrimSpace(whereClause)
		eqIdx := strings.IndexByte(whereClause, '=')
		if eqIdx >= 0 {
			q.whereCol = strings.TrimSpace(whereClause[:eqIdx])
			val := strings.TrimSpace(whereClause[eqIdx+1:])
			q.whereVal = strings.Trim(val, "'\"`")
		}
	}

	if limitIdx >= 0 {
		limitStr := strings.TrimSpace(expr[limitIdx+7:])
		if n, err := strconv.Atoi(limitStr); err == nil {
			q.limit = n
		}
	}

	return q
}

// s3SelectCSV parses a CSV byte slice into a slice of row maps, using the first
// line as headers when fileHeaderInfo is "USE".
func s3SelectCSV(data []byte, fileHeaderInfo string) ([]string, []map[string]string) {
	lines := strings.Split(string(data), "\n")
	var headers []string
	start := 0
	if strings.EqualFold(fileHeaderInfo, "USE") && len(lines) > 0 {
		headers = s3SplitCSVLine(strings.TrimRight(lines[0], "\r"))
		start = 1
	}
	var rows []map[string]string
	for _, line := range lines[start:] {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		values := s3SplitCSVLine(line)
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(values) {
				row[h] = values[i]
			}
		}
		rows = append(rows, row)
	}
	return headers, rows
}

// s3SplitCSVLine splits a CSV line on commas, stripping surrounding whitespace
// from each field. This handles the common case without quoted commas.
func s3SplitCSVLine(line string) []string {
	parts := strings.Split(line, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// s3SelectJSONLines parses a JSON Lines byte slice into a slice of raw JSON objects.
func s3SelectJSONLines(data []byte) []json.RawMessage {
	var rows []json.RawMessage
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if json.Valid(line) {
			cp := make([]byte, len(line))
			copy(cp, line)
			rows = append(rows, cp)
		}
	}
	return rows
}

// s3SelectRowMatchesQuery returns true when the row satisfies the WHERE clause
// of q (if any). The value is compared as a string regardless of type.
func s3SelectRowMatchesQuery(row map[string]string, q s3SelectQuery) bool {
	if q.whereCol == "" {
		return true
	}
	return row[q.whereCol] == q.whereVal
}

// s3EventFrame encodes a single AWS binary event stream frame with the given
// ordered headers (as [name, value] pairs) and payload.
func s3EventFrame(headers [][2]string, payload []byte) []byte {
	// Encode headers.
	var hdrBuf bytes.Buffer
	for _, h := range headers {
		name, value := h[0], h[1]
		hdrBuf.WriteByte(byte(len(name)))
		hdrBuf.WriteString(name)
		hdrBuf.WriteByte(7) // string type
		vl := uint16(len(value))
		hdrBuf.WriteByte(byte(vl >> 8))
		hdrBuf.WriteByte(byte(vl & 0xff))
		hdrBuf.WriteString(value)
	}
	headerBytes := hdrBuf.Bytes()

	totalLen := uint32(16 + len(headerBytes) + len(payload))
	headerLen := uint32(len(headerBytes))

	// Build prelude and compute its CRC.
	var prelude bytes.Buffer
	_ = binary.Write(&prelude, binary.BigEndian, totalLen)
	_ = binary.Write(&prelude, binary.BigEndian, headerLen)
	preludeCRC := crc32.ChecksumIEEE(prelude.Bytes())

	// Build full message (without trailing message CRC).
	var msg bytes.Buffer
	msg.Write(prelude.Bytes())
	_ = binary.Write(&msg, binary.BigEndian, preludeCRC)
	msg.Write(headerBytes)
	msg.Write(payload)

	// Append message CRC.
	msgCRC := crc32.ChecksumIEEE(msg.Bytes())
	_ = binary.Write(&msg, binary.BigEndian, msgCRC)

	return msg.Bytes()
}

// s3SelectRecordsFrame returns a Records event frame containing payload bytes.
func s3SelectRecordsFrame(payload []byte) []byte {
	return s3EventFrame([][2]string{
		{":event-type", "Records"},
		{":message-type", "event"},
		{":content-type", "application/octet-stream"},
	}, payload)
}

// s3SelectStatsFrame returns a Stats event frame with byte counts.
func s3SelectStatsFrame(bytesScanned, bytesReturned int) []byte {
	statsXML := fmt.Sprintf(
		`<Stats><Details><BytesScanned>%d</BytesScanned><BytesProcessed>%d</BytesProcessed><BytesReturned>%d</BytesReturned></Details></Stats>`,
		bytesScanned, bytesScanned, bytesReturned,
	)
	return s3EventFrame([][2]string{
		{":event-type", "Stats"},
		{":message-type", "event"},
		{":content-type", "application/xml"},
	}, []byte(statsXML))
}

// s3SelectEndFrame returns an End event frame signalling stream completion.
func s3SelectEndFrame() []byte {
	return s3EventFrame([][2]string{
		{":event-type", "End"},
		{":message-type", "event"},
	}, nil)
}

// selectObjectContent implements S3 SelectObjectContent using a simple in-memory
// SQL evaluator. Supported input formats are CSV (FileHeaderInfo=USE) and JSON
// Lines. Output is always newline-delimited JSON. Only SELECT * with an optional
// simple WHERE col = 'val' clause and an optional LIMIT are handled.
func (p *S3Plugin) selectObjectContent(reqCtx *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	// Verify bucket exists.
	goCtx := context.Background()
	if b, _ := p.state.Get(goCtx, s3Namespace, "bucket:"+bucket); b == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	// Load the object metadata.
	objKey := "object:" + bucket + "/" + key
	objData, err := p.state.Get(goCtx, s3Namespace, objKey)
	if err != nil || objData == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}
	var s3obj S3Object
	if err := json.Unmarshal(objData, &s3obj); err != nil {
		return s3ErrorResponse("InternalError", "corrupt object metadata", http.StatusInternalServerError), nil
	}

	// Load the object body from the afero filesystem.
	objectBody, err := afero.ReadFile(p.fs, "/"+bucket+"/"+key)
	if err != nil {
		return s3ErrorResponse("InternalError", "could not read object body", http.StatusInternalServerError), nil
	}
	_ = s3obj // metadata available if needed for future extensions

	// Parse the XML request body.
	var selReq selectObjectContentRequest
	if err := xml.Unmarshal(req.Body, &selReq); err != nil {
		return s3ErrorResponse("MalformedXML", "The XML provided is not well-formed.", http.StatusBadRequest), nil
	}

	q := parseS3SelectExpression(selReq.Expression)

	// Convert object bytes to rows and apply filter.
	var recordsBuf bytes.Buffer
	bytesScanned := len(objectBody)
	count := 0

	if selReq.InputSerialization.CSV != nil {
		fileHeaderInfo := "NONE"
		if selReq.InputSerialization.CSV.FileHeaderInfo != "" {
			fileHeaderInfo = selReq.InputSerialization.CSV.FileHeaderInfo
		}
		_, rows := s3SelectCSV(objectBody, fileHeaderInfo)
		for _, row := range rows {
			if q.limit >= 0 && count >= q.limit {
				break
			}
			if !s3SelectRowMatchesQuery(row, q) {
				continue
			}
			rowJSON, err := json.Marshal(row)
			if err != nil {
				continue
			}
			recordsBuf.Write(rowJSON)
			recordsBuf.WriteByte('\n')
			count++
		}
	} else {
		// Default: treat as JSON Lines.
		rows := s3SelectJSONLines(objectBody)
		for _, row := range rows {
			if q.limit >= 0 && count >= q.limit {
				break
			}
			recordsBuf.Write(row)
			recordsBuf.WriteByte('\n')
			count++
		}
	}

	recordsPayload := recordsBuf.Bytes()
	bytesReturned := len(recordsPayload)

	// Build event stream response body.
	var stream bytes.Buffer
	if len(recordsPayload) > 0 {
		stream.Write(s3SelectRecordsFrame(recordsPayload))
	}
	stream.Write(s3SelectStatsFrame(bytesScanned, bytesReturned))
	stream.Write(s3SelectEndFrame())

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":     "application/vnd.amazon.eventstream",
			"x-amz-request-id": fmt.Sprintf("EXAMPLE%d", bytesScanned),
		},
		Body: stream.Bytes(),
	}, nil
}
