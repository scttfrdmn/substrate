package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Timestream's endpoint discovery, and the shape an unseeded Query is answered in (#1209).
//
// # Endpoints
//
// Timestream is two APIs, Write and Query, sharing one target prefix and one signing name, and both
// require endpoint discovery. A client sends DescribeEndpoints to a regional host —
// ingest.timestream.{region}.amazonaws.com for Write, query.timestream.{region}.amazonaws.com for
// Query — and makes its calls against the address that returns. The developer guide's usage notes
// say "The DescribeEndpoints action is the only action that Timestream Live Analytics regional
// endpoints recognize", and each operation page publishes InvalidEndpointException/400 for a request
// at an endpoint that is not valid for it.
//
// So a request is classified by the host it arrived on:
//
//   - a regional host (ingest. or query.) serves DescribeEndpoints and refuses everything else;
//   - a cell host (ingest-cellN. or query-cellN.) serves its own API's operations, and
//     DescribeEndpoints, and refuses the other API's;
//   - any other host — localhost, an --endpoint-url, a plugin called directly — names no API, so
//     nothing is enforced there. Substrate cannot tell a Write call from a Query call by a host that
//     carries neither label.
//
// The cell address DescribeEndpoints answers, query-cell1. or ingest-cell1., is substrate's reading:
// the guide says the returned addresses are cell-specific and publishes no form for them, and
// "-cellN" is the form real responses carry.

// timestreamEndpointAPI is the Timestream API a host serves.
type timestreamEndpointAPI int

const (
	// timestreamAPIUnknown is a host that names neither API.
	timestreamAPIUnknown timestreamEndpointAPI = iota
	// timestreamAPIWrite is the Write (ingest) API.
	timestreamAPIWrite
	// timestreamAPIQuery is the Query API.
	timestreamAPIQuery
)

// timestreamHost is the classification of the host a Timestream request arrived on.
type timestreamHost struct {
	api      timestreamEndpointAPI
	regional bool
	region   string
	name     string
}

// classifyTimestreamHost reads the Timestream API and endpoint kind out of a request's Host.
func classifyTimestreamHost(host string) timestreamHost {
	name := strings.ToLower(host)
	if h, _, ok := strings.Cut(name, ":"); ok {
		name = h
	}
	out := timestreamHost{name: name}
	first, rest, ok := strings.Cut(name, ".")
	if !ok {
		return out
	}
	region, ok := strings.CutPrefix(rest, "timestream.")
	if !ok {
		return out
	}
	region, _, _ = strings.Cut(region, ".")
	api, regional, ok := timestreamEndpointLabel(first)
	if !ok {
		return out
	}
	out.api, out.regional, out.region = api, regional, region
	return out
}

// timestreamEndpointLabel classifies a Timestream host's first label: "ingest" and "query" are the
// regional discovery hosts, and "ingest-cellN" and "query-cellN" the cell hosts discovery hands out.
func timestreamEndpointLabel(label string) (api timestreamEndpointAPI, regional, ok bool) {
	for _, c := range []struct {
		prefix string
		api    timestreamEndpointAPI
	}{{"ingest", timestreamAPIWrite}, {"query", timestreamAPIQuery}} {
		rest, found := strings.CutPrefix(label, c.prefix)
		if !found {
			continue
		}
		if rest == "" {
			return c.api, true, true
		}
		digits, isCell := strings.CutPrefix(rest, "-cell")
		if isCell && digits != "" && strings.Trim(digits, "0123456789") == "" {
			return c.api, false, true
		}
	}
	return timestreamAPIUnknown, false, false
}

// isTimestreamEndpointHost reports whether host (lowercased, without ".amazonaws.com") is one of
// Timestream's regional or cell endpoints, for the parser's service resolution.
func isTimestreamEndpointHost(host string) bool {
	first, rest, ok := strings.Cut(host, ".")
	if !ok || !strings.HasPrefix(rest, "timestream.") {
		return false
	}
	_, _, ok = timestreamEndpointLabel(first)
	return ok
}

// timestreamOperationAPI is the API an operation belongs to. DescribeEndpoints belongs to both.
func timestreamOperationAPI(op string) timestreamEndpointAPI {
	switch op {
	case "Query", "CancelQuery":
		return timestreamAPIQuery
	case "DescribeEndpoints":
		return timestreamAPIUnknown
	default:
		return timestreamAPIWrite
	}
}

// checkTimestreamEndpoint refuses an operation that the host it arrived on does not serve.
func checkTimestreamEndpoint(req *AWSRequest) error {
	host := classifyTimestreamHost(req.Headers["Host"])
	if host.api == timestreamAPIUnknown || req.Operation == "DescribeEndpoints" {
		return nil
	}
	opAPI := timestreamOperationAPI(req.Operation)
	switch {
	case host.regional:
		return timestreamInvalidEndpoint(opAPI, fmt.Sprintf(
			"%s is a regional discovery endpoint, which recognizes only DescribeEndpoints; send %s to the address DescribeEndpoints returns",
			host.name, req.Operation))
	case opAPI != host.api:
		return timestreamInvalidEndpoint(opAPI, fmt.Sprintf(
			"%s is a %s endpoint, and %s is a %s operation",
			host.name, timestreamAPIName(host.api), req.Operation, timestreamAPIName(opAPI)))
	}
	return nil
}

// timestreamInvalidEndpoint is InvalidEndpointException/400, prefixed with the sentence the
// operation's own API publishes for it.
func timestreamInvalidEndpoint(api timestreamEndpointAPI, detail string) *AWSError {
	sentence := "The requested endpoint was not valid." // API_WriteRecords and the other Write pages.
	if api == timestreamAPIQuery {
		sentence = "The requested endpoint is invalid." // API_query_Query.
	}
	return &AWSError{Code: "InvalidEndpointException", Message: sentence + " " + detail, HTTPStatus: http.StatusBadRequest}
}

// timestreamAPIName names an API for a refusal's message.
func timestreamAPIName(api timestreamEndpointAPI) string {
	if api == timestreamAPIQuery {
		return "Query"
	}
	return "Write"
}

// timestreamEndpointAddress is the address DescribeEndpoints answers for a request at host.
//
// A regional or cell host answers its API's cell address. A host naming neither API answers itself,
// since a caller that reached substrate through an --endpoint-url has no other address to be told
// about; a request with no host at all answers the regional-less form substrate answered before.
func timestreamEndpointAddress(host timestreamHost, region string) string {
	if host.region != "" {
		region = host.region
	}
	switch host.api {
	case timestreamAPIQuery:
		return "query-cell1.timestream." + region + ".amazonaws.com"
	case timestreamAPIWrite:
		return "ingest-cell1.timestream." + region + ".amazonaws.com"
	}
	if host.name != "" {
		return host.name
	}
	return "timestream." + region + ".amazonaws.com"
}

// # An unseeded Query
//
// Substrate does not evaluate Timestream SQL — per CLAUDE.md, a query's result is a seeded value,
// not a computation — and before #1209 an unseeded Query answered a plausible wrong result instead of
// saying so: every column VARCHAR, alphabetized, every record a row, any WHERE or GROUP BY ignored.
// A consumer who forgot to seed got rows to write assertions against.
//
// #1209 offered two ways to make the fallback's limits observable, and this takes the refusal.
// QueryStatus publishes only ProgressPercentage, CumulativeBytesScanned and CumulativeBytesMetered,
// so no member could carry "this was reconstructed", and a flag in an unpublished member is one no
// consumer reads. So an unseeded query is answered only when substrate can answer it correctly:
// `SELECT * FROM db.table`, with nothing after the table, which every stored record answers. Any
// other unseeded query is ValidationException/400, which API_query_Query publishes, and the message
// names the seed endpoint.
//
// `SELECT *` answers the table in the logical shape the developer guide's Writes page draws: `time`
// (TIMESTAMP), then the dimensions (VARCHAR) in the order they were first written, then
// `measure_name` (VARCHAR), then one column per measure: `measure_value::<type>` for a single-measure
// record, each MeasureValues name for a multi-measure one. A record without a column answers
// `"NullValue": true` there, as API_query_Datum publishes.

// timestreamUnseededSelectAll parses the one query an unseeded Query answers: `SELECT * FROM db.table`
// with optional double quotes and an optional trailing semicolon, and nothing else.
func timestreamUnseededSelectAll(sql string) (db, table string, ok bool) {
	fields := strings.Fields(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	if len(fields) != 4 || !strings.EqualFold(fields[0], "SELECT") || fields[1] != "*" || !strings.EqualFold(fields[2], "FROM") {
		return "", "", false
	}
	db, table, ok = strings.Cut(fields[3], ".")
	if !ok || strings.Contains(table, ".") {
		return "", "", false
	}
	db, table = strings.Trim(db, `"`), strings.Trim(table, `"`)
	if db == "" || table == "" {
		return "", "", false
	}
	return db, table, true
}

// timestreamUnevaluated refuses an unseeded query substrate cannot answer correctly.
func timestreamUnevaluated(qs string) *AWSError {
	return &AWSError{
		Code: "ValidationException",
		Message: fmt.Sprintf("substrate does not evaluate Timestream SQL. Without a seeded result it answers only "+
			"SELECT * FROM db.table, reconstructed from the table's records; seed POST /v1/timestream-query/results "+
			"with this queryString for %q (#1209)", qs),
		HTTPStatus: http.StatusBadRequest,
	}
}

// timestreamStoredRecord is a WriteRecords record as substrate stores it, CommonAttributes merged in.
type timestreamStoredRecord struct {
	Dimensions       []timestreamDimension    `json:"Dimensions,omitempty"`
	MeasureName      string                   `json:"MeasureName,omitempty"`
	MeasureValue     string                   `json:"MeasureValue,omitempty"`
	MeasureValueType string                   `json:"MeasureValueType,omitempty"`
	MeasureValues    []timestreamMeasureValue `json:"MeasureValues,omitempty"`
	Time             string                   `json:"Time,omitempty"`
	TimeUnit         string                   `json:"TimeUnit,omitempty"`
	Version          *int64                   `json:"Version,omitempty"`
}

// timestreamDimension is a record's dimension.
type timestreamDimension struct {
	Name               string `json:"Name"`
	Value              string `json:"Value"`
	DimensionValueType string `json:"DimensionValueType,omitempty"`
}

// timestreamMeasureValue is one measure of a multi-measure record.
type timestreamMeasureValue struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
	Type  string `json:"Type"`
}

// mergeTimestreamCommon applies a WriteRecords request's CommonAttributes to one record. API_WriteRecords:
// "The measure and dimension attributes specified will be merged with the measure and dimension
// attributes in the records object", so the common dimensions come first and every other attribute a
// record sets wins over the common one.
func mergeTimestreamCommon(common, rec timestreamStoredRecord) timestreamStoredRecord {
	out := rec
	if len(common.Dimensions) > 0 {
		out.Dimensions = append(append([]timestreamDimension{}, common.Dimensions...), rec.Dimensions...)
	}
	if out.MeasureName == "" {
		out.MeasureName = common.MeasureName
	}
	if out.MeasureValue == "" {
		out.MeasureValue = common.MeasureValue
	}
	if out.MeasureValueType == "" {
		out.MeasureValueType = common.MeasureValueType
	}
	if len(out.MeasureValues) == 0 {
		out.MeasureValues = common.MeasureValues
	}
	if out.Time == "" {
		out.Time = common.Time
	}
	if out.TimeUnit == "" {
		out.TimeUnit = common.TimeUnit
	}
	if out.Version == nil {
		out.Version = common.Version
	}
	return out
}

// timestreamSelectAll answers `SELECT * FROM db.table` from the table's stored records.
func (p *TimestreamPlugin) timestreamSelectAll(reqCtx *RequestContext, db, table string) (TimestreamQueryResult, error) {
	if _, err := p.loadTable(reqCtx.AccountID, reqCtx.Region, db, table); err != nil {
		var awsErr *AWSError
		if errors.As(err, &awsErr) {
			return TimestreamQueryResult{}, &AWSError{
				Code: "ValidationException", Message: fmt.Sprintf("Table %s.%s does not exist", db, table),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		return TimestreamQueryResult{}, err
	}
	raw, err := p.state.Get(context.Background(), timestreamNamespace, timestreamRecordsKey(reqCtx.AccountID, reqCtx.Region, db, table))
	if err != nil {
		return TimestreamQueryResult{}, fmt.Errorf("timestream query read records: %w", err)
	}
	if raw == nil {
		return TimestreamQueryResult{}, nil
	}
	var records []timestreamStoredRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return TimestreamQueryResult{}, fmt.Errorf("timestream query decode records: %w", err)
	}
	return timestreamRecordsToResult(records), nil
}

// timestreamRecordsToResult lays stored records out in the table's logical shape; see above.
func timestreamRecordsToResult(records []timestreamStoredRecord) TimestreamQueryResult {
	if len(records) == 0 {
		return TimestreamQueryResult{}
	}
	var cols []TimestreamColumnInfo
	index := map[string]int{}
	add := func(name, scalar string) {
		if _, seen := index[name]; !seen {
			index[name] = len(cols)
			cols = append(cols, TimestreamColumnInfo{Name: name, Type: TimestreamColumnInfoType{ScalarType: scalar}})
		}
	}
	add("time", "TIMESTAMP")
	for _, rec := range records {
		for _, d := range rec.Dimensions {
			add(d.Name, "VARCHAR")
		}
	}
	add("measure_name", "VARCHAR")
	for _, rec := range records {
		if strings.EqualFold(rec.MeasureValueType, "MULTI") {
			for _, mv := range rec.MeasureValues {
				add(mv.Name, strings.ToUpper(mv.Type))
			}
			continue
		}
		add(timestreamMeasureColumn(rec), timestreamSingleMeasureType(rec))
	}

	rows := make([]TimestreamRow, 0, len(records))
	for _, rec := range records {
		data := make([]TimestreamDatum, len(cols))
		for i := range data {
			data[i] = TimestreamDatum{NullValue: true}
		}
		set := func(name, value string) {
			i := index[name]
			if v, ok := timestreamScalarValue(cols[i].Type.ScalarType, value, rec.TimeUnit); ok {
				data[i] = TimestreamDatum{ScalarValue: v}
			}
		}
		set("time", rec.Time)
		for _, d := range rec.Dimensions {
			set(d.Name, d.Value)
		}
		set("measure_name", rec.MeasureName)
		if strings.EqualFold(rec.MeasureValueType, "MULTI") {
			for _, mv := range rec.MeasureValues {
				set(mv.Name, mv.Value)
			}
		} else {
			set(timestreamMeasureColumn(rec), rec.MeasureValue)
		}
		rows = append(rows, TimestreamRow{Data: data})
	}
	return TimestreamQueryResult{Rows: rows, ColumnInfo: cols}
}

// timestreamSingleMeasureType is a single-measure record's value type, DOUBLE when it names none, as
// API_Record publishes ("Default type is DOUBLE").
func timestreamSingleMeasureType(rec timestreamStoredRecord) string {
	if rec.MeasureValueType == "" {
		return "DOUBLE"
	}
	return strings.ToUpper(rec.MeasureValueType)
}

// timestreamMeasureColumn is the column a single-measure record's value lands in, `measure_value::double`
// and the like, as the Writes page names them.
func timestreamMeasureColumn(rec timestreamStoredRecord) string {
	return "measure_value::" + strings.ToLower(timestreamSingleMeasureType(rec))
}

// timestreamTimestampLayout is the query language's timestamp rendering, from the Supported data types
// page: `YYYY-MM-DD hh:mm:ss.sssssssss`, nanosecond precision, UTC.
const timestreamTimestampLayout = "2006-01-02 15:04:05.000000000"

// timestreamScalarValue renders a stored value as its column's ScalarValue, or reports false when the
// record holds nothing there, which answers NullValue.
//
// VARCHAR, DOUBLE and BIGINT answer the string the record was written with: WriteRecords takes every
// value as a string, and substrate does not re-render a number it was handed. BOOLEAN answers the
// lowercase literal. TIMESTAMP — the record's `time`, and a multi-measure TIMESTAMP value — is the
// epoch value read in the record's TimeUnit (MILLISECONDS when it names none, as API_Record publishes)
// and rendered in the query language's form; a value that is not an integer answers as written.
func timestreamScalarValue(scalarType, value, timeUnit string) (string, bool) {
	if value == "" {
		return "", false
	}
	switch scalarType {
	case "BOOLEAN":
		if b, err := strconv.ParseBool(value); err == nil {
			return strconv.FormatBool(b), true
		}
	case "TIMESTAMP":
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			return timestreamEpoch(n, timeUnit).UTC().Format(timestreamTimestampLayout), true
		}
	}
	return value, true
}

// timestreamEpoch reads an epoch value in one of API_Record's four TimeUnits.
func timestreamEpoch(n int64, unit string) time.Time {
	switch strings.ToUpper(unit) {
	case "SECONDS":
		return time.Unix(n, 0)
	case "MICROSECONDS":
		return time.UnixMicro(n)
	case "NANOSECONDS":
		return time.Unix(0, n)
	default:
		return time.UnixMilli(n)
	}
}

// timestreamQueryStatus is the QueryStatus a Query answers when its seed does not supply one.
//
// substrate's Query is synchronous, so ProgressPercentage is 100. CumulativeBytesScanned and
// CumulativeBytesMetered are both the size in bytes of the Rows member as answered: substrate scans
// nothing, and the answered rows are the only bytes there are. Neither is Timestream's own accounting
// (which meters a 10 MB minimum per query); a test that needs particular figures seeds a QueryStatus.
func timestreamQueryStatus(rows []TimestreamRow) (TimestreamQueryStatus, error) {
	data, err := json.Marshal(rows)
	if err != nil {
		return TimestreamQueryStatus{}, fmt.Errorf("timestream query status: %w", err)
	}
	n := int64(len(data))
	return TimestreamQueryStatus{ProgressPercentage: 100, CumulativeBytesScanned: n, CumulativeBytesMetered: n}, nil
}
