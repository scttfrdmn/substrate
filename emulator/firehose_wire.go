package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// FirehoseDeliveryStream (firehose_types.go) is a persisted state record, and describeDeliveryStream
// handed it straight to the caller under `DeliveryStreamDescription`. That answered AccountId and
// Region, which API_DeliveryStreamDescription does not publish (#756), and Tags, which it does not
// publish either: a stream's tags are read through ListTagsForDeliveryStream. It also rendered
// CreateTimestamp as an RFC3339 string (#1305).
//
// Do not "fix" that by adding `json:"-"` to a state field or by retyping the date in the record.
// Either changes the format of every recorded run, because MemoryStateManager snapshots those bytes
// and a replay reads them back. Projecting leaves the stored bytes unchanged.
//
// # Why CreateTimestamp is EpochSeconds
//
// Firehose speaks awsJson1_1, where a Timestamp is published as epoch seconds with fractional
// precision. API_DeliveryStreamDescription types CreateTimestamp as Timestamp, and an awsJson1_1
// timestamp deserializer expects a number, so the RFC3339 string the record marshaled to made
// DescribeDeliveryStream undecodable in a typed SDK (#1305).

// firehoseDeliveryStreamOut is the DeliveryStreamDescription element of DescribeDeliveryStream's
// response.
//
// Five of API_DeliveryStreamDescription's members — the ones the record models. Destinations,
// HasMoreDestinations and VersionId are published as required and the record holds none of them,
// since CreateDeliveryStream does not keep a destination; they are absent rather than invented
// (#1013's rule, #1199's gap).
type firehoseDeliveryStreamOut struct {
	CreateTimestamp      EpochSeconds `json:"CreateTimestamp"`
	DeliveryStreamARN    string       `json:"DeliveryStreamARN"`
	DeliveryStreamName   string       `json:"DeliveryStreamName"`
	DeliveryStreamStatus string       `json:"DeliveryStreamStatus"`
	DeliveryStreamType   string       `json:"DeliveryStreamType"`
}

// firehoseDeliveryStreamToWire projects a persisted delivery stream onto the published shape.
func firehoseDeliveryStreamToWire(stream FirehoseDeliveryStream) firehoseDeliveryStreamOut {
	return firehoseDeliveryStreamOut{
		CreateTimestamp:      EpochSeconds(stream.CreatedAt),
		DeliveryStreamARN:    stream.DeliveryStreamARN,
		DeliveryStreamName:   stream.DeliveryStreamName,
		DeliveryStreamStatus: stream.DeliveryStreamStatus,
		DeliveryStreamType:   stream.DeliveryStreamType,
	}
}
