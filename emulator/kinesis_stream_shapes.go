package emulator

// The two describe shapes, which share a record and not a member set.
//
// DescribeStream answers an API_StreamDescription and DescribeStreamSummary an
// API_StreamDescriptionSummary. Until #1076 one builder served both and emitted the union of
// the two, so each operation answered members its own page does not publish:
//
//	member           StreamDescription   StreamDescriptionSummary
//	Shards           Required: Yes       not a member
//	HasMoreShards    Required: Yes       not a member
//	OpenShardCount   not a member        Required: Yes
//
// An SDK tolerates an unknown member, so this is milder than the EnhancedMonitoring defect
// below; it is still a value a caller can read out of a response that AWS will never put there,
// which is the #1013 class. The two builders share nothing but the six members both pages
// publish, written out in each rather than factored into a third helper, because the whole point
// is that the shapes are separate and a shared map is what let them drift together.
//
// Neither builder renders EncryptionType, KeyId or StreamModeDetails, and the summary renders
// none of ChannelCount, ConsumerCount, MaxRecordSizeInKiB, StreamId or WarmThroughput. All are
// Required: No on their pages and substrate holds no state for any of them; capacity mode is
// #1118.

// kinesisRenderEnhancedMonitoring renders a stream's stored metric names as the published shape.
//
// API_StreamDescription and API_StreamDescriptionSummary both publish EnhancedMonitoring as
// Required: Yes and as an *array of EnhancedMetrics objects*, each object carrying one
// ShardLevelMetrics array. Until #1076 substrate rendered the stored []string straight through,
// so a response read ["IncomingBytes"] where AWS answers
// [{"ShardLevelMetrics": ["IncomingBytes"]}] — an SDK decoding into []types.EnhancedMetrics gets
// a json.UnmarshalTypeError, so it is a hard failure for a real client rather than a cosmetic
// difference. Same class as #1017's ECR tags.
//
// A stream with nothing enhanced renders [] rather than [{"ShardLevelMetrics": []}]. Both
// readings were open in #1076; API_EnhancedMetrics settles it, because ShardLevelMetrics
// publishes "Array Members: Minimum number of 1 item" — an object with an empty list is a shape
// the model does not permit — while EnhancedMonitoring itself publishes no array minimum. The
// member is present either way, as Required: Yes demands, and no impossible inner object is
// invented. The slice is non-nil so it marshals as [] and not null (#938).
//
// The names go through the #999 set/render pair rather than out of the record directly, so a
// record written before #999 that holds the literal ALL reads back as the seven metrics it means
// rather than as one wildcard, and the order is the pages' own.
func kinesisRenderEnhancedMonitoring(stored []string) []map[string]interface{} {
	rendered := kinesisRenderShardLevelMetrics(kinesisShardLevelMetricSet(stored))
	if len(rendered) == 0 {
		return []map[string]interface{}{}
	}
	return []map[string]interface{}{{"ShardLevelMetrics": rendered}}
}

// buildStreamDescription builds the API_StreamDescription body DescribeStream answers.
func buildStreamDescription(stream KinesisStream) map[string]interface{} {
	return map[string]interface{}{
		"StreamName":              stream.StreamName,
		"StreamARN":               stream.StreamArn,
		"StreamStatus":            stream.StreamStatus,
		"RetentionPeriodHours":    stream.RetentionPeriodHours,
		"StreamCreationTimestamp": stream.CreatedAt.Unix(),
		"EnhancedMonitoring":      kinesisRenderEnhancedMonitoring(stream.EnhancedMonitoring),
		"Shards":                  stream.Shards,
		"HasMoreShards":           false,
	}
}

// buildStreamDescriptionSummary builds the body DescribeStreamSummary answers.
//
// OpenShardCount is the summary's own member and is read from the stored ShardCount. Substrate
// closes no shard, so every shard it holds is open and the two counts are the same number; that
// they are the same is a property of substrate's model rather than of the API, which is why the
// member is named here and not derived from len(Shards) as if it were a synonym.
func buildStreamDescriptionSummary(stream KinesisStream) map[string]interface{} {
	return map[string]interface{}{
		"StreamName":              stream.StreamName,
		"StreamARN":               stream.StreamArn,
		"StreamStatus":            stream.StreamStatus,
		"RetentionPeriodHours":    stream.RetentionPeriodHours,
		"StreamCreationTimestamp": stream.CreatedAt.Unix(),
		"EnhancedMonitoring":      kinesisRenderEnhancedMonitoring(stream.EnhancedMonitoring),
		"OpenShardCount":          stream.ShardCount,
	}
}
