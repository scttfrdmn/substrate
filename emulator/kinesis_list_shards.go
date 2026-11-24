package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ListShards (#1613), the shard listing the Kinesis Client Library reads.
//
// It lists the shards DescribeStream reports, ordered by shard ID, filtered by ShardFilter and
// paged by MaxResults (1–10000; "the default value is 1000. If you specify a value greater than
// 1000, at most 1000 results are returned") and NextToken. ExclusiveStartShardId and the
// AFTER_SHARD_ID filter start strictly after the named ID, which need not exist.
//
// # The token
//
// "Don't specify StreamName or StreamCreationTimestamp if you specify NextToken because the latter
// unambiguously identifies the stream", and ExclusiveStartShardId "cannot" be given with it either:
// all three are InvalidArgumentException with a NextToken. So the token must carry the stream and
// the filter its first page was cut with. It is pagination.go's opaque token, with the page's last
// shard ID as the cursor's Sort and the stream ARN and filter, JSON-encoded, as its Tie: every
// shard in one listing shares the Tie, so the order is the shard IDs'. A StreamARN or ShardFilter
// sent beside a NextToken must match what the token carries.
//
// # Shard times
//
// The time filters need each shard's start and end. A shard's sequence numbers carry them: a
// reshard mints them from the simulated clock (generateKinesisSeqNo), and a shard CreateStream made
// starts at "0", which is read as the stream's creation time. The trim horizon is the later of the
// stream's creation and now minus its retention period. Records are never expired, so a shard
// outside the retention period is still readable through GetShardIterator; ListShards only stops
// listing it, as AWS's does.

// kinesisListShardsMaxPage is the most shards one ListShards page returns.
const kinesisListShardsMaxPage = 1000

// The ShardFilter types API_ShardFilter publishes.
const (
	kinesisShardFilterAfterShardID    = "AFTER_SHARD_ID"
	kinesisShardFilterAtTrimHorizon   = "AT_TRIM_HORIZON"
	kinesisShardFilterFromTrimHorizon = "FROM_TRIM_HORIZON"
	kinesisShardFilterAtLatest        = "AT_LATEST"
	kinesisShardFilterAtTimestamp     = "AT_TIMESTAMP"
	kinesisShardFilterFromTimestamp   = "FROM_TIMESTAMP"
)

// kinesisShardFilter is API_ShardFilter.
type kinesisShardFilter struct {
	Type      string        `json:"Type"`
	ShardID   string        `json:"ShardId,omitempty"`
	Timestamp *EpochSeconds `json:"Timestamp,omitempty"`
}

// validate checks the filter's members against its type: ShardId only with AFTER_SHARD_ID, which
// requires it, and Timestamp only with AT_TIMESTAMP and FROM_TIMESTAMP, which require it.
func (f kinesisShardFilter) validate() *AWSError {
	wantShard, wantTime := false, false
	switch f.Type {
	case kinesisShardFilterAfterShardID:
		wantShard = true
	case kinesisShardFilterAtTimestamp, kinesisShardFilterFromTimestamp:
		wantTime = true
	case kinesisShardFilterAtTrimHorizon, kinesisShardFilterFromTrimHorizon, kinesisShardFilterAtLatest:
	case "":
		return kinesisInvalidArgument("ShardFilter.Type is required")
	default:
		return kinesisInvalidArgument(fmt.Sprintf("ShardFilter.Type %q is not one of AFTER_SHARD_ID, AT_TRIM_HORIZON, "+
			"FROM_TRIM_HORIZON, AT_LATEST, AT_TIMESTAMP, FROM_TIMESTAMP.", f.Type))
	}
	switch {
	case wantShard != (f.ShardID != ""):
		return kinesisInvalidArgument("ShardFilter.ShardId is required with, and only with, type AFTER_SHARD_ID")
	case wantTime != (f.Timestamp != nil):
		return kinesisInvalidArgument("ShardFilter.Timestamp is required with, and only with, types AT_TIMESTAMP and FROM_TIMESTAMP")
	}
	return nil
}

// kinesisListShardsContext is what a ListShards token carries besides its position.
type kinesisListShardsContext struct {
	StreamARN string              `json:"a"`
	Filter    *kinesisShardFilter `json:"f,omitempty"`
}

func (p *KinesisPlugin) listShards(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		NextToken               string              `json:"NextToken"`
		ExclusiveStartShardID   string              `json:"ExclusiveStartShardId"`
		MaxResults              *int                `json:"MaxResults"`
		StreamCreationTimestamp *EpochSeconds       `json:"StreamCreationTimestamp"`
		ShardFilter             *kinesisShardFilter `json:"ShardFilter"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if body.ShardFilter != nil {
		if refusal := body.ShardFilter.validate(); refusal != nil {
			return nil, refusal
		}
	}
	paging := jsonPaging{
		op:           "ListShards",
		size:         pageSizeRange{member: "MaxResults", min: 1, max: 10000, def: kinesisListShardsMaxPage},
		invalidSize:  kinesisInvalidArgument,
		invalidToken: kinesisInvalidNextToken,
	}
	pr, pageErr := jsonNextToken(paging, nil, body.NextToken, body.MaxResults)
	if pageErr != nil {
		return nil, pageErr
	}
	pr.Size = min(pr.Size, kinesisListShardsMaxPage)

	var (
		target kinesisStreamTarget
		filter = body.ShardFilter
		after  *cursorKey
	)
	if body.NextToken != "" {
		// A slice, not a map, so a request carrying several conflicting members names the same one
		// every time.
		for _, conflict := range []struct {
			member string
			set    bool
		}{
			{"StreamName", body.StreamName != ""},
			{"ExclusiveStartShardId", body.ExclusiveStartShardID != ""},
			{"StreamCreationTimestamp", body.StreamCreationTimestamp != nil},
		} {
			if conflict.set {
				return nil, kinesisInvalidArgument(conflict.member + " cannot be specified with NextToken")
			}
		}
		cursor, err := pr.cursor()
		if err != nil {
			return nil, kinesisInvalidNextToken()
		}
		var tc kinesisListShardsContext
		if json.Unmarshal([]byte(cursor.Tie), &tc) != nil {
			return nil, kinesisInvalidNextToken()
		}
		if body.StreamARN != "" && body.StreamARN != tc.StreamARN {
			return nil, kinesisInvalidArgument("StreamARN names a different stream than NextToken")
		}
		if filter != nil && !kinesisShardFiltersEqual(filter, tc.Filter) {
			return nil, kinesisInvalidArgument("ShardFilter differs from the one NextToken was issued for")
		}
		var refErr *AWSError
		if target, refErr = kinesisParseStreamARN(tc.StreamARN); refErr != nil {
			return nil, kinesisInvalidNextToken()
		}
		filter, after = tc.Filter, cursor
	} else {
		var refErr *AWSError
		if target, refErr = kinesisResolveStream(ctx, body.kinesisStreamRef); refErr != nil {
			return nil, refErr
		}
		if body.ExclusiveStartShardID != "" {
			after = &cursorKey{Sort: body.ExclusiveStartShardID}
		}
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	if refusal := kinesisCheckCreationTimestamp(stream, target, body.StreamCreationTimestamp); refusal != nil {
		return nil, refusal
	}
	if filter != nil && filter.Type == kinesisShardFilterAfterShardID && after == nil {
		after = &cursorKey{Sort: filter.ShardID}
	}

	tieBytes, err := json.Marshal(kinesisListShardsContext{StreamARN: kinesisStreamARN(target), Filter: filter})
	if err != nil {
		return nil, fmt.Errorf("kinesis listShards token context: %w", err)
	}
	tie := string(tieBytes)
	shards := kinesisFilterShards(stream, filter, p.tc.Now())
	// The cursor is compared by shard ID alone: a token's Tie and an ExclusiveStartShardId's empty
	// one must both resume strictly after the named shard.
	key := func(s KinesisShard) cursorKey { return cursorKey{Sort: s.ShardID} }
	if after != nil {
		after = &cursorKey{Sort: after.Sort}
	}
	res := pageAfter(shards, key, after, pr.Size, false)
	out := map[string]any{"Shards": res.Items}
	if res.Truncated {
		out["NextToken"] = pr.encode(cursorKey{Sort: res.Last.Sort, Tie: tie})
	}
	return kinesisJSONResponse(http.StatusOK, out)
}

// kinesisShardFiltersEqual reports whether two filters select the same shards.
func kinesisShardFiltersEqual(a, b *kinesisShardFilter) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Type != b.Type || a.ShardID != b.ShardID || (a.Timestamp == nil) != (b.Timestamp == nil) {
		return false
	}
	// At millisecond precision, the precision the token renders a timestamp at (EpochSeconds) and
	// the one API_ShardFilter publishes for it.
	return a.Timestamp == nil || time.Time(*a.Timestamp).UnixMilli() == time.Time(*b.Timestamp).UnixMilli()
}

// kinesisFilterShards returns the shards a filter selects, as of now. No filter is
// FROM_TRIM_HORIZON, the published default.
func kinesisFilterShards(stream KinesisStream, filter *kinesisShardFilter, now time.Time) []KinesisShard {
	trimHorizon := now.Add(-time.Duration(stream.RetentionPeriodHours) * time.Hour)
	if trimHorizon.Before(stream.CreatedAt) {
		trimHorizon = stream.CreatedAt
	}
	kind := kinesisShardFilterFromTrimHorizon
	var at time.Time
	if filter != nil {
		kind = filter.Type
		if filter.Timestamp != nil {
			at = time.Time(*filter.Timestamp)
		}
	}
	// "Corrected to TRIM_HORIZON of the data stream if FROM_TIMESTAMP is less than the
	// TRIM_HORIZON value."
	if kind == kinesisShardFilterFromTimestamp && at.Before(trimHorizon) {
		at = trimHorizon
	}

	out := make([]KinesisShard, 0, len(stream.Shards))
	for _, shard := range stream.Shards {
		start := kinesisSequenceTime(shard.SequenceNumberRange.StartingSequenceNumber, stream.CreatedAt)
		open := kinesisShardOpen(shard)
		end := kinesisSequenceTime(shard.SequenceNumberRange.EndingSequenceNumber, stream.CreatedAt)
		var keep bool
		switch kind {
		case kinesisShardFilterAtLatest:
			keep = open
		case kinesisShardFilterAtTrimHorizon:
			keep = !start.After(trimHorizon) && (open || !end.Before(trimHorizon))
		case kinesisShardFilterAtTimestamp:
			keep = !start.After(at) && (open || !end.Before(at))
		case kinesisShardFilterFromTimestamp:
			keep = open || !end.Before(at)
		case kinesisShardFilterAfterShardID, kinesisShardFilterFromTrimHorizon:
			keep = open || !end.Before(trimHorizon)
		}
		if keep {
			out = append(out, shard)
		}
	}
	return out
}

// kinesisSequenceTime returns the time a sequence number was minted at: its leading Unix
// nanoseconds, or fallback for one that carries none ("0", the first sequence number of a shard
// CreateStream made).
func kinesisSequenceTime(seq string, fallback time.Time) time.Time {
	prefix, _, _ := strings.Cut(seq, "-")
	n, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || n == 0 {
		return fallback
	}
	return time.Unix(0, n)
}
