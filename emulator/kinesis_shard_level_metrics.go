package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// Shard-level metrics on a Kinesis stream — #999.
//
// API_EnableEnhancedMonitoring and API_DisableEnhancedMonitoring publish a byte-identical
// ShardLevelMetrics shape: Required: Yes, "Array Members: Minimum number of 1 item. Maximum number of
// 7 items", and an eight-entry Valid Values enum — the seven metric names plus ALL. Substrate decoded
// the member and checked none of that, so an empty array, a fifty-item array and a misspelled metric
// name were all accepted and written to the stream.
//
// Three readings this file makes, each because the pages disagree with themselves:
//
//  1. **ALL is expanded into the seven, so it never appears in a response.** The enum's eighth entry
//     is listed on the *response* arrays too, which would let AWS report the literal ALL; but
//     "The value "ALL" enables every metric" composed with DesiredShardLevelMetrics' own description —
//     "the list of all the metrics that would be in the enhanced state after the operation" — names
//     seven metrics, not one wildcard. Expanding is also the only reading under which Disable works
//     at all: against a stored ["ALL"], disabling IncomingBytes would remove nothing. The eighth
//     entry on the response side is taken as an artifact of one MetricsName enum shape reused in both
//     directions.
//  2. **The seven-item maximum is enforced as published.** The eight-entry enum cannot fit in a
//     seven-item array, so a caller naming every metric *and* ALL is refused — and does not need to
//     ask, because ALL alone is one item and already means all seven. Only the enum and the bound can
//     both be honored; a maximum AWS states is the half substrate keeps, per the rule that only what
//     the API model states is modeled.
//  3. **An empty array is refused, where an empty Tags map is accepted.** This is the opposite
//     reading from [kinesisValidateTagMap], and deliberately so: Tags publishes a maximum and no
//     minimum, while ShardLevelMetrics publishes "Minimum number of 1 item". Each operation gets the
//     bound its own shape states.
//
// InvalidArgumentException/400 is the landing code. Both pages publish it, its description —
// "a specified parameter exceeds its restrictions, is not supported, or can't be used" — is exactly
// this case, and it is the code the sibling tag-constraint refusals already use.
//
// Response ordering is substrate's: neither page states one, and a set has none, so the enabled
// metrics are always rendered in the page's own bullet order. That makes the two response arrays
// reproducible whatever order a caller sent, which is what a replayed event log needs, and follows
// ListTagsForStream's choice to sort an unordered map by key rather than report Go's iteration order.

// kinesisShardLevelMetricAll is the wildcard the two pages publish alongside the seven metric names.
const kinesisShardLevelMetricAll = "ALL"

// kinesisMaxShardLevelMetrics is the published "Maximum number of 7 items" on both operations'
// ShardLevelMetrics array and on both of their response arrays.
const kinesisMaxShardLevelMetrics = 7

// kinesisShardLevelMetricNames returns the seven metric names in the order both pages list them.
//
// A function rather than a package-level slice so the order is stated once and cannot be reordered in
// place by a caller that renders it.
func kinesisShardLevelMetricNames() []string {
	return []string{
		"IncomingBytes",
		"IncomingRecords",
		"OutgoingBytes",
		"OutgoingRecords",
		"WriteProvisionedThroughputExceeded",
		"ReadProvisionedThroughputExceeded",
		"IteratorAgeMilliseconds",
	}
}

// kinesisIsShardLevelMetric reports whether name is one of the eight values the enum publishes.
func kinesisIsShardLevelMetric(name string) bool {
	if name == kinesisShardLevelMetricAll {
		return true
	}
	for _, known := range kinesisShardLevelMetricNames() {
		if name == known {
			return true
		}
	}
	return false
}

// kinesisValidateShardLevelMetrics checks a ShardLevelMetrics member against the three constraints its
// own shape publishes: present, 1 to 7 items, and every entry in the enum.
//
// The message names the offending value and the enum, because a caller whose one misspelled metric is
// among seven cannot act on a refusal that will not say which. See the file comment for why an empty
// array is refused here although an empty Tags map is not.
func kinesisValidateShardLevelMetrics(metrics []string) *AWSError {
	if metrics == nil {
		return kinesisInvalidArgument("ShardLevelMetrics is required")
	}
	if len(metrics) < 1 {
		return kinesisInvalidArgument("ShardLevelMetrics must contain at least 1 item")
	}
	if len(metrics) > kinesisMaxShardLevelMetrics {
		return kinesisInvalidArgument(fmt.Sprintf(
			"ShardLevelMetrics may contain at most %d items; got %d",
			kinesisMaxShardLevelMetrics, len(metrics)))
	}
	for _, metric := range metrics {
		if !kinesisIsShardLevelMetric(metric) {
			return kinesisInvalidArgument(fmt.Sprintf(
				"%q is not a valid shard-level metric; valid values are %s",
				metric, strings.Join(append(kinesisShardLevelMetricNames(), kinesisShardLevelMetricAll), ", ")))
		}
	}
	return nil
}

// kinesisShardLevelMetricSet reads a list of metric names into a set, expanding ALL into the seven.
//
// It is used on both the request's list and the stream's stored list. Applying it to stored state too
// is what keeps a record written before #999 — which could hold the literal ALL — from reading back as
// a stream with exactly one metric enhanced. Unknown names are dropped rather than carried, since a
// request carrying one never reaches here: [kinesisValidateShardLevelMetrics] refuses it first, so a
// stored unknown can only be a record substrate should not have written.
func kinesisShardLevelMetricSet(metrics []string) map[string]struct{} {
	set := make(map[string]struct{}, kinesisMaxShardLevelMetrics)
	for _, metric := range metrics {
		if metric == kinesisShardLevelMetricAll {
			for _, name := range kinesisShardLevelMetricNames() {
				set[name] = struct{}{}
			}
			continue
		}
		if kinesisIsShardLevelMetric(metric) {
			set[metric] = struct{}{}
		}
	}
	return set
}

// kinesisRenderShardLevelMetrics renders a metric set in the page's own order.
//
// The slice is always non-nil, so an empty set marshals as [] rather than null. Both pages' Sample
// Responses show an empty array on one of the two members — Enable's CurrentShardLevelMetrics and
// Disable's DesiredShardLevelMetrics — although both members publish "Minimum number of 1 item", so
// the samples are the authority on the shape a caller has to handle and the minimum cannot be read as
// a response guarantee. This is the #938 rule: an empty collection is [], never null.
func kinesisRenderShardLevelMetrics(set map[string]struct{}) []string {
	rendered := make([]string, 0, len(set))
	for _, name := range kinesisShardLevelMetricNames() {
		if _, ok := set[name]; ok {
			rendered = append(rendered, name)
		}
	}
	return rendered
}

// kinesisEnhancedMonitoringResponse builds the body both operations publish.
//
// Current is the set before the operation and Desired the set after, per their descriptions —
// "the current state of the metrics that are in the enhanced state before the operation" and "the list
// of all the metrics that would be in the enhanced state after the operation". Substrate rendered the
// *same* slice for both, read after the write, so Current reported the after-state on every call; and
// because disableEnhancedMonitoring filtered its stored slice in place, the before-state had already
// been overwritten by the time either was rendered, which is why the fix builds two sets rather than
// reordering two renders.
//
// StreamARN is rendered from the resolved target rather than the stored record, so it names the stream
// the request actually addressed — the account and Region an ARN-only request resolved to, not the
// caller's own (#966).
func kinesisEnhancedMonitoringResponse(
	target kinesisStreamTarget,
	before, after map[string]struct{},
) (*AWSResponse, error) {
	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamName":               target.Name,
		"StreamARN":                kinesisStreamARN(target),
		"CurrentShardLevelMetrics": kinesisRenderShardLevelMetrics(before),
		"DesiredShardLevelMetrics": kinesisRenderShardLevelMetrics(after),
	})
}
