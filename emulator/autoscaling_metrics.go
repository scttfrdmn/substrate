package emulator

// Group metrics collection: EnableMetricsCollection, DisableMetricsCollection and
// DescribeMetricCollectionTypes (#1594).
//
// What is modeled is the switch, which is what a caller can observe through Auto Scaling: the
// enabled set is stored on the group and reported as DescribeAutoScalingGroups' EnabledMetrics.
// The metrics themselves are not published to CloudWatch, because nothing emits group metrics in
// substrate yet; a GetMetricData for AWS/AutoScaling answers no datapoints, as it would for a group
// whose collection is off.
//
// The refusals are ValidationError/400, the Common Error Types code for "The input fails to satisfy
// the constraints specified by an AWS service". No operation page publishes a specific code or
// wording for an unknown metric or granularity, so the messages are substrate's own.

// asgMetricNames are the group metrics EnableMetricsCollection accepts, in the order
// DescribeMetricCollectionTypes' published sample lists them.
var asgMetricNames = []string{ //nolint:gochecknoglobals // read-only reference data from API_DescribeMetricCollectionTypes.
	"GroupMinSize", "GroupMaxSize", "GroupDesiredCapacity", "GroupInServiceInstances",
	"GroupPendingInstances", "GroupTerminatingInstances", "GroupStandbyInstances", "GroupTotalInstances",
	"GroupInServiceCapacity", "GroupPendingCapacity", "GroupStandbyCapacity", "GroupTerminatingCapacity",
	"GroupTotalCapacity", "WarmPoolDesiredCapacity", "WarmPoolWarmedCapacity", "WarmPoolPendingCapacity",
	"WarmPoolTerminatingCapacity", "WarmPoolTotalCapacity", "GroupAndWarmPoolDesiredCapacity",
	"GroupAndWarmPoolTotalCapacity",
}

// asgMetricGranularity is the only published Granularity ("The only valid value is 1Minute").
const asgMetricGranularity = "1Minute"

// asgRequestedMetrics reads Metrics.member.N, all of them when omitted, refusing a name that is not
// a group metric.
func asgRequestedMetrics(req *AWSRequest) ([]string, error) {
	names := iamMemberList(req.Params, "Metrics")
	if len(names) == 0 {
		return asgMetricNames, nil
	}
	for _, n := range names {
		if !containsStr(asgMetricNames, n) {
			return nil, asgValidationError("Metric %s is not a valid group metric.", n)
		}
	}
	return names, nil
}

func (p *AutoScalingPlugin) enableMetricsCollection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeMetricsCollection(reqCtx, req, "EnableMetricsCollection", true)
}

func (p *AutoScalingPlugin) disableMetricsCollection(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeMetricsCollection(reqCtx, req, "DisableMetricsCollection", false)
}

// changeMetricsCollection enables or disables the named group metrics, or all of them: "If you
// specify Granularity and don't specify any metrics, all metrics are enabled"
// (API_EnableMetricsCollection), and "If you omit this property, all metrics are disabled"
// (API_DisableMetricsCollection). It is modeled on [AutoScalingPlugin.changeProcesses].
func (p *AutoScalingPlugin) changeMetricsCollection(reqCtx *RequestContext, req *AWSRequest, operation string, enable bool) (*AWSResponse, error) {
	g, err := p.requireGroup(reqCtx, req)
	if err != nil {
		return nil, err
	}
	granularity := req.Params["Granularity"]
	if enable {
		// Granularity is Required: Yes on EnableMetricsCollection, and has no member on Disable.
		if granularity == "" {
			return nil, asgMissingParameter("Granularity")
		}
		if granularity != asgMetricGranularity {
			return nil, asgValidationError("Granularity %s is not valid; the only valid value is %s.", granularity, asgMetricGranularity)
		}
	}
	names, err := asgRequestedMetrics(req)
	if err != nil {
		return nil, err
	}
	enabled := make(map[string]bool, len(asgMetricNames))
	for _, em := range g.EnabledMetrics {
		enabled[em.Metric] = true
	}
	for _, n := range names {
		enabled[n] = enable
	}
	// Rebuilt in the published order, so a describe is stable however the metrics were enabled.
	kept := make([]AutoScalingEnabledMetric, 0, len(asgMetricNames))
	for _, n := range asgMetricNames {
		if enabled[n] {
			kept = append(kept, AutoScalingEnabledMetric{Metric: n, Granularity: asgMetricGranularity})
		}
	}
	g.EnabledMetrics = kept
	if err := p.saveGroup(reqCtx, g); err != nil {
		return nil, err
	}
	return asgNoResultResponse(reqCtx, operation)
}

// describeMetricCollectionTypes answers the published catalog: one granularity and the twenty group
// metrics, as API_DescribeMetricCollectionTypes' sample response lists them.
func (p *AutoScalingPlugin) describeMetricCollectionTypes(reqCtx *RequestContext) (*AWSResponse, error) {
	type granularity struct {
		Granularity string `xml:"Granularity"`
	}
	type metric struct {
		Metric string `xml:"Metric"`
	}
	type result struct {
		Granularities asgList[granularity] `xml:"Granularities"`
		Metrics       asgList[metric]      `xml:"Metrics"`
	}
	metrics := make([]metric, len(asgMetricNames))
	for i, n := range asgMetricNames {
		metrics[i] = metric{Metric: n}
	}
	return asgOKResponse(reqCtx, "DescribeMetricCollectionTypes", result{
		Granularities: asgListOf([]granularity{{Granularity: asgMetricGranularity}}),
		Metrics:       asgListOf(metrics),
	})
}
