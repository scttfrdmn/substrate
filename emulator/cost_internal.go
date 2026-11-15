package emulator

// A charge incurred on behalf of a request (#1202).
//
// Cost is computed once per top-level request, in [Server.handleAWSRequest], from the request the
// caller sent. A plugin that dispatches further requests through [PluginRegistry.RouteRequest] — the
// execute-api proxy invoking a Lambda function is the case that motivated this — reaches no cost
// controller, so before #1202 everything such a request caused was free: a stage invocation, and the
// function it invoked.
//
// Two places could have closed that. Consulting the controller inside RouteRequest would price every
// internal route in the emulator, CloudFormation's included, which already prices its own dispatches
// (StackDeployer.dispatch) and would then be charged twice. The proxy's call site is the narrower
// change, and it is the one taken: a plugin names the charges a request incurred on its behalf with
// [chargeOnBehalf], and the server prices them with the request and folds them into the one cost the
// request's event records. No child event is recorded, so the event log is unchanged in shape, and a
// replay — which recomputes no cost — reads back the folded figure the recording wrote.
//
// The consequence a reader of a cost summary should know is attribution: the folded charge is part of
// the outer event, so `ByService` reports a proxied Lambda invocation's cost under execute-api, not
// under lambda. The two cost keys stay separate in the table, so an override still prices each.

// internalChargesMetadataKey is the [RequestContext.Metadata] key under which charges accumulate.
// Metadata is per request and never recorded on an event, so the list lives and dies with the request.
const internalChargesMetadataKey = "substrate.internal_charges"

// internalCharge is one charge a request incurred on its behalf: the request it is priced as, and how
// many units of that price it costs.
type internalCharge struct {
	req   *AWSRequest
	units int
}

// chargeOnBehalf records that the request reqCtx belongs to incurred units of req's price. A nil
// context, or one with no metadata, records nothing: such a request was not dispatched by a caller
// that prices anything.
func chargeOnBehalf(reqCtx *RequestContext, req *AWSRequest, units int) {
	if reqCtx == nil || reqCtx.Metadata == nil || req == nil || units <= 0 {
		return
	}
	charges, _ := reqCtx.Metadata[internalChargesMetadataKey].([]internalCharge)
	reqCtx.Metadata[internalChargesMetadataKey] = append(charges, internalCharge{req: req, units: units})
}

// costOnBehalf prices every charge recorded against reqCtx.
func (c *CostController) costOnBehalf(reqCtx *RequestContext) float64 {
	if c == nil || reqCtx == nil || reqCtx.Metadata == nil {
		return 0
	}
	charges, _ := reqCtx.Metadata[internalChargesMetadataKey].([]internalCharge)
	var total float64
	for _, ch := range charges {
		total += c.CostForRequest(ch.req) * float64(ch.units)
	}
	return total
}
