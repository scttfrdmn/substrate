package emulator

// No ELB response carried the Query protocol's `ResponseMetadata` (#1149).
//
// Every sample response on every Elastic Load Balancing page, in both generations, closes with the
// envelope — e.g. the 2012-06-01 `API_CreateLoadBalancer`:
//
//	<CreateLoadBalancerResponse xmlns="http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/">
//	  <CreateLoadBalancerResult>
//	    <DNSName>my-vpc-loadbalancer-1234567890.us-east-1.elb.amazonaws.com</DNSName>
//	  </CreateLoadBalancerResult>
//	  <ResponseMetadata>
//	    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
//	  </ResponseMetadata>
//	</CreateLoadBalancerResponse>
//
// Substrate emitted none of it, uniformly: no ELB response struct declared the member, so every
// response — ELBv2 and classic, result-bearing and memberless — stopped after the result element.
// The envelope ELB built for that fix is now the shared awsQuery builder, [queryOKResponse] in
// `query_envelope.go`, which Redshift, Auto Scaling, RDS and ElastiCache answer through too (#1242);
// every ELB handler calls it with [elbXMLNS] or [elbClassicXMLNS] to select the generation. What
// stays here is the ELB-specific half: every memberless ELB operation publishes the empty result
// element, and the helpers below name that class at the call site.

// elbEmptyResult is the result of an operation whose output shape declares no members.
//
// A named type rather than a bare `struct{}{}` at eight call sites, so the class is visible in the
// call: these are the operations whose whole published response is the empty result element and the
// metadata.
type elbEmptyResult struct{}

// elbEmptyOKResponse answers an ELBv2 operation whose output shape carries no members.
//
// The empty result element is not decoration: ELBv2 speaks the Query protocol, where every output
// shape declares a `resultWrapper` and botocore looks that wrapper up by name in the parsed body, so
// a bare `<OperationResponse/>` makes the AWS CLI and boto3 raise
// `KeyError: 'DeleteLoadBalancerResult'` instead of reporting success. Six operations answered that
// way and were unusable from a real client while passing substrate's own tests, which read the XML
// directly rather than through an SDK's parser (#748).
func elbEmptyOKResponse(reqCtx *RequestContext, operation string) (*AWSResponse, error) {
	return queryOKResponse(reqCtx, operation, elbXMLNS, elbEmptyResult{})
}

// elbClassicEmptyOKResponse answers a classic operation whose output shape carries no members.
//
// The 2012-06-01 twin of [elbEmptyOKResponse]; the two differ in the document namespace and in
// nothing else. `DeleteLoadBalancer` is the one routed operation in this class, and its published
// sample response is `<DeleteLoadBalancerResult/>` followed by the metadata.
func elbClassicEmptyOKResponse(reqCtx *RequestContext, operation string) (*AWSResponse, error) {
	return queryOKResponse(reqCtx, operation, elbClassicXMLNS, elbEmptyResult{})
}
