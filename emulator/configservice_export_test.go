package emulator

// This file exports the AWS Config authorization helpers for the external test
// package, following organizations_export_test.go. It is compiled only under test.
//
// The ARN builders are exported because their identifier components are minted by
// hash: a test cannot spell one out without asserting the hash, and cannot store a
// tag under a resource's ARN without knowing it. Exporting the builder rather than
// the hash keeps the test asserting the *resolution* — which operation names which
// resource — rather than the digest.

// CfgsvcAuthzResourceARNForTest wraps cfgsvcAuthzResourceARN for external tests.
func CfgsvcAuthzResourceARNForTest(reqCtx *RequestContext, req *AWSRequest) string {
	return cfgsvcAuthzResourceARN(reqCtx, req)
}

// CfgsvcRecorderARNForTest wraps cfgsvcRecorderARN for external tests.
func CfgsvcRecorderARNForTest(ctx *RequestContext, name string) string {
	return cfgsvcRecorderARN(ctx, name)
}

// CfgsvcRuleARNForTest builds a Config rule's ARN from its name, minting the rule ID
// the way the handlers do.
func CfgsvcRuleARNForTest(ctx *RequestContext, name string) string {
	return cfgsvcRuleARN(ctx, cfgsvcMintRuleID(ctx.AccountID, ctx.Region, name))
}

// CfgsvcPackARNForTest builds a conformance pack's ARN from its name, minting the pack
// ID the way the handlers do.
func CfgsvcPackARNForTest(ctx *RequestContext, name string) string {
	return cfgsvcPackARN(ctx, name, cfgsvcMintPackID(ctx.AccountID, ctx.Region, name))
}

// CfgsvcTagsKeyForTest wraps cfgsvcTagsKey for external tests.
func CfgsvcTagsKeyForTest(arn string) string { return cfgsvcTagsKey(arn) }

// ConfigServiceNamespaceForTest is the state namespace AWS Config stores under.
const ConfigServiceNamespaceForTest = configServiceNamespace
