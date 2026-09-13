package emulator

// cfn_intrinsics.go holds the per-resource-type resolution rules for CloudFormation's
// intrinsic functions.
//
// CloudFormation's Ref does not return one kind of value. Each resource type's Template
// Reference page documents its own "Return values" section, and the value there is an ARN for
// some types, a name for others, an opaque service-assigned ID for others, and a URL for one.
// resolveRef returned DeployedResource.PhysicalID for every type, which is the service-native
// identifier substrate addresses a resource by — correct for the majority of types and the
// wrong *kind of thing* for the rest (#827).
//
// Nothing refuses a wrong-kind value. A template writing `LoadBalancerArn: !Ref lb` passed a
// bare load balancer name to CreateListener, so AWS's own ELBv2 template shape did not deploy;
// where a service was more permissive the wrong value was simply stored.

import "strings"

// cfnRefValue returns the value AWS documents `Ref` as returning for dr's resource type.
//
// The second result reports whether this type has a per-type rule at all. When it is false the
// caller falls back to DeployedResource.PhysicalID, which is what AWS documents for the
// majority of types — a name for an S3 bucket or an IAM role, an ID for a VPC or an instance.
//
// When it is true the returned string is used verbatim, **including when it is empty**. An empty
// value means the deploy did not yield the source the documented value is built from, which
// happens only for a resource that failed and is about to be reported CREATE_FAILED. Falling
// back to the physical ID there would hand back exactly the wrong-kind value this function
// exists to stop, and a plausible-looking wrong answer is worse than an obviously missing one —
// the same rule resolveFnGetAtt states on its ConfigRuleId arm.
//
// PhysicalID is deliberately not changed to carry these values. It is what
// DescribeStackResources reports, what cfnResolveStampTarget builds every tag state key from,
// and what a redeploy recognizes an existing resource by, so the Ref value is *derived* here
// instead. Whether the reported PhysicalResourceId should also become per-type is #837.
func cfnRefValue(dr DeployedResource, cctx *cfnContext) (string, bool) {
	switch dr.Type {
	// ----- Ref is the resource's ARN ---------------------------------------------------
	//
	// For these types AWS documents Ref as the ARN, and substrate stored a name. This is the
	// group that broke real templates: an ELBv2 listener names its load balancer and its
	// target group by Ref, and both are validated as ARNs by the receiving operation.
	case "AWS::ElasticLoadBalancingV2::LoadBalancer",
		"AWS::ElasticLoadBalancingV2::TargetGroup",
		"AWS::ECS::Service",
		"AWS::StepFunctions::StateMachine",
		"AWS::StepFunctions::Activity",
		"AWS::AppSync::GraphQLApi",
		"AWS::AppSync::DataSource",
		"AWS::AppSync::Resolver",
		"AWS::AppSync::FunctionConfiguration",
		"AWS::Transfer::Server":
		return dr.ARN, true

	// ----- Ref is a value recorded at deploy time --------------------------------------

	case "AWS::KMS::Key", "AWS::KMS::ReplicaKey":
		// "Ref returns the key ID". substrate stores the key ARN as the physical ID, which is
		// what its own KMS operations accept, so the ID is read from where deployKMSKey
		// records it rather than cut off the end of the ARN.
		return cfnRefMetadata(dr, "KeyId"), true

	case "AWS::EC2::EIP":
		// "Ref returns the Elastic IP address" — not the allocation ID, which is what
		// substrate addresses the allocation by and stores as the physical ID.
		return cfnRefMetadata(dr, "PublicIp"), true

	case "AWS::CloudTrail::Trail":
		// "Ref returns the resource name", where substrate's physical ID is the trail ARN.
		return cfnRefMetadata(dr, "TrailName"), true

	// ----- Ref is a composite ----------------------------------------------------------

	case "AWS::WAFv2::WebACL":
		// "The Ref for the resource, containing the resource name, physical ID, and scope,
		// formatted as follows: name|id|scope", example
		// "my-webacl-name|1234a1a-a1b1-12a1-abcd-a123b123456|REGIONAL". The scope is the
		// template's own Scope property, CLOUDFRONT or REGIONAL, spelled as AWS spells it in
		// the example rather than lowercased as the ARN segment is.
		name, scope := cfnRefMetadata(dr, "Name"), cfnRefMetadata(dr, "Scope")
		if name == "" || dr.PhysicalID == "" || scope == "" {
			return "", true
		}
		return name + "|" + dr.PhysicalID + "|" + scope, true

	case "AWS::ApiGateway::UsagePlanKey":
		// "Ref returns the ID of the key and ID of the usage plan combined with a ':', such
		// as 123abcdef:abc123." The key ID is the physical ID; the plan ID is recorded by
		// deployAPIGatewayUsagePlanKey, which resolves it to build the request path.
		planID := cfnRefMetadata(dr, "UsagePlanId")
		if dr.PhysicalID == "" || planID == "" {
			return "", true
		}
		return dr.PhysicalID + ":" + planID, true

	// ----- Ref is derived from the stack's own region and account ----------------------

	case "AWS::SQS::Queue":
		// "Ref returns the queue URL", which is a different string from both the name and the
		// ARN, and the one a caller passes straight back as a QueueUrl.
		//
		// The URL is built by sqsQueueURL — the same function the SQS plugin's CreateQueue
		// answers with — so the value a template resolves is byte-identical to the one the
		// API hands out, and neither can drift from the other. AWS's own URLs are
		// https://sqs.{region}.amazonaws.com/...; substrate's point at its own endpoint,
		// because a Ref the emulator's own SQS operations reject would be useless.
		if dr.PhysicalID == "" {
			return "", true
		}
		return sqsQueueURL(cctx.region, cctx.accountID, dr.PhysicalID), true
	}
	return "", false
}

// cfnRefMetadata reads a string out of dr.Metadata, or returns empty if it is absent.
//
// Empty rather than a fallback: every caller here is resolving a value whose only correct source
// is the one being read, so a substitute would be a guess. See cfnRefValue on why an empty Ref
// is the right answer when the source is missing.
func cfnRefMetadata(dr DeployedResource, key string) string {
	if dr.Metadata == nil {
		return ""
	}
	v, ok := dr.Metadata[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}
