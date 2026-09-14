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

import (
	"fmt"
	"strings"
)

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
// DescribeStackResources reports, what most aws:cloudformation:* tag state keys are built from
// (cfnResolveStampTarget), and the key the deletion path and drift detection address a resource
// by, so the Ref value is *derived* here instead. Whether the reported PhysicalResourceId
// should also become per-type is #837.
//
// Two earlier justifications for that are corrected here rather than left standing, because
// both were checkable and neither held (#837). A redeploy does *not* recognize a resource by
// its physical ID: deployedResource matches on logical ID with no physical-ID fallback, and
// recognition is clearUnchangedRedeploys, whose own comment gives the reason the physical ID
// cannot be the key — a refused create returns none at all. And the stamp claim was "every"
// key, which the four ELBv2 types disprove: cfnStampELBResource finds the record by ARN and
// bypasses cfnResolveStampTarget entirely.
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

// cfnGetAttValue returns the value AWS documents `Fn::GetAtt` as returning for attr on dr's
// resource type.
//
// The resolver it replaced switched on the **attribute name alone**, which made an attribute name
// two services share resolve with the wrong service's rule: `Arn` fell back to the physical ID, so
// `!GetAtt Bucket.Arn` answered the bucket *name*; `DomainName` was CloudFront's arm, so
// `!GetAtt Bucket.DomainName` answered the name too; `Value` returned the physical ID, so
// `!GetAtt Param.Value` answered an SSM parameter's *name* instead of its value; and every
// unrecognized attribute fell through to the physical ID (#827).
//
// The rule is now the one the ConfigRuleId arm already stated: an attribute this function cannot
// answer resolves to **empty**, never to the physical ID, because a bare name where an ARN, a URL
// or a stored value belongs is a plausible-looking wrong answer that a stack Output would carry
// and a test would assert against happily. Empty is distinguishable from a real value; a name is
// not.
//
// Resolution runs in three steps, each answering only from a fact rather than a guess:
//
//  1. A per-type rule (cfnGetAttPerType), for the attributes whose value is specific to the type —
//     an S3 bucket's four domain names, a log group's ":*"-suffixed ARN, an ELBv2 full name.
//  2. dr.Metadata under the attribute's own name, which is the channel every deploy helper already
//     records a GetAtt-resolvable attribute through (RepositoryUri, Endpoint.Address, InvokeURL,
//     ProviderName, AllocationId, …). A hit means the helper recorded *this* attribute, so there is
//     nothing to infer.
//  3. cfnGetAttARN, when the attribute's name says it returns an ARN. This is a check on the
//     attribute's spelling rather than a table of 113 resource types, which is why LoadBalancerArn,
//     ListenerArn, TopicArn, TaskDefinitionArn and ResourceARN all resolve without an arm each.
//
// Step 3's one blind spot is an attribute naming a *different* resource's ARN — DynamoDB's
// StreamArn is the stream's, not the table's — so those get an explicit per-type arm that answers
// empty rather than let the rule hand back the wrong ARN.
func cfnGetAttValue(dr DeployedResource, attr string, cctx *cfnContext) string {
	if v, ok := cfnGetAttPerType(dr, attr, cctx); ok {
		return v
	}
	if v := cfnGetAttMetadata(dr, attr); v != "" {
		return v
	}
	if cfnGetAttNamesAnARN(attr) {
		return cfnGetAttARN(dr)
	}
	return ""
}

// cfnGetAttMetadata reads a recorded attribute out of dr.Metadata, formatted as a string.
//
// Unlike cfnRefMetadata this formats a non-string value rather than treating it as absent, because
// a deploy helper records an endpoint port as it read it — a JSON number, not a string — and a port
// is a value Fn::GetAtt must be able to answer.
func cfnGetAttMetadata(dr DeployedResource, key string) string {
	if dr.Metadata == nil {
		return ""
	}
	v, ok := dr.Metadata[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return fmt.Sprintf("%v", v)
}

// cfnGetAttPerType answers the attributes whose value depends on the resource type.
//
// The second result reports whether this type has a rule for *this* attribute. False sends the
// caller on to the generic steps, so a type listed here still resolves its ARN-shaped and
// metadata-recorded attributes the same way every other type does — a listed type is not a closed
// set of attributes.
//
// Only attributes whose value is not already reachable generically appear here. An attribute AWS
// documents that substrate retains nothing for is deliberately absent: it falls through and
// resolves to empty, which is the honest answer for a value the emulator does not model.
func cfnGetAttPerType(dr DeployedResource, attr string, cctx *cfnContext) (string, bool) {
	switch dr.Type {
	case "AWS::S3::Bucket":
		// Every one of these is a pure function of the bucket name and the region, built to the
		// forms the Template Reference gives as examples: DOC-EXAMPLE-BUCKET.s3.amazonaws.com,
		// DOC-EXAMPLE-BUCKET.s3.us-east-2.amazonaws.com,
		// DOC-EXAMPLE-BUCKET.s3.dualstack.us-east-2.amazonaws.com and
		// http://DOC-EXAMPLE-BUCKET.s3-website.us-east-2.amazonaws.com. They name AWS's real
		// endpoints rather than substrate's, because they are values a template passes to another
		// resource (a CloudFront origin, a redirect target), not endpoints the emulator serves.
		if dr.PhysicalID == "" {
			// A failed create: every form below would otherwise render a hostname with an empty
			// label, like ".s3.amazonaws.com", which reads as a real value.
			switch attr {
			case "DomainName", "RegionalDomainName", "DualStackDomainName", "WebsiteURL":
				return "", true
			}
			return "", false
		}
		switch attr {
		case "DomainName":
			return dr.PhysicalID + ".s3.amazonaws.com", true
		case "RegionalDomainName":
			return dr.PhysicalID + ".s3." + cctx.region + ".amazonaws.com", true
		case "DualStackDomainName":
			return dr.PhysicalID + ".s3.dualstack." + cctx.region + ".amazonaws.com", true
		case "WebsiteURL":
			return "http://" + dr.PhysicalID + ".s3-website." + cctx.region + ".amazonaws.com", true
		}

	case "AWS::Logs::LogGroup":
		if attr == "Arn" {
			// A log group's GetAtt Arn carries a trailing ":*" —
			// "arn:aws:logs:us-west-1:123456789012:log-group:/mystack-testgroup-12ABC1AB12A1:*" —
			// which is also the form the Logs API's own `arn` member reports. Built by the
			// function that builds it there, so the two cannot disagree; it is idempotent, so an
			// ARN already carrying the suffix is unchanged.
			return cwLogGroupPolicyARN(dr.ARN), true
		}

	case "AWS::ElasticLoadBalancingV2::LoadBalancer":
		switch attr {
		case "LoadBalancerName":
			return dr.PhysicalID, true
		case "LoadBalancerFullName":
			// "app/my-load-balancer/50dc6c495c0c9188" — the ARN's resource portion with the
			// "loadbalancer/" segment dropped, which is exactly what the ARN carries after it.
			return cfnARNAfter(dr.ARN, "loadbalancer/", ""), true
		}

	case "AWS::ElasticLoadBalancingV2::TargetGroup":
		switch attr {
		case "TargetGroupName":
			return dr.PhysicalID, true
		case "TargetGroupFullName":
			// "targetgroup/my-target-group/73e2d6bc24d8a067" — unlike a load balancer's full
			// name this one keeps its leading segment, so it is restored.
			return cfnARNAfter(dr.ARN, "targetgroup/", "targetgroup/"), true
		}

	case "AWS::SNS::Topic":
		if attr == "TopicName" {
			// substrate overwrites a topic's physical ID with its ARN, so the name is the ARN's
			// last segment rather than the physical ID.
			if i := strings.LastIndex(dr.ARN, ":"); i >= 0 {
				return dr.ARN[i+1:], true
			}
			return "", true
		}

	case "AWS::SQS::Queue":
		switch attr {
		case "QueueName":
			return dr.PhysicalID, true
		case "QueueUrl":
			// The same URL `Ref` resolves to, from the same builder — see cfnRefValue.
			if dr.PhysicalID == "" {
				return "", true
			}
			return sqsQueueURL(cctx.region, cctx.accountID, dr.PhysicalID), true
		}

	case "AWS::SecretsManager::Secret":
		if attr == "Id" {
			// "The ARN of the secret." Id is not an opaque identifier here; it is the ARN.
			return dr.ARN, true
		}

	case "AWS::Glue::Database":
		if attr == "CatalogId" {
			// The only attribute the type documents, and it is the account the catalog belongs
			// to — which for a stack's own database is the deploying account.
			return cctx.accountID, true
		}

	case "AWS::ECS::Service", "AWS::ApiGateway::Stage":
		if attr == "Name" {
			// Both document Name, and for both substrate's physical ID *is* that name — the
			// service name and the stage name. (An ECS service's Ref is its ARN, which is why
			// the name is not reachable from there.)
			return dr.PhysicalID, true
		}

	case "AWS::Cognito::UserPool":
		if attr == "UserPoolId" {
			// substrate overwrites the physical ID with the generated pool ID, which is both
			// what Ref returns and what this attribute is.
			return dr.PhysicalID, true
		}

	case "AWS::ApiGatewayV2::Api":
		switch attr {
		case "ApiId":
			return dr.PhysicalID, true
		case "ApiEndpoint":
			// "https://abcdef.execute-api.us-west-2.amazonaws.com". Built rather than read
			// because CreateApi's response carries no endpoint in substrate; the form is AWS's,
			// for the same reason the S3 domain names are.
			if dr.PhysicalID == "" {
				return "", true
			}
			return "https://" + dr.PhysicalID + ".execute-api." + cctx.region + ".amazonaws.com", true
		}

	case "AWS::AppSync::GraphQLApi":
		switch attr {
		case "ApiId":
			// The API's own ID, which is what substrate addresses it by — a data source
			// naming its API by !GetAtt Api.ApiId is AWS's own template shape, and Ref is
			// the ARN, so this attribute is the only way to reach the ID.
			return dr.PhysicalID, true
		case "GraphQLEndpointArn":
			// The *endpoint's* ARN, not the API's. substrate records no endpoint, and the
			// generic ARN rule would answer with the API ARN — a different resource.
			return "", true
		}

	case "AWS::ElastiCache::CacheCluster":
		// AWS documents "RedisEndpoint.Address" and "RedisEndpoint.Port"; the ElastiCache deploy
		// helper records them under "RedisEndPoint.*", with a capital P, because that is how the
		// ElastiCache API spells the member. The documented spelling is translated here rather
		// than either side being renamed, so both a template and the API keep working.
		switch attr {
		case "RedisEndpoint.Address", "RedisEndpoint.Port":
			return cfnGetAttMetadata(dr, strings.Replace(attr, "RedisEndpoint", "RedisEndPoint", 1)), true
		}

	case "AWS::DynamoDB::Table":
		if attr == "StreamArn" {
			// The *stream's* ARN, not the table's — "You must specify the StreamSpecification
			// property to use this attribute." substrate records no stream, and the generic
			// ARN rule would otherwise hand back the table ARN, which is a different resource
			// wearing the right shape.
			return cfnGetAttMetadata(dr, "StreamArn"), true
		}
	}
	return "", false
}

// cfnGetAttNamesAnARN reports whether attr's name says its value is an ARN.
//
// A spelling check, not a guess about the type: CloudFormation names these attributes
// consistently — Arn, LoadBalancerArn, ListenerArn, RuleArn, TopicArn, TaskDefinitionArn,
// ServiceArn, KeyArn, ExecuteApiArn, and FSx's ResourceARN. The plural "…Arns" is deliberately
// not matched: it is a list, which Fn::GetAtt cannot return as a string here.
//
// A dotted attribute is not matched either. A nested path names a *member's* ARN rather than the
// resource's — an RDS instance's "MasterUserSecret.SecretArn" is the secret's — so the rule would
// hand back the wrong resource's ARN with the right shape, which is the failure it exists to stop.
func cfnGetAttNamesAnARN(attr string) bool {
	if strings.Contains(attr, ".") {
		return false
	}
	return strings.HasSuffix(attr, "Arn") || strings.HasSuffix(attr, "ARN")
}

// cfnGetAttARN returns the resource's own ARN, or empty when the deploy did not yield one.
//
// The physical ID is used only when it *is* an ARN, which is true for the handful of types whose
// creating operation returns nothing else to address the resource by — an SNS topic, a CloudTrail
// trail, a KMS key, an ECS task definition. That is a test on the string, not an assumption about
// the type, so it cannot go stale as helpers change. Anything else resolves to empty: returning a
// bare name where an ARN belongs is what #827 is about.
func cfnGetAttARN(dr DeployedResource) string {
	if dr.ARN != "" {
		return dr.ARN
	}
	if strings.HasPrefix(dr.PhysicalID, "arn:") {
		return dr.PhysicalID
	}
	return ""
}

// cfnARNAfter returns the part of arn following marker, with prefix put back in front of it.
//
// It exists for the two ELBv2 "full name" attributes, which are both a slice of the resource's own
// ARN. An ARN that does not contain the marker yields empty rather than the whole ARN, so a
// missing ARN cannot masquerade as a full name.
func cfnARNAfter(arn, marker, prefix string) string {
	i := strings.Index(arn, marker)
	if arn == "" || i < 0 {
		return ""
	}
	return prefix + arn[i+len(marker):]
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
