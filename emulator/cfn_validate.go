package emulator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// cfn_validate.go is CloudFormation's template validation (#1605): the checks CreateStack,
// UpdateStack, CreateChangeSet and ValidateTemplate make before anything deploys, strict mode,
// the per-resource property check made at provisioning, and ValidateTemplate itself.
//
// Three classes of refusal, which differ in where their wording comes from:
//
//   - **CloudFormation's own template checks** — an unrecognized resource type, a resource or
//     parameter missing its Type, an unrecognized parameter type, no Resources at all, and the
//     unresolved and circular references of cfn_dependency_order.go. AWS makes each at the
//     request, and each message is AWS's, with its source beside the literal.
//   - **Strict mode** — a resource type CloudFormation recognizes but substrate does not
//     emulate. AWS has no such refusal; the message is substrate's own and names the opt-out.
//   - **Property validation** — a top-level property the resource's type does not declare.
//     AWS makes it at provisioning, so the resource fails CREATE_FAILED (UPDATE_FAILED on an
//     update) and the stack rolls back, rather than the request being refused.

var (
	// ErrCFNTemplateFormat is returned when a template fails one of CloudFormation's own
	// template checks that is not about references: an unrecognized resource type, a resource
	// or parameter with no Type, an unrecognized parameter type, or no Resources (#1605). It is
	// a refusal of the request, made before any resource deploys. The wire maps it to
	// ValidationError at 400 and answers the message verbatim; each message is AWS's, cited
	// where it is built.
	ErrCFNTemplateFormat = errors.New("template format error")

	// ErrCFNUnemulatedType is returned in strict mode — the default — when a template declares
	// a resource type CloudFormation recognizes but substrate does not emulate, or a custom
	// resource (#1605). Deploying it would mean a stub that reports CREATE_COMPLETE for a
	// resource nothing created. The wire maps it to ValidationError at 400; the code is
	// substrate's choice and the message is substrate's own, because AWS has no such refusal.
	// Opt out with cloudformation.strict: false, [WithLenientCloudFormation] or
	// [WithDeployerLenient].
	ErrCFNUnemulatedType = errors.New("resource type not emulated")
)

// cfnIAMCapabilityNames maps each resource type that needs a capability acknowledgement to the
// property that gives it a custom name. The types are the list in CreateStack's Capabilities
// parameter: "The following IAM resources require you to specify either the CAPABILITY_IAM or
// CAPABILITY_NAMED_IAM capability", and "If you have IAM resources with custom names, you must
// specify CAPABILITY_NAMED_IAM". An empty name is a type that takes no custom name.
var cfnIAMCapabilityNames = map[string]string{
	"AWS::IAM::AccessKey":           "",
	"AWS::IAM::Group":               "GroupName",
	"AWS::IAM::InstanceProfile":     "InstanceProfileName",
	"AWS::IAM::ManagedPolicy":       "ManagedPolicyName",
	"AWS::IAM::Policy":              "",
	"AWS::IAM::Role":                "RoleName",
	"AWS::IAM::User":                "UserName",
	"AWS::IAM::UserToGroupAddition": "",
}

// cfnAWSSpecificParameterTypes are the AWS-specific parameter types the User Guide's
// "CloudFormation-supplied parameter types" page lists. Each may also appear as List<T> and
// inside AWS::SSM::Parameter::Value<…>.
var cfnAWSSpecificParameterTypes = map[string]bool{
	"AWS::EC2::AvailabilityZone::Name":   true,
	"AWS::EC2::Image::Id":                true,
	"AWS::EC2::Instance::Id":             true,
	"AWS::EC2::KeyPair::KeyName":         true,
	"AWS::EC2::SecurityGroup::GroupName": true,
	"AWS::EC2::SecurityGroup::Id":        true,
	"AWS::EC2::Subnet::Id":               true,
	"AWS::EC2::Volume::Id":               true,
	"AWS::EC2::VPC::Id":                  true,
	"AWS::Route53::HostedZone::Id":       true,
}

// cfnParameterTypeRecognized reports whether t is a parameter type CloudFormation accepts: String,
// Number, List<Number>, CommaDelimitedList, an AWS-specific type or a list of one, and the
// Systems Manager types AWS::SSM::Parameter::Name and AWS::SSM::Parameter::Value<…> over String,
// List<String>, CommaDelimitedList or an AWS-specific type or list of one — the types the
// Parameters section's Type property documents.
func cfnParameterTypeRecognized(t string) bool {
	switch t {
	// List<String> is not in the page's list, but templates use it and substrate has always
	// accepted it as a list parameter (cfnListParameterType); it is accepted rather than refused
	// on an absence of evidence, because a refusal AWS does not make is worse than a gap.
	case "String", "Number", "List<Number>", "List<String>", "CommaDelimitedList", "AWS::SSM::Parameter::Name":
		return true
	}
	if cfnAWSSpecificParameterTypes[t] {
		return true
	}
	if inner, ok := cfnUnwrapType(t, "List<"); ok {
		return cfnAWSSpecificParameterTypes[inner]
	}
	if inner, ok := cfnUnwrapType(t, "AWS::SSM::Parameter::Value<"); ok {
		switch inner {
		case "String", "List<String>", "CommaDelimitedList":
			return true
		}
		if cfnAWSSpecificParameterTypes[inner] {
			return true
		}
		if listed, ok := cfnUnwrapType(inner, "List<"); ok {
			return cfnAWSSpecificParameterTypes[listed]
		}
	}
	return false
}

// cfnUnwrapType returns what t holds between prefix and a trailing ">".
func cfnUnwrapType(t, prefix string) (string, bool) {
	if !strings.HasPrefix(t, prefix) || !strings.HasSuffix(t, ">") {
		return "", false
	}
	return t[len(prefix) : len(t)-1], true
}

// cfnDeclaredTransforms returns the macros a template's Transform section names, in the order
// it names them. A macro is a name, or the object form `{Name: AWS::Include, Parameters: …}`,
// on its own or as a member of a list.
func cfnDeclaredTransforms(tmpl *cfnTemplate) []string {
	var out []string
	add := func(v interface{}) {
		switch t := v.(type) {
		case string:
			if t != "" {
				out = append(out, t)
			}
		case map[string]interface{}:
			if name, ok := t["Name"].(string); ok && name != "" {
				out = append(out, name)
			}
		}
	}
	if list, ok := tmpl.Transform.([]interface{}); ok {
		for _, v := range list {
			add(v)
		}
		return out
	}
	add(tmpl.Transform)
	return out
}

// cfnValidateTemplateFormat makes CloudFormation's template checks that are not about
// references, in the order a reader of the template meets them: Parameters, then Resources.
func cfnValidateTemplateFormat(tmpl *cfnTemplate) error {
	for _, name := range cfnSortedKeys(tmpl.Parameters) {
		p := tmpl.Parameters[name]
		if p.Type == "" {
			// "An error occurred (ValidationError) when calling the ValidateTemplate operation:
			// Template format error: Every Parameters object must contain a Type member." —
			// the CLI's report in Stack Overflow question 55412786, and the CreateChangeSet
			// failure checkov issue #2965 quotes.
			return cfnErrf(ErrCFNTemplateFormat, "Template format error: Every Parameters object must contain a Type member.")
		}
		if !cfnParameterTypeRecognized(p.Type) {
			// "Template format error: Unrecognized parameter type: SecureString" — the
			// ValidationError CreateStack answers in aws-cdk issue #6786 and Stack Overflow
			// question 56891180.
			return cfnErrf(ErrCFNTemplateFormat, "Template format error: Unrecognized parameter type: %s", p.Type)
		}
	}
	if len(tmpl.Resources) == 0 {
		// "Template format error: At least one Resources member must be defined." — Stack
		// Overflow question 41667181 and the questions linked from it quote it.
		return cfnErrf(ErrCFNTemplateFormat, "Template format error: At least one Resources member must be defined.")
	}
	unrecognized := map[string]bool{}
	for _, name := range cfnSortedKeys(tmpl.Resources) {
		res := tmpl.Resources[name]
		if res.Type == "" {
			// "Template format error: Every Resources object must contain a Type member." —
			// Stack Overflow question 41667181.
			return cfnErrf(ErrCFNTemplateFormat, "Template format error: Every Resources object must contain a Type member.")
		}
		if !cfnRecognizedType(res.Type) {
			unrecognized[res.Type] = true
		}
	}
	// A template that declares a Transform may use types only the macro defines — the
	// AWS::Serverless types of the SAM transform are the common case — and substrate runs no
	// macro, so it cannot tell such a type from a typo. They are left to strict mode, which
	// refuses them as unemulated; lenient mode stubs them, as it always has.
	if len(unrecognized) > 0 && len(cfnDeclaredTransforms(tmpl)) == 0 {
		// "An error occurred (ValidationError) when calling the CreateChangeSet operation:
		// Template format error: Unrecognized resource types: [AWS::OSIS::Pipeline]" — aws-cli
		// issue #8032; the AWS re:Post Knowledge Center article on template validation errors
		// gives the same message for a type that is misspelled or unavailable in the Region. A
		// list is written "[AWS:EC2::VPC, AWS::RDS:DBInstance]" (Stack Overflow question
		// 63511383); substrate sorts it.
		return cfnErrf(ErrCFNTemplateFormat, "Template format error: Unrecognized resource types: [%s]",
			strings.Join(cfnSortedNames(unrecognized), ", "))
	}
	return nil
}

// cfnUnemulatedTypes returns, sorted, the resource types of the template's deployable resources
// that substrate does not emulate, each with the logical IDs declaring it. A resource under a
// false Condition is skipped: it does not deploy, so no stub would stand in for it.
func cfnUnemulatedTypes(tmpl *cfnTemplate, cctx *cfnContext) []string {
	byType := map[string][]string{}
	for id, res := range cfnActiveResources(tmpl, cctx) {
		if !cfnEmulatedType(res.Type) {
			byType[res.Type] = append(byType[res.Type], id)
		}
	}
	out := make([]string, 0, len(byType))
	for _, typ := range cfnSortedKeys(byType) {
		ids := byType[typ]
		sort.Strings(ids)
		out = append(out, typ+" ("+strings.Join(ids, ", ")+")")
	}
	return out
}

// cfnRefuseUnemulated is strict mode's refusal, or nil when every deployable resource's type is
// emulated. The message is substrate's own (see [ErrCFNUnemulatedType]).
func cfnRefuseUnemulated(tmpl *cfnTemplate, cctx *cfnContext) error {
	types := cfnUnemulatedTypes(tmpl, cctx)
	if len(types) == 0 {
		return nil
	}
	return cfnErrf(ErrCFNUnemulatedType,
		"Template declares resource types substrate does not emulate: [%s]. Substrate refuses them "+
			"rather than report a stub that creates nothing as CREATE_COMPLETE; to deploy them as stubs, "+
			"set cloudformation.strict: false (SUBSTRATE_CLOUDFORMATION_STRICT=false) or start the test "+
			"server with WithLenientCloudFormation()", strings.Join(types, "; "))
}

// cfnValidateTemplate makes every request-level check a template is held to before CreateStack,
// UpdateStack or CreateChangeSet deploys or records anything, and answers the deploy order.
//
// CloudFormation's own checks go first, so a template that AWS would refuse is refused with
// AWS's message whatever substrate emulates; strict mode's refusal, which is substrate's own,
// goes last.
func (d *StackDeployer) cfnValidateTemplate(tmpl *cfnTemplate, cctx *cfnContext) ([]string, error) {
	if err := cfnValidateTemplateFormat(tmpl); err != nil {
		return nil, err
	}
	order, err := cfnValidateDependencies(tmpl, cctx)
	if err != nil {
		return nil, err
	}
	if err := cfnValidateUpdateReplacePolicies(tmpl); err != nil {
		return nil, err
	}
	if err := d.cfnValidateOutputAttributes(tmpl, cctx); err != nil {
		return nil, err
	}
	if !d.lenient {
		if err := cfnRefuseUnemulated(tmpl, cctx); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// cfnExtraneousProperties returns the failure CloudFormation reports for a resource whose
// Properties carry a top-level key its type does not declare, or "" when there is none.
//
// The wording is observed behavior: "Properties validation failed for resource
// CognitoUserPoolClient with message: #: extraneous key [CallBackURLs] is not permitted" is the
// CREATE_FAILED reason quoted on AWS re:Post, and aws-cdk issues #25409 and #28984 quote the same
// form as the stack's ROLLBACK_COMPLETE and UPDATE_ROLLBACK_COMPLETE reason. Several failures are
// joined with a space, as the reports quoting several show; substrate sorts the keys.
//
// Only top-level keys are checked, against the vendored specification: a nested property type's
// members are not. A custom resource, AWS::CDK::Metadata and any type the specification does not
// list are not checked at all, since nothing declares their properties. A key whose value is
// AWS::NoValue is dropped before CloudFormation validates, so it is not extraneous.
func cfnExtraneousProperties(logicalID string, res cfnResource, cctx *cfnContext) string {
	if cfnCustomResourceType(res.Type) || len(res.Properties) == 0 {
		return ""
	}
	schema, ok := cfnSchemaLookup(res.Type)
	if !ok {
		return ""
	}
	var msgs []string
	for _, key := range cfnSortedKeys(res.Properties) {
		if _, declared := schema.property(key); declared {
			continue
		}
		if isCFNNoValue(res.Properties[key], cctx) {
			continue
		}
		msgs = append(msgs, "#: extraneous key ["+key+"] is not permitted")
	}
	if len(msgs) == 0 {
		return ""
	}
	return fmt.Sprintf("Properties validation failed for resource %s with message: %s",
		logicalID, strings.Join(msgs, " "))
}

// cfnTemplateCapabilities computes the capabilities a template must be created with, and the
// CapabilitiesReason ValidateTemplate reports: CAPABILITY_NAMED_IAM when any resource of a type in
// [cfnIAMCapabilityNames] gives its custom-name property, CAPABILITY_IAM when there are such
// resources and none is named, and nothing otherwise.
//
// Every resource is counted, whatever its Condition, because the check is on the template.
// The reason's wording is observed behavior: "The following resource(s) require capabilities:
// [AWS::IAM::ManagedPolicy]" is ValidateTemplate's CapabilitiesReason in Stack Overflow question
// 57516444, and "[AWS::IAM::Role]" in question 41246108. With several types substrate lists each
// once, sorted.
func cfnTemplateCapabilities(tmpl *cfnTemplate) ([]string, string) {
	types := map[string]bool{}
	named := false
	for _, res := range tmpl.Resources {
		nameProp, ok := cfnIAMCapabilityNames[res.Type]
		if !ok {
			continue
		}
		types[res.Type] = true
		if nameProp != "" {
			if _, set := res.Properties[nameProp]; set {
				named = true
			}
		}
	}
	if len(types) == 0 {
		return nil, ""
	}
	capability := "CAPABILITY_IAM"
	if named {
		capability = "CAPABILITY_NAMED_IAM"
	}
	return []string{capability},
		"The following resource(s) require capabilities: [" + strings.Join(cfnSortedNames(types), ", ") + "]"
}

// CFNTemplateParameter is one parameter a template declares, as ValidateTemplate reports it.
type CFNTemplateParameter struct {
	// ParameterKey is the parameter's name.
	ParameterKey string

	// DefaultValue is the parameter's declared default, or nil when it declares none.
	DefaultValue *string

	// NoEcho reports whether the parameter's value is masked.
	NoEcho bool

	// Description is the parameter's declared description.
	Description string
}

// CFNTemplateValidation is what ValidateTemplate reports about a valid template.
type CFNTemplateValidation struct {
	// Description is the template's own Description.
	Description string

	// Parameters lists the declared parameters, sorted by key. AWS publishes no order, and a
	// decoded template keeps none.
	Parameters []CFNTemplateParameter

	// Capabilities lists the capabilities the template must be created with: CAPABILITY_IAM or
	// CAPABILITY_NAMED_IAM, or none.
	Capabilities []string

	// CapabilitiesReason names the resource types that generated Capabilities.
	CapabilitiesReason string

	// DeclaredTransforms lists the macros the template's Transform section names.
	DeclaredTransforms []string
}

// ValidateTemplate checks a template the way CloudFormation's ValidateTemplate does — that it
// parses, that its Parameters and resource types are ones CloudFormation recognizes, and that
// every name it references is declared and can be ordered (#1605, #1548) — and reports its
// Description, Parameters, Capabilities and transforms.
//
// It answers what AWS would, so strict mode is not consulted: a type substrate does not emulate
// is a valid template. Nothing is deployed or recorded.
func (d *StackDeployer) ValidateTemplate(_ context.Context, body string) (*CFNTemplateValidation, error) {
	tmpl, err := d.parseCFNTemplate(body)
	if err != nil {
		return nil, cfnErrf(ErrCFNTemplateInvalid, "parse template: %w", err)
	}
	if err := cfnValidateTemplateFormat(tmpl); err != nil {
		return nil, err
	}
	if err := cfnValidateAllDependencies(tmpl); err != nil {
		return nil, err
	}
	if err := cfnValidateUpdateReplacePolicies(tmpl); err != nil {
		return nil, err
	}
	out := &CFNTemplateValidation{Description: tmpl.Description, DeclaredTransforms: cfnDeclaredTransforms(tmpl)}
	for _, name := range cfnSortedKeys(tmpl.Parameters) {
		p := tmpl.Parameters[name]
		out.Parameters = append(out.Parameters, CFNTemplateParameter{
			ParameterKey: name, DefaultValue: p.Default, NoEcho: p.noEcho(), Description: p.Description,
		})
	}
	out.Capabilities, out.CapabilitiesReason = cfnTemplateCapabilities(tmpl)
	return out, nil
}

// cfnSortedKeys returns a map's keys in ascending order.
func cfnSortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
