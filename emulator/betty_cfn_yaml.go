package emulator

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// cfnShortFormTags maps each CloudFormation YAML short-form tag to the long-form
// key it expands to.
//
// A YAML parser has no notion of these tags: it drops the tag and keeps the node
// value, so `!Sub 'x-${P}'` decodes to the bare string "x-${P}" and becomes
// indistinguishable from a literal. Rewriting the tagged node into its long form
// before decoding is what lets the two syntaxes resolve identically, which is
// what CloudFormation guarantees.
//
// Every entry wraps the node's value unchanged. Two are irregular:
// Ref and Condition take no "Fn::" prefix, and Fn::GetAtt additionally splits a
// scalar on its first period (see cfnExpandGetAtt).
var cfnShortFormTags = map[string]string{
	"!Ref":         "Ref",
	"!Condition":   "Condition",
	"!GetAtt":      "Fn::GetAtt",
	"!Sub":         "Fn::Sub",
	"!Join":        "Fn::Join",
	"!Select":      "Fn::Select",
	"!Split":       "Fn::Split",
	"!Base64":      "Fn::Base64",
	"!If":          "Fn::If",
	"!Equals":      "Fn::Equals",
	"!Not":         "Fn::Not",
	"!And":         "Fn::And",
	"!Or":          "Fn::Or",
	"!FindInMap":   "Fn::FindInMap",
	"!ImportValue": "Fn::ImportValue",
	"!Cidr":        "Fn::Cidr",
	"!GetAZs":      "Fn::GetAZs",
	"!Transform":   "Fn::Transform",
}

// cfnExpandYAMLTags rewrites every CloudFormation short-form intrinsic tag in n
// into its long form in place, so that decoding n yields the same structure a
// long-form template would.
//
// The walk is depth-first, children before parents, because the tags nest:
// `!Not [!Equals [!Ref VpcId, ""]]` has to expand from the inside out or the
// inner tags are carried into the parent's replacement node and lost.
//
// A tag the map does not recognize is left exactly as it is, and reported through
// unknown so the caller can warn. Dropping it silently is the defect this
// function exists to fix, and refusing the template outright would reject
// templates real CloudFormation accepts (a macro or a transform may introduce
// tags substrate has never heard of). Leaving the node's value in place is what
// a YAML parser does anyway, so an unrecognized tag is no worse off than before.
func cfnExpandYAMLTags(n *yaml.Node, unknown *[]string) {
	// Children first. A mapping's keys are skipped: a key is a scalar in every
	// CloudFormation template, and replacing one with a mapping would make it
	// unusable as a map key.
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			cfnExpandYAMLTags(n.Content[i+1], unknown)
		}
	} else {
		for _, child := range n.Content {
			cfnExpandYAMLTags(child, unknown)
		}
	}

	if !cfnIsCustomTag(n.Tag) {
		return
	}
	longForm, ok := cfnShortFormTags[n.Tag]
	if !ok {
		*unknown = append(*unknown, n.Tag)
		return
	}

	// The original node becomes the value of the long-form mapping. Its tag is
	// replaced rather than cleared: clearing it would let the decoder resolve the
	// scalar's type afresh, and CloudFormation values are strings — `!Sub 12345`
	// would decode to an int and `!Ref 2026-08-02` to a time.Time, neither of which
	// resolveValue's string handling expects. "!!str" is correct for a sequence or
	// mapping too, because the decoder is kind-driven and ignores the tag on a
	// composite node.
	value := new(yaml.Node)
	*value = *n
	value.Tag = "!!str"
	if longForm == "Fn::GetAtt" {
		value = cfnExpandGetAtt(value)
	}

	*n = yaml.Node{
		Kind:   yaml.MappingNode,
		Tag:    "!!map",
		Line:   value.Line,
		Column: value.Column,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: longForm},
			value,
		},
	}
}

// cfnIsCustomTag reports whether tag is an application-specific YAML tag such as
// "!Sub", as opposed to a native type tag such as "!!str" or an absent tag.
func cfnIsCustomTag(tag string) bool {
	return strings.HasPrefix(tag, "!") && !strings.HasPrefix(tag, "!!")
}

// cfnExpandGetAtt converts the scalar form of !GetAtt into the two-element
// sequence Fn::GetAtt requires.
//
// !GetAtt is the one irregular short form: it accepts a dotted string where the
// long form takes a list. The split is on the *first* period only, because an
// attribute name may itself contain periods — the AWS reference's own example
// maps `!GetAtt myELB.SourceSecurityGroup.OwnerAlias` to
// `["myELB", "SourceSecurityGroup.OwnerAlias"]`.
//
// A !GetAtt already written as a sequence, and a scalar with no period, are
// returned unchanged: the first is already in long form, and the second is
// malformed either way and is left for the resolver to reject.
func cfnExpandGetAtt(value *yaml.Node) *yaml.Node {
	if value.Kind != yaml.ScalarNode {
		return value
	}
	logicalID, attribute, found := strings.Cut(value.Value, ".")
	if !found {
		return value
	}
	return &yaml.Node{
		Kind:   yaml.SequenceNode,
		Tag:    "!!seq",
		Line:   value.Line,
		Column: value.Column,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: logicalID, Line: value.Line, Column: value.Column},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: attribute, Line: value.Line, Column: value.Column},
		},
	}
}

// cfnUnmarshalYAMLTemplate decodes a YAML CloudFormation template into tmpl,
// expanding short-form intrinsic tags first. Any tag it does not recognize is
// returned so the caller can warn about it.
func cfnUnmarshalYAMLTemplate(cfn string, tmpl *cfnTemplate) ([]string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(cfn), &doc); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	var unknown []string
	cfnExpandYAMLTags(&doc, &unknown)
	// An empty document decodes to a zero Node, which Decode rejects; treat it as
	// an empty template and let the caller fall through to its JSON error path.
	if doc.Kind == 0 {
		return unknown, nil
	}
	if err := doc.Decode(tmpl); err != nil {
		return unknown, fmt.Errorf("decode YAML template: %w", err)
	}
	return unknown, nil
}
