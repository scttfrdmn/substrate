package emulator

// Helpers shared by the CloudFormation resource helpers of more than one service: property
// readers and DeployedResource metadata.

import (
	"strconv"
	"strings"
)

// cfnIntProp resolves an integer property, which a template may write as a number or as a string
// (a Ref to a Number parameter resolves to one). The second result is false when the property is
// absent or does not resolve to an integer.
func cfnIntProp(props map[string]interface{}, key string, cctx *cfnContext) (int, bool) {
	v, ok := props[key]
	if !ok || v == nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(resolveValue(v, cctx)))
	if err != nil {
		return 0, false
	}
	return n, true
}

// cfnTagMap resolves a Tags property — a list of {Key, Value} — to a map. A keyless entry is
// skipped, as [cfnELBClassicTagParams] skips one.
func cfnTagMap(v interface{}, cctx *cfnContext) map[string]string {
	list, _ := v.([]interface{})
	out := make(map[string]string, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		key := resolveValue(m["Key"], cctx)
		if key == "" {
			continue
		}
		out[key] = resolveValue(m["Value"], cctx)
	}
	return out
}

// resolveStringList resolves a CFN list-valued property to a string slice for a
// caller that indexes the result into an AWS query parameter (Member.1,
// Member.2, …).
//
// It delegates the resolution to resolveValueList and drops empty members. The
// numbering is what makes that necessary: an empty member would still occupy an
// index, so a query API would receive `SecurityGroupId.2=` and reject it. A
// caller that needs Fn::Split's documented empty elements preserved uses
// resolveValueList directly.
func resolveStringList(v interface{}, cctx *cfnContext) []string {
	resolved := resolveValueList(v, cctx)
	out := make([]string, 0, len(resolved))
	for _, s := range resolved {
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cfnSetMetadata records a GetAtt-resolvable attribute on a resource, allocating the
// map on first use and ignoring an empty value so an absent attribute stays absent
// rather than resolving to "".
func cfnSetMetadata(dr *DeployedResource, key, value string) {
	if value == "" {
		return
	}
	if dr.Metadata == nil {
		dr.Metadata = map[string]interface{}{}
	}
	dr.Metadata[key] = value
}

// cfnCopyMetadata returns a copy of m that an update can change without altering the record it
// was read from, which the rollback still compares against.
func cfnCopyMetadata(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

// cfnBoolValue reads a template boolean, which a JSON template spells true and a YAML or
// parameter-fed one may spell "true".
func cfnBoolValue(v interface{}) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	case string:
		parsed, err := strconv.ParseBool(b)
		return parsed, err == nil
	}
	return false, false
}
