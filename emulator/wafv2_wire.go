package emulator

// The wire is a different thing from the state, and the two types below exist to keep them
// apart.
//
// WAFv2WebACL and WAFv2IPSet (wafv2_types.go) are persisted state records, and each was handed
// straight to the caller at the one site that answers it: getWebACL under `WebACL`, getIPSet under
// `IPSet`. Both records carry AccountID and Region, which no WAFv2 shape publishes (#756), and the
// web ACL also carries CreatedAt, which API_WebACL does not publish either. Both also carry Scope and
// LockToken. Neither is a member of API_WebACL or API_IPSet: Scope is a request parameter, and
// LockToken is published once, at the top level of GetWebACLResponse and GetIPSetResponse, which the
// handlers already answer beside the object. So each response answered its lock token twice, once in
// an unpublished position.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. Projecting
// leaves the stored bytes unchanged, so a run recorded before this replays identically.

// wafv2WebACLOut is the WebACL element of GetWebACL's response.
//
// Seven of API_WebACL's members, each with the omitempty the record already gives it; the rest —
// Capacity, LabelNamespace, the Firewall Manager members and the configuration objects — are not
// modeled and are absent rather than present and empty (#1013's rule).
type wafv2WebACLOut struct {
	ARN              string                 `json:"ARN"`
	DefaultAction    map[string]interface{} `json:"DefaultAction,omitempty"`
	Description      string                 `json:"Description,omitempty"`
	ID               string                 `json:"Id"`
	Name             string                 `json:"Name"`
	Rules            []WAFv2Rule            `json:"Rules,omitempty"`
	VisibilityConfig map[string]interface{} `json:"VisibilityConfig,omitempty"`
}

// wafv2WebACLToWire projects a persisted web ACL onto the published shape.
func wafv2WebACLToWire(acl WAFv2WebACL) wafv2WebACLOut {
	return wafv2WebACLOut{
		ARN:              acl.ARN,
		DefaultAction:    acl.DefaultAction,
		Description:      acl.Description,
		ID:               acl.ID,
		Name:             acl.Name,
		Rules:            acl.Rules,
		VisibilityConfig: acl.VisibilityConfig,
	}
}

// wafv2IPSetOut is the IPSet element of GetIPSet's response: all six of API_IPSet's members.
type wafv2IPSetOut struct {
	Addresses        []string `json:"Addresses"`
	ARN              string   `json:"ARN"`
	Description      string   `json:"Description,omitempty"`
	ID               string   `json:"Id"`
	IPAddressVersion string   `json:"IPAddressVersion"`
	Name             string   `json:"Name"`
}

// wafv2IPSetToWire projects a persisted IP set onto the published shape.
func wafv2IPSetToWire(ipset WAFv2IPSet) wafv2IPSetOut {
	return wafv2IPSetOut{
		Addresses:        ipset.Addresses,
		ARN:              ipset.ARN,
		Description:      ipset.Description,
		ID:               ipset.ID,
		IPAddressVersion: ipset.IPAddressVersion,
		Name:             ipset.Name,
	}
}
