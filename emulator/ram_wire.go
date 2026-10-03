package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// RAMResourceShare (ram_types.go) is the persisted state record, and CreateResourceShare,
// UpdateResourceShare and GetResourceShares handed it straight to the caller under
// `resourceShare` and `resourceShares`. Four of its members are not in API_ResourceShare: accountID
// and region are substrate's own and carry no omitempty, so every one of those responses answered
// them (#756); principals and resourceArns are the share's associations, which RAM publishes
// through GetResourceShareAssociations, ListPrincipals and ListResources rather than on the share.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. The
// associations are what ListPrincipals and ListResources answer from, so they have to stay in the
// record. For the same reason CreationTime and LastUpdatedTime stay time.Time in the record and are
// converted on projection, so a run recorded before this change replays identically.
//
// # Why RAM's dates are EpochSeconds
//
// RAM speaks restJson1, where a Timestamp in a body is epoch seconds unless the model says
// otherwise. API_ResourceShare types creationTime and lastUpdatedTime as Timestamp, and
// API_CreateResourceShare's response syntax renders both as `number`. A Go time.Time marshals to
// RFC3339, which is what all three sites answered, and a restJson1 deserializer that expects a
// number fails the whole response in a typed SDK.

// ramResourceShareOut is API_ResourceShare, the element CreateResourceShare and UpdateResourceShare
// answer under `resourceShare` and GetResourceShares answers in `resourceShares`.
//
// Eight of the shape's eleven members. featureSet, resourceShareConfiguration and statusMessage are
// not modeled and are therefore absent rather than present and empty; every member of the shape is
// Required: No. tags is omitted when empty, as the record already omits it.
type ramResourceShareOut struct {
	AllowExternalPrincipals bool         `json:"allowExternalPrincipals"`
	CreationTime            EpochSeconds `json:"creationTime"`
	LastUpdatedTime         EpochSeconds `json:"lastUpdatedTime"`
	Name                    string       `json:"name"`
	OwningAccountID         string       `json:"owningAccountId"`
	ResourceShareArn        string       `json:"resourceShareArn"`
	Status                  string       `json:"status"`
	Tags                    []RAMTag     `json:"tags,omitempty"`
}

// ramResourceShareToWire projects a persisted resource share onto the published shape.
func ramResourceShareToWire(share RAMResourceShare) ramResourceShareOut {
	return ramResourceShareOut{
		AllowExternalPrincipals: share.AllowExternalPrincipals,
		CreationTime:            EpochSeconds(share.CreationTime),
		LastUpdatedTime:         EpochSeconds(share.LastUpdatedTime),
		Name:                    share.Name,
		OwningAccountID:         share.OwningAccountID,
		ResourceShareArn:        share.ResourceShareArn,
		Status:                  share.Status,
		Tags:                    share.Tags,
	}
}
