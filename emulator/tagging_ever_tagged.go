package emulator

import (
	"encoding/json"
	"fmt"
)

// GetResources reports what has been tagged rather than what is tagged, and substrate could not tell
// the two apart (#938).
//
// AWS's rule has two halves, and the second is what makes this more than a filter. API_GetResources
// states "GetResources does not return untagged resources", and on TagFilters: "If you don't specify
// a TagFilter, the response includes all resources that are currently tagged or ever had a tag.
// Resources that were previously tagged, but do not currently have tags, are shown with an empty tag
// set, like this: "Tags": []." So a resource that has never carried a tag is absent, one whose tags
// were all removed is present with an empty set — and a scanner reading the record's tags sees an
// empty set in both cases. The distinction is not in the tags, which is why it is new state.
//
// # Why a field on each record rather than a set of ARNs
//
// The alternative is a side-car of previously-tagged ARNs owned by the tagging plugin. #819 already
// records that a side-car tag store is the harder shape: there are two in the tree, and the second —
// Organizations' — is keyed by resource ID rather than by ARN, so the two cannot even share a
// lookup. A side-car here needs the resource's ARN at every write, and most tag writers do not have
// one: [mergeResourceTags] is handed a namespace and a state key, and several services' own tag
// operations resolve a name to a key without ever building the ARN. The record, by contrast, is
// already open in front of every one of them.
//
// The field is safe to add because #1013 removed the last record type that was marshaled onto the
// wire as a response shape. Every one of the thirty-three records this flag lives on is either
// projected through an explicit wire type or carries substrate's own AccountID or Region already, so
// a new persisted member cannot become a response member — which is the property that made
// json:"-" unnecessary here.
//
// # One member name for thirty-three records
//
// The member is spelled ever_tagged on every record, including those whose own members are
// PascalCase (KMSKey) or lowerCamel (ECSCluster). That is deliberate: three writers edit a record as
// raw JSON rather than as a concrete type — [mergeRecordStringMapTags], [mergeRecordTagListTags] and
// [ecsMergeRecordTags] — and they address the member through [taggingEverTaggedJSONMember]. A
// per-record spelling would make a generic writer impossible, and the member is never on the wire,
// so no reference publishes a name to match.
//
// # Creating with tags does not stamp it, and does not need to
//
// The flag is read only when a record's tag set is empty: a resource holding a tag is reported
// because it holds one. So a creation-with-tags path writes nothing here, and a new one added later
// is inert rather than a rule someone has to remember — the first write that empties the set stamps
// the flag, because that writer is looking at the set it is about to empty. It is stamped on an add
// as well, so the field means something on its own rather than only in combination with a removal
// that happened to pass through the same writer.
//
// Nothing clears it. AWS publishes no way for a resource to stop having been tagged.
//
// # The one path this does not catch
//
// A writer that replaces a record's whole tag set without reading the set it replaces — an Update
// that overwrites tags rather than merging them — empties the tags and leaves the flag unwritten, so
// the resource reverts to never-tagged. Every tag operation in the tree reads the existing set,
// because it has to in order to merge or to remove; the gap is a non-tag operation that happens to
// own the same member, and it is recorded here rather than guarded against, because a guard would
// have to live at exactly the sites that do not know they are tag writers.

// taggingEverTaggedJSONMember is the member name the previously-tagged flag is stored under on every
// record that carries it. It is a single constant because three writers edit the member as raw JSON.
const taggingEverTaggedJSONMember = "ever_tagged"

// taggingEverTagged reports whether a resource has carried a tag, given the flag as stored, the
// number of tags the record holds before this write, and the number this write adds.
//
// tagsBefore must be counted before the merge. A removal empties the set, so a writer that read the
// count afterwards would see zero for the one case the flag exists to record.
//
// Both are counts rather than the sets themselves because the writers do not agree on a shape: the
// tagging plugin merges a map[string]string, ACM and CloudFront decode a list of key/value pairs, and
// EFS keeps a slice. A count is the only thing all of them have.
func taggingEverTagged(stored bool, tagsBefore, tagsAdded int) bool {
	return stored || tagsBefore > 0 || tagsAdded > 0
}

// taggingStampRecordEverTagged sets the previously-tagged flag on a record being edited as raw JSON,
// for the writers that cannot assign a struct field because their namespace holds several shapes.
//
// tagsBefore is counted before the merge, per [taggingEverTagged]. A stored member that is not a
// boolean is an error rather than an overwrite: it means the record uses this name for something
// else, which is the same failure the surrounding helpers refuse for a tags member.
func taggingStampRecordEverTagged(record map[string]json.RawMessage, tagsBefore, tagsAdded int) error {
	stored := false
	if v, ok := record[taggingEverTaggedJSONMember]; ok && len(v) > 0 && string(v) != "null" {
		if err := json.Unmarshal(v, &stored); err != nil {
			return fmt.Errorf("unmarshal %s member: %w", taggingEverTaggedJSONMember, err)
		}
	}
	if !taggingEverTagged(stored, tagsBefore, tagsAdded) {
		return nil
	}
	record[taggingEverTaggedJSONMember] = json.RawMessage("true")
	return nil
}

// taggingStampRecordAnyEverTagged is [taggingStampRecordEverTagged] for a record decoded as
// map[string]interface{} rather than map[string]json.RawMessage. Two writers decode that way —
// ElastiCache's and Glue's — and converting them to raw messages is a change to what they preserve,
// which is not this issue's to make.
//
// A stored value that is not a boolean is an error rather than an overwrite, for the same reason as
// in the sibling: it means the record uses this name for something else.
func taggingStampRecordAnyEverTagged(record map[string]interface{}, tagsBefore, tagsAdded int) error {
	stored := false
	if v, ok := record[taggingEverTaggedJSONMember]; ok && v != nil {
		b, isBool := v.(bool)
		if !isBool {
			return fmt.Errorf("record stores a non-boolean %s member", taggingEverTaggedJSONMember)
		}
		stored = b
	}
	if !taggingEverTagged(stored, tagsBefore, tagsAdded) {
		return nil
	}
	record[taggingEverTaggedJSONMember] = true
	return nil
}

// taggingResourceReported reports whether GetResources includes a scanned resource.
//
// The disjunction is the rule, not a shortcut for it: a resource holding a tag is reported whatever
// the flag says, which is what lets a creation-with-tags path leave the flag alone. See the file
// preamble.
func taggingResourceReported(rm resourceTagMapping) bool {
	return len(rm.Tags) > 0 || rm.everTagged
}
