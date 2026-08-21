package emulator

import (
	"strconv"
	"strings"
)

// ec2SnapshotMatchesFilters reports whether a snapshot satisfies every supplied
// DescribeSnapshots filter. Filters AND with each other and each one's values OR, which is
// AWS's documented rule for all of them.
//
// Until #685 this operation honored one of the fourteen names its reference documents —
// snapshot-id — and silently ignored the other thirteen, so "the snapshots of volume
// vol-x" answered with every snapshot in the region. That is the failure mode an ignored
// filter always has: a caller cannot tell an unfiltered answer from a filtered one.
func ec2SnapshotMatchesFilters(snap EC2Snapshot, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2SnapshotMatchesFilter(snap, name, values) {
			return false
		}
	}
	return true
}

// ec2SnapshotMatchesFilter evaluates a single DescribeSnapshots filter against a snapshot.
//
// Ten of AWS's fourteen names are answerable from [EC2Snapshot]. The four that are not —
// owner-alias, progress, storage-tier and transfer-type — are inert, per the rule
// [ec2FilterSpec] states: substrate renders none of those members either, so a filter over
// them has nothing to compare against. A name outside all fourteen never arrives, because
// [ec2SnapshotFilterSpec] refuses it before the scan (#687).
func ec2SnapshotMatchesFilter(snap EC2Snapshot, name string, values []string) bool {
	if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range snap.Tags {
			if t.Key == tagKey && containsStr(values, t.Value) {
				return true
			}
		}
		return false
	}

	switch name {
	case "description":
		return containsStr(values, snap.Description)
	case "encrypted":
		return containsStr(values, strconv.FormatBool(snap.Encrypted))
	case "owner-id":
		// The account that owns the snapshot, which is always the requesting account:
		// substrate is single-account, so this filter either matches everything or
		// nothing. It is still evaluated rather than inert, because a caller who names
		// another account is asking a question whose honest answer is "none".
		return containsStr(values, snap.AccountID)
	case "snapshot-id":
		return containsStr(values, snap.SnapshotID)
	case "start-time":
		// Compared as the string substrate renders in startTime. AWS documents this
		// filter with no matching semantics of its own, and its values are timestamps, so
		// a caller wanting a range needs the wildcards tracked in #697 rather than this.
		return containsStr(values, snap.StartTime)
	case "status":
		// AWS names the filter "status" and the response element "status" while the API
		// model calls the member State; EC2Snapshot follows the model.
		return containsStr(values, snap.State)
	case "tag-key":
		for _, t := range snap.Tags {
			if containsStr(values, t.Key) {
				return true
			}
		}
		return false
	case "volume-id":
		return containsStr(values, snap.VolumeID)
	case "volume-size":
		return containsStr(values, strconv.FormatInt(snap.VolumeSize, 10))
	default:
		// A documented filter substrate cannot evaluate is inert; see the doc comment.
		return true
	}
}

// ec2SnapshotAccountScope filters DescribeSnapshots by an account-valued parameter list —
// Owner.N or RestorableBy.N — both of which sit outside Filter.N and outside the ID list.
//
// The two collapse to one type because substrate is single-account, so they cannot disagree:
// every stored snapshot is owned by the requesting account, and its owner can always create
// a volume from it. "self" and the caller's own account ID therefore match everything, and
// any other value — another account ID, or the "amazon" alias Owner.N documents — matches
// nothing, because substrate models neither cross-account snapshots nor
// createVolumePermission. Matching nothing is the honest answer to "snapshots owned by
// amazon" in an emulator that has none; answering with the account's own snapshots would
// claim they are public.
type ec2SnapshotAccountScope struct {
	requested []string
	accountID string
}

// newEC2SnapshotOwners reads Owner.N, which AWS documents as "a combination of AWS account
// IDs, self, and amazon".
func newEC2SnapshotOwners(params map[string]string, accountID string) ec2SnapshotAccountScope {
	return ec2SnapshotAccountScope{
		requested: extractIndexedParams(params, "Owner"),
		accountID: accountID,
	}
}

// newEC2SnapshotRestorableBy reads RestorableBy.N, "the IDs of the AWS accounts that can
// create volumes from the snapshot".
func newEC2SnapshotRestorableBy(params map[string]string, accountID string) ec2SnapshotAccountScope {
	return ec2SnapshotAccountScope{
		requested: extractIndexedParams(params, "RestorableBy"),
		accountID: accountID,
	}
}

// match reports whether snap is in scope. A scope the caller did not send matches
// everything, which is how both parameters are documented to behave when absent.
func (s ec2SnapshotAccountScope) match(snap EC2Snapshot) bool {
	if len(s.requested) == 0 {
		return true
	}
	for _, want := range s.requested {
		if want == "self" && snap.AccountID == s.accountID {
			return true
		}
		if want == snap.AccountID {
			return true
		}
	}
	return false
}
