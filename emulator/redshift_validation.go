package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Redshift request validation and the shared describe pager (#1195, #1197).
//
// Every refusal here takes its code from a Redshift page: the operation's own, or the Common Errors
// page it links to, never a sibling operation's (#671). MissingParameter, InvalidParameterValue and
// InvalidParameterCombination are all on Redshift's Common Errors page at HTTP 400.

// redshiftMissingParameter is the refusal for a member the operation's page marks `Required: Yes`.
//
// The code is Redshift's Common Errors `MissingParameter`/400, "A required parameter for the specified
// action isn't included in the request." The message names the member, so a caller can tell which of
// several required members it omitted.
func redshiftMissingParameter(member string) *AWSError {
	return &AWSError{
		Code:       "MissingParameter",
		Message:    "The request must contain the parameter " + member + ".",
		HTTPStatus: http.StatusBadRequest,
	}
}

// redshiftInvalidParameterValue is Common Errors' `InvalidParameterValue`/400, "A value that you
// provided for a parameter isn't valid", for a member outside its published constraints.
func redshiftInvalidParameterValue(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterValue", Message: message, HTTPStatus: http.StatusBadRequest}
}

// redshiftNodeTypes is API_CreateCluster's published `Valid Values` for NodeType.
var redshiftNodeTypes = []string{
	"dc2.large", "dc2.8xlarge", "rg.xlarge", "rg.4xlarge",
	"ra3.large", "ra3.xlplus", "ra3.4xlarge", "ra3.16xlarge",
}

// redshiftCheckCreateCluster refuses a CreateCluster request missing a `Required: Yes` member, or
// naming a value outside the constraints API_CreateCluster publishes for a member checked here.
//
// The three required members are checked in the order the page lists them. Of the constraints on
// them:
//
//   - NodeType must be one of the page's `Valid Values` ([redshiftNodeTypes]).
//   - MasterUsername must be 1 to 128 characters; a lowercase letter first; only lowercase letters,
//     digits, `_`, `+`, `.`, `@` and `-`; and not `PUBLIC`. The page's reserved-word list is **not**
//     enforced: it is a separate document of several hundred SQL keywords, and enforcing part of it
//     would answer a different set than AWS does.
//   - ClusterIdentifier's own constraints are not enforced here. It was the one member already
//     checked before #1197, and #1197's criterion covers members it newly checks.
func redshiftCheckCreateCluster(params map[string]string) *AWSError {
	for _, member := range []string{"ClusterIdentifier", "MasterUsername", "NodeType"} {
		if params[member] == "" {
			return redshiftMissingParameter(member)
		}
	}
	if nodeType := params["NodeType"]; !slices.Contains(redshiftNodeTypes, nodeType) {
		return redshiftInvalidParameterValue("Invalid node type: " + nodeType + ".")
	}
	return redshiftCheckMasterUsername(params["MasterUsername"])
}

// redshiftCheckMasterUsername applies API_CreateCluster's MasterUsername constraints, as
// [redshiftCheckCreateCluster] records them.
func redshiftCheckMasterUsername(user string) *AWSError {
	bad := func(why string) *AWSError {
		return redshiftInvalidParameterValue("MasterUsername " + why + ".")
	}
	if len(user) > 128 {
		return bad("must be 1 to 128 characters")
	}
	if strings.EqualFold(user, "public") {
		return bad("can't be PUBLIC")
	}
	if user[0] < 'a' || user[0] > 'z' {
		return bad("must begin with a lowercase letter")
	}
	for _, r := range user {
		lower := r >= 'a' && r <= 'z'
		digit := r >= '0' && r <= '9'
		if !lower && !digit && !strings.ContainsRune("_+.@-", r) {
			return bad("must contain only lowercase letters, numbers, underscore, plus sign, period, at symbol or hyphen")
		}
	}
	return nil
}

// redshiftCheckGroupName applies the constraints API_CreateClusterParameterGroup publishes for
// ParameterGroupName: 1 to 255 alphanumeric characters or hyphens, a letter first, and no trailing
// hyphen or two consecutive hyphens. Case is accepted either way, since the page stores the value
// lower-case.
func redshiftCheckGroupName(member, name string) *AWSError {
	bad := func(why string) *AWSError {
		return redshiftInvalidParameterValue(member + " " + why + ".")
	}
	if len(name) > 255 {
		return bad("must be 1 to 255 characters")
	}
	if !redshiftIsLetter(rune(name[0])) {
		return bad("must begin with a letter")
	}
	if strings.HasSuffix(name, "-") || strings.Contains(name, "--") {
		return bad("cannot end with a hyphen or contain two consecutive hyphens")
	}
	if !redshiftAlnumOrHyphen(name) {
		return bad("must contain only alphanumeric characters or hyphens")
	}
	return nil
}

// redshiftCheckSubnetGroupName applies the constraints API_CreateClusterSubnetGroup publishes for
// ClusterSubnetGroupName: "no more than 255 alphanumeric characters or hyphens" and "must not be
// Default". The page publishes no first-character or hyphen-placement rule for this name, so none is
// applied; it is a different rule from [redshiftCheckGroupName]'s, and the two are kept apart.
func redshiftCheckSubnetGroupName(name string) *AWSError {
	if len(name) > 255 || !redshiftAlnumOrHyphen(name) {
		return redshiftInvalidParameterValue("ClusterSubnetGroupName must contain no more than 255 alphanumeric characters or hyphens.")
	}
	if strings.EqualFold(name, "default") {
		return redshiftInvalidParameterValue("ClusterSubnetGroupName must not be Default.")
	}
	return nil
}

func redshiftIsLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func redshiftAlnumOrHyphen(s string) bool {
	for _, r := range s {
		if !redshiftIsLetter(r) && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

// redshiftIndexedParams returns the values of a Query list parameter, `<prefix>1`, `<prefix>2`, …, in
// index order. A gap does not end the list: the members are sorted by their numeric suffix, so
// `.1` and `.3` both count, which is what a caller who removed one entry would expect.
func redshiftIndexedParams(params map[string]string, prefix string) []string {
	type indexed struct {
		n     int
		value string
	}
	var found []indexed
	for k, v := range params {
		suffix, ok := strings.CutPrefix(k, prefix)
		if !ok || v == "" {
			continue
		}
		n, err := strconv.Atoi(suffix)
		if err != nil || n < 1 {
			continue
		}
		found = append(found, indexed{n, v})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].n < found[j].n })
	out := make([]string, 0, len(found))
	for _, f := range found {
		out = append(out, f.value)
	}
	return out
}

// redshiftPagination reads a describe's Marker and MaxRecords.
//
// All four Redshift describes publish the RDS and ElastiCache contract word for word: MaxRecords
// "Default: 100", "Constraints: minimum 20, maximum 100", and an opaque Marker. So they use the shared
// [parseQueryMarker] and [queryMaxRecords], and their `InvalidParameterValue`/400. None of the four
// pages publishes a code for an unusable Marker or an out-of-range MaxRecords; the code is Redshift's
// Common Errors one for a parameter value that isn't valid, which the pages link to.
func redshiftPagination(params map[string]string) (queryMarkerCursor, int, *AWSError) {
	cursor, awsErr := parseQueryMarker(params["Marker"])
	if awsErr != nil {
		return queryMarkerCursor{}, 0, awsErr
	}
	maxRecords, awsErr := queryMaxRecords(params["MaxRecords"])
	if awsErr != nil {
		return queryMarkerCursor{}, 0, awsErr
	}
	return cursor, maxRecords, nil
}

// redshiftPage collects one page of the records stored under prefix, decoded as R and rendered by
// render, resuming after cursor.
//
// The keys come from [StateManager.List], which returns them in lexicographic order (#865), because
// [queryMarkerPage]'s cursor is only well defined over a stable order. Until #1195 the describes read
// their name indexes, in creation order, and answered every record in one page.
//
// A store error or an undecodable record is returned rather than skipped. A skipped record would
// read as absent, and with a filter that is a not-found fault for a record that exists. render may
// also report an error of its own through readErr, which is how the snapshot describe returns a fault
// on the cluster read its ClusterExists filter makes.
func redshiftPage[T, R any](p *RedshiftPlugin, prefix string, cursor queryMarkerCursor, maxRecords int,
	readErr *error, render func(id string, rec R) (T, bool),
) ([]T, string, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, redshiftNamespace, prefix)
	if err != nil {
		return nil, "", fmt.Errorf("list %s: %w", prefix, err)
	}
	page, next := queryMarkerPage(keys, prefix, cursor, maxRecords, func(key, id string) (T, bool) {
		var zero T
		if *readErr != nil {
			return zero, false
		}
		data, getErr := p.state.Get(goCtx, redshiftNamespace, key)
		if getErr != nil {
			*readErr = fmt.Errorf("get %s: %w", key, getErr)
			return zero, false
		}
		if data == nil {
			return zero, false
		}
		var rec R
		if decodeErr := json.Unmarshal(data, &rec); decodeErr != nil {
			*readErr = fmt.Errorf("decode %s: %w", key, decodeErr)
			return zero, false
		}
		return render(id, rec)
	})
	if *readErr != nil {
		return nil, "", *readErr
	}
	return page, next, nil
}

// redshiftSnapshotFilter is DescribeClusterSnapshots' filter members, decoded and checked.
//
// Each member API_DescribeClusterSnapshots publishes is applied, except three:
//
//   - TagKeys and TagValues, for the reason [RedshiftPlugin.describeClusters] gives: substrate
//     records no snapshot tags, so a tag filter has nothing to match.
//   - SortingEntities, whose page publishes no description at all, only its type. The listing is in
//     identifier order, which is what the cursor requires.
//
// The applied ones:
//
//   - ClusterIdentifier and SnapshotIdentifier match exactly. A SnapshotIdentifier matching nothing
//     is the page's ClusterSnapshotNotFound; a ClusterIdentifier matching nothing is an empty
//     listing, since its snapshots may outlive it.
//   - SnapshotArn matches the snapshot's ARN, `arn:aws:redshift:{region}:{account}:snapshot:{cluster}/{snapshot}`.
//   - SnapshotType must be one of its `Valid Values`, `automated` or `manual`.
//   - StartTime and EndTime bound SnapshotCreateTime inclusively ("at or after", "at or before"), and
//     must be ISO 8601.
//   - OwnerAccount narrows to snapshots owned by that account. Substrate stores only the caller's
//     own, so any other account matches none.
//   - ClusterExists follows the page's four rules. `true` requires ClusterIdentifier, and is
//     MissingParameter without it. Each snapshot is then kept only if its cluster's existence equals
//     the value, which yields all four of the page's cases.
type redshiftSnapshotFilter struct {
	clusterID, snapshotID, snapshotArn, snapshotType, ownerAccount string
	start, end                                                     *time.Time
	clusterExists                                                  *bool
}

func parseRedshiftSnapshotFilter(params map[string]string) (redshiftSnapshotFilter, *AWSError) {
	f := redshiftSnapshotFilter{
		clusterID:    params["ClusterIdentifier"],
		snapshotID:   params["SnapshotIdentifier"],
		snapshotArn:  params["SnapshotArn"],
		snapshotType: params["SnapshotType"],
		ownerAccount: params["OwnerAccount"],
	}
	if f.snapshotType != "" && f.snapshotType != "automated" && f.snapshotType != "manual" {
		return f, redshiftInvalidParameterValue("SnapshotType must be automated or manual.")
	}
	for member, dst := range map[string]**time.Time{"StartTime": &f.start, "EndTime": &f.end} {
		raw := params[member]
		if raw == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return f, redshiftInvalidParameterValue(member + " must be an ISO 8601 time, such as 2012-07-16T18:00:00Z.")
		}
		*dst = &t
	}
	if raw := params["ClusterExists"]; raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return f, redshiftInvalidParameterValue("ClusterExists must be true or false.")
		}
		if v && f.clusterID == "" {
			return f, redshiftMissingParameter("ClusterIdentifier")
		}
		f.clusterExists = &v
	}
	return f, nil
}

// matches reports whether snap passes every filter but ClusterExists, which needs a state read and
// is applied by the caller.
func (f redshiftSnapshotFilter) matches(reqCtx *RequestContext, snap RedshiftSnapshot) bool {
	switch {
	case f.clusterID != "" && snap.ClusterIdentifier != f.clusterID,
		f.snapshotID != "" && snap.SnapshotIdentifier != f.snapshotID,
		f.snapshotType != "" && snap.SnapshotType != f.snapshotType,
		f.ownerAccount != "" && f.ownerAccount != reqCtx.AccountID,
		f.start != nil && snap.SnapshotCreateTime.Before(*f.start),
		f.end != nil && snap.SnapshotCreateTime.After(*f.end):
		return false
	}
	if f.snapshotArn != "" && f.snapshotArn != redshiftSnapshotARN(reqCtx.Region, reqCtx.AccountID, snap.ClusterIdentifier, snap.SnapshotIdentifier) {
		return false
	}
	return true
}

// redshiftSnapshotARN is a cluster snapshot's ARN, in the `snapshot` resource-type form the Service
// Authorization Reference publishes for Amazon Redshift:
// `arn:aws:redshift:{region}:{account}:snapshot:{cluster}/{snapshot}`.
func redshiftSnapshotARN(region, account, cluster, snapshot string) string {
	return "arn:aws:redshift:" + region + ":" + account + ":snapshot:" + cluster + "/" + snapshot
}
