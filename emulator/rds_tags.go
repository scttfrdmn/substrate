package emulator

// RDS tagging: AddTagsToResource, RemoveTagsFromResource, ListTagsForResource, and the ARN
// resolver all four of them and the Resource Groups Tagging API key through.
//
// Split out of rds_plugin.go because the resolver is the load-bearing part and it had drifted
// from what RDS itself mints. rdsResolveARN accepted `db:` and `snapshot:` only, while
// rdsDBClusterARN and rdsSubnetGroupARN (rds_plugin.go) hand callers `cluster:` and `subgrp:`
// ARNs — so substrate emitted two ARNs its own tag operations then refused as
// "unsupported RDS ARN resource type". #765's rule is that a value substrate reports must be
// readable back through the API that reports it; two of the five ARN shapes RDS builds failed
// that (part of #835).
//
// The resource-type segments are AWS's, from "Constructing an ARN for Amazon RDS":
//
//	DB instance      arn:aws:rds:{region}:{account}:db:{name}
//	DB cluster       arn:aws:rds:{region}:{account}:cluster:{name}
//	Manual snapshot  arn:aws:rds:{region}:{account}:snapshot:{name}
//	DB subnet group  arn:aws:rds:{region}:{account}:subgrp:{name}
//
// An *automated* snapshot is `snapshot:rds:{name}`, which needs no arm of its own: the extra
// segment belongs to the identifier — an automated snapshot really is named
// "rds:mydb-2019-07-22-07-23" — and the resolver's SplitN keeps the remainder intact, so it
// resolves to that identifier rather than to a truncated one.

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// RDS state-key prefixes the tagging path resolves to. Each names one record shape in the rds
// namespace; the namespace also holds prefixes no ARN addresses (a parameter group, a container
// handle, the index keys), which is why the merge path guards on this set rather than assuming
// every key in the namespace carries tags.
const (
	rdsDBInstanceKeyPrefix    = "dbinstance:"
	rdsDBClusterKeyPrefix     = "dbcluster:"
	rdsDBSnapshotKeyPrefix    = "dbsnapshot:"
	rdsDBSubnetGroupKeyPrefix = "dbsubnetgroup:"
)

// rdsTagsJSONMember is the JSON member every taggable RDS record stores its tags in.
//
// RDSDBInstance, RDSDBCluster and RDSDBSubnetGroup all declare `json:"Tags"` (rds_types.go), so
// one name serves every shape the resolver reaches. It is named rather than written inline
// because [mergeRecordStringMapTags] writes whichever member it is given, and a misspelling
// would add a second tags member while leaving the real one untouched — a tag call that
// answers 200 and stores nothing.
const rdsTagsJSONMember = "Tags"

// rdsResolveARN parses an RDS ARN and returns the namespace and state key it addresses.
//
// The account and Region come from the ARN, never from the caller's request context — the rule
// #826 established, so an ARN naming another account's cluster cannot resolve the caller's own
// same-named one.
func rdsResolveARN(arn string) (ns, key string, err error) {
	// arn:aws:rds:{region}:{acct}:{type}:{id}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != "rds" {
		return "", "", fmt.Errorf("invalid RDS ARN: %q", arn)
	}
	region := parts[3]
	acct := parts[4]
	resType := parts[5]
	resID := parts[6]
	if resID == "" {
		return "", "", fmt.Errorf("RDS ARN names no resource: %q", arn)
	}
	scope := acct + "/" + region

	var prefix string
	switch resType {
	case "db":
		prefix = rdsDBInstanceKeyPrefix
	case "cluster":
		// Distinct from "cluster-pg" and "cluster-snapshot", which are their own resource-type
		// segments and fall through to the refusal below — substrate stores neither.
		prefix = rdsDBClusterKeyPrefix
	case "snapshot":
		// Returned before the plain-identifier check below, and deliberately: an automated
		// snapshot's identifier carries the "rds:" prefix described in this file's preamble, so
		// a colon here is part of the name rather than a stray segment.
		return rdsNamespace, rdsDBSnapshotKeyPrefix + scope + "/" + resID, nil
	case "subgrp":
		prefix = rdsDBSubnetGroupKeyPrefix
	default:
		return "", "", fmt.Errorf("unsupported RDS ARN resource type: %q", resType)
	}

	// A DB instance, cluster or subnet-group name is letters, digits and hyphens — RDS accepts
	// neither "/" nor ":" in one. Without the check an ARN carrying either built a state key with
	// an extra segment, which addresses nothing and so reported the resource absent rather than
	// the ARN malformed: the wrong error to hand a caller and the wrong one to see in a log.
	if strings.ContainsAny(resID, "/:") {
		return "", "", fmt.Errorf("invalid RDS resource identifier: %q", resID)
	}
	return rdsNamespace, prefix + scope + "/" + resID, nil
}

// rdsKeyIsTaggable reports whether an rds-namespace state key names a record substrate stores
// tags on — that is, one of the four shapes [rdsResolveARN] can produce.
//
// The namespace also holds a parameter group, a container handle and the plugin's own index
// keys, none of which any ARN addresses. Naming the reachable set here keeps a caller that
// invented a key from writing a Tags member onto a record whose own service never reads one.
func rdsKeyIsTaggable(key string) bool {
	for _, prefix := range []string{
		rdsDBInstanceKeyPrefix,
		rdsDBClusterKeyPrefix,
		rdsDBSnapshotKeyPrefix,
		rdsDBSubnetGroupKeyPrefix,
	} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// rdsNotFoundError reports that the resource a resolved key addresses does not exist, under the
// fault code AWS publishes for that kind of resource.
//
// Reporting one code for every kind — which is what this path did, always DBInstanceNotFound —
// tells a caller that polls for a cluster or waits on a snapshot that it asked about the wrong
// kind of thing. AddTagsToResource and ListTagsForResource publish a separate 404 per kind, and
// three of substrate's four are on those lists verbatim:
//
//	DBInstanceNotFound      "DBInstanceIdentifier doesn't refer to an existing DB instance."
//	DBClusterNotFoundFault  "DBClusterIdentifier doesn't refer to an existing DB cluster."
//	DBSnapshotNotFound      "DBSnapshotIdentifier doesn't refer to an existing DB snapshot."
//
// A DB subnet group is the exception. It is a taggable resource type in AWS's own ARN table, but
// neither tagging operation's error list names a subnet-group fault. The code and status are
// still AWS's — DescribeDBSubnetGroups publishes DBSubnetGroupNotFoundFault/404,
// "DBSubnetGroupName doesn't refer to an existing DB subnet group." — so what is substrate's
// reading is only the decision to answer it *here*, at an operation whose published list omits
// it. The alternative, keeping DBInstanceNotFound for a subnet group, is wrong under any reading.
func rdsNotFoundError(key, arn string) *AWSError {
	code := "DBInstanceNotFound"
	switch {
	case strings.HasPrefix(key, rdsDBClusterKeyPrefix):
		code = "DBClusterNotFoundFault"
	case strings.HasPrefix(key, rdsDBSnapshotKeyPrefix):
		code = "DBSnapshotNotFound"
	case strings.HasPrefix(key, rdsDBSubnetGroupKeyPrefix):
		code = "DBSubnetGroupNotFoundFault"
	}
	return &AWSError{
		Code:       code,
		Message:    "Resource not found: " + arn,
		HTTPStatus: http.StatusNotFound,
	}
}

// --- Tagging operations ---

func (p *RDSPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	tags, err := p.loadTagsByARN(resourceARN)
	if err != nil {
		return nil, err
	}

	type xmlTag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type result struct {
		TagList []xmlTag `xml:"TagList>Tag"`
	}
	type response struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"ListTagsForResourceResult"`
	}
	// Sorted by key. AWS documents no order for TagList — its own sample response renders
	// "owner" before "environment" — so this is substrate's reading, taken for the reason
	// sortTagsByKey records: the slice was built by ranging a Go map, so two identical calls
	// answered in different orders and a caller asserting on the body could not replay.
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	xmlTags := make([]xmlTag, 0, len(keys))
	for _, k := range keys {
		xmlTags = append(xmlTags, xmlTag{Key: k, Value: tags[k]})
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{TagList: xmlTags},
	})
}

func (p *RDSPlugin) addTagsToResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	newTags := rdsTagsFromParams(req.Params)
	if err := p.updateTagsByARN(resourceARN, newTags, nil); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

func (p *RDSPlugin) removeTagsFromResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	keys := extractIndexedParams(req.Params, "TagKeys.member")
	if err := p.updateTagsByARN(resourceARN, nil, keys); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

// loadTagsByARN resolves an RDS ARN and returns the resource's tags.
func (p *RDSPlugin) loadTagsByARN(arn string) (map[string]string, error) {
	ns, key, err := rdsResolveARN(arn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), ns, key)
	if err != nil {
		return nil, fmt.Errorf("rds loadTagsByARN get %s: %w", key, err)
	}
	if data == nil {
		return nil, rdsNotFoundError(key, arn)
	}
	var res struct {
		Tags map[string]string `json:"Tags"`
	}
	// Reported rather than swallowed. This returned (nil, nil) on an unmarshal failure, so a
	// record substrate could not read answered 200 with an empty TagList — indistinguishable
	// from an untagged resource, and the same accepted-and-ignored shape #835 exists to remove.
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("rds loadTagsByARN unmarshal %s: %w", key, err)
	}
	return res.Tags, nil
}

// updateTagsByARN merges or removes tags on the resource identified by arn.
func (p *RDSPlugin) updateTagsByARN(arn string, add map[string]string, removeKeys []string) error {
	ns, key, err := rdsResolveARN(arn)
	if err != nil {
		return &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ns, key)
	if err != nil {
		return fmt.Errorf("rds updateTagsByARN get %s: %w", key, err)
	}
	if data == nil {
		return rdsNotFoundError(key, arn)
	}
	// Through the shared raw-JSON merge, so the four record shapes the resolver reaches — an
	// instance, a cluster, a snapshot and a subnet group — cannot lose a member none of the
	// others carries. The same helper serves the tagging API's rds arm, so the two paths cannot
	// disagree about what a tag write does to a record.
	updated, err := mergeRecordStringMapTags(data, rdsTagsJSONMember, add, removeKeys)
	if err != nil {
		return fmt.Errorf("rds updateTagsByARN merge %s: %w", key, err)
	}
	return p.state.Put(goCtx, ns, key, updated)
}
