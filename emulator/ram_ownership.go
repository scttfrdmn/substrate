package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// RAM's read operations consult the request, a disassociation reports what it did, and a delete is
// soft (#1171).
//
// # resourceOwner is required and read
//
// GetResourceShares, ListPrincipals and ListResources each mark `resourceOwner` Required: Yes with Valid
// Values `SELF | OTHER-ACCOUNTS`. GetResourceShares decoded it and never read it; the two list
// operations took no request at all. Now:
//
//   - An absent `resourceOwner` is [ramMissingParameter]'s ValidationError/400, the vocabulary #1169
//     settled for every RAM member marked Required: Yes.
//   - A value outside the two is InvalidParameterException/400, which all three pages publish as "The
//     operation failed because a parameter you specified isn't valid." The message is substrate's.
//   - `SELF` answers the caller's own shares, as every request did before.
//   - `OTHER-ACCOUNTS` answers an **empty list**. It asks for shares other accounts have shared with the
//     caller, and nothing in substrate shares across accounts: a share's principals are recorded but no
//     other account's view is derived from them. So the empty list is the true answer here, not a
//     placeholder, and it replaced the caller's own shares, which was the opposite of what was asked.
//
// # The published filters are applied
//
// GetResourceShares applies `name`, `resourceShareArns` and now `resourceShareStatus` (validated
// against `PENDING | ACTIVE | FAILED | DELETING | DELETED`). ListPrincipals applies `principals`,
// `resourceArn` (principals of the shares that include that resource), `resourceShareArns` and
// `resourceType`. ListResources applies `principal`, `resourceArns`, `resourceShareArns`, `resourceType`
// and `resourceRegionScope` (validated against `ALL | REGIONAL | GLOBAL`, default ALL).
//
// A resource's type is derived from its ARN as `service:resource-type` — `arn:aws:ec2:…:subnet/subnet-1`
// is `ec2:subnet` — and compared case-insensitively, since API_Resource glosses the type as "of the form
// service-code:resource-code, and is case-insensitive". Substrate records ARNs, not types, so this is
// its reading of an ARN rather than RAM's own catalog, and a `resourceType` that RAM's ListResourceTypes
// would reject is not refused but simply matches nothing. A resource's scope is GLOBAL when its ARN has
// an empty Region field and REGIONAL otherwise.
//
// Not read, and recorded so they are not mistaken for applied: `maxResults` and `nextToken` on all three
// (every share, principal and resource is one page), and `tagFilters`, `permissionArn` and
// `permissionVersion` on GetResourceShares.
//
// # Statuses
//
// DisassociateResourceShare reports each association it removed as `DISASSOCIATED`; it reported
// `ASSOCIATED`, contradicting the operation that produced it. That is the settled state, as
// AssociateResourceShare's `ASSOCIATED` is; the transitional ASSOCIATING and DISASSOCIATING the pages
// also publish are not modeled. ListPrincipals no longer answers a `status` on a principal, because
// API_Principal publishes none. ListResources keeps `AVAILABLE`, which API_Resource publishes and which
// is the state of a resource substrate holds in a share.
//
// # A delete is soft
//
// DeleteResourceShare moved the record out of state, so a GetResourceShares after it reported the share
// absent where RAM reports it `DELETED`. The share now stays, with status DELETED and lastUpdatedTime the
// moment of deletion, for [ramDeletedShareRetention] of simulated time — the RAM User Guide's "The
// deleted resource share disappears after two hours. Until then, it remains visible in the console with
// a deleted status." — and is then neither listed nor addressable, answering UnknownResourceException as
// a share that never existed does. The window is read against the simulated clock at request time, so a
// replay sees exactly what the recording saw.
//
// Within the window a DELETED share is listed by GetResourceShares with no status filter or with
// `resourceShareStatus: DELETED`, and by no other filter value. It shares nothing: its principals and
// resources are not reported by ListPrincipals or ListResources, since "all principals that were
// associated with the resource share lose access to the shared resources". A second delete is
// InvalidStateTransitionException/400, as is an associate or a disassociate — each of those three pages
// publishes the code as "the requested operation isn't valid for the resource share in its current
// state". UpdateResourceShare does not publish that code; it publishes OperationNotPermittedException,
// "the requested operation isn't permitted", and answers that. The messages are substrate's.
//
// # Read errors
//
// The three read operations skipped a share whose record failed to load or decode, so a store fault
// shortened the answer and reported success. They now fail. An index entry whose record is absent is
// still passed over, since that is an index outliving its record rather than a fault.

// ramDeletedShareRetention is how long a deleted share stays visible, per the RAM User Guide.
const ramDeletedShareRetention = 2 * time.Hour

// ramShareStatusDeleted is the terminal ResourceShare status a delete moves a share to.
const ramShareStatusDeleted = "DELETED"

// ramShareStatuses is the Valid Values list API_GetResourceShares publishes for resourceShareStatus.
var ramShareStatuses = []string{"PENDING", "ACTIVE", "FAILED", "DELETING", ramShareStatusDeleted}

// ramRegionScopes is the Valid Values list API_ListResources publishes for resourceRegionScope.
var ramRegionScopes = []string{"ALL", "REGIONAL", "GLOBAL"}

// ramInvalidParameter is RAM's published refusal for a parameter value it does not accept.
func ramInvalidParameter(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// ramInvalidStateTransition refuses an operation on a share whose status does not permit it.
func ramInvalidStateTransition(share *RAMResourceShare) *AWSError {
	return &AWSError{
		Code:       "InvalidStateTransitionException",
		Message:    "ResourceShare " + share.ResourceShareArn + " is " + share.Status,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramOperationNotPermitted refuses an UpdateResourceShare on a deleted share.
func ramOperationNotPermitted(share *RAMResourceShare) *AWSError {
	return &AWSError{
		Code:       "OperationNotPermittedException",
		Message:    "ResourceShare " + share.ResourceShareArn + " is " + share.Status + " and cannot be updated",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramCheckResourceOwner reads a required resourceOwner, reporting whether it asks for the caller's own
// shares (SELF) rather than other accounts' (OTHER-ACCOUNTS).
func ramCheckResourceOwner(owner string) (self bool, err error) {
	switch owner {
	case "":
		return false, ramMissingParameter("resourceOwner is required")
	case "SELF":
		return true, nil
	case "OTHER-ACCOUNTS":
		return false, nil
	default:
		return false, ramInvalidParameter("resourceOwner must be SELF or OTHER-ACCOUNTS, was " + owner)
	}
}

// ramShareVisible reports whether a share is still observable at now: every share but a DELETED one
// whose retention window has passed.
func ramShareVisible(share *RAMResourceShare, now time.Time) bool {
	return share.Status != ramShareStatusDeleted || now.Before(share.LastUpdatedTime.Add(ramDeletedShareRetention))
}

// loadShares returns every visible share in the caller's account and Region, in index order, failing
// on a record that cannot be read.
func (p *RAMPlugin) loadShares(goCtx context.Context, acct, region string) ([]RAMResourceShare, error) {
	arns, err := loadStringIndex(goCtx, p.state, ramNamespace, ramShareArnsKey(acct, region))
	if err != nil {
		return nil, fmt.Errorf("ram loadShares load index: %w", err)
	}
	now := p.tc.Now()
	shares := make([]RAMResourceShare, 0, len(arns))
	for _, arn := range arns {
		data, err := p.state.Get(goCtx, ramNamespace, ramShareKey(acct, region, arn))
		if err != nil {
			return nil, fmt.Errorf("ram loadShares get %s: %w", arn, err)
		}
		if data == nil {
			continue // an index entry outliving its record, which is not a fault
		}
		var share RAMResourceShare
		if err := json.Unmarshal(data, &share); err != nil {
			return nil, fmt.Errorf("ram loadShares unmarshal %s: %w", arn, err)
		}
		if ramShareVisible(&share, now) {
			shares = append(shares, share)
		}
	}
	return shares, nil
}

// ramResourceType derives `service:resource-type` from a resource ARN, lowercased.
func ramResourceType(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 {
		return ""
	}
	resource := parts[5]
	if i := strings.IndexAny(resource, "/:"); i >= 0 {
		resource = resource[:i]
	}
	return strings.ToLower(parts[2] + ":" + resource)
}

// ramResourceRegionScope is GLOBAL for an ARN with no Region and REGIONAL otherwise.
func ramResourceRegionScope(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 && parts[3] == "" {
		return "GLOBAL"
	}
	return "REGIONAL"
}

// ramMatchesAny reports whether filter is empty or contains v.
func ramMatchesAny(filter []string, v string) bool {
	return len(filter) == 0 || slices.Contains(filter, v)
}

// ramMatchesType reports whether want is empty or names the type of arn, case-insensitively.
func ramMatchesType(want, arn string) bool {
	return want == "" || strings.EqualFold(want, ramResourceType(arn))
}
