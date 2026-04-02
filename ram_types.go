package substrate

import (
	"strings"
	"time"
)

// ramNamespace is the state namespace for AWS Resource Access Manager resources.
const ramNamespace = "ram"

// RAMResourceShare represents an AWS RAM resource share.
type RAMResourceShare struct {
	// ResourceShareArn is the ARN of the resource share.
	ResourceShareArn string `json:"resourceShareArn"`
	// Name is the display name of the resource share.
	Name string `json:"name"`
	// OwningAccountId is the account that owns the resource share.
	OwningAccountId string `json:"owningAccountId"`
	// AllowExternalPrincipals indicates whether the share can include external accounts.
	AllowExternalPrincipals bool `json:"allowExternalPrincipals"`
	// Status is the status of the resource share (ACTIVE).
	Status string `json:"status"`
	// Principals is the list of principal ARNs or account IDs with access.
	Principals []string `json:"principals,omitempty"`
	// ResourceArns is the list of resource ARNs included in the share.
	ResourceArns []string `json:"resourceArns,omitempty"`
	// Tags is the list of key-value tags for the resource share.
	Tags []RAMTag `json:"tags,omitempty"`
	// CreationTime is when the resource share was created.
	CreationTime time.Time `json:"creationTime"`
	// LastUpdatedTime is when the resource share was last modified.
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	// AccountID is the AWS account that owns this resource share.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the resource share exists.
	Region string `json:"region"`
}

// RAMTag is a key-value tag for an AWS RAM resource.
type RAMTag struct {
	// Key is the tag key.
	Key string `json:"key"`
	// Value is the tag value.
	Value string `json:"value"`
}

// parseRAMOperation maps an HTTP method and URL path to an AWS RAM operation name.
// RAM uses lowercase POST paths for all operations.
func parseRAMOperation(method, path string) string {
	// Normalize: lowercase path, trim leading slash.
	p := strings.ToLower(strings.TrimPrefix(path, "/"))
	// Strip any query string.
	if idx := strings.Index(p, "?"); idx >= 0 {
		p = p[:idx]
	}
	switch {
	case p == "createresourceshare" && method == "POST":
		return "CreateResourceShare"
	case p == "getresourceshares" && method == "POST":
		return "GetResourceShares"
	case p == "updateresourceshare" && method == "POST":
		return "UpdateResourceShare"
	case p == "deleteresourceshare" && (method == "DELETE" || method == "POST"):
		return "DeleteResourceShare"
	case p == "associateresourceshare" && method == "POST":
		return "AssociateResourceShare"
	case p == "disassociateresourceshare" && method == "POST":
		return "DisassociateResourceShare"
	case p == "listprincipals" && method == "POST":
		return "ListPrincipals"
	case p == "listresources" && method == "POST":
		return "ListResources"
	}
	return method
}

// State key helpers.

func ramShareKey(acct, region, shareArn string) string {
	return "share:" + acct + "/" + region + "/" + shareArn
}

func ramShareArnsKey(acct, region string) string {
	return "share_arns:" + acct + "/" + region
}
