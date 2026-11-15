package emulator

import (
	"strings"
)

// opensearchDefaultDomain is the domain segment of a state key when the request's host names no
// domain: a base endpoint such as `localhost:4566` or `es.us-east-1.amazonaws.com`, or any host
// whose first label is not one of the documented domain-endpoint forms.
const opensearchDefaultDomain = "_default"

// openSearchScope is the account, Region and domain an OpenSearch request is addressed to. Every
// OpenSearch state key is built from one, so two accounts, two Regions or two domains in one run are
// isolated from each other the way they are against AWS (#1200).
type openSearchScope struct {
	account string
	region  string
	domain  string
}

// openSearchScopeFor builds the scope of a request from its context and Host header.
//
// The account and Region come from the request context, as every other plugin's keys do. The domain
// comes from the host, because that is the only place an OpenSearch data-plane request names it: the
// path is the index's, and the engine has no notion of a domain at all. See [openSearchDomainFromHost].
func openSearchScopeFor(ctx *RequestContext, req *AWSRequest) openSearchScope {
	s := openSearchScope{domain: openSearchDomainFromHost(req.Headers["Host"])}
	if ctx != nil {
		s.account, s.region = ctx.AccountID, ctx.Region
	}
	return s
}

// prefix is the `{account}/{region}/{domain}` component every OpenSearch key carries.
func (s openSearchScope) prefix() string {
	return s.account + "/" + s.region + "/" + s.domain
}

// indexKey is the state key of an index record, `index:{account}/{region}/{domain}/{index}`.
func (s openSearchScope) indexKey(index string) string {
	return "index:" + s.prefix() + "/" + index
}

// docKey is the state key of one document, `doc:{account}/{region}/{domain}/{index}/{id}`.
func (s openSearchScope) docKey(index, id string) string {
	return s.docPrefix(index) + id
}

// docPrefix is the prefix every document of index shares, which is how an index's documents are
// listed. An index name cannot contain `/` (the engine refuses it in a name), so the prefix of one
// index never matches a document of another.
func (s openSearchScope) docPrefix(index string) string {
	return "doc:" + s.prefix() + "/" + index + "/"
}

// scrollKey is the state key of a scroll context, `scroll:{account}/{region}/{domain}/{id}`. A
// continuation is addressed to the same host as the search that opened it, so it resolves to the
// same scope.
func (s openSearchScope) scrollKey(id string) string {
	return "scroll:" + s.prefix() + "/" + id
}

// openSearchDomainFromHost returns the domain segment of a state key for host.
//
// The Amazon OpenSearch Service developer guide documents two domain-endpoint forms, the public
// `search-{domain-name}-{identifier}.{region}.es.amazonaws.com` and the VPC
// `vpc-{domain-name}-{identifier}.{region}.es.amazonaws.com` ("Launching your Amazon OpenSearch
// Service domains within a VPC"). A domain name may itself contain hyphens, and the identifier is the
// final hyphen-separated part, so the name is what lies between the prefix and the last hyphen. An
// OpenSearch Serverless collection endpoint is `{collection-id}.{region}.aoss.amazonaws.com`, and its
// segment is `aoss.{collection-id}`: a domain name cannot contain a dot, so the two never collide.
//
// Any other host — a base endpoint such as `localhost:4566`, the control-plane host
// `es.{region}.amazonaws.com`, or an `.es.` host whose first label is in neither form — answers
// [opensearchDefaultDomain]. That is the documented simplification: every client that addresses a
// base endpoint shares one domain per account and Region.
func openSearchDomainFromHost(host string) string {
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}
	host = strings.ToLower(host)
	if !strings.HasSuffix(host, ".amazonaws.com") {
		return opensearchDefaultDomain
	}
	labels := strings.Split(strings.TrimSuffix(host, ".amazonaws.com"), ".")
	// {first}.{region}.es or {first}.{region}.aoss — exactly three labels.
	if len(labels) != 3 || labels[0] == "" {
		return opensearchDefaultDomain
	}
	first := labels[0]
	switch labels[2] {
	case "aoss":
		return "aoss." + first
	case "es":
		for _, p := range []string{"search-", "vpc-"} {
			rest, ok := strings.CutPrefix(first, p)
			if !ok {
				continue
			}
			dash := strings.LastIndexByte(rest, '-')
			if dash <= 0 || dash == len(rest)-1 {
				return opensearchDefaultDomain
			}
			return rest[:dash]
		}
	}
	return opensearchDefaultDomain
}
