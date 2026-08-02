package emulator

import "strings"

// s3CannedACLHeader is the request header carrying a canned ACL name, and
// s3GroupLogDelivery the third predefined group — the one that is not public.
//
// The two public group URIs live in s3_publicaccess.go, where the definition of
// "public" is. LogDelivery is here instead because it is only ever a grantee, never
// a reason to refuse a request: "WRITE permission on a bucket enables this group to
// write server access logs" (S3 user guide, ACL overview, Amazon S3 predefined
// groups).
const (
	s3CannedACLHeader   = "x-amz-acl"
	s3GroupLogDelivery  = "http://acs.amazonaws.com/groups/s3/LogDelivery"
	s3PermissionRead    = "READ"
	s3PermissionWrite   = "WRITE"
	s3PermissionReadACP = "READ_ACP"
	s3PermissionFull    = "FULL_CONTROL"
)

// s3ACLResourceKind says whether an ACL is being resolved for a bucket or for an
// object.
//
// Two canned ACLs mean different things on the two, so the resolver cannot be
// kind-blind: log-delivery-write is documented "Applies to: Bucket" and
// bucket-owner-read/bucket-owner-full-control as "Applies to: Object", the latter
// pair adding "If you specify this canned ACL when creating a bucket, Amazon S3
// ignores it" (S3 user guide, ACL overview, Canned ACL table).
type s3ACLResourceKind int

// The two resource kinds an ACL can be resolved for.
const (
	s3ACLBucket s3ACLResourceKind = iota
	s3ACLObject
)

// s3GrantHeader pairs one x-amz-grant-* header with the ACL permission it grants.
type s3GrantHeader struct {
	name       string
	permission string
}

// s3GrantHeaders are the five headers that name grantees for one permission each,
// in the order the API reference lists them.
//
// The set is uniform across the four operations substrate resolves ACLs on, with
// one documented exception: PutObject's request syntax omits x-amz-grant-write,
// consistent with the permissions table giving WRITE no object meaning ("Not
// applicable"). Substrate records it there anyway rather than inventing a
// rejection — no error code is documented for an unaccepted grant header, and
// refusing a request real S3 may accept is the defect this family of fixes exists
// to remove. WRITE on an object is inert either way.
var s3GrantHeaders = []s3GrantHeader{
	{"x-amz-grant-full-control", s3PermissionFull},
	{"x-amz-grant-read", s3PermissionRead},
	{"x-amz-grant-read-acp", s3PermissionReadACP},
	{"x-amz-grant-write", s3PermissionWrite},
	{"x-amz-grant-write-acp", "WRITE_ACP"},
}

// s3ParseGrantees parses one x-amz-grant-* header value into its grantees.
//
// The value is a comma-separated list of type=value pairs — for example
// `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="abc123"`. Values may
// be quoted, so quotes are trimmed, and the type is matched case-insensitively
// because the documented spelling of one of them is mixed-case (emailAddress).
//
// Only id and uri produce a grantee, which is exactly the pair the user guide's
// "Who is a grantee?" section lists. An emailAddress grantee is skipped rather
// than stored: S3 has ended support for it — "As of October 1, 2025, Amazon S3 has
// discontinued support for Email Grantee Access Control Lists (ACLs). If you
// attempt to use an Email Grantee ACL in a request after October 1, 2025, the
// request will receive an HTTP 405 (Method Not Allowed) error" — and substrate's
// clock is past that date. Returning the 405 is a separate behavior, scoped out
// because it is Region-conditional and applies to the XML body form too (#507).
func s3ParseGrantees(value string) []S3Grantee {
	if value == "" {
		return nil
	}
	var grantees []S3Grantee
	for _, entry := range strings.Split(value, ",") {
		kind, val, found := strings.Cut(strings.TrimSpace(entry), "=")
		if !found {
			continue
		}
		val = strings.Trim(strings.TrimSpace(val), `"`)
		if val == "" {
			continue
		}
		switch {
		case strings.EqualFold(strings.TrimSpace(kind), "id"):
			grantees = append(grantees, S3Grantee{Type: "CanonicalUser", ID: val})
		case strings.EqualFold(strings.TrimSpace(kind), "uri"):
			grantees = append(grantees, S3Grantee{Type: "Group", URI: val})
		}
	}
	return grantees
}

// s3GrantsFromHeaders resolves the x-amz-grant-* headers on a request into grants,
// or nil when the request carries none.
//
// Header names are read case-insensitively through [headerValueFold]: an
// in-process request never passes through net/http's canonicalization, so a
// canonical-case map lookup silently misses one.
func s3GrantsFromHeaders(headers map[string]string) []S3Grant {
	var grants []S3Grant
	for _, gh := range s3GrantHeaders {
		for _, grantee := range s3ParseGrantees(headerValueFold(headers, gh.name)) {
			grants = append(grants, S3Grant{Grantee: grantee, Permission: gh.permission})
		}
	}
	return grants
}

// s3RequestNamesACL reports whether a request carries any of the six ACL headers.
//
// PutObject and CreateBucket use this to store an ACL only when one was asked for,
// keeping "the caller named an ACL" distinguishable in state from "the caller named
// none and got the default". PutBucketAcl and PutObjectAcl do not: their whole
// purpose is to set an ACL, and one sent with no header at all is documented to
// resolve to private.
func s3RequestNamesACL(headers map[string]string) bool {
	if headerValueFold(headers, s3CannedACLHeader) != "" {
		return true
	}
	for _, gh := range s3GrantHeaders {
		if headerValueFold(headers, gh.name) != "" {
			return true
		}
	}
	return false
}

// s3RequestACL resolves the ACL a create operation named, or nil when it named
// none.
//
// The nil is the whole point: PutObject, CopyObject, CreateMultipartUpload and
// CreateBucket all store an ACL only when one was asked for, so "the caller named
// none and got the default" stays distinguishable in state from "the caller named
// the default". [S3Plugin.s3StoreObjectACL] and
// [S3Plugin.s3RequestACLDenied] both read the distinction from this one pointer
// rather than from a parallel boolean.
func s3RequestACL(headers map[string]string, resource string, kind s3ACLResourceKind) *S3AccessControlList {
	if !s3RequestNamesACL(headers) {
		return nil
	}
	acl := s3ResolveRequestACL(headers, resource, kind)
	return &acl
}

// s3ResolveRequestACL resolves the ACL a request expresses through its headers,
// for a resource whose owner is derived from resource.
//
// A request with no ACL header at all resolves to the owner-only ACL, which is
// what makes this safe to call unconditionally on every create: "By default, all
// objects are private. Only the owner has full access control" (PutObject,
// x-amz-acl).
//
// The two forms are documented as mutually exclusive — "If you use these ACL-
// specific headers, you cannot use the x-amz-acl header to set a canned ACL" — but
// no error code is documented for sending both, so substrate resolves rather than
// refuses and the grant headers win, being the more specific expression. Which one
// wins cannot open a Block Public Access hole either way: [S3Plugin.s3PublicACLDenied]
// examines the resolved ACL *and* the raw headers.
func s3ResolveRequestACL(headers map[string]string, resource string, kind s3ACLResourceKind) S3AccessControlList {
	grants := s3GrantsFromHeaders(headers)
	if len(grants) == 0 {
		return s3CannedACL(headerValueFold(headers, s3CannedACLHeader), resource, kind)
	}

	// The named grants are added to the default ACL rather than replacing it: "you
	// specify explicit access permissions and grantees … These permissions are then
	// added to the ACL on the object. By default, all objects are private. Only the
	// owner has full access control" (PutObject, x-amz-acl). So the owner keeps
	// FULL_CONTROL, which is also what makes every ACL substrate stores have an
	// owner grant, as every ACL in the API reference's own examples does.
	acl := s3DefaultACL(resource)
	acl.Grants = append(acl.Grants, grants...)
	return acl
}

// s3CannedACL maps a canned ACL name (the x-amz-acl header) to an
// S3AccessControlList, from the user guide's Canned ACL table.
//
// The owner grant is present in every case because a canned ACL is applied on top
// of the ACL the resource already has: "When Amazon S3 receives a request with a
// canned ACL in the request, it adds the predefined grants to the ACL of the
// resource", and a new bucket or object starts with an ACL granting its owner
// FULL_CONTROL. That is why log-delivery-write keeps the owner grant even though
// the table's entry for it names only the LogDelivery grants.
//
// Four canned names resolve to owner-only, each for its own documented reason
// rather than by falling through:
//
//   - private — "Owner gets FULL_CONTROL. No one else has access rights (default)."
//   - aws-exec-read — grants Amazon EC2 READ, whose canonical user ID AWS does not
//     publish, so substrate cannot name the grantee and records no second grant.
//   - bucket-owner-read and bucket-owner-full-control — both distinguish the object
//     owner from the bucket owner, and substrate has one owner identity per bucket,
//     so the two principals are the same and the grants collapse. On a bucket the
//     table says S3 "ignores it", which is the same answer.
//
// An unrecognized value also resolves to owner-only rather than being refused: the
// per-operation Valid Values lists differ (CreateBucket and PutBucketAcl document
// four names, PutObject and PutObjectAcl seven), and no error code is documented
// for a canned name outside them.
func s3CannedACL(cannedACL, resource string, kind s3ACLResourceKind) S3AccessControlList {
	acl := s3DefaultACL(resource)
	group := func(uri, permission string) S3Grant {
		return S3Grant{Grantee: S3Grantee{Type: "Group", URI: uri}, Permission: permission}
	}

	switch cannedACL {
	case "public-read":
		acl.Grants = append(acl.Grants, group(s3GroupAllUsers, s3PermissionRead))
	case "public-read-write":
		acl.Grants = append(acl.Grants,
			group(s3GroupAllUsers, s3PermissionRead),
			group(s3GroupAllUsers, s3PermissionWrite))
	case "authenticated-read":
		// AuthenticatedUsers is every AWS account, not every account in yours, so
		// this ACL is public by Block Public Access's definition and a bucket that
		// blocks public ACLs refuses it. Substrate resolved it to owner-only until
		// #470, which meant the block could be walked straight through.
		acl.Grants = append(acl.Grants, group(s3GroupAuthenticatedUsers, s3PermissionRead))
	case "log-delivery-write":
		// "Applies to: Bucket" — the table gives this name no object meaning, so on
		// an object it resolves to owner-only like any other unrecognized value.
		if kind == s3ACLBucket {
			acl.Grants = append(acl.Grants,
				group(s3GroupLogDelivery, s3PermissionWrite),
				group(s3GroupLogDelivery, s3PermissionReadACP))
		}
	}
	return acl
}

// s3DefaultACL returns the ACL a bucket or object has when nothing has set one:
// its owner with FULL_CONTROL and no other grant.
//
// "When you create a bucket or an object, Amazon S3 creates a default ACL that
// grants the resource owner full control over the resource" — and the sample that
// follows notes the default object ACL has the same structure, which is why one
// function serves both kinds.
//
// The owner identity is derived from the resource name because substrate has no
// canonical user IDs: there is one owner per bucket and it owns everything in it.
func s3DefaultACL(resource string) S3AccessControlList {
	return S3AccessControlList{
		Owner: S3Owner{ID: resource + "-owner", DisplayName: resource},
		Grants: []S3Grant{{
			Grantee:    S3Grantee{Type: "CanonicalUser", ID: resource + "-owner"},
			Permission: s3PermissionFull,
		}},
	}
}
