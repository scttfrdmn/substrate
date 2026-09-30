package emulator

// An instance's iamInstanceProfile is resolved from IAM, not minted per read (#1291).
//
// `ec2InstanceItemFor` — the builder both RunInstances and DescribeInstances render an
// instance through — used to report `"AIPA" + randomHex(8)`, which is a value drawn on
// **every call**. Three things followed. Two DescribeInstances calls on one unchanged
// instance reported two different `iamInstanceProfile.id` values, so a consumer that
// describes and compares — a drift check, a Terraform refresh, a cache — saw a change that
// did not happen. The id named nothing: substrate's IAM plugin mints a real AIPA… id when
// `CreateInstanceProfile` runs, and this value was unrelated to it, so a caller taking the id
// from a describe and handing it to IAM got a not-found. And it was the last caller of
// randomHex, the shared crypto/rand helper #856 exists to remove.
//
// # What decides the value
//
// Real EC2 reports the id of the actual instance profile, which is stable for the life of
// the profile — so substrate resolves the profile out of IAM state and reports what IAM
// stored, both its id and its ARN. The ARN matters as well as the id: IAM's ARN carries the
// profile's **path**, and a synthesized one cannot, so a profile created at `/dev/` was
// reported at the root.
//
// The account comes from the ARN when the launch recorded an ARN, never from the caller's
// context (#826). An instance launched with `arn:aws:iam::111111111111:instance-profile/app`
// names another account's profile, and resolving that against the caller's account would
// report the caller's same-named profile's id for it.
//
// # When IAM has no such profile
//
// Substrate lets an instance launch with a profile name that was never created, so the
// resolution has to answer for a profile that does not exist. It derives the id from the
// account and the name — the way a public IP is derived from its instance id and a secret's
// ARN from its name — rather than drawing one. Deriving keeps the property the draw lacked:
// the same instance reports the same id on every read, and two profiles report different
// ids. It deliberately does *not* derive from the request id through [IDMint], which is
// #856's seam: a request-derived value is stable across a *replay* of one request and would
// still differ between two describes, which is the bug.
//
// # Rendering
//
// EC2's `API_IamInstanceProfile` publishes `id` as a String with no pattern and no length, so
// the shape is decided by the IAM page the value comes from: `API_InstanceProfile` publishes
// `InstanceProfileId` with a **minimum length of 16, a maximum of 128, and pattern `[\w]+`**.
// Substrate's own [generateIAMID] renders 21 characters of `[A-Z0-9]` behind the AIPA prefix
// and satisfies both, so a derived id uses that form and is indistinguishable from one IAM
// itself stored. The old 20-character lowercase-hex form satisfied the published pattern too —
// this is not a case of #671's rule being broken — but it disagreed with the rendering
// substrate's own IAM plugin uses, which is the rendering a resolved profile now reports.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
)

// ec2InstanceProfileItem renders an instance's iamInstanceProfile element from the name or
// ARN the launch recorded, resolving it against IAM state.
//
// Returns nil when the instance recorded no profile, which is what leaves the element off the
// response entirely: `iamInstanceProfile` is `Required: No`, and an empty element would claim
// the instance has a profile whose id and ARN are blank.
func (p *EC2Plugin) ec2InstanceProfileItem(reqCtx *RequestContext, stored string) *ec2IAMInstanceProfileItem {
	if stored == "" {
		return nil
	}

	accountID, name := ec2InstanceProfileAccountAndName(reqCtx.AccountID, stored)
	if profile := p.lookupInstanceProfile(accountID, name); profile != nil {
		return &ec2IAMInstanceProfileItem{ARN: profile.ARN, ID: profile.InstanceProfileID}
	}

	// No IAM record: report the ARN the launch named — or the one the bare name implies at
	// the root, since a path is exactly what an unresolved profile cannot tell us — and a
	// derived id.
	arn := stored
	if !strings.HasPrefix(arn, "arn:") {
		arn = iamInstanceProfileARN(accountID, "", name)
	}
	return &ec2IAMInstanceProfileItem{ARN: arn, ID: ec2DerivedInstanceProfileID(accountID, name)}
}

// lookupInstanceProfile reads one instance profile out of IAM's state, or nil when IAM has
// no record of it.
//
// A read failure is nil rather than an error for the same reason the resolution has a derived
// fallback at all: an instance describes with the profile it recorded whether or not IAM knows
// the profile, and a state backend that cannot answer must not turn a DescribeInstances into a
// refusal (the caller asked EC2 about an instance, not IAM about a profile).
func (p *EC2Plugin) lookupInstanceProfile(accountID, name string) *IAMInstanceProfile {
	if p.state == nil || accountID == "" || name == "" {
		return nil
	}
	raw, err := p.state.Get(context.Background(), iamNamespace, iamInstanceProfileKey(accountID, name))
	if err != nil || raw == nil {
		return nil
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(raw, &profile); err != nil {
		return nil
	}
	if profile.InstanceProfileID == "" || profile.ARN == "" {
		return nil
	}
	return &profile
}

// ec2InstanceProfileAccountAndName splits the value a launch recorded into the account the
// profile belongs to and its name.
//
// A recorded ARN carries both; a bare name carries neither, so it belongs to the account
// doing the describing. The name is the last path segment, because an ARN for a profile at a
// path is `instance-profile/dev/app` and IAM keys a profile by its name alone.
func ec2InstanceProfileAccountAndName(callerAccountID, stored string) (accountID, name string) {
	if !strings.HasPrefix(stored, "arn:") {
		return callerAccountID, stored
	}
	accountID = arnAccountID(stored)
	if accountID == "" {
		accountID = callerAccountID
	}
	resource := stored
	if i := strings.Index(stored, ":instance-profile/"); i >= 0 {
		resource = stored[i+len(":instance-profile/"):]
	}
	if i := strings.LastIndex(resource, "/"); i >= 0 {
		resource = resource[i+1:]
	}
	return accountID, resource
}

// ec2DerivedInstanceProfileID derives the AIPA… id substrate reports for an instance profile
// IAM has no record of, from the account and the profile's name.
//
// Stable across reads and distinct per profile, which is the whole of what the drawn id
// lacked. The rendering matches [generateIAMID]'s — 21 characters, `[A-Z0-9]` behind the
// prefix — so a consumer cannot tell a derived id from a stored one by looking at it, and
// both satisfy `API_InstanceProfile`'s published `[\w]+` and 16–128 length.
func ec2DerivedInstanceProfileID(accountID, name string) string {
	sum := sha256.Sum256([]byte("instance-profile/" + accountID + "/" + name))
	const suffixLen = 17 // 21 total, behind "AIPA".
	out := make([]byte, suffixLen)
	for i := range out {
		out[i] = iamIDChars[int(sum[i])%len(iamIDChars)]
	}
	return "AIPA" + string(out)
}
