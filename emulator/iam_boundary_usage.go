package emulator

import (
	"context"
	"encoding/json"
	"fmt"
)

// PermissionsBoundaryUsageCount on the IAM policy shapes (#815).
//
// AWS documents the member on the `Policy` data type, whose own scope note says the type "is
// used as a response element in the CreatePolicy, GetPolicy, and ListPolicies operations":
//
//	PermissionsBoundaryUsageCount — The number of entities (users and roles) for which the
//	policy is used to set the permissions boundary.
//
// so it belongs on every policy shape substrate renders. `Description` on the same type carries
// an explicit carve-out — "It is not included in the response to the ListPolicies operation" —
// and this member carries none; `ListPolicies`' own sample response renders it on all four
// members, two of them non-zero. #807 rendered every other documented member of the shape and
// recorded this one as its stated cost. This is that cost paid.
//
// The count is derived per read rather than stored, for the same reason a listing derives
// `AttachmentCount` (see [IAMPlugin.iamPolicyAttachmentCounts]): substrate keeps a boundary as
// an ARN on the entity — [IAMUser.PermissionsBoundary], [IAMRole.PermissionsBoundary] — and not
// as a back-reference on the policy. Three of #815's acceptance criteria then hold by
// construction instead of by maintenance:
//
//   - the count reaches zero when the last boundary is removed, because there is no counter to
//     forget to decrement;
//   - deleting a user or a role decrements it, because DeleteUser and DeleteRole delete the
//     whole entity record and the boundary goes with it — neither has to learn about policies;
//   - a replayed run reports the same count as the live one, because the count is a function of
//     the state replayed events rebuild rather than an accumulator that has to end up agreeing
//     with one accumulated live.
//
// A counter on [IAMPolicy] would instead have to be maintained on PutUserPermissionsBoundary,
// DeleteUserPermissionsBoundary, PutRolePermissionsBoundary and DeleteRolePermissionsBoundary
// *and* on DeleteUser and DeleteRole, where missing any one path reports a count that is then
// wrong forever — and the live counter would still have to be reconciled against a replayed one.
//
// The cost #815 names, O(policies × entities), is avoided by hoisting: the scan runs once per
// request, not once per policy, and the renderer is handed the finished map. A policy read is
// therefore O(policies + entities). Threading the count as an argument rather than assigning it
// to a field also keeps the bundled catalog safe to share — ListManagedPolicies hands back
// shared pointers, which is why the derived `AttachmentCount` needs a local copy first.

// iamBoundaryUsageCounts counts, per policy ARN, the users and roles that name it as their
// permissions boundary.
//
// One pass over the user and role records answers for every policy at once, so a caller renders
// a whole listing from a single call. Both records store the boundary under the same
// `PermissionsBoundary` JSON key, so the pass decodes that member alone rather than two whole
// entity types: it is the only member the count reads, and a change to any other cannot affect
// it.
//
// A record that does not decode is skipped rather than failing the read, matching
// [IAMPlugin.iamPolicyCandidates] — one corrupt key should not make every policy unreadable. A
// store failure is returned, because reporting zero there would be indistinguishable from a
// policy no entity uses as a boundary.
func (p *IAMPlugin) iamBoundaryUsageCounts(goCtx context.Context, accountID string) (map[string]int, error) {
	counts := make(map[string]int)

	for _, prefix := range []string{iamUserPrefix(accountID), iamRolePrefix(accountID)} {
		keys, err := p.state.List(goCtx, iamNamespace, prefix)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, k := range keys {
			raw, err := p.state.Get(goCtx, iamNamespace, k)
			if err != nil {
				return nil, fmt.Errorf("get %s: %w", k, err)
			}
			if raw == nil {
				continue
			}
			var entity struct {
				PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary"`
			}
			if err := json.Unmarshal(raw, &entity); err != nil {
				continue
			}
			if entity.PermissionsBoundary == nil || entity.PermissionsBoundary.PolicyARN == "" {
				continue
			}
			counts[entity.PermissionsBoundary.PolicyARN]++
		}
	}

	return counts, nil
}
