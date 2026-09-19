package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Propagating a stack's own tags to the resources it creates (#764).
//
// `CreateStack`'s `Tags.member.N` is recorded on the stack and reported by `DescribeStacks`,
// and AWS says what else it does: "CloudFormation also propagates these tags to the resources
// created in the stack." So the tags reach the same resources the three `aws:cloudformation:*`
// keys do, through the same per-service tag stores [cfnStampResourceTags] writes to.
//
// **The hard part is not the write, it is deciding whose tag a key is.** A propagated tag must
// not clobber one the caller set directly on the resource, and a stack tag whose value the
// caller *changes* on the stack must still reach the resource — those two pull in opposite
// directions, and neither is decidable from the new tag set alone. What decides it is the
// stack's *previous* tag set, which substrate already stores on [CFNStackState]: a resource
// whose value for a key equals the value the stack previously carried for that key is holding
// the stack's own propagated tag, and one holding anything else is holding the caller's. That
// needs no new bookkeeping and cannot delete a key the stack never propagated.
//
// The one case it cannot distinguish is a caller who set a resource tag to the *same* value the
// stack propagates: removing the stack tag then removes theirs too. `docs/services.md` records
// that rather than papering over it, because the alternative — recording which keys the stamp
// wrote, per resource — is real bookkeeping in the event stream for an ambiguity AWS itself
// does not resolve (it publishes no per-resource provenance for a propagated tag either).
//
// # Propagation does not enforce the target's own tag quota (#1077)
//
// None of the four arms below checks the per-resource tag quota the owning service publishes, so a
// stack carrying enough tags can leave a resource over its own service's limit. **That is substrate's
// recorded reading of an unpublished case, not an oversight**, and #1000 filed it rather than deciding
// it. The search that establishes there is nothing to cite:
//
//   - `API_CreateStack` publishes exactly four errors — `AlreadyExists`, `InsufficientCapabilities`,
//     `LimitExceeded` and `TokenAlreadyExists`, all 400 — and none of them is about tags.
//     `LimitExceeded`'s own description scopes it to *"the quota for the resource … see CloudFormation
//     quotas"*, and that quotas page has **no row for tags at all**.
//   - CloudFormation's resource-tagging reference publishes only that *"[t]he propagation of
//     stack-level tags to resources, including tags with the `aws:` prefix, varies by resource type"*.
//     It says nothing about a target whose service caps tags below the stack's count.
//
// **What makes that decisive rather than merely open is that a refusal has no vocabulary.** To refuse,
// substrate must answer a published code, and there is none at either layer: CloudFormation publishes
// no tag error, and each service's own `TagLimitExceeded` / `TooManyTags` / `LimitExceeded` /
// `LimitExceededException` is published for *that service's own tagging operation*, not for a
// CloudFormation propagation. Borrowing one would be the analogy #671 forbids, and it would make
// substrate's deployer fail a template real CloudFormation deploys — a false failure in a consumer's
// test, which is the worst outcome this emulator can produce.
//
// The case is also narrower than it looks, for two published reasons. A caller cannot supply a
// reserved key — CloudFormation's `Tag` `Key` publishes *"can't be prefixed with `aws:`"* — and the
// three `aws:cloudformation:*` keys the stamp writes do not count toward a per-resource limit, which
// EC2's tag restrictions, ELBv2's, Classic ELB's and the general Tag Editor rule (*"a maximum of 50
// **user created** tags"*) all state. Since CloudFormation caps a stack at 50 tags and each of the four
// quotas substrate models is 50, a resource carrying no tags of its own can always take a full stack's
// worth. Reaching the overflow needs a resource tagged independently, through its own service or the
// tagging API.
//
// A quota mode would only reach one arm in any case: `mergeResourceTags` takes it, and only
// [cfnPropagateRecordStackTags] goes through that merge. [cfnPropagateEC2StackTags],
// [cfnPropagateELBStackTags] and [cfnPropagateConfigStackTags] each write through their own service's
// writer and never see it, so enforcing would mean four decisions rather than one parameter.

// cfnStackTagChanges decides what a stack's tags mean for one resource, given the tags the
// resource already carries and the stack's previous and next tag sets.
//
// Returns the keys to write and the keys to remove; the removals are sorted so a write is the
// same on every run. Both are empty for the common case of an unchanged tag set, which is what
// makes a redeploy of an untouched stack write nothing at all.
//
// The rules, in the order they are decided:
//
//   - A key the resource does not carry is written. Nothing can be clobbered.
//   - A key whose stored value equals the value the stack carried *before* this operation is
//     the stack's own, and is overwritten with the new value.
//   - Any other stored value is the caller's, and is left alone — including a value equal to
//     the one being propagated, where writing would be a no-op anyway.
//   - A key the stack carried before and does not carry now is removed, but only if the stored
//     value still matches what the stack propagated.
func cfnStackTagChanges(existing, prev, next map[string]string) (map[string]string, []string) {
	write := make(map[string]string)
	for key, value := range next {
		stored, held := existing[key]
		if !held {
			write[key] = value
			continue
		}
		if before, was := prev[key]; was && stored == before {
			if stored != value {
				write[key] = value
			}
		}
	}

	var remove []string
	for key, before := range prev {
		if _, still := next[key]; still {
			continue
		}
		if stored, held := existing[key]; held && stored == before {
			remove = append(remove, key)
		}
	}
	sort.Strings(remove)

	return write, remove
}

// cfnPropagateStackTags reconciles one resource's tags with the stack's, reporting whether the
// resource's tags could be reached at all.
//
// The four families are the four tag stores, exactly as in [cfnStampResourceTags]: ELBv2's
// ordered `[]ELBTag` found by ARN, AWS Config's ARN-keyed side-car whose whole document is the
// tag map, the services keyed by [cfnResolveStampTarget], and EC2's own prefix-keyed resolver for
// everything else. The CFN resource type decides which,
// rather than the physical ID: a type the type-keyed resolver claims is never also an EC2 one,
// and asking the ID first would let a bucket named `i-orders` be reconciled as an instance —
// which the stamp does do, and which is recorded in `docs/services.md` as a limit rather than
// reproduced here for symmetry's sake.
//
// A resource nothing can reach reports false and is skipped in silence, as the stamp is: the
// services that model no tags at all are the majority of what a stack creates.
func cfnPropagateStackTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, prev, next map[string]string,
) (bool, error) {
	if cfnELBStampableTypes[dr.Type] {
		return cfnPropagateELBStackTags(state, reqCtx, dr, prev, next)
	}
	if cfnConfigStampableTypes[dr.Type] {
		return cfnPropagateConfigStackTags(state, reqCtx, dr, prev, next)
	}
	if target, ok := cfnResolveStampTarget(dr, reqCtx.AccountID, reqCtx.Region); ok {
		return cfnPropagateRecordStackTags(state, target, dr, prev, next)
	}
	return cfnPropagateEC2StackTags(state, reqCtx, dr, prev, next)
}

// cfnPropagateRecordStackTags reconciles the tags on a record [cfnResolveStampTarget] resolves,
// whatever shape that record keeps its tags in.
//
// The existing set is read through [cfnRecordTags] rather than by unmarshaling the concrete
// type, so this arm needs no per-service case of its own: the merge back is
// [mergeResourceTags], which already has one for each of the four and is the writer the
// Resource Groups Tagging API uses, so a propagated tag and a `TagResources` call cannot end up
// merging differently.
func cfnPropagateRecordStackTags(
	state StateManager, target cfnStampTarget, dr DeployedResource, prev, next map[string]string,
) (bool, error) {
	existing, found, err := cfnRecordTags(state, target)
	if err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.PhysicalID, err)
	}
	if !found {
		return false, nil
	}

	write, remove := cfnStackTagChanges(existing, prev, next)
	if len(write) == 0 && len(remove) == 0 {
		return true, nil
	}
	// skipTagQuota: these are the caller's own stack tags, so a quota could legitimately refuse them —
	// but AWS publishes no outcome for that case and no code a refusal could carry, so substrate writes
	// them and records the resulting over-quota resource as a divergence. See this file's
	// "Propagation does not enforce the target's own tag quota" section for the decision and the
	// search behind it (#1077, decided; #1000 filed it).
	if err := mergeResourceTags(
		context.Background(), state, target.namespace, target.stateKey, write, remove, skipTagQuota,
	); err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.PhysicalID, err)
	}
	return true, nil
}

// cfnRecordTags reads the tags off a stored record, reporting false when no record is there.
//
// Both spellings of the member and both shapes of its contents are real, and decoding all four
// combinations rather than switching on the namespace keeps this from acquiring a second copy of
// [cfnResolveStampTarget]'s table, which is the copy that could drift. `S3Bucket` spells the
// member `"tags"` and Lambda's, SQS's and DynamoDB's records spell it `"Tags"`; ECS keeps a
// `[]ECSTag`, EFS a `[]EFSTag` and KMS a `[]KMSTag` where the rest keep a `map[string]string`
// (#819). Getting that third list shape wrong is not merely a missing read: a KMS key whose tags
// read back as `nil` looks to [cfnStackTagChanges] like a resource carrying none, so a caller's own
// tag of the same name is overwritten and a removed stack tag is never removed. A record
// carrying neither member — or one whose tags are in a shape neither decode reaches — reads as
// untagged, which is what an absent tag set is, rather than failing the whole reconciliation.
func cfnRecordTags(state StateManager, target cfnStampTarget) (map[string]string, bool, error) {
	raw, err := state.Get(context.Background(), target.namespace, target.stateKey)
	if err != nil {
		return nil, false, fmt.Errorf("get %s/%s: %w", target.namespace, target.stateKey, err)
	}
	if raw == nil {
		return nil, false, nil
	}

	var record struct {
		Lower json.RawMessage `json:"tags"`
		Upper json.RawMessage `json:"Tags"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, false, fmt.Errorf("unmarshal %s/%s: %w", target.namespace, target.stateKey, err)
	}
	member := record.Lower
	if len(member) == 0 {
		member = record.Upper
	}
	return cfnDecodeRecordTags(member), true, nil
}

// cfnDecodeRecordTags reads a record's tag member as a key/value map, in either of the two
// shapes substrate's records keep tags in.
//
// A shape neither decode reaches reads as untagged rather than as an error, for the reason
// [cfnRecordTags] gives: this feeds a reconciliation that must not fail a stack, and a record
// whose tags cannot be read has none this can reconcile against either way.
func cfnDecodeRecordTags(member json.RawMessage) map[string]string {
	if len(member) == 0 {
		return nil
	}
	var asMap map[string]string
	if err := json.Unmarshal(member, &asMap); err == nil {
		return asMap
	}

	// Three spellings, because ECS marshals `key`/`value`, EFS `Key`/`Value`, and KMS
	// `TagKey`/`TagValue` — which is AWS's own member naming for [KMSTag] and not substrate's
	// invention. One struct with six members rather than three decode attempts: the pairs that are
	// absent decode as empty.
	var asList []struct {
		LowerKey   string `json:"key"`
		LowerValue string `json:"value"`
		UpperKey   string `json:"Key"`
		UpperValue string `json:"Value"`
		TagKey     string `json:"TagKey"`
		TagValue   string `json:"TagValue"`
	}
	if err := json.Unmarshal(member, &asList); err != nil {
		return nil
	}
	out := make(map[string]string, len(asList))
	for _, tag := range asList {
		key, value := tag.UpperKey, tag.UpperValue
		if key == "" {
			key, value = tag.LowerKey, tag.LowerValue
		}
		if key == "" {
			key, value = tag.TagKey, tag.TagValue
		}
		if key != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cfnPropagateEC2StackTags reconciles the tags on an EC2 resource.
//
// [ec2ResourceTags] and [ec2ApplyTagsToResource] are the same pair the stamp uses, so the
// prefix switch over EC2's sixteen id shapes lives in one place. Two writes rather than one
// when a key is both written and removed, because the EC2 writer takes a direction rather than
// a pair of sets; the alternative is a third mode on a function every `CreateTags` goes
// through, for a case that only arises when a stack tag is replaced by a differently named one.
//
// No quota check here either, and not because the mode was forgotten: this arm does not go through
// [mergeResourceTags], so there is no mode to pass. [ec2CheckTagLimit] would have to be called directly.
// See the file's quota section for why none of the four arms checks (#1077).
func cfnPropagateEC2StackTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, prev, next map[string]string,
) (bool, error) {
	current, found, err := ec2ResourceTags(state, reqCtx, dr.PhysicalID)
	if err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.PhysicalID, err)
	}
	if !found {
		return false, nil
	}

	existing := make(map[string]string, len(current))
	for _, tag := range current {
		existing[tag.Key] = tag.Value
	}
	write, remove := cfnStackTagChanges(existing, prev, next)

	if len(write) > 0 {
		if err := ec2ApplyTagsToResource(
			state, reqCtx, dr.PhysicalID, cfnSortedEC2Tags(write), false,
		); err != nil {
			return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.PhysicalID, err)
		}
	}
	if len(remove) > 0 {
		gone := make([]EC2Tag, 0, len(remove))
		for _, key := range remove {
			gone = append(gone, EC2Tag{Key: key})
		}
		if err := ec2ApplyTagsToResource(state, reqCtx, dr.PhysicalID, gone, true); err != nil {
			return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.PhysicalID, err)
		}
	}
	return true, nil
}

// cfnPropagateELBStackTags reconciles the tags on one ELBv2 resource, found by its ARN.
//
// The ARN for the reason [cfnStampELBResource] gives: it is what ELBv2's resolver takes, and a
// load balancer's and a target group's physical ID is a name rather than an ARN.
func cfnPropagateELBStackTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, prev, next map[string]string,
) (bool, error) {
	if dr.ARN == "" {
		return false, nil
	}
	res, _, err := elbResolveTaggedResource(state, reqCtx.AccountID+"/"+reqCtx.Region, dr.ARN)
	if err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.ARN, err)
	}
	if res == nil {
		return false, nil
	}

	existing := make(map[string]string, len(res.tags))
	for _, tag := range res.tags {
		existing[tag.Key] = tag.Value
	}
	write, remove := cfnStackTagChanges(existing, prev, next)
	if len(write) == 0 && len(remove) == 0 {
		return true, nil
	}

	incoming := make([]ELBTag, 0, len(write))
	for _, tag := range cfnSortedEC2Tags(write) {
		incoming = append(incoming, ELBTag(tag))
	}
	updated, err := res.encode(elbRemoveTagKeys(elbMergeTags(res.tags, incoming), remove))
	if err != nil {
		return true, fmt.Errorf("stack tags %s %s: marshal: %w", dr.Type, dr.ARN, err)
	}
	if err := state.Put(context.Background(), elbNamespace, res.stateKey, updated); err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.ARN, err)
	}
	return true, nil
}

// cfnPropagateConfigStackTags reconciles the tags on one AWS Config resource, found by its ARN.
//
// The ARN for the reason [cfnStampConfigResource] gives: a Config tag is keyed by ARN, and neither
// a rule's nor a recorder's ARN is derivable from the physical ID the deployer records.
//
// This arm reads and writes the side-car directly rather than through [cfnRecordTags] and
// [mergeResourceTags], and both halves are load-bearing. On the read side the side-car has no tag
// *member* — the document is the map — so `cfnRecordTags` would find neither `tags` nor `Tags` and
// report the resource as carrying none. That is not a missing read but the KMS defect this file
// already records: an empty existing set makes [cfnStackTagChanges] overwrite a caller's own tag
// of the same name and never remove a withdrawn stack tag, so both of the reconciliation's
// safeguards fail together. On the write side the side-car goes through
// [cfgsvcSaveStateTags], the same writer Config's own `UntagResource` uses, so the rule that
// distinguishes an untagged resource from one holding `{}` has one implementation rather than a
// copy here that could drift — even though this path cannot reach the empty case while the
// three stamp keys are written before it runs.
func cfnPropagateConfigStackTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, prev, next map[string]string,
) (bool, error) {
	if dr.ARN == "" {
		return false, nil
	}
	arn, existing, found, err := cfgsvcResolveStampTags(
		state, reqCtx.AccountID, reqCtx.Region, dr.ARN,
	)
	if err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.ARN, err)
	}
	if !found {
		return false, nil
	}

	write, remove := cfnStackTagChanges(existing, prev, next)
	if len(write) == 0 && len(remove) == 0 {
		return true, nil
	}
	for key, value := range write {
		existing[key] = value
	}
	for _, key := range remove {
		delete(existing, key)
	}
	if err := cfgsvcSaveStateTags(context.Background(), state, arn, existing); err != nil {
		return true, fmt.Errorf("stack tags %s %s: %w", dr.Type, dr.ARN, err)
	}
	return true, nil
}

// cfnSortedEC2Tags renders a tag map as the ordered pair list the EC2 and ELBv2 writers take,
// sorted by key so a resource's stored tag order is the same on every run — the property
// [cfnStackResourceTags] fixes its own order for.
func cfnSortedEC2Tags(tags map[string]string) []EC2Tag {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]EC2Tag, 0, len(keys))
	for _, key := range keys {
		out = append(out, EC2Tag{Key: key, Value: tags[key]})
	}
	return out
}
