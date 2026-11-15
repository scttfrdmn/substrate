package emulator

// cfn_resources_v129_iam.go holds the StackDeployer helpers v0.129.0 added for IAM: an
// AWS::IAM::Policy deployed as the inline policies its page describes (#1562), an
// AWS::IAM::ManagedPolicy deployed and updated through the managed-policy API (#1560),
// AWS::IAM::User and AWS::IAM::Group deployed at all, and an AWS::IAM::Role that honors
// ManagedPolicyArns, Policies, Tags and PermissionsBoundary and updates in place (#1545). The name
// records the release rather than the service, as the other cfn_resources_v*.go files do.
//
// **What a resource remembers.** Each of these types records, in its DeployedResource.Metadata
// under [cfnIAMRecordKey], what it attached, embedded and joined — what is *in place*, written as
// each call succeeds, so a failure part-way still leaves an accurate record. A delete undoes
// exactly what the record lists: the template's declarations, not whatever else the entity has
// acquired. That is the conservative reading: a policy someone attached to a stack's role outside
// the stack is not the stack's to detach, and leaving it makes DeleteRole answer DeleteConflict,
// which is AWS's own answer for it. A record written before v0.129.0 holds no such entry, which
// reads as "nothing attached" — exactly what those deploys attached.
//
// **An update diffs against what is in place, not against the previous deployment.** In an
// ordinary update the two are the same. In the rollback of a failed update they are not: the
// rollback re-applies the previous declaration, and the previous deployment's record does not know
// what the failed update attached, tagged or embedded. The stack's persisted record does — it is
// the failed update's, until the rollback finishes — so [StackDeployer.liveIAMRecord] reads it,
// and the rollback removes what the failed update added.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// cfnIAMRecordKey is the Metadata key an IAM resource's [cfnIAMRecord] is stored under. The
// colon keeps it out of the attribute namespace: no published Fn::GetAtt attribute contains one.
const cfnIAMRecordKey = "substrate:iam"

// cfnIAMRecord is what an IAM resource has in place, so its update and its delete know what to
// change and undo.
type cfnIAMRecord struct {
	// Embedded lists, for an AWS::IAM::Policy, each principal the inline policy is embedded in and
	// the name it is embedded under. It is per principal, and carries the name, so a rename that
	// fails part-way still records every copy — under the old name and the new one — until the
	// cleanup has removed it.
	Embedded []cfnIAMEmbedding `json:"Embedded,omitempty"`

	// Roles, Users and Groups are the principals an AWS::IAM::ManagedPolicy is attached to. For
	// an AWS::IAM::User, Groups is the groups it joined.
	Roles  []string `json:"Roles,omitempty"`
	Users  []string `json:"Users,omitempty"`
	Groups []string `json:"Groups,omitempty"`

	// ManagedPolicyArns and InlinePolicies are the managed policies an entity attached and the
	// names of the inline policies it embedded.
	ManagedPolicyArns []string `json:"ManagedPolicyArns,omitempty"`
	InlinePolicies    []string `json:"InlinePolicies,omitempty"`

	// TagKeys are the tag keys the template declared, so an update can remove one it drops.
	TagKeys []string `json:"TagKeys,omitempty"`

	// PermissionsBoundary is the boundary ARN the template declared.
	PermissionsBoundary string `json:"PermissionsBoundary,omitempty"`
}

// cfnIAMEmbedding is one copy of an inline policy: the principal it is embedded in and its name
// there.
type cfnIAMEmbedding struct {
	Kind       string `json:"Kind"`
	Name       string `json:"Name"`
	PolicyName string `json:"PolicyName"`
}

// cfnIAMRecordOf reads a resource's record, or the zero record for a resource deployed before
// v0.129.0 or by a deploy that recorded none.
func cfnIAMRecordOf(dr DeployedResource) cfnIAMRecord {
	var rec cfnIAMRecord
	raw, ok := dr.Metadata[cfnIAMRecordKey].(string)
	if !ok || raw == "" {
		return rec
	}
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return cfnIAMRecord{}
	}
	return rec
}

// setCFNIAMRecord stores rec on dr.
func setCFNIAMRecord(dr *DeployedResource, rec cfnIAMRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("cfn: marshal IAM record: %w", err)
	}
	if dr.Metadata == nil {
		dr.Metadata = map[string]interface{}{}
	}
	dr.Metadata[cfnIAMRecordKey] = string(raw)
	return nil
}

// liveIAMRecord returns the record of what is in place on prior's resource: the stack's persisted
// record of the same resource when it holds one, and prior's own otherwise.
//
// The persisted record is the most recent deployment's. In an ordinary update that is prior; in
// the rollback of a failed update it is the failed update's, which is what the failed update left
// in place — and so what the rollback has to diff against to remove what that update added. The
// same resource means the same logical ID, type and physical ID: a resource the failed update
// replaced has a new physical ID, and its replacement is deleted by the rollback's own sweep.
func (d *StackDeployer) liveIAMRecord(ctx context.Context, cctx *cfnContext, prior DeployedResource) cfnIAMRecord {
	if d.state != nil && cctx != nil && cctx.stackName != "" {
		if stack, err := d.loadStack(ctx, cctx.stackName); err == nil && stack != nil {
			for _, r := range stack.Resources {
				if r.LogicalID != prior.LogicalID || r.Type != prior.Type || r.PhysicalID != prior.PhysicalID {
					continue
				}
				if _, ok := r.Metadata[cfnIAMRecordKey]; ok {
					return cfnIAMRecordOf(r)
				}
			}
		}
	}
	return cfnIAMRecordOf(prior)
}

// cfnIAMEntity names an IAM user, group or role: Kind is "User", "Group" or "Role", which is also
// how the IAM API spells each type's operations (PutRolePolicy, AttachUserPolicy, …) and its name
// member (RoleName, UserName, GroupName).
type cfnIAMEntity struct {
	Kind string
	Name string
}

// body returns a request body naming the entity, with extra members added.
func (e cfnIAMEntity) body(extra map[string]string) map[string]string {
	b := map[string]string{e.Kind + "Name": e.Name}
	for k, v := range extra {
		b[k] = v
	}
	return b
}

// cfnInlinePolicy is one member of a Policies property.
type cfnInlinePolicy struct {
	Name     string
	Document string
}

// cfnInlinePolicies resolves an AWS::IAM::Role, User or Group Policies property — a list of
// {PolicyName, PolicyDocument} — in declaration order. A member with no name carries nothing an
// IAM call could store and is skipped.
func cfnInlinePolicies(v interface{}, cctx *cfnContext) []cfnInlinePolicy {
	list, _ := v.([]interface{})
	out := make([]cfnInlinePolicy, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name := resolveValue(m["PolicyName"], cctx)
		if name == "" {
			continue
		}
		out = append(out, cfnInlinePolicy{Name: name, Document: cfnJSONDocument(m["PolicyDocument"], cctx)})
	}
	return out
}

// cfnTagMap resolves a Tags property — a list of {Key, Value} — to a map. A keyless entry is
// skipped, as [cfnELBClassicTagParams] skips one.
func cfnTagMap(v interface{}, cctx *cfnContext) map[string]string {
	list, _ := v.([]interface{})
	out := make(map[string]string, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		key := resolveValue(m["Key"], cctx)
		if key == "" {
			continue
		}
		out[key] = resolveValue(m["Value"], cctx)
	}
	return out
}

// cfnIAMTagParams renders tags as the Tags.member.N query list CreateUser, CreateRole, TagUser and
// TagRole take.
func cfnIAMTagParams(tags map[string]string) map[string]string {
	params := make(map[string]string, len(tags)*2)
	for i, k := range sortedStringKeys(tags) {
		prefix := "Tags.member." + strconv.Itoa(i+1) + "."
		params[prefix+"Key"] = k
		params[prefix+"Value"] = tags[k]
	}
	return params
}

// iamCall dispatches one IAM operation with a JSON body and optional query parameters.
func (d *StackDeployer) iamCall(
	ctx context.Context, op string, body map[string]string, params map[string]string, streamID string,
) (*AWSResponse, float64, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("cfn: marshal %s body: %w", op, err)
	}
	if params == nil {
		params = map[string]string{}
	}
	return d.dispatch(ctx, &AWSRequest{
		Service: "iam", Operation: op, Body: data, Headers: map[string]string{}, Params: params,
	}, streamID)
}

// iamCallTolerant is iamCall for an undo step: an entity or attachment that is already gone is
// success, the reading [cfnDeleteIsAbsent] gives a stack delete.
func (d *StackDeployer) iamCallTolerant(
	ctx context.Context, op string, body map[string]string, streamID string,
) (float64, error) {
	_, cost, err := d.iamCall(ctx, op, body, nil, streamID)
	if err != nil && !cfnDeleteIsAbsent(err) {
		return cost, err
	}
	return cost, nil
}

// cfnStringSetMinus returns the members of a not in b, in a's order.
func cfnStringSetMinus(a, b []string) []string {
	var out []string
	for _, s := range a {
		if !slices.Contains(b, s) {
			out = append(out, s)
		}
	}
	return out
}

// syncIAMEntityPolicies brings an entity's attached managed policies and embedded inline policies
// from what rec records to managed and inline, updating rec as each call succeeds so that on a
// failure rec lists exactly what is in place. New attachments and inline policies are applied
// before the removed ones are taken away, so the entity is never without a permission both the
// old and the new declaration grant.
//
// An inline policy rec already lists is put again only when rePut is set — when the Policies
// property changed, which is the only way its document can have — so an update that changed
// something else makes no Put call for it.
func (d *StackDeployer) syncIAMEntityPolicies(
	ctx context.Context, e cfnIAMEntity, rec *cfnIAMRecord,
	managed []string, inline []cfnInlinePolicy, rePut bool, streamID string,
) (float64, error) {
	var total float64
	for _, arn := range cfnStringSetMinus(managed, rec.ManagedPolicyArns) {
		_, cost, err := d.iamCall(ctx, "Attach"+e.Kind+"Policy", e.body(map[string]string{"PolicyArn": arn}), nil, streamID)
		total += cost
		if err != nil {
			return total, err
		}
		rec.ManagedPolicyArns = append(rec.ManagedPolicyArns, arn)
	}
	names := make([]string, 0, len(inline))
	for _, pol := range inline {
		names = append(names, pol.Name)
		if !rePut && slices.Contains(rec.InlinePolicies, pol.Name) {
			continue
		}
		// Put is an upsert, so a policy whose document changed is replaced by the same call that
		// adds a new one.
		_, cost, err := d.iamCall(ctx, "Put"+e.Kind+"Policy", e.body(map[string]string{
			"PolicyName": pol.Name, "PolicyDocument": pol.Document,
		}), nil, streamID)
		total += cost
		if err != nil {
			return total, err
		}
		if !slices.Contains(rec.InlinePolicies, pol.Name) {
			rec.InlinePolicies = append(rec.InlinePolicies, pol.Name)
		}
	}
	for _, arn := range cfnStringSetMinus(rec.ManagedPolicyArns, managed) {
		cost, err := d.iamCallTolerant(ctx, "Detach"+e.Kind+"Policy", e.body(map[string]string{"PolicyArn": arn}), streamID)
		total += cost
		if err != nil {
			return total, err
		}
		rec.ManagedPolicyArns = slices.DeleteFunc(rec.ManagedPolicyArns, func(s string) bool { return s == arn })
	}
	for _, name := range cfnStringSetMinus(rec.InlinePolicies, names) {
		cost, err := d.iamCallTolerant(ctx, "Delete"+e.Kind+"Policy", e.body(map[string]string{"PolicyName": name}), streamID)
		total += cost
		if err != nil {
			return total, err
		}
		rec.InlinePolicies = slices.DeleteFunc(rec.InlinePolicies, func(s string) bool { return s == name })
	}
	return total, nil
}

// cfnTagKeysRemoved returns the recorded keys the declaration no longer holds. A user's and a
// role's tag keys are not case sensitive — "you cannot have separate Department and department tag
// keys. If you have tagged a user with the Department=finance tag and you add the department=hr
// tag, it replaces the first tag" (IAM User Guide, Tags for IAM resources) — so a key whose case
// alone changed is the same key, replaced by the TagUser or TagRole that applies the declaration,
// and must not be untagged.
func cfnTagKeysRemoved(recorded []string, tags map[string]string) []string {
	var out []string
	for _, k := range recorded {
		kept := false
		for declared := range tags {
			if strings.EqualFold(k, declared) {
				kept = true
				break
			}
		}
		if !kept {
			out = append(out, k)
		}
	}
	return out
}

// syncIAMEntityTags brings a user's or role's tags from the keys rec records to tags. When apply is
// set — the Tags property changed — every declared tag is written through TagUser or TagRole.
// Either way a recorded key the declaration no longer holds is removed through UntagUser or
// UntagRole: in a rollback that is a tag the failed update added, whether or not the rollback
// counts Tags as changed. A group has no tags in CloudFormation or in IAM, so it is never passed
// here.
func (d *StackDeployer) syncIAMEntityTags(
	ctx context.Context, e cfnIAMEntity, rec *cfnIAMRecord, tags map[string]string, apply bool, streamID string,
) (float64, error) {
	var total float64
	if apply && len(tags) > 0 {
		_, cost, err := d.iamCall(ctx, "Tag"+e.Kind, e.body(nil), cfnIAMTagParams(tags), streamID)
		total += cost
		if err != nil {
			return total, err
		}
	}
	removed := cfnTagKeysRemoved(rec.TagKeys, tags)
	if len(removed) > 0 {
		params := make(map[string]string, len(removed))
		for i, k := range removed {
			params["TagKeys.member."+strconv.Itoa(i+1)] = k
		}
		_, cost, err := d.iamCall(ctx, "Untag"+e.Kind, e.body(nil), params, streamID)
		total += cost
		if err != nil {
			return total, err
		}
	}
	if apply {
		rec.TagKeys = sortedStringKeys(tags)
	} else {
		rec.TagKeys = cfnStringSetMinus(rec.TagKeys, removed)
	}
	return total, nil
}

// syncIAMEntityBoundary brings a user's or role's permissions boundary from the one rec records to
// boundary, through Put{User,Role}PermissionsBoundary or Delete{User,Role}PermissionsBoundary.
func (d *StackDeployer) syncIAMEntityBoundary(
	ctx context.Context, e cfnIAMEntity, rec *cfnIAMRecord, boundary, streamID string,
) (float64, error) {
	if boundary == rec.PermissionsBoundary {
		return 0, nil
	}
	if boundary == "" {
		cost, err := d.iamCallTolerant(ctx, "Delete"+e.Kind+"PermissionsBoundary", e.body(nil), streamID)
		if err == nil {
			rec.PermissionsBoundary = ""
		}
		return cost, err
	}
	_, cost, err := d.iamCall(ctx, "Put"+e.Kind+"PermissionsBoundary",
		e.body(map[string]string{"PermissionsBoundary": boundary}), nil, streamID)
	if err == nil {
		rec.PermissionsBoundary = boundary
	}
	return cost, err
}

// syncIAMUserGroups brings a user's group memberships from the groups rec records to groups.
func (d *StackDeployer) syncIAMUserGroups(
	ctx context.Context, user string, rec *cfnIAMRecord, groups []string, streamID string,
) (float64, error) {
	var total float64
	for _, g := range cfnStringSetMinus(groups, rec.Groups) {
		_, cost, err := d.iamCall(ctx, "AddUserToGroup", map[string]string{"UserName": user, "GroupName": g}, nil, streamID)
		total += cost
		if err != nil {
			return total, err
		}
		rec.Groups = append(rec.Groups, g)
	}
	for _, g := range cfnStringSetMinus(rec.Groups, groups) {
		cost, err := d.iamCallTolerant(ctx, "RemoveUserFromGroup", map[string]string{"UserName": user, "GroupName": g}, streamID)
		total += cost
		if err != nil {
			return total, err
		}
		rec.Groups = slices.DeleteFunc(rec.Groups, func(s string) bool { return s == g })
	}
	return total, nil
}

// iamLivePath reads a user's or group's current path through GetUser or GetGroup.
func (d *StackDeployer) iamLivePath(ctx context.Context, e cfnIAMEntity, streamID string) (string, float64, error) {
	resp, cost, err := d.iamCall(ctx, "Get"+e.Kind, e.body(nil), nil, streamID)
	if err != nil || resp == nil {
		return "", cost, err
	}
	var out struct {
		UserPath  string `xml:"GetUserResult>User>Path"`
		GroupPath string `xml:"GetGroupResult>Group>Path"`
	}
	if err := xmlUnmarshalIAM(resp.Body, &out); err != nil {
		return "", cost, fmt.Errorf("cfn: read the path of %s %s: %w", e.Kind, e.Name, err)
	}
	if e.Kind == "User" {
		return out.UserPath, cost, nil
	}
	return out.GroupPath, cost, nil
}

// cfnIAMPathRefusal reports, for a user or group whose Path the planner counts as changed, the
// refusal a real change earns: IAM changes the path only through UpdateUser or UpdateGroup, which
// substrate does not route (#1582). The live path is read first, so a Path whose value is what
// the entity already has — a rollback re-applying the previous declaration counts every declared
// property as changed — is not refused.
func (d *StackDeployer) cfnIAMPathRefusal(
	ctx context.Context, e cfnIAMEntity, props map[string]interface{}, changed []string,
	streamID string, cctx *cfnContext,
) (string, float64) {
	if !slices.Contains(changed, "Path") {
		return "", 0
	}
	live, cost, err := d.iamLivePath(ctx, e, streamID)
	if err != nil {
		return err.Error(), cost
	}
	if live == resolveStringProp(props, "Path", "/", cctx) {
		return "", cost
	}
	return fmt.Sprintf("updating Path of AWS::IAM::%s is not modeled: IAM changes a %s's path "+
		"through Update%s, which substrate does not route", e.Kind, strings.ToLower(e.Kind), e.Kind), cost
}

// deleteIAMEntity undoes what rec records on the entity and deletes it: group memberships, then
// attached managed policies, then inline policies, then the entity — the order DeleteUser,
// DeleteGroup and DeleteRole require their subordinates be removed in. Each step tolerates an
// absence.
func (d *StackDeployer) deleteIAMEntity(ctx context.Context, e cfnIAMEntity, rec cfnIAMRecord, streamID string) error {
	if e.Kind == "User" {
		for _, g := range rec.Groups {
			if _, err := d.iamCallTolerant(ctx, "RemoveUserFromGroup",
				map[string]string{"UserName": e.Name, "GroupName": g}, streamID); err != nil {
				return err
			}
		}
	}
	for _, arn := range rec.ManagedPolicyArns {
		if _, err := d.iamCallTolerant(ctx, "Detach"+e.Kind+"Policy",
			e.body(map[string]string{"PolicyArn": arn}), streamID); err != nil {
			return err
		}
	}
	for _, name := range rec.InlinePolicies {
		if _, err := d.iamCallTolerant(ctx, "Delete"+e.Kind+"Policy",
			e.body(map[string]string{"PolicyName": name}), streamID); err != nil {
			return err
		}
	}
	_, err := d.iamCallTolerant(ctx, "Delete"+e.Kind, e.body(nil), streamID)
	return err
}

// cfnIAMDeleteFailure turns a sequenced delete's error into the sweep's failure record.
func cfnIAMDeleteFailure(dr DeployedResource, err error) *CFNResourceDeletion {
	if err == nil {
		return nil
	}
	return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
}

// cfnDeleteIAMEntityFor returns the sequenced deleter for an IAM user, group or role: undo what the
// resource's record lists, then delete the entity named by its physical ID.
func cfnDeleteIAMEntityFor(kind string) func(context.Context, *StackDeployer, DeployedResource, map[string]interface{}, *cfnContext, string) *CFNResourceDeletion {
	return func(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext, streamID string) *CFNResourceDeletion {
		return cfnIAMDeleteFailure(dr, d.deleteIAMEntity(ctx, cfnIAMEntity{Kind: kind, Name: dr.PhysicalID},
			cfnIAMRecordOf(dr), streamID))
	}
}

// cfnIAMUpdated finishes an in-place update: the record of what is now in place goes on dr, and err
// — the first call that failed, if one did — becomes the resource's failure.
func cfnIAMUpdated(dr DeployedResource, rec cfnIAMRecord, total float64, err error) (DeployedResource, float64, error) {
	dr.Error = ""
	if err != nil {
		dr.Error = err.Error()
	}
	if recErr := setCFNIAMRecord(&dr, rec); recErr != nil {
		return DeployedResource{}, total, recErr
	}
	return dr, total, nil
}

// ----- AWS::IAM::Role (#1545) ------------------------------------------------------

// applyIAMRoleExtras applies what CreateRole cannot carry — PermissionsBoundary, ManagedPolicyArns
// and Policies — to a role CreateRole has just made, recording each on rec. A failure deletes the
// role again, undoing whatever rec lists: a role that failed is skipped by the stack's sweep (its
// create did not succeed), so anything left here would outlive the stack.
func (d *StackDeployer) applyIAMRoleExtras(
	ctx context.Context, dr *DeployedResource, props map[string]interface{}, rec *cfnIAMRecord,
	streamID string, cctx *cfnContext,
) float64 {
	e := cfnIAMEntity{Kind: "Role", Name: dr.PhysicalID}
	var total float64
	cost, err := d.syncIAMEntityBoundary(ctx, e, rec, resolveStringProp(props, "PermissionsBoundary", "", cctx), streamID)
	total += cost
	if err == nil {
		cost, err = d.syncIAMEntityPolicies(ctx, e, rec,
			resolveStringList(props["ManagedPolicyArns"], cctx), cfnInlinePolicies(props["Policies"], cctx), true, streamID)
		total += cost
	}
	if err != nil {
		dr.Error = err.Error()
		if undoErr := d.deleteIAMEntity(ctx, e, *rec, streamID); undoErr != nil {
			d.logger.Warn("cfn: could not remove a role whose create failed",
				"logical_id", dr.LogicalID, "role", e.Name, "error", undoErr)
		}
	}
	return total
}

// updateIAMRole applies a changed AWS::IAM::Role in place (#1545). RoleName and Path are
// "Update requires: Replacement" (cfnRecreationProperties); every other property is No
// interruption on the page, and each goes through the operation that changes it:
// AssumeRolePolicyDocument through UpdateAssumeRolePolicy, Description and MaxSessionDuration
// through UpdateRole, ManagedPolicyArns through Attach/DetachRolePolicy, Policies through
// Put/DeleteRolePolicy, Tags through TagRole/UntagRole and PermissionsBoundary through
// Put/DeleteRolePermissionsBoundary. The role keeps its name, so it keeps its physical ID. The
// attachments, inline policies, tags and boundary are diffed against what is in place
// ([StackDeployer.liveIAMRecord]), so a rollback removes what the failed update added.
func (d *StackDeployer) updateIAMRole(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	e := cfnIAMEntity{Kind: "Role", Name: prior.PhysicalID}
	rec := d.liveIAMRecord(ctx, cctx, prior)
	var total float64

	steps := []func() (float64, error){
		func() (float64, error) {
			if !slices.Contains(changed, "AssumeRolePolicyDocument") {
				return 0, nil
			}
			_, cost, err := d.iamCall(ctx, "UpdateAssumeRolePolicy", e.body(map[string]string{
				"PolicyDocument": cfnJSONDocument(props["AssumeRolePolicyDocument"], cctx),
			}), nil, streamID)
			return cost, err
		},
		func() (float64, error) {
			if !slices.Contains(changed, "Description") && !slices.Contains(changed, "MaxSessionDuration") {
				return 0, nil
			}
			// Both members are sent, so a property the new declaration dropped returns to its
			// default: an empty description, and the one-hour session CreateRole applies when none
			// is given.
			_, cost, err := d.iamCall(ctx, "UpdateRole", e.body(map[string]string{
				"Description":        resolveStringProp(props, "Description", "", cctx),
				"MaxSessionDuration": resolveStringProp(props, "MaxSessionDuration", strconv.Itoa(iamMinSessionDuration), cctx),
			}), nil, streamID)
			return cost, err
		},
		func() (float64, error) {
			return d.syncIAMEntityPolicies(ctx, e, &rec, resolveStringList(props["ManagedPolicyArns"], cctx),
				cfnInlinePolicies(props["Policies"], cctx), slices.Contains(changed, "Policies"), streamID)
		},
		func() (float64, error) {
			return d.syncIAMEntityTags(ctx, e, &rec, cfnTagMap(props["Tags"], cctx), slices.Contains(changed, "Tags"), streamID)
		},
		func() (float64, error) {
			return d.syncIAMEntityBoundary(ctx, e, &rec, resolveStringProp(props, "PermissionsBoundary", "", cctx), streamID)
		},
	}
	for _, step := range steps {
		cost, err := step()
		total += cost
		if err != nil {
			return cfnIAMUpdated(dr, rec, total, err)
		}
	}
	return cfnIAMUpdated(dr, rec, total, nil)
}

// ----- AWS::IAM::User and AWS::IAM::Group ----------------------------------------------

// deployIAMUser creates an AWS::IAM::User through CreateUser, then applies its
// PermissionsBoundary, Groups, ManagedPolicyArns and Policies. Until v0.129.0 the type had no
// deploy arm and reached the generic stub, so no user existed.
//
// "Ref returns the UserName", which is the physical ID, and "Arn" is CreateUser's own response
// member. A UserName the template omits is generated, as "CloudFormation generates a unique
// physical ID and uses that ID for the user name". LoginProfile is not applied: substrate routes
// no CreateLoginProfile (#1583); the declaration is logged rather than failing the user.
func (d *StackDeployer) deployIAMUser(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "UserName", cfnGeneratedName(cctx, "AWS::IAM::User", logicalID), cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::User", PhysicalID: name}
	tags := cfnTagMap(props["Tags"], cctx)
	resp, total, err := d.iamCall(ctx, "CreateUser", map[string]string{
		"UserName": name, "Path": resolveStringProp(props, "Path", "/", cctx),
	}, cfnIAMTagParams(tags), streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var result struct {
		ARN string `xml:"CreateUserResult>User>Arn"`
	}
	if resp != nil && xmlUnmarshalIAM(resp.Body, &result) == nil {
		dr.ARN = result.ARN
	}
	if _, ok := props["LoginProfile"]; ok {
		d.logger.Warn("cfn: AWS::IAM::User LoginProfile is not applied; CreateLoginProfile is not routed",
			"logical_id", logicalID)
	}

	rec := cfnIAMRecord{TagKeys: sortedStringKeys(tags)}
	e := cfnIAMEntity{Kind: "User", Name: name}
	cost, err := d.syncIAMEntityBoundary(ctx, e, &rec, resolveStringProp(props, "PermissionsBoundary", "", cctx), streamID)
	total += cost
	if err == nil {
		cost, err = d.syncIAMUserGroups(ctx, name, &rec, resolveStringList(props["Groups"], cctx), streamID)
		total += cost
	}
	if err == nil {
		cost, err = d.syncIAMEntityPolicies(ctx, e, &rec,
			resolveStringList(props["ManagedPolicyArns"], cctx), cfnInlinePolicies(props["Policies"], cctx), true, streamID)
		total += cost
	}
	if err != nil {
		dr.Error = err.Error()
		if undoErr := d.deleteIAMEntity(ctx, e, rec, streamID); undoErr != nil {
			d.logger.Warn("cfn: could not remove a user whose create failed",
				"logical_id", logicalID, "user", name, "error", undoErr)
		}
		return dr, total, nil
	}
	if err := setCFNIAMRecord(&dr, rec); err != nil {
		return DeployedResource{}, total, err
	}
	return dr, total, nil
}

// updateIAMUser applies a changed AWS::IAM::User in place. UserName is Replacement; Groups,
// ManagedPolicyArns, Policies, Tags and PermissionsBoundary go through the operations that change
// them, diffed against what is in place. Path is No interruption on the page but changes only
// through UpdateUser, which substrate does not route, so a Path that really differs from the
// user's fails the user with a reason saying so rather than replacing a user AWS would have kept;
// a changed LoginProfile is logged and not applied, as on create.
func (d *StackDeployer) updateIAMUser(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	e := cfnIAMEntity{Kind: "User", Name: prior.PhysicalID}
	refusal, total := d.cfnIAMPathRefusal(ctx, e, props, changed, streamID, cctx)
	if refusal != "" {
		dr.Error = refusal
		return dr, total, nil
	}
	if slices.Contains(changed, "LoginProfile") {
		d.logger.Warn("cfn: AWS::IAM::User LoginProfile is not applied; CreateLoginProfile is not routed",
			"logical_id", logicalID)
	}
	rec := d.liveIAMRecord(ctx, cctx, prior)
	steps := []func() (float64, error){
		func() (float64, error) {
			return d.syncIAMUserGroups(ctx, e.Name, &rec, resolveStringList(props["Groups"], cctx), streamID)
		},
		func() (float64, error) {
			return d.syncIAMEntityPolicies(ctx, e, &rec, resolveStringList(props["ManagedPolicyArns"], cctx),
				cfnInlinePolicies(props["Policies"], cctx), slices.Contains(changed, "Policies"), streamID)
		},
		func() (float64, error) {
			return d.syncIAMEntityTags(ctx, e, &rec, cfnTagMap(props["Tags"], cctx), slices.Contains(changed, "Tags"), streamID)
		},
		func() (float64, error) {
			return d.syncIAMEntityBoundary(ctx, e, &rec, resolveStringProp(props, "PermissionsBoundary", "", cctx), streamID)
		},
	}
	for _, step := range steps {
		cost, err := step()
		total += cost
		if err != nil {
			return cfnIAMUpdated(dr, rec, total, err)
		}
	}
	return cfnIAMUpdated(dr, rec, total, nil)
}

// deployIAMGroup creates an AWS::IAM::Group through CreateGroup, then applies its
// ManagedPolicyArns and Policies. Until v0.129.0 the type had no deploy arm.
//
// "Ref returns the GroupName" and "Arn" is CreateGroup's own response member; a GroupName the
// template omits is generated. The type declares no Tags, and IAM's CreateGroup takes none.
func (d *StackDeployer) deployIAMGroup(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "GroupName", cfnGeneratedName(cctx, "AWS::IAM::Group", logicalID), cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::Group", PhysicalID: name}
	resp, total, err := d.iamCall(ctx, "CreateGroup", map[string]string{
		"GroupName": name, "Path": resolveStringProp(props, "Path", "/", cctx),
	}, nil, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var result struct {
		ARN string `xml:"CreateGroupResult>Group>Arn"`
	}
	if resp != nil && xmlUnmarshalIAM(resp.Body, &result) == nil {
		dr.ARN = result.ARN
	}
	rec := cfnIAMRecord{}
	e := cfnIAMEntity{Kind: "Group", Name: name}
	cost, err := d.syncIAMEntityPolicies(ctx, e, &rec,
		resolveStringList(props["ManagedPolicyArns"], cctx), cfnInlinePolicies(props["Policies"], cctx), true, streamID)
	total += cost
	if err != nil {
		dr.Error = err.Error()
		if undoErr := d.deleteIAMEntity(ctx, e, rec, streamID); undoErr != nil {
			d.logger.Warn("cfn: could not remove a group whose create failed",
				"logical_id", logicalID, "group", name, "error", undoErr)
		}
		return dr, total, nil
	}
	if err := setCFNIAMRecord(&dr, rec); err != nil {
		return DeployedResource{}, total, err
	}
	return dr, total, nil
}

// updateIAMGroup applies a changed AWS::IAM::Group in place. GroupName is Replacement;
// ManagedPolicyArns and Policies go through Attach/DetachGroupPolicy and Put/DeleteGroupPolicy,
// diffed against what is in place. Path is No interruption but changes only through UpdateGroup,
// which substrate does not route, so a Path that really differs from the group's fails the group
// with a reason saying so.
func (d *StackDeployer) updateIAMGroup(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	e := cfnIAMEntity{Kind: "Group", Name: prior.PhysicalID}
	refusal, total := d.cfnIAMPathRefusal(ctx, e, props, changed, streamID, cctx)
	if refusal != "" {
		dr.Error = refusal
		return dr, total, nil
	}
	rec := d.liveIAMRecord(ctx, cctx, prior)
	cost, err := d.syncIAMEntityPolicies(ctx, e, &rec, resolveStringList(props["ManagedPolicyArns"], cctx),
		cfnInlinePolicies(props["Policies"], cctx), slices.Contains(changed, "Policies"), streamID)
	return cfnIAMUpdated(dr, rec, total+cost, err)
}

// ----- AWS::IAM::Policy (#1562) --------------------------------------------------------

// cfnIAMPolicyNoPrincipal is AWS::IAM::Policy's own statement of the rule a principal-less
// declaration breaks, used as the resource's failure reason.
const cfnIAMPolicyNoPrincipal = "The Groups, Roles, and Users properties are optional. " +
	"However, you must specify at least one of these properties."

// cfnPolicyPrincipals resolves an AWS::IAM::Policy's or AWS::IAM::ManagedPolicy's Roles, Users and
// Groups as the entities they name.
func cfnPolicyPrincipals(props map[string]interface{}, cctx *cfnContext) []cfnIAMEntity {
	var out []cfnIAMEntity
	for _, kind := range []string{"Role", "User", "Group"} {
		for _, name := range resolveStringList(props[kind+"s"], cctx) {
			out = append(out, cfnIAMEntity{Kind: kind, Name: name})
		}
	}
	return out
}

// cfnRecordPrincipals returns the principals rec lists for a managed policy.
func cfnRecordPrincipals(rec cfnIAMRecord) []cfnIAMEntity {
	var out []cfnIAMEntity
	for _, n := range rec.Roles {
		out = append(out, cfnIAMEntity{Kind: "Role", Name: n})
	}
	for _, n := range rec.Users {
		out = append(out, cfnIAMEntity{Kind: "User", Name: n})
	}
	for _, n := range rec.Groups {
		out = append(out, cfnIAMEntity{Kind: "Group", Name: n})
	}
	return out
}

// addRecordPrincipal records e on rec as a managed policy's principal.
func addRecordPrincipal(rec *cfnIAMRecord, e cfnIAMEntity) {
	switch e.Kind {
	case "Role":
		rec.Roles = append(rec.Roles, e.Name)
	case "User":
		rec.Users = append(rec.Users, e.Name)
	case "Group":
		rec.Groups = append(rec.Groups, e.Name)
	}
}

// entity returns the principal an embedding names.
func (m cfnIAMEmbedding) entity() cfnIAMEntity { return cfnIAMEntity{Kind: m.Kind, Name: m.Name} }

// deleteEmbedding removes one copy of an inline policy, tolerating an absence.
func (d *StackDeployer) deleteEmbedding(ctx context.Context, m cfnIAMEmbedding, streamID string) (float64, error) {
	return d.iamCallTolerant(ctx, "Delete"+m.Kind+"Policy",
		m.entity().body(map[string]string{"PolicyName": m.PolicyName}), streamID)
}

// putEmbeddings puts doc on each embedding in want that rec does not list — or on every one when
// rePut is set — adding each success to rec. It stops at the first failure.
func (d *StackDeployer) putEmbeddings(
	ctx context.Context, rec *cfnIAMRecord, want []cfnIAMEmbedding, doc string, rePut bool, streamID string,
) (float64, error) {
	var total float64
	for _, m := range want {
		listed := slices.Contains(rec.Embedded, m)
		if listed && !rePut {
			continue
		}
		_, cost, err := d.iamCall(ctx, "Put"+m.Kind+"Policy", m.entity().body(map[string]string{
			"PolicyName": m.PolicyName, "PolicyDocument": doc,
		}), nil, streamID)
		total += cost
		if err != nil {
			return total, err
		}
		if !listed {
			rec.Embedded = append(rec.Embedded, m)
		}
	}
	return total, nil
}

// cfnPolicyEmbeddings returns the copies an AWS::IAM::Policy declaration asks for.
func cfnPolicyEmbeddings(name string, principals []cfnIAMEntity) []cfnIAMEmbedding {
	out := make([]cfnIAMEmbedding, 0, len(principals))
	for _, e := range principals {
		out = append(out, cfnIAMEmbedding{Kind: e.Kind, Name: e.Name, PolicyName: name})
	}
	return out
}

// deployIAMPolicy deploys an AWS::IAM::Policy as what its page says it is (#1562): "Adds or
// updates an inline policy document that is embedded in the specified IAM group, user or role" —
// one Put{Role,User,Group}Policy per principal, under PolicyName. Until v0.129.0 it called
// CreatePolicy, which made an unattached customer managed policy and left every role the template
// granted permissions to without them; AWS::IAM::ManagedPolicy is the type for that.
//
// "The Groups, Roles, and Users properties are optional. However, you must specify at least one
// of these properties": a declaration naming none fails the resource with that sentence, and
// PolicyName and PolicyDocument are Required: Yes, so a missing one fails it too (the messages for
// those two are substrate's own).
//
// The physical ID is a generated one in the stack-logical-suffix shape cfnGeneratedName makes,
// because an inline policy has no identifier of its own for the stack to record — the page says
// "Ref returns the resource name" and publishes `Id`, "the stable and unique string identifying
// the policy", without a format for either, so both answer this ID (substrate's own format). A
// failure part-way removes the inline policies already embedded, since a resource that failed is
// skipped by the stack's sweep.
func (d *StackDeployer) deployIAMPolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::Policy",
		PhysicalID: cfnGeneratedName(cctx, "AWS::IAM::Policy", logicalID)}
	name := resolveStringProp(props, "PolicyName", "", cctx)
	if name == "" {
		dr.Error = "PolicyName is a required property of AWS::IAM::Policy"
		return dr, 0, nil
	}
	if _, ok := props["PolicyDocument"]; !ok {
		dr.Error = "PolicyDocument is a required property of AWS::IAM::Policy"
		return dr, 0, nil
	}
	principals := cfnPolicyPrincipals(props, cctx)
	if len(principals) == 0 {
		dr.Error = cfnIAMPolicyNoPrincipal
		return dr, 0, nil
	}
	rec := cfnIAMRecord{}
	total, err := d.putEmbeddings(ctx, &rec, cfnPolicyEmbeddings(name, principals),
		cfnJSONDocument(props["PolicyDocument"], cctx), true, streamID)
	if err != nil {
		dr.Error = err.Error()
		for _, m := range rec.Embedded {
			cost, undoErr := d.deleteEmbedding(ctx, m, streamID)
			total += cost
			if undoErr != nil {
				d.logger.Warn("cfn: could not remove an inline policy", "principal", m.Name,
					"policy", m.PolicyName, "error", undoErr)
			}
		}
		return dr, total, nil
	}
	cfnSetMetadata(&dr, "Id", dr.PhysicalID)
	if err := setCFNIAMRecord(&dr, rec); err != nil {
		return DeployedResource{}, total, err
	}
	return dr, total, nil
}

// updateIAMPolicy applies a changed AWS::IAM::Policy in place. Every property is No interruption.
// The document is put on every principal the new declaration names — on each one already holding
// it only when PolicyDocument changed — and then every copy in place that the new declaration does
// not ask for is deleted: the principals dropped, and every principal's copy under the old name
// when PolicyName changed. The record tracks each copy per principal and name as it is put or
// deleted. A put that fails takes back out the copies this update put, so a rename that fails
// part-way leaves no copy under the new name; a delete that fails leaves its copy recorded, beside
// the new ones, for the next update or the stack's delete to remove.
//
// A stack recorded before v0.129.0 holds a managed policy here instead (see
// [cfnDeleteIAMPolicy]). Its update deploys the inline form afresh, under the new physical ID, and
// the update then deletes the previous record as it deletes any replaced resource — which removes
// that managed policy.
func (d *StackDeployer) updateIAMPolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	rec := d.liveIAMRecord(ctx, cctx, prior)
	if len(rec.Embedded) == 0 {
		return d.deployIAMPolicy(ctx, logicalID, props, streamID, cctx)
	}
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	name := resolveStringProp(props, "PolicyName", "", cctx)
	principals := cfnPolicyPrincipals(props, cctx)
	switch {
	case name == "":
		dr.Error = "PolicyName is a required property of AWS::IAM::Policy"
		return dr, 0, nil
	case len(principals) == 0:
		dr.Error = cfnIAMPolicyNoPrincipal
		return dr, 0, nil
	}
	want := cfnPolicyEmbeddings(name, principals)
	before := slices.Clone(rec.Embedded)
	total, err := d.putEmbeddings(ctx, &rec, want, cfnJSONDocument(props["PolicyDocument"], cctx),
		slices.Contains(changed, "PolicyDocument"), streamID)
	if err != nil {
		// A put failed, so the update fails, and the copies this update added are taken back out.
		// The resource is then reported failed, and a rollback does not re-apply a failed resource,
		// so a copy left here — under a new name, say — would be known to nothing that could remove
		// it. A copy whose removal fails stays recorded.
		for _, m := range slices.Clone(rec.Embedded) {
			if slices.Contains(before, m) {
				continue
			}
			cost, undoErr := d.deleteEmbedding(ctx, m, streamID)
			total += cost
			if undoErr == nil {
				rec.Embedded = slices.DeleteFunc(rec.Embedded, func(x cfnIAMEmbedding) bool { return x == m })
			}
		}
	}
	if err == nil {
		for _, m := range slices.Clone(rec.Embedded) {
			if slices.Contains(want, m) {
				continue
			}
			cost, delErr := d.deleteEmbedding(ctx, m, streamID)
			total += cost
			if delErr != nil {
				err = delErr
				break
			}
			rec.Embedded = slices.DeleteFunc(rec.Embedded, func(x cfnIAMEmbedding) bool { return x == m })
		}
	}
	return cfnIAMUpdated(dr, rec, total, err)
}

// cfnDeleteIAMPolicy deletes an AWS::IAM::Policy: every copy of the inline policy its record
// lists.
//
// A stack recorded before v0.129.0 deployed the type through CreatePolicy, so its record lists no
// copy and its ARN is a managed policy's (`arn:…:policy/…`); that policy is deleted instead, so
// upgrading substrate does not leak it. The old deploy attached it to nothing, so there is nothing
// to detach first.
func cfnDeleteIAMPolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext, streamID string) *CFNResourceDeletion {
	rec := cfnIAMRecordOf(dr)
	if len(rec.Embedded) == 0 {
		if !strings.Contains(dr.ARN, ":policy/") {
			return nil
		}
		_, err := d.iamCallTolerant(ctx, "DeletePolicy", map[string]string{"PolicyArn": dr.ARN}, streamID)
		return cfnIAMDeleteFailure(dr, err)
	}
	for _, m := range rec.Embedded {
		if _, err := d.deleteEmbedding(ctx, m, streamID); err != nil {
			return cfnIAMDeleteFailure(dr, err)
		}
	}
	return nil
}

// ----- AWS::IAM::ManagedPolicy (#1560) -------------------------------------------------

// deployIAMManagedPolicy creates an AWS::IAM::ManagedPolicy through CreatePolicy and attaches it to
// its Roles, Users and Groups. Until v0.129.0 the type had no deploy arm: it reached the generic
// stub, its physical ID was its logical ID, and no policy existed.
//
// "Ref returns the ARN", so the physical ID is the ARN CreatePolicy answers, which is also what
// PolicyArn resolves to. The other published attributes — PolicyId, DefaultVersionId,
// AttachmentCount, IsAttachable, PermissionsBoundaryUsageCount, CreateDate, UpdateDate — are read
// from a GetPolicy after the attachments, so each is the value the API reports. A
// ManagedPolicyName the template omits is generated. A failure part-way detaches and deletes the
// policy again, since a resource that failed is skipped by the stack's sweep.
func (d *StackDeployer) deployIAMManagedPolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::ManagedPolicy"}
	name := resolveStringProp(props, "ManagedPolicyName",
		cfnGeneratedName(cctx, "AWS::IAM::ManagedPolicy", logicalID), cctx)
	body := map[string]string{
		"PolicyName":     name,
		"Path":           resolveStringProp(props, "Path", "/", cctx),
		"PolicyDocument": cfnJSONDocument(props["PolicyDocument"], cctx),
	}
	if desc := resolveStringProp(props, "Description", "", cctx); desc != "" {
		body["Description"] = desc
	}
	resp, total, err := d.iamCall(ctx, "CreatePolicy", body, nil, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var result struct {
		ARN string `xml:"CreatePolicyResult>Policy>Arn"`
	}
	if resp != nil && xmlUnmarshalIAM(resp.Body, &result) == nil {
		dr.ARN = result.ARN
	}
	dr.PhysicalID = dr.ARN

	rec := cfnIAMRecord{}
	for _, e := range cfnPolicyPrincipals(props, cctx) {
		_, cost, attachErr := d.iamCall(ctx, "Attach"+e.Kind+"Policy",
			e.body(map[string]string{"PolicyArn": dr.ARN}), nil, streamID)
		total += cost
		if attachErr != nil {
			dr.Error = attachErr.Error()
			if undo := cfnDeleteManagedPolicy(ctx, d, dr, rec, streamID); undo != nil {
				d.logger.Warn("cfn: could not remove a managed policy whose create failed",
					"logical_id", logicalID, "policy", dr.ARN, "reason", undo.Reason)
			}
			return dr, total, nil
		}
		addRecordPrincipal(&rec, e)
	}
	total += d.recordManagedPolicyAttributes(ctx, &dr, streamID)
	if err := setCFNIAMRecord(&dr, rec); err != nil {
		return DeployedResource{}, total, err
	}
	return dr, total, nil
}

// recordManagedPolicyAttributes reads the policy back through GetPolicy and records its published
// Fn::GetAtt attributes. A failed read leaves them unrecorded, which resolves them to empty.
func (d *StackDeployer) recordManagedPolicyAttributes(ctx context.Context, dr *DeployedResource, streamID string) float64 {
	resp, cost, err := d.iamCall(ctx, "GetPolicy", map[string]string{"PolicyArn": dr.ARN}, nil, streamID)
	if err != nil || resp == nil {
		return cost
	}
	var out struct {
		Policy struct {
			PolicyID                      string `xml:"PolicyId"`
			DefaultVersionID              string `xml:"DefaultVersionId"`
			AttachmentCount               string `xml:"AttachmentCount"`
			PermissionsBoundaryUsageCount string `xml:"PermissionsBoundaryUsageCount"`
			IsAttachable                  string `xml:"IsAttachable"`
			CreateDate                    string `xml:"CreateDate"`
			UpdateDate                    string `xml:"UpdateDate"`
		} `xml:"GetPolicyResult>Policy"`
	}
	if xmlUnmarshalIAM(resp.Body, &out) != nil {
		return cost
	}
	for k, v := range map[string]string{
		"PolicyId":                      out.Policy.PolicyID,
		"DefaultVersionId":              out.Policy.DefaultVersionID,
		"AttachmentCount":               out.Policy.AttachmentCount,
		"PermissionsBoundaryUsageCount": out.Policy.PermissionsBoundaryUsageCount,
		"IsAttachable":                  out.Policy.IsAttachable,
		"CreateDate":                    out.Policy.CreateDate,
		"UpdateDate":                    out.Policy.UpdateDate,
	} {
		cfnSetMetadata(dr, k, v)
	}
	return cost
}

// updateIAMManagedPolicy applies a changed AWS::IAM::ManagedPolicy in place. ManagedPolicyName,
// Path and Description are Replacement (cfnRecreationProperties). A changed PolicyDocument is made
// the default version — see [StackDeployer.applyManagedPolicyDocument]. Roles, Users and Groups
// are attached and detached to match the declaration, diffed against what is in place.
//
// A stack recorded before v0.129.0 holds a stub here, whose physical ID is the logical ID rather
// than an ARN. Its update creates the policy afresh, and the stub record is deleted as a replaced
// resource is.
func (d *StackDeployer) updateIAMManagedPolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	if !strings.HasPrefix(prior.PhysicalID, "arn:") {
		return d.deployIAMManagedPolicy(ctx, logicalID, props, streamID, cctx)
	}
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	rec := d.liveIAMRecord(ctx, cctx, prior)
	var (
		total float64
		err   error
	)
	if slices.Contains(changed, "PolicyDocument") {
		total, err = d.applyManagedPolicyDocument(ctx, prior.PhysicalID,
			cfnJSONDocument(props["PolicyDocument"], cctx), streamID)
	}
	if err == nil {
		want := cfnPolicyPrincipals(props, cctx)
		have := cfnRecordPrincipals(rec)
		next := cfnIAMRecord{}
		for _, e := range want {
			if err == nil && !slices.Contains(have, e) {
				_, cost, attachErr := d.iamCall(ctx, "Attach"+e.Kind+"Policy",
					e.body(map[string]string{"PolicyArn": prior.PhysicalID}), nil, streamID)
				total += cost
				if attachErr != nil {
					err = attachErr
					continue
				}
			}
			if err == nil || slices.Contains(have, e) {
				addRecordPrincipal(&next, e)
			}
		}
		for _, e := range have {
			if slices.Contains(want, e) {
				continue
			}
			if err == nil {
				cost, detachErr := d.iamCallTolerant(ctx, "Detach"+e.Kind+"Policy",
					e.body(map[string]string{"PolicyArn": prior.PhysicalID}), streamID)
				total += cost
				if detachErr == nil {
					continue
				}
				err = detachErr
			}
			// Still attached, so still the stack's to detach later.
			addRecordPrincipal(&next, e)
		}
		rec = next
	}
	total += d.recordManagedPolicyAttributes(ctx, &dr, streamID)
	return cfnIAMUpdated(dr, rec, total, err)
}

// applyManagedPolicyDocument makes doc the policy's default version.
//
// A version already holding doc is reused rather than duplicated: the default itself, which needs
// no call — a rollback that re-applies an unchanged document, or one counted as changed only
// because every declared property is — or another version, made the default through
// SetDefaultPolicyVersion, which is how the rollback of a document change returns to the version
// it replaced. Otherwise CreatePolicyVersion with SetAsDefault, the page's own See also; when the
// policy already holds the five versions IAM allows, the oldest non-default version is deleted
// first so the create can succeed. Reusing a version and pruning are substrate's reading: the page
// describes neither.
func (d *StackDeployer) applyManagedPolicyDocument(ctx context.Context, arn, doc, streamID string) (float64, error) {
	rows, total, err := d.listPolicyVersionRows(ctx, arn, streamID)
	if err != nil {
		return total, err
	}
	// The default is compared first, so an unchanged document costs one read.
	ordered := slices.Clone(rows)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].IsDefault && !ordered[j].IsDefault })
	for _, row := range ordered {
		live, cost, readErr := d.policyVersionDocument(ctx, arn, row.VersionID, streamID)
		total += cost
		if readErr != nil {
			return total, readErr
		}
		if !cfnPolicyTextsEqual(live, doc) {
			continue
		}
		if row.IsDefault {
			return total, nil
		}
		_, cost, err := d.iamCall(ctx, "SetDefaultPolicyVersion",
			map[string]string{"PolicyArn": arn, "VersionId": row.VersionID}, nil, streamID)
		return total + cost, err
	}
	if len(rows) >= iamMaxPolicyVersions {
		for i := len(rows) - 1; i >= 0; i-- {
			if rows[i].IsDefault {
				continue
			}
			_, cost, delErr := d.iamCall(ctx, "DeletePolicyVersion",
				map[string]string{"PolicyArn": arn, "VersionId": rows[i].VersionID}, nil, streamID)
			total += cost
			if delErr != nil {
				return total, delErr
			}
			break
		}
	}
	_, cost, err := d.iamCall(ctx, "CreatePolicyVersion", map[string]string{
		"PolicyArn": arn, "PolicyDocument": doc, "SetAsDefault": "true",
	}, nil, streamID)
	return total + cost, err
}

// policyVersionDocument reads one version's document through GetPolicyVersion, URL-decoded.
func (d *StackDeployer) policyVersionDocument(ctx context.Context, arn, versionID, streamID string) (string, float64, error) {
	resp, cost, err := d.iamCall(ctx, "GetPolicyVersion",
		map[string]string{"PolicyArn": arn, "VersionId": versionID}, nil, streamID)
	if err != nil || resp == nil {
		return "", cost, err
	}
	var out struct {
		Document string `xml:"GetPolicyVersionResult>PolicyVersion>Document"`
	}
	if err := xmlUnmarshalIAM(resp.Body, &out); err != nil {
		return "", cost, fmt.Errorf("cfn: read version %s of %s: %w", versionID, arn, err)
	}
	text, err := url.PathUnescape(out.Document)
	if err != nil {
		return "", cost, fmt.Errorf("cfn: decode version %s of %s: %w", versionID, arn, err)
	}
	return text, cost, nil
}

// cfnPolicyTextsEqual reports whether two policy documents say the same thing: compared as parsed
// documents with [policyDocumentsEqual] when both parse, so formatting and statement order do not
// count, and as text otherwise.
func cfnPolicyTextsEqual(a, b string) bool {
	var da, db PolicyDocument
	if json.Unmarshal([]byte(a), &da) == nil && json.Unmarshal([]byte(b), &db) == nil {
		return policyDocumentsEqual(da, db)
	}
	return a == b
}

// cfnPolicyVersionRow is one ListPolicyVersions member.
type cfnPolicyVersionRow struct {
	VersionID string `xml:"VersionId"`
	IsDefault bool   `xml:"IsDefaultVersion"`
}

// listPolicyVersionRows lists a policy's versions, newest first, as ListPolicyVersions answers.
func (d *StackDeployer) listPolicyVersionRows(ctx context.Context, arn, streamID string) ([]cfnPolicyVersionRow, float64, error) {
	resp, cost, err := d.iamCall(ctx, "ListPolicyVersions", map[string]string{"PolicyArn": arn}, nil, streamID)
	if err != nil {
		return nil, cost, err
	}
	var out struct {
		Versions []cfnPolicyVersionRow `xml:"ListPolicyVersionsResult>Versions>member"`
	}
	if resp != nil {
		if err := xmlUnmarshalIAM(resp.Body, &out); err != nil {
			return nil, cost, fmt.Errorf("cfn: read the versions of %s: %w", arn, err)
		}
	}
	return out.Versions, cost, nil
}

// cfnDeleteManagedPolicy detaches the policy from every principal rec lists, deletes its
// non-default versions, then deletes it — the three steps API_DeletePolicy lists, in its order.
// Each tolerates an absence.
func cfnDeleteManagedPolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, rec cfnIAMRecord, streamID string) *CFNResourceDeletion {
	arn := dr.PhysicalID
	if arn == "" {
		arn = dr.ARN
	}
	for _, e := range cfnRecordPrincipals(rec) {
		if _, err := d.iamCallTolerant(ctx, "Detach"+e.Kind+"Policy",
			e.body(map[string]string{"PolicyArn": arn}), streamID); err != nil {
			return cfnIAMDeleteFailure(dr, err)
		}
	}
	rows, _, err := d.listPolicyVersionRows(ctx, arn, streamID)
	if err != nil {
		if cfnDeleteIsAbsent(err) {
			return nil
		}
		return cfnIAMDeleteFailure(dr, err)
	}
	for _, row := range rows {
		if row.IsDefault {
			continue
		}
		if _, err := d.iamCallTolerant(ctx, "DeletePolicyVersion",
			map[string]string{"PolicyArn": arn, "VersionId": row.VersionID}, streamID); err != nil {
			return cfnIAMDeleteFailure(dr, err)
		}
	}
	_, err = d.iamCallTolerant(ctx, "DeletePolicy", map[string]string{"PolicyArn": arn}, streamID)
	return cfnIAMDeleteFailure(dr, err)
}

// cfnDeleteIAMManagedPolicy is AWS::IAM::ManagedPolicy's sequenced deleter. A stack recorded
// before v0.129.0 holds a stub, whose physical ID is not an ARN; its only trace is the stub record,
// which is removed instead.
func cfnDeleteIAMManagedPolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, cctx *cfnContext, streamID string) *CFNResourceDeletion {
	if !strings.HasPrefix(dr.PhysicalID, "arn:") {
		if cctx == nil {
			cctx = &cfnContext{accountID: d.identity.accountID, region: d.identity.region}
		}
		return cfnIAMDeleteFailure(dr, d.deleteStubKey(ctx, dr, cctx))
	}
	return cfnDeleteManagedPolicy(ctx, d, dr, cfnIAMRecordOf(dr), streamID)
}
