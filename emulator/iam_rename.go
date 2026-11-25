package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
)

// UpdateUser and UpdateGroup: an IAM user's or group's rename and path change.
//
// Everything substrate stores under a user's or group's name is moved with it, so the renamed
// entity keeps its attached and inline policies, its group memberships (both sides of the index),
// its access keys (whose record names the user, and is what a signed request resolves its caller
// from), its login profile, its tags and its GetSessionToken sessions — AWS ties those to the user,
// not the name, so a session minted before the rename keeps the renamed user's permissions.
//
// Two properties hold for every reader of these records, whether or not it holds
// [IAMPlugin.entityMu]:
//
//   - A reader sees the user under its old name in full or its new name in full. The new name's
//     records are written first, then the new entity record, then the pointers that name the user
//     (access keys, the other side of each membership, sessions) are flipped one by one, and only
//     then are the old records deleted, old entity record first. At every point each name that has
//     an entity record also has all of its dependents.
//   - A failure part way through is undone: every write is recorded with the value it replaced, and
//     an error rolls them all back, so the entity is left wholly under its old name and the rename
//     can be retried.
//
// Writers that would race a rename — IAM's own operations, and the tagging API's writes to IAM
// records, which reach IAM through [IAMPlugin.tagRecord] — take entityMu, which a rename holds for
// writing, so none can recreate a record the rename removed.
//
// Readers that act on what they read take it too, through [PluginRegistry.iamEntityReadLock]: the
// server holds it across principal resolution and the authorization gate, so a request is decided
// against one side of a rename, and GetSessionToken holds it while it writes the caller into a new
// session. A caller resolved before a handler took the lock is looked up again by its unique ID
// ([iamRefreshedPrincipal]); one whose user is gone is refused ([authzVanishedPrincipal]).

// iamEntityLocker is implemented by the plugin that owns IAM's users and groups. Principal
// resolution and authorization in the server, and GetSessionToken in STS, read those records
// outside IAM's own requests; they hold this lock for reading, as IAM's own operations do, so a
// rename (which holds it for writing) never runs between their reading a user and their acting
// on what they read.
type iamEntityLocker interface {
	// readLockEntities holds the entity lock for reading and returns its release.
	readLockEntities() (release func())
}

// readLockEntities implements [iamEntityLocker].
func (p *IAMPlugin) readLockEntities() func() {
	p.entityMu.RLock()
	return p.entityMu.RUnlock
}

// iamEntityReadLock holds the entity lock of the IAM plugin registered in r for reading, and
// returns a release that may be called more than once. With no registry or no IAM plugin there
// is nothing to race, and the release is a no-op. It is reached through the registry rather than
// a package variable, so each server's lock is its own IAM plugin's.
func (r *PluginRegistry) iamEntityReadLock() func() {
	if r == nil {
		return func() {}
	}
	plugin, ok := r.Plugin("iam")
	if !ok {
		return func() {}
	}
	locker, ok := plugin.(iamEntityLocker)
	if !ok {
		return func() {}
	}
	release := locker.readLockEntities()
	var once sync.Once
	return func() { once.Do(release) }
}

// iamCurrentUser returns the user a principal resolved as accountID's name with unique ID
// userID is now: the record under name when it is that user, else the user holding userID under
// another name (a rename ran after the principal was resolved), else nil. With no userID, which
// a key written before #771 lacks, the record under name is taken as it is.
func iamCurrentUser(ctx context.Context, state StateManager, accountID, name, userID string) (*IAMUser, error) {
	named, err := iamUserAt(ctx, state, iamUserKey(accountID, name))
	switch {
	case err != nil:
		return nil, err
	case named != nil && (userID == "" || named.UserID == userID):
		return named, nil
	case userID == "":
		return nil, nil
	}
	keys, err := state.List(ctx, iamNamespace, iamUserPrefix(accountID))
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	for _, key := range keys {
		u, err := iamUserAt(ctx, state, key)
		if err != nil || (u != nil && u.UserID == userID) {
			return u, err
		}
	}
	return nil, nil
}

// iamRefreshedPrincipal returns principal as the IAM user it is now, for a principal
// [resolvePrincipal] built from a user's access key or GetSessionToken session. The principal was
// resolved before the caller took [IAMPlugin.entityMu], so a rename may have run since: the user
// is found by its unique ID ([iamCurrentUser]) and a copy naming its current name, ARN and tags is
// returned. Any other principal, and a user that has not moved, is returned as it is. nil means
// the user no longer exists, which the authorization doors refuse ([authzVanishedPrincipal]).
func iamRefreshedPrincipal(ctx context.Context, state StateManager, principal *Principal) (*Principal, error) {
	if principal == nil || !principal.iamCredential {
		return principal, nil
	}
	entity, ok := iamEntityForPrincipalARN(principal.ARN)
	if !ok || entity.Kind != "user" {
		return principal, nil
	}
	user, err := iamCurrentUser(ctx, state, entity.Account, entity.Name, principal.UserID)
	if err != nil || user == nil {
		return nil, err
	}
	if user.UserName == entity.Name && user.ARN == principal.ARN {
		return principal, nil
	}
	moved := *principal
	moved.ARN, moved.UserName, moved.Tags = user.ARN, user.UserName, iamTagMap(user.Tags)
	return &moved, nil
}

// iamTagMap returns tags keyed by tag key, or nil for none.
func iamTagMap(tags []IAMTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}
	return out
}

// iamUserAt reads the user record at key, or nil when there is none.
func iamUserAt(ctx context.Context, state StateManager, key string) (*IAMUser, error) {
	raw, err := state.Get(ctx, iamNamespace, key)
	if err != nil || raw == nil {
		return nil, err
	}
	var u IAMUser
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return &u, nil
}

// iamValidPath reports whether path matches the pattern every IAM path parameter documents:
// `(/)|(/[!-~]+/)`, at most 512 characters.
func iamValidPath(path string) bool {
	if path == "/" {
		return true
	}
	if len(path) < 3 || len(path) > 512 || !strings.HasPrefix(path, "/") || !strings.HasSuffix(path, "/") {
		return false
	}
	for _, r := range path {
		if r < 0x21 || r > 0x7E {
			return false
		}
	}
	return true
}

// iamInvalidPath is the refusal for a NewPath outside the documented pattern. The message is
// substrate's own.
func iamInvalidPath(path string) *AWSResponse {
	return iamErrorResponse("ValidationError",
		fmt.Sprintf("NewPath %q must be \"/\" or begin and end with \"/\".", path), http.StatusBadRequest)
}

// iamTxn is a sequence of state writes that can be undone: each records the value it replaced.
type iamTxn struct {
	p     *IAMPlugin
	ctx   context.Context
	undo  []func() error
	moves [][2]string
}

// get reads IAM's key.
func (t *iamTxn) get(key string) ([]byte, error) {
	return t.getIn(iamNamespace, key)
}

// getIn reads key in namespace ns.
func (t *iamTxn) getIn(ns, key string) ([]byte, error) {
	raw, err := t.p.state.Get(t.ctx, ns, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return raw, nil
}

// put writes value at IAM's key, recording how to restore what was there.
func (t *iamTxn) put(key string, value []byte) error {
	return t.putIn(iamNamespace, key, value)
}

// putIn writes value at key in namespace ns, recording how to restore what was there.
func (t *iamTxn) putIn(ns, key string, value []byte) error {
	prev, err := t.getIn(ns, key)
	if err != nil {
		return err
	}
	if err := t.p.state.Put(t.ctx, ns, key, value); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	t.undo = append(t.undo, t.restorer(ns, key, prev))
	return nil
}

// putJSON writes v, encoded as JSON, at key in namespace ns, as [iamTxn.putIn] does.
func (t *iamTxn) putJSON(ns, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	return t.putIn(ns, key, raw)
}

// del deletes key, recording how to restore it.
func (t *iamTxn) del(key string) error {
	prev, err := t.get(key)
	if err != nil || prev == nil {
		return err
	}
	if err := t.p.state.Delete(t.ctx, iamNamespace, key); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	t.undo = append(t.undo, t.restorer(iamNamespace, key, prev))
	return nil
}

// restorer returns the undo of a write to key in ns whose previous value was prev (nil: absent).
func (t *iamTxn) restorer(ns, key string, prev []byte) func() error {
	return func() error {
		if prev == nil {
			return t.p.state.Delete(t.ctx, ns, key)
		}
		return t.p.state.Put(t.ctx, ns, key, prev)
	}
}

// copyKey writes the record at from to to, and records the move so [iamTxn.finishMoves] deletes
// from once every pointer has been flipped. A destination with no source is deleted: a stale record
// left under the new name by an earlier, partial delete must not be inherited.
func (t *iamTxn) copyKey(from, to string) error {
	raw, err := t.get(from)
	if err != nil {
		return err
	}
	if raw == nil {
		return t.del(to)
	}
	if err := t.put(to, raw); err != nil {
		return err
	}
	t.moves = append(t.moves, [2]string{from, to})
	return nil
}

// finishMoves deletes the sources of every [iamTxn.copyKey].
func (t *iamTxn) finishMoves() error {
	for _, m := range t.moves {
		if err := t.del(m[0]); err != nil {
			return err
		}
	}
	return nil
}

// rollback undoes every recorded write, newest first. A failure to undo is logged: the state is
// then as consistent as the store allows, and the original error is what the caller reports.
func (t *iamTxn) rollback() {
	for i := len(t.undo) - 1; i >= 0; i-- {
		if err := t.undo[i](); err != nil {
			t.p.logger.Warn("iam: could not undo a rename write", "error", err)
		}
	}
}

// renameInList replaces oldName with newName in the sorted string list at key.
func (t *iamTxn) renameInList(key, oldName, newName string) error {
	list, err := t.p.loadStringList(t.ctx, key)
	if err != nil {
		return err
	}
	i := slices.Index(list, oldName)
	if i < 0 {
		return nil
	}
	list[i] = newName
	sort.Strings(list)
	return t.putJSON(iamNamespace, key, slices.Compact(list))
}

// copyInlinePolicies copies an entity's inline-policy name list and every document it names.
func (t *iamTxn) copyInlinePolicies(accountID, kind, oldName, newName string) error {
	names, err := t.p.loadInlinePolicyNames(t.ctx, accountID, kind, oldName)
	if err != nil {
		return err
	}
	for _, policy := range names {
		if err := t.copyKey(iamInlinePolicyKey(accountID, kind, oldName, policy),
			iamInlinePolicyKey(accountID, kind, newName, policy)); err != nil {
			return err
		}
	}
	return t.copyKey(iamInlinePolicyNamesKey(accountID, kind, oldName), iamInlinePolicyNamesKey(accountID, kind, newName))
}

// iamRenameParams are UpdateUser's and UpdateGroup's parameters, under either entity's names.
type iamRenameParams struct {
	name, newName, newPath string
}

func (p *IAMPlugin) updateUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName    string `json:"UserName"`
		NewUserName string `json:"NewUserName"`
		NewPath     string `json:"NewPath"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	if params.NewPath != "" && !iamValidPath(params.NewPath) {
		return iamInvalidPath(params.NewPath), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:UpdateUser", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, ctx.AccountID, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName), http.StatusNotFound), nil
	}
	r := iamRenameParams{name: params.UserName, newName: params.NewUserName, newPath: params.NewPath}
	if r.newName == "" {
		r.newName = r.name
	}
	if r.newPath == "" {
		r.newPath = user.Path
	}
	if r.newName != r.name {
		// "To change a user name, the requester must have appropriate permissions on both the
		// source object and the target object."
		target := authzResource{ARN: iamUserARN(ctx.AccountID, r.newPath, r.newName)}
		if err := p.authorize(goCtx, ctx, "iam:UpdateUser", target); err != nil {
			return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
		}
		existing, err := p.loadUser(goCtx, ctx.AccountID, r.newName)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return iamErrorResponse("EntityAlreadyExists",
				fmt.Sprintf("User with name %s already exists.", r.newName), http.StatusConflict), nil
		}
	}
	if err := p.renameUser(goCtx, ctx.AccountID, user, r); err != nil {
		return nil, err
	}
	return iamXMLEmptyResponse("UpdateUser"), nil
}

// renameUser rewrites user under r's name and path and moves every record keyed by its name, in
// the order the file comment gives, undoing everything on failure.
func (p *IAMPlugin) renameUser(goCtx context.Context, accountID string, user *IAMUser, r iamRenameParams) error {
	t := &iamTxn{p: p, ctx: goCtx}
	if err := p.renameUserSteps(t, accountID, user, r); err != nil {
		t.rollback()
		return err
	}
	return nil
}

func (p *IAMPlugin) renameUserSteps(t *iamTxn, accountID string, user *IAMUser, r iamRenameParams) error {
	oldARN := user.ARN
	user.UserName, user.Path = r.newName, r.newPath
	user.ARN = iamUserARN(accountID, r.newPath, r.newName)
	if r.newName == r.name {
		if err := t.putJSON(iamNamespace, iamUserKey(accountID, r.name), user); err != nil {
			return err
		}
		return p.rewriteUserSessions(t, accountID, user.UserID, r.name, r.name, oldARN, user.ARN)
	}

	// The new name's dependents, then its entity record.
	for _, kind := range []func(string) string{
		func(n string) string { return iamAttachedPoliciesKey(accountID, "user", n) },
		func(n string) string { return iamUserAccessKeysKey(accountID, n) },
		func(n string) string { return iamUserGroupsKey(accountID, n) },
	} {
		if err := t.copyKey(kind(r.name), kind(r.newName)); err != nil {
			return err
		}
	}
	if err := t.copyInlinePolicies(accountID, "user", r.name, r.newName); err != nil {
		return err
	}
	if err := p.copyLoginProfile(t, accountID, r.name, r.newName); err != nil {
		return err
	}
	if err := t.putJSON(iamNamespace, iamUserKey(accountID, r.newName), user); err != nil {
		return err
	}

	// The pointers that name the user.
	keyIDs, err := p.loadStringList(t.ctx, iamUserAccessKeysKey(accountID, r.name))
	if err != nil {
		return err
	}
	for _, id := range keyIDs {
		if err := p.rewriteAccessKeyUser(t, id, r.newName); err != nil {
			return err
		}
	}
	groups, err := p.loadStringList(t.ctx, iamUserGroupsKey(accountID, r.name))
	if err != nil {
		return err
	}
	for _, g := range groups {
		if err := t.renameInList(iamGroupUsersKey(accountID, g), r.name, r.newName); err != nil {
			return err
		}
	}
	if err := p.rewriteUserSessions(t, accountID, user.UserID, r.name, r.newName, oldARN, user.ARN); err != nil {
		return err
	}

	// The old name: entity record first, then its dependents.
	if err := t.del(iamUserKey(accountID, r.name)); err != nil {
		return err
	}
	return t.finishMoves()
}

// copyLoginProfile copies a user's login profile to newName, naming the new user.
func (p *IAMPlugin) copyLoginProfile(t *iamTxn, accountID, oldName, newName string) error {
	lp, err := p.loadLoginProfile(t.ctx, accountID, oldName)
	if err != nil {
		return err
	}
	if lp == nil {
		return t.del(iamLoginProfileKey(accountID, newName))
	}
	lp.UserName = newName
	if err := t.putJSON(iamNamespace, iamLoginProfileKey(accountID, newName), lp); err != nil {
		return err
	}
	t.moves = append(t.moves, [2]string{iamLoginProfileKey(accountID, oldName), iamLoginProfileKey(accountID, newName)})
	return nil
}

// rewriteAccessKeyUser sets the UserName on an access key's record.
func (p *IAMPlugin) rewriteAccessKeyUser(t *iamTxn, accessKeyID, userName string) error {
	key := iamAccessKeyKey(accessKeyID)
	raw, err := t.get(key)
	if err != nil || raw == nil {
		return err
	}
	var record IAMAccessKey
	if err := json.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("unmarshal access key: %w", err)
	}
	record.UserName = userName
	return t.putJSON(iamNamespace, key, &record)
}

// rewriteUserSessions points the user's GetSessionToken sessions at its new name and ARN, so they
// keep the user's permissions. AWS ties a session to the user rather than to its name; substrate
// resolves a session's principal from the name it records, so the record is rewritten instead.
//
// A session is the user's when it records the user's unique ID (`PrincipalID`, the AIDA... the
// user was created with), which no other user shares. One recorded before #771 carries no ID and
// is matched by the name and ARN it recorded.
func (p *IAMPlugin) rewriteUserSessions(t *iamTxn, accountID, userID, oldName, newName, oldARN, newARN string) error {
	if oldARN == newARN {
		return nil
	}
	keys, err := p.state.List(t.ctx, stsNamespace, "session:")
	if err != nil {
		return fmt.Errorf("list sessions: %w", err)
	}
	for _, key := range keys {
		raw, err := t.getIn(stsNamespace, key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		var sess STSSessionCredentials
		if err := json.Unmarshal(raw, &sess); err != nil {
			return fmt.Errorf("unmarshal session %s: %w", key, err)
		}
		if !iamSessionOfUser(sess, accountID, userID, oldName, oldARN) {
			continue
		}
		sess.UserName, sess.PrincipalARN = newName, newARN
		if err := t.putJSON(stsNamespace, key, sess); err != nil {
			return err
		}
	}
	return nil
}

// iamSessionOfUser reports whether sess is a GetSessionToken session of accountID's user with
// unique ID userID, named name with ARN arn: by the ID when both sides have one, and by the name and
// ARN for a session or user recorded before #771. A session of an assumed role names no user.
func iamSessionOfUser(sess STSSessionCredentials, accountID, userID, name, arn string) bool {
	if sess.AccountID != accountID || sess.UserName == "" {
		return false
	}
	if sess.PrincipalID != "" && userID != "" {
		return sess.PrincipalID == userID
	}
	return sess.UserName == name && sess.PrincipalARN == arn
}

func (p *IAMPlugin) updateGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName    string `json:"GroupName"`
		NewGroupName string `json:"NewGroupName"`
		NewPath      string `json:"NewPath"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" {
		return iamErrorResponse("ValidationError", "GroupName is required", http.StatusBadRequest), nil
	}
	if params.NewPath != "" && !iamValidPath(params.NewPath) {
		return iamInvalidPath(params.NewPath), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:UpdateGroup", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	group, err := p.loadGroup(goCtx, ctx.AccountID, params.GroupName)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName), http.StatusNotFound), nil
	}
	r := iamRenameParams{name: params.GroupName, newName: params.NewGroupName, newPath: params.NewPath}
	if r.newName == "" {
		r.newName = r.name
	}
	if r.newPath == "" {
		r.newPath = group.Path
	}
	if r.newName != r.name {
		// "The principal must have permission to change the role group with the old name and the
		// new name."
		target := authzResource{ARN: iamGroupARN(ctx.AccountID, r.newPath, r.newName)}
		if err := p.authorize(goCtx, ctx, "iam:UpdateGroup", target); err != nil {
			return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
		}
		existing, err := p.loadGroup(goCtx, ctx.AccountID, r.newName)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return iamErrorResponse("EntityAlreadyExists",
				fmt.Sprintf("Group with name %s already exists.", r.newName), http.StatusConflict), nil
		}
	}
	if err := p.renameGroup(goCtx, ctx.AccountID, group, r); err != nil {
		return nil, err
	}
	return iamXMLEmptyResponse("UpdateGroup"), nil
}

// renameGroup rewrites group under r's name and path and moves every record keyed by its name, in
// the same order as [IAMPlugin.renameUser], undoing everything on failure.
func (p *IAMPlugin) renameGroup(goCtx context.Context, accountID string, group *IAMGroup, r iamRenameParams) error {
	t := &iamTxn{p: p, ctx: goCtx}
	if err := p.renameGroupSteps(t, accountID, group, r); err != nil {
		t.rollback()
		return err
	}
	return nil
}

func (p *IAMPlugin) renameGroupSteps(t *iamTxn, accountID string, group *IAMGroup, r iamRenameParams) error {
	group.GroupName, group.Path = r.newName, r.newPath
	group.ARN = iamGroupARN(accountID, r.newPath, r.newName)
	if r.newName == r.name {
		return t.putJSON(iamNamespace, iamGroupKey(accountID, r.name), group)
	}
	if err := t.copyKey(iamGroupPoliciesKey(accountID, r.name), iamGroupPoliciesKey(accountID, r.newName)); err != nil {
		return err
	}
	if err := t.copyKey(iamGroupUsersKey(accountID, r.name), iamGroupUsersKey(accountID, r.newName)); err != nil {
		return err
	}
	if err := t.copyInlinePolicies(accountID, "group", r.name, r.newName); err != nil {
		return err
	}
	if err := t.putJSON(iamNamespace, iamGroupKey(accountID, r.newName), group); err != nil {
		return err
	}
	members, err := p.loadStringList(t.ctx, iamGroupUsersKey(accountID, r.name))
	if err != nil {
		return err
	}
	for _, u := range members {
		if err := t.renameInList(iamUserGroupsKey(accountID, u), r.name, r.newName); err != nil {
			return err
		}
	}
	if err := t.del(iamGroupKey(accountID, r.name)); err != nil {
		return err
	}
	return t.finishMoves()
}
