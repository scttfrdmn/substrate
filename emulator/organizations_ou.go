package emulator

import (
	"context"
	"fmt"
	"unicode/utf8"
)

// ouOperation claims the organizational-unit and hierarchy operations. The
// hierarchy itself lives in organizations_state.go; this file owns only the
// operations that read and build it.
func (p *OrganizationsPlugin) ouOperation(op string) (orgHandler, bool) {
	switch op {
	case "CreateOrganizationalUnit":
		return p.createOrganizationalUnit, true
	case "DescribeOrganizationalUnit":
		return p.describeOrganizationalUnit, true
	case "UpdateOrganizationalUnit":
		return p.updateOrganizationalUnit, true
	case "DeleteOrganizationalUnit":
		return p.deleteOrganizationalUnit, true
	case "ListOrganizationalUnitsForParent":
		return p.listOrganizationalUnitsForParent, true
	case "ListParents":
		return p.listParents, true
	case "ListChildren":
		return p.listChildren, true
	case "ListAccountsForParent":
		return p.listAccountsForParent, true
	default:
		return nil, false
	}
}

// orgMaxOUNameChars is the OrganizationalUnitName ceiling from the API model
// (min 1, max 128). A name past it is InvalidInputException rather than a
// truncated OU, so a caller generating names from an account title learns the
// name was rejected instead of finding an OU it cannot address by name. The limit
// counts characters, not bytes, so a name of accented or CJK characters is
// measured the way AWS measures it.
const orgMaxOUNameChars = 128

// --- operations ---

// createOrganizationalUnit implements CreateOrganizationalUnit. Every refusal it
// can answer with is in the operation's declared error list: ParentNotFound for a
// parent that is not there, DuplicateOrganizationalUnit for a name already used
// among the parent's children, ConstraintViolation for the depth and count
// quotas, and InvalidInput for a malformed request.
func (p *OrganizationsPlugin) createOrganizationalUnit(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ParentID string        `json:"ParentId"`
		Name     string        `json:"Name"`
		Tags     []orgTagInput `json:"Tags"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.Name == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify a Name for the new organizational unit")
	}
	if utf8.RuneCountInString(input.Name) > orgMaxOUNameChars {
		return nil, orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("the Name value must be at most %d characters", orgMaxOUNameChars))
	}
	// Tags go through the same validation TagResource applies, so a key that
	// operation refuses cannot be planted through a create instead. A partially
	// created OU would be worse than the refusal: the caller's retry would then hit
	// DuplicateOrganizationalUnitException for an OU it does not believe it created.
	tags, err := validateOrgCreateTags(input.Tags)
	if err != nil {
		return nil, err
	}

	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit ensure org: %w", err)
	}
	root, err := p.loadRoot(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit load root: %w", err)
	}
	if err := p.requireOrgParent(goCtx, reqCtx.AccountID, input.ParentID); err != nil {
		return nil, err
	}

	// The depth limit counts OU levels below the root, so a parent already at the
	// limit can hold accounts but no further OU. A 5-deep tree and an illegal 6th
	// level are indistinguishable on a nominal run, which is why this comparison is
	// the one an off-by-one hides in.
	parentDepth, err := p.ouDepth(goCtx, input.ParentID)
	if err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit depth: %w", err)
	}
	if parentDepth+1 > orgMaxOUDepth {
		return nil, orgConstraintViolation("OU_DEPTH_LIMIT_EXCEEDED",
			fmt.Sprintf("you cannot nest organizational units more than %d levels below the root", orgMaxOUDepth))
	}

	// Uniqueness is scoped to the parent, not to the organization: "Sandbox" under
	// two different parents is legal, and refusing it would break the common layout
	// where every business unit has its own identically named child OUs.
	taken, err := p.ouSiblingNameTaken(goCtx, input.ParentID, input.Name, "")
	if err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit duplicate check: %w", err)
	}
	if taken {
		return nil, orgErr("DuplicateOrganizationalUnitException",
			"An OU with the same name already exists: "+input.Name)
	}

	existing, err := p.loadOUIDs(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit load ids: %w", err)
	}
	if len(existing) >= orgMaxOUsPerOrg {
		return nil, orgConstraintViolation("OU_NUMBER_LIMIT_EXCEEDED",
			fmt.Sprintf("you have exceeded the number of organizational units you can have in an organization (%d)", orgMaxOUsPerOrg))
	}

	// The middle segment of an OU ID is the containing root's suffix, whatever the
	// OU's immediate parent is — nesting does not extend the ID.
	ouID := fmt.Sprintf("ou-%s-%s", root.ID[2:], randomLowerAlphanum(8))
	ou := OrgOrganizationalUnit{
		ID:   ouID,
		Arn:  fmt.Sprintf("arn:aws:organizations::%s:ou/%s/%s", reqCtx.AccountID, org.ID, ouID),
		Name: input.Name,
	}
	if err := p.saveOU(goCtx, reqCtx.AccountID, ou); err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit save: %w", err)
	}
	if err := p.placeChild(goCtx, input.ParentID, ouID); err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit place: %w", err)
	}
	if err := p.attachFullAWSAccess(goCtx, reqCtx.AccountID, ouID); err != nil {
		return nil, fmt.Errorf("createOrganizationalUnit attach FullAWSAccess: %w", err)
	}
	if len(tags) > 0 {
		if err := p.saveTags(goCtx, ouID, tags); err != nil {
			return nil, fmt.Errorf("createOrganizationalUnit save tags: %w", err)
		}
	}

	return orgJSONResponse(map[string]interface{}{"OrganizationalUnit": ou}, "createOrganizationalUnit")
}

func (p *OrganizationsPlugin) describeOrganizationalUnit(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		OrganizationalUnitID string `json:"OrganizationalUnitId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	ou, err := p.requireOU(goCtx, reqCtx.AccountID, input.OrganizationalUnitID)
	if err != nil {
		return nil, err
	}
	return orgJSONResponse(map[string]interface{}{"OrganizationalUnit": ou}, "describeOrganizationalUnit")
}

// updateOrganizationalUnit implements UpdateOrganizationalUnit, which renames an
// OU in place: the ID, the ARN, the children, and the attached policies all
// survive it, so a caller holding any of those keeps a valid handle.
func (p *OrganizationsPlugin) updateOrganizationalUnit(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	// Name is optional in the model, and an omitted name is not the same request as
	// an empty one: the first leaves the OU alone, the second asks for a name the
	// model's minimum length forbids.
	var input struct {
		OrganizationalUnitID string  `json:"OrganizationalUnitId"`
		Name                 *string `json:"Name"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.Name != nil {
		switch {
		case *input.Name == "":
			return nil, orgInvalidInput("MIN_LENGTH_EXCEEDED", "the Name value must be at least 1 character")
		case utf8.RuneCountInString(*input.Name) > orgMaxOUNameChars:
			return nil, orgInvalidInput("MAX_LENGTH_EXCEEDED",
				fmt.Sprintf("the Name value must be at most %d characters", orgMaxOUNameChars))
		}
	}

	ou, err := p.requireOU(goCtx, reqCtx.AccountID, input.OrganizationalUnitID)
	if err != nil {
		return nil, err
	}
	if input.Name != nil && *input.Name != ou.Name {
		parent, parentErr := p.loadParent(goCtx, ou.ID)
		if parentErr != nil {
			return nil, fmt.Errorf("updateOrganizationalUnit load parent: %w", parentErr)
		}
		// The OU itself is excluded from the scan, so renaming an OU to the name it
		// already has is not a duplicate. That matters because a governance script
		// re-run reissues the same rename, and refusing the second run would make
		// convergence impossible to express.
		taken, takenErr := p.ouSiblingNameTaken(goCtx, parent, *input.Name, ou.ID)
		if takenErr != nil {
			return nil, fmt.Errorf("updateOrganizationalUnit duplicate check: %w", takenErr)
		}
		if taken {
			return nil, orgErr("DuplicateOrganizationalUnitException",
				"An OU with the same name already exists: "+*input.Name)
		}
		ou.Name = *input.Name
		if err := p.saveOU(goCtx, reqCtx.AccountID, *ou); err != nil {
			return nil, fmt.Errorf("updateOrganizationalUnit save: %w", err)
		}
	}
	return orgJSONResponse(map[string]interface{}{"OrganizationalUnit": ou}, "updateOrganizationalUnit")
}

// deleteOrganizationalUnit implements DeleteOrganizationalUnit. AWS requires the
// OU be emptied first, and the refusal is what makes a teardown script's ordering
// bug visible: deleting an OU that still holds accounts would otherwise orphan
// them somewhere no listing reaches.
func (p *OrganizationsPlugin) deleteOrganizationalUnit(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		OrganizationalUnitID string `json:"OrganizationalUnitId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	ou, err := p.requireOU(goCtx, reqCtx.AccountID, input.OrganizationalUnitID)
	if err != nil {
		return nil, err
	}

	children, err := p.loadChildren(goCtx, ou.ID)
	if err != nil {
		return nil, fmt.Errorf("deleteOrganizationalUnit load children: %w", err)
	}
	if len(children) > 0 {
		return nil, orgErr("OrganizationalUnitNotEmptyException",
			"The specified OU is not empty. Move all accounts to another root or to other OUs, "+
				"remove all child OUs, and try the operation again.")
	}

	// Every index that names the OU is unwound here. A leftover entry is invisible
	// until something reads it, and then it reads as an entity that exists in one
	// listing and not in another — a state no sequence of API calls can produce, so
	// nothing downstream is prepared for it.
	if err := p.forgetOU(goCtx, reqCtx.AccountID, ou.ID); err != nil {
		return nil, fmt.Errorf("deleteOrganizationalUnit: %w", err)
	}
	return orgEmptyResponse(), nil
}

func (p *OrganizationsPlugin) listOrganizationalUnitsForParent(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ParentID   string `json:"ParentId"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listOrganizationalUnitsForParent ensure org: %w", err)
	}
	if err := p.requireOrgParent(goCtx, reqCtx.AccountID, input.ParentID); err != nil {
		return nil, err
	}

	ids, err := p.childrenOfKind(goCtx, input.ParentID, orgKindOU)
	if err != nil {
		return nil, fmt.Errorf("listOrganizationalUnitsForParent load children: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	ous := make([]OrgOrganizationalUnit, 0, len(page))
	for _, id := range page {
		ou, loadErr := p.loadOU(goCtx, id)
		if loadErr != nil {
			return nil, fmt.Errorf("listOrganizationalUnitsForParent load OU: %w", loadErr)
		}
		// An index entry whose record is gone is skipped rather than reported as a
		// zero-valued OU, for the reason ListAccounts skips one: a consumer iterating
		// an OU with an empty Id would call Describe with "" and get a refusal it
		// cannot explain.
		if ou == nil {
			continue
		}
		ous = append(ous, *ou)
	}

	out := map[string]interface{}{"OrganizationalUnits": ous}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listOrganizationalUnitsForParent")
}

// listParents implements ListParents. A child has exactly one parent in the
// current API, so the list has one entry — but it is still a list, and it is the
// step a caller walks upward with until it reaches the root ListRoots reports.
func (p *OrganizationsPlugin) listParents(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ChildID    string `json:"ChildId"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listParents ensure org: %w", err)
	}
	if input.ChildID == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify a ChildId")
	}
	// The ChildId pattern admits only a 12-digit account ID or an OU ID; a root is
	// explicitly not a child. Answering a malformed ID with InvalidInputException
	// rather than ChildNotFoundException tells the caller its ID is the wrong shape
	// instead of sending it looking for an entity that could never exist.
	if !isOrgOUID(input.ChildID) && !isOrgAccountID(input.ChildID) {
		return nil, orgInvalidInput("INVALID_PATTERN",
			"the ChildId must be a 12-digit account ID or an organizational unit ID: "+input.ChildID)
	}

	kind, err := p.resolveOrgTarget(goCtx, reqCtx.AccountID, input.ChildID)
	if err != nil {
		return nil, fmt.Errorf("listParents resolve child: %w", err)
	}
	if kind != orgKindOU && kind != orgKindAccount {
		return nil, orgErr("ChildNotFoundException",
			"We can't find an organizational unit (OU) or Amazon Web Services account with the ChildId "+input.ChildID)
	}

	parent, err := p.loadParent(goCtx, input.ChildID)
	if err != nil {
		return nil, fmt.Errorf("listParents load parent: %w", err)
	}
	if parent == "" {
		// Unreachable through the API: an account or OU is placed at the moment it is
		// created. Reporting no parents would silently break the upward walk, so this
		// is a failure rather than an empty list.
		return nil, fmt.Errorf("organizations: %s has no recorded parent", input.ChildID)
	}
	// The token is validated even though the list has one entry, so a caller
	// passing a stale token learns it rather than silently restarting.
	if _, _, err := orgPaginate([]string{parent}, input.NextToken, input.MaxResults); err != nil {
		return nil, err
	}

	parentType := orgKindOU
	if isOrgRootID(parent) {
		parentType = orgKindRoot
	}
	return orgJSONResponse(map[string]interface{}{
		"Parents": []OrgParent{{ID: parent, Type: parentType}},
	}, "listParents")
}

// listChildren implements ListChildren. ChildType is required by the model, so
// the operation never mixes accounts and OUs in one answer — a caller walking the
// tree downward asks twice.
func (p *OrganizationsPlugin) listChildren(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ParentID   string `json:"ParentId"`
		ChildType  string `json:"ChildType"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listChildren ensure org: %w", err)
	}
	if input.ChildType == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify a ChildType")
	}
	// An unrecognized ChildType is refused rather than treated as "everything": a
	// caller with a typo would otherwise get a superset of what it asked for and
	// draw conclusions about entity types from a filter that never applied.
	if input.ChildType != orgKindAccount && input.ChildType != orgKindOU {
		return nil, orgInvalidInput("INVALID_ENUM",
			fmt.Sprintf("the ChildType must be %s or %s", orgKindAccount, orgKindOU))
	}
	if err := p.requireOrgParent(goCtx, reqCtx.AccountID, input.ParentID); err != nil {
		return nil, err
	}

	ids, err := p.childrenOfKind(goCtx, input.ParentID, input.ChildType)
	if err != nil {
		return nil, fmt.Errorf("listChildren load children: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	children := make([]OrgChild, 0, len(page))
	for _, id := range page {
		children = append(children, OrgChild{ID: id, Type: input.ChildType})
	}
	out := map[string]interface{}{"Children": children}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listChildren")
}

func (p *OrganizationsPlugin) listAccountsForParent(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ParentID   string `json:"ParentId"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listAccountsForParent ensure org: %w", err)
	}
	if err := p.requireOrgParent(goCtx, reqCtx.AccountID, input.ParentID); err != nil {
		return nil, err
	}

	ids, err := p.childrenOfKind(goCtx, input.ParentID, orgKindAccount)
	if err != nil {
		return nil, fmt.Errorf("listAccountsForParent load children: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	accounts := make([]OrgAccount, 0, len(page))
	for _, id := range page {
		a, loadErr := p.loadAccount(goCtx, id)
		if loadErr != nil {
			return nil, fmt.Errorf("listAccountsForParent load account: %w", loadErr)
		}
		if a == nil {
			continue
		}
		accounts = append(accounts, *a)
	}

	out := map[string]interface{}{"Accounts": accounts}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listAccountsForParent")
}

// --- helpers ---

// requireOrgParent validates a ParentId the way every operation that takes one
// has to. The two refusals are deliberately different: a syntactically impossible
// parent (an account ID, say) is InvalidInputException, while a well-formed root
// or OU ID naming nothing is ParentNotFoundException. Collapsing them would send
// a caller that passed the wrong kind of ID looking for a missing OU.
func (p *OrganizationsPlugin) requireOrgParent(ctx context.Context, acct, parentID string) error {
	if parentID == "" {
		return orgInvalidInput("INPUT_REQUIRED", "you must specify a ParentId")
	}
	if !isOrgRootID(parentID) && !isOrgOUID(parentID) {
		return orgInvalidInput("INVALID_PATTERN",
			"the ParentId must be a root ID or an organizational unit ID: "+parentID)
	}
	kind, err := p.resolveOrgTarget(ctx, acct, parentID)
	if err != nil {
		return fmt.Errorf("resolve parent %s: %w", parentID, err)
	}
	if kind != orgKindRoot && kind != orgKindOU {
		return orgErr("ParentNotFoundException", "We can't find a root or OU with the ParentId "+parentID)
	}
	return nil
}

// requireOU loads the OU an OrganizationalUnitId names, refusing with the code
// the operation documents. A malformed ID is InvalidInputException for the same
// reason it is on a parent: the caller's ID is the wrong shape, not missing.
func (p *OrganizationsPlugin) requireOU(ctx context.Context, acct, ouID string) (*OrgOrganizationalUnit, error) {
	if ouID == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify an OrganizationalUnitId")
	}
	if !isOrgOUID(ouID) {
		return nil, orgInvalidInput("INVALID_PATTERN",
			"the OrganizationalUnitId must be an organizational unit ID: "+ouID)
	}
	if _, err := p.ensureOrganization(ctx, acct); err != nil {
		return nil, fmt.Errorf("ensure org: %w", err)
	}
	ou, err := p.loadOU(ctx, ouID)
	if err != nil {
		return nil, fmt.Errorf("load OU %s: %w", ouID, err)
	}
	if ou == nil {
		return nil, orgErr("OrganizationalUnitNotFoundException",
			"We can't find an OU with the OrganizationalUnitId "+ouID)
	}
	return ou, nil
}

// childrenOfKind returns the sorted IDs of parent's children of one kind. The
// placement index holds accounts and OUs together, and the ID prefix is what
// tells them apart, so the filter is a read rather than a second index.
func (p *OrganizationsPlugin) childrenOfKind(ctx context.Context, parent, kind string) ([]string, error) {
	children, err := p.loadChildren(ctx, parent)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(children))
	for _, id := range children {
		if (kind == orgKindOU) == isOrgOUID(id) {
			filtered = append(filtered, id)
		}
	}
	return filtered, nil
}

// ouSiblingNameTaken reports whether another OU directly under parent already
// carries name. exceptID is skipped, so an OU does not collide with itself on a
// rename.
func (p *OrganizationsPlugin) ouSiblingNameTaken(ctx context.Context, parent, name, exceptID string) (bool, error) {
	siblings, err := p.childrenOfKind(ctx, parent, orgKindOU)
	if err != nil {
		return false, err
	}
	for _, id := range siblings {
		if id == exceptID {
			continue
		}
		sibling, loadErr := p.loadOU(ctx, id)
		if loadErr != nil {
			return false, loadErr
		}
		if sibling != nil && sibling.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// forgetOU removes every trace of a deleted OU: its record, the organization's OU
// index, both directions of its placement, its policy attachments, and its tags.
// Anything left behind outlives the OU and contradicts the rest of the state.
func (p *OrganizationsPlugin) forgetOU(ctx context.Context, acct, ouID string) error {
	attached, err := p.loadAttachments(ctx, ouID)
	if err != nil {
		return err
	}
	for _, policyID := range attached {
		// Detaching keeps the reverse index honest, so ListTargetsForPolicy stops
		// naming an OU that no longer exists.
		if _, err := p.detachPolicyFrom(ctx, policyID, ouID); err != nil {
			return err
		}
	}
	parent, err := p.loadParent(ctx, ouID)
	if err != nil {
		return err
	}
	if parent != "" {
		if _, err := p.orgRemoveID(ctx, orgChildrenKey(parent), ouID); err != nil {
			return err
		}
	}
	if _, err := p.orgRemoveID(ctx, orgOUIDsKey(acct), ouID); err != nil {
		return err
	}
	for _, key := range []string{
		orgOUKey(ouID),
		orgParentKey(ouID),
		orgChildrenKey(ouID),
		orgAttachmentsKey(ouID),
		orgTagsKey(ouID),
	} {
		if err := p.orgDeleteKey(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// orgDeleteKey removes a state key outright. Deletion is the one thing the
// foundation's storage helpers do not cover, and an emptied-but-present index key
// would keep a deleted entity's shadow in a state dump.
func (p *OrganizationsPlugin) orgDeleteKey(ctx context.Context, key string) error {
	if err := p.state.Delete(ctx, organizationsNamespace, key); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// isOrgAccountID reports whether id has the exactly-12-digits shape the model's
// AccountId pattern requires.
func isOrgAccountID(id string) bool {
	if len(id) != 12 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}
