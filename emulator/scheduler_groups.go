package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Schedule groups and their tags (#1501).
//
// `API_CreateScheduleGroup`, `API_GetScheduleGroup`, `API_ListScheduleGroups` and
// `API_DeleteScheduleGroup` publish the four routes, and `API_TagResource`, `API_UntagResource` and
// `API_ListTagsForResource` the three tag routes, whose ResourceArn pattern admits only a
// `schedule-group/` ARN: "You can only assign tags to schedule groups."
//
// The User Guide's `managing-schedule-group.html` publishes the rest: "Your AWS account comes with a
// default scheduler group"; a group "has two possible states: ACTIVE and DELETING"; "When you delete
// a group, the state changes to DELETING until EventBridge Scheduler finishes the deletion of the
// associated schedules. After EventBridge Scheduler deletes the schedules in the group, the group is
// no longer available in your account." `managing-schedule-group-delete.html` adds "You can't
// delete, or edit, the default group".
//
// # The default group
//
// The `default` group exists in every account and Region without a create. Substrate materializes
// it the first time a Scheduler request reaches an account and Region, at that request's simulated
// time, so `GetScheduleGroup(Name="default")` and `ListScheduleGroups` report a creation date that a
// replay reproduces.
//
// # Deletion
//
// `DeleteScheduleGroup` deletes the group's schedules and then the group, inside the one request.
// DELETING is therefore a state no later request observes: by the time the delete answers, every
// schedule in the group is gone, which is the condition the guide gives for the group to go too.
// No schedule of a deleted group fires afterwards.

// schedulerDefaultGroup is the group every account and Region has.
const schedulerDefaultGroup = "default"

// schedulerGroupStateActive is the state a group reports from creation until it is deleted.
const schedulerGroupStateActive = "ACTIVE"

// schedulerTagsMaxItems is the published maximum of Tags and TagKeys.
const schedulerTagsMaxItems = 200

// schedulerGroupARNPattern is the published ResourceArn pattern of the three tag operations.
var schedulerGroupARNPattern = regexp.MustCompile( //nolint:gochecknoglobals // immutable compiled pattern
	`^arn:aws[a-z-]*:scheduler:([a-z0-9\-]+):(\d{12}):schedule-group/([0-9a-zA-Z-_.]+)$`)

// schedulerGroupRecord is a stored schedule group. It carries no account or Region: both are in its
// state key.
type schedulerGroupRecord struct {
	// Name is the group's name.
	Name string `json:"name"`
	// ARN is the group's ARN.
	ARN string `json:"arn"`
	// State is ACTIVE.
	State string `json:"state"`
	// CreationDate is the RFC 3339 time the group was created.
	CreationDate string `json:"creation_date"`
	// LastModificationDate is the RFC 3339 time the group was last modified.
	LastModificationDate string `json:"last_modification_date"`
	// Tags are the group's tags, sorted by key.
	Tags []schedulerTag `json:"tags,omitempty"`
}

// schedulerTag is one tag, in the published shape on the wire and in state.
type schedulerTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// schedGroupKey is the state key of one group.
func schedGroupKey(accountID, region, name string) string {
	return "sched_group:" + accountID + "/" + region + "/" + name
}

// schedGroupNamesKey is the state key of an account and Region's group-name index.
func schedGroupNamesKey(accountID, region string) string {
	return "sched_group_names:" + accountID + "/" + region
}

// schedGroupARN returns a group's ARN.
func schedGroupARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule-group/%s", region, accountID, name)
}

// schedulerGroupNotFound is the refusal for a group that does not exist, in the wording
// `managing-schedule-group-delete.html` shows GetScheduleGroup answering.
func schedulerGroupNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("Schedule group %s does not exist.", name),
		HTTPStatus: http.StatusNotFound,
	}
}

// ensureDefaultGroup materializes the account and Region's default group if it is not yet stored.
func (p *SchedulerPlugin) ensureDefaultGroup(ctx context.Context, accountID, region string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ensureDefaultGroupLocked(ctx, accountID, region)
}

// ensureDefaultGroupLocked is [SchedulerPlugin.ensureDefaultGroup] for a caller holding p.mu.
func (p *SchedulerPlugin) ensureDefaultGroupLocked(ctx context.Context, accountID, region string) error {
	key := schedGroupKey(accountID, region, schedulerDefaultGroup)
	data, err := p.state.Get(ctx, schedulerNamespace, key)
	if err != nil {
		return fmt.Errorf("scheduler default group get: %w", err)
	}
	if data != nil {
		return nil
	}
	now := p.tc.Now().UTC().Format(time.RFC3339)
	return p.putGroup(ctx, accountID, region, &schedulerGroupRecord{
		Name:                 schedulerDefaultGroup,
		ARN:                  schedGroupARN(region, accountID, schedulerDefaultGroup),
		State:                schedulerGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
	})
}

// loadGroup reads one group, nil when it does not exist.
func (p *SchedulerPlugin) loadGroup(ctx context.Context, accountID, region, name string) (*schedulerGroupRecord, error) {
	data, err := p.state.Get(ctx, schedulerNamespace, schedGroupKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("scheduler load group: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var g schedulerGroupRecord
	if err := json.Unmarshal(data, &g); err != nil {
		return nil, fmt.Errorf("scheduler decode group: %w", err)
	}
	return &g, nil
}

// putGroup stores a group and indexes its name.
func (p *SchedulerPlugin) putGroup(ctx context.Context, accountID, region string, g *schedulerGroupRecord) error {
	data, err := json.Marshal(g)
	if err != nil {
		return fmt.Errorf("scheduler marshal group: %w", err)
	}
	if err := p.state.Put(ctx, schedulerNamespace, schedGroupKey(accountID, region, g.Name), data); err != nil {
		return fmt.Errorf("scheduler put group: %w", err)
	}
	if err := updateStringIndex(ctx, p.state, schedulerNamespace, schedGroupNamesKey(accountID, region), g.Name); err != nil {
		return fmt.Errorf("scheduler index group: %w", err)
	}
	return nil
}

// groupNames lists an account and Region's group names, sorted.
func (p *SchedulerPlugin) groupNames(ctx context.Context, accountID, region string) ([]string, error) {
	names, err := loadStringIndex(ctx, p.state, schedulerNamespace, schedGroupNamesKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("scheduler group names: %w", err)
	}
	return names, nil
}

// schedGroupNameFromPath extracts the name from /schedule-groups/{Name}.
func schedGroupNameFromPath(path string) string {
	return strings.TrimPrefix(path, "/schedule-groups/")
}

// schedulerGroupWire is GetScheduleGroup's response and ListScheduleGroups' ScheduleGroupSummary.
type schedulerGroupWire struct {
	Arn                  string  `json:"Arn"`
	CreationDate         float64 `json:"CreationDate"`
	LastModificationDate float64 `json:"LastModificationDate"`
	Name                 string  `json:"Name"`
	State                string  `json:"State"`
}

func (g *schedulerGroupRecord) wire() schedulerGroupWire {
	ct, _ := time.Parse(time.RFC3339, g.CreationDate)
	mt, _ := time.Parse(time.RFC3339, g.LastModificationDate)
	return schedulerGroupWire{
		Arn:                  g.ARN,
		CreationDate:         float64(ct.Unix()),
		LastModificationDate: float64(mt.Unix()),
		Name:                 g.Name,
		State:                g.State,
	}
}

// schedulerValidateTags checks a Tags array against `API_Tag`: Key 1–128, Value 1–256, both
// Required, at most 200 items.
func schedulerValidateTags(tags []schedulerTag) *AWSError {
	if len(tags) > schedulerTagsMaxItems {
		return schedulerValidation(fmt.Sprintf("1 validation error detected: Value at 'tags' failed to "+
			"satisfy constraint: Member must have length less than or equal to %d", schedulerTagsMaxItems))
	}
	for i, t := range tags {
		member := fmt.Sprintf("tags.%d.member", i+1)
		if t.Key == "" {
			return schedulerValidation(schedulerRequiredMessage(member + ".key"))
		}
		if err := schedulerValidateLength(member+".key", t.Key, 128); err != nil {
			return err
		}
		if t.Value == "" {
			return schedulerValidation(schedulerRequiredMessage(member + ".value"))
		}
		if err := schedulerValidateLength(member+".value", t.Value, 256); err != nil {
			return err
		}
	}
	return nil
}

// mergeSchedulerTags applies add over existing, removing the keys in remove, sorted by key.
func mergeSchedulerTags(existing, add []schedulerTag, remove []string) []schedulerTag {
	m := make(map[string]string, len(existing)+len(add))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for _, t := range add {
		m[t.Key] = t.Value
	}
	for _, k := range remove {
		delete(m, k)
	}
	out := make([]schedulerTag, 0, len(m))
	for k, v := range m {
		out = append(out, schedulerTag{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) == 0 {
		return nil
	}
	return out
}

// --- Operations --------------------------------------------------------------

func (p *SchedulerPlugin) createScheduleGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedGroupNameFromPath(req.Path)
	var body struct {
		ClientToken string         `json:"ClientToken"`
		Tags        []schedulerTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, schedulerInvalidBody()
		}
	}
	if err := schedulerValidateName("name", name); err != nil {
		return nil, err
	}
	if err := schedulerValidateTags(body.Tags); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	if err := p.ensureDefaultGroup(goCtx, ctx.AccountID, ctx.Region); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	existing, err := p.loadGroup(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ConflictException",
			Message:    fmt.Sprintf("Schedule group %s already exists.", name),
			HTTPStatus: http.StatusConflict,
		}
	}
	now := p.tc.Now().UTC().Format(time.RFC3339)
	g := &schedulerGroupRecord{
		Name:                 name,
		ARN:                  schedGroupARN(ctx.Region, ctx.AccountID, name),
		State:                schedulerGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
		Tags:                 mergeSchedulerTags(nil, body.Tags, nil),
	}
	if err := p.putGroup(goCtx, ctx.AccountID, ctx.Region, g); err != nil {
		return nil, err
	}
	return schedulerJSONResponse(http.StatusOK, struct {
		ScheduleGroupArn string `json:"ScheduleGroupArn"`
	}{ScheduleGroupArn: g.ARN})
}

func (p *SchedulerPlugin) getScheduleGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedGroupNameFromPath(req.Path)
	if err := schedulerValidateName("name", name); err != nil {
		return nil, err
	}
	if err := p.ensureDefaultGroup(context.Background(), ctx.AccountID, ctx.Region); err != nil {
		return nil, err
	}
	g, err := p.loadGroup(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, schedulerGroupNotFound(name)
	}
	return schedulerJSONResponse(http.StatusOK, g.wire())
}

func (p *SchedulerPlugin) listScheduleGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	offset, ok := decodeOffsetPaginationToken(req.Params[schedListNextTokenKey])
	if !ok {
		return nil, schedulerValidation("1 validation error detected: Value at 'nextToken' failed to " +
			"satisfy constraint: Member must be a token returned by a previous ListScheduleGroups request")
	}
	maxResults := schedListDefaultMaxResults
	if mr := req.Params[schedListMaxResultsKey]; mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 {
			maxResults = min(n, schedListMaxMaxResults)
		}
	}
	prefix := req.Params[schedListNamePrefixKey]

	goCtx := context.Background()
	if err := p.ensureDefaultGroup(goCtx, ctx.AccountID, ctx.Region); err != nil {
		return nil, err
	}
	names, err := p.groupNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	groups := make([]schedulerGroupWire, 0, len(names))
	for _, n := range names {
		if prefix != "" && !strings.HasPrefix(n, prefix) {
			continue
		}
		g, err := p.loadGroup(goCtx, ctx.AccountID, ctx.Region, n)
		if err != nil {
			return nil, err
		}
		if g == nil {
			continue
		}
		groups = append(groups, g.wire())
	}
	page, next := pageByOffsetToken(groups, offset, maxResults)
	return schedulerJSONResponse(http.StatusOK, struct {
		NextToken      string               `json:"NextToken,omitempty"`
		ScheduleGroups []schedulerGroupWire `json:"ScheduleGroups"`
	}{NextToken: next, ScheduleGroups: page})
}

func (p *SchedulerPlugin) deleteScheduleGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := schedGroupNameFromPath(req.Path)
	if err := schedulerValidateName("name", name); err != nil {
		return nil, err
	}
	if name == schedulerDefaultGroup {
		// "You can't delete, or edit, the default group." No page publishes the refusal's code;
		// ValidationException is the one this service publishes for a request it will not serve.
		return nil, schedulerValidation("The default schedule group cannot be deleted.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	g, err := p.loadGroup(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, schedulerGroupNotFound(name)
	}
	scheduleNames, err := loadStringIndex(goCtx, p.state, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("scheduler deleteScheduleGroup index: %w", err)
	}
	for _, s := range scheduleNames {
		if err := p.removeSchedule(goCtx, ctx.AccountID, ctx.Region, name, s); err != nil {
			return nil, err
		}
	}
	if err := p.state.Delete(goCtx, schedulerNamespace, schedNamesKey(ctx.AccountID, ctx.Region, name)); err != nil {
		return nil, fmt.Errorf("scheduler deleteScheduleGroup names: %w", err)
	}
	if err := p.state.Delete(goCtx, schedulerNamespace, schedGroupKey(ctx.AccountID, ctx.Region, name)); err != nil {
		return nil, fmt.Errorf("scheduler deleteScheduleGroup: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, schedulerNamespace, schedGroupNamesKey(ctx.AccountID, ctx.Region), name); err != nil {
		return nil, fmt.Errorf("scheduler deleteScheduleGroup group index: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// tagTarget resolves the group a tag operation's ResourceArn names. The caller holds p.mu.
func (p *SchedulerPlugin) tagTarget(ctx *RequestContext, req *AWSRequest) (*schedulerGroupRecord, error) {
	arn := strings.TrimPrefix(req.Path, "/tags/")
	if arn == "" {
		return nil, schedulerValidation(schedulerRequiredMessage("resourceArn"))
	}
	m := schedulerGroupARNPattern.FindStringSubmatch(arn)
	if m == nil {
		return nil, schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%s' at "+
			"'resourceArn' failed to satisfy constraint: Member must satisfy regular expression pattern: "+
			`arn:aws[a-z-]*:scheduler:[a-z0-9\-]+:\d{12}:schedule-group\/[0-9a-zA-Z-_.]+`, arn))
	}
	region, account, name := m[1], m[2], m[3]
	if region != ctx.Region || account != ctx.AccountID {
		return nil, schedulerGroupNotFound(name)
	}
	if name == schedulerDefaultGroup {
		if err := p.ensureDefaultGroupLocked(context.Background(), account, region); err != nil {
			return nil, err
		}
	}
	g, err := p.loadGroup(context.Background(), account, region, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, schedulerGroupNotFound(name)
	}
	return g, nil
}

func (p *SchedulerPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Tags *[]schedulerTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, schedulerInvalidBody()
		}
	}
	if body.Tags == nil {
		return nil, schedulerValidation(schedulerRequiredMessage("tags"))
	}
	if err := schedulerValidateTags(*body.Tags); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.tagTarget(ctx, req)
	if err != nil {
		return nil, err
	}
	g.Tags = mergeSchedulerTags(g.Tags, *body.Tags, nil)
	if err := p.putGroup(context.Background(), ctx.AccountID, ctx.Region, g); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

func (p *SchedulerPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys := req.MultiValueParams["TagKeys"]
	if len(keys) == 0 {
		if k, ok := req.Params["TagKeys"]; ok {
			keys = []string{k}
		}
	}
	if len(keys) == 0 {
		return nil, schedulerValidation(schedulerRequiredMessage("tagKeys"))
	}
	if len(keys) > schedulerTagsMaxItems {
		return nil, schedulerValidation(fmt.Sprintf("1 validation error detected: Value at 'tagKeys' "+
			"failed to satisfy constraint: Member must have length less than or equal to %d", schedulerTagsMaxItems))
	}
	for _, k := range keys {
		if k == "" {
			return nil, schedulerValidation("1 validation error detected: Value at 'tagKeys' failed to " +
				"satisfy constraint: Member must have length greater than or equal to 1")
		}
		if err := schedulerValidateLength("tagKeys", k, 128); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.tagTarget(ctx, req)
	if err != nil {
		return nil, err
	}
	g.Tags = mergeSchedulerTags(g.Tags, nil, keys)
	if err := p.putGroup(context.Background(), ctx.AccountID, ctx.Region, g); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

func (p *SchedulerPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.tagTarget(ctx, req)
	if err != nil {
		return nil, err
	}
	tags := g.Tags
	if tags == nil {
		tags = []schedulerTag{}
	}
	return schedulerJSONResponse(http.StatusOK, struct {
		Tags []schedulerTag `json:"Tags"`
	}{Tags: tags})
}
