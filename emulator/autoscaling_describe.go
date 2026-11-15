package emulator

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The describes, their pagination and their filters.
//
// Every describe pages on NextToken and MaxRecords. The token is a base64 cursor naming the
// last item of the page it ended, so a token substrate did not issue fails to decode and is
// refused with InvalidNextToken, which every one of these operations publishes. MaxRecords
// outside the published range is refused with ValidationError, the Coral constraint
// message's shape, rather than clamped: a consumer paging by a size AWS would refuse must
// fail here as it would there.

// asgPageParams reads MaxRecords and NextToken, with the operation's published default and
// maximum.
func asgPageParams(params map[string]string, defaultMax, maxMax int) (int, string, *AWSError) {
	n, ok, awsErr := asgInt(params, "MaxRecords")
	if awsErr != nil {
		return 0, "", awsErr
	}
	if !ok {
		n = defaultMax
	}
	if n < 1 || n > maxMax {
		return 0, "", asgValidationError("1 validation error detected: Value '%d' at 'maxRecords' failed to satisfy constraint: Member must have value between 1 and %d", n, maxMax)
	}
	token := params["NextToken"]
	if token == "" {
		return n, "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil || len(raw) == 0 {
		return 0, "", &AWSError{Code: "InvalidNextToken", Message: "The token '" + token + "' is invalid.", HTTPStatus: 400}
	}
	return n, string(raw), nil
}

// asgPage returns one page of items, which are in the operation's published order and keyed
// by key: the items after the one whose key is cursor, up to maxRecords, and the token for
// the next page. A cursor naming no item — one that was deleted between pages — is refused,
// since resuming from an arbitrary place would silently skip or repeat items.
func asgPage[T any](items []T, key func(T) string, cursor string, maxRecords int) ([]T, string, *AWSError) {
	start := 0
	if cursor != "" {
		start = -1
		for i, it := range items {
			if key(it) == cursor {
				start = i + 1
				break
			}
		}
		if start < 0 {
			return nil, "", &AWSError{Code: "InvalidNextToken", Message: "The token is invalid.", HTTPStatus: 400}
		}
	}
	end := start + maxRecords
	if end >= len(items) {
		return items[start:], "", nil
	}
	return items[start:end], base64.StdEncoding.EncodeToString([]byte(key(items[end-1]))), nil
}

// asgRequestFilter is one Filters.member.N: a name and its values, in the order sent.
type asgRequestFilter struct {
	name   string
	values []string
}

// asgParseFilters reads the Filters list.
func asgParseFilters(params map[string]string) []asgRequestFilter {
	var out []asgRequestFilter
	for _, m := range iamMemberStructs(params, "Filters") {
		out = append(out, asgRequestFilter{name: m["Name"], values: iamMemberList(m, "Values")})
	}
	return out
}

// asgGroupMatchesFilters applies DescribeAutoScalingGroups' tag filters, which API_Filter
// lists as tag-key, tag-value and tag:<key>. Filters are ANDed and a filter's values ORed;
// values are case-sensitive. A filter name outside those three is refused rather than
// ignored, since ignoring it answers more groups than the caller asked for.
func asgGroupMatchesFilters(g *AutoScalingGroup, filters []asgRequestFilter) (bool, *AWSError) {
	for _, f := range filters {
		matched := false
		switch {
		case f.name == "tag-key":
			for _, t := range g.Tags {
				matched = matched || containsStr(f.values, t.Key)
			}
		case f.name == "tag-value":
			for _, t := range g.Tags {
				matched = matched || containsStr(f.values, t.Value)
			}
		case strings.HasPrefix(f.name, "tag:"):
			key := strings.TrimPrefix(f.name, "tag:")
			for _, t := range g.Tags {
				matched = matched || (t.Key == key && containsStr(f.values, t.Value))
			}
		default:
			return false, asgValidationError("Filter type %s is not correct. Allowed Filter types are: tag-key tag-value tag:<key>", f.name)
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

// observeGroups reconciles every group an observation reads, so an instance terminated
// through EC2 is noticed and replaced before the describe answers. See the package comment.
// A group the reconciliation leaves unchanged is not written back.
func (p *AutoScalingPlugin) observeGroups(reqCtx *RequestContext, groups []*AutoScalingGroup) error {
	for _, g := range groups {
		if err := p.observe(reqCtx, g); err != nil {
			return err
		}
	}
	return nil
}

func (p *AutoScalingPlugin) describeAutoScalingGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	maxRecords, cursor, awsErr := asgPageParams(req.Params, 50, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	names := iamMemberList(req.Params, "AutoScalingGroupNames")
	filters := asgParseFilters(req.Params)
	includeInstances := true
	if v, ok := asgBool(req.Params, "IncludeInstances"); ok {
		includeInstances = v
	}
	all, err := p.listGroups(reqCtx)
	if err != nil {
		return nil, err
	}
	var selected []*AutoScalingGroup
	for _, g := range all {
		if len(names) > 0 && !containsStr(names, g.Name) {
			continue
		}
		ok, awsErr := asgGroupMatchesFilters(g, filters)
		if awsErr != nil {
			return nil, awsErr
		}
		if ok {
			selected = append(selected, g)
		}
	}
	page, next, awsErr := asgPage(selected, func(g *AutoScalingGroup) string { return g.Name }, cursor, maxRecords)
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.observeGroups(reqCtx, page); err != nil {
		return nil, err
	}
	type result struct {
		AutoScalingGroups asgList[asgGroupXML] `xml:"AutoScalingGroups"`
		NextToken         string               `xml:"NextToken,omitempty"`
	}
	out := result{NextToken: next}
	for _, g := range page {
		out.AutoScalingGroups.Members = append(out.AutoScalingGroups.Members, asgGroupToXML(g, includeInstances))
	}
	return asgOKResponse(reqCtx, "DescribeAutoScalingGroups", out)
}

func (p *AutoScalingPlugin) describeAutoScalingInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	maxRecords, cursor, awsErr := asgPageParams(req.Params, 50, 50)
	if awsErr != nil {
		return nil, awsErr
	}
	ids := iamMemberList(req.Params, "InstanceIds")
	if len(ids) > 50 {
		return nil, asgValidationError("1 validation error detected: Value at 'instanceIds' failed to satisfy constraint: Member must have length less than or equal to 50")
	}
	all, err := p.listGroups(reqCtx)
	if err != nil {
		return nil, err
	}
	// Only the groups the answer is about are observed: every group when no InstanceIds were
	// named, otherwise the ones holding one of them.
	groups := all
	if len(ids) > 0 {
		groups = nil
		for _, g := range all {
			for _, id := range ids {
				if g.instanceIndex(id) >= 0 {
					groups = append(groups, g)
					break
				}
			}
		}
	}
	if err := p.observeGroups(reqCtx, groups); err != nil {
		return nil, err
	}
	var items []asgInstanceDetailsXML
	for _, g := range groups {
		for _, inst := range g.Instances {
			// "If you specify an ID that does not exist, it is ignored with no error."
			if len(ids) > 0 && !containsStr(ids, inst.InstanceID) {
				continue
			}
			lt := inst.LaunchTemplate
			items = append(items, asgInstanceDetailsXML{
				InstanceID:           inst.InstanceID,
				InstanceType:         inst.InstanceType,
				AutoScalingGroupName: g.Name,
				AvailabilityZone:     inst.AvailabilityZone,
				LifecycleState:       asgLifecycleState,
				// The published sample spells this member in capitals here ("HEALTHY") and in
				// title case in DescribeAutoScalingGroups ("Healthy"); both are reproduced.
				HealthStatus:         strings.ToUpper(inst.HealthStatus),
				LaunchTemplate:       asgSpecXML(&lt),
				ImageID:              inst.ImageID,
				ProtectedFromScaleIn: inst.ProtectedFromScaleIn,
				WeightedCapacity:     asgWeightedCapacityXML(g, &inst),
			})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].InstanceID < items[j].InstanceID })
	page, next, awsErr := asgPage(items, func(it asgInstanceDetailsXML) string { return it.InstanceID }, cursor, maxRecords)
	if awsErr != nil {
		return nil, awsErr
	}
	type result struct {
		AutoScalingInstances asgList[asgInstanceDetailsXML] `xml:"AutoScalingInstances"`
		NextToken            string                         `xml:"NextToken,omitempty"`
	}
	return asgOKResponse(reqCtx, "DescribeAutoScalingInstances", result{AutoScalingInstances: asgListOf(page), NextToken: next})
}

func (p *AutoScalingPlugin) describeScalingActivities(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	maxRecords, cursor, awsErr := asgPageParams(req.Params, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	name := req.Params["AutoScalingGroupName"]
	activityIDs := iamMemberList(req.Params, "ActivityIds")
	includeDeleted, _ := asgBool(req.Params, "IncludeDeletedGroups")

	var lower, upper *time.Time
	var statuses []string
	for _, f := range asgParseFilters(req.Params) {
		switch f.name {
		case "Status":
			if name == "" {
				// API_DescribeScalingActivities: Status "can only be used in combination with
				// the AutoScalingGroupName parameter".
				return nil, asgValidationError("The Status filter can only be used in combination with the AutoScalingGroupName parameter.")
			}
			statuses = append(statuses, f.values...)
		case "StartTimeLowerBound", "StartTimeUpperBound":
			if len(f.values) == 0 {
				continue
			}
			t, err := asgParseISO8601(f.values[0])
			if err != nil {
				return nil, asgValidationError("%s must be an ISO 8601 timestamp: %s", f.name, f.values[0])
			}
			if f.name == "StartTimeLowerBound" {
				lower = &t
			} else {
				upper = &t
			}
		default:
			return nil, asgValidationError("Filter type %s is not correct. Allowed Filter types are: StartTimeLowerBound StartTimeUpperBound Status", f.name)
		}
	}

	groups, err := p.listGroups(reqCtx)
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	var observed []*AutoScalingGroup
	for _, g := range groups {
		live[g.ARN] = true
		if name == "" || g.Name == name {
			observed = append(observed, g)
		}
	}
	if err := p.observeGroups(reqCtx, observed); err != nil {
		return nil, err
	}

	all, err := p.listActivities(reqCtx)
	if err != nil {
		return nil, err
	}
	var selected []AutoScalingActivity
	for _, a := range all {
		deleted := !live[a.GroupARN]
		switch {
		case name != "" && a.GroupName != name,
			deleted && !includeDeleted,
			len(activityIDs) > 0 && !containsStr(activityIDs, a.ActivityID),
			len(statuses) > 0 && !containsStr(statuses, a.StatusCode),
			lower != nil && a.StartTime.Before(*lower),
			upper != nil && a.StartTime.After(*upper):
			continue
		}
		selected = append(selected, a)
	}
	page, next, awsErr := asgPage(selected, func(a AutoScalingActivity) string { return strconv.FormatInt(a.Seq, 10) }, cursor, maxRecords)
	if awsErr != nil {
		return nil, awsErr
	}
	type result struct {
		Activities asgList[asgActivityXML] `xml:"Activities"`
		NextToken  string                  `xml:"NextToken,omitempty"`
	}
	out := result{NextToken: next}
	for _, a := range page {
		state := ""
		if includeDeleted {
			state = "InService"
			if !live[a.GroupARN] {
				state = "Deleted"
			}
		}
		out.Activities.Members = append(out.Activities.Members, asgActivityToXML(a, state))
	}
	return asgOKResponse(reqCtx, "DescribeScalingActivities", out)
}

// asgParseISO8601 parses a filter timestamp. "Timestamps without a timezone offset are
// assumed to be UTC" (API_DescribeScalingActivities).
func asgParseISO8601(v string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02T15:04:05.000"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("not an ISO 8601 timestamp: %s", v)
}

func (p *AutoScalingPlugin) terminateInstanceInAutoScalingGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	decrement, ok := asgBool(req.Params, "ShouldDecrementDesiredCapacity")
	if !ok {
		return nil, asgMissingParameter("ShouldDecrementDesiredCapacity")
	}
	single := req.Params["InstanceId"]
	ids := iamMemberList(req.Params, "InstanceIds")
	switch {
	case single != "" && len(ids) > 0:
		return nil, asgValidationError("Specify either InstanceId or InstanceIds, not both.")
	case single == "" && len(ids) == 0:
		return nil, asgMissingParameter("InstanceId")
	case len(ids) > 0 && req.Params["AutoScalingGroupName"] == "":
		// "This parameter requires that you also specify AutoScalingGroupName."
		return nil, asgMissingParameter("AutoScalingGroupName")
	case len(ids) > 100:
		return nil, asgValidationError("You can specify up to 100 instances.")
	}

	// Validation, all of it, before anything is terminated: a refusal leaves EC2, the activity
	// log and the group exactly as they were. A duplicate ID is refused rather than collapsed,
	// since the request would otherwise ask to terminate one instance twice.
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return nil, asgValidationError("The instance ID %s is specified more than once.", id)
		}
		seen[id] = true
	}

	var g *AutoScalingGroup
	var err error
	if single != "" {
		ids = []string{single}
		if g, err = asgGroupForInstance(p.state, reqCtx, single); err != nil {
			return nil, err
		}
		if g == nil {
			// Observed wording.
			return nil, asgValidationError("Instance Id not found - No managed instance found for instance ID: %s", single)
		}
		// The instance decides the group — that is what the authorization decision is made
		// about too (autoscaling_authz.go) — so a request also naming a different group is
		// contradictory rather than a second way to address the instance. The reference
		// documents AutoScalingGroupName only as "Required when using InstanceIds"; the
		// refusal is substrate's, in the common ValidationError.
		if named := req.Params["AutoScalingGroupName"]; named != "" && named != g.Name {
			return nil, asgValidationError("The instance %s is not part of Auto Scaling group %s.", single, named)
		}
	} else if g, err = p.requireGroup(reqCtx, req); err != nil {
		return nil, err
	}
	g.refreshWeights()
	members := make([]AutoScalingInstance, 0, len(ids))
	for _, id := range ids {
		idx := g.instanceIndex(id)
		if idx < 0 {
			return nil, asgValidationError("Instance Id not found - No managed instance found for instance ID: %s", id)
		}
		members = append(members, g.Instances[idx])
	}
	// A decrement lowers desired by each instance's weight — its capacity units — never below
	// zero; for an unweighted group that is one per instance.
	decremented := g.DesiredCapacity
	for i := range members {
		decremented = max(decremented-members[i].weightOf(), 0)
	}
	if decrement && decremented < g.MinSize {
		// Observed wording.
		return nil, asgValidationError("Currently, desiredSize equals minSize (%d). Terminating instance without replacement will violate group's min size constraint. Either set shouldDecrementDesiredCapacity flag to false or lower group's min size.", g.MinSize)
	}

	// One EC2 termination for the whole batch. EC2 resolves every instance before terminating
	// any, so it either terminates all of them or none, and termination protection does not
	// apply to a group's terminations (see terminateEC2). Only once it has succeeded are the
	// activities recorded and the group changed — and the group is saved once, by reconcile.
	if err := p.terminateEC2(reqCtx, ids); err != nil {
		return nil, err
	}
	var activities []AutoScalingActivity
	for _, inst := range members {
		id := inst.InstanceID
		idx := g.instanceIndex(id)
		from := g.capacity()
		g.Instances = append(g.Instances[:idx:idx], g.Instances[idx+1:]...)
		now := asgCauseTime(p.tc.Now())
		cause := "At " + now + " instance " + id + " was taken out of service in response to a user request."
		if decrement {
			g.DesiredCapacity = max(g.DesiredCapacity-inst.weightOf(), 0)
			// The published sample: "…in response to a user request, shrinking the capacity
			// from 4 to 3." The capacity is in the group's units.
			cause = fmt.Sprintf("At %s instance %s was taken out of service in response to a user request, shrinking the capacity from %d to %d.", now, id, from, g.capacity())
		}
		a, err := p.record(reqCtx, g, "Successful", "Terminating EC2 instance: "+id, cause,
			asgDetails(inst.AvailabilityZone, inst.SubnetID), "")
		if err != nil {
			return nil, err
		}
		activities = append(activities, *a)
	}
	// "If you do not specify the option to decrement the desired capacity, Amazon EC2 Auto
	// Scaling launches instances to replace the ones that are terminated."
	if err := p.reconcile(reqCtx, g, ""); err != nil {
		return nil, err
	}

	type result struct {
		Activity   *asgActivityXML          `xml:"Activity,omitempty"`
		Activities *asgList[asgActivityXML] `xml:"Activities,omitempty"`
	}
	var out result
	if single != "" {
		x := asgInProgressXML(activities[0])
		out.Activity = &x
	} else {
		// "When terminating multiple instances, the response populates Activities instead of
		// Activity."
		xs := make([]asgActivityXML, 0, len(activities))
		for _, a := range activities {
			xs = append(xs, asgInProgressXML(a))
		}
		out.Activities = &asgList[asgActivityXML]{Members: xs}
	}
	return asgOKResponse(reqCtx, "TerminateInstanceInAutoScalingGroup", out)
}

// secondsDuration converts a whole number of seconds to a Duration.
func secondsDuration(s int) time.Duration { return time.Duration(s) * time.Second }
