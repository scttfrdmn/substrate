package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CloudWatch dashboards: PutDashboard, GetDashboard, ListDashboards and DeleteDashboards (#1613).
//
// A dashboard is a name and a JSON body. Substrate records the body as the caller sent it and
// answers it back byte for byte; it does not render a widget or evaluate a metric a widget names,
// which is what the console does with a body rather than what the API observes of it.
//
// # Global, not regional
//
// "All dashboards in your account are global, not region-specific" (API_PutDashboard), so the
// record is keyed by account and name alone, a dashboard put through one Region's endpoint is read
// through any other, and its ARN has an empty Region segment:
// arn:aws:cloudwatch::{account}:dashboard/{name}.
//
// # What a record carries
//
// The body, the ARN, LastModified on the simulated clock (set by every Put, since a Put replaces
// the entire contents), and the tags a creating Put sent. Size, which ListDashboards reports, is
// the body's length in bytes. The tags are validated and kept, but TagResource and
// ListTagsForResource still answer ResourceNotFoundException for a dashboard ARN, as they did
// before dashboards were modeled.
//
// # What is refused
//
//   - PutDashboard: InvalidParameterInput/400 (the DashboardInvalidInputError shape) for a body that
//     is not a JSON object, with the message "The field DashboardBody must be a valid JSON object",
//     which is CloudWatch's own answer as boto3 and CloudFormation report it; and for a missing or
//     malformed DashboardName, a missing body, or a tag outside the published limits, whose messages
//     are substrate's own. A body that is a JSON object is accepted with empty
//     DashboardValidationMessages: substrate does not validate widgets, so it never has a warning to
//     report.
//   - GetDashboard: ResourceNotFound/404 (the DashboardNotFoundError shape), "Dashboard {name} does
//     not exist", as the AWS provider for Terraform reports it
//     (hashicorp/terraform-provider-aws#25548).
//   - DeleteDashboards: the same ResourceNotFound for the first named dashboard that does not exist,
//     after deleting every named one that does — "If there is an error during this call, the
//     operation attempts to delete as many dashboards as possible". The current model no longer
//     lists DashboardNotFoundError for DeleteDashboards, though the aws-sdk-go (v1) model did;
//     substrate answers it because a delete of a name that is not there has to say so somehow, and
//     that is the code the service has used for it. It also answers InvalidParameterValue/400
//     for no names or more than the published 100.
//   - ListDashboards: InvalidParameterValue/400 for a NextToken substrate did not issue for the
//     same DashboardNamePrefix. InvalidParameterValue is the only client error the page publishes;
//     the message is substrate's own.

// cwDashboardKeyPrefix is the state-key prefix of a dashboard record:
// dashboard:{account}/{name}.
const cwDashboardKeyPrefix = "dashboard:"

// cwListDashboardsPageSize is ListDashboards' page: "ListDashboards returns up to 1000 results on
// one page".
const cwListDashboardsPageSize = 1000

// cwDeleteDashboardsMax is DeleteDashboards' published limit of 100 names per request.
const cwDeleteDashboardsMax = 100

// cwDashboardNameMax is DashboardName's published maximum length.
const cwDashboardNameMax = 255

// cwDashboard is one stored dashboard.
type cwDashboard struct {
	Name         string            `json:"name"`
	ARN          string            `json:"arn"`
	Body         string            `json:"body"`
	LastModified time.Time         `json:"last_modified"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// cwDashboardKey returns the state key of account's dashboard name.
func cwDashboardKey(account, name string) string {
	return cwDashboardKeyPrefix + account + "/" + name
}

// cwDashboardARN returns the ARN of account's dashboard name. Dashboards are global, so the Region
// segment is empty.
func cwDashboardARN(account, name string) string {
	return "arn:aws:cloudwatch::" + account + ":dashboard/" + name
}

// cwDashboardInvalidInput is InvalidParameterInput/400, PutDashboard's "Some part of the dashboard
// data is invalid".
func cwDashboardInvalidInput(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterInput", Message: message, HTTPStatus: http.StatusBadRequest}
}

// cwDashboardNotFound is the DashboardNotFoundError for name. Its Query code is ResourceNotFound;
// see [cloudWatchQueryErrorCodes].
func cwDashboardNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "DashboardNotFoundError",
		Message:    "Dashboard " + name + " does not exist",
		HTTPStatus: http.StatusNotFound,
	}
}

// cwDashboardNameValid reports whether name meets DashboardName's published rule: "The maximum
// length is 255, and valid characters are A-Z, a-z, 0-9, "-", and "_"".
func cwDashboardNameValid(name string) bool {
	if name == "" || len(name) > cwDashboardNameMax {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// loadDashboard reads account's dashboard name, reporting false when there is none.
func (p *CloudWatchPlugin) loadDashboard(ctx context.Context, account, name string) (cwDashboard, bool, error) {
	data, err := p.state.Get(ctx, monitoringNamespace, cwDashboardKey(account, name))
	if err != nil {
		return cwDashboard{}, false, fmt.Errorf("cloudwatch load dashboard: %w", err)
	}
	if data == nil {
		return cwDashboard{}, false, nil
	}
	var d cwDashboard
	if err := json.Unmarshal(data, &d); err != nil {
		return cwDashboard{}, false, fmt.Errorf("cloudwatch decode dashboard: %w", err)
	}
	return d, true, nil
}

// putDashboard handles PutDashboard: it creates the dashboard, or replaces an existing one's body.
// As the page says, "If you specify Tags when updating an existing dashboard, the tag updates are
// ignored", and they are.
func (p *CloudWatchPlugin) putDashboard(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DashboardName"]
	if name == "" {
		return nil, cwDashboardInvalidInput("The field DashboardName is required.")
	}
	if !cwDashboardNameValid(name) {
		return nil, cwDashboardInvalidInput("The value for field DashboardName contains invalid characters or is " +
			"longer than 255 characters. It can contain only A-Z, a-z, 0-9, \"-\" and \"_\".")
	}
	body, present := req.Params["DashboardBody"]
	if !present || body == "" {
		return nil, cwDashboardInvalidInput("The field DashboardBody is required.")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(body), &object); err != nil || object == nil {
		return nil, cwDashboardInvalidInput("The field DashboardBody must be a valid JSON object")
	}
	tags, _ := cwParseTagList(req.Params, "Tags")

	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	existing, found, err := p.loadDashboard(goCtx, ctx.AccountID, name)
	if err != nil {
		return nil, err
	}
	record := cwDashboard{
		Name:         name,
		ARN:          cwDashboardARN(ctx.AccountID, name),
		Body:         body,
		LastModified: p.tc.Now().UTC(),
		Tags:         existing.Tags,
	}
	if !found {
		// The limits are the alarm path's, but the code is PutDashboard's: its model declares
		// DashboardInvalidInputError as its one client error, not InvalidParameterValueException.
		if awsErr := cwCheckTags(tags); awsErr != nil {
			return nil, cwDashboardInvalidInput(awsErr.Message)
		}
		record.Tags = tags
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch marshal dashboard: %w", err)
	}
	if err := p.state.Put(goCtx, monitoringNamespace, cwDashboardKey(ctx.AccountID, name), data); err != nil {
		return nil, fmt.Errorf("cloudwatch store dashboard: %w", err)
	}
	return cwRespond(req, "PutDashboard", ctx.RequestID, cwDoc{}.with("DashboardValidationMessages", cwList{}))
}

// getDashboard handles GetDashboard.
func (p *CloudWatchPlugin) getDashboard(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DashboardName"]
	if name == "" {
		return nil, cwInvalidParameterValue("The parameter DashboardName is required.")
	}
	d, found, err := p.loadDashboard(context.Background(), ctx.AccountID, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cwDashboardNotFound(name)
	}
	result := cwDoc{}.
		with("DashboardArn", d.ARN).
		with("DashboardBody", d.Body).
		with("DashboardName", d.Name)
	return cwRespond(req, "GetDashboard", ctx.RequestID, result)
}

// deleteDashboards handles DeleteDashboards. Every named dashboard that exists is deleted before
// the first missing one is reported.
func (p *CloudWatchPlugin) deleteDashboards(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names := parseMemberList(req.Params, "DashboardNames")
	if len(names) == 0 {
		return nil, cwInvalidParameterValue("The parameter DashboardNames is required.")
	}
	if len(names) > cwDeleteDashboardsMax {
		return nil, cwInvalidParameterValue("The parameter DashboardNames must contain at most " +
			strconv.Itoa(cwDeleteDashboardsMax) + " names.")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	var missing *AWSError
	for _, name := range names {
		_, found, err := p.loadDashboard(goCtx, ctx.AccountID, name)
		if err != nil {
			return nil, err
		}
		if !found {
			if missing == nil {
				missing = cwDashboardNotFound(name)
			}
			continue
		}
		if err := p.state.Delete(goCtx, monitoringNamespace, cwDashboardKey(ctx.AccountID, name)); err != nil {
			return nil, fmt.Errorf("cloudwatch delete dashboard: %w", err)
		}
	}
	if missing != nil {
		return nil, missing
	}
	return cwRespond(req, "DeleteDashboards", ctx.RequestID, cwDoc{})
}

// listDashboards handles ListDashboards: the account's dashboards whose names start with
// DashboardNamePrefix, in name order, up to [cwListDashboardsPageSize] per page.
func (p *CloudWatchPlugin) listDashboards(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	prefix := req.Params["DashboardNamePrefix"]
	pr := pageRequest{
		Op:     "ListDashboards",
		Filter: map[string][]string{"DashboardNamePrefix": {prefix}},
		Token:  req.Params["NextToken"],
		Size:   cwListDashboardsPageSize,
	}
	if err := pr.validate(); err != nil {
		return nil, cwInvalidParameterValue("The NextToken is not valid for this request.")
	}

	goCtx := context.Background()
	keys, err := p.state.List(goCtx, monitoringNamespace, cwDashboardKey(ctx.AccountID, prefix))
	if err != nil {
		return nil, fmt.Errorf("cloudwatch list dashboards: %w", err)
	}
	dashboards := make([]cwDashboard, 0, len(keys))
	for _, key := range keys {
		name := strings.TrimPrefix(key, cwDashboardKey(ctx.AccountID, ""))
		d, found, loadErr := p.loadDashboard(goCtx, ctx.AccountID, name)
		if loadErr != nil {
			return nil, loadErr
		}
		if found {
			dashboards = append(dashboards, d)
		}
	}
	res, err := page(dashboards, func(d cwDashboard) cursorKey { return cursorKey{Sort: d.Name} }, pr)
	if err != nil {
		return nil, cwInvalidParameterValue("The NextToken is not valid for this request.")
	}

	entries := make(cwList, 0, len(res.Items))
	for _, d := range res.Items {
		entries = append(entries, cwDoc{}.
			with("DashboardName", d.Name).
			with("DashboardArn", d.ARN).
			with("LastModified", d.LastModified).
			with("Size", int64(len(d.Body))))
	}
	result := cwDoc{}.with("DashboardEntries", entries).withNonEmpty("NextToken", res.Next)
	return cwRespond(req, "ListDashboards", ctx.RequestID, result)
}
