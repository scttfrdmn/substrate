package emulator

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// ListApps and ListTrainingJobs read the members their pages publish (#1400).
//
// Both used to read no pagination member at all and answered every record in one response, and both
// discarded the error from loading their index, so a store fault answered an empty listing — the one
// result a caller cannot tell from "there is nothing here". Each now pages with NextToken and
// MaxResults, sorts by its published SortBy/SortOrder, and returns the index error wrapped.
//
// The token is the base64 offset of offset_pagination_token.go, over a listing put in a stable order
// before it is cut. A token neither operation could have issued is ValidationError/400: neither page
// publishes an error of its own, and SageMaker's Common Errors publishes ValidationError/400 — "The
// input doesn't meet the required format or constraints. Check that all required parameters are
// included and that values are valid." — which is the code every other SageMaker refusal in the tree
// already answers. The same code refuses a MaxResults outside 1–100 and
// a SortBy, SortOrder or StatusEquals outside its published values.
//
// What is substrate's reading rather than the page's, recorded so it is not mistaken for published:
//
//   - ListTrainingJobs publishes no MaxResults default; 100, the published maximum, is used.
//   - ListApps publishes CreationTime as its one SortBy value. The app record now carries the creation
//     time API_AppDetails publishes; an app recorded before #1400 has none and sorts as the earliest.
//     Equal times keep the index's key order, under Descending as well.
//   - TrainingPlanArnEquals, WarmPoolStatusEquals and SpaceNameEquals match nothing, because substrate
//     models no training plan, warm pool or space; a filter on a thing no record has selects no record.

// sagemakerListNameContains is ListTrainingJobs' published NameContains pattern; its length is
// 0–63.
var sagemakerListNameContains = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

// sagemakerListPage resolves a list request's MaxResults and NextToken, refusing either when it is
// outside what the page publishes. defaultSize is the page size an absent MaxResults means.
func sagemakerListPage(operation string, maxResults *int, nextToken string, defaultSize int) (offset, size int, awsErr *AWSError) {
	size = defaultSize
	if maxResults != nil {
		if *maxResults < 1 || *maxResults > 100 {
			return 0, 0, sagemakerValidationError(fmt.Sprintf("MaxResults must be between 1 and 100, got %d", *maxResults))
		}
		size = *maxResults
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return 0, 0, sagemakerValidationError("NextToken is not a token returned by a previous " + operation + " request")
	}
	return offset, size, nil
}

// sagemakerListOneOf refuses value when it is set and not one of the published values.
func sagemakerListOneOf(member, value string, published ...string) *AWSError {
	if value == "" || slices.Contains(published, value) {
		return nil
	}
	return sagemakerValidationError(fmt.Sprintf("%s must be one of %s, got %q", member, strings.Join(published, ", "), value))
}

func (p *SageMakerPlugin) listApps(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		DomainIDEquals        string `json:"DomainIdEquals"`
		MaxResults            *int   `json:"MaxResults"`
		NextToken             string `json:"NextToken"`
		SortBy                string `json:"SortBy"`
		SortOrder             string `json:"SortOrder"`
		SpaceNameEquals       string `json:"SpaceNameEquals"`
		UserProfileNameEquals string `json:"UserProfileNameEquals"`
	}
	// Every member is optional, so ListApps accepts no body at all. A body that is present and will not
	// parse is refused (#1007); an absent one lists everything.
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, sagemakerInvalidBody()
		}
	}
	// The page: "If UserProfileNameEquals is set, then this value cannot be set."
	if body.SpaceNameEquals != "" && body.UserProfileNameEquals != "" {
		return nil, sagemakerValidationError("SpaceNameEquals and UserProfileNameEquals cannot both be set")
	}
	if e := sagemakerListOneOf("SortBy", body.SortBy, "CreationTime"); e != nil {
		return nil, e
	}
	if e := sagemakerListOneOf("SortOrder", body.SortOrder, "Ascending", "Descending"); e != nil {
		return nil, e
	}
	// The page: "The default value for MaxResults is 10."
	offset, size, awsErr := sagemakerListPage("ListApps", body.MaxResults, body.NextToken, 10)
	if awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	keysKey := "app_keys:" + ctx.AccountID + "/" + ctx.Region
	keys, err := loadStringIndex(goCtx, p.state, sagemakerNamespace, keysKey)
	if err != nil {
		return nil, fmt.Errorf("sagemaker listApps load index: %w", err)
	}

	apps := make([]SageMakerApp, 0, len(keys))
	if body.SpaceNameEquals == "" {
		for _, k := range keys {
			data, err := p.state.Get(goCtx, sagemakerNamespace, k)
			if err != nil {
				return nil, fmt.Errorf("sagemaker listApps get %s: %w", k, err)
			}
			if data == nil {
				// An index entry whose record is gone names nothing to list.
				continue
			}
			var app SageMakerApp
			if err := json.Unmarshal(data, &app); err != nil {
				return nil, fmt.Errorf("sagemaker listApps unmarshal %s: %w", k, err)
			}
			if body.DomainIDEquals != "" && app.DomainID != body.DomainIDEquals {
				continue
			}
			if body.UserProfileNameEquals != "" && app.UserProfileName != body.UserProfileNameEquals {
				continue
			}
			apps = append(apps, app)
		}
	}
	// The index is sorted by key, not by creation, so the published CreationTime order is imposed here;
	// the key order breaks ties, which keeps every offset token stable.
	slices.SortStableFunc(apps, func(a, b SageMakerApp) int {
		c := cmp.Compare(a.CreationTime, b.CreationTime)
		if body.SortOrder == "Descending" {
			c = -c
		}
		return c
	})
	page, next := pageByOffsetToken(apps, offset, size)
	out := map[string]interface{}{"Apps": sagemakerAppsToDetails(page)}
	if next != "" {
		out["NextToken"] = next
	}
	return sagemakerJSONResponse(http.StatusOK, out)
}

func (p *SageMakerPlugin) listTrainingJobs(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		MaxResults            *int   `json:"MaxResults"`
		NameContains          string `json:"NameContains"`
		NextToken             string `json:"NextToken"`
		SortBy                string `json:"SortBy"`
		SortOrder             string `json:"SortOrder"`
		StatusEquals          string `json:"StatusEquals"`
		TrainingPlanArnEquals string `json:"TrainingPlanArnEquals"`
		WarmPoolStatusEquals  string `json:"WarmPoolStatusEquals"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, sagemakerInvalidBody()
		}
	}
	if body.NameContains != "" && (len(body.NameContains) > 63 || !sagemakerListNameContains.MatchString(body.NameContains)) {
		return nil, sagemakerValidationError("NameContains must be 1 to 63 letters, digits or hyphens")
	}
	if e := sagemakerListOneOf("SortBy", body.SortBy, "Name", "CreationTime", "Status"); e != nil {
		return nil, e
	}
	if e := sagemakerListOneOf("SortOrder", body.SortOrder, "Ascending", "Descending"); e != nil {
		return nil, e
	}
	if e := sagemakerListOneOf("StatusEquals", body.StatusEquals, "InProgress", "Completed", "Failed", "Stopping", "Stopped", "Deleting"); e != nil {
		return nil, e
	}
	if e := sagemakerListOneOf("WarmPoolStatusEquals", body.WarmPoolStatusEquals, "Available", "Terminated", "Reused", "InUse"); e != nil {
		return nil, e
	}
	offset, size, awsErr := sagemakerListPage("ListTrainingJobs", body.MaxResults, body.NextToken, 100)
	if awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	namesKey := "trainingjob_names:" + ctx.AccountID + "/" + ctx.Region
	names, err := loadStringIndex(goCtx, p.state, sagemakerNamespace, namesKey)
	if err != nil {
		return nil, fmt.Errorf("sagemaker listTrainingJobs load index: %w", err)
	}

	jobs := make([]SageMakerTrainingJob, 0, len(names))
	if body.TrainingPlanArnEquals == "" && body.WarmPoolStatusEquals == "" {
		for _, name := range names {
			if body.NameContains != "" && !strings.Contains(name, body.NameContains) {
				continue
			}
			key := "trainingjob:" + ctx.AccountID + "/" + ctx.Region + "/" + name
			data, err := p.state.Get(goCtx, sagemakerNamespace, key)
			if err != nil {
				return nil, fmt.Errorf("sagemaker listTrainingJobs get %s: %w", name, err)
			}
			if data == nil {
				continue
			}
			var job SageMakerTrainingJob
			if err := json.Unmarshal(data, &job); err != nil {
				return nil, fmt.Errorf("sagemaker listTrainingJobs unmarshal %s: %w", name, err)
			}
			// The same observation Describe makes, so the two agree on one job (#1162). Every job the
			// listing visits is observed, as before pagination, because a Status sort orders by the
			// observed status and so must observe before it can cut.
			if err := p.observeTrainingJob(&job); err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
	}
	sagemakerSortTrainingJobs(jobs, body.SortBy, body.SortOrder == "Descending")

	page, next := pageByOffsetToken(jobs, offset, size)
	// The page's Note: with both set, "the MaxResults number of training jobs are first retrieved
	// ignoring the StatusEquals parameter and then they are filtered by the StatusEquals parameter".
	// The filter therefore applies to the cut page, and the token is the unfiltered listing's.
	summaries := make([]sagemakerTrainingJobSummaryOut, 0, len(page))
	for _, job := range page {
		if body.StatusEquals != "" && job.TrainingJobStatus != body.StatusEquals {
			continue
		}
		summaries = append(summaries, sagemakerTrainingJobToSummary(job))
	}
	out := map[string]interface{}{"TrainingJobSummaries": summaries}
	if next != "" {
		out["NextToken"] = next
	}
	return sagemakerJSONResponse(http.StatusOK, out)
}

// sagemakerSortTrainingJobs orders jobs by ListTrainingJobs' SortBy — CreationTime when empty, the
// published default — breaking ties by name so the order, and so every offset token, is stable.
func sagemakerSortTrainingJobs(jobs []SageMakerTrainingJob, sortBy string, descending bool) {
	slices.SortStableFunc(jobs, func(a, b SageMakerTrainingJob) int {
		c := 0
		switch sortBy {
		case "Name":
			c = strings.Compare(a.TrainingJobName, b.TrainingJobName)
		case "Status":
			c = strings.Compare(a.TrainingJobStatus, b.TrainingJobStatus)
		default:
			switch {
			case a.CreationTime < b.CreationTime:
				c = -1
			case a.CreationTime > b.CreationTime:
				c = 1
			}
		}
		if c == 0 {
			c = strings.Compare(a.TrainingJobName, b.TrainingJobName)
		}
		if descending {
			c = -c
		}
		return c
	})
}
