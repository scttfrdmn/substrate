package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1169: the QuickSight and RAM refusal codes are the ones their pages publish. #1400: SageMaker's
// training-job status pair, its two list operations' paging, CodeDeploy's group last-deployment
// members, and QuickSight's describeDataSource store fault. Every success assertion is on the raw
// response bytes, because a typed decoder cannot see a missing member; every refusal assertion pins
// code and status together, because a `!= 200` check cannot see a wrong code.

// mlbiRequireRefused requires err to be the AWS refusal code at status.
func mlbiRequireRefused(t *testing.T, err error, code string, status int, context string) {
	t.Helper()
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "%s: want refusal %s, got %v", context, code, err)
	require.Equalf(t, code, awsErr.Code, "%s answers the code its page publishes", context)
	require.Equalf(t, status, awsErr.HTTPStatus, "%s answers %d", context, status)
}

// mlbiFrozenClock is a frozen time controller at a fixed instant, so creation times and endTimes are
// reproducible and a test can advance it between creates.
func mlbiFrozenClock() *emulator.TimeController {
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	return tc
}

// mlbiInit initializes p over state on tc and returns a request context.
func mlbiInit(t *testing.T, p emulator.Plugin, state emulator.StateManager, tc *emulator.TimeController) *emulator.RequestContext {
	t.Helper()
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize %s", p.Name())
	return &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1",
		RequestID: "req-mlbi", IDs: emulator.NewIDMint("req-mlbi"),
	}
}

// mlbiQuickSight issues one QuickSight REST request with a raw body.
func mlbiQuickSight(p *emulator.QuickSightPlugin, ctx *emulator.RequestContext, method, path, body string) (*emulator.AWSResponse, error) {
	return p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "quicksight", HTTPMethod: method, Path: path, Body: []byte(body),
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	})
}

// mlbiRAM issues one RAM REST request with a raw body.
func mlbiRAM(p *emulator.RAMPlugin, ctx *emulator.RequestContext, path, body string) (*emulator.AWSResponse, error) {
	return p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "ram", Operation: http.MethodPost, Path: path, Body: []byte(body),
		Headers: map[string]string{}, Params: map[string]string{},
	})
}

// mlbiJSONTarget issues one JSON-target operation with a raw body.
func mlbiJSONTarget(p emulator.Plugin, ctx *emulator.RequestContext, service, target, op, body string) (*emulator.AWSResponse, error) {
	return p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: service, Operation: op, Path: "/", Body: []byte(body),
		Headers: map[string]string{"X-Amz-Target": target + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	})
}

// mlbiOK requires a 2xx and returns the raw body.
func mlbiOK(t *testing.T, resp *emulator.AWSResponse, err error, what string) string {
	t.Helper()
	require.NoErrorf(t, err, "%s", what)
	require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s answered %d: %s", what, resp.StatusCode, resp.Body)
	return string(resp.Body)
}

func TestRefusalSpelling1169_QuickSightAndRAMAnswerTheirPublishedCodes(t *testing.T) {
	t.Parallel()
	const qsDataSources = "/accounts/123456789012/data-sources"
	const qsDataSets = "/accounts/123456789012/data-sets"
	for _, tc := range []struct {
		name    string
		call    func(qs *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error)
		code    string
		oldCode string
	}{
		{"QuickSight CreateDataSource, DataSourceId absent", func(qs *emulator.QuickSightPlugin, _ *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSources, `{"Name":"n","Type":"S3"}`)
		}, "InvalidParameterValueException", "InvalidParameterValue"},
		{"QuickSight CreateDataSource, body unparseable", func(qs *emulator.QuickSightPlugin, _ *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSources, `{not json`)
		}, "InvalidParameterValueException", "InvalidParameterValue"},
		{"QuickSight CreateDataSet, DataSetId absent", func(qs *emulator.QuickSightPlugin, _ *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSets, `{"Name":"n"}`)
		}, "InvalidParameterValueException", "InvalidParameterValue"},
		{"QuickSight CreateDataSet, body unparseable", func(qs *emulator.QuickSightPlugin, _ *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSets, `[`)
		}, "InvalidParameterValueException", "InvalidParameterValue"},
		{"RAM CreateResourceShare, name absent", func(_ *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiRAM(ram, ctx, "/createresourceshare", `{"principals":["111122223333"]}`)
		}, "ValidationError", "MissingRequiredParameter"},
		{"RAM UpdateResourceShare, resourceShareArn absent", func(_ *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiRAM(ram, ctx, "/updateresourceshare", `{"name":"renamed"}`)
		}, "ValidationError", "MissingRequiredParameter"},
		{"RAM DeleteResourceShare, resourceShareArn absent", func(_ *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiRAM(ram, ctx, "/deleteresourceshare", `{}`)
		}, "ValidationError", "MissingRequiredParameter"},
		{"RAM AssociateResourceShare, resourceShareArn absent", func(_ *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiRAM(ram, ctx, "/associateresourceshare", `{"principals":["111122223333"]}`)
		}, "ValidationError", "MissingRequiredParameter"},
		{"RAM DisassociateResourceShare, resourceShareArn absent", func(_ *emulator.QuickSightPlugin, ram *emulator.RAMPlugin, ctx *emulator.RequestContext) (*emulator.AWSResponse, error) {
			return mlbiRAM(ram, ctx, "/disassociateresourceshare", `{"principals":["111122223333"]}`)
		}, "ValidationError", "MissingRequiredParameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := emulator.NewMemoryStateManager()
			qs, ram := &emulator.QuickSightPlugin{}, &emulator.RAMPlugin{}
			ctx := mlbiInit(t, qs, state, mlbiFrozenClock())
			mlbiInit(t, ram, state, mlbiFrozenClock())
			_, err := tc.call(qs, ram, ctx)
			mlbiRequireRefused(t, err, tc.code, http.StatusBadRequest, tc.name)
			var awsErr *emulator.AWSError
			require.True(t, errors.As(err, &awsErr))
			require.NotEqualf(t, tc.oldCode, awsErr.Code, "%s no longer answers the unpublished %s", tc.name, tc.oldCode)
		})
	}

	// Controls: the same operations with the member present succeed, so each refusal above is about
	// the member and not the route.
	t.Run("controls", func(t *testing.T) {
		t.Parallel()
		state := emulator.NewMemoryStateManager()
		qs, ram := &emulator.QuickSightPlugin{}, &emulator.RAMPlugin{}
		ctx := mlbiInit(t, qs, state, mlbiFrozenClock())
		mlbiInit(t, ram, state, mlbiFrozenClock())
		resp, err := mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSources, `{"DataSourceId":"ds1","Name":"n","Type":"S3"}`)
		require.Contains(t, mlbiOK(t, resp, err, "CreateDataSource"), `"DataSourceId":"ds1"`)
		resp, err = mlbiQuickSight(qs, ctx, http.MethodPost, qsDataSets, `{"DataSetId":"set1","Name":"n"}`)
		require.Contains(t, mlbiOK(t, resp, err, "CreateDataSet"), `"DataSetId":"set1"`)
		resp, err = mlbiRAM(ram, ctx, "/createresourceshare", `{"name":"share"}`)
		body := mlbiOK(t, resp, err, "CreateResourceShare")
		var share struct {
			ResourceShare struct {
				ResourceShareArn string `json:"resourceShareArn"`
			} `json:"resourceShare"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &share))
		resp, err = mlbiRAM(ram, ctx, "/updateresourceshare", fmt.Sprintf(`{"resourceShareArn":%q,"name":"renamed"}`, share.ResourceShare.ResourceShareArn))
		require.Contains(t, mlbiOK(t, resp, err, "UpdateResourceShare"), `"name":"renamed"`)
	})
}

func TestQuickSight1400_DescribeDataSourceStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	qs := &emulator.QuickSightPlugin{}
	ctx := mlbiInit(t, qs, fault, mlbiFrozenClock())
	resp, err := mlbiQuickSight(qs, ctx, http.MethodPost, "/accounts/123456789012/data-sources", `{"DataSourceId":"ds1","Name":"n","Type":"S3"}`)
	mlbiOK(t, resp, err, "CreateDataSource")

	// Control: the data source exists, and an absent one is the published not-found.
	resp, err = mlbiQuickSight(qs, ctx, http.MethodGet, "/accounts/123456789012/data-sources/ds1", "")
	require.Contains(t, mlbiOK(t, resp, err, "DescribeDataSource"), `"DataSourceId":"ds1"`)
	_, err = mlbiQuickSight(qs, ctx, http.MethodGet, "/accounts/123456789012/data-sources/absent", "")
	mlbiRequireRefused(t, err, "ResourceNotFoundException", http.StatusNotFound, "DescribeDataSource of an absent source")

	fault.failGet = "datasource:"
	_, err = mlbiQuickSight(qs, ctx, http.MethodGet, "/accounts/123456789012/data-sources/ds1", "")
	requireStoreFault(t, err, "DescribeDataSource")
	require.ErrorIs(t, err, errCFFaultStore, "the store's error is wrapped, not replaced")
}

// sagemakerMLBI is a SageMaker plugin over a store, on a frozen clock the test can advance.
type sagemakerMLBI struct {
	t     *testing.T
	p     *emulator.SageMakerPlugin
	ctx   *emulator.RequestContext
	state emulator.StateManager
	tc    *emulator.TimeController
}

func newSageMakerMLBI(t *testing.T, state emulator.StateManager) *sagemakerMLBI {
	t.Helper()
	tc := mlbiFrozenClock()
	p := &emulator.SageMakerPlugin{}
	return &sagemakerMLBI{t: t, p: p, ctx: mlbiInit(t, p, state, tc), state: state, tc: tc}
}

func (h *sagemakerMLBI) call(op, body string) (*emulator.AWSResponse, error) {
	return mlbiJSONTarget(h.p, h.ctx, "sagemaker", "SageMaker.", op, body)
}

func (h *sagemakerMLBI) ok(op, body string) string {
	h.t.Helper()
	resp, err := h.call(op, body)
	return mlbiOK(h.t, resp, err, op)
}

// seed writes a training-job status seed straight to the control namespace, as the control-plane
// endpoint stores it.
func (h *sagemakerMLBI) seed(name, body string) {
	h.t.Helper()
	require.NoError(h.t, h.state.Put(h.t.Context(), "sagemaker-ctrl", "status:"+name, []byte(body)))
}

func TestSageMaker1400_SecondaryStatusFollowsTheObservedStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		seed string
		stop bool
		// want is each observation's raw status pair, in order.
		want []string
	}{
		{"unseeded completes", "", false, []string{
			`"SecondaryStatus":"Completed","TrainingJobArn"`,
		}},
		{"seeded in progress then failed", `{"trainingJobName":"job","pendingObservations":2,"status":"Failed","failureReason":"CapacityError: no ml.p4d"}`, false, []string{
			`"SecondaryStatus":"Training"`, `"SecondaryStatus":"Training"`, `"SecondaryStatus":"Failed"`,
		}},
		{"seeded in progress then completed", `{"trainingJobName":"job","pendingObservations":1,"status":"Completed"}`, false, []string{
			`"SecondaryStatus":"Training"`, `"SecondaryStatus":"Completed"`,
		}},
		{"seeded straight to stopped", `{"trainingJobName":"job","status":"Stopped"}`, false, []string{
			`"SecondaryStatus":"Stopped"`,
		}},
		{"stopped through stopping", `{"trainingJobName":"job","pendingObservations":1}`, true, []string{
			`"SecondaryStatus":"Stopping"`, `"SecondaryStatus":"Stopped"`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newSageMakerMLBI(t, emulator.NewMemoryStateManager())
			h.ok("CreateTrainingJob", `{"TrainingJobName":"job"}`)
			if tc.seed != "" {
				h.seed("job", tc.seed)
			}
			if tc.stop {
				h.ok("StopTrainingJob", `{"TrainingJobName":"job"}`)
			}
			for i, want := range tc.want {
				got := h.ok("DescribeTrainingJob", `{"TrainingJobName":"job"}`)
				require.Containsf(t, got, want, "observation %d: %s", i+1, got)
				// The pair agrees: SecondaryStatus is one of the values the page groups under the
				// TrainingJobStatus this same observation reports.
				var pair struct{ SecondaryStatus, TrainingJobStatus string }
				require.NoError(t, json.Unmarshal([]byte(got), &pair))
				require.Containsf(t, map[string][]string{
					"InProgress": {"Starting", "Pending", "Downloading", "Training", "Interrupted", "Uploading"},
					"Completed":  {"Completed"},
					"Failed":     {"Failed"},
					"Stopped":    {"MaxRuntimeExceeded", "MaxWaitTimeExceeded", "Stopped"},
					"Stopping":   {"Stopping"},
				}[pair.TrainingJobStatus], pair.SecondaryStatus, "observation %d pairs %s with %s", i+1, pair.TrainingJobStatus, pair.SecondaryStatus)
			}
		})
	}
}

func TestSageMaker1400_ListTrainingJobsSummariesCarryTheObservedPair(t *testing.T) {
	t.Parallel()
	h := newSageMakerMLBI(t, emulator.NewMemoryStateManager())
	h.ok("CreateTrainingJob", `{"TrainingJobName":"running"}`)
	h.ok("CreateTrainingJob", `{"TrainingJobName":"done"}`)
	h.seed("running", `{"trainingJobName":"running","pendingObservations":5,"status":"Completed"}`)

	got := h.ok("ListTrainingJobs", `{}`)
	require.Contains(t, got, `{"CreationTime":1700000000,"SecondaryStatus":"Training","TrainingJobArn":"arn:aws:sagemaker:us-east-1:123456789012:training-job/running","TrainingJobName":"running","TrainingJobStatus":"InProgress"}`, "%s", got)
	require.Contains(t, got, `{"CreationTime":1700000000,"SecondaryStatus":"Completed","TrainingJobArn":"arn:aws:sagemaker:us-east-1:123456789012:training-job/done","TrainingJobName":"done","TrainingJobStatus":"Completed"}`, "%s", got)
}

func TestSageMaker1400_ListTrainingJobsPagesSortsAndFilters(t *testing.T) {
	t.Parallel()
	h := newSageMakerMLBI(t, emulator.NewMemoryStateManager())
	// Created out of name order, one second apart, so CreationTime and Name orders differ.
	for i, name := range []string{"charlie", "alpha", "bravo", "delta"} {
		h.tc.SetTime(time.Unix(1700000000+int64(i), 0).UTC())
		h.ok("CreateTrainingJob", fmt.Sprintf(`{"TrainingJobName":%q}`, name))
	}
	h.seed("bravo", `{"trainingJobName":"bravo","status":"Failed"}`)

	names := func(body string) []string {
		var out struct {
			TrainingJobSummaries []struct{ TrainingJobName string }
		}
		require.NoError(t, json.Unmarshal([]byte(body), &out), "%s", body)
		got := make([]string, 0, len(out.TrainingJobSummaries))
		for _, s := range out.TrainingJobSummaries {
			got = append(got, s.TrainingJobName)
		}
		return got
	}
	token := func(body string) string {
		var out struct{ NextToken string }
		require.NoError(t, json.Unmarshal([]byte(body), &out), "%s", body)
		return out.NextToken
	}

	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{"default is CreationTime Ascending", `{}`, []string{"charlie", "alpha", "bravo", "delta"}},
		{"CreationTime Descending", `{"SortOrder":"Descending"}`, []string{"delta", "bravo", "alpha", "charlie"}},
		{"Name", `{"SortBy":"Name"}`, []string{"alpha", "bravo", "charlie", "delta"}},
		{"Status, name breaks ties", `{"SortBy":"Status"}`, []string{"alpha", "charlie", "delta", "bravo"}},
		{"NameContains", `{"NameContains":"ha"}`, []string{"charlie", "alpha"}},
		{"NameContains, one match", `{"NameContains":"elt"}`, []string{"delta"}},
		{"StatusEquals", `{"StatusEquals":"Failed"}`, []string{"bravo"}},
		{"StatusEquals applies after the MaxResults cut", `{"MaxResults":2,"StatusEquals":"Failed"}`, []string{}},
		{"TrainingPlanArnEquals matches nothing modeled", `{"TrainingPlanArnEquals":"arn:aws:sagemaker:us-east-1:123456789012:training-plan/p"}`, []string{}},
		{"WarmPoolStatusEquals matches nothing modeled", `{"WarmPoolStatusEquals":"Available"}`, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, names(h.ok("ListTrainingJobs", tc.body)))
		})
	}

	t.Run("a walk visits every job once and ends without a token", func(t *testing.T) {
		first := h.ok("ListTrainingJobs", `{"MaxResults":3,"SortBy":"Name"}`)
		require.Equal(t, []string{"alpha", "bravo", "charlie"}, names(first))
		next := token(first)
		require.NotEmpty(t, next, "a truncated page carries a token")
		second := h.ok("ListTrainingJobs", fmt.Sprintf(`{"MaxResults":3,"SortBy":"Name","NextToken":%q}`, next))
		require.Equal(t, []string{"delta"}, names(second))
		require.NotContains(t, second, "NextToken", "the last page carries none: %s", second)
		// An exact multiple of the page size costs no empty page.
		require.NotContains(t, h.ok("ListTrainingJobs", `{"MaxResults":4}`), "NextToken")
		// The page's Note: the token is the unfiltered listing's, so a filtered walk still advances.
		filtered := h.ok("ListTrainingJobs", `{"MaxResults":2,"StatusEquals":"Failed"}`)
		require.Contains(t, filtered, `"NextToken"`, "%s", filtered)
		require.Equal(t, []string{"bravo"}, names(h.ok("ListTrainingJobs", fmt.Sprintf(`{"MaxResults":2,"StatusEquals":"Failed","NextToken":%q}`, token(filtered)))))
	})

	for _, tc := range []struct {
		name string
		body string
	}{
		{"MaxResults 0", `{"MaxResults":0}`},
		{"MaxResults 101", `{"MaxResults":101}`},
		{"unissued token", `{"NextToken":"page-two"}`},
		{"non-canonical token", `{"NextToken":"` + base64.StdEncoding.EncodeToString([]byte("+2")) + `"}`},
		{"SortBy outside its values", `{"SortBy":"LastModifiedTime"}`},
		{"SortOrder outside its values", `{"SortOrder":"Up"}`},
		{"StatusEquals outside its values", `{"StatusEquals":"Running"}`},
		{"WarmPoolStatusEquals outside its values", `{"WarmPoolStatusEquals":"Warm"}`},
		{"NameContains outside its pattern", `{"NameContains":"a_b"}`},
		{"NameContains over 63", `{"NameContains":"` + strings.Repeat("a", 64) + `"}`},
		{"body unparseable", `{`},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			_, err := h.call("ListTrainingJobs", tc.body)
			mlbiRequireRefused(t, err, "ValidationError", http.StatusBadRequest, "ListTrainingJobs "+tc.name)
		})
	}
	t.Run("control: the boundary values are accepted", func(t *testing.T) {
		h.ok("ListTrainingJobs", `{"MaxResults":1}`)
		h.ok("ListTrainingJobs", `{"MaxResults":100,"SortBy":"Status","SortOrder":"Ascending","StatusEquals":"Deleting","NameContains":"`+strings.Repeat("a", 63)+`"}`)
		h.ok("ListTrainingJobs", `{"NextToken":"`+base64.StdEncoding.EncodeToString([]byte("99"))+`"}`)
	})
}

func TestSageMaker1400_ListAppsPagesSortsAndFilters(t *testing.T) {
	t.Parallel()
	h := newSageMakerMLBI(t, emulator.NewMemoryStateManager())
	// Alternating profiles put the store's key order (alice's apps, then bob's) out of creation order,
	// so a listing in key order would fail every assertion below.
	var created []string
	for i := range 12 {
		name := fmt.Sprintf("app-%02d", i)
		profile := "alice"
		if i%2 == 1 {
			profile = "bob"
		}
		h.tc.SetTime(time.Unix(1700000000+int64(i), 0).UTC())
		h.ok("CreateApp", fmt.Sprintf(`{"AppName":%q,"AppType":"JupyterServer","DomainId":"d-abc","UserProfileName":%q}`, name, profile))
		created = append(created, name)
	}
	names := func(body string) []string {
		var out struct {
			Apps []struct{ AppName string }
		}
		require.NoError(t, json.Unmarshal([]byte(body), &out), "%s", body)
		got := make([]string, 0, len(out.Apps))
		for _, a := range out.Apps {
			got = append(got, a.AppName)
		}
		return got
	}

	t.Run("the published default page is 10, then the rest", func(t *testing.T) {
		first := h.ok("ListApps", `{}`)
		require.Equal(t, created[:10], names(first))
		var out struct{ NextToken string }
		require.NoError(t, json.Unmarshal([]byte(first), &out))
		require.NotEmpty(t, out.NextToken, "%s", first)
		second := h.ok("ListApps", fmt.Sprintf(`{"NextToken":%q}`, out.NextToken))
		require.Equal(t, created[10:], names(second))
		require.NotContains(t, second, "NextToken", "%s", second)
	})
	t.Run("Descending reverses creation order", func(t *testing.T) {
		got := names(h.ok("ListApps", `{"MaxResults":3,"SortBy":"CreationTime","SortOrder":"Descending"}`))
		require.Equal(t, []string{"app-11", "app-10", "app-09"}, got)
	})
	t.Run("UserProfileNameEquals", func(t *testing.T) {
		got := names(h.ok("ListApps", `{"MaxResults":100,"UserProfileNameEquals":"bob"}`))
		require.Equal(t, []string{"app-01", "app-03", "app-05", "app-07", "app-09", "app-11"}, got)
	})
	t.Run("SpaceNameEquals matches no app, since none is in a space", func(t *testing.T) {
		require.Equal(t, `{"Apps":[]}`, h.ok("ListApps", `{"SpaceNameEquals":"space"}`))
	})
	for _, tc := range []struct {
		name string
		body string
	}{
		{"SpaceNameEquals with UserProfileNameEquals", `{"SpaceNameEquals":"s","UserProfileNameEquals":"bob"}`},
		{"MaxResults 0", `{"MaxResults":0}`},
		{"MaxResults 101", `{"MaxResults":101}`},
		{"unissued token", `{"NextToken":"nope"}`},
		{"SortBy outside its values", `{"SortBy":"Name"}`},
		{"SortOrder outside its values", `{"SortOrder":"down"}`},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			_, err := h.call("ListApps", tc.body)
			mlbiRequireRefused(t, err, "ValidationError", http.StatusBadRequest, "ListApps "+tc.name)
		})
	}
}

func TestSageMaker1400_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body string
	}{
		{"ListApps, index read", func(m *cfFaultStateManager) { m.failGet = "app_keys:" }, "ListApps", `{}`},
		{"ListApps, record read", func(m *cfFaultStateManager) { m.failGet = "app:" }, "ListApps", `{}`},
		{"ListApps, record corrupt", func(m *cfFaultStateManager) { m.corruptGet = "app:" }, "ListApps", `{}`},
		{"ListTrainingJobs, index read", func(m *cfFaultStateManager) { m.failGet = "trainingjob_names:" }, "ListTrainingJobs", `{}`},
		{"ListTrainingJobs, record read", func(m *cfFaultStateManager) { m.failGet = "trainingjob:" }, "ListTrainingJobs", `{}`},
		{"ListTrainingJobs, record corrupt", func(m *cfFaultStateManager) { m.corruptGet = "trainingjob:" }, "ListTrainingJobs", `{}`},
		{"DescribeTrainingJob, record read", func(m *cfFaultStateManager) { m.failGet = "trainingjob:" }, "DescribeTrainingJob", `{"TrainingJobName":"job"}`},
		{"StopTrainingJob, record read", func(m *cfFaultStateManager) { m.failGet = "trainingjob:" }, "StopTrainingJob", `{"TrainingJobName":"job"}`},
		{"DescribeApp, record read", func(m *cfFaultStateManager) { m.failGet = "app:" }, "DescribeApp", `{"AppName":"a","AppType":"JupyterServer","DomainId":"d-abc","UserProfileName":"u"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newSageMakerMLBI(t, fault)
			h.ok("CreateApp", `{"AppName":"a","AppType":"JupyterServer","DomainId":"d-abc","UserProfileName":"u"}`)
			h.ok("CreateTrainingJob", `{"TrainingJobName":"job"}`)
			// Control: the same call succeeds before the fault is armed.
			h.ok(tc.op, tc.body)
			tc.arm(fault)
			_, err := h.call(tc.op, tc.body)
			requireStoreFault(t, err, tc.name)
		})
	}
}

// codedeployMLBI is a CodeDeploy plugin with one application and group, over a store.
func codedeployMLBI(t *testing.T, state emulator.StateManager) (*codedeployAuditHarness, func(string)) {
	t.Helper()
	h := newCodeDeployAuditHarness(t, state)
	h.ok("CreateApplication", map[string]any{"applicationName": "app"})
	h.ok("CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp", "serviceRoleArn": codedeployAuditRole})
	seed := func(body string) {
		t.Helper()
		var probe struct {
			DeploymentID string `json:"deploymentId"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &probe))
		id := probe.DeploymentID
		if id == "" {
			id = "*"
		}
		require.NoError(t, state.Put(t.Context(), "codedeploy-deploy-ctrl", "status:"+id, []byte(body)))
	}
	return h, seed
}

// codedeployDeploy creates a deployment in the group and returns its ID.
func codedeployDeploy(t *testing.T, h *codedeployAuditHarness) string {
	t.Helper()
	var out struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(h.ok("CreateDeployment", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}), &out))
	return out.DeploymentID
}

// codedeployLastMembers returns the group's two last-deployment members as raw JSON, "" when absent.
func codedeployLastMembers(t *testing.T, h *codedeployAuditHarness) (attempted, successful string) {
	t.Helper()
	var out struct {
		DeploymentGroupInfo map[string]json.RawMessage `json:"deploymentGroupInfo"`
	}
	require.NoError(t, json.Unmarshal(h.ok("GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}), &out))
	return string(out.DeploymentGroupInfo["lastAttemptedDeployment"]), string(out.DeploymentGroupInfo["lastSuccessfulDeployment"])
}

func TestCodeDeploy1400_GroupLastDeploymentsReflectTheObservedStatus(t *testing.T) {
	t.Parallel()

	t.Run("unseeded: both name the deployment, Succeeded, ended", func(t *testing.T) {
		t.Parallel()
		h, _ := codedeployMLBI(t, emulator.NewMemoryStateManager())
		id := codedeployDeploy(t, h)
		want := fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"endTime":1700000000.000,"status":"Succeeded"}`, id)
		attempted, successful := codedeployLastMembers(t, h)
		require.Equal(t, want, attempted)
		require.Equal(t, want, successful)
	})

	t.Run("seeded in progress then failed, after an earlier success", func(t *testing.T) {
		t.Parallel()
		h, seed := codedeployMLBI(t, emulator.NewMemoryStateManager())
		earlier := codedeployDeploy(t, h)
		later := codedeployDeploy(t, h)
		seed(fmt.Sprintf(`{"deploymentId":%q,"pendingObservations":1,"finalState":"Failed","errorCode":"HEALTH_CONSTRAINTS"}`, later))
		earlierRef := fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"endTime":1700000000.000,"status":"Succeeded"}`, earlier)

		// Reading the group twice spends no observation: both reads see InProgress, with no endTime.
		for range 2 {
			attempted, successful := codedeployLastMembers(t, h)
			require.Equal(t, fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"status":"InProgress"}`, later), attempted)
			require.Equal(t, earlierRef, successful, "the in-progress deployment is not the last successful one")
		}

		// GetDeployment spends the countdown, and the group then agrees with it.
		require.Contains(t, string(h.ok("GetDeployment", map[string]any{"deploymentId": later})), `"status":"InProgress"`)
		require.Contains(t, string(h.ok("GetDeployment", map[string]any{"deploymentId": later})), `"status":"Failed"`)
		attempted, successful := codedeployLastMembers(t, h)
		require.Equal(t, fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"endTime":1700000000.000,"status":"Failed"}`, later), attempted)
		require.Equal(t, earlierRef, successful, "a later failure leaves the earlier success in place")
	})

	t.Run("wildcard seed with no success: lastSuccessfulDeployment is absent", func(t *testing.T) {
		t.Parallel()
		h, seed := codedeployMLBI(t, emulator.NewMemoryStateManager())
		seed(`{"deploymentId":"*","finalState":"Stopped"}`)
		id := codedeployDeploy(t, h)
		attempted, successful := codedeployLastMembers(t, h)
		require.Equal(t, fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"endTime":1700000000.000,"status":"Stopped"}`, id), attempted)
		require.Empty(t, successful)
	})

	t.Run("a group with no deployment answers neither", func(t *testing.T) {
		t.Parallel()
		h, _ := codedeployMLBI(t, emulator.NewMemoryStateManager())
		attempted, successful := codedeployLastMembers(t, h)
		require.Empty(t, attempted)
		require.Empty(t, successful)
	})

	t.Run("a group recorded before #1400 answers its stored references", func(t *testing.T) {
		t.Parallel()
		state := emulator.NewMemoryStateManager()
		h, _ := codedeployMLBI(t, state)
		id := codedeployDeploy(t, h)
		key := "group:123456789012/us-east-1/app/grp"
		data, err := state.Get(t.Context(), "codedeploy", key)
		require.NoError(t, err)
		require.NotNil(t, data, "the group record is at %s", key)
		var record map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &record))
		require.Contains(t, record, "deployments", "control: the new record carries history")
		delete(record, "deployments")
		data, err = json.Marshal(record)
		require.NoError(t, err)
		require.NoError(t, state.Put(t.Context(), "codedeploy", key, data))
		want := fmt.Sprintf(`{"createTime":1700000000.000,"deploymentId":%q,"endTime":1700000000.000,"status":"Succeeded"}`, id)
		attempted, successful := codedeployLastMembers(t, h)
		require.Equal(t, want, attempted)
		require.Equal(t, want, successful)
	})
}

func TestCodeDeploy1400_GroupPeekStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		seed  string
		fault string
	}{
		{"seed read", "", "status:"},
		{"observation counter read", `{"deploymentId":"*","pendingObservations":3}`, "observed:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h, seed := codedeployMLBI(t, fault)
			if tc.seed != "" {
				seed(tc.seed)
			}
			codedeployDeploy(t, h)
			codedeployLastMembers(t, h) // control: the read succeeds before the fault is armed
			fault.failGet = tc.fault
			_, err := h.call("GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"})
			requireStoreFault(t, err, "GetDeploymentGroup "+tc.name)
			require.ErrorIs(t, err, errCFFaultStore)
		})
	}
}

// The group's observed last deployment is recorded and replayed like every other observation: the
// seed is a control-plane write, and the group read spends nothing, so a replay sees the same countdown.
func TestCodeDeploy1400_GroupLastDeploymentsReplay(t *testing.T) {
	t.Parallel()
	ts := progressionServer(t)
	const target = "CodeDeploy_20141006."
	idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateApplication", map[string]any{"applicationName": "rep-app"})
	idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateDeploymentGroup", map[string]any{
		"applicationName": "rep-app", "deploymentGroupName": "rep-grp", "serviceRoleArn": codedeployAuditRole,
	})
	progressionSeed(t, ts, "/v1/codedeploy/deployment-status", `{"deploymentId":"*","pendingObservations":1,"finalState":"Failed"}`)
	var dep struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateDeployment",
		map[string]any{"applicationName": "rep-app", "deploymentGroupName": "rep-grp"}), &dep))
	group := map[string]any{"applicationName": "rep-app", "deploymentGroupName": "rep-grp"}
	_, got := progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeploymentGroup", group)
	require.Equal(t, "InProgress", progressionMember(t, got, "deploymentGroupInfo", "lastAttemptedDeployment", "status"))
	require.Nil(t, progressionMember(t, got, "deploymentGroupInfo", "lastSuccessfulDeployment"))
	progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeployment", map[string]any{"deploymentId": dep.DeploymentID})
	_, got = progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeploymentGroup", group)
	require.Equal(t, "Failed", progressionMember(t, got, "deploymentGroupInfo", "lastAttemptedDeployment", "status"))
	progressionReplays(t, ts, 0)
}
