package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// CodeDeployPlugin emulates the AWS CodeDeploy service.
// It handles application, deployment group, and deployment CRUD operations using
// the CodeDeploy JSON-target protocol (X-Amz-Target: CodeDeploy_20141006.{Op}).
type CodeDeployPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// seedMu serializes the deployment progression's read-modify-write; see [progression.observe].
	seedMu sync.Mutex
}

// Name returns the service name "codedeploy".
func (p *CodeDeployPlugin) Name() string { return codedeployNamespace }

// Initialize sets up the CodeDeployPlugin with the provided configuration.
func (p *CodeDeployPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CodeDeployPlugin.
func (p *CodeDeployPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CodeDeploy JSON-target request to the appropriate handler.
func (p *CodeDeployPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateApplication":
		return p.createApplication(reqCtx, req)
	case "GetApplication":
		return p.getApplication(reqCtx, req)
	case "DeleteApplication":
		return p.deleteApplication(reqCtx, req)
	case "ListApplications":
		return p.listApplications(reqCtx, req)
	case "CreateDeploymentGroup":
		return p.createDeploymentGroup(reqCtx, req)
	case "GetDeploymentGroup":
		return p.getDeploymentGroup(reqCtx, req)
	case "DeleteDeploymentGroup":
		return p.deleteDeploymentGroup(reqCtx, req)
	case "CreateDeployment":
		return p.createDeployment(reqCtx, req)
	case "GetDeployment":
		return p.getDeployment(reqCtx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// codedeployListPageSize is how many application names one ListApplications page answers.
//
// API_ListApplications publishes nextToken and no page-size member, and states no page size. 100 is
// substrate's reading, recorded here so it is not mistaken for a published figure: it is large enough
// that an ordinary account fits on one page, and small enough that a test can force a second.
const codedeployListPageSize = 100

func (p *CodeDeployPlugin) createApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
		ComputePlatform string `json:"computePlatform"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}
	// computePlatform is Required: No. An absent one is Server, the platform GetApplication reports
	// for an application created without one; a present one must be a published value.
	if input.ComputePlatform == "" {
		input.ComputePlatform = "Server"
	} else if !codedeployComputePlatforms[input.ComputePlatform] {
		return nil, codedeployErr("InvalidComputePlatformException", "The computePlatform is invalid. The computePlatform should be Lambda, Server, or ECS.")
	}

	goCtx := context.Background()
	key := codedeployAppKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	existing, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createApplication get: %w", err)
	}
	if existing != nil {
		return nil, codedeployErr("ApplicationAlreadyExistsException", "Application "+input.ApplicationName+" already exists.")
	}

	appID := generateCodeDeployAppID(reqCtx.IDs)
	app := CodeDeployApp{
		ApplicationID:   appID,
		ApplicationName: input.ApplicationName,
		ComputePlatform: input.ComputePlatform,
		CreateTime:      p.tc.Now(),
		AccountID:       reqCtx.AccountID,
		Region:          reqCtx.Region,
	}

	data, err := json.Marshal(app)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createApplication marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codedeployNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codedeploy createApplication put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region), input.ApplicationName); err != nil {
		return nil, fmt.Errorf("codedeploy createApplication index: %w", err)
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"applicationId": appID,
	})
}

func (p *CodeDeployPlugin) getApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}

	app, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err != nil {
		return nil, err
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"application": codedeployAppToWire(*app),
	})
}

// deleteApplication handles DeleteApplication.
//
// API_DeleteApplication publishes ApplicationNameRequiredException, InvalidApplicationNameException and
// InvalidRoleException, and on success "an HTTP 200 response with an empty HTTP body". It publishes no
// not-found code, so deleting an application that does not exist succeeds rather than answering
// ApplicationDoesNotExistException, a code published on four other pages (#1198). The application's
// deployment groups go with it, since a group is addressed only through its application.
func (p *CodeDeployPlugin) deleteApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	groupsKey := codedeployGroupNamesKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	groups, err := loadStringIndex(goCtx, p.state, codedeployNamespace, groupsKey)
	if err != nil {
		return nil, fmt.Errorf("codedeploy deleteApplication load groups: %w", err)
	}
	for _, group := range groups {
		if err := p.state.Delete(goCtx, codedeployNamespace, codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, group)); err != nil {
			return nil, fmt.Errorf("codedeploy deleteApplication delete group: %w", err)
		}
	}
	if len(groups) > 0 {
		if err := p.state.Delete(goCtx, codedeployNamespace, groupsKey); err != nil {
			return nil, fmt.Errorf("codedeploy deleteApplication delete group index: %w", err)
		}
	}
	key := codedeployAppKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err := p.state.Delete(goCtx, codedeployNamespace, key); err != nil {
		return nil, fmt.Errorf("codedeploy deleteApplication delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region), input.ApplicationName); err != nil {
		return nil, fmt.Errorf("codedeploy deleteApplication index: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
	}, nil
}

// listApplications handles ListApplications.
//
// nextToken is the only request member API_ListApplications publishes, and InvalidNextTokenException is
// its only published error (#1195). A page is [codedeployListPageSize] names in the index's insertion
// order, which is stable; the token is the offset of the next page, issued only when one exists, so
// the last page carries no nextToken rather than an empty one. A token substrate did not issue is
// refused rather than read as page one.
func (p *CodeDeployPlugin) listApplications(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		NextToken string `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	offset, ok := decodeOffsetPaginationToken(input.NextToken)
	if !ok {
		return nil, codedeployErr("InvalidNextTokenException", "The next token was specified in an invalid format.")
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("codedeploy listApplications load index: %w", err)
	}
	page, next := pageByOffsetToken(names, offset, codedeployListPageSize)
	if page == nil {
		page = []string{}
	}
	out := map[string]interface{}{"applications": page}
	if next != "" {
		out["nextToken"] = next
	}
	return codedeployJSONResponse(http.StatusOK, out)
}

// createDeploymentGroup handles CreateDeploymentGroup.
//
// serviceRoleArn is Required: Yes, and its absence is RoleRequiredException (#1197). The members
// DeploymentGroupInfo answers back unchanged are recorded as sent; see [codedeployGroupEchoMembers].
// The checks that need no state come first, so a malformed request is refused for what it is before
// the application is looked up.
//
// Two published combinations are refused because they are decidable from the body alone:
// ec2TagFilters with ec2TagSet (InvalidEC2TagCombinationException) and onPremisesInstanceTagFilters with
// onPremisesTagSet (InvalidOnPremisesTagCombinationException). outdatedInstancesStrategy is checked
// against its Valid Values, UPDATE | IGNORE, under InvalidInputException, the page's own code for "input
// specified in an invalid format"; no narrower code is published for it.
//
// The rest of the page's codes have no site, each because what it checks is not modeled: the
// configuration, Auto Scaling group, load balancer, ECS service and alarm a member names are not looked
// up, and the limits are not counted.
func (p *CodeDeployPlugin) createDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName           string   `json:"applicationName"`
		DeploymentGroupName       string   `json:"deploymentGroupName"`
		ServiceRoleArn            string   `json:"serviceRoleArn"`
		DeploymentConfigName      string   `json:"deploymentConfigName"`
		AutoScalingGroups         []string `json:"autoScalingGroups"`
		OutdatedInstancesStrategy string   `json:"outdatedInstancesStrategy"`
	}
	var raw map[string]json.RawMessage
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
		if err := json.Unmarshal(req.Body, &raw); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := codedeployCheckGroupName(input.DeploymentGroupName); awsErr != nil {
		return nil, awsErr
	}
	if input.ServiceRoleArn == "" {
		return nil, codedeployErr("RoleRequiredException", "The role ID was not specified.")
	}
	if !codedeployRoleARNPattern.MatchString(input.ServiceRoleArn) {
		return nil, codedeployErr("InvalidRoleException", "The service role ARN was specified in an invalid format.")
	}
	if awsErr := codedeployCheckConfigName(input.DeploymentConfigName); awsErr != nil {
		return nil, awsErr
	}
	if codedeployPresent(raw, "ec2TagFilters") && codedeployPresent(raw, "ec2TagSet") {
		return nil, codedeployErr("InvalidEC2TagCombinationException", "A call was submitted that specified both Ec2TagFilters and Ec2TagSet, but only one of these data types can be used in a single call.")
	}
	if codedeployPresent(raw, "onPremisesInstanceTagFilters") && codedeployPresent(raw, "onPremisesTagSet") {
		return nil, codedeployErr("InvalidOnPremisesTagCombinationException", "A call was submitted that specified both OnPremisesTagFilters and OnPremisesTagSet, but only one of these data types can be used in a single call.")
	}
	switch input.OutdatedInstancesStrategy {
	case "", "UPDATE", "IGNORE":
	default:
		return nil, codedeployErr("InvalidInputException", "outdatedInstancesStrategy must be UPDATE or IGNORE.")
	}

	app, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	existing, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createDeploymentGroup get: %w", err)
	}
	if existing != nil {
		return nil, codedeployErr("DeploymentGroupAlreadyExistsException", "Deployment group "+input.DeploymentGroupName+" already exists.")
	}

	groupID := generateCodeDeployGroupID(reqCtx.IDs)
	group := CodeDeployGroup{
		DeploymentGroupID:    groupID,
		DeploymentGroupName:  input.DeploymentGroupName,
		ApplicationName:      input.ApplicationName,
		ServiceRoleArn:       input.ServiceRoleArn,
		ComputePlatform:      app.ComputePlatform,
		DeploymentConfigName: codedeployDefaultConfigFor(input.DeploymentConfigName, "", app.ComputePlatform),
		AutoScalingGroups:    input.AutoScalingGroups,
		Config:               codedeployEcho(raw, codedeployGroupEchoMembers),
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}
	if err := p.putGroup(goCtx, key, group); err != nil {
		return nil, err
	}
	if err := updateStringIndex(goCtx, p.state, codedeployNamespace, codedeployGroupNamesKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName), input.DeploymentGroupName); err != nil {
		return nil, fmt.Errorf("codedeploy createDeploymentGroup index: %w", err)
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentGroupId": groupID,
	})
}

func (p *CodeDeployPlugin) getDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := codedeployCheckGroupName(input.DeploymentGroupName); awsErr != nil {
		return nil, awsErr
	}
	// API_GetDeploymentGroup publishes ApplicationDoesNotExistException as well as the group's own
	// not-found code, so the application is looked up first and each code reports the thing absent.
	if _, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName); err != nil {
		return nil, err
	}

	group, err := p.loadGroup(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	if err != nil {
		return nil, err
	}

	attempted, successful, err := p.groupLastDeployments(*group)
	if err != nil {
		return nil, err
	}
	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentGroupInfo": codedeployGroupToWire(*group, attempted, successful),
	})
}

// deleteDeploymentGroup handles DeleteDeploymentGroup.
//
// API_DeleteDeploymentGroup publishes the two name-required codes, the two invalid-name codes and
// InvalidRoleException, and no not-found code. So deleting a group that does not exist succeeds with
// the published body rather than answering DeploymentGroupDoesNotExistException (#1198).
// hooksNotCleanedUp is always empty: no Auto Scaling lifecycle hook is installed, so none is left
// behind.
func (p *CodeDeployPlugin) deleteDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := codedeployCheckGroupName(input.DeploymentGroupName); awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	key := codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	if err := p.state.Delete(goCtx, codedeployNamespace, key); err != nil {
		return nil, fmt.Errorf("codedeploy deleteDeploymentGroup delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, codedeployNamespace, codedeployGroupNamesKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName), input.DeploymentGroupName); err != nil {
		return nil, fmt.Errorf("codedeploy deleteDeploymentGroup index: %w", err)
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"hooksNotCleanedUp": []interface{}{},
	})
}

// createDeployment handles CreateDeployment.
//
// deploymentGroupName is Required: No on API_CreateDeployment, so a deployment may name none; when it
// does, the group must exist. The request members DeploymentInfo answers back are recorded as sent
// (see [codedeployDeploymentEchoMembers]), with two published Valid Values checked:
// fileExistsBehavior (InvalidFileExistsBehaviorException) and deploymentMode (InvalidInputException,
// the page's code for input in an invalid format; no narrower one is published). deploymentMode is
// recorded only as RESTART: DeploymentInfo's page says the member "is absent … for STANDARD
// deployments" and that an absent value "must not be interpreted as STANDARD".
//
// The deployment completes at once (#1196 owns making it progress), so startTime, createTime and
// completeTime are the same instant, and the group's lastAttemptedDeployment, lastSuccessfulDeployment
// and targetRevision are updated in the same request.
func (p *CodeDeployPlugin) createDeployment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName      string `json:"applicationName"`
		DeploymentGroupName  string `json:"deploymentGroupName"`
		DeploymentConfigName string `json:"deploymentConfigName"`
		FileExistsBehavior   string `json:"fileExistsBehavior"`
		DeploymentMode       string `json:"deploymentMode"`
	}
	var raw map[string]json.RawMessage
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
		if err := json.Unmarshal(req.Body, &raw); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if awsErr := codedeployCheckApplicationName(input.ApplicationName); awsErr != nil {
		return nil, awsErr
	}
	if input.DeploymentGroupName != "" {
		if awsErr := codedeployCheckGroupName(input.DeploymentGroupName); awsErr != nil {
			return nil, awsErr
		}
	}
	if awsErr := codedeployCheckConfigName(input.DeploymentConfigName); awsErr != nil {
		return nil, awsErr
	}
	switch input.FileExistsBehavior {
	case "", "DISALLOW", "OVERWRITE", "RETAIN":
	default:
		return nil, codedeployErr("InvalidFileExistsBehaviorException", `An invalid fileExistsBehavior option was specified. Valid values include "DISALLOW," "OVERWRITE," and "RETAIN."`)
	}
	switch input.DeploymentMode {
	case "", "STANDARD", "RESTART":
	default:
		return nil, codedeployErr("InvalidInputException", "deploymentMode must be STANDARD or RESTART.")
	}

	app, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err != nil {
		return nil, err
	}
	var group *CodeDeployGroup
	if input.DeploymentGroupName != "" {
		if group, err = p.loadGroup(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName); err != nil {
			return nil, err
		}
	}

	config := codedeployEcho(raw, codedeployDeploymentEchoMembers)
	if input.DeploymentMode != "RESTART" {
		delete(config, "deploymentMode")
	}
	groupConfig := ""
	if group != nil {
		groupConfig = group.DeploymentConfigName
		for _, member := range []string{"deploymentStyle", "loadBalancerInfo"} {
			if v, ok := group.Config[member]; ok {
				if config == nil {
					config = map[string]json.RawMessage{}
				}
				config[member] = v
			}
		}
	}

	deploymentID := generateCodeDeployDeploymentID(reqCtx.IDs)
	now := p.tc.Now()
	deployment := CodeDeployDeployment{
		DeploymentID:         deploymentID,
		ApplicationName:      input.ApplicationName,
		DeploymentGroupName:  input.DeploymentGroupName,
		Status:               "Succeeded",
		CreateTime:           now,
		StartTime:            now,
		CompleteTime:         now,
		Creator:              "user",
		ComputePlatform:      app.ComputePlatform,
		DeploymentConfigName: codedeployDefaultConfigFor(input.DeploymentConfigName, groupConfig, app.ComputePlatform),
		Config:               config,
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}

	data, err := json.Marshal(deployment)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createDeployment marshal: %w", err)
	}

	goCtx := context.Background()
	key := codedeployDeploymentKey(reqCtx.AccountID, reqCtx.Region, deploymentID)
	if err := p.state.Put(goCtx, codedeployNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codedeploy createDeployment put: %w", err)
	}

	if group != nil {
		ref := &CodeDeployDeploymentRef{DeploymentID: deploymentID, Status: deployment.Status, CreateTime: now, EndTime: now}
		group.LastAttemptedDeployment = ref
		group.LastSuccessfulDeployment = ref
		group.Deployments = append(group.Deployments, *ref)
		if revision, ok := raw["revision"]; ok {
			group.TargetRevision = revision
		}
		groupKey := codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
		if err := p.putGroup(goCtx, groupKey, *group); err != nil {
			return nil, err
		}
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentId": deploymentID,
	})
}

// getDeployment handles GetDeployment.
//
// An absent deploymentId is DeploymentIdRequiredException, published on API_GetDeployment, rather
// than a lookup of the empty ID answering DeploymentDoesNotExistException, which reported the caller's
// own validation bug as a missing resource (#1198). InvalidDeploymentIdException is published too and
// has no site: the page gives deploymentId no pattern, and the d- shape substrate mints is observed
// rather than published (see [generateCodeDeployDeploymentID]), so no ID is malformed by any rule the
// page states.
func (p *CodeDeployPlugin) getDeployment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DeploymentID string `json:"deploymentId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, codedeployInvalidBody()
		}
	}
	if input.DeploymentID == "" {
		return nil, codedeployErr("DeploymentIdRequiredException", "At least one deployment ID must be specified.")
	}

	goCtx := context.Background()
	key := codedeployDeploymentKey(reqCtx.AccountID, reqCtx.Region, input.DeploymentID)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy getDeployment get: %w", err)
	}
	if data == nil {
		return nil, codedeployErr("DeploymentDoesNotExistException", "Deployment "+input.DeploymentID+" does not exist.")
	}

	var deployment CodeDeployDeployment
	if err := json.Unmarshal(data, &deployment); err != nil {
		return nil, fmt.Errorf("codedeploy getDeployment unmarshal: %w", err)
	}

	info, err := p.observeDeployment(deployment)
	if err != nil {
		return nil, err
	}
	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentInfo": info,
	})
}

// loadApp loads a CodeDeployApp from state by name or returns a not-found error. Callers check the
// name first; the empty-name guard here is the backstop that keeps an empty key from being read.
func (p *CodeDeployPlugin) loadApp(acct, region, name string) (*CodeDeployApp, error) {
	if awsErr := codedeployCheckApplicationName(name); awsErr != nil {
		return nil, awsErr
	}
	goCtx := context.Background()
	key := codedeployAppKey(acct, region, name)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy loadApp get: %w", err)
	}
	if data == nil {
		return nil, codedeployErr("ApplicationDoesNotExistException", "Application "+name+" does not exist.")
	}
	var app CodeDeployApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("codedeploy loadApp unmarshal: %w", err)
	}
	return &app, nil
}

// loadGroup loads a CodeDeployGroup from state or returns a not-found error.
func (p *CodeDeployPlugin) loadGroup(acct, region, appName, groupName string) (*CodeDeployGroup, error) {
	if awsErr := codedeployCheckGroupName(groupName); awsErr != nil {
		return nil, awsErr
	}
	goCtx := context.Background()
	key := codedeployGroupKey(acct, region, appName, groupName)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy loadGroup get: %w", err)
	}
	if data == nil {
		return nil, codedeployErr("DeploymentGroupDoesNotExistException", "Deployment group "+groupName+" does not exist.")
	}
	var group CodeDeployGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("codedeploy loadGroup unmarshal: %w", err)
	}
	return &group, nil
}

// putGroup persists a deployment group.
func (p *CodeDeployPlugin) putGroup(ctx context.Context, key string, group CodeDeployGroup) error {
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("codedeploy group marshal: %w", err)
	}
	if err := p.state.Put(ctx, codedeployNamespace, key, data); err != nil {
		return fmt.Errorf("codedeploy group put: %w", err)
	}
	return nil
}

// codedeployPresent reports whether a request member was sent and is not JSON null.
func codedeployPresent(raw map[string]json.RawMessage, member string) bool {
	v, ok := raw[member]
	return ok && string(v) != "null"
}

// codedeployEcho returns the sent members among names, as the caller sent them, or nil if none was.
func codedeployEcho(raw map[string]json.RawMessage, names []string) map[string]json.RawMessage {
	var out map[string]json.RawMessage
	for _, name := range names {
		if codedeployPresent(raw, name) {
			if out == nil {
				out = map[string]json.RawMessage{}
			}
			out[name] = raw[name]
		}
	}
	return out
}

// codedeployDefaultConfigFor resolves a deployment configuration name: the request's, else the
// group's, else CodeDeployDefault.OneAtATime, which API_CreateDeploymentGroup and API_CreateDeployment
// both name as the default.
//
// The default is applied only to a Server application. OneAtATime is an EC2/on-premises configuration,
// and both pages state it as the default without qualifying the platform; a Lambda or ECS deployment
// group's default is a different predefined configuration that the API Reference does not name. So for
// those platforms an unnamed configuration stays unnamed rather than reporting one that would be wrong.
func codedeployDefaultConfigFor(requested, group, platform string) string {
	switch {
	case requested != "":
		return requested
	case group != "":
		return group
	case platform == "" || platform == "Server":
		return codedeployDefaultDeploymentConfig
	default:
		return ""
	}
}

// State key helpers.
func codedeployAppKey(acct, region, name string) string {
	return "app:" + acct + "/" + region + "/" + name
}

func codedeployAppNamesKey(acct, region string) string {
	return "app_names:" + acct + "/" + region
}

func codedeployGroupKey(acct, region, appName, groupName string) string {
	return "group:" + acct + "/" + region + "/" + appName + "/" + groupName
}

func codedeployGroupNamesKey(acct, region, appName string) string {
	return "group_names:" + acct + "/" + region + "/" + appName
}

func codedeployDeploymentKey(acct, region, deployID string) string {
	return "deployment:" + acct + "/" + region + "/" + deployID
}

// generateCodeDeployAppID mints a CodeDeploy application ID from m, derived from the request id
// so a replayed CreateApplication reports the ID the recording reported (#856).
//
// Both CodeDeploy identity IDs are reported rather than addressed: every operation here keys off
// `applicationName` and `deploymentGroupName`, so a re-minted application ID does not break a
// later call the way a deployment ID does. What it breaks is state validation — the ID is
// persisted in the application record, so a replayed create writes a record differing from the
// recorded one and `ValidateState` reports a `state_hash_after` mismatch for the create and for
// every event after it in the stream.
//
// `ApplicationInfo.applicationId` publishes neither a pattern nor length constraints — the type
// is String and the description is "The application ID" — so #671 keeps the rendering the
// crypto/rand form produced: [IDMint.HexUUID], 8-4-4-4-12 hex without RFC 4122's version and
// variant nibbles.
func generateCodeDeployAppID(m *IDMint) string {
	return m.HexUUID()
}

// generateCodeDeployGroupID mints a CodeDeploy deployment-group ID from m, derived from the
// request id so a replayed CreateDeploymentGroup reports the ID the recording reported (#856).
//
// `DeploymentGroupInfo.deploymentGroupId` publishes as little as the application ID does — type
// String, no pattern, no length constraints — so it takes the same rendering, for the reason
// [generateCodeDeployAppID] records. It stays a separate function because the two IDs are
// separate concepts that happen to share a shape, and folding them together is how a Batch job
// id came to be minted by a function named for a Lambda revision (see [IDMint.HexUUID]).
func generateCodeDeployGroupID(m *IDMint) string {
	return m.HexUUID()
}

// codedeployJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func codedeployJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("codedeploy json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
