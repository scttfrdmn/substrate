package emulator

// StackDeployer helpers for AWS::CloudWatch::Dashboard (#1613): its deploy, in-place update and
// delete. The rules every in-place updater follows are stated with cfnUpdateUnmodelledProperties.

import "context"

// deployCloudWatchDashboard creates a dashboard through PutDashboard. DashboardName is optional on
// the resource page ("If you do not specify a name, one will be generated automatically"); Ref
// returns the name, which is the physical ID, and the type publishes no Fn::GetAtt attributes.
func (d *StackDeployer) deployCloudWatchDashboard(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DashboardName",
		cfnGeneratedName(cctx, "AWS::CloudWatch::Dashboard", logicalID), cctx)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudWatch::Dashboard",
		PhysicalID: name,
		ARN:        cwDashboardARN(cctx.accountID, name),
	}
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, cfnPutDashboardRequest(name, props, cctx), streamID, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// updateCloudWatchDashboard applies a changed DashboardBody in place: PutDashboard replaces a
// dashboard's entire contents. DashboardName is Replacement, so a rename never reaches here.
func (d *StackDeployer) updateCloudWatchDashboard(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, cfnPutDashboardRequest(prior.PhysicalID, props, cctx), streamID, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnPutDashboardRequest is the PutDashboard a dashboard's deploy and update send.
func cfnPutDashboardRequest(name string, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
	return &AWSRequest{
		Service:   "monitoring",
		Operation: "PutDashboard",
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":        "PutDashboard",
			"DashboardName": name,
			"DashboardBody": resolveStringProp(props, "DashboardBody", "", cctx),
		},
	}
}

// cfnCloudWatchDashboardDeleter removes the dashboard with DeleteDashboards, which takes a list.
func cfnCloudWatchDashboardDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	if dr.PhysicalID == "" {
		return nil
	}
	return &AWSRequest{Service: "monitoring", Operation: "DeleteDashboards",
		Headers: map[string]string{},
		Params:  map[string]string{"DashboardNames.member.1": dr.PhysicalID}}
}
