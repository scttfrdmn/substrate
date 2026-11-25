package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// CloudFormation's Cloud Map types (#1614): the three namespace types, AWS::ServiceDiscovery::Service
// and AWS::ServiceDiscovery::Instance. A namespace, a namespace update and an instance registration
// are asynchronous, so their helpers wait on GetOperation as CloudFormation's own handlers do, and a
// FAIL is the resource's failure. Ref is the namespace's or service's Id and the instance's
// InstanceId; the namespaces' Fn::GetAtt answers Id, Arn and (DNS) HostedZoneId, and the service's Id,
// Arn and Name, each from its type's page.

// cfnSDOperationPolls bounds how many GetOperation calls a helper makes before it reports the
// operation as not settled. Each call is one observation of a seeded countdown.
const cfnSDOperationPolls = 100

// cfnSDRequest builds a Cloud Map request carrying body.
func cfnSDRequest(operation string, body map[string]interface{}) (*AWSRequest, error) {
	return cfnJSONRequest(serviceDiscoveryNamespace, operation, body)
}

// cfnSDCall dispatches a Cloud Map request and decodes its answer into out, which may be nil.
func (d *StackDeployer) cfnSDCall(ctx context.Context, operation string, body map[string]interface{}, out any, streamID string) (float64, error) {
	req, err := cfnSDRequest(operation, body)
	if err != nil {
		return 0, err
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		return cost, routeErr
	}
	if out != nil && resp != nil {
		if err := json.Unmarshal(resp.Body, out); err != nil {
			return cost, fmt.Errorf("cfn servicediscovery %s decode: %w", operation, err)
		}
	}
	return cost, nil
}

// cfnSDWait polls GetOperation until the operation settles, returning its targets; a FAIL is an
// error carrying the operation's ErrorCode and ErrorMessage.
func (d *StackDeployer) cfnSDWait(ctx context.Context, operationID, streamID string) (map[string]string, float64, error) {
	var cost float64
	for range cfnSDOperationPolls {
		var out struct {
			Operation struct {
				Status       string            `json:"Status"`
				ErrorCode    string            `json:"ErrorCode"`
				ErrorMessage string            `json:"ErrorMessage"`
				Targets      map[string]string `json:"Targets"`
			} `json:"Operation"`
		}
		c, err := d.cfnSDCall(ctx, "GetOperation", map[string]interface{}{"OperationId": operationID}, &out, streamID)
		cost += c
		if err != nil {
			return nil, cost, err
		}
		switch out.Operation.Status {
		case sdOpSuccess:
			return out.Operation.Targets, cost, nil
		case sdOpFail:
			return nil, cost, fmt.Errorf("operation %s failed: %s: %s", operationID, out.Operation.ErrorCode, out.Operation.ErrorMessage)
		}
	}
	return nil, cost, fmt.Errorf("operation %s did not settle after %d observations", operationID, cfnSDOperationPolls)
}

// cfnSDTags renders a Tags property as the Tag list Cloud Map takes, in key order.
func cfnSDTags(props map[string]interface{}, cctx *cfnContext) []map[string]string {
	tags := cfnTagMap(cfnResolveStructured(props["Tags"], cctx), cctx)
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]string{"Key": k, "Value": tags[k]})
	}
	return out
}

// ----- Namespaces ----------------------------------------------------------------------------------

// cfnSDNamespaceOperations maps each namespace type to its create and update operations.
var cfnSDNamespaceOperations = map[string]struct{ create, update string }{ //nolint:gochecknoglobals // immutable lookup table
	"AWS::ServiceDiscovery::HttpNamespace":       {"CreateHttpNamespace", "UpdateHttpNamespace"},
	"AWS::ServiceDiscovery::PrivateDnsNamespace": {"CreatePrivateDnsNamespace", "UpdatePrivateDnsNamespace"},
	"AWS::ServiceDiscovery::PublicDnsNamespace":  {"CreatePublicDnsNamespace", "UpdatePublicDnsNamespace"},
}

// cfnSDCreatorRequestID derives a namespace create's CreatorRequestId from its inputs: "cfn-" and the
// FNV-64a hash of the account, Region, stack, logical ID, type, Name and Vpc.
func cfnSDCreatorRequestID(cctx *cfnContext, logicalID string, parts ...string) string {
	h := fnv.New64a()
	// Errors are impossible: hash.Hash.Write never returns one.
	_, _ = h.Write([]byte(strings.Join(append([]string{cctx.accountID, cctx.region, cctx.stackName, logicalID}, parts...), "\x00")))
	return fmt.Sprintf("cfn-%016x", h.Sum64())
}

// cfnDeployServiceDiscoveryNamespace is the deploy helper for the three namespace types: it creates
// the namespace, waits for the operation, and reads the namespace back for its ARN and hosted zone.
// The CreatorRequestId is derived from the stack, the logical ID and the declaration's identity
// (type, Name and Vpc), so a retry of the same declaration is idempotent and a replacement — which
// changes one of them — is never answered with the namespace it replaces. A replacement that keeps
// its Name is then refused as a duplicate, as CloudFormation's own is: "If you specify a name, you
// cannot perform updates that require replacement of this resource" (the CloudFormation user
// guide's Name type). It draws nothing from the deployer's mint.
func cfnDeployServiceDiscoveryNamespace(typ string) cfnDeployFunc {
	return func(d *StackDeployer, ctx context.Context, logicalID string, props map[string]interface{},
		streamID string, cctx *cfnContext) (DeployedResource, float64, error) {
		dr := DeployedResource{LogicalID: logicalID, Type: typ}
		body := map[string]interface{}{
			"Name": resolveStringProp(props, "Name", "", cctx),
			"CreatorRequestId": cfnSDCreatorRequestID(cctx, logicalID, typ, resolveStringProp(props, "Name", "", cctx),
				resolveStringProp(props, "Vpc", "", cctx)),
		}
		if v := resolveStringProp(props, "Description", "", cctx); v != "" {
			body["Description"] = v
		}
		if v := resolveStringProp(props, "Vpc", "", cctx); v != "" {
			body["Vpc"] = v
		}
		if p, ok := cfnResolveStructured(props["Properties"], cctx).(map[string]interface{}); ok {
			body["Properties"] = cfnSDCoerce(p)
		}
		if tags := cfnSDTags(props, cctx); len(tags) > 0 {
			body["Tags"] = tags
		}
		var out struct {
			OperationID string `json:"OperationId"`
		}
		cost, err := d.cfnSDCall(ctx, cfnSDNamespaceOperations[typ].create, body, &out, streamID)
		if err != nil {
			dr.Error = err.Error()
			return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
		}
		targets, c, err := d.cfnSDWait(ctx, out.OperationID, streamID)
		cost += c
		if err != nil {
			dr.Error = err.Error()
			return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
		}
		dr.PhysicalID = targets["NAMESPACE"]
		c, err = d.cfnSDReadNamespace(ctx, &dr, streamID)
		cost += c
		if err != nil {
			dr.Error = err.Error()
		}
		return dr, cost, nil
	}
}

// cfnSDReadNamespace fills a namespace's Fn::GetAtt attributes from GetNamespace.
func (d *StackDeployer) cfnSDReadNamespace(ctx context.Context, dr *DeployedResource, streamID string) (float64, error) {
	var ns struct {
		Namespace struct {
			Arn        string `json:"Arn"`
			Properties struct {
				DNSProperties struct {
					HostedZoneID string `json:"HostedZoneId"`
				} `json:"DnsProperties"`
			} `json:"Properties"`
		} `json:"Namespace"`
	}
	cost, err := d.cfnSDCall(ctx, "GetNamespace", map[string]interface{}{"Id": dr.PhysicalID}, &ns, streamID)
	if err != nil {
		return cost, err
	}
	dr.ARN = ns.Namespace.Arn
	dr.Metadata = map[string]interface{}{"Id": dr.PhysicalID, "Arn": ns.Namespace.Arn}
	if zone := ns.Namespace.Properties.DNSProperties.HostedZoneID; zone != "" {
		dr.Metadata["HostedZoneId"] = zone
	}
	return cost, nil
}

// cfnUpdateServiceDiscoveryNamespace applies a changed Description, and a DNS namespace's changed
// Properties, through the type's update operation. Name and Vpc are Replacement and never reach here.
func cfnUpdateServiceDiscoveryNamespace(typ string) cfnUpdateFunc {
	return func(d *StackDeployer, ctx context.Context, logicalID string, prior DeployedResource,
		props map[string]interface{}, _ []string, streamID string, cctx *cfnContext) (DeployedResource, float64, error) {
		dr := prior
		dr.LogicalID = logicalID
		dr.Error = ""
		change := map[string]interface{}{"Description": resolveStringProp(props, "Description", "", cctx)}
		if p, ok := cfnResolveStructured(props["Properties"], cctx).(map[string]interface{}); ok && typ != "AWS::ServiceDiscovery::HttpNamespace" {
			change["Properties"] = cfnSDCoerce(p)
		}
		var out struct {
			OperationID string `json:"OperationId"`
		}
		cost, err := d.cfnSDCall(ctx, cfnSDNamespaceOperations[typ].update,
			map[string]interface{}{"Id": prior.PhysicalID, "UpdaterRequestId": streamID + "-" + logicalID, "Namespace": change}, &out, streamID)
		if err == nil {
			var c float64
			_, c, err = d.cfnSDWait(ctx, out.OperationID, streamID)
			cost += c
		}
		if err != nil {
			dr.Error = err.Error()
		}
		return dr, cost, nil
	}
}

// cfnServiceDiscoveryNamespaceDeleter is the namespace's delete request, which
// [cfnDeleteServiceDiscoveryNamespace] dispatches and waits on; its hosted zone goes with it.
func cfnServiceDiscoveryNamespaceDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	if dr.PhysicalID == "" {
		return nil
	}
	req, err := cfnSDRequest("DeleteNamespace", map[string]interface{}{"Id": dr.PhysicalID})
	if err != nil {
		return nil
	}
	return req
}

// ----- Services ------------------------------------------------------------------------------------

// cfnSDServiceBody renders the members of a service the template declares.
func cfnSDServiceBody(props map[string]interface{}, cctx *cfnContext) map[string]interface{} {
	body := map[string]interface{}{}
	for _, k := range []string{"DnsConfig", "HealthCheckConfig", "HealthCheckCustomConfig"} {
		if m, ok := cfnResolveStructured(props[k], cctx).(map[string]interface{}); ok {
			body[k] = cfnSDCoerce(m)
		}
	}
	return body
}

// cfnSDNumericMembers are the members of Cloud Map's configuration structures the model types as
// numbers. A template that sets one through a Ref to a parameter resolves it to a string, which the
// JSON protocol would refuse, so it is converted back.
var cfnSDNumericMembers = map[string]bool{"TTL": true, "FailureThreshold": true} //nolint:gochecknoglobals // immutable lookup table

// cfnSDCoerce copies a resolved configuration, converting the string spelling of a numeric member
// back to a number, recursively.
func cfnSDCoerce(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = cfnSDCoerceValue(k, v)
	}
	return out
}

// cfnSDCoerceValue converts one member's value; see [cfnSDCoerce].
func cfnSDCoerceValue(key string, v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return cfnSDCoerce(x)
	case []interface{}:
		list := make([]interface{}, len(x))
		for i, e := range x {
			list[i] = cfnSDCoerceValue(key, e)
		}
		return list
	case string:
		if cfnSDNumericMembers[key] {
			if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
				return n
			}
		}
	}
	return v
}

// deployServiceDiscoveryService creates the service with CreateService. ServiceAttributes needs
// UpdateServiceAttributes, which substrate does not route, so a template setting it fails the
// resource rather than losing them.
func (d *StackDeployer) deployServiceDiscoveryService(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ServiceDiscovery::Service"}
	if _, ok := props["ServiceAttributes"]; ok {
		dr.Error = "ServiceAttributes needs UpdateServiceAttributes, which substrate does not route"
		return dr, 0, nil
	}
	body := cfnSDServiceBody(props, cctx)
	body["Name"] = resolveStringProp(props, "Name", logicalID, cctx)
	body["CreatorRequestId"] = streamID + "-" + logicalID
	for _, k := range []string{"NamespaceId", "Description", "Type"} {
		if v := resolveStringProp(props, k, "", cctx); v != "" {
			body[k] = v
		}
	}
	if tags := cfnSDTags(props, cctx); len(tags) > 0 {
		body["Tags"] = tags
	}
	var out struct {
		Service struct {
			ID   string `json:"Id"`
			Arn  string `json:"Arn"`
			Name string `json:"Name"`
		} `json:"Service"`
	}
	cost, err := d.cfnSDCall(ctx, "CreateService", body, &out, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	dr.PhysicalID, dr.ARN = out.Service.ID, out.Service.Arn
	dr.Metadata = map[string]interface{}{"Id": out.Service.ID, "Arn": out.Service.Arn, "Name": out.Service.Name}
	return dr, cost, nil
}

// updateServiceDiscoveryService applies a changed Description, DnsConfig or HealthCheckConfig
// through UpdateService. Every one is sent, so a removed HealthCheckConfig is deleted, as
// UpdateService deletes an omitted one.
func (d *StackDeployer) updateServiceDiscoveryService(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	change := map[string]interface{}{"Description": resolveStringProp(props, "Description", "", cctx)}
	body := cfnSDServiceBody(props, cctx)
	if dns, ok := body["DnsConfig"].(map[string]interface{}); ok {
		change["DnsConfig"] = map[string]interface{}{"DnsRecords": dns["DnsRecords"]}
	}
	if hc, ok := body["HealthCheckConfig"]; ok {
		change["HealthCheckConfig"] = hc
	}
	var out struct {
		OperationID string `json:"OperationId"`
	}
	cost, err := d.cfnSDCall(ctx, "UpdateService", map[string]interface{}{"Id": prior.PhysicalID, "Service": change}, &out, streamID)
	if err == nil {
		var c float64
		_, c, err = d.cfnSDWait(ctx, out.OperationID, streamID)
		cost += c
	}
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// ----- Instances -----------------------------------------------------------------------------------

// deployServiceDiscoveryInstance registers the instance with RegisterInstance and waits for the
// operation. An absent InstanceId is the logical ID. RegisterInstance updates an instance it already
// holds, so a re-deploy of changed InstanceAttributes updates it in place.
//
// The physical ID is the InstanceId, which Ref returns as the type's page publishes. A ServiceId
// change is a replacement that keeps the InstanceId, so the instance's ServiceId is part of its
// identity ([cfnIdentityMetadata]) and the update deregisters the old one.
func (d *StackDeployer) deployServiceDiscoveryInstance(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	serviceID := resolveStringProp(props, "ServiceId", "", cctx)
	instanceID := resolveStringProp(props, "InstanceId", logicalID, cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ServiceDiscovery::Instance",
		PhysicalID: instanceID, Metadata: map[string]interface{}{"ServiceId": serviceID, "InstanceId": instanceID}}
	attrs := map[string]string{}
	if m, ok := cfnResolveStructured(props["InstanceAttributes"], cctx).(map[string]interface{}); ok {
		for k, v := range m {
			attrs[k] = fmt.Sprint(v)
		}
	}
	var out struct {
		OperationID string `json:"OperationId"`
	}
	cost, err := d.cfnSDCall(ctx, "RegisterInstance", map[string]interface{}{
		"ServiceId": serviceID, "InstanceId": instanceID, "Attributes": attrs, "CreatorRequestId": streamID + "-" + logicalID,
	}, &out, streamID)
	if err == nil {
		var c float64
		_, c, err = d.cfnSDWait(ctx, out.OperationID, streamID)
		cost += c
	}
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnServiceDiscoveryInstanceDeleter is the instance's deregistration, which
// [cfnDeleteServiceDiscoveryInstance] dispatches and waits on.
func cfnServiceDiscoveryInstanceDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	serviceID, instanceID := cfnRefMetadata(dr, "ServiceId"), cfnRefMetadata(dr, "InstanceId")
	if instanceID == "" {
		instanceID = dr.PhysicalID
	}
	if serviceID == "" || instanceID == "" {
		return nil
	}
	req, err := cfnSDRequest("DeregisterInstance", map[string]interface{}{"ServiceId": serviceID, "InstanceId": instanceID})
	if err != nil {
		return nil
	}
	return req
}

// cfnDeleteServiceDiscoveryAsync dispatches an asynchronous Cloud Map delete — a namespace's or an
// instance's — and waits on its operation, so a FAIL (seeded, or a hosted zone Route 53 refuses to
// delete) fails the resource rather than reading as deleted. A resource already gone is a completed
// delete, as [cfnDeleteIsAbsent] reads it for every type.
func cfnDeleteServiceDiscoveryAsync(build cfnDeleteRequestFunc) func(context.Context, *StackDeployer, DeployedResource,
	map[string]interface{}, *cfnContext, string) *CFNResourceDeletion {
	return func(ctx context.Context, d *StackDeployer, dr DeployedResource, props map[string]interface{},
		cctx *cfnContext, streamID string) *CFNResourceDeletion {
		failed := func(err error) *CFNResourceDeletion {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
		req := build(d, dr, props, cctx)
		if req == nil {
			return nil
		}
		resp, _, err := d.dispatch(ctx, req, streamID)
		if err != nil {
			if cfnDeleteIsAbsent(err) {
				return nil
			}
			return failed(err)
		}
		var out struct {
			OperationID string `json:"OperationId"`
		}
		if resp != nil {
			if err := json.Unmarshal(resp.Body, &out); err != nil {
				return failed(fmt.Errorf("cfn servicediscovery delete decode: %w", err))
			}
		}
		if out.OperationID == "" {
			return failed(fmt.Errorf("%s answered no OperationId to wait on", req.Operation))
		}
		if _, _, err := d.cfnSDWait(ctx, out.OperationID, streamID); err != nil {
			return failed(err)
		}
		return nil
	}
}

// cfnDeleteServiceDiscoveryNamespace deletes a namespace and waits on the operation.
var cfnDeleteServiceDiscoveryNamespace = cfnDeleteServiceDiscoveryAsync(cfnServiceDiscoveryNamespaceDeleter) //nolint:gochecknoglobals // immutable function value

// cfnDeleteServiceDiscoveryInstance deregisters an instance and waits on the operation.
var cfnDeleteServiceDiscoveryInstance = cfnDeleteServiceDiscoveryAsync(cfnServiceDiscoveryInstanceDeleter) //nolint:gochecknoglobals // immutable function value
