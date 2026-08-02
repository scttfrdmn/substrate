package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// EC2FleetOverride is one launch-template override in an EC2 Fleet request: an
// (instance type, subnet) pool the fleet may draw capacity from.
type EC2FleetOverride struct {
	// InstanceType is the instance type for this pool.
	InstanceType string `json:"instanceType,omitempty"`

	// SubnetID is the subnet the pool launches into.
	SubnetID string `json:"subnetId,omitempty"`

	// AvailabilityZone is the AZ for this pool, when specified instead of a subnet.
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// ImageID overrides the launch template's AMI. Supported only for instant fleets.
	ImageID string `json:"imageId,omitempty"`

	// MaxPrice is the maximum Spot price per unit hour for this pool.
	MaxPrice string `json:"maxPrice,omitempty"`

	// Priority orders the pool when the allocation strategy is "prioritized";
	// lower numbers are launched first.
	Priority float64 `json:"priority,omitempty"`

	// WeightedCapacity is the number of units this instance type provides. It is
	// echoed back but does not scale the instance count — see [EC2Plugin.createFleet].
	WeightedCapacity float64 `json:"weightedCapacity,omitempty"`

	// PlacementGroupName constrains this pool to a placement group.
	PlacementGroupName string `json:"placementGroupName,omitempty"`
}

// EC2FleetLaunchTemplateConfig pairs a launch template with the overrides that
// replace values in it.
type EC2FleetLaunchTemplateConfig struct {
	// LaunchTemplateID is the referenced launch template's ID.
	LaunchTemplateID string `json:"launchTemplateId,omitempty"`

	// LaunchTemplateName is the referenced launch template's name.
	LaunchTemplateName string `json:"launchTemplateName,omitempty"`

	// Version is the launch template version ("$Latest", "$Default", or a number).
	Version string `json:"version,omitempty"`

	// Overrides holds the per-pool overrides for this launch template.
	Overrides []EC2FleetOverride `json:"overrides,omitempty"`
}

// EC2FleetInstanceGroup records the instances launched from a single pool. The
// IDs are grouped per pool because that is how CreateFleet reports them:
// fleetInstanceSet contains one item per pool, each carrying a *list* of
// instance IDs (#387).
type EC2FleetInstanceGroup struct {
	// InstanceIDs are the instances launched from this pool.
	InstanceIDs []string `json:"instanceIds"`

	// InstanceType is the instance type the pool launched.
	InstanceType string `json:"instanceType,omitempty"`

	// Lifecycle is "spot" or "on-demand".
	Lifecycle string `json:"lifecycle,omitempty"`

	// LaunchTemplateID and LaunchTemplateName identify the pool's launch template.
	LaunchTemplateID   string `json:"launchTemplateId,omitempty"`
	LaunchTemplateName string `json:"launchTemplateName,omitempty"`

	// Override is the override that produced this pool.
	Override EC2FleetOverride `json:"override"`
}

// EC2FleetError records capacity a fleet could not fulfill, mirroring one
// CreateFleetError item.
type EC2FleetError struct {
	// ErrorCode is the reason the launch failed, e.g. "InsufficientInstanceCapacity".
	ErrorCode string `json:"errorCode"`

	// ErrorMessage describes the failure.
	ErrorMessage string `json:"errorMessage"`

	// Lifecycle is "spot" or "on-demand".
	Lifecycle string `json:"lifecycle,omitempty"`

	// LaunchTemplateID and LaunchTemplateName identify the pool that failed.
	LaunchTemplateID   string `json:"launchTemplateId,omitempty"`
	LaunchTemplateName string `json:"launchTemplateName,omitempty"`

	// Override is the override describing the pool that failed.
	Override EC2FleetOverride `json:"override"`
}

// EC2Fleet represents an EC2 Fleet request and the outcome of fulfilling it.
type EC2Fleet struct {
	// FleetID is the unique identifier (e.g. "fleet-12a34b56-...").
	FleetID string `json:"fleetId"`

	// FleetState is the fleet's state ("active", "deleted_terminating", ...).
	FleetState string `json:"fleetState"`

	// ActivityStatus is the fulfillment progress. For instant fleets this is
	// always "fulfilled" once requests are placed, even on a partial fulfillment.
	ActivityStatus string `json:"activityStatus"`

	// Type is the fleet type: "instant", "request", or "maintain".
	Type string `json:"type"`

	// TotalTargetCapacity is the requested capacity — what was asked for, not
	// what was delivered. Callers routinely mistake this for the result.
	TotalTargetCapacity int `json:"totalTargetCapacity"`

	// OnDemandTargetCapacity and SpotTargetCapacity are the per-lifecycle requests.
	OnDemandTargetCapacity int `json:"onDemandTargetCapacity"`
	SpotTargetCapacity     int `json:"spotTargetCapacity"`

	// DefaultTargetCapacityType is "on-demand" or "spot".
	DefaultTargetCapacityType string `json:"defaultTargetCapacityType"`

	// FulfilledCapacity is the number of instances actually launched.
	FulfilledCapacity int `json:"fulfilledCapacity"`

	// LaunchTemplateConfigs is the request's launch-template/override matrix.
	LaunchTemplateConfigs []EC2FleetLaunchTemplateConfig `json:"launchTemplateConfigs,omitempty"`

	// Instances groups the launched instances by pool.
	Instances []EC2FleetInstanceGroup `json:"instances,omitempty"`

	// Errors describes capacity the fleet could not fulfill.
	Errors []EC2FleetError `json:"errors,omitempty"`

	// Tags holds the fleet's own tags (ResourceType=fleet).
	Tags []EC2Tag `json:"tags,omitempty"`

	// ClientToken is the caller-supplied idempotency token.
	ClientToken string `json:"clientToken,omitempty"`

	// CreateTime is the RFC3339 creation timestamp.
	CreateTime string `json:"createTime"`

	// AccountID and Region locate the fleet.
	AccountID string `json:"accountId"`
	Region    string `json:"region"`
}

// generateFleetID generates an EC2 Fleet ID in the AWS format, "fleet-"
// followed by a UUID-shaped hex string.
func generateFleetID() string {
	return fmt.Sprintf("fleet-%s-%s-%s-%s-%s",
		randomHex(4), randomHex(2), randomHex(2), randomHex(2), randomHex(6))
}

// ec2FleetStateKey returns the state key for a fleet.
func ec2FleetStateKey(accountID, region, fleetID string) string {
	return "fleet:" + accountID + "/" + region + "/" + fleetID
}

// createFleet handles CreateFleet.
//
// The emulated fulfillment model is deliberately simple and fully deterministic:
// the launch-template/override matrix defines an ordered list of capacity pools
// (sorted by Priority when the allocation strategy is "prioritized"), and the
// fulfilled instances are distributed round-robin across those pools. Grouping
// by pool is what makes fleetInstanceSet contain one item per pool, each with a
// *list* of instance IDs — the shape callers most often flatten incorrectly.
//
// By default a fleet fulfills its entire TotalTargetCapacity. A seeded shortfall
// (POST /v1/ec2/fleet-shortfall) makes the fleet fulfill fewer instances and
// report the remainder in errorSet, which is how a test exercises the partial
// fulfillment path that is rare and hard to trigger against real AWS (#387).
//
// Instances are created through runInstances, so everything that path
// establishes — subnet/security-group validation, placement-group existence,
// public IP assignment, launch-time tag propagation, and visibility to
// DescribeInstances — applies to fleet instances too.
//
// TargetCapacityUnitType and WeightedCapacity are recorded and echoed but do not
// scale the instance count: capacity is counted in instances, which is the
// default "units" behavior.
func (p *EC2Plugin) createFleet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	fleetType := req.Params["Type"]
	if fleetType == "" {
		fleetType = "maintain" // AWS default.
	}
	switch fleetType {
	case "instant", "request", "maintain":
	default:
		return nil, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "Invalid fleet type: " + fleetType,
			HTTPStatus: http.StatusBadRequest,
		}
	}

	total, err := strconv.Atoi(req.Params["TargetCapacitySpecification.TotalTargetCapacity"])
	if err != nil || total < 0 {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "TargetCapacitySpecification.TotalTargetCapacity is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	onDemand, _ := strconv.Atoi(req.Params["TargetCapacitySpecification.OnDemandTargetCapacity"])
	spot, _ := strconv.Atoi(req.Params["TargetCapacitySpecification.SpotTargetCapacity"])
	defaultType := req.Params["TargetCapacitySpecification.DefaultTargetCapacityType"]
	if defaultType == "" {
		defaultType = "on-demand" // AWS default.
	}

	configs := parseFleetLaunchTemplateConfigs(req.Params)
	if len(configs) == 0 {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "LaunchTemplateConfigs is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// Every referenced launch template must exist, mirroring AWS — so a
	// create-template → create-fleet ordering error is observable.
	for _, cfg := range configs {
		if lt := p.resolveLaunchTemplate(goCtx, reqCtx, cfg.LaunchTemplateID, cfg.LaunchTemplateName); lt == nil {
			if cfg.LaunchTemplateID != "" {
				return nil, &AWSError{
					Code:       "InvalidLaunchTemplateId.NotFound",
					Message:    "Launch template " + cfg.LaunchTemplateID + " does not exist",
					HTTPStatus: http.StatusBadRequest,
				}
			}
			return nil, &AWSError{
				Code:       "InvalidLaunchTemplateName.NotFoundException",
				Message:    "Launch template with name '" + cfg.LaunchTemplateName + "' does not exist",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}

	pools := fleetPools(configs, fleetAllocationStrategy(req.Params, defaultType))

	// Resolve a seeded shortfall against the first config's template.
	fulfill := total
	shortfallCode := "InsufficientInstanceCapacity"
	shortfallMsg := ""
	shortfallLifecycle := ""
	seed, err := p.resolveFleetShortfall(configs[0].LaunchTemplateID, configs[0].LaunchTemplateName)
	if err != nil {
		return nil, err
	}
	if seed != nil {
		fulfill = min(seed.Fulfill, total)
		if seed.ErrorCode != "" {
			shortfallCode = seed.ErrorCode
		}
		shortfallMsg = seed.ErrorMessage
		shortfallLifecycle = seed.Lifecycle
	}
	if shortfallMsg == "" {
		shortfallMsg = fleetDefaultErrorMessage(shortfallCode)
	}
	if shortfallLifecycle == "" {
		shortfallLifecycle = defaultType
	}

	// Requested and fulfilled counts per pool, distributed round-robin.
	wantPerPool := distributeAcrossPools(total, len(pools))
	gotPerPool := distributeAcrossPools(fulfill, len(pools))

	// The fleet's own instance-scoped tags are the caller's, so they are checked here
	// exactly as RunInstances checks its own. Only the fleet-ID tag substrate adds
	// afterwards is exempt, and it is exempt by never being part of a request (#468).
	instanceTags := ec2LaunchTagsForResource(req.Params, "instance")
	if awsErr := ec2CheckReservedTagKeys(instanceTags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, instanceTags); awsErr != nil {
		return nil, awsErr
	}
	fleetTags := ec2LaunchTagsForResource(req.Params, "fleet")
	if awsErr := ec2CheckReservedTagKeys(fleetTags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, fleetTags); awsErr != nil {
		return nil, awsErr
	}

	fleet := EC2Fleet{
		FleetID:                   generateFleetID(),
		FleetState:                "active",
		Type:                      fleetType,
		TotalTargetCapacity:       total,
		OnDemandTargetCapacity:    onDemand,
		SpotTargetCapacity:        spot,
		DefaultTargetCapacityType: defaultType,
		LaunchTemplateConfigs:     configs,
		Tags:                      fleetTags,
		ClientToken:               req.Params["ClientToken"],
		CreateTime:                p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:                 reqCtx.AccountID,
		Region:                    reqCtx.Region,
	}
	// For instant fleets AWS reports "fulfilled" once all requests are placed,
	// regardless of whether target capacity was actually met.
	if fleetType == "instant" {
		fleet.ActivityStatus = "fulfilled"
	} else {
		fleet.ActivityStatus = "pending_fulfillment"
	}

	for i, pool := range pools {
		lifecycle := defaultType
		if pool.override.MaxPrice != "" {
			lifecycle = "spot"
		}

		if gotPerPool[i] > 0 {
			ids, launchErr := p.launchFleetPool(reqCtx, pool, gotPerPool[i], instanceTags, fleet.FleetID)
			if launchErr != nil {
				return nil, launchErr
			}
			fleet.Instances = append(fleet.Instances, EC2FleetInstanceGroup{
				InstanceIDs:        ids,
				InstanceType:       pool.instanceType(),
				Lifecycle:          lifecycle,
				LaunchTemplateID:   pool.ltID,
				LaunchTemplateName: pool.ltName,
				Override:           pool.override,
			})
			fleet.FulfilledCapacity += len(ids)
		}

		// Any pool that received less than its share reports the shortfall.
		if gotPerPool[i] < wantPerPool[i] {
			errLifecycle := shortfallLifecycle
			if pool.override.MaxPrice != "" {
				errLifecycle = "spot"
			}
			fleet.Errors = append(fleet.Errors, EC2FleetError{
				ErrorCode:          shortfallCode,
				ErrorMessage:       shortfallMsg,
				Lifecycle:          errLifecycle,
				LaunchTemplateID:   pool.ltID,
				LaunchTemplateName: pool.ltName,
				Override:           pool.override,
			})
		}
	}

	data, err := json.Marshal(fleet)
	if err != nil {
		return nil, fmt.Errorf("ec2 createFleet marshal: %w", err)
	}
	key := ec2FleetStateKey(reqCtx.AccountID, reqCtx.Region, fleet.FleetID)
	if err := p.state.Put(goCtx, ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createFleet state.Put: %w", err)
	}

	return p.createFleetResponse(&fleet)
}

// fleetPool is one resolved capacity pool: a launch template plus one override.
type fleetPool struct {
	ltID     string
	ltName   string
	version  string
	override EC2FleetOverride
}

// instanceType returns the pool's overridden instance type, which may be empty
// when the launch template's own type should apply.
func (f fleetPool) instanceType() string { return f.override.InstanceType }

// fleetPools flattens launch-template configs into an ordered pool list. With
// the "prioritized" allocation strategy the pools are sorted by Priority
// (lowest first, AWS's highest-priority-first rule); otherwise request order is
// preserved. A config with no overrides contributes a single pool that launches
// straight from the launch template.
func fleetPools(configs []EC2FleetLaunchTemplateConfig, strategy string) []fleetPool {
	var pools []fleetPool
	for _, cfg := range configs {
		if len(cfg.Overrides) == 0 {
			pools = append(pools, fleetPool{ltID: cfg.LaunchTemplateID, ltName: cfg.LaunchTemplateName, version: cfg.Version})
			continue
		}
		for _, ov := range cfg.Overrides {
			pools = append(pools, fleetPool{
				ltID:     cfg.LaunchTemplateID,
				ltName:   cfg.LaunchTemplateName,
				version:  cfg.Version,
				override: ov,
			})
		}
	}
	if strategy == "prioritized" || strategy == "capacity-optimized-prioritized" {
		sort.SliceStable(pools, func(i, j int) bool {
			return pools[i].override.Priority < pools[j].override.Priority
		})
	}
	return pools
}

// fleetAllocationStrategy returns the allocation strategy that orders the pools,
// reading the Spot or On-Demand options according to the default capacity type.
func fleetAllocationStrategy(params map[string]string, defaultType string) string {
	if defaultType == "spot" {
		if s := params["SpotOptions.AllocationStrategy"]; s != "" {
			return s
		}
		return ""
	}
	return params["OnDemandOptions.AllocationStrategy"]
}

// distributeAcrossPools splits n items across pools round-robin, so earlier
// pools take the remainder. Returns a per-pool count slice.
func distributeAcrossPools(n, pools int) []int {
	out := make([]int, pools)
	if pools == 0 || n <= 0 {
		return out
	}
	base, rem := n/pools, n%pools
	for i := range out {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

// launchFleetPool launches count instances for one pool by dispatching through
// runInstances, and returns the resulting instance IDs. Going through
// runInstances (rather than writing instance state directly) is what makes fleet
// instances indistinguishable from directly-launched ones to DescribeInstances.
//
// fleetID is stamped on every instance as [ec2FleetIDTagKey], which is what lets a
// caller get from a fleet back to its instances at all (#443).
func (p *EC2Plugin) launchFleetPool(
	reqCtx *RequestContext,
	pool fleetPool,
	count int,
	instanceTags []EC2Tag,
	fleetID string,
) ([]string, error) {
	params := map[string]string{
		"Action":   "RunInstances",
		"MinCount": strconv.Itoa(count),
		"MaxCount": strconv.Itoa(count),
	}
	if pool.ltID != "" {
		params["LaunchTemplate.LaunchTemplateId"] = pool.ltID
	}
	if pool.ltName != "" {
		params["LaunchTemplate.LaunchTemplateName"] = pool.ltName
	}
	// The config's version is forwarded so a fleet pinned to a specific launch
	// template version gets that version's parameters. fleetPools has always parsed
	// this value; before #456 nothing consumed it, so a fleet naming version 1 of a
	// template silently launched whatever the template held latest.
	if pool.version != "" {
		params["LaunchTemplate.Version"] = pool.version
	}
	// An override's ImageId replaces the launch template's AMI.
	if pool.override.ImageID != "" {
		params["ImageId"] = pool.override.ImageID
	}
	if pool.override.InstanceType != "" {
		params["InstanceType"] = pool.override.InstanceType
	}
	if pool.override.SubnetID != "" {
		params["SubnetId"] = pool.override.SubnetID
	}
	if pool.override.PlacementGroupName != "" {
		params["Placement.GroupName"] = pool.override.PlacementGroupName
	}
	// The caller's tags travel as already-parsed values rather than as re-emitted
	// TagSpecification.N params, so the fleet-ID tag can be appended without hunting
	// for a free index — and, more to the point, without riding the request-tag path
	// that now rejects reserved keys. See [EC2Plugin.runInstances] for why that split
	// is structural rather than a bypass flag (#468).
	tags := instanceTags
	if fleetID != "" {
		tags = append(append([]EC2Tag{}, instanceTags...), EC2Tag{
			Key:   ec2FleetIDTagKey,
			Value: fleetID,
		})
	}

	resp, err := p.runInstancesWithTags(reqCtx, &AWSRequest{
		Service:   "ec2",
		Operation: "RunInstances",
		Params:    params,
		Headers:   map[string]string{},
	}, tags)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("ec2 launchFleetPool parse RunInstances response: %w", err)
	}
	ids := make([]string, 0, len(parsed.Instances))
	for _, inst := range parsed.Instances {
		ids = append(ids, inst.InstanceID)
	}
	return ids, nil
}

// parseFleetLaunchTemplateConfigs parses LaunchTemplateConfigs.N and their
// nested Overrides.M from the flattened query parameters.
func parseFleetLaunchTemplateConfigs(params map[string]string) []EC2FleetLaunchTemplateConfig {
	var configs []EC2FleetLaunchTemplateConfig
	for n := 1; ; n++ {
		prefix := fmt.Sprintf("LaunchTemplateConfigs.%d.", n)
		spec := prefix + "LaunchTemplateSpecification."
		ltID := params[spec+"LaunchTemplateId"]
		ltName := params[spec+"LaunchTemplateName"]
		version := params[spec+"Version"]

		cfg := EC2FleetLaunchTemplateConfig{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: ltName,
			Version:            version,
		}
		for m := 1; ; m++ {
			ovPrefix := fmt.Sprintf("%sOverrides.%d.", prefix, m)
			ov, ok := parseFleetOverride(params, ovPrefix)
			if !ok {
				break
			}
			cfg.Overrides = append(cfg.Overrides, ov)
		}
		if ltID == "" && ltName == "" && len(cfg.Overrides) == 0 {
			break
		}
		configs = append(configs, cfg)
	}
	return configs
}

// parseFleetOverride parses a single Overrides.M entry. The bool reports whether
// any field was present, which terminates the caller's index scan.
func parseFleetOverride(params map[string]string, prefix string) (EC2FleetOverride, bool) {
	ov := EC2FleetOverride{
		InstanceType:       params[prefix+"InstanceType"],
		SubnetID:           params[prefix+"SubnetId"],
		AvailabilityZone:   params[prefix+"AvailabilityZone"],
		ImageID:            params[prefix+"ImageId"],
		MaxPrice:           params[prefix+"MaxPrice"],
		PlacementGroupName: params[prefix+"Placement.GroupName"],
	}
	priority, hasPriority := params[prefix+"Priority"]
	if hasPriority {
		ov.Priority, _ = strconv.ParseFloat(priority, 64)
	}
	weight, hasWeight := params[prefix+"WeightedCapacity"]
	if hasWeight {
		ov.WeightedCapacity, _ = strconv.ParseFloat(weight, 64)
	}

	present := ov.InstanceType != "" || ov.SubnetID != "" || ov.AvailabilityZone != "" ||
		ov.ImageID != "" || ov.MaxPrice != "" || ov.PlacementGroupName != "" ||
		hasPriority || hasWeight
	return ov, present
}

// ec2FleetIDTagKey is the reserved tag EC2 stamps on every instance a fleet
// launches, naming the fleet that created it.
//
// Source note: this tag is not described in the EC2 API reference or the fleet
// tagging/describe pages — the authority is observed real-AWS behavior, reported
// by the parsl-aws-provider consumer in #443, whose fleet-to-instance lookup is
// built on it. Substrate models it because for an "instant" fleet it is the only
// route from a fleet back to its live instances: DescribeFleetInstances rejects
// instant fleets outright, and DescribeFleets' instance list never drops
// terminated instances, so a fully-running fleet is indistinguishable from an
// empty one without this tag. It is deliberately not gated on the fleet type,
// since real EC2 applies it to every fleet.
//
// The "aws:" prefix is reserved: per the EC2 tagging documentation such a tag
// cannot be edited or deleted by a caller and does not count against the 50-tag
// per-resource limit. CreateTags and DeleteTags enforce the first rule (#452), and
// every tag-on-create path now enforces it too (#468). The second rule is why
// [ec2CheckTagLimit] excludes reserved keys from its count (#469): this tag is stamped
// on every fleet instance, so counting it would reject a fleet instance carrying the
// full 50 user tags, which real EC2 accepts.
//
// Substrate stamps this tag without tripping its own check because
// [EC2Plugin.launchFleetPool] appends it to the already-parsed, already-checked tag
// slice it hands [EC2Plugin.runInstancesWithTags] — it never becomes a
// TagSpecification.N param, and so is never part of anything a caller could send.
// That is deliberately a structural exemption rather than a validation-skipping flag:
// a flag would make the check's outcome depend on internal state a consumer cannot
// see, whereas this way the checked path is simply the only path a request can reach.
const ec2FleetIDTagKey = "aws:ec2:fleet-id"

// fleetDefaultErrorMessage returns the message AWS pairs with a fleet error code.
func fleetDefaultErrorMessage(code string) string {
	switch code {
	case "InsufficientInstanceCapacity":
		return "There is no Spot capacity available that matches your request."
	case "InsufficientFreeAddressesInSubnet":
		return "The specified subnet does not have enough free addresses to satisfy the request."
	case "UnfulfillableCapacity":
		return "Unable to fulfill capacity due to your request configuration."
	default:
		return "The fleet could not launch the requested capacity."
	}
}

// fleetOverrideXML is the XML rendering of a launch template override.
type fleetOverrideXML struct {
	InstanceType     string  `xml:"instanceType,omitempty"`
	SubnetID         string  `xml:"subnetId,omitempty"`
	AvailabilityZone string  `xml:"availabilityZone,omitempty"`
	ImageID          string  `xml:"imageId,omitempty"`
	MaxPrice         string  `xml:"maxPrice,omitempty"`
	Priority         float64 `xml:"priority,omitempty"`
	WeightedCapacity float64 `xml:"weightedCapacity,omitempty"`
	Placement        *struct {
		GroupName string `xml:"groupName,omitempty"`
	} `xml:"placement,omitempty"`
}

// fleetLTSpecXML is the XML rendering of a fleet launch template specification.
type fleetLTSpecXML struct {
	LaunchTemplateID   string `xml:"launchTemplateId,omitempty"`
	LaunchTemplateName string `xml:"launchTemplateName,omitempty"`
	Version            string `xml:"version,omitempty"`
}

// fleetLTAndOverridesXML is the XML rendering of LaunchTemplateAndOverridesResponse.
type fleetLTAndOverridesXML struct {
	LaunchTemplateSpecification fleetLTSpecXML   `xml:"launchTemplateSpecification"`
	Overrides                   fleetOverrideXML `xml:"overrides"`
}

// fleetLTAndOverrides renders the launch template plus override for a pool.
func fleetLTAndOverrides(ltID, ltName, version string, ov EC2FleetOverride) fleetLTAndOverridesXML {
	out := fleetLTAndOverridesXML{
		LaunchTemplateSpecification: fleetLTSpecXML{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: ltName,
			Version:            version,
		},
		Overrides: fleetOverrideXML{
			InstanceType:     ov.InstanceType,
			SubnetID:         ov.SubnetID,
			AvailabilityZone: ov.AvailabilityZone,
			ImageID:          ov.ImageID,
			MaxPrice:         ov.MaxPrice,
			Priority:         ov.Priority,
			WeightedCapacity: ov.WeightedCapacity,
		},
	}
	if ov.PlacementGroupName != "" {
		out.Overrides.Placement = &struct {
			GroupName string `xml:"groupName,omitempty"`
		}{GroupName: ov.PlacementGroupName}
	}
	return out
}

// createFleetResponse renders the CreateFleet response. For non-instant fleets
// AWS returns only the fleet ID: instances and errors are reported
// asynchronously, so a caller that reads Instances from a "request"/"maintain"
// fleet legitimately sees nothing.
func (p *EC2Plugin) createFleetResponse(fleet *EC2Fleet) (*AWSResponse, error) {
	type instanceItem struct {
		LaunchTemplateAndOverrides fleetLTAndOverridesXML `xml:"launchTemplateAndOverrides"`
		Lifecycle                  string                 `xml:"lifecycle,omitempty"`
		InstanceIDs                []string               `xml:"instanceIds>item"`
		InstanceType               string                 `xml:"instanceType,omitempty"`
	}
	type errorItem struct {
		LaunchTemplateAndOverrides fleetLTAndOverridesXML `xml:"launchTemplateAndOverrides"`
		Lifecycle                  string                 `xml:"lifecycle,omitempty"`
		ErrorCode                  string                 `xml:"errorCode"`
		ErrorMessage               string                 `xml:"errorMessage"`
	}
	type response struct {
		XMLName   xml.Name       `xml:"CreateFleetResponse"`
		XMLNS     string         `xml:"xmlns,attr"`
		FleetID   string         `xml:"fleetId"`
		Instances []instanceItem `xml:"fleetInstanceSet>item,omitempty"`
		Errors    []errorItem    `xml:"errorSet>item,omitempty"`
	}

	resp := response{
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		FleetID: fleet.FleetID,
	}
	// fleetInstanceSet and errorSet are populated only for instant fleets.
	if fleet.Type == "instant" {
		for _, g := range fleet.Instances {
			resp.Instances = append(resp.Instances, instanceItem{
				LaunchTemplateAndOverrides: fleetLTAndOverrides(g.LaunchTemplateID, g.LaunchTemplateName, "", g.Override),
				Lifecycle:                  g.Lifecycle,
				InstanceIDs:                g.InstanceIDs,
				InstanceType:               g.InstanceType,
			})
		}
		for _, e := range fleet.Errors {
			resp.Errors = append(resp.Errors, errorItem{
				LaunchTemplateAndOverrides: fleetLTAndOverrides(e.LaunchTemplateID, e.LaunchTemplateName, "", e.Override),
				Lifecycle:                  e.Lifecycle,
				ErrorCode:                  e.ErrorCode,
				ErrorMessage:               e.ErrorMessage,
			})
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// describeFleets handles DescribeFleets. Mirroring AWS, an instant fleet appears
// only when its ID is named explicitly.
func (p *EC2Plugin) describeFleets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids := extractIndexedParams(req.Params, "FleetId")
	filters := extractEC2Filters(req.Params)

	keys, err := p.state.List(goCtx, ec2Namespace, "fleet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeFleets: %w", err)
	}

	type targetCapacityXML struct {
		TotalTargetCapacity       int    `xml:"totalTargetCapacity"`
		OnDemandTargetCapacity    int    `xml:"onDemandTargetCapacity"`
		SpotTargetCapacity        int    `xml:"spotTargetCapacity"`
		DefaultTargetCapacityType string `xml:"defaultTargetCapacityType,omitempty"`
	}
	type tagItem struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type fleetItem struct {
		FleetID                     string             `xml:"fleetId"`
		FleetState                  string             `xml:"fleetState"`
		ActivityStatus              string             `xml:"activityStatus,omitempty"`
		Type                        string             `xml:"type"`
		CreateTime                  string             `xml:"createTime"`
		FulfilledCapacity           int                `xml:"fulfilledCapacity"`
		FulfilledOnDemandCapacity   int                `xml:"fulfilledOnDemandCapacity"`
		TargetCapacitySpecification targetCapacityXML  `xml:"targetCapacitySpecification"`
		ClientToken                 string             `xml:"clientToken,omitempty"`
		Tags                        []tagItem          `xml:"tagSet>item,omitempty"`
		Errors                      []fleetDescribeErr `xml:"errorSet>item,omitempty"`
	}
	type response struct {
		XMLName xml.Name    `xml:"DescribeFleetsResponse"`
		XMLNS   string      `xml:"xmlns,attr"`
		Fleets  []fleetItem `xml:"fleetSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var fleet EC2Fleet
		if json.Unmarshal(data, &fleet) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, fleet.FleetID) {
			continue
		}
		// An instant fleet is only returned when named explicitly.
		if len(ids) == 0 && fleet.Type == "instant" {
			continue
		}
		if vals, ok := filters["fleet-state"]; ok && !containsStr(vals, fleet.FleetState) {
			continue
		}
		if vals, ok := filters["type"]; ok && !containsStr(vals, fleet.Type) {
			continue
		}
		if vals, ok := filters["activity-status"]; ok && !containsStr(vals, fleet.ActivityStatus) {
			continue
		}

		item := fleetItem{
			FleetID:           fleet.FleetID,
			FleetState:        fleet.FleetState,
			ActivityStatus:    fleet.ActivityStatus,
			Type:              fleet.Type,
			CreateTime:        fleet.CreateTime,
			FulfilledCapacity: fleet.FulfilledCapacity,
			ClientToken:       fleet.ClientToken,
			TargetCapacitySpecification: targetCapacityXML{
				TotalTargetCapacity:       fleet.TotalTargetCapacity,
				OnDemandTargetCapacity:    fleet.OnDemandTargetCapacity,
				SpotTargetCapacity:        fleet.SpotTargetCapacity,
				DefaultTargetCapacityType: fleet.DefaultTargetCapacityType,
			},
		}
		if fleet.DefaultTargetCapacityType != "spot" {
			item.FulfilledOnDemandCapacity = fleet.FulfilledCapacity
		}
		for _, t := range fleet.Tags {
			item.Tags = append(item.Tags, tagItem{Key: t.Key, Value: t.Value}) //nolint:staticcheck // XML tags differ from EC2Tag's JSON tags.
		}
		for _, e := range fleet.Errors {
			item.Errors = append(item.Errors, fleetDescribeErr{
				LaunchTemplateAndOverrides: fleetLTAndOverrides(e.LaunchTemplateID, e.LaunchTemplateName, "", e.Override),
				Lifecycle:                  e.Lifecycle,
				ErrorCode:                  e.ErrorCode,
				ErrorMessage:               e.ErrorMessage,
			})
		}
		resp.Fleets = append(resp.Fleets, item)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// fleetDescribeErr is the errorSet item rendered by DescribeFleets.
type fleetDescribeErr struct {
	LaunchTemplateAndOverrides fleetLTAndOverridesXML `xml:"launchTemplateAndOverrides"`
	Lifecycle                  string                 `xml:"lifecycle,omitempty"`
	ErrorCode                  string                 `xml:"errorCode"`
	ErrorMessage               string                 `xml:"errorMessage"`
}

// deleteFleets handles DeleteFleets. TerminateInstances is required by AWS; when
// set (and always for instant fleets, which cannot outlive their instances) the
// fleet's instances are terminated through the normal TerminateInstances path.
func (p *EC2Plugin) deleteFleets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids := extractIndexedParams(req.Params, "FleetId")
	if len(ids) == 0 {
		return nil, &AWSError{Code: "MissingParameter", Message: "FleetId is required", HTTPStatus: http.StatusBadRequest}
	}
	terminate := req.Params["TerminateInstances"] == "true"

	type successItem struct {
		CurrentFleetState  string `xml:"currentFleetState"`
		PreviousFleetState string `xml:"previousFleetState"`
		FleetID            string `xml:"fleetId"`
	}
	type errorItem struct {
		FleetID string `xml:"fleetId"`
		Error   struct {
			Code    string `xml:"code"`
			Message string `xml:"message"`
		} `xml:"error"`
	}
	type response struct {
		XMLName      xml.Name      `xml:"DeleteFleetsResponse"`
		XMLNS        string        `xml:"xmlns,attr"`
		Successful   []successItem `xml:"successfulFleetDeletionSet>item,omitempty"`
		Unsuccessful []errorItem   `xml:"unsuccessfulFleetDeletionSet>item,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	for _, id := range ids {
		key := ec2FleetStateKey(reqCtx.AccountID, reqCtx.Region, id)
		data, err := p.state.Get(goCtx, ec2Namespace, key)
		if err != nil || data == nil {
			item := errorItem{FleetID: id}
			item.Error.Code = "fleetIdDoesNotExist"
			item.Error.Message = "The fleet ID " + id + " does not exist"
			resp.Unsuccessful = append(resp.Unsuccessful, item)
			continue
		}
		var fleet EC2Fleet
		if err := json.Unmarshal(data, &fleet); err != nil {
			return nil, fmt.Errorf("ec2 deleteFleets unmarshal: %w", err)
		}

		previous := fleet.FleetState
		// An instant fleet always terminates its instances on delete.
		if terminate || fleet.Type == "instant" {
			var all []string
			for _, g := range fleet.Instances {
				all = append(all, g.InstanceIDs...)
			}
			if len(all) > 0 {
				params := map[string]string{"Action": "TerminateInstances"}
				for i, iid := range all {
					params[fmt.Sprintf("InstanceId.%d", i+1)] = iid
				}
				if _, err := p.terminateInstances(reqCtx, &AWSRequest{
					Service:   "ec2",
					Operation: "TerminateInstances",
					Params:    params,
					Headers:   map[string]string{},
				}); err != nil {
					return nil, err
				}
			}
			fleet.FleetState = "deleted_terminating"
		} else {
			fleet.FleetState = "deleted_running"
		}

		updated, err := json.Marshal(fleet)
		if err != nil {
			return nil, fmt.Errorf("ec2 deleteFleets marshal: %w", err)
		}
		if err := p.state.Put(goCtx, ec2Namespace, key, updated); err != nil {
			return nil, fmt.Errorf("ec2 deleteFleets state.Put: %w", err)
		}
		resp.Successful = append(resp.Successful, successItem{
			CurrentFleetState:  fleet.FleetState,
			PreviousFleetState: previous,
			FleetID:            id,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}
