package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// Burstable performance credit specifications (#1614).
//
// A T instance's credit option is standard or unlimited. It is a recorded value the API reports
// and changes, never a CPU-credit balance: substrate runs no workload, so it accrues and spends
// nothing.

// The two credit options, as CreditSpecificationRequest's CpuCredits publishes them: "Valid
// values: standard | unlimited".
const (
	ec2CPUCreditsStandard  = "standard"
	ec2CPUCreditsUnlimited = "unlimited"
)

// ec2BurstableFamilyDefaults is each burstable family's default credit option, from the EC2 User
// Guide's "Configure burstable performance instances": "T9g, T8i, T4g, T3a, and T3 instances
// launch as unlimited" and "T2 instances launch as standard". The families are the
// UnlimitedSupportedInstanceFamily enumeration GetDefaultCreditSpecification takes.
var ec2BurstableFamilyDefaults = map[string]string{ //nolint:gochecknoglobals // immutable table
	"t2":  ec2CPUCreditsStandard,
	"t3":  ec2CPUCreditsUnlimited,
	"t3a": ec2CPUCreditsUnlimited,
	"t4g": ec2CPUCreditsUnlimited,
	"t8i": ec2CPUCreditsUnlimited,
	"t9g": ec2CPUCreditsUnlimited,
}

// ec2InstanceFamily is the family half of an instance type, "t3" for "t3.micro".
func ec2InstanceFamily(instanceType string) string {
	family, _, _ := strings.Cut(instanceType, ".")
	return family
}

// ec2BurstableFamily reports whether an instance type is a burstable performance type.
func ec2BurstableFamily(instanceType string) bool {
	_, ok := ec2BurstableFamilyDefaults[ec2InstanceFamily(instanceType)]
	return ok
}

// ec2InstanceCPUCredits is the credit option DescribeInstanceCreditSpecifications reports for an
// instance: standard when its current type is not burstable, else the option it carries, else its
// family's default.
//
// The first is the operation's own rule: "If you specify an instance ID that is not a burstable
// performance instance, Amazon EC2 returns the standard credit option." It holds for an instance
// resized off a burstable type too, whose carried option is kept for a resize back. An instance
// from an event log predating [EC2Instance.CPUCredits] carries none and reads as its family's
// default.
func ec2InstanceCPUCredits(inst EC2Instance) string {
	def, burstable := ec2BurstableFamilyDefaults[ec2InstanceFamily(inst.InstanceType)]
	switch {
	case !burstable:
		return ec2CPUCreditsStandard
	case inst.CPUCredits != "":
		return inst.CPUCredits
	default:
		return def
	}
}

// ec2ResizeCPUCredits updates an instance's credit option for a change of type from oldType
// (#1614). An instance moving onto a burstable type from a type that is not one takes the
// account's default for the new family, as a launch would, since it has no option of its own; any
// other change keeps the option it carries, which [ec2InstanceCPUCredits] reports only while the
// type is burstable.
func ec2ResizeCPUCredits(state StateManager, reqCtx *RequestContext, inst *EC2Instance, oldType string) error {
	if inst.InstanceType == oldType || !ec2BurstableFamily(inst.InstanceType) || ec2BurstableFamily(oldType) {
		return nil
	}
	credits, err := ec2DefaultCPUCredits(state, reqCtx, ec2InstanceFamily(inst.InstanceType))
	if err != nil {
		return err
	}
	inst.CPUCredits = credits
	return nil
}

// ec2InvalidCPUCreditsError is InvalidCpuCredits, with the EC2 client-error table's gloss, "The
// specified CpuCredit value is invalid. Valid values are standard and unlimited", as its
// message.
func ec2InvalidCPUCreditsError() *AWSError {
	return &AWSError{
		Code:       "InvalidCpuCredits",
		Message:    "The specified CpuCredit value is invalid. Valid values are standard and unlimited.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CheckCPUCredits refuses a stated credit option that is neither standard nor unlimited. An
// empty one states nothing.
func ec2CheckCPUCredits(value string) *AWSError {
	if value == "" || value == ec2CPUCreditsStandard || value == ec2CPUCreditsUnlimited {
		return nil
	}
	return ec2InvalidCPUCreditsError()
}

// ec2DefaultCreditStateKey is the state key of an account's default credit option for one
// family in one region.
func ec2DefaultCreditStateKey(accountID, region, family string) string {
	return "credit_default:" + accountID + "/" + region + "/" + family
}

// ec2DefaultCPUCredits is the account's default credit option for a family in the region: the
// one ModifyDefaultCreditSpecification set, else the family's published default.
func ec2DefaultCPUCredits(state StateManager, reqCtx *RequestContext, family string) (string, error) {
	data, err := state.Get(context.Background(), ec2Namespace,
		ec2DefaultCreditStateKey(reqCtx.AccountID, reqCtx.Region, family))
	if err != nil {
		return "", fmt.Errorf("ec2 default credit specification get: %w", err)
	}
	if len(data) > 0 {
		var stored string
		if json.Unmarshal(data, &stored) == nil && stored != "" {
			return stored, nil
		}
	}
	return ec2BurstableFamilyDefaults[family], nil
}

// ec2LaunchCPUCredits resolves the credit option an instance launches with: the request's
// CreditSpecification.CpuCredits, else the launch template's, else the account's default for the
// family ([ec2DefaultCPUCredits]). The User Guide: "all new T instances in the account launch
// using the default credit specification", which the launch's own value overrides.
//
// A non-burstable type launches with none, and a credit option stated for it is ignored rather
// than refused. No AWS source publishes a refusal, and the Terraform AWS provider drops the
// option client-side for such a type ("credit_specification is defined but instance type does
// not support burstable performance. Ignoring..."), so substrate records the request and reports
// the type standard, as DescribeInstanceCreditSpecifications does for any non-burstable instance.
// A value that is neither standard nor unlimited is still InvalidCpuCredits, whatever the type.
func ec2LaunchCPUCredits(state StateManager, reqCtx *RequestContext, instanceType, requested, template string) (string, error) {
	if awsErr := ec2CheckCPUCredits(requested); awsErr != nil {
		return "", awsErr
	}
	if !ec2BurstableFamily(instanceType) {
		return "", nil
	}
	if requested != "" {
		return requested, nil
	}
	if template != "" {
		return template, nil
	}
	return ec2DefaultCPUCredits(state, reqCtx, ec2InstanceFamily(instanceType))
}

// ec2CreditSpecItem is AWS's InstanceCreditSpecification.
type ec2CreditSpecItem struct {
	InstanceID string `xml:"instanceId"`
	CPUCredits string `xml:"cpuCredits"`
}

// ec2CreditSpecificationFilterSpec is DescribeInstanceCreditSpecifications' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstanceCreditSpecifications.html:
// instance-id alone, and no tag filter.
func ec2CreditSpecificationFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{evaluated: []string{"instance-id"}}
}

// describeInstanceCreditSpecifications reports instances' credit options (#1614).
//
// The page states two selections: "If you do not specify an instance ID, Amazon EC2 returns
// burstable performance instances with the unlimited credit option, as well as instances that
// were previously configured as T2, T3, and T3a with the unlimited credit option", and "If you
// specify one or more instance IDs, Amazon EC2 returns the credit option (standard or unlimited)
// of those instances. If you specify an instance ID that is not a burstable performance
// instance, Amazon EC2 returns the standard credit option." The first selection is every instance
// whose option is unlimited, as [ec2InstanceCPUCredits] reports it. An instance resized onto a
// non-burstable type reports standard, by the second sentence, so substrate leaves it out of the
// first selection where the page would list it; docs/reference/ec2.md says so. Terminated instances are left out of it, which the page's "Recently terminated
// instances might appear" permits; one named by ID is reported.
//
// InstanceId.N asserts that each ID exists ([ec2IDFilter]). It pages on the published range,
// "Minimum value of 5. Maximum value of 1000.", and "You cannot specify this parameter and the
// instance IDs parameter in the same call" ([ec2RefuseIDsWithMaxResults]).
func (p *EC2Plugin) describeInstanceCreditSpecifications(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instanceIDs := extractIndexedParams(req.Params, "InstanceId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "InstanceId", instanceIDs); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinPublishedMaxResults, ec2MaxPublishedMaxResults1000)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	ids := newEC2IDFilter(instanceIDs, ec2InstanceIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2CreditSpecificationFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	keys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInstanceCreditSpecifications list: %w", err)
	}
	type response struct {
		XMLName   xml.Name            `xml:"DescribeInstanceCreditSpecificationsResponse"`
		XMLNS     string              `xml:"xmlns,attr"`
		Specs     []ec2CreditSpecItem `xml:"instanceCreditSpecificationSet>item"`
		NextToken string              `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		if !ids.match(inst.InstanceID) {
			continue
		}
		credits := ec2InstanceCPUCredits(inst)
		if len(instanceIDs) == 0 && (credits != ec2CPUCreditsUnlimited || inst.State.Name == "terminated") {
			continue
		}
		if values, ok := filters["instance-id"]; ok && !ec2FilterAccepts(values, inst.InstanceID) {
			continue
		}
		resp.Specs = append(resp.Specs, ec2CreditSpecItem{InstanceID: inst.InstanceID, CPUCredits: credits})
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	resp.Specs, resp.NextToken = ec2Page(resp.Specs, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2CreditSpecError is AWS's UnsuccessfulInstanceCreditSpecificationItemError.
type ec2CreditSpecError struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// ec2UnsuccessfulCreditSpecItem is AWS's UnsuccessfulInstanceCreditSpecificationItem.
type ec2UnsuccessfulCreditSpecItem struct {
	InstanceID string             `xml:"instanceId"`
	Error      ec2CreditSpecError `xml:"error"`
}

// modifyInstanceCreditSpecification changes the credit option of each instance
// InstanceCreditSpecification.N names (#1614).
//
// Each instance succeeds or fails on its own, into successfulInstanceCreditSpecificationSet or
// unsuccessfulInstanceCreditSpecificationSet, with a code of the four
// UnsuccessfulInstanceCreditSpecificationErrorCode publishes: InvalidInstanceID.Malformed and
// InvalidInstanceID.NotFound for an ID naming no instance; InstanceCreditSpecification.NotSupported
// for a non-burstable instance, with the client-error table's gloss "The specified instance does
// not use CPU credits for CPU usage; only T2 instances use CPU credits for CPU usage."; and
// IncorrectInstanceState for an instance that is neither running nor stopped — the operation
// "Modifies the credit option for CPU usage on a running or stopped burstable performance
// instance" — whose message is substrate's own.
//
// Two refusals apply to the whole request: a CpuCredits value that is neither standard nor
// unlimited is InvalidCpuCredits, and an instance named twice is
// InvalidInstanceCreditSpecification.DuplicateInstanceId, whose table gloss is "the request may
// not contain duplicate instance IDs".
func (p *EC2Plugin) modifyInstanceCreditSpecification(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	type change struct{ id, credits string }
	var changes []change
	seen := map[string]bool{}
	for n := 1; ; n++ {
		prefix := fmt.Sprintf("InstanceCreditSpecification.%d.", n)
		id, ok := req.Params[prefix+"InstanceId"]
		if !ok {
			break
		}
		credits := req.Params[prefix+"CpuCredits"]
		if awsErr := ec2CheckCPUCredits(credits); awsErr != nil {
			return nil, awsErr
		}
		if seen[id] {
			return nil, &AWSError{
				Code:       "InvalidInstanceCreditSpecification.DuplicateInstanceId",
				Message:    "The request may not contain duplicate instance IDs.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		seen[id] = true
		changes = append(changes, change{id: id, credits: credits})
	}
	if len(changes) == 0 {
		return nil, ec2MissingParameter("InstanceCreditSpecification")
	}
	type response struct {
		XMLName      xml.Name                        `xml:"ModifyInstanceCreditSpecificationResponse"`
		XMLNS        string                          `xml:"xmlns,attr"`
		Successful   []ec2CreditSpecItem             `xml:"successfulInstanceCreditSpecificationSet>item"`
		Unsuccessful []ec2UnsuccessfulCreditSpecItem `xml:"unsuccessfulInstanceCreditSpecificationSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	fail := func(id, code, message string) {
		resp.Unsuccessful = append(resp.Unsuccessful, ec2UnsuccessfulCreditSpecItem{
			InstanceID: id, Error: ec2CreditSpecError{Code: code, Message: message},
		})
	}
	// The instance record is read and rewritten whole, so the write is serialized with CreateTags
	// and DeleteTags on the instance, which rewrite the same record under tagMu.
	p.tagMu.Lock()
	defer p.tagMu.Unlock()
	for _, c := range changes {
		inst, awsErr, err := p.loadInstance(reqCtx, c.id, "modifyInstanceCreditSpecification")
		if err != nil {
			return nil, err
		}
		switch {
		case awsErr != nil:
			fail(c.id, awsErr.Code, awsErr.Message)
			continue
		case !ec2BurstableFamily(inst.InstanceType):
			fail(c.id, "InstanceCreditSpecification.NotSupported",
				"The specified instance does not use CPU credits for CPU usage; only T2 instances use CPU credits for CPU usage.")
			continue
		case inst.State.Name != "running" && inst.State.Name != "stopped":
			fail(c.id, "IncorrectInstanceState",
				"The instance '"+c.id+"' is not in a state from which its credit option can be modified")
			continue
		}
		if c.credits != "" {
			inst.CPUCredits = c.credits
			data, err := json.Marshal(inst)
			if err != nil {
				return nil, fmt.Errorf("ec2 modifyInstanceCreditSpecification marshal: %w", err)
			}
			key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + c.id
			if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
				return nil, fmt.Errorf("ec2 modifyInstanceCreditSpecification state.Put: %w", err)
			}
		}
		resp.Successful = append(resp.Successful, ec2CreditSpecItem{InstanceID: c.id})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2FamilyCreditSpecItem is AWS's InstanceFamilyCreditSpecification.
type ec2FamilyCreditSpecItem struct {
	InstanceFamily string `xml:"instanceFamily"`
	CPUCredits     string `xml:"cpuCredits"`
}

// ec2CreditFamily reads InstanceFamily and refuses one that is absent or not a burstable family
// — the client-error table's InvalidInstanceFamily: "you can only modify the default credit
// specification for burstable performance instance families". The message is substrate's own.
func ec2CreditFamily(params map[string]string) (string, *AWSError) {
	family := params["InstanceFamily"]
	if family == "" {
		return "", ec2MissingParameter("InstanceFamily")
	}
	if _, ok := ec2BurstableFamilyDefaults[family]; !ok {
		return "", &AWSError{
			Code:       "InvalidInstanceFamily",
			Message:    "The instance family '" + family + "' is not a burstable performance instance family",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return family, nil
}

// getDefaultCreditSpecification reports the account's default credit option for a burstable
// family in the region (#1614); see [ec2DefaultCPUCredits].
func (p *EC2Plugin) getDefaultCreditSpecification(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	family, awsErr := ec2CreditFamily(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	credits, err := ec2DefaultCPUCredits(p.state, reqCtx, family)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name                `xml:"GetDefaultCreditSpecificationResponse"`
		XMLNS   string                  `xml:"xmlns,attr"`
		Spec    ec2FamilyCreditSpecItem `xml:"instanceFamilyCreditSpecification"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		Spec:  ec2FamilyCreditSpecItem{InstanceFamily: family, CPUCredits: credits},
	})
}

// modifyDefaultCreditSpecification sets the account's default credit option for a burstable
// family in the region, which every later launch of the family without a credit option of its
// own takes (#1614).
//
// The User Guide describes the change as asynchronous ("It takes about five minutes to
// complete", CreditSpecificationUpdateInProgress) and limits it to "once in a rolling 5-minute
// period, and up to four times in a rolling 24-hour period" (UpdateLimitExceeded). Substrate
// applies it at once and limits nothing; docs/reference/ec2.md says so.
func (p *EC2Plugin) modifyDefaultCreditSpecification(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	family, awsErr := ec2CreditFamily(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	credits := req.Params["CpuCredits"]
	if credits == "" {
		return nil, ec2MissingParameter("CpuCredits")
	}
	if awsErr := ec2CheckCPUCredits(credits); awsErr != nil {
		return nil, awsErr
	}
	data, err := json.Marshal(credits)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyDefaultCreditSpecification marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace,
		ec2DefaultCreditStateKey(reqCtx.AccountID, reqCtx.Region, family), data); err != nil {
		return nil, fmt.Errorf("ec2 modifyDefaultCreditSpecification state.Put: %w", err)
	}
	type response struct {
		XMLName xml.Name                `xml:"ModifyDefaultCreditSpecificationResponse"`
		XMLNS   string                  `xml:"xmlns,attr"`
		Spec    ec2FamilyCreditSpecItem `xml:"instanceFamilyCreditSpecification"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		Spec:  ec2FamilyCreditSpecItem{InstanceFamily: family, CPUCredits: credits},
	})
}
