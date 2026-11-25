package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Burstable credit specifications (#1614).

// ec2LaunchType launches one instance of instanceType with extra params and returns its ID.
func ec2LaunchType(t *testing.T, ts *httptest.Server, instanceType string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"InstanceType": instanceType,
	}
	for k, v := range extra {
		params[k] = v
	}
	ids := ec2RunInstanceIDs(t, ts, params)
	require.Len(t, ids, 1)
	return ids[0]
}

// ec2CreditSpecs sends DescribeInstanceCreditSpecifications and returns instance ID → option.
func ec2CreditSpecs(t *testing.T, ts *httptest.Server, params map[string]string) (map[string]string, string) {
	t.Helper()
	params["Action"] = "DescribeInstanceCreditSpecifications"
	var doc struct {
		Specs []struct {
			ID      string `xml:"instanceId"`
			Credits string `xml:"cpuCredits"`
		} `xml:"instanceCreditSpecificationSet>item"`
		Next string `xml:"nextToken"`
	}
	ec2FleetXML(t, ts, params, &doc)
	out := map[string]string{}
	for _, s := range doc.Specs {
		out[s.ID] = s.Credits
	}
	return out, doc.Next
}

func TestEC2_CreditSpecs_LaunchDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var lt struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateLaunchTemplate", "LaunchTemplateName": "burst",
		"LaunchTemplateData.ImageId":                        ec2TestImage,
		"LaunchTemplateData.CreditSpecification.CpuCredits": "standard",
	}, &lt)
	require.NotEmpty(t, lt.ID)

	t3 := ec2LaunchType(t, ts, "t3.micro", nil)
	t2 := ec2LaunchType(t, ts, "t2.micro", nil)
	t4g := ec2LaunchType(t, ts, "t4g.small", map[string]string{"ImageId": ec2TestImageArm})
	m5 := ec2LaunchType(t, ts, "m5.large", map[string]string{"CreditSpecification.CpuCredits": "unlimited"})
	stated := ec2LaunchType(t, ts, "t3.small", map[string]string{"CreditSpecification.CpuCredits": "standard"})
	fromTemplate := ec2LaunchType(t, ts, "t3a.micro", map[string]string{"LaunchTemplate.LaunchTemplateId": lt.ID})
	overTemplate := ec2LaunchType(t, ts, "t3a.micro", map[string]string{
		"LaunchTemplate.LaunchTemplateId": lt.ID, "CreditSpecification.CpuCredits": "unlimited",
	})

	got, _ := ec2CreditSpecs(t, ts, map[string]string{
		"InstanceId.1": t3, "InstanceId.2": t2, "InstanceId.3": t4g, "InstanceId.4": m5,
		"InstanceId.5": stated, "InstanceId.6": fromTemplate, "InstanceId.7": overTemplate,
	})
	assert.Equal(t, map[string]string{
		t3: "unlimited", t2: "standard", t4g: "unlimited",
		m5:     "standard", // a non-burstable type ignores the option and reads standard
		stated: "standard", fromTemplate: "standard", overTemplate: "unlimited",
	}, got)

	// With no IDs, only instances whose option is unlimited are listed.
	unfiltered, next := ec2CreditSpecs(t, ts, map[string]string{})
	assert.Empty(t, next)
	assert.Equal(t, map[string]string{t3: "unlimited", t4g: "unlimited", overTemplate: "unlimited"}, unfiltered)
	only, _ := ec2CreditSpecs(t, ts, ec2Filter("instance-id", t4g))
	assert.Equal(t, map[string]string{t4g: "unlimited"}, only)

	// The template renders the option it stores.
	var versions struct {
		Credits string `xml:"launchTemplateVersionSet>item>launchTemplateData>creditSpecification>cpuCredits"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": lt.ID,
	}, &versions)
	assert.Equal(t, "standard", versions.Credits)
}

func TestEC2_CreditSpecs_Modify(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	t3 := ec2LaunchType(t, ts, "t3.micro", nil)
	m5 := ec2LaunchType(t, ts, "m5.large", nil)
	gone := ec2LaunchType(t, ts, "t3.micro", nil)
	ec2FleetXML(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": gone}, nil)

	var doc struct {
		Successful []struct {
			ID string `xml:"instanceId"`
		} `xml:"successfulInstanceCreditSpecificationSet>item"`
		Unsuccessful []struct {
			ID   string `xml:"instanceId"`
			Code string `xml:"error>code"`
		} `xml:"unsuccessfulInstanceCreditSpecificationSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "ModifyInstanceCreditSpecification",
		"InstanceCreditSpecification.1.InstanceId": t3,
		"InstanceCreditSpecification.1.CpuCredits": "standard",
		"InstanceCreditSpecification.2.InstanceId": m5,
		"InstanceCreditSpecification.2.CpuCredits": "unlimited",
		"InstanceCreditSpecification.3.InstanceId": "i-00000000000000000",
		"InstanceCreditSpecification.3.CpuCredits": "unlimited",
		"InstanceCreditSpecification.4.InstanceId": "nope",
		"InstanceCreditSpecification.4.CpuCredits": "unlimited",
		"InstanceCreditSpecification.5.InstanceId": gone,
		"InstanceCreditSpecification.5.CpuCredits": "standard",
	}, &doc)
	require.Len(t, doc.Successful, 1)
	assert.Equal(t, t3, doc.Successful[0].ID)
	codes := map[string]string{}
	for _, u := range doc.Unsuccessful {
		codes[u.ID] = u.Code
	}
	assert.Equal(t, map[string]string{
		m5:                    "InstanceCreditSpecification.NotSupported",
		"i-00000000000000000": "InvalidInstanceID.NotFound",
		"nope":                "InvalidInstanceID.Malformed",
		gone:                  "IncorrectInstanceState",
	}, codes)

	got, _ := ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": t3})
	assert.Equal(t, "standard", got[t3])

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"an invalid option", map[string]string{
			"InstanceCreditSpecification.1.InstanceId": t3, "InstanceCreditSpecification.1.CpuCredits": "burst",
		}, "InvalidCpuCredits"},
		{"a duplicate instance", map[string]string{
			"InstanceCreditSpecification.1.InstanceId": t3, "InstanceCreditSpecification.1.CpuCredits": "standard",
			"InstanceCreditSpecification.2.InstanceId": t3, "InstanceCreditSpecification.2.CpuCredits": "unlimited",
		}, "InvalidInstanceCreditSpecification.DuplicateInstanceId"},
		{"nothing to modify", map[string]string{}, "MissingParameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "ModifyInstanceCreditSpecification"
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestEC2_CreditSpecs_DescribeRefusalsAndPaging(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var ids []string
	for range 7 {
		ids = append(ids, ec2LaunchType(t, ts, "t3.nano", nil))
	}
	seen := map[string]bool{}
	token := ""
	for {
		params := map[string]string{"MaxResults": "5"}
		if token != "" {
			params["NextToken"] = token
		}
		page, next := ec2CreditSpecs(t, ts, params)
		assert.LessOrEqual(t, len(page), 5)
		for id := range page {
			seen[id] = true
		}
		if next == "" {
			break
		}
		token = next
	}
	assert.Len(t, seen, len(ids))

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"an unknown instance", map[string]string{"InstanceId.1": "i-00000000000000000"}, "InvalidInstanceID.NotFound"},
		{"IDs with MaxResults", map[string]string{"InstanceId.1": ids[0], "MaxResults": "5"}, "InvalidParameterCombination"},
		{"MaxResults out of range", map[string]string{"MaxResults": strconv.Itoa(1001)}, "InvalidParameterValue"},
		{"an undocumented filter", ec2Filter("instance-type", "t3.nano"), "InvalidParameterValue"},
		{"RunInstances with an invalid option", map[string]string{
			"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
			"CreditSpecification.CpuCredits": "turbo",
		}, "InvalidCpuCredits"},
		{"a template with an invalid option", map[string]string{
			"Action": "CreateLaunchTemplate", "LaunchTemplateName": "bad",
			"LaunchTemplateData.CreditSpecification.CpuCredits": "turbo",
		}, "InvalidCpuCredits"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.params["Action"] == "" {
				tc.params["Action"] = "DescribeInstanceCreditSpecifications"
			}
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestEC2_CreditSpecs_AccountDefault(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	before := ec2LaunchType(t, ts, "t3.micro", nil)

	type familySpec struct {
		Family  string `xml:"instanceFamilyCreditSpecification>instanceFamily"`
		Credits string `xml:"instanceFamilyCreditSpecification>cpuCredits"`
	}
	var got familySpec
	ec2FleetXML(t, ts, map[string]string{"Action": "GetDefaultCreditSpecification", "InstanceFamily": "t3"}, &got)
	assert.Equal(t, familySpec{"t3", "unlimited"}, got)

	var set familySpec
	ec2FleetXML(t, ts, map[string]string{
		"Action": "ModifyDefaultCreditSpecification", "InstanceFamily": "t3", "CpuCredits": "standard",
	}, &set)
	assert.Equal(t, familySpec{"t3", "standard"}, set)
	var after familySpec
	ec2FleetXML(t, ts, map[string]string{"Action": "GetDefaultCreditSpecification", "InstanceFamily": "t3"}, &after)
	assert.Equal(t, "standard", after.Credits)
	var t2 familySpec
	ec2FleetXML(t, ts, map[string]string{"Action": "GetDefaultCreditSpecification", "InstanceFamily": "t2"}, &t2)
	assert.Equal(t, "standard", t2.Credits)

	launched := ec2LaunchType(t, ts, "t3.large", nil)
	specs, _ := ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": before, "InstanceId.2": launched})
	assert.Equal(t, map[string]string{before: "unlimited", launched: "standard"}, specs,
		"a new default applies to new launches and leaves existing instances as they were")

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"a non-burstable family", map[string]string{"Action": "GetDefaultCreditSpecification", "InstanceFamily": "m5"}, "InvalidInstanceFamily"},
		{"no family", map[string]string{"Action": "GetDefaultCreditSpecification"}, "MissingParameter"},
		{"no option", map[string]string{"Action": "ModifyDefaultCreditSpecification", "InstanceFamily": "t3"}, "MissingParameter"},
		{"an invalid option", map[string]string{
			"Action": "ModifyDefaultCreditSpecification", "InstanceFamily": "t3", "CpuCredits": "x",
		}, "InvalidCpuCredits"},
		{"modify a non-burstable family", map[string]string{
			"Action": "ModifyDefaultCreditSpecification", "InstanceFamily": "c5", "CpuCredits": "standard",
		}, "InvalidInstanceFamily"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

// TestEC2_CreditSpecs_TerraformInstanceRead mirrors aws_instance's read: DescribeInstanceTypes
// for the type, and DescribeInstanceCreditSpecifications InstanceId.1 only when the type reports
// burstablePerformanceSupported.
func TestEC2_CreditSpecs_TerraformInstanceRead(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	burstable := func(instanceType string) bool {
		var doc struct {
			Burstable bool `xml:"instanceTypeSet>item>burstablePerformanceSupported"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstanceTypes", "InstanceType.1": instanceType}, &doc)
		return doc.Burstable
	}
	assert.True(t, burstable("t3.micro"))
	assert.True(t, burstable("t4g.micro"))
	assert.False(t, burstable("m5.large"))

	id := ec2LaunchType(t, ts, "t3.micro", nil)
	specs, _ := ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": id})
	assert.Equal(t, map[string]string{id: "unlimited"}, specs)

	var filtered struct {
		Types []string `xml:"instanceTypeSet>item>instanceType"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribeInstanceTypes", "Filter.1.Name": "burstable-performance-supported", "Filter.1.Value.1": "true",
	}, &filtered)
	require.NotEmpty(t, filtered.Types)
	for _, typ := range filtered.Types {
		assert.Regexp(t, `^t\d`, typ)
	}
}
