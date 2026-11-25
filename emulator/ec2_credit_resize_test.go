package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2_CreditSpecs_FollowTheInstanceType pins that a credit option follows a type change: an
// instance resized onto a non-burstable type reports standard and leaves the unfiltered list, and
// one resized onto a burstable type from a non-burstable one takes the account's default (#1614).
func TestEC2_CreditSpecs_FollowTheInstanceType(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	resize := func(id, instanceType string) {
		t.Helper()
		stopInstance(t, ts, id)
		require.Equal(t, http.StatusOK, modifyInstanceAttribute(t, ts, map[string]string{
			"Action": "ModifyInstanceAttribute", "InstanceId": id, "InstanceType.Value": instanceType,
		}))
	}

	burst := ec2LaunchType(t, ts, "t3.micro", nil)
	resize(burst, "m5.large")
	got, _ := ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": burst})
	assert.Equal(t, "standard", got[burst], "a non-burstable type reports standard")
	unfiltered, _ := ec2CreditSpecs(t, ts, map[string]string{})
	assert.NotContains(t, unfiltered, burst)
	resize(burst, "t3.large")
	got, _ = ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": burst})
	assert.Equal(t, "unlimited", got[burst], "a resize back keeps the option it carried")

	ec2FleetXML(t, ts, map[string]string{
		"Action": "ModifyDefaultCreditSpecification", "InstanceFamily": "t3", "CpuCredits": "standard",
	}, nil)
	plain := ec2LaunchType(t, ts, "m5.large", nil)
	resize(plain, "t3.micro")
	got, _ = ec2CreditSpecs(t, ts, map[string]string{"InstanceId.1": plain})
	assert.Equal(t, "standard", got[plain], "m5 to t3 takes the account's t3 default")
}
