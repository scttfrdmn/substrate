package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// eniFaultState faults one StateManager method for keys (or List prefixes) under a prefix.
type eniFaultState struct {
	emulator.StateManager
	mu     sync.Mutex
	method string
	prefix string
}

func (m *eniFaultState) arm(method, prefix string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.method, m.prefix = method, prefix
}

func (m *eniFaultState) faults(method, key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.method == method && m.prefix != "" && strings.HasPrefix(key, m.prefix)
}

var errENIFault = errors.New("eni test store fault")

func (m *eniFaultState) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if m.faults("Get", key) {
		return nil, errENIFault
	}
	return m.StateManager.Get(ctx, ns, key)
}

func (m *eniFaultState) List(ctx context.Context, ns, prefix string) ([]string, error) {
	if m.faults("List", prefix) {
		return nil, errENIFault
	}
	return m.StateManager.List(ctx, ns, prefix)
}

func (m *eniFaultState) Put(ctx context.Context, ns, key string, value []byte) error {
	if m.faults("Put", key) {
		return errENIFault
	}
	return m.StateManager.Put(ctx, ns, key, value)
}

func (m *eniFaultState) Delete(ctx context.Context, ns, key string) error {
	if m.faults("Delete", key) {
		return errENIFault
	}
	return m.StateManager.Delete(ctx, ns, key)
}

// eniFaultWorld is a subnet with an instance, an available interface and an older instance.
type eniFaultWorld struct {
	ts       *httptest.Server
	state    *eniFaultState
	vpc      string
	subnet   string
	group    string
	instance string
	eni      string
}

const eniFaultLegacyENI = "eni-00000000000000020"

func newENIFaultWorld(t *testing.T) eniFaultWorld {
	t.Helper()
	state := &eniFaultState{StateManager: emulator.NewMemoryStateManager()}
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.95.0.0/16")
	w := eniFaultWorld{ts: ts, state: state, vpc: vpc, subnet: ec2CreateSubnetIn(t, ts, vpc, "10.95.1.0/24")}
	w.group = ec2CreateSG(t, ts, "fault", "fault", vpc)
	w.instance = ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": w.subnet,
	})[0]
	var doc struct {
		ID string `xml:"networkInterface>networkInterfaceId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateNetworkInterface", "SubnetId": w.subnet}, &doc)
	w.eni = doc.ID
	inst := emulator.EC2Instance{
		InstanceID: "i-00000000000000020", ReservationID: "r-00000000000000001", ImageID: ec2TestImage,
		InstanceType: "t3.micro", SubnetID: w.subnet, VPCID: vpc, PrivateIPAddress: "10.95.1.50",
		State: emulator.EC2InstanceState{Code: 16, Name: "running"}, AccountID: "123456789012", Region: "us-east-1",
	}
	raw, err := json.Marshal(inst)
	require.NoError(t, err)
	require.NoError(t, state.StateManager.Put(t.Context(), "ec2", "instance:123456789012/us-east-1/"+inst.InstanceID, raw))
	return w
}

// TestEC2NetworkInterface_StoreFaultsAreInternalErrors pins that a record the interface and
// address paths cannot read or write fails the request with a server error rather than an answer
// decided on partial state.
func TestEC2NetworkInterface_StoreFaultsAreInternalErrors(t *testing.T) {
	t.Parallel()
	type call func(w eniFaultWorld) map[string]string
	cases := []struct {
		name, method, prefix string
		call                 call
	}{
		{"describe interfaces, interface list", "List", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DescribeNetworkInterfaces"}
		}},
		{"describe interfaces, marker list", "List", "eni_legacy:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DescribeNetworkInterfaces"}
		}},
		{"describe instances, interface list", "List", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DescribeInstances"}
		}},
		{"describe instance groups, interface list", "List", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DescribeInstanceAttribute", "InstanceId": w.instance, "Attribute": "groupSet"}
		}},
		{"describe subnets, address book", "Get", "ipalloc:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DescribeSubnets"}
		}},
		{"launch, address book", "Get", "ipalloc:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": w.subnet}
		}},
		{"launch, address book write", "Put", "ipalloc:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": w.subnet}
		}},
		{"launch, interface write", "Put", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": w.subnet}
		}},
		{"launch, existing interface read", "Get", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
				"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkInterfaceId": w.eni}
		}},
		{"create interface, write", "Put", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "CreateNetworkInterface", "SubnetId": w.subnet}
		}},
		{"create interface, address book write", "Put", "ipalloc:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "CreateNetworkInterface", "SubnetId": w.subnet}
		}},
		{"NAT gateway, address book", "Get", "ipalloc:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "CreateNatGateway", "SubnetId": w.subnet, "ConnectivityType": "private"}
		}},
		{"attach, interface list", "List", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": w.eni, "InstanceId": w.instance, "DeviceIndex": "1"}
		}},
		{"attach, interface write", "Put", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": w.eni, "InstanceId": w.instance, "DeviceIndex": "1"}
		}},
		{"attach, interface read", "Get", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": w.eni, "InstanceId": w.instance, "DeviceIndex": "1"}
		}},
		{"detach, interface list", "List", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-0123456789abcdef0"}
		}},
		{"detach, older instances", "List", "instance:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-0123456789abcdef0"}
		}},
		{"delete interface, read", "Get", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": w.eni}
		}},
		{"delete interface, delete", "Delete", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": w.eni}
		}},
		{"delete interface, address book write", "Put", "ipalloc:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": w.eni}
		}},
		{"delete interface, release subnet", "Get", "subnet:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": w.eni}
		}},
		{"modify, interface write", "Put", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": w.eni, "Description.Value": "x"}
		}},
		{"modify, older lookup", "List", "instance:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": "eni-0123456789abcdef0", "Description.Value": "x"}
		}},
		{"modify older, marker write", "Put", "eni_legacy:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniFaultLegacyENI, "Description.Value": "x"}
		}},
		{"modify older, marker list", "List", "eni_legacy:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniFaultLegacyENI, "Description.Value": "x"}
		}},
		{"tag older, interface write", "Put", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "CreateTags", "ResourceId.1": eniFaultLegacyENI, "Tag.1.Key": "k", "Tag.1.Value": "v"}
		}},
		{"terminate, interface list", "List", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": w.instance}
		}},
		{"terminate, interface delete", "Delete", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": w.instance}
		}},
		{"terminate, release reporters", "List", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000020"}
		}},
		{"terminate older, marker list", "List", "eni_legacy:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000020"}
		}},
		{"terminate older, interface write", "Put", "eni:", func(eniFaultWorld) map[string]string {
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000020"}
		}},
		{"delete subnet, interface scan", "List", "eni:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteSubnet", "SubnetId": w.subnet}
		}},
		{"delete group, marker list", "List", "eni_legacy:", func(w eniFaultWorld) map[string]string {
			return map[string]string{"Action": "DeleteSecurityGroup", "GroupId": w.group}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newENIFaultWorld(t)
			params := tc.call(w)
			w.state.arm(tc.method, tc.prefix)
			resp := ec2Request(t, w.ts, params)
			_ = resp.Body.Close()
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode, "%v", params)
		})
	}
}
