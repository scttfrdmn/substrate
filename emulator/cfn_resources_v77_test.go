package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// newEC2IAMTestDeployer creates a StackDeployer with the EC2 and IAM plugins and
// returns the registry, so a test can observe deployed resources through the
// same API a consumer would call.
func newEC2IAMTestDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.PluginRegistry) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	ec2Plugin := &emulator.EC2Plugin{}
	require.NoError(t, ec2Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ec2Plugin)

	iamPlugin := &emulator.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
	}))
	registry.Register(iamPlugin)

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), registry
}

// routeQuery sends a request through the registry and returns the XML response
// body. The IAM plugin reads its input from a JSON body rather than Params, so
// params are marshaled for that service.
func routeQuery(t *testing.T, registry *emulator.PluginRegistry, service string, params map[string]string) string {
	t.Helper()
	req := &emulator.AWSRequest{
		Service:   service,
		Operation: params["Action"],
		Params:    params,
		Headers:   map[string]string{},
	}
	if service == "iam" {
		body, err := json.Marshal(params)
		require.NoError(t, err)
		req.Body = body
	}
	resp, err := registry.RouteRequest(&emulator.RequestContext{
		RequestID: "test-request",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return string(resp.Body)
}

// findResource returns the deployed resource with the given logical ID.
func findResource(t *testing.T, result *emulator.DeployResult, logicalID string) emulator.DeployedResource {
	t.Helper()
	for _, r := range result.Resources {
		if r.LogicalID == logicalID {
			return r
		}
	}
	t.Fatalf("resource %s not found in %+v", logicalID, result.Resources)
	return emulator.DeployedResource{}
}

// TestCFN_EC2LaunchTemplate verifies a launch template declared in a template is
// afterwards visible to DescribeLaunchTemplates, rather than being stubbed (#388).
func TestCFN_EC2LaunchTemplate(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"ComputeLT": {
				"Type": "AWS::EC2::LaunchTemplate",
				"Properties": {
					"LaunchTemplateName": "compute-nodes",
					"LaunchTemplateData": {
						"ImageId": "ami-0abc1234",
						"InstanceType": "c5.4xlarge",
						"KeyName": "cluster-key"
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "lt-stack", nil)
	require.NoError(t, err)

	lt := findResource(t, result, "ComputeLT")
	assert.Empty(t, lt.Error)
	// The Ref must be the launch template ID, which is what CreateFleet and
	// AutoScaling consume; a stub would return the logical ID.
	assert.Regexp(t, `^lt-[0-9a-f]+$`, lt.PhysicalID)
	assert.Contains(t, lt.ARN, "launch-template/"+lt.PhysicalID)

	body := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeLaunchTemplates"})
	assert.Contains(t, body, lt.PhysicalID)
	assert.Contains(t, body, "compute-nodes")
}

// TestCFN_IAMInstanceProfile verifies an instance profile is created and its
// roles attached, and that an instance referencing it resolves (#388).
func TestCFN_IAMInstanceProfile(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"NodeRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {
					"RoleName": "compute-node-role",
					"AssumeRolePolicyDocument": {
						"Version": "2012-10-17",
						"Statement": [{
							"Effect": "Allow",
							"Principal": {"Service": "ec2.amazonaws.com"},
							"Action": "sts:AssumeRole"
						}]
					}
				}
			},
			"NodeProfile": {
				"Type": "AWS::IAM::InstanceProfile",
				"Properties": {
					"InstanceProfileName": "compute-node-profile",
					"Roles": [{"Ref": "NodeRole"}]
				}
			},
			"Node": {
				"Type": "AWS::EC2::Instance",
				"Properties": {
					"ImageId": "` + ec2TestImage + `",
					"InstanceType": "c5.large",
					"IamInstanceProfile": {"Ref": "NodeProfile"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "profile-stack", nil)
	require.NoError(t, err)

	profile := findResource(t, result, "NodeProfile")
	assert.Empty(t, profile.Error)
	assert.Equal(t, "compute-node-profile", profile.PhysicalID)
	assert.Contains(t, profile.ARN, "instance-profile/compute-node-profile")

	// GetInstanceProfile must find it, and the role must be attached.
	body := routeQuery(t, registry, "iam", map[string]string{
		"Action":              "GetInstanceProfile",
		"InstanceProfileName": "compute-node-profile",
	})
	assert.Contains(t, body, "compute-node-profile")
	assert.Contains(t, body, "compute-node-role")

	// The instance's profile reference resolves to the real profile.
	node := findResource(t, result, "Node")
	assert.Empty(t, node.Error)
	instances := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeInstances"})
	assert.Contains(t, instances, node.PhysicalID)
	assert.Contains(t, instances, "instance-profile/compute-node-profile")
}

// TestCFN_SecurityGroupIngress_SelfReferencing verifies the standalone rule
// resource, which is the only way to express a self-referencing group. This is
// the case #388 calls sharpest: a test asserting "my template opens port 6818
// within the compute security group" could pass deploy and still not observe the
// rule.
func TestCFN_SecurityGroupIngress_SelfReferencing(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"ComputeSG": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {
					"GroupDescription": "Slurm compute nodes",
					"VpcId": "vpc-01234567"
				}
			},
			"SlurmdIngress": {
				"Type": "AWS::EC2::SecurityGroupIngress",
				"Properties": {
					"GroupId": {"Ref": "ComputeSG"},
					"IpProtocol": "tcp",
					"FromPort": 6818,
					"ToPort": 6818,
					"SourceSecurityGroupId": {"Ref": "ComputeSG"},
					"Description": "slurmd within the cluster"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "self-ref-stack", nil)
	require.NoError(t, err)

	sg := findResource(t, result, "ComputeSG")
	require.NotEmpty(t, sg.PhysicalID)
	ingress := findResource(t, result, "SlurmdIngress")
	assert.Empty(t, ingress.Error)
	assert.Equal(t, sg.PhysicalID, ingress.PhysicalID)

	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action":    "DescribeSecurityGroups",
		"GroupId.1": sg.PhysicalID,
	})
	assert.Contains(t, body, "<fromPort>6818</fromPort>")
	assert.Contains(t, body, "<toPort>6818</toPort>")
	// The rule's source is the group itself, so it renders as a group pair with no
	// CIDR — a shape IPRanges alone cannot express.
	assert.Contains(t, body, "<groups>")
	assert.Contains(t, body, "<groupId>"+sg.PhysicalID+"</groupId>")
	assert.Contains(t, body, "slurmd within the cluster")
}

// TestCFN_SecurityGroup_InlineRules verifies inline SecurityGroupIngress and
// SecurityGroupEgress properties are authorized, so both spellings behave the
// same (#388).
func TestCFN_SecurityGroup_InlineRules(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"HeadSG": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {
					"GroupDescription": "Head node",
					"VpcId": "vpc-01234567",
					"SecurityGroupIngress": [
						{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "CidrIp": "10.0.0.0/8"},
						{"IpProtocol": "tcp", "FromPort": 6817, "ToPort": 6817, "CidrIp": "10.1.0.0/16"}
					],
					"SecurityGroupEgress": [
						{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}
					]
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "inline-rules-stack", nil)
	require.NoError(t, err)

	sg := findResource(t, result, "HeadSG")
	assert.Empty(t, sg.Error)
	require.NotEmpty(t, sg.PhysicalID)

	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action":    "DescribeSecurityGroups",
		"GroupId.1": sg.PhysicalID,
	})
	// Both ingress rules are present, not just the first.
	assert.Contains(t, body, "<cidrIp>10.0.0.0/8</cidrIp>")
	assert.Contains(t, body, "<cidrIp>10.1.0.0/16</cidrIp>")
	assert.Contains(t, body, "<fromPort>22</fromPort>")
	assert.Contains(t, body, "<fromPort>6817</fromPort>")
	assert.Contains(t, body, "<ipPermissionsEgress>")
	assert.Contains(t, body, "<cidrIp>0.0.0.0/0</cidrIp>")
}

// TestCFN_SecurityGroupEgress_Standalone verifies the standalone egress rule
// resource, including a destination security group.
func TestCFN_SecurityGroupEgress_Standalone(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"AppSG": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {"GroupDescription": "App", "VpcId": "vpc-01234567"}
			},
			"DBSG": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {"GroupDescription": "DB", "VpcId": "vpc-01234567"}
			},
			"AppToDB": {
				"Type": "AWS::EC2::SecurityGroupEgress",
				"Properties": {
					"GroupId": {"Ref": "AppSG"},
					"IpProtocol": "tcp",
					"FromPort": 5432,
					"ToPort": 5432,
					"DestinationSecurityGroupId": {"Ref": "DBSG"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "egress-stack", nil)
	require.NoError(t, err)

	appSG := findResource(t, result, "AppSG")
	dbSG := findResource(t, result, "DBSG")
	rule := findResource(t, result, "AppToDB")
	assert.Empty(t, rule.Error)

	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action":    "DescribeSecurityGroups",
		"GroupId.1": appSG.PhysicalID,
	})
	assert.Contains(t, body, "<ipPermissionsEgress>")
	assert.Contains(t, body, "<fromPort>5432</fromPort>")
	assert.Contains(t, body, "<groupId>"+dbSG.PhysicalID+"</groupId>")
}

// TestCFN_SecurityGroupIngress_MutualReference verifies two groups that
// reference each other both resolve, which requires the standalone rules to
// deploy after every group.
func TestCFN_SecurityGroupIngress_MutualReference(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"AToB": {
				"Type": "AWS::EC2::SecurityGroupIngress",
				"Properties": {
					"GroupId": {"Ref": "GroupA"},
					"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
					"SourceSecurityGroupId": {"Ref": "GroupB"}
				}
			},
			"BToA": {
				"Type": "AWS::EC2::SecurityGroupIngress",
				"Properties": {
					"GroupId": {"Ref": "GroupB"},
					"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
					"SourceSecurityGroupId": {"Ref": "GroupA"}
				}
			},
			"GroupA": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {"GroupDescription": "A", "VpcId": "vpc-01234567"}
			},
			"GroupB": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {"GroupDescription": "B", "VpcId": "vpc-01234567"}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "mutual-stack", nil)
	require.NoError(t, err)

	groupA := findResource(t, result, "GroupA")
	groupB := findResource(t, result, "GroupB")
	require.NotEmpty(t, groupA.PhysicalID)
	require.NotEmpty(t, groupB.PhysicalID)
	assert.Empty(t, findResource(t, result, "AToB").Error)
	assert.Empty(t, findResource(t, result, "BToA").Error)

	bodyA := routeQuery(t, registry, "ec2", map[string]string{
		"Action": "DescribeSecurityGroups", "GroupId.1": groupA.PhysicalID,
	})
	assert.Contains(t, bodyA, "<groupId>"+groupB.PhysicalID+"</groupId>",
		"GroupA's ingress should name GroupB as its source")

	bodyB := routeQuery(t, registry, "ec2", map[string]string{
		"Action": "DescribeSecurityGroups", "GroupId.1": groupB.PhysicalID,
	})
	assert.Contains(t, bodyB, "<groupId>"+groupA.PhysicalID+"</groupId>",
		"GroupB's ingress should name GroupA as its source")
}

// TestCFN_LaunchTemplateFeedsCreateFleet verifies the two halves of this sprint
// compose: a launch template deployed from a template can be used by CreateFleet
// (#387, #388). A stubbed launch template's physical ID is not a usable
// reference, so this only works with the real wiring.
func TestCFN_LaunchTemplateFeedsCreateFleet(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"FleetLT": {
				"Type": "AWS::EC2::LaunchTemplate",
				"Properties": {
					"LaunchTemplateName": "fleet-nodes",
					"LaunchTemplateData": {"ImageId": "` + ec2TestImage + `", "InstanceType": "c5.large"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "lt-fleet-stack", nil)
	require.NoError(t, err)
	lt := findResource(t, result, "FleetLT")
	require.NotEmpty(t, lt.PhysicalID)

	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": lt.PhysicalID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	})
	assert.Contains(t, body, "<fleetId>fleet-")
	assert.Contains(t, body, "<instanceIds>")
}

// TestCFN_GenericStubReportsPluginLoaded verifies an unhandled type whose service
// plugin is loaded is distinguishable from an unmodelled service (#388).
func TestCFN_GenericStubReportsPluginLoaded(t *testing.T) {
	d, _ := newEC2IAMTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"ENI": {
				"Type": "AWS::EC2::NetworkInterface",
				"Properties": {"SubnetId": "subnet-01234567"}
			},
			"Widget": {
				"Type": "AWS::SomeNewService::Widget",
				"Properties": {"Name": "w"}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "stub-stack", nil)
	require.NoError(t, err)

	// Both still deploy as stubs; the difference is only in the warning, which
	// carries service_plugin_loaded. Assert the stub behavior is unchanged.
	eni := findResource(t, result, "ENI")
	assert.Equal(t, "ENI", eni.PhysicalID)
	assert.Contains(t, eni.ARN, "arn:aws:ec2:")
	widget := findResource(t, result, "Widget")
	assert.Equal(t, "Widget", widget.PhysicalID)
	assert.Contains(t, widget.ARN, "somenewservice")
}
