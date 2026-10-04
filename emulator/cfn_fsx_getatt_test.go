package emulator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// AWS::FSx::FileSystem publishes four Fn::GetAtt attributes. LustreMountName resolved empty until
// #1199's audit, though the plugin always held it: the deployer recorded only DNSName. RootVolumeId
// still resolves empty, because OpenZFS volumes are not modeled.
func TestCFN_FSxGetAttLustreMountName(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"FS": {
				"Type": "AWS::FSx::FileSystem",
				"Properties": {
					"FileSystemType": "LUSTRE",
					"StorageCapacity": 1200,
					"SubnetIds": ["subnet-0123456789abcdef0"]
				}
			}
		},
		"Outputs": {
			"Mount":   {"Value": {"Fn::GetAtt": ["FS", "LustreMountName"]}},
			"DNSName": {"Value": {"Fn::GetAtt": ["FS", "DNSName"]}},
			"Arn":     {"Value": {"Fn::GetAtt": ["FS", "ResourceARN"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "fsx-getatt", nil)
	require.NoError(t, err)
	require.Empty(t, resourceByLogicalID(t, result, "FS").Error)

	// SCRATCH_1 is the published default deployment type, and its mount name is always "fsx".
	require.Equal(t, "fsx", result.Outputs["Mount"])
	require.Contains(t, result.Outputs["DNSName"], ".fsx.us-east-1.amazonaws.com")
	require.Contains(t, result.Outputs["Arn"], ":file-system/fs-")
}

// A template that omits SubnetIds, which AWS::FSx::FileSystem marks Required, fails the resource
// rather than creating a file system in no subnet (#1197).
func TestCFN_FSxWithoutSubnetIdsFails(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"FS": {"Type": "AWS::FSx::FileSystem", "Properties": {"FileSystemType": "LUSTRE", "StorageCapacity": 1200}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "fsx-no-subnets", nil)
	require.NoError(t, err, "the deploy reports the failure on the resource, not as a deployer error")
	require.Equal(t, "BadRequest: SubnetIds is required.", resourceByLogicalID(t, result, "FS").Error)
}
