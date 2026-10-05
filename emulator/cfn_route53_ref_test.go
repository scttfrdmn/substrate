package emulator_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A hosted zone's Ref, Fn::GetAtt Id and ARN, and a record set group that reports its children's
// refusals (#1256).
//
// AWS::Route53::HostedZone publishes Ref as "the hosted zone ID, such as Z23ABC4XYZL05B" and
// Fn::GetAtt Id as "The ID that Amazon Route 53 assigned to the hosted zone when you created it";
// the same page quotes the zone's ARN as arn:aws:route53:::hostedzone/<hosted zone ID>.
// CreateHostedZone answers its Id member in the API's path form, /hostedzone/Z…, and until #1256
// that path form was both the Ref and the ARN.

func cfnRoute53ByLogical(result *emulator.DeployResult) map[string]emulator.DeployedResource {
	out := make(map[string]emulator.DeployedResource, len(result.Resources))
	for _, r := range result.Resources {
		out[r.LogicalID] = r
	}
	return out
}

func TestCFNRoute53_RefGetAttIdAndARNAnswerThePublishedForms(t *testing.T) {
	d := newV016FullDeployer(t)
	result, err := d.Deploy(context.Background(), `{
		"Resources": {
			"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "ref.example.com"}}
		},
		"Outputs": {
			"ZoneRef": {"Value": {"Ref": "MyZone"}},
			"ZoneId":  {"Value": {"Fn::GetAtt": ["MyZone", "Id"]}}
		}
	}`, "r53-ref-stack", nil)
	require.NoError(t, err)
	zone := cfnRoute53ByLogical(result)["MyZone"]
	require.Empty(t, zone.Error)

	assert.Regexp(t, `^Z[0-9A-Z]+$`, result.Outputs["ZoneRef"], "Ref answers the bare hosted zone ID")
	assert.NotContains(t, result.Outputs["ZoneRef"], "/hostedzone/")
	assert.Equal(t, result.Outputs["ZoneRef"], result.Outputs["ZoneId"],
		"Fn::GetAtt Id answers the same bare ID as Ref")
	assert.Equal(t, "arn:aws:route53:::hostedzone/"+zone.PhysicalID, zone.ARN,
		"the ARN is the form the page quotes, not the API's path form")
}

// newCFNRoute53Deployer is a deployer with only the Route 53 plugin, over a caller's state, so a test
// can read the record sets the deploy wrote or fault the write.
func newCFNRoute53Deployer(t *testing.T, state emulator.StateManager) *emulator.StackDeployer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	p := &emulator.Route53Plugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(p)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	return emulator.NewStackDeployer(registry, emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig()),
		state, tc, logger, emulator.NewCostController(emulator.CostConfig{Enabled: true}))
}

// cfnRoute53GroupTemplate is a zone and a record set group that names it at group level, the way
// AWS::Route53::RecordSetGroup declares it; the element inside the group names no zone.
const cfnRoute53GroupTemplate = `{
	"Resources": {
		"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "group.example.com"}},
		"Group": {
			"Type": "AWS::Route53::RecordSetGroup",
			"Properties": {
				"HostedZoneId": {"Ref": "MyZone"},
				"RecordSets": [{"Name": "www.group.example.com", "Type": "A", "TTL": "60", "ResourceRecords": "192.0.2.1"}]
			}
		}
	}
}`

// A record set in a group lands in the group's zone. Each element was deployed with its own
// properties alone, which name no zone, so the record set was written under an empty zone ID.
func TestCFNRoute53_ARecordSetGroupWritesIntoTheGroupsZone(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	d := newCFNRoute53Deployer(t, state)
	result, err := d.Deploy(context.Background(), cfnRoute53GroupTemplate, "r53-group-zone", nil)
	require.NoError(t, err)
	byLogical := cfnRoute53ByLogical(result)
	require.Empty(t, byLogical["Group"].Error)
	zoneID := byLogical["MyZone"].PhysicalID

	keys, err := state.List(context.Background(), "route53", "rrset:")
	require.NoError(t, err)
	assert.Contains(t, keys, "rrset:"+zoneID+":A:www.group.example.com",
		"the group's record set is written into the zone the group names: %v", keys)
	for _, k := range keys {
		assert.NotContains(t, k, "rrset::", "no record set is written under an empty zone ID: %v", keys)
	}
}

// A child record set's refusal is the group's own. deployRoute53RecordSet reports a refusal on its
// DeployedResource, never as a Go error, and the group discarded that resource, so a group whose
// every record set was refused reported clean.
func TestCFNRoute53_ARecordSetGroupReportsItsChildsRefusal(t *testing.T) {
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager(), failPut: "rrset:"}
	d := newCFNRoute53Deployer(t, fault)
	result, err := d.Deploy(context.Background(), cfnRoute53GroupTemplate, "r53-group-refused", nil)
	require.NoError(t, err)

	byLogical := cfnRoute53ByLogical(result)
	assert.Empty(t, byLogical["MyZone"].Error, "the zone itself deployed")
	group := byLogical["Group"]
	assert.Contains(t, group.Error, "record set 0",
		"a child record set's refusal is the group's own, where it used to be discarded")
	assert.NotEqual(t, "CREATE_COMPLETE", result.Status, "a refused group fails the stack it is in")
}

// A stack delete reaches the zone. The deleter is the zone path plus the physical ID, so while the
// physical ID was the path form it sent /2013-04-01/hostedzone//hostedzone/Z…, which no route
// answers, and the zone outlived its stack.
func TestCFNRoute53_AStackDeleteDeletesTheHostedZone(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{
		"Resources": {
			"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "delete.example.com"}},
			"Rec": {"Type": "AWS::Route53::RecordSet", "Properties": {
				"HostedZoneId": {"Ref": "MyZone"}, "Name": "a.delete.example.com", "Type": "A", "TTL": "60",
				"ResourceRecords": "192.0.2.2"}}
		}
	}`, "r53-delete-stack", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "%s", r.LogicalID)
	}

	deletions := d.DeleteStackResourcesForTest(ctx, "r53-delete-stack")
	byLogical := map[string]emulator.CFNResourceDeletion{}
	for _, del := range deletions {
		byLogical[del.LogicalID] = del
	}
	assert.Equal(t, "DELETE_COMPLETE", byLogical["MyZone"].Status, "%+v", byLogical["MyZone"])
	assert.Equal(t, "DELETE_COMPLETE", byLogical["Rec"].Status, "%+v", byLogical["Rec"])
}
