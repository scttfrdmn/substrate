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

// newECSDynamoTestDeployer creates a StackDeployer with the ECS and DynamoDB
// plugins and returns the registry, so a test can read a deployed resource back
// through the same API a consumer's SDK would call.
//
// Reading back through the plugin is the point: #527's whole symptom is that the
// deploy result and the stack status are both correct while the container list a
// consumer parses is empty, so an assertion on the DeployResult cannot see it.
func newECSDynamoTestDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.PluginRegistry, *emulator.EventStore) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	// IncludeBodies, so a test can assert on the exact bytes a deploy path put on
	// the wire. That is the only place a *casing* defect is observable for a
	// service whose plugin is typed: encoding/json matches a field
	// case-insensitively absent an exact match, so DynamoDB's PascalCase struct
	// reads "keySchema" perfectly well and a read-back agrees with the defect.
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:       true,
		Backend:       "memory",
		IncludeBodies: true,
	})
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	opts := emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}
	for _, p := range []emulator.Plugin{
		&emulator.ECSPlugin{},
		&emulator.DynamoDBPlugin{},
	} {
		require.NoError(t, p.Initialize(context.Background(), opts))
		registry.Register(p)
	}

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), registry, store
}

// dispatchedBody returns the raw request body the deploy path sent for the named
// service and operation.
//
// The event store is the only view of what actually went over the wire. A
// read-back through the plugin cannot substitute for it when the question is a
// member's *name*, because the plugin unmarshals the name it was sent and
// re-marshals the name its own struct is tagged with.
//
// operation is the semantic API operation ("CreateCluster"), not the HTTP verb.
// A recorded REST event was named after its method until #572 resolved the name
// before the pipeline read it, which is what made an event log greppable by
// operation at all.
func dispatchedBody(t *testing.T, store *emulator.EventStore, service, operation string) []byte {
	t.Helper()
	events, err := store.GetEvents(context.Background(), emulator.EventFilter{
		Service:   service,
		Operation: operation,
	})
	require.NoError(t, err)
	require.Len(t, events, 1, "expected exactly one %s %s request", service, operation)
	require.NotNil(t, events[0].Request, "the event store must have captured the body")
	return events[0].Request.Body
}

// routeJSON sends a JSON-protocol request through the registry.
func routeJSON(
	t *testing.T,
	registry *emulator.PluginRegistry,
	service, target, op string,
	body []byte,
) (*emulator.AWSResponse, error) {
	t.Helper()
	return registry.RouteRequest(&emulator.RequestContext{
		RequestID: "test-request",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}, &emulator.AWSRequest{
		Service:   service,
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": target},
		Body:      body,
		Params:    map[string]string{},
	})
}

// describeTaskDefinition reads a task definition back through the ECS plugin and
// returns its containerDefinitions as raw JSON.
//
// The container list is returned as json.RawMessage rather than unmarshalled into
// a typed struct because the defect is a *key name*: a struct with `json:"name"`
// tags would read nothing from a PascalCase body, and a struct tagged PascalCase
// would agree with the defect. Only the raw member names can tell them apart.
func describeTaskDefinition(t *testing.T, registry *emulator.PluginRegistry, family string) []map[string]json.RawMessage {
	t.Helper()
	body, err := json.Marshal(map[string]any{"taskDefinition": family})
	require.NoError(t, err)

	resp, err := routeJSON(t, registry, "ecs",
		"AmazonEC2ContainerServiceV20141113.DescribeTaskDefinition", "DescribeTaskDefinition", body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "DescribeTaskDefinition: %s", resp.Body)

	var out struct {
		TaskDefinition struct {
			ContainerDefinitions []map[string]json.RawMessage `json:"containerDefinitions"`
		} `json:"taskDefinition"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out.TaskDefinition.ContainerDefinitions
}

// rawString unmarshals a JSON member expected to hold a string.
func rawString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	require.NoError(t, json.Unmarshal(raw, &s), "not a JSON string: %s", raw)
	return s
}

// TestCFN_ECSContainerDefinitions_MemberNamesAndNesting is #527's own
// reproduction plus #526's: a CloudFormation-declared container reaches the ECS
// plugin under the ECS API's member names, with the intrinsics inside it
// resolved.
//
// Before this, the stack reached CREATE_COMPLETE, DescribeTaskDefinition answered
// 200, and `--query 'taskDefinition.containerDefinitions'` returned `[{}]`.
func TestCFN_ECSContainerDefinitions_MemberNamesAndNesting(t *testing.T) {
	d, registry, _ := newECSDynamoTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Image":    {"Type": "String", "Default": "python:3.11"},
			"Command":  {"Type": "String", "Default": "python,-m,worker"},
			"LogGroup": {"Type": "String", "Default": "/ecs/worker"}
		},
		"Resources": {
			"TD": {
				"Type": "AWS::ECS::TaskDefinition",
				"Properties": {
					"Family": "member-names",
					"ContainerDefinitions": [{
						"Name": "worker",
						"Image": {"Ref": "Image"},
						"Essential": true,
						"Command": {"Fn::Split": [",", {"Ref": "Command"}]},
						"Environment": [
							{"Name": "MODE", "Value": {"Fn::Sub": "${AWS::Region}-prod"}}
						],
						"PortMappings": [
							{"ContainerPort": 8080, "Protocol": "tcp"}
						],
						"LogConfiguration": {
							"LogDriver": "awslogs",
							"Options": {
								"awslogs-group":         {"Ref": "LogGroup"},
								"awslogs-region":        {"Ref": "AWS::Region"},
								"awslogs-stream-prefix": "worker"
							}
						}
					}]
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "member-names-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "TD").Error)

	cdefs := describeTaskDefinition(t, registry, "member-names")
	require.Len(t, cdefs, 1)
	c := cdefs[0]

	// The keys an SDK reads.
	assert.Equal(t, "worker", rawString(t, c["name"]))
	assert.Equal(t, "python:3.11", rawString(t, c["image"]), "a Ref inside a container definition resolves")
	assert.JSONEq(t, `true`, string(c["essential"]))

	// The PascalCase names must be gone entirely, not merely duplicated.
	for _, pascal := range []string{"Name", "Image", "Essential", "Command", "Environment", "PortMappings", "LogConfiguration"} {
		assert.NotContains(t, c, pascal, "CloudFormation's %q must not reach the ECS API", pascal)
	}

	// #521's list return, observed where it was reported: the command is three
	// elements, not one string and not an Fn::Split object.
	assert.JSONEq(t, `["python","-m","worker"]`, string(c["command"]))

	// A nested structured member's own keys are mapped too, and an Fn::Sub
	// inside one resolves.
	assert.JSONEq(t, `[{"name":"MODE","value":"us-east-1-prod"}]`, string(c["environment"]))
	assert.JSONEq(t, `[{"containerPort":8080,"protocol":"tcp"}]`, string(c["portMappings"]),
		"a literal number stays a number")

	// logConfiguration's own members are mapped; its options keys are user data
	// and are carried through verbatim, values resolved.
	assert.JSONEq(t, `{
		"logDriver": "awslogs",
		"options": {
			"awslogs-group":         "/ecs/worker",
			"awslogs-region":        "us-east-1",
			"awslogs-stream-prefix": "worker"
		}
	}`, string(c["logConfiguration"]))
}

// TestCFN_ECSContainerDefinitions_UnknownMemberSurvives pins that a member
// substrate has not enumerated passes through unchanged rather than being
// lowercased or dropped.
//
// The alternative — a first-letter-lowering function — would be shorter and would
// even be right for all 42 members ECS defines today, but it cannot tell "not
// mapped" from "mapped to itself", so a member added to the API after this table
// was written would be silently renamed to something ECS does not accept. A
// verbatim unknown member is visible in a response; a corrupted one is not.
func TestCFN_ECSContainerDefinitions_UnknownMemberSurvives(t *testing.T) {
	d, registry, _ := newECSDynamoTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"TD": {
				"Type": "AWS::ECS::TaskDefinition",
				"Properties": {
					"Family": "unknown-member",
					"ContainerDefinitions": [{
						"Name": "worker",
						"SomeFutureMember": "kept-verbatim"
					}]
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "unknown-member-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "TD").Error)

	cdefs := describeTaskDefinition(t, registry, "unknown-member")
	require.Len(t, cdefs, 1)
	assert.Equal(t, "worker", rawString(t, cdefs[0]["name"]))
	assert.Equal(t, "kept-verbatim", rawString(t, cdefs[0]["SomeFutureMember"]),
		"an unmapped member is delivered verbatim rather than guessed at")
}

// TestCFN_ECSContainerDefinitions_NoValueRemovesMember covers the form
// ecs_worker.yml writes: a property that is present or absent according to a
// condition.
//
// CloudFormation *removes* the property when Fn::If chooses AWS::NoValue. Leaving
// an empty string or an empty list behind would make ECS see a container that
// overrides its image's entrypoint with nothing.
func TestCFN_ECSContainerDefinitions_NoValueRemovesMember(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Command": {"Type": "String", "Default": ""}
		},
		"Conditions": {
			"HasCommand": {"Fn::Not": [{"Fn::Equals": [{"Ref": "Command"}, ""]}]}
		},
		"Resources": {
			"TD": {
				"Type": "AWS::ECS::TaskDefinition",
				"Properties": {
					"Family": "novalue",
					"ContainerDefinitions": [{
						"Name": "worker",
						"Image": "python:3.11",
						"Command": {"Fn::If": [
							"HasCommand",
							{"Fn::Split": [",", {"Ref": "Command"}]},
							{"Ref": "AWS::NoValue"}
						]}
					}]
				}
			}
		}
	}`

	t.Run("condition false removes the member", func(t *testing.T) {
		d, registry, _ := newECSDynamoTestDeployer(t)
		result, err := d.Deploy(context.Background(), tmpl, "novalue-absent", nil)
		require.NoError(t, err)
		assert.Empty(t, findResource(t, result, "TD").Error)

		cdefs := describeTaskDefinition(t, registry, "novalue")
		require.Len(t, cdefs, 1)
		assert.NotContains(t, cdefs[0], "command",
			"AWS::NoValue removes the member rather than emptying it")
		assert.NotContains(t, cdefs[0], "Command")
		assert.Equal(t, "python:3.11", rawString(t, cdefs[0]["image"]))
	})

	t.Run("condition true resolves the list", func(t *testing.T) {
		d, registry, _ := newECSDynamoTestDeployer(t)
		result, err := d.Deploy(context.Background(), tmpl, "novalue-present", map[string]string{
			"Command": "python,-m,worker",
		})
		require.NoError(t, err)
		assert.Empty(t, findResource(t, result, "TD").Error)

		cdefs := describeTaskDefinition(t, registry, "novalue")
		require.Len(t, cdefs, 1)
		assert.JSONEq(t, `["python","-m","worker"]`, string(cdefs[0]["command"]))
	})
}

// TestCFN_DynamoDB_NestedRefInKeySchema is #526's own reproduction. A
// `{"Ref": "PK"}` inside KeySchema failed the resource outright, the typed
// DynamoDB plugin rejecting the object:
//
//	SerializationException: Failed to parse request: json: cannot unmarshal
//	object into Go struct field
//	DynamoDBAttributeDefinition.AttributeDefinitions.AttributeName of type string
//
// The table is read back through DescribeTable, because a resource reporting
// CREATE_COMPLETE is exactly what #519 taught not to trust on its own.
func TestCFN_DynamoDB_NestedRefInKeySchema(t *testing.T) {
	d, registry, store := newECSDynamoTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"PK": {"Type": "String", "Default": "pk"}},
		"Resources": {
			"T": {
				"Type": "AWS::DynamoDB::Table",
				"Properties": {
					"TableName": "nested-probe",
					"BillingMode": "PAY_PER_REQUEST",
					"KeySchema": [{"AttributeName": {"Ref": "PK"}, "KeyType": "HASH"}],
					"AttributeDefinitions": [{"AttributeName": {"Ref": "PK"}, "AttributeType": "S"}]
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "ddb-nested-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "T").Error)

	body, err := json.Marshal(map[string]any{"TableName": "nested-probe"})
	require.NoError(t, err)
	resp, err := routeJSON(t, registry, "dynamodb",
		"DynamoDB_20120810.DescribeTable", "DescribeTable", body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "DescribeTable: %s", resp.Body)

	var out struct {
		Table struct {
			KeySchema []struct {
				AttributeName string `json:"AttributeName"`
				KeyType       string `json:"KeyType"`
			} `json:"KeySchema"`
			AttributeDefinitions []struct {
				AttributeName string `json:"AttributeName"`
				AttributeType string `json:"AttributeType"`
			} `json:"AttributeDefinitions"`
		} `json:"Table"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))

	// DynamoDB's API is natively PascalCase, so these member names are
	// CloudFormation's own — the guard against a generic case converter.
	require.Len(t, out.Table.KeySchema, 1)
	assert.Equal(t, "pk", out.Table.KeySchema[0].AttributeName)
	assert.Equal(t, "HASH", out.Table.KeySchema[0].KeyType)
	require.Len(t, out.Table.AttributeDefinitions, 1)
	assert.Equal(t, "pk", out.Table.AttributeDefinitions[0].AttributeName)
	assert.Equal(t, "S", out.Table.AttributeDefinitions[0].AttributeType)

	// The bytes, not the round-trip. #527's key mapping is per-service precisely
	// because a converter that helped ECS would corrupt this request, and only the
	// raw body can tell — the assertions above pass either way.
	sent := string(dispatchedBody(t, store, "dynamodb", "CreateTable"))
	for _, member := range []string{
		`"TableName"`, `"KeySchema"`, `"AttributeName"`, `"KeyType"`,
		`"AttributeDefinitions"`, `"AttributeType"`, `"BillingMode"`,
	} {
		assert.Contains(t, sent, member,
			"the DynamoDB API is natively PascalCase; %s must reach it as CloudFormation spells it", member)
	}
	for _, camel := range []string{
		`"tableName"`, `"keySchema"`, `"attributeName"`, `"keyType"`,
		`"attributeDefinitions"`, `"attributeType"`, `"billingMode"`,
	} {
		assert.NotContains(t, sent, camel,
			"a generic case converter would produce %s, which DynamoDB does not define", camel)
	}
}

// TestResolveNested_Conventions pins resolveNested's four rules at the seam.
//
// Each is a rule a naive walk gets wrong, and most are invisible through the
// deploy paths: a plugin that stores an untyped property cannot report that a key
// was rewritten, and a multi-key map resolving to whichever key Go's map
// iteration reached first is a race that a single deploy passes by luck.
func TestResolveNested_Conventions(t *testing.T) {
	params := map[string]string{
		"Image":   "python:3.11",
		"Command": "python,-m,worker",
		"Empty":   "",
		"Subnets": "subnet-1, subnet-2",
	}
	listParams := map[string]bool{"Subnets": true}
	conditions := map[string]bool{"Yes": true, "No": false}

	cases := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		{
			name: "a literal is returned unchanged, keeping its type",
			in:   map[string]interface{}{"ContainerPort": float64(8080), "Essential": true},
			want: map[string]interface{}{"ContainerPort": float64(8080), "Essential": true},
		},
		{
			name: "an intrinsic nested in a map resolves",
			in:   map[string]interface{}{"Image": map[string]interface{}{"Ref": "Image"}},
			want: map[string]interface{}{"Image": "python:3.11"},
		},
		{
			name: "an intrinsic nested in a list element resolves",
			in: []interface{}{
				map[string]interface{}{"AttributeName": map[string]interface{}{"Ref": "Command"}},
			},
			want: []interface{}{map[string]interface{}{"AttributeName": "python,-m,worker"}},
		},
		{
			name: "an intrinsic at arbitrary depth resolves",
			in: map[string]interface{}{
				"a": []interface{}{map[string]interface{}{
					"b": map[string]interface{}{"c": map[string]interface{}{"Ref": "Image"}},
				}},
			},
			want: map[string]interface{}{
				"a": []interface{}{map[string]interface{}{
					"b": map[string]interface{}{"c": "python:3.11"},
				}},
			},
		},
		{
			// A list position is where a list-valued intrinsic belongs, so its
			// elements splice rather than nesting.
			name: "a list-valued intrinsic in a list position splices",
			in: []interface{}{
				"first",
				map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
			},
			want: []interface{}{"first", "python", "-m", "worker"},
		},
		{
			// The property itself is the intrinsic, which is how ECS's
			// `"Command": !Split [',', !Ref Command]` arrives — and a `command`
			// is a list of strings, so this member must *be* a list.
			//
			// This is where resolveNested departs from resolveValue rather than
			// delegating to it: resolveValue rejoins on the delimiter (#521)
			// because it has to return a string, and returning interface{} means
			// this walk does not have to make that trade.
			name: "a member that is a list-valued intrinsic resolves to a list",
			in: map[string]interface{}{
				"Command": map[string]interface{}{
					"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}},
				},
			},
			want: map[string]interface{}{
				"Command": []interface{}{"python", "-m", "worker"},
			},
		},
		{
			// A scalar intrinsic in the same position stays a string, so the
			// list shape is the list-valued intrinsics' own and not a blanket
			// promotion of every resolved member.
			name: "a member that is a scalar intrinsic resolves to a string",
			in:   map[string]interface{}{"Image": map[string]interface{}{"Fn::Join": []interface{}{":", []interface{}{"python", "3.11"}}}},
			want: map[string]interface{}{"Image": "python:3.11"},
		},
		{
			// Fn::If is transparent to list-valuedness: whether the member is a
			// list depends on which branch the condition selects, so the
			// condition has to be evaluated rather than the branches inspected.
			name: "Fn::If choosing a list branch resolves to a list",
			in: map[string]interface{}{
				"Command": map[string]interface{}{"Fn::If": []interface{}{
					"Yes",
					map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
					map[string]interface{}{"Ref": "Image"},
				}},
			},
			want: map[string]interface{}{
				"Command": []interface{}{"python", "-m", "worker"},
			},
		},
		{
			name: "Fn::If choosing a scalar branch resolves to a string",
			in: map[string]interface{}{
				"Command": map[string]interface{}{"Fn::If": []interface{}{
					"No",
					map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
					map[string]interface{}{"Ref": "Image"},
				}},
			},
			want: map[string]interface{}{"Command": "python:3.11"},
		},
		{
			name: "Fn::If choosing a literal list branch resolves to a list",
			in: map[string]interface{}{
				"Command": map[string]interface{}{"Fn::If": []interface{}{
					"Yes",
					[]interface{}{"a", "b"},
					map[string]interface{}{"Ref": "AWS::NoValue"},
				}},
			},
			want: map[string]interface{}{"Command": []interface{}{"a", "b"}},
		},
		{
			name: "Ref AWS::NoValue removes the member",
			in: map[string]interface{}{
				"Keep": "kept",
				"Drop": map[string]interface{}{"Ref": "AWS::NoValue"},
			},
			want: map[string]interface{}{"Keep": "kept"},
		},
		{
			name: "Fn::If choosing AWS::NoValue removes the member",
			in: map[string]interface{}{
				"Keep": "kept",
				"Drop": map[string]interface{}{"Fn::If": []interface{}{
					"No",
					map[string]interface{}{"Ref": "Image"},
					map[string]interface{}{"Ref": "AWS::NoValue"},
				}},
			},
			want: map[string]interface{}{"Keep": "kept"},
		},
		{
			name: "Fn::If choosing a value keeps the member",
			in: map[string]interface{}{
				"Image": map[string]interface{}{"Fn::If": []interface{}{
					"Yes",
					map[string]interface{}{"Ref": "Image"},
					map[string]interface{}{"Ref": "AWS::NoValue"},
				}},
			},
			want: map[string]interface{}{"Image": "python:3.11"},
		},
		{
			// A property resolving to the *empty string* is not the same as an
			// absent one: `Default: ''` plus Ref is how a template spells an
			// optional value, and CloudFormation passes it through.
			name: "a Ref to an empty parameter keeps the member as an empty string",
			in:   map[string]interface{}{"Image": map[string]interface{}{"Ref": "Empty"}},
			want: map[string]interface{}{"Image": ""},
		},
		{
			// The rule #521 established for resolveValue and resolveValueList,
			// applied here for the same two reasons: an ECS container definition
			// or an IAM policy document may hold a member named "Ref", and
			// resolving a multi-key map returns whichever key Go's map iteration
			// reached first.
			name: "a multi-key map containing Ref is user data, not an intrinsic",
			in: map[string]interface{}{
				"Ref":   "Image",
				"Other": "value",
			},
			want: map[string]interface{}{
				"Ref":   "Image",
				"Other": "value",
			},
		},
		{
			name: "a multi-key map's own members still resolve",
			in: map[string]interface{}{
				"Ref":   map[string]interface{}{"Ref": "Image"},
				"Other": "value",
			},
			want: map[string]interface{}{
				"Ref":   "python:3.11",
				"Other": "value",
			},
		},
		{
			// Rewriting a key is #527's job. Conflating the two is how
			// logConfiguration.options — whose keys are user data — would get
			// mangled.
			name: "keys are never rewritten",
			in: map[string]interface{}{
				"awslogs-group": map[string]interface{}{"Ref": "Image"},
				"MixedCase":     "kept",
			},
			want: map[string]interface{}{
				"awslogs-group": "python:3.11",
				"MixedCase":     "kept",
			},
		},
		{
			name: "an unrecognized intrinsic name is an ordinary member",
			in:   map[string]interface{}{"Fn::Unknown": "x"},
			want: map[string]interface{}{"Fn::Unknown": "x"},
		},
		{
			// A Ref is list-valued when the parameter's *declared type* is,
			// which is the one list-valued intrinsic whose shape cannot be seen
			// from the template expression alone.
			name: "a Ref to a list-typed parameter resolves to a list",
			in:   map[string]interface{}{"SubnetIds": map[string]interface{}{"Ref": "Subnets"}},
			want: map[string]interface{}{"SubnetIds": []interface{}{"subnet-1", "subnet-2"}},
		},
		{
			name: "a Ref to a String parameter resolves to a string",
			in:   map[string]interface{}{"SubnetId": map[string]interface{}{"Ref": "Image"}},
			want: map[string]interface{}{"SubnetId": "python:3.11"},
		},
		{
			name: "a scalar is returned as itself",
			in:   "plain",
			want: "plain",
		},
		{
			name: "nil is returned as nil",
			in:   nil,
			want: nil,
		},
		{
			name: "an empty list stays an empty list",
			in:   []interface{}{},
			want: []interface{}{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := emulator.ResolveNestedForTest(tc.in, params, listParams, conditions)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestECSRewriteKeys_UserDataMembers pins that the key rewrite refuses to descend
// into a member whose keys are user-supplied.
//
// logConfiguration.options is keyed by log-driver option names and dockerLabels
// by whatever the consumer chose. A rewrite walk cannot tell a member name it has
// never heard of from a user's label, so the only safe rule is to stop.
func TestECSRewriteKeys_UserDataMembers(t *testing.T) {
	in := []interface{}{map[string]interface{}{
		"Name": "worker",
		"LogConfiguration": map[string]interface{}{
			"LogDriver": "awslogs",
			"Options": map[string]interface{}{
				// Deliberately named like members the table maps, to prove the
				// walk stopped rather than merely missing them.
				"Name":          "not-a-member",
				"Image":         "not-a-member",
				"awslogs-group": "/ecs/worker",
			},
		},
		"DockerLabels": map[string]interface{}{
			"Name":             "not-a-member",
			"com.example.team": "platform",
		},
	}}

	got := emulator.ECSRewriteContainerKeysForTest(in)
	assert.Equal(t, []interface{}{map[string]interface{}{
		"name": "worker",
		"logConfiguration": map[string]interface{}{
			"logDriver": "awslogs",
			"options": map[string]interface{}{
				"Name":          "not-a-member",
				"Image":         "not-a-member",
				"awslogs-group": "/ecs/worker",
			},
		},
		"dockerLabels": map[string]interface{}{
			"Name":             "not-a-member",
			"com.example.team": "platform",
		},
	}}, got)
}

// TestECSContainerDefinitionKeys_MatchAPICase asserts the mapping table against
// the rule every one of ECS's 42 ContainerDefinition members follows: the API
// member name is the CloudFormation property name with its first letter lowered.
//
// The table is still written out rather than generated from this rule, because
// the rule is a coincidence of this one type — an unmapped key must pass through
// unchanged, and only a table can tell "not mapped" from "mapped to itself". This
// test is what keeps a typo in 42 hand-written entries from shipping.
func TestECSContainerDefinitionKeys_MatchAPICase(t *testing.T) {
	for cfnName, apiName := range emulator.ECSContainerDefinitionKeysForTest() {
		require.NotEmpty(t, cfnName)
		want := string(cfnName[0]|0x20) + cfnName[1:]
		assert.Equal(t, want, apiName, "CFN %q", cfnName)
	}
}

// newCollateralTestDeployer creates a StackDeployer with the ACM, MSK and FSx
// plugins and an event store that captures bodies.
//
// These three are #526's collateral: their deploy paths asserted a property was a
// literal `string` and silently dropped anything else, so a `!Ref`-valued subnet
// list or an `!Sub`-valued tag never reached the API while the resource reported
// CREATE_COMPLETE. Their observable is the request body, since none of the three
// echoes these members back.
func newCollateralTestDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.EventStore) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:       true,
		Backend:       "memory",
		IncludeBodies: true,
	})
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	opts := emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}
	for _, p := range []emulator.Plugin{
		&emulator.ACMPlugin{},
		&emulator.MSKPlugin{},
		&emulator.FSxPlugin{},
	} {
		require.NoError(t, p.Initialize(context.Background(), opts))
		registry.Register(p)
	}

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), store
}

// TestCFN_ACMSubjectAlternativeNames_Resolved pins that a SAN list carrying
// intrinsics reaches ACM resolved and complete.
//
// ACM's RequestCertificate is natively PascalCase, so the member name is
// CloudFormation's own — this is the second guard against a generic converter,
// in a different plugin from DynamoDB's.
func TestCFN_ACMSubjectAlternativeNames_Resolved(t *testing.T) {
	d, store := newCollateralTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env":  {"Type": "String", "Default": "prod"},
			"Alts": {"Type": "String", "Default": "a.example.com,b.example.com"}
		},
		"Resources": {
			"Cert": {
				"Type": "AWS::CertificateManager::Certificate",
				"Properties": {
					"DomainName": "example.com",
					"SubjectAlternativeNames": [
						{"Fn::Sub": "${Env}.example.com"},
						{"Fn::Split": [",", {"Ref": "Alts"}]}
					]
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "acm-sans", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "Cert").Error)

	var sent struct {
		DomainName              string   `json:"DomainName"`
		SubjectAlternativeNames []string `json:"SubjectAlternativeNames"`
	}
	require.NoError(t, json.Unmarshal(dispatchedBody(t, store, "acm", "RequestCertificate"), &sent))
	assert.Equal(t, "example.com", sent.DomainName)
	// The Fn::Split contributes both of its elements rather than one rejoined
	// string, which is #521's list return observed through #526's walk.
	assert.Equal(t, []string{"prod.example.com", "a.example.com", "b.example.com"},
		sent.SubjectAlternativeNames)
}

// TestCFN_MSKClientSubnets_Resolved pins that a `!Ref`-valued ClientSubnets
// reaches MSK, which a `.(string)` assertion dropped: a template names its
// subnets with a Ref far more often than as literals.
func TestCFN_MSKClientSubnets_Resolved(t *testing.T) {
	d, store := newCollateralTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Subnets": {"Type": "List<String>", "Default": "subnet-a,subnet-b"},
			"Size":    {"Type": "String", "Default": "kafka.m5.xlarge"}
		},
		"Resources": {
			"Cluster": {
				"Type": "AWS::MSK::Cluster",
				"Properties": {
					"ClusterName": "cfn-kafka",
					"BrokerNodeGroupInfo": {
						"InstanceType": {"Ref": "Size"},
						"ClientSubnets": {"Ref": "Subnets"}
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "msk-subnets", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "Cluster").Error)

	var sent struct {
		BrokerNodeGroupInfo struct {
			InstanceType  string   `json:"InstanceType"`
			ClientSubnets []string `json:"ClientSubnets"`
		} `json:"BrokerNodeGroupInfo"`
	}
	require.NoError(t, json.Unmarshal(dispatchedBody(t, store, "msk", "CreateCluster"), &sent))
	assert.Equal(t, "kafka.m5.xlarge", sent.BrokerNodeGroupInfo.InstanceType)
	assert.Equal(t, []string{"subnet-a", "subnet-b"}, sent.BrokerNodeGroupInfo.ClientSubnets)
}

// TestCFN_MSKClientSubnets_AbsentStaysEmptyList pins that an absent
// ClientSubnets keeps its empty-slice default rather than becoming a JSON null.
//
// The resolved value overwrites the default only when it resolved to something,
// because `null` and `[]` are different requests: a consumer decoding the body
// gets a nil slice for one and an empty one for the other, and MSK's own
// validation distinguishes them.
func TestCFN_MSKClientSubnets_AbsentStaysEmptyList(t *testing.T) {
	d, store := newCollateralTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Cluster": {
				"Type": "AWS::MSK::Cluster",
				"Properties": {
					"ClusterName": "cfn-kafka-bare",
					"BrokerNodeGroupInfo": {"InstanceType": "kafka.m5.large"}
				}
			}
		}
	}`
	_, err := d.Deploy(context.Background(), tmpl, "msk-bare", nil)
	require.NoError(t, err)

	sent := string(dispatchedBody(t, store, "msk", "CreateCluster"))
	assert.Contains(t, sent, `"ClientSubnets":[]`)
	assert.NotContains(t, sent, `"ClientSubnets":null`)
}

// TestCFN_FSxIntrinsicProperties_Resolved pins FSx's two dropped properties: a
// `!Ref`-valued SubnetIds list and an `!Sub`-valued tag value.
//
// `Value: !Sub '${AWS::StackName}-data'` is the single most common way a template
// writes a tag, and a `.(string)` assertion dropped every one of them — leaving
// the key present with an empty value, which is worse than absent because it
// looks deliberate.
func TestCFN_FSxIntrinsicProperties_Resolved(t *testing.T) {
	d, store := newCollateralTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"Subnets": {"Type": "List<String>", "Default": "subnet-a,subnet-b"}},
		"Resources": {
			"FS": {
				"Type": "AWS::FSx::FileSystem",
				"Properties": {
					"FileSystemType": "LUSTRE",
					"StorageCapacity": 1200,
					"SubnetIds": {"Ref": "Subnets"},
					"Tags": [
						{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-data"}},
						{"Key": {"Fn::Sub": "Owner"}, "Value": "team"}
					]
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "fsx-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "FS").Error)

	var sent struct {
		SubnetIds []string `json:"SubnetIds"`
		Tags      []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(dispatchedBody(t, store, "fsx", "CreateFileSystem"), &sent))
	assert.Equal(t, []string{"subnet-a", "subnet-b"}, sent.SubnetIds)
	require.Len(t, sent.Tags, 2)
	assert.Equal(t, "Name", sent.Tags[0].Key)
	assert.Equal(t, "fsx-stack-data", sent.Tags[0].Value)
	assert.Equal(t, "Owner", sent.Tags[1].Key)
	assert.Equal(t, "team", sent.Tags[1].Value)
}
