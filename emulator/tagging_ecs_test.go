package emulator_test

// ECS discovery: the last piece of #835, and the one row whose two halves were split across two
// releases.
//
// ECS's service and task-definition rows shipped their *tagging* half first: mergeResourceTags' ecs
// arm reaches all four ECS ARN shapes through ecsTagStateKey, so TagResources has been able to write
// a tag to a service, a task or a task definition, and ECS's own ListTagsForResource has been able to
// read it back. What did not exist was the discovery half. scanECSClusters was the only ECS
// descriptor, so GetResources reported a cluster and nothing else in the namespace: a tag the tagging
// API itself had written was invisible to the tagging API's own inventory call.
//
// That asymmetry is what every row of #835 closes, and it is worse than a plain gap. A caller uses
// GetResources to answer "which of my resources carry this tag?", and a service that answers "none"
// about a resource it just tagged is not incomplete, it is wrong.
//
// A *task* is in scope alongside the two types #835's table names. ecsTagStateKey already resolved a
// task ARN, so a tag could be written there before this file existed, and AWS lists tasks first among
// the taggable ECS resources — "There are multiple ways that Amazon ECS tasks, services, task
// definitions, and clusters are tagged" (Tagging Amazon ECS resources). Leaving it out would have
// left the row's own defect behind in a third of the namespace.
//
// Two things came with the row rather than after it. scanECSClusters prefixed by account alone, so a
// us-east-1 caller was reported a us-west-2 cluster; had that been left, ECS would answer one caller
// cross-Region about clusters and same-Region about services, which is a worse state than either. And
// a ResourceTypeFilters entry was matched as an unanchored prefix (#936), so "ecs:task" selected a
// task definition and the selectivity assertions below could not have been written honestly.
//
// Everything here goes through signed wire calls against emulator.StartTestServer. Nothing writes
// state directly, per #765's rule that a helper writing state cannot prove a value is readable
// through the owning service's own call.

import (
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ecsWest2Target is ecsTarget's us-west-2 counterpart. The parser takes the Region off the Host, so
// the two differ only there — which is what makes a cross-Region assertion a real request rather than
// a rewritten state key.
var ecsWest2Target = signedRequestTarget{host: "ecs.us-west-2.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113", signingName: "ecs"}

// ecsTagServer starts a server callable as [taggingTestAccount].
func ecsTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// createECSServiceIn creates a service in one Region and returns the ARN ECS minted for it.
func createECSServiceIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, cluster, taskDef, name string) string {
	t.Helper()
	var out struct {
		Service struct {
			ServiceARN string `json:"serviceArn"`
		} `json:"service"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "CreateService", map[string]any{
			"serviceName":    name,
			"cluster":        cluster,
			"taskDefinition": taskDef,
			"desiredCount":   1,
		}), &out)
	require.Empty(t, errCode, "CreateService %s", name)
	require.Equal(t, 200, status, "CreateService %s", name)
	require.NotEmpty(t, out.Service.ServiceARN, "CreateService %s returned a serviceArn", name)
	return out.Service.ServiceARN
}

// runECSTaskIn runs one task in one Region and returns the ARN ECS minted for it.
func runECSTaskIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, cluster, taskDef string) string {
	t.Helper()
	var out struct {
		Tasks []struct {
			TaskARN string `json:"taskArn"`
		} `json:"tasks"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "RunTask", map[string]any{
			"cluster":        cluster,
			"taskDefinition": taskDef,
			"count":          1,
		}), &out)
	require.Empty(t, errCode, "RunTask on %s", taskDef)
	require.Equal(t, 200, status, "RunTask on %s", taskDef)
	require.Len(t, out.Tasks, 1, "RunTask returned one task")
	require.NotEmpty(t, out.Tasks[0].TaskARN, "RunTask returned a taskArn")
	return out.Tasks[0].TaskARN
}

// createECSClusterIn is [createECSCluster] against a chosen Region's endpoint.
func createECSClusterIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, name string) string {
	t.Helper()
	var out struct {
		Cluster struct {
			ClusterARN string `json:"clusterArn"`
		} `json:"cluster"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "CreateCluster",
			map[string]any{"clusterName": name}), &out)
	require.Empty(t, errCode, "CreateCluster %s", name)
	require.Equal(t, 200, status, "CreateCluster %s", name)
	require.NotEmpty(t, out.Cluster.ClusterARN, "CreateCluster %s returned a clusterArn", name)
	return out.Cluster.ClusterARN
}

// registerECSTaskDefinitionIn is [registerECSTaskDefinition] against a chosen Region's endpoint.
func registerECSTaskDefinitionIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, family string) string {
	t.Helper()
	var out struct {
		TaskDefinition struct {
			TaskDefinitionARN string `json:"taskDefinitionArn"`
		} `json:"taskDefinition"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "RegisterTaskDefinition", map[string]any{
			"family":               family,
			"containerDefinitions": []map[string]any{{"name": "nginx", "image": "nginx:latest"}},
		}), &out)
	require.Empty(t, errCode, "RegisterTaskDefinition %s", family)
	require.Equal(t, 200, status, "RegisterTaskDefinition %s", family)
	require.NotEmpty(t, out.TaskDefinition.TaskDefinitionARN, "RegisterTaskDefinition %s returned an ARN", family)
	return out.TaskDefinition.TaskDefinitionARN
}

// ecsFourResources creates one of each ECS type in us-east-1 and returns their ARNs in the order
// cluster, task definition, service, task.
func ecsFourResources(t *testing.T, ts *emulator.TestServer) (cluster, taskDef, service, task string) {
	t.Helper()
	cluster = createECSClusterIn(t, ts, ecsTarget, "web")
	taskDef = registerECSTaskDefinitionIn(t, ts, ecsTarget, "sidecar")
	service = createECSServiceIn(t, ts, ecsTarget, "web", taskDef, "api")
	task = runECSTaskIn(t, ts, ecsTarget, "web", taskDef)
	return cluster, taskDef, service, task
}

// ecsFourTaggedResourcesIn is [ecsFourResources] in a chosen Region, with each of the four tagged
// through ECS's own TagResource.
//
// A tag is what makes a resource discoverable at all: GetResources reports what has been tagged, and
// one that never was is absent by rule (#938). The tests that assert a scanner's *selection* — which
// type a filter picks, which Region a resource is attributed to, which keys are records — therefore
// need every subject tagged before the assertion, or the scanner would have nothing to select from.
func ecsFourTaggedResourcesIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget) (cluster, taskDef, service, task string) {
	t.Helper()
	cluster = createECSClusterIn(t, ts, tgt, "web")
	taskDef = registerECSTaskDefinitionIn(t, ts, tgt, "sidecar")
	service = createECSServiceIn(t, ts, tgt, "web", taskDef, "api")
	task = runECSTaskIn(t, ts, tgt, "web", taskDef)
	for _, arn := range []string{cluster, taskDef, service, task} {
		ecsTagResourceIn(t, ts, tgt, arn, map[string]string{"env": "test"})
	}
	return cluster, taskDef, service, task
}

// TestTaggingECS_GetResourcesReportsEveryECSType is the row itself: each of the four types is created
// through its owning operation, tagged through the tagging API, and then found by the tagging API's
// own discovery call carrying that tag.
//
// Before this row the last step reported only the cluster.
func TestTaggingECS_GetResourcesReportsEveryECSType(t *testing.T) {
	ts := ecsTagServer(t)
	cluster, taskDef, service, task := ecsFourResources(t, ts)

	for _, arn := range []string{cluster, taskDef, service, task} {
		tagResourcesWith(t, ts, arn, map[string]string{"env": "test"})
	}

	assert.ElementsMatch(t, []string{cluster, taskDef, service, task}, getResourcesARNs(t, ts, "ecs"),
		"GetResources reports all four ECS types")

	for _, arn := range []string{cluster, taskDef, service, task} {
		assert.Equal(t, map[string]string{"env": "test"}, getResourcesTags(t, ts, arn),
			"GetResources reports the tag it wrote to %s", arn)
	}
}

// TestTaggingECS_ATagIsReadableThroughBothAPIsForEveryType is #765's criterion in both directions for
// the three types this row adds.
//
// The write direction — tagging API in, ECS out — proves the two APIs agree on where one resource's
// tags live. The read direction — ECS in, tagging API out — is the one the scanner adds, and it is the
// one that failed before: a tag written through ECS's own TagResource was unreachable through
// GetResources because nothing scanned the record.
func TestTaggingECS_ATagIsReadableThroughBothAPIsForEveryType(t *testing.T) {
	ts := ecsTagServer(t)
	_, taskDef, service, task := ecsFourResources(t, ts)

	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"service", service},
		{"task", task},
		{"task definition", taskDef},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tagResourcesWith(t, ts, tc.arn, map[string]string{"via": "tagging"})
			assert.Equal(t, map[string]string{"via": "tagging"}, ecsTags(t, ts, tc.arn),
				"a tag the tagging API wrote reads back through ECS")

			ecsTagResource(t, ts, tc.arn, map[string]string{"via": "ecs"})
			assert.Equal(t, map[string]string{"via": "ecs"}, getResourcesTags(t, ts, tc.arn),
				"and a tag ECS wrote is reported by GetResources")
		})
	}
}

// TestTaggingECS_AResourceTypeFilterSelectsOnlyItsOwnType is why #936 had to ship with this row.
//
// AWS states that a filter of "ec2:instance" "returns only EC2 instances". "ecs:task" and
// "ecs:task-definition" are the pair that makes an unanchored prefix match observable: a task
// definition's resource portion is "task-definition/{family}:{revision}", which begins with the
// string "task", so before #936 asking for tasks answered with both.
func TestTaggingECS_AResourceTypeFilterSelectsOnlyItsOwnType(t *testing.T) {
	ts := ecsTagServer(t)
	cluster, taskDef, service, task := ecsFourTaggedResourcesIn(t, ts, ecsTarget)

	for _, tc := range []struct {
		filter string
		want   []string
	}{
		{"ecs:cluster", []string{cluster}},
		{"ecs:service", []string{service}},
		{"ecs:task", []string{task}},
		{"ecs:task-definition", []string{taskDef}},
		{"ecs", []string{cluster, taskDef, service, task}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, getResourcesARNs(t, ts, tc.filter))
		})
	}
}

// TestTaggingECS_GetResourcesDoesNotReportAnotherRegionsResources pins the Region attribution of all
// four scanners at once.
//
// Every ECS resource is Region-scoped, its ARN carries the Region, and its state key is
// Region-qualified — so the only reason a us-east-1 caller ever saw a us-west-2 cluster was that
// scanECSClusters prefixed by account alone. GetResources' own opening sentence settles it: it
// "[r]eturns all the tagged or previously tagged resources that are located in the specified AWS
// Region for the account".
//
// Both directions are asserted through real requests to the two Regions' endpoints, because a
// one-sided assertion passes against a scanner that reports nothing at all.
func TestTaggingECS_GetResourcesDoesNotReportAnotherRegionsResources(t *testing.T) {
	ts := ecsTagServer(t)
	eastCluster, eastTaskDef, eastService, eastTask := ecsFourTaggedResourcesIn(t, ts, ecsTarget)

	// The same names in us-west-2, so a Region-blind prefix would report eight ARNs to each caller.
	// Tagged as well, because an untagged resource is absent from GetResources for a reason that is
	// not the Region (#938), and this assertion has to fail if the Region gate is what breaks.
	westCluster, westTaskDef, westService, westTask := ecsFourTaggedResourcesIn(t, ts, ecsWest2Target)

	assert.ElementsMatch(t, []string{eastCluster, eastTaskDef, eastService, eastTask},
		getResourcesARNsIn(t, ts, taggingTarget), "us-east-1 reports only its own")
	assert.ElementsMatch(t, []string{westCluster, westTaskDef, westService, westTask},
		getResourcesARNsIn(t, ts, taggingWest2Target), "and us-west-2 only its own")
}

// TestTaggingECS_AnIndexKeyIsNotReportedAsAResource is the colon-terminated-prefix guard, on its sixth
// namespace.
//
// The ECS namespace holds five index keys beside its resource records — "cluster_names:",
// "taskdef_families:", "taskdef_revisions:", "service_names:" and "task_ids:" — whose values are JSON
// arrays of names. A prefix of "cluster" rather than "cluster:" lists "cluster_names:…" too, and the
// scan then drops it silently when the unmarshal into a resource type fails, which is exactly the
// failure mode that hides a real defect: the count is right by accident.
//
// The assertion is on the exact set rather than on a count, so an index key rendered as an empty or
// malformed ARN fails it rather than being absorbed.
func TestTaggingECS_AnIndexKeyIsNotReportedAsAResource(t *testing.T) {
	ts := ecsTagServer(t)
	cluster, taskDef, service, task := ecsFourTaggedResourcesIn(t, ts, ecsTarget)

	got := getResourcesARNs(t, ts, "ecs")
	assert.ElementsMatch(t, []string{cluster, taskDef, service, task}, got,
		"exactly the four resource records, and none of the five index keys")
	for _, arn := range got {
		assert.Regexp(t, `^arn:aws:ecs:us-east-1:\d{12}:(cluster|service|task|task-definition)/`, arn,
			"every reported ARN is a resource ARN")
	}
}

// TestTaggingECS_EveryTaskDefinitionRevisionIsReported records the scanner's choice about revisions.
//
// Each revision is a resource with its own ARN and its own tags: RegisterTaskDefinition accepts tags
// per revision, and ecsTagStateKey splits a "{family}:{revision}" ARN to reach exactly one of them.
// Reporting only a family's newest revision would hide a tag the tagging API itself had written to an
// older one.
func TestTaggingECS_EveryTaskDefinitionRevisionIsReported(t *testing.T) {
	ts := ecsTagServer(t)
	first := registerECSTaskDefinitionIn(t, ts, ecsTarget, "sidecar")
	second := registerECSTaskDefinitionIn(t, ts, ecsTarget, "sidecar")
	require.NotEqual(t, first, second, "the two revisions have different ARNs")

	tagResourcesWith(t, ts, first, map[string]string{"rev": "one"})
	tagResourcesWith(t, ts, second, map[string]string{"rev": "two"})

	assert.ElementsMatch(t, []string{first, second}, getResourcesARNs(t, ts, "ecs:task-definition"),
		"both revisions are reported")
	assert.Equal(t, map[string]string{"rev": "one"}, getResourcesTags(t, ts, first))
	assert.Equal(t, map[string]string{"rev": "two"}, getResourcesTags(t, ts, second))
}

// TestTaggingECS_GetResourcesReportsTagsInKeyOrder holds #862's guarantee across the new scanners.
//
// The sort is in ecsTagsToTaggingTags rather than at the four call sites, so no ECS scanner can report
// an order that depends on the order the tags happened to be written in. Two writes in reverse
// alphabetical order is the case that catches a scanner rendering the stored slice as-is.
func TestTaggingECS_GetResourcesReportsTagsInKeyOrder(t *testing.T) {
	ts := ecsTagServer(t)
	_, _, service, _ := ecsFourResources(t, ts)

	tagResourcesWith(t, ts, service, map[string]string{"zulu": "1"})
	tagResourcesWith(t, ts, service, map[string]string{"alpha": "2"})
	tagResourcesWith(t, ts, service, map[string]string{"mike": "3"})

	var keys []string
	for _, m := range getResourcesMappings(t, ts, "ecs:service") {
		if m.ResourceARN != service {
			continue
		}
		for _, tag := range m.Tags {
			keys = append(keys, tag.Key)
		}
	}
	assert.Equal(t, []string{"alpha", "mike", "zulu"}, keys, "reported in key order")
}

// ecsTagResource writes tags through ECS's own TagResource, which is the direction the scanner is
// asserted from.
func ecsTagResource(t *testing.T, ts *emulator.TestServer, arn string, tags map[string]string) {
	t.Helper()
	ecsTagResourceIn(t, ts, ecsTarget, arn, tags)
}

// ecsTagResourceIn is [ecsTagResource] against a chosen Region's endpoint, for the Region-attribution
// assertions, which have to write a tag in each of the two Regions they compare.
func ecsTagResourceIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, arn string, tags map[string]string) {
	t.Helper()
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"key": k, "value": v})
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "TagResource",
			map[string]any{"resourceArn": arn, "tags": list}), nil)
	require.Empty(t, errCode, "ECS TagResource %s", arn)
	require.Equal(t, 200, status, "ECS TagResource %s", arn)
}
