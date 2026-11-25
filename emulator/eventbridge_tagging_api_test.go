package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API on EventBridge rules and buses (#1640). EventBridge implements the
// tagging provider interfaces, so TagResources and UntagResources write through the same code as
// EventBridge's own TagResource, under the same lock, and GetResources reads through its lister. With
// no registry the tagging plugin reaches the same records directly.

// ebTaggingEnv is a server holding an EventBridge plugin and a tagging plugin.
type ebTaggingEnv struct {
	t   *testing.T
	srv http.Handler
}

func newEBTaggingEnv(t *testing.T, reach bool) *ebTaggingEnv {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	eb := &emulator.EventBridgePlugin{}
	require.NoError(t, eb.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger, Options: map[string]any{"time_controller": tc}}))
	registry.Register(eb)
	opts := map[string]any{"time_controller": tc}
	if reach {
		opts["registry"] = registry
	}
	tagging := &emulator.TaggingPlugin{}
	require.NoError(t, tagging.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger, Options: opts}))
	registry.Register(tagging)
	return &ebTaggingEnv{t: t, srv: emulator.NewServer(*cfg, registry, store, state, tc, logger)}
}

// call sends one JSON operation and returns its status and body.
func (e *ebTaggingEnv) call(host, target, op string, body any) (int, string) {
	e.t.Helper()
	pe := &providerEnv{t: e.t, srv: e.srv}
	raw, err := json.Marshal(body)
	require.NoError(e.t, err)
	return pe.do(http.MethodPost, host, "/", "application/x-amz-json-1.1", string(raw), map[string]string{"X-Amz-Target": target + "." + op})
}

func (e *ebTaggingEnv) eb(op string, body any) string {
	e.t.Helper()
	status, out := e.call("events.us-east-1.amazonaws.com", "AWSEvents", op, body)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

func (e *ebTaggingEnv) rgta(op string, body any) string {
	e.t.Helper()
	status, out := e.call("tagging.us-east-1.amazonaws.com", "ResourceGroupsTaggingAPI_20170126", op, body)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

// tags reads ListTagsForResource for arn as a map.
func (e *ebTaggingEnv) tags(arn string) map[string]string {
	e.t.Helper()
	var out struct{ Tags []struct{ Key, Value string } }
	require.NoError(e.t, json.Unmarshal([]byte(e.eb("ListTagsForResource", map[string]string{"ResourceARN": arn})), &out))
	m := map[string]string{}
	for _, t := range out.Tags {
		m[t.Key] = t.Value
	}
	return m
}

// reported returns GetResources' tags by ARN for the events service.
func (e *ebTaggingEnv) reported() map[string]map[string]string {
	e.t.Helper()
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string
			Tags        []struct{ Key, Value string }
		}
	}
	require.NoError(e.t, json.Unmarshal([]byte(e.rgta("GetResources", map[string]any{"ResourceTypeFilters": []string{"events"}})), &out))
	got := map[string]map[string]string{}
	for _, m := range out.ResourceTagMappingList {
		tags := map[string]string{}
		for _, t := range m.Tags {
			tags[t.Key] = t.Value
		}
		got[m.ResourceARN] = tags
	}
	return got
}

const (
	ebAcct          = "123456789012"
	ebEnvRuleARN    = "arn:aws:events:us-east-1:" + ebAcct + ":rule/orders/r"
	ebEnvBusARN     = "arn:aws:events:us-east-1:" + ebAcct + ":event-bus/orders"
	ebEnvDefaultARN = "arn:aws:events:us-east-1:" + ebAcct + ":event-bus/default"
)

func TestEBTaggingAPI_TagsWrittenByEitherAPIAreReadByTheOther(t *testing.T) {
	for _, reach := range []bool{true, false} {
		t.Run(fmt.Sprintf("registry=%v", reach), func(t *testing.T) {
			e := newEBTaggingEnv(t, reach)
			e.eb("CreateEventBus", map[string]any{"Name": "orders"})
			e.eb("PutRule", map[string]any{"Name": "r", "EventBusName": "orders", "EventPattern": `{"source":["x"]}`})
			assert.Empty(t, e.reported(), "nothing has been tagged")

			out := e.rgta("TagResources", map[string]any{
				"ResourceARNList": []string{ebEnvRuleARN, ebEnvBusARN, ebEnvDefaultARN},
				"Tags":            map[string]string{"team": "events"},
			})
			assert.NotContains(t, out, "FailedResourcesMap", out)
			for _, arn := range []string{ebEnvRuleARN, ebEnvBusARN, ebEnvDefaultARN} {
				assert.Equal(t, map[string]string{"team": "events"}, e.tags(arn), arn)
			}

			e.eb("TagResource", map[string]any{"ResourceARN": ebEnvBusARN, "Tags": ebTagList("env", "prod")})
			e.rgta("UntagResources", map[string]any{"ResourceARNList": []string{ebEnvRuleARN}, "TagKeys": []string{"team"}})
			assert.Equal(t, map[string]map[string]string{
				ebEnvRuleARN:    {},
				ebEnvBusARN:     {"env": "prod", "team": "events"},
				ebEnvDefaultARN: {"team": "events"},
			}, e.reported(), "a previously tagged rule is still reported (#938)")
		})
	}
}

func TestEBTaggingAPI_RefusalsAreTheOwnersAnswer(t *testing.T) {
	for _, reach := range []bool{true, false} {
		t.Run(fmt.Sprintf("registry=%v", reach), func(t *testing.T) {
			e := newEBTaggingEnv(t, reach)
			e.eb("CreateEventBus", map[string]any{"Name": "orders"})
			fifty := map[string]string{}
			for i := range 51 {
				fifty[fmt.Sprintf("k%02d", i)] = "v"
			}
			absent := "arn:aws:events:us-east-1:" + ebAcct + ":rule/absent"
			for _, tc := range []struct {
				name, arn string
				tags      map[string]string
				code      string
			}{
				{"an aws: key", ebEnvBusARN, map[string]string{"aws:x": "v"}, "InvalidParameterException"},
				{"more than fifty tags", ebEnvBusARN, fifty, "LimitExceededException"},
				{"an absent rule", absent, map[string]string{"k": "v"}, "InvalidParameterException"},
				{"an untaggable type", "arn:aws:events:us-east-1:" + ebAcct + ":archive/a", map[string]string{"k": "v"}, "InternalServiceException"},
				{"another account's default bus", "arn:aws:events:us-east-1:210987654321:event-bus/default", map[string]string{"k": "v"}, "InvalidParameterException"},
			} {
				out := e.rgta("TagResources", map[string]any{"ResourceARNList": []string{tc.arn}, "Tags": tc.tags})
				code, _ := providerFailure(t, out, tc.arn)
				assert.Equal(t, tc.code, code, "%s: %s", tc.name, out)
			}
			if reach {
				// The provider knows the caller's Region; the direct path is handed only its account.
				west := "arn:aws:events:us-west-2:" + ebAcct + ":event-bus/orders"
				out := e.rgta("TagResources", map[string]any{"ResourceARNList": []string{west}, "Tags": map[string]string{"k": "v"}})
				code, _ := providerFailure(t, out, west)
				assert.Equal(t, "InvalidParameterException", code, "another Region's bus: %s", out)
			}
			assert.Empty(t, e.tags(ebEnvBusARN), "no refused write stored a tag")
		})
	}
}

func TestEBTaggingAPI_TagWritesDoNotRace(t *testing.T) {
	e := newEBTaggingEnv(t, true)
	e.eb("CreateEventBus", map[string]any{"Name": "orders"})
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			e.rgta("TagResources", map[string]any{"ResourceARNList": []string{ebEnvBusARN}, "Tags": map[string]string{fmt.Sprintf("api%02d", i): "v"}})
		}()
		go func() {
			defer wg.Done()
			e.eb("TagResource", map[string]any{"ResourceARN": ebEnvBusARN, "Tags": ebTagList(fmt.Sprintf("own%02d", i), "v")})
		}()
	}
	wg.Wait()
	assert.Len(t, e.tags(ebEnvBusARN), 40, "no write was lost")
}
