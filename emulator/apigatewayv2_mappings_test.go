package emulator_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// These are #566's gate: an API mapping can be listed, read, retargeted and deleted, not only
// created. Assertions name literal wire keys, per #529.

// agwv2Mapping creates a domain (once per name) and a mapping on it, returning the mapping's ID.
func agwv2Mapping(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext,
	domain, apiID, key string) string {
	t.Helper()
	m, _ := agwv2Wire(t, p, ctx, "POST", "/v2/domainnames/"+domain+"/apimappings", map[string]any{
		"apiId": apiID, "stage": "$default", "apiMappingKey": key,
	})
	id, _ := m["apiMappingId"].(string)
	if id == "" {
		t.Fatalf("CreateApiMapping: no apiMappingId on the wire; got %v", m)
	}
	return id
}

// TestAPIGatewayV2Mappings_Lifecycle walks a mapping through every operation.
func TestAPIGatewayV2Mappings_Lifecycle(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "mappings")
	otherAPI := agwv2API(t, p, ctx, "mappings-other")
	agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "m.example.com"})

	first := agwv2Mapping(t, p, ctx, "m.example.com", apiID, "v1")
	second := agwv2Mapping(t, p, ctx, "m.example.com", apiID, "v2")

	list, raw := agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/m.example.com/apimappings", nil)
	agwv2NoInternalFields(t, "GetApiMappings", raw)
	items := agwv2Items(t, "GetApiMappings", list)
	if len(items) != 2 {
		t.Fatalf("GetApiMappings: want 2 items, got %d: %s", len(items), raw)
	}
	keys := map[string]bool{}
	for _, it := range items {
		m, _ := it.(map[string]any)
		requireKeys(t, "GetApiMappings item", m, "apiMappingId", "apiId", "stage", "apiMappingKey")
		k, _ := m["apiMappingKey"].(string)
		keys[k] = true
	}
	if !keys["v1"] || !keys["v2"] {
		t.Errorf("GetApiMappings: want keys v1 and v2, got %v", keys)
	}
	if _, ok := list["nextToken"]; ok {
		t.Error("GetApiMappings: a complete single page carries no nextToken")
	}

	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/m.example.com/apimappings/"+first, nil)
	requireValues(t, "GetApiMapping", got, map[string]any{
		"apiMappingId": first, "apiId": apiID, "stage": "$default", "apiMappingKey": "v1",
	})

	// An update names only what changes: stage is absent and stays $default.
	upd, _ := agwv2Wire(t, p, ctx, "PATCH", "/v2/domainnames/m.example.com/apimappings/"+first, map[string]any{
		"apiId": otherAPI, "apiMappingKey": "v3",
	})
	requireValues(t, "UpdateApiMapping", upd, map[string]any{
		"apiMappingId": first, "apiId": otherAPI, "stage": "$default", "apiMappingKey": "v3",
	})
	got, _ = agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/m.example.com/apimappings/"+first, nil)
	requireValues(t, "GetApiMapping after update", got, map[string]any{"apiId": otherAPI, "apiMappingKey": "v3"})

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE", "/v2/domainnames/m.example.com/apimappings/"+first, nil))
	if err != nil {
		t.Fatalf("DeleteApiMapping: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("DeleteApiMapping: want 204, got %d", resp.StatusCode)
	}

	list, raw = agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/m.example.com/apimappings", nil)
	items = agwv2Items(t, "GetApiMappings after delete", list)
	if len(items) != 1 {
		t.Fatalf("GetApiMappings after delete: want 1 item, got %s", raw)
	}
	if m, _ := items[0].(map[string]any); m["apiMappingId"] != second {
		t.Errorf("GetApiMappings after delete: the remaining mapping is %v, want %s", m, second)
	}
}

// TestAPIGatewayV2Mappings_NotFound covers every 404 the mapping operations publish.
func TestAPIGatewayV2Mappings_NotFound(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "mappings-404")
	agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "n.example.com"})
	id := agwv2Mapping(t, p, ctx, "n.example.com", apiID, "")

	cases := []struct {
		name, method, path string
		body               map[string]any
	}{
		{"get an absent mapping", "GET", "/v2/domainnames/n.example.com/apimappings/absent", nil},
		{"update an absent mapping", "PATCH", "/v2/domainnames/n.example.com/apimappings/absent", map[string]any{"stage": "s"}},
		{"delete an absent mapping", "DELETE", "/v2/domainnames/n.example.com/apimappings/absent", nil},
		{"list an absent domain", "GET", "/v2/domainnames/absent.example.com/apimappings", nil},
		{"get under an absent domain", "GET", "/v2/domainnames/absent.example.com/apimappings/" + id, nil},
		{"create under an absent domain", "POST", "/v2/domainnames/absent.example.com/apimappings", map[string]any{
			"apiId": apiID, "stage": "$default",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.HandleRequest(ctx, apigwv2Request(t, tc.method, tc.path, tc.body))
			var awsErr *emulator.AWSError
			if !errors.As(err, &awsErr) {
				t.Fatalf("want an AWSError, got %v", err)
			}
			if awsErr.Code != "NotFoundException" || awsErr.HTTPStatus != http.StatusNotFound {
				t.Errorf("want 404 NotFoundException, got %d %s", awsErr.HTTPStatus, awsErr.Code)
			}
		})
	}
}
