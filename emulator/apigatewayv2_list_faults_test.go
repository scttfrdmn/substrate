package emulator_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetApis, GetRoutes, GetIntegrations, GetStages and GetAuthorizers report a record they cannot read
// rather than leaving it out of the page (#1538); only an index entry that outlived its record is
// skipped. GetApiMappings already did.

// apigwv2ListFixture is the update harness's API, route and stage, plus one integration and one
// authorizer, so each of the five listings has one item to read.
func apigwv2ListFixture(t *testing.T, state emulator.StateManager) *apigwv2Update {
	t.Helper()
	h := newAPIGWv2Update(t, state)
	status, body := h.call(http.MethodPost, "/v2/apis/"+h.apiID+"/integrations",
		`{"integrationType":"HTTP_PROXY","integrationUri":"https://example.com","integrationMethod":"GET","payloadFormatVersion":"1.0"}`)
	require.Equal(t, http.StatusCreated, status, "CreateIntegration: %s", body)
	status, body = h.call(http.MethodPost, "/v2/apis/"+h.apiID+"/authorizers",
		`{"name":"jwt","authorizerType":"JWT","identitySource":["$request.header.Authorization"],"jwtConfiguration":{"audience":["a"],"issuer":"https://issuer.example.com"}}`)
	require.Equal(t, http.StatusCreated, status, "CreateAuthorizer: %s", body)
	return h
}

// apigwv2Listings is each listing's path and the state-key prefix of the records it reads.
func apigwv2Listings(apiID string) []struct{ name, path, record string } {
	return []struct{ name, path, record string }{
		{"GetApis", "/v2/apis", "apiv2:"},
		{"GetRoutes", "/v2/apis/" + apiID + "/routes", "routev2:"},
		{"GetIntegrations", "/v2/apis/" + apiID + "/integrations", "integrationv2:"},
		{"GetStages", "/v2/apis/" + apiID + "/stages", "stagev2:"},
		{"GetAuthorizers", "/v2/apis/" + apiID + "/authorizers", "authorizerv2:"},
	}
}

func TestAPIGatewayV2Lists_AnUnreadableRecordIsAnErrorNotAnOmission(t *testing.T) {
	t.Parallel()
	for _, listing := range apigwv2Listings("{api}") {
		for _, fault := range []string{"read", "decode"} {
			t.Run(listing.name+", "+fault, func(t *testing.T) {
				t.Parallel()
				state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
				h := apigwv2ListFixture(t, state)
				path := strings.ReplaceAll(listing.path, "{api}", h.apiID)
				if fault == "read" {
					state.failGet = listing.record
				} else {
					state.corruptGet = listing.record
				}
				if listing.record != "apiv2:" {
					// The listings below an API read the API first; leave that read alone.
					status, _ := h.call(http.MethodGet, "/v2/apis/"+h.apiID, "")
					require.Equal(t, http.StatusOK, status, "the parent API stays readable")
				}
				_, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
					Service: "apigatewayv2", HTTPMethod: http.MethodGet, Path: path,
					Headers: map[string]string{}, Params: map[string]string{},
				})
				require.Error(t, err, "%s must fail on an unreadable record, not leave it out", listing.name)
				var awsErr *emulator.AWSError
				assert.Falsef(t, errors.As(err, &awsErr), "a store fault is an internal error, not a published refusal: %v", err)
			})
		}
	}
}

func TestAPIGatewayV2Lists_AnIndexEntryWithNoRecordIsSkipped(t *testing.T) {
	t.Parallel()
	for _, listing := range apigwv2Listings("{api}") {
		t.Run(listing.name, func(t *testing.T) {
			t.Parallel()
			state := emulator.NewMemoryStateManager()
			h := apigwv2ListFixture(t, state)
			path := strings.ReplaceAll(listing.path, "{api}", h.apiID)
			if listing.record == "apiv2:" {
				// A second API, whose record is then removed under its index entry.
				status, body := h.call(http.MethodPost, "/v2/apis", `{"name":"orphan","protocolType":"HTTP"}`)
				require.Equal(t, http.StatusCreated, status, "%s", body)
			}
			keys, err := state.List(t.Context(), "apigatewayv2", listing.record)
			require.NoError(t, err)
			require.NotEmpty(t, keys)
			orphan := keys[len(keys)-1]
			if listing.record == "apiv2:" {
				for _, k := range keys {
					if !strings.HasSuffix(k, "/"+h.apiID) {
						orphan = k
					}
				}
			}
			require.NoError(t, state.Delete(t.Context(), "apigatewayv2", orphan))

			status, body := h.call(http.MethodGet, path, "")
			require.Equal(t, http.StatusOK, status, "%s: %s", listing.name, body)
			items := h.members(body)["items"]
			assert.NotContains(t, string(items), orphan[strings.LastIndex(orphan, "/")+1:],
				"the record that is gone is left out of %s", listing.name)
		})
	}
}
