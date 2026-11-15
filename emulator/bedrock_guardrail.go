package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"regexp"
	"strings"
)

// A guardrail's decision, shared by ApplyGuardrail and InvokeModel (#1183).
//
// Substrate does not evaluate a guardrail's policies: content filtering is resource-internal. What it
// models is the observable outcome, an intervention or none, and that outcome is **seeded**: a test
// names the substrings a guardrail blocks with POST /v1/bedrock/guardrail-blocklist, and a request
// whose text contains one of them is intervened on. An unseeded guardrail intervenes on nothing.
//
// The seed is keyed by account, Region, guardrail ID and guardrail version, each of which may be "*",
// and the most specific seed wins: the guardrail's own ID before "*", then its version before "*",
// then the account, then the Region. A guardrail named by ARN is keyed by the ID the ARN ends in.
//
// Until #1183 the blocklist lived at a key that carried no Region, was readable only by a test that
// wrote the StateManager directly, and was created empty by ApplyGuardrail on first use — a state
// write on a read path, whose unmarshal error was then discarded. The seed replaces all three: reads
// write nothing, a decode failure is an error, and the key is Region-scoped.
//
// InvokeModel reads two headers, X-Amzn-Bedrock-GuardrailIdentifier and
// X-Amzn-Bedrock-GuardrailVersion. API_runtime_InvokeModel publishes three request errors for them,
// "An error will be thrown in the following situations":
//
//   - "You don't provide a guardrail identifier but you specify the amazon-bedrock-guardrailConfig
//     field in the request body."
//   - "You enable the guardrail but the contentType isn't application/json."
//   - "You provide a guardrail identifier, but guardrailVersion isn't specified."
//
// Each is ValidationException/400, which the page publishes ("The input fails to satisfy the
// constraints specified by Amazon Bedrock"), as is an identifier or version that does not match its
// published pattern. A version with no identifier is not refused: the page says "If you don't provide
// a value, no guardrail is applied to the invocation", and lists no error for it. The messages are
// substrate's own.
//
// The page's guardrail example response is a body carrying "amazon-bedrock-guardrailAction":
// "INTERVENED | NONE" and an "amazon-bedrock-trace" object of unpublished detail. An intervened
// invocation answers {"amazon-bedrock-guardrailAction": "INTERVENED"} instead of the inference. The
// trace member is not rendered, and X-Amzn-Bedrock-Trace is not read, since its contents are not
// published. A non-intervening invocation answers exactly what it did before, so a recorded run
// replays byte for byte. A seeded InvokeModel response wins over the guardrail outright: a response
// seed is the caller stating what the wire carries, and a guardrail decision must not rewrite it.

// bedrockGuardrailBlocklistSeed is the body of POST /v1/bedrock/guardrail-blocklist.
type bedrockGuardrailBlocklistSeed struct {
	// AccountID scopes the seed; "" or "*" is every account.
	AccountID string `json:"accountId,omitempty"`
	// Region scopes the seed; "" or "*" is every Region.
	Region string `json:"region,omitempty"`
	// GuardrailIdentifier is the guardrail's ID or ARN; "" or "*" is every guardrail.
	GuardrailIdentifier string `json:"guardrailIdentifier,omitempty"`
	// GuardrailVersion is the version; "" or "*" is every version.
	GuardrailVersion string `json:"guardrailVersion,omitempty"`
	// Terms are the substrings the guardrail blocks.
	Terms []string `json:"terms"`
}

// bedrockGuardrailWildcard is the value that matches anything in one seed dimension.
const bedrockGuardrailWildcard = "*"

// bedrockGuardrailBlocklistPrefix prefixes every blocklist seed's key in the control-plane namespace.
const bedrockGuardrailBlocklistPrefix = "guardrail-blocklist:"

// bedrockGuardrailID reduces a guardrail identifier to its ID: an ARN ends in guardrail/{id}.
func bedrockGuardrailID(identifier string) string {
	if i := strings.LastIndex(identifier, ":guardrail/"); i >= 0 {
		return identifier[i+len(":guardrail/"):]
	}
	return identifier
}

// bedrockGuardrailOrWildcard reads an empty seed dimension as the wildcard.
func bedrockGuardrailOrWildcard(v string) string {
	if v == "" {
		return bedrockGuardrailWildcard
	}
	return v
}

// bedrockGuardrailBlocklistKey is a blocklist seed's key.
func bedrockGuardrailBlocklistKey(accountID, region, guardrailID, version string) string {
	return bedrockGuardrailBlocklistPrefix + bedrockGuardrailOrWildcard(accountID) + "/" +
		bedrockGuardrailOrWildcard(region) + "/" + bedrockGuardrailOrWildcard(bedrockGuardrailID(guardrailID)) + "/" +
		bedrockGuardrailOrWildcard(version)
}

// guardrailBlocklist returns the terms the most specific matching seed names, or nil when none does.
func (p *BedrockRuntimePlugin) guardrailBlocklist(goCtx context.Context, accountID, region, guardrailID, version string) ([]string, error) {
	id := bedrockGuardrailID(guardrailID)
	for _, g := range []string{id, bedrockGuardrailWildcard} {
		for _, v := range []string{version, bedrockGuardrailWildcard} {
			for _, a := range []string{accountID, bedrockGuardrailWildcard} {
				for _, r := range []string{region, bedrockGuardrailWildcard} {
					key := bedrockGuardrailBlocklistKey(a, r, g, v)
					data, err := p.state.Get(goCtx, bedrockRuntimeCtrlNamespace, key)
					if err != nil {
						return nil, fmt.Errorf("bedrock guardrail blocklist %s: %w", key, err)
					}
					if data == nil {
						continue
					}
					var seed bedrockGuardrailBlocklistSeed
					if err := json.Unmarshal(data, &seed); err != nil {
						return nil, fmt.Errorf("bedrock guardrail blocklist %s unmarshal: %w", key, err)
					}
					return seed.Terms, nil
				}
			}
		}
	}
	return nil, nil
}

// guardrailIntervenes reports whether the guardrail blocks any of texts: whether a seeded term is a
// substring of one of them. It is the one decision ApplyGuardrail and InvokeModel share.
func (p *BedrockRuntimePlugin) guardrailIntervenes(ctx *RequestContext, guardrailID, version string, texts []string) (bool, error) {
	terms, err := p.guardrailBlocklist(context.Background(), ctx.AccountID, ctx.Region, guardrailID, version)
	if err != nil {
		return false, err
	}
	for _, term := range terms {
		if term == "" {
			continue
		}
		for _, text := range texts {
			if strings.Contains(text, term) {
				return true, nil
			}
		}
	}
	return false, nil
}

// bedrockGuardrailIdentifierPattern and bedrockGuardrailVersionPattern are the patterns
// API_runtime_InvokeModel publishes for the two headers.
var (
	bedrockGuardrailIdentifierPattern = regexp.MustCompile(`^(|([a-z0-9]+)|(arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:guardrail/[a-z0-9]+))$`)
	bedrockGuardrailVersionPattern    = regexp.MustCompile(`^(|([1-9][0-9]{0,7})|(DRAFT))$`)
)

// bedrockInvokeGuardrail reads and checks InvokeModel's guardrail headers, returning the identifier
// and version to apply, or "" when none is named. See the file comment for each refusal.
func bedrockInvokeGuardrail(req *AWSRequest) (identifier, version string, refusal *AWSError) {
	identifier = headerValueFold(req.Headers, "X-Amzn-Bedrock-GuardrailIdentifier")
	version = headerValueFold(req.Headers, "X-Amzn-Bedrock-GuardrailVersion")
	if len(identifier) > 2048 || !bedrockGuardrailIdentifierPattern.MatchString(identifier) {
		return "", "", bedrockValidationError("X-Amzn-Bedrock-GuardrailIdentifier is not a guardrail ID or ARN")
	}
	if !bedrockGuardrailVersionPattern.MatchString(version) {
		return "", "", bedrockValidationError("X-Amzn-Bedrock-GuardrailVersion must be a version number or DRAFT")
	}
	if identifier == "" {
		if bedrockBodyNamesGuardrailConfig(req.Body) {
			return "", "", bedrockValidationError(
				"the request body specifies amazon-bedrock-guardrailConfig but no guardrail identifier is provided")
		}
		return "", "", nil
	}
	if version == "" {
		return "", "", bedrockValidationError("a guardrail identifier is provided but no guardrail version is specified")
	}
	if contentType := headerValueFold(req.Headers, "Content-Type"); contentType != "" {
		if mediaType, _, err := mime.ParseMediaType(contentType); err != nil || mediaType != "application/json" {
			return "", "", bedrockValidationError("a guardrail requires the content type application/json")
		}
	}
	return identifier, version, nil
}

// bedrockBodyNamesGuardrailConfig reports whether a JSON request body has a top-level
// amazon-bedrock-guardrailConfig member. A body that is not a JSON object names nothing.
func bedrockBodyNamesGuardrailConfig(body []byte) bool {
	var doc map[string]json.RawMessage
	if json.Unmarshal(body, &doc) != nil {
		return false
	}
	_, ok := doc["amazon-bedrock-guardrailConfig"]
	return ok
}

// bedrockBodyTexts returns every string value in a JSON request body, at any depth, so a blocklist
// term matches text wherever the model's request format puts it. A body that is not JSON is one text.
func bedrockBodyTexts(body []byte) []string {
	var doc any
	if json.Unmarshal(body, &doc) != nil {
		return []string{string(body)}
	}
	var texts []string
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case string:
			texts = append(texts, t)
		case []any:
			for _, e := range t {
				walk(e)
			}
		case map[string]any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(doc)
	return texts
}

// handleBedrockSeedGuardrailBlocklist handles POST /v1/bedrock/guardrail-blocklist.
func (s *Server) handleBedrockSeedGuardrailBlocklist(w http.ResponseWriter, r *http.Request) {
	var seed bedrockGuardrailBlocklistSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, `{"error":"the body is not a guardrail blocklist seed"}`, http.StatusBadRequest)
		return
	}
	if seed.Terms == nil {
		http.Error(w, `{"error":"terms is required"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := bedrockGuardrailBlocklistKey(seed.AccountID, seed.Region, seed.GuardrailIdentifier, seed.GuardrailVersion)
	if err := s.state.Put(r.Context(), bedrockRuntimeCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "key": strings.TrimPrefix(key, bedrockGuardrailBlocklistPrefix)})
}

// handleBedrockClearGuardrailBlocklist handles DELETE /v1/bedrock/guardrail-blocklist. With any of
// ?accountId, ?region, ?guardrailIdentifier or ?guardrailVersion it removes the one seed those name
// (an omitted one is "*"); with none it removes every blocklist seed.
func (s *Server) handleBedrockClearGuardrailBlocklist(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("accountId") || q.Has("region") || q.Has("guardrailIdentifier") || q.Has("guardrailVersion") {
		key := bedrockGuardrailBlocklistKey(q.Get("accountId"), q.Get("region"), q.Get("guardrailIdentifier"), q.Get("guardrailVersion"))
		if err := s.state.Delete(r.Context(), bedrockRuntimeCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), bedrockRuntimeCtrlNamespace, bedrockGuardrailBlocklistPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), bedrockRuntimeCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
