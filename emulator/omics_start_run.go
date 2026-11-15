package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// StartRun, as API_StartRun publishes it (#1166).
//
// # The request
//
// The page marks exactly three request members Required: Yes — outputUri, requestId and roleArn — and
// publishes ValidationException/400 for "The input fails to satisfy the constraints specified by an
// AWS service". Until #1166 none was checked, so a StartRun with an empty object for a body answered
// 201. A request missing any of the three, or naming a roleArn outside the published pattern
// `arn:.+`, is now refused. The two enum members substrate reads — networkingMode
// (RESTRICTED | VPC) and outputUriPrefixMode (SERVICE_DEFINED | NONE_WITH_OVERWRITE) — are refused
// outside their published values. The messages are substrate's own: the page publishes the code and
// its description, not the text a refusal carries.
//
// requestId is Required: Yes on the wire but an idempotency token in the API model, so every AWS SDK
// fills it in when the caller does not; only a hand-built request reaches the refusal.
//
// # requestId deduplication
//
// "If you provide request IDs, AWS HealthOmics identifies duplicate requests and starts the run
// only once." A StartRun whose requestId names a run already started in the account and Region
// answers 201 with that run, as its own StartRun did, and starts nothing. The page publishes neither
// the token's lifetime nor what a duplicate carrying a different body gets; substrate's reading is:
//
//   - **the token lives as long as the run.** DeleteRun releases it, as deleting a CloudFront
//     distribution releases its CallerReference (#1116), because the record that holds it is gone;
//   - **the first run is answered whatever the second body says.** The page describes the duplicate as
//     "the run only once", not as a conflict, and it publishes no code for a mismatched retry.
//
// The lookup is one read of an index entry keyed by account, Region and requestId
// ([omicsRequestIDKey]) naming the run it started, written by StartRun and removed by DeleteRun. The
// token itself still lives on the run record, and an entry is honored only when the run it names
// exists and carries the token, so an entry a half-finished delete left behind releases itself. A
// run written before the index existed is not in it, which reaches only state restored from an
// earlier substrate's snapshot.
//
// # The response
//
// The 201 publishes eight members. Seven are answered: arn, id, networkingMode, runOutputUri,
// status, tags (when the request sent any) and uuid. **configuration is omitted**: it is a
// ConfigurationDetails (arn, name, uuid) resolved from the request's configurationName, and substrate
// models no HealthOmics configuration resource to resolve one against, so it would be invented (#1569).

// omicsRoleArnMatches reports whether roleArn matches its published pattern, `arn:.+`: the prefix
// and at least one character after it.
func omicsRoleArnMatches(roleArn string) bool {
	return strings.HasPrefix(roleArn, "arn:") && len(roleArn) > len("arn:")
}

// omicsStartRunBody is the part of StartRun's request substrate reads.
type omicsStartRunBody struct {
	WorkflowID          string            `json:"workflowId"`
	WorkflowType        string            `json:"workflowType"`
	Name                string            `json:"name"`
	RoleArn             string            `json:"roleArn"`
	OutputURI           string            `json:"outputUri"`
	RequestID           string            `json:"requestId"`
	Tags                map[string]string `json:"tags"`
	NetworkingMode      string            `json:"networkingMode"`
	OutputURIPrefixMode string            `json:"outputUriPrefixMode"`
	Parameters          json.RawMessage   `json:"parameters"`
}

// omicsValidation is a ValidationException/400 refusal.
func omicsValidation(message string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// validate refuses a StartRun request API_StartRun's constraints refuse.
func (b *omicsStartRunBody) validate() error {
	for _, m := range []struct{ name, value string }{
		{"outputUri", b.OutputURI}, {"requestId", b.RequestID}, {"roleArn", b.RoleArn},
	} {
		if m.value == "" {
			return omicsValidation(m.name + " is required")
		}
	}
	if !omicsRoleArnMatches(b.RoleArn) {
		return omicsValidation("roleArn must match the pattern arn:.+")
	}
	switch b.NetworkingMode {
	case "", "RESTRICTED", "VPC":
	default:
		return omicsValidation("networkingMode must be one of RESTRICTED, VPC")
	}
	switch b.OutputURIPrefixMode {
	case "", "SERVICE_DEFINED", "NONE_WITH_OVERWRITE":
	default:
		return omicsValidation("outputUriPrefixMode must be one of SERVICE_DEFINED, NONE_WITH_OVERWRITE")
	}
	return nil
}

// omicsStartRunOut is StartRun's 201 body: seven of its eight published members (see the file
// comment for configuration).
type omicsStartRunOut struct {
	Arn            string            `json:"arn"`
	ID             string            `json:"id"`
	NetworkingMode string            `json:"networkingMode,omitempty"`
	RunOutputURI   string            `json:"runOutputUri,omitempty"`
	Status         string            `json:"status"`
	Tags           map[string]string `json:"tags,omitempty"`
	UUID           string            `json:"uuid,omitempty"`
}

// omicsRunARN renders a run's ARN in the form the Service Authorization Reference publishes for the
// run resource type: arn:${Partition}:omics:${Region}:${Account}:run/${Id}.
func omicsRunARN(r OmicsRun) string {
	return "arn:" + cfnPartition(r.Region) + ":omics:" + r.Region + ":" + r.AccountID + ":run/" + r.ID
}

// omicsRunOutputURI is runOutputUri, "the destination for workflow outputs".
//
// The HealthOmics user guide ("HealthOmics run outputs") publishes the layout: under the default
// SERVICE_DEFINED mode the outputs go "under a prefix named after the run ID", and under
// NONE_WITH_OVERWRITE "directly to the output location". It also says a trailing slash on outputUri
// is insignificant. WDL and CWL runs nest one level further, under an `out` prefix; substrate models
// no workflow and so knows no engine, and answers the run-ID prefix both engines share. A record
// written before #1166 has no prefix mode and answers the default.
func omicsRunOutputURI(r OmicsRun) string {
	if r.OutputURI == "" {
		return ""
	}
	if r.OutputURIPrefixMode == "NONE_WITH_OVERWRITE" {
		return r.OutputURI
	}
	return strings.TrimSuffix(r.OutputURI, "/") + "/" + r.ID
}

// startRun handles StartRun: POST /run. See the file comment.
func (p *OmicsPlugin) startRun(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body omicsStartRunBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, omicsValidation("invalid request body")
	}
	if err := body.validate(); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	p.startMu.Lock()
	defer p.startMu.Unlock()
	prior, err := p.runByRequestID(ctx, body.RequestID)
	if err != nil {
		return nil, err
	}
	if prior != nil {
		return p.startRunResponse(goCtx, *prior)
	}

	networkingMode := body.NetworkingMode
	if networkingMode == "" {
		networkingMode = "RESTRICTED" // API_StartRun: "If not specified, this will default to RESTRICTED."
	}
	run := OmicsRun{
		ID:                  generateOmicsRunID(ctx.IDs),
		Status:              "COMPLETED",
		WorkflowID:          body.WorkflowID,
		WorkflowType:        body.WorkflowType,
		Name:                body.Name,
		RoleArn:             body.RoleArn,
		OutputURI:           body.OutputURI,
		RequestID:           body.RequestID,
		Tags:                body.Tags,
		NetworkingMode:      networkingMode,
		OutputURIPrefixMode: body.OutputURIPrefixMode,
		AccountID:           ctx.AccountID,
		Region:              ctx.Region,
	}
	// Minted after the ID, from the same request's mint, so a replayed StartRun reproduces both.
	run.UUID = ctx.IDs.UUID()

	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + run.ID
	data, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("startRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, omicsNamespace, runKey, data); err != nil {
		return nil, fmt.Errorf("startRun: put: %w", err)
	}
	idsKey := "run_ids:" + ctx.AccountID + "/" + ctx.Region
	if err := updateStringIndex(goCtx, p.state, omicsNamespace, idsKey, run.ID); err != nil {
		return nil, fmt.Errorf("omics startRun index: %w", err)
	}
	if err := p.putRequestID(goCtx, run); err != nil {
		return nil, err
	}
	return p.startRunResponse(goCtx, run)
}

// startRunResponse answers run as StartRun's 201. The status is what the run's first observation
// would report, peeked rather than observed, so a "*" seed's countdown is not spent by the start —
// nor by a deduplicated retry of it.
func (p *OmicsPlugin) startRunResponse(goCtx context.Context, run OmicsRun) (*AWSResponse, error) {
	obs, err := p.peekRun(goCtx, run)
	if err != nil {
		return nil, fmt.Errorf("startRun: status: %w", err)
	}
	return omicsJSONResponse(http.StatusCreated, omicsStartRunOut{
		Arn:            omicsRunARN(run),
		ID:             run.ID,
		NetworkingMode: run.NetworkingMode,
		RunOutputURI:   omicsRunOutputURI(run),
		Status:         obs.status,
		Tags:           run.Tags,
		UUID:           run.UUID,
	})
}

// omicsRequestIDKey is the state key of a requestId's index entry, whose value is the ID of the run
// it started. "run_reqid:" is not a prefix of "run:", so a scan of run records does not see it.
func omicsRequestIDKey(accountID, region, requestID string) string {
	return "run_reqid:" + accountID + "/" + region + "/" + requestID
}

// runByRequestID returns the caller's run started with requestID, or nil when none was. The caller
// holds startMu.
//
// A store fault is an error: it says nothing about whether the token is held. An entry naming a run
// that is gone, or whose record does not decode or carries another token, is treated as free — a
// bad record is logged, not allowed to refuse every StartRun in the Region.
func (p *OmicsPlugin) runByRequestID(ctx *RequestContext, requestID string) (*OmicsRun, error) {
	data, err := p.state.Get(context.Background(), omicsNamespace, omicsRequestIDKey(ctx.AccountID, ctx.Region, requestID))
	if err != nil {
		return nil, fmt.Errorf("startRun: requestId index: %w", err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) = "no run was started with this requestId".
	}
	var runID string
	if err := json.Unmarshal(data, &runID); err != nil {
		p.warn("omics: requestId index entry does not decode; treating the token as free", "requestId", requestID, "err", err)
		return nil, nil //nolint:nilnil // as above.
	}
	run, _, err := p.loadRun(ctx, runID)
	var awsErr *AWSError
	if errors.As(err, &awsErr) {
		return nil, nil //nolint:nilnil // the run is gone; a half-finished delete left the entry.
	}
	if err != nil {
		p.warn("omics: run record does not decode; treating its requestId as free", "run", runID, "err", err)
		return nil, nil //nolint:nilnil // as above.
	}
	if run.RequestID != requestID {
		return nil, nil //nolint:nilnil // as above.
	}
	return &run, nil
}

// putRequestID records that runID was started with requestID.
func (p *OmicsPlugin) putRequestID(goCtx context.Context, r OmicsRun) error {
	data, err := json.Marshal(r.ID)
	if err != nil {
		return fmt.Errorf("startRun: requestId index marshal: %w", err)
	}
	if err := p.state.Put(goCtx, omicsNamespace, omicsRequestIDKey(r.AccountID, r.Region, r.RequestID), data); err != nil {
		return fmt.Errorf("startRun: requestId index put: %w", err)
	}
	return nil
}

// releaseRequestID removes a run's requestId index entry, when it names that run.
func (p *OmicsPlugin) releaseRequestID(goCtx context.Context, r OmicsRun) error {
	if r.RequestID == "" {
		return nil
	}
	key := omicsRequestIDKey(r.AccountID, r.Region, r.RequestID)
	data, err := p.state.Get(goCtx, omicsNamespace, key)
	if err != nil {
		return fmt.Errorf("deleteRun: requestId index read: %w", err)
	}
	if data == nil {
		return nil
	}
	var holder string
	if err := json.Unmarshal(data, &holder); err != nil {
		// An entry that does not decode names no run, this one included, and runByRequestID already
		// treats it as free; it is not this delete's to remove.
		return nil //nolint:nilerr // see above.
	}
	if holder != r.ID {
		return nil
	}
	if err := p.state.Delete(goCtx, omicsNamespace, key); err != nil {
		return fmt.Errorf("deleteRun: requestId index delete: %w", err)
	}
	return nil
}

// warn logs through the plugin's logger, which a hand-built plugin may not have.
func (p *OmicsPlugin) warn(msg string, args ...any) {
	if p.logger != nil {
		p.logger.Warn(msg, args...)
	}
}

// generateOmicsRunID mints a run ID: ten decimal digits with no leading zero, the shape substrate
// has always answered, inside the published `[0-9]+` of at most 18 characters.
//
// It draws from the request's [IDMint] (#1166), so a replayed StartRun mints the ID its recording
// minted in any process. Until #1166 it drew from a math/rand source seeded per process from the
// wall clock, which a reset could rewind within one process (#886) but which two processes could not
// agree on — the gap ids.go names as #856's.
func generateOmicsRunID(m *IDMint) string {
	return m.Chars(1, "123456789") + m.Digits(9)
}
