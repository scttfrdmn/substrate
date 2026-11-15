package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// CodePipeline keeps every version of a pipeline, so GetPipeline can answer the one it is asked for
// (#1160).
//
// API_GetPipeline publishes `version` — "The version number of the pipeline. If you do not specify a
// version, defaults to the current version." Valid Range minimum 1, Required: No — and
// PipelineVersionNotFoundException/400, "The pipeline version was specified in an invalid format or
// cannot be found." The handler decoded `version` and never read it, and UpdatePipeline incremented the
// version without keeping the one it replaced, so a request for version 1 of a pipeline updated three
// times answered version 4 with 200.
//
// Every write of a pipeline — CreatePipeline's version 1 and each UpdatePipeline's next version — now
// also stores that version's snapshot at `pipeline_version:{account}/{region}/{name}/{version}`, beside
// the current record at `pipeline:…`, which keeps its shape. GetPipeline with no version reads the
// current record as before; with a version, it reads that snapshot, and one that does not exist — or is
// below the published minimum of 1, which is the "invalid format" half of the gloss — is
// PipelineVersionNotFoundException/400. The current version is answered from the current record even
// when it has no snapshot, which is what keeps a pipeline created before this change readable at its own
// version; the versions it went through before this change were never kept, so asking for one of them
// is refused. DeletePipeline removes every snapshot with the pipeline, so a pipeline re-created under
// the same name does not inherit its predecessor's history.
//
// The metadata answered for an older version is that version's: `updated` is when that version was
// written, and `created` and `pipelineArn` are the pipeline's, which no update changes.

// codepipelineVersionKey is the state key one stored version of a pipeline lives at.
func codepipelineVersionKey(acct, region, name string, version int) string {
	return "pipeline_version:" + acct + "/" + region + "/" + name + "/" + strconv.Itoa(version)
}

// putPipelineVersion stores pl as the snapshot of its own version.
func (p *CodePipelinePlugin) putPipelineVersion(goCtx context.Context, pl CodePipelineState) error {
	data, err := json.Marshal(pl)
	if err != nil {
		return fmt.Errorf("codepipeline putPipelineVersion marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codepipelineNamespace, codepipelineVersionKey(pl.AccountID, pl.Region, pl.Name, pl.Version), data); err != nil {
		return fmt.Errorf("codepipeline putPipelineVersion put: %w", err)
	}
	return nil
}

// loadPipelineVersion returns the pipeline as it stood at version, given the current record.
func (p *CodePipelinePlugin) loadPipelineVersion(goCtx context.Context, current *CodePipelineState, version int) (*CodePipelineState, error) {
	if version == current.Version {
		return current, nil
	}
	if version < 1 || version > current.Version {
		return nil, codepipelineVersionNotFound(current.Name, version)
	}
	data, err := p.state.Get(goCtx, codepipelineNamespace, codepipelineVersionKey(current.AccountID, current.Region, current.Name, version))
	if err != nil {
		return nil, fmt.Errorf("codepipeline loadPipelineVersion get: %w", err)
	}
	if data == nil {
		return nil, codepipelineVersionNotFound(current.Name, version)
	}
	var pl CodePipelineState
	if err := json.Unmarshal(data, &pl); err != nil {
		return nil, fmt.Errorf("codepipeline loadPipelineVersion unmarshal: %w", err)
	}
	return &pl, nil
}

// deletePipelineVersions removes every stored version of a pipeline up to its current one.
func (p *CodePipelinePlugin) deletePipelineVersions(goCtx context.Context, pl *CodePipelineState) error {
	for v := 1; v <= pl.Version; v++ {
		if err := p.state.Delete(goCtx, codepipelineNamespace, codepipelineVersionKey(pl.AccountID, pl.Region, pl.Name, v)); err != nil {
			return fmt.Errorf("codepipeline deletePipelineVersions: %w", err)
		}
	}
	return nil
}

// codepipelineVersionNotFound refuses a GetPipeline version that does not exist. The code and status are
// API_GetPipeline's; the message is substrate's.
func codepipelineVersionNotFound(name string, version int) *AWSError {
	return &AWSError{
		Code:       "PipelineVersionNotFoundException",
		Message:    fmt.Sprintf("Pipeline %s has no version %d.", name, version),
		HTTPStatus: http.StatusBadRequest,
	}
}

// codepipelineMissingMember refuses a request that omits a member its page marks Required: Yes.
//
// ValidationException/400, which API_GetPipelineExecution publishes on its own page ("The validation was
// specified in an invalid format.") and which [codepipelineInvalidBody] already answers for every routed
// operation. The message, naming the member, is substrate's.
func codepipelineMissingMember(member string) *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    member + " is a required parameter",
		HTTPStatus: http.StatusBadRequest,
	}
}
