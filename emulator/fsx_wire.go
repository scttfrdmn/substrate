package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"time"
)

// FSx's published shapes for a deleted file system and for a file system's creation time, and
// the ClientRequestToken idempotency CreateFileSystem and DeleteFileSystem publish.
//
// # The delete response is flat (#1210)
//
// API_DeleteFileSystem's Response Syntax is a flat object: FileSystemId, Lifecycle, and one of
// LustreResponse, OpenZFSResponse or WindowsResponse. Substrate answered {"FileSystem": {…}}, the
// CreateFileSystem shape, so an SDK decoding the delete read neither member. The issue filed it as
// LifecycleStatus; the page names it Lifecycle, and the page is what this follows. Its value is
// DELETING: "If the DeleteFileSystem operation is successful, this status is DELETING". Substrate
// answered DELETED, which the Valid Values list does not contain.
//
// The <Type>Response object carries FinalBackupId and FinalBackupTags. Substrate models no backup,
// so FinalBackupId is never answered. WindowsResponse is answered for a WINDOWS file system, the one
// type whose delete takes a final backup by default; LustreResponse and OpenZFSResponse are answered
// when the request carried that type's configuration. Each echoes the FinalBackupTags it was sent.
//
// # A deleted file system is removed in the request that deletes it
//
// The page: the delete "returns while the file system has the DELETING status", and DescribeFileSystems
// on a deleted ID "returns a FileSystemNotFound error". Substrate removes the record at once, so the
// next DescribeFileSystems answers FileSystemNotFound and an SDK's deletion waiter completes on its
// first poll. Making DELETING observable to DescribeFileSystems for a seeded number of observations is
// the progression #1196 owns for every service that reaches a terminal state at birth, FSx's CREATING
// included, so it is not duplicated here.
//
// # CreationTime keeps its fraction (#1373)
//
// API_FileSystem types CreationTime as Timestamp: epoch seconds with fractional precision under
// FSx's awsJson1_1 protocol. The record stored float64(Unix()), whole seconds. It now stores the
// fraction in the same float64 field, so a record written before the fix decodes unchanged, and
// fsxToWire renders it through EpochSeconds to three decimals.

// fsxDeleteFileSystemOut is DeleteFileSystem's response.
type fsxDeleteFileSystemOut struct {
	FileSystemID    string             `json:"FileSystemId"`
	Lifecycle       string             `json:"Lifecycle"`
	LustreResponse  *fsxFinalBackupOut `json:"LustreResponse,omitempty"`
	OpenZFSResponse *fsxFinalBackupOut `json:"OpenZFSResponse,omitempty"`
	WindowsResponse *fsxFinalBackupOut `json:"WindowsResponse,omitempty"`
}

// fsxFinalBackupOut is the shape DeleteFileSystemLustreResponse, DeleteFileSystemOpenZFSResponse and
// DeleteFileSystemWindowsResponse share. FinalBackupId is published; substrate takes no backup, so it
// is never set.
type fsxFinalBackupOut struct {
	FinalBackupTags []FSxTag `json:"FinalBackupTags,omitempty"`
}

// fsxDeleteConfig is the per-type configuration a DeleteFileSystem request may carry.
type fsxDeleteConfig struct {
	FinalBackupTags []FSxTag `json:"FinalBackupTags"`
}

// fsxDeleteToWire builds DeleteFileSystem's response for fs.
func fsxDeleteToWire(fs FSxFileSystem, lustre, openZFS, windows *fsxDeleteConfig) fsxDeleteFileSystemOut {
	out := fsxDeleteFileSystemOut{FileSystemID: fs.FileSystemID, Lifecycle: fsxLifecycleDeleting}
	backup := func(c *fsxDeleteConfig) *fsxFinalBackupOut {
		if c == nil {
			return &fsxFinalBackupOut{}
		}
		return &fsxFinalBackupOut{FinalBackupTags: c.FinalBackupTags}
	}
	switch fs.FileSystemType {
	case "WINDOWS":
		out.WindowsResponse = backup(windows)
	case "LUSTRE":
		if lustre != nil {
			out.LustreResponse = backup(lustre)
		}
	case "OPENZFS":
		if openZFS != nil {
			out.OpenZFSResponse = backup(openZFS)
		}
	}
	return out
}

// Lifecycle values substrate writes or reads.
const (
	// fsxLifecycleDeleting is the value API_DeleteFileSystem publishes for a successful delete.
	fsxLifecycleDeleting = "DELETING"
	// fsxLifecycleLegacyDeleted is the off-enum value substrate stored before #1210. No record is
	// written with it any more; DescribeFileSystems still treats one as absent, so a recording made
	// before the fix reads back as it did.
	fsxLifecycleLegacyDeleted = "DELETED"
)

// fsxEpochNow returns the simulated clock as fractional epoch seconds, the form the record stores.
func fsxEpochNow(tc *TimeController) float64 {
	return float64(tc.Now().UnixNano()) / 1e9
}

// fsxCreationTime renders a stored creation time as the published Timestamp.
func fsxCreationTime(stored float64) EpochSeconds {
	return EpochSeconds(time.Unix(0, int64(math.Round(stored*1e9))).UTC())
}

// fsxClientRequestTokenPattern is the pattern both pages publish for ClientRequestToken, with its
// published length of 1 to 63. The class is `A-z`, as published, not `A-Z`.
var fsxClientRequestTokenPattern = regexp.MustCompile(`^[A-za-z0-9_.-]{1,63}$`)

// fsxTokenRecord is what a ClientRequestToken resolves to: the file system its request addressed,
// and a fingerprint of every other member of that request.
//
// Response is set for DeleteFileSystem, whose first answer is replayed to a retry: the file system
// it named is gone by then, so the answer cannot be rebuilt from state.
type fsxTokenRecord struct {
	FileSystemID string          `json:"file_system_id"`
	Fingerprint  string          `json:"fingerprint"`
	Response     json.RawMessage `json:"response,omitempty"`
}

// fsxTokenKey is the state key a token is recorded under, scoped by account, Region and operation,
// because one token on CreateFileSystem and DeleteFileSystem names two different requests.
func fsxTokenKey(accountID, region, op, token string) string {
	return "fs_token:" + accountID + "/" + region + "/" + op + "/" + token
}

// fsxRequestFingerprint hashes a request's members other than ClientRequestToken, so a retry and
// an incompatible reuse of one token can be told apart. The body is decoded and re-encoded, which
// sorts its keys, so two encodings of one request agree.
func fsxRequestFingerprint(body []byte) (string, error) {
	members := map[string]any{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &members); err != nil {
			return "", fsxInvalidBody()
		}
	}
	delete(members, "ClientRequestToken")
	canonical, err := json.Marshal(members)
	if err != nil {
		return "", fmt.Errorf("fsx request fingerprint marshal: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// fsxCheckToken validates token, and reports the request it was first used for. found is false when
// no request has used it. A token used before with a different fingerprint is refused with
// IncompatibleParameterError, which both pages publish for that case.
func (p *FSxPlugin) fsxCheckToken(ctx *RequestContext, op, token, fingerprint string) (rec fsxTokenRecord, found bool, err error) {
	if !fsxClientRequestTokenPattern.MatchString(token) {
		return rec, false, fsxBadRequest("ClientRequestToken must be 1 to 63 characters matching [A-za-z0-9_.-]")
	}
	data, getErr := p.state.Get(context.Background(), fsxNamespace, fsxTokenKey(ctx.AccountID, ctx.Region, op, token))
	if getErr != nil {
		return rec, false, fmt.Errorf("fsx %s token get: %w", op, getErr)
	}
	if data == nil {
		return rec, false, nil
	}
	if unmarshalErr := json.Unmarshal(data, &rec); unmarshalErr != nil {
		return rec, false, fmt.Errorf("fsx %s token unmarshal: %w", op, unmarshalErr)
	}
	if rec.Fingerprint != fingerprint {
		return rec, false, &AWSError{
			Code:       "IncompatibleParameterError",
			Message:    "The ClientRequestToken provided was already used with a different set of parameters.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return rec, true, nil
}

// fsxRecordToken records token as having been used for the request fingerprint names.
func (p *FSxPlugin) fsxRecordToken(ctx *RequestContext, op, token string, rec fsxTokenRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("fsx %s token marshal: %w", op, err)
	}
	if err := p.state.Put(context.Background(), fsxNamespace, fsxTokenKey(ctx.AccountID, ctx.Region, op, token), data); err != nil {
		return fmt.Errorf("fsx %s token put: %w", op, err)
	}
	return nil
}
