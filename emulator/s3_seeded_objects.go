package emulator

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 by definition, not a security use.
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// s3CtrlSeededObjectPrefix is the key prefix for every seeded object, used to
// clear all of them.
const s3CtrlSeededObjectPrefix = "seeded-object:"

// s3CtrlSupersededPrefix is the key prefix for the tombstones
// [S3Plugin.supersedeSeededObject] writes.
const s3CtrlSupersededPrefix = "seeded-object-superseded:"

// s3SeededObjectDefaultContentType is the Content-Type a seed without one is served with,
// the same default PutObject applies.
const s3SeededObjectDefaultContentType = "application/octet-stream"

// s3SeededObject is an object a GET or HEAD observes at a key nothing has written. It
// models an object that something outside the emulated account writes, such as a
// result file a job uploads when it finishes: substrate does not run the writer, so a
// seed sets the object it would have written, and availableAt sets when on the
// simulated clock. Before that instant the key is absent, which is the "not finished
// yet" observation a poll loop waits on.
//
// Keyed by bucket and key, with "*" as a wildcard bucket. A real object at the key
// always wins, and the bucket itself must exist.
type s3SeededObject struct {
	// Bucket the seed applies to, or "*" for any bucket. Empty means "*".
	Bucket string `json:"bucket"`

	// Key is the exact object key.
	Key string `json:"key"`

	// Body is the object body as a JSON value. A JSON string is served as its text;
	// any other value (an object, array, number) is served as its JSON encoding.
	Body json.RawMessage `json:"body,omitempty"`

	// BodyBase64 is the object body as base64, for bytes that are not text. At most
	// one of Body and BodyBase64 may be set; with neither the object is empty.
	BodyBase64 string `json:"bodyBase64,omitempty"`

	// ContentType is the object's Content-Type. Defaults to application/octet-stream.
	ContentType string `json:"contentType,omitempty"`

	// AvailableAt is the RFC 3339 instant on the simulated clock from which the
	// object is served. Empty means immediately.
	AvailableAt string `json:"availableAt,omitempty"`

	// SeededAt is the simulated time the seed was written, the object's
	// Last-Modified when AvailableAt is empty.
	SeededAt time.Time `json:"seededAt"`
}

// s3SeededObjectKey returns the state key for a seeded object.
func s3SeededObjectKey(bucket, key string) string {
	if bucket == "" {
		bucket = "*"
	}
	return s3CtrlSeededObjectPrefix + bucket + "/" + key
}

// bytes returns the seeded object's body.
func (s *s3SeededObject) bytes() ([]byte, error) {
	if s.BodyBase64 != "" {
		b, err := base64.StdEncoding.DecodeString(s.BodyBase64)
		if err != nil {
			return nil, fmt.Errorf("bodyBase64: %w", err)
		}
		return b, nil
	}
	raw := bytes.TrimSpace(s.Body)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return []byte{}, nil
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("body: %w", err)
		}
		return []byte(text), nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("body: %w", err)
	}
	return compact.Bytes(), nil
}

// availableAt parses AvailableAt, returning the zero time when it is empty.
func (s *s3SeededObject) availableAt() (time.Time, error) {
	if s.AvailableAt == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s.AvailableAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("availableAt: %w", err)
	}
	return t, nil
}

// s3SeededObjectSupersededKey returns the state key recording that a write or delete
// of bucket/key superseded the seed that applied to it.
func s3SeededObjectSupersededKey(bucket, key string) string {
	return s3CtrlSupersededPrefix + bucket + "/" + key
}

// availableSeededObject returns the seed a read of bucket/key observes now: the exact-bucket
// seed if there is one, otherwise the "*" seed. It returns nil when neither exists, when the
// one that applies is still ahead of the simulated clock, or when a write or delete of the
// key has superseded it; see [S3Plugin.supersedeSeededObject].
func (p *S3Plugin) availableSeededObject(ctx context.Context, bucket, key string) (*s3SeededObject, time.Time, error) {
	for _, stateKey := range []string{s3SeededObjectKey(bucket, key), s3SeededObjectKey("*", key)} {
		data, err := p.state.Get(ctx, s3CtrlNamespace, stateKey)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("s3 seeded object get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed s3SeededObject
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, time.Time{}, fmt.Errorf("s3 seeded object unmarshal: %w", err)
		}
		at, err := seed.availableAt()
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("s3 seeded object: %w", err)
		}
		if !at.IsZero() && p.tc.Now().Before(at) {
			return nil, time.Time{}, nil
		}
		superseded, err := p.state.Get(ctx, s3CtrlNamespace, s3SeededObjectSupersededKey(bucket, key))
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("s3 seeded object superseded get: %w", err)
		}
		if superseded != nil {
			return nil, time.Time{}, nil
		}
		return &seed, at, nil
	}
	return nil, time.Time{}, nil
}

// resolveSeededObject returns the seeded object for bucket and key; see
// [S3Plugin.availableSeededObject] for which seed applies. The caller has already found
// no real object at the key. It returns handled=false when no seed applies, so the
// caller's NoSuchKey is the answer. A served seed goes through the caller's normal object
// path, so range and conditional reads apply to it as to any other object.
func (p *S3Plugin) resolveSeededObject(ctx context.Context, bucket, key string) (S3Object, []byte, bool, error) {
	seed, at, err := p.availableSeededObject(ctx, bucket, key)
	if err != nil || seed == nil {
		return S3Object{}, nil, false, err
	}
	body, err := seed.bytes()
	if err != nil {
		return S3Object{}, nil, false, fmt.Errorf("s3 resolveSeededObject: %w", err)
	}
	modified := seed.SeededAt
	if !at.IsZero() {
		modified = at
	}
	hash := md5.Sum(body) //nolint:gosec // S3 ETags are MD5 by definition.
	return S3Object{
		Bucket:       bucket,
		Key:          key,
		Size:         int64(len(body)),
		ContentType:  seed.ContentType,
		ETag:         fmt.Sprintf("%q", fmt.Sprintf("%x", hash)),
		LastModified: modified.UTC(),
	}, body, true, nil
}

// supersedeSeededObject records that a write or delete of bucket/key has replaced the
// seed that currently applies to it, so the seed is not served again once the key is
// absent. A seed stands for an object written by something outside the account; once
// that object has been overwritten or deleted, it is gone, as it would be in S3.
//
// Only a seed already available is superseded. One whose availableAt is still ahead
// describes a write that has not happened yet, so a delete before it leaves the seed to
// appear on time. A tombstone rather than a deletion, because the seed may be the "*"
// one, which other buckets still observe. A new seed for the key clears the tombstone.
func (p *S3Plugin) supersedeSeededObject(ctx context.Context, bucket, key string) error {
	seed, _, err := p.availableSeededObject(ctx, bucket, key)
	if err != nil || seed == nil {
		return err
	}
	if err := p.state.Put(ctx, s3CtrlNamespace, s3SeededObjectSupersededKey(bucket, key), []byte("{}")); err != nil {
		return fmt.Errorf("s3 supersede seeded object: %w", err)
	}
	return nil
}

// clearSupersededSeeds removes the tombstones a new or cleared seed for bucket/key makes
// stale: the one for that bucket, or every bucket's for the key when bucket is "*". An
// empty key clears every tombstone.
func (s *Server) clearSupersededSeeds(ctx context.Context, bucket, key string) error {
	keys, err := s.state.List(ctx, s3CtrlNamespace, s3CtrlSupersededPrefix)
	if err != nil {
		return fmt.Errorf("list superseded seeds: %w", err)
	}
	for _, k := range keys {
		b, kk, _ := strings.Cut(strings.TrimPrefix(k, s3CtrlSupersededPrefix), "/")
		if key != "" && (kk != key || (bucket != "*" && b != bucket)) {
			continue
		}
		if err := s.state.Delete(ctx, s3CtrlNamespace, k); err != nil {
			return fmt.Errorf("delete superseded seed: %w", err)
		}
	}
	return nil
}

// handleS3SeedObject handles POST /v1/s3/seeded-objects. It seeds the object a GET
// or HEAD of a key observes when nothing has written one, served from availableAt on
// the simulated clock. Body:
// {"bucket","key","body","bodyBase64","contentType","availableAt"}; an empty bucket
// seeds the "*" wildcard.
func (s *Server) handleS3SeedObject(w http.ResponseWriter, r *http.Request) {
	var seed s3SeededObject
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Key == "" {
		http.Error(w, `{"error":"key is required"}`, http.StatusBadRequest)
		return
	}
	if len(bytes.TrimSpace(seed.Body)) > 0 && seed.BodyBase64 != "" {
		http.Error(w, `{"error":"give body or bodyBase64, not both"}`, http.StatusBadRequest)
		return
	}
	if _, err := seed.bytes(); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if _, err := seed.availableAt(); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Bucket == "" {
		seed.Bucket = "*"
	}
	if seed.ContentType == "" {
		seed.ContentType = s3SeededObjectDefaultContentType
	}
	if s.tc != nil {
		seed.SeededAt = s.tc.Now().UTC()
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), s3CtrlNamespace, s3SeededObjectKey(seed.Bucket, seed.Key), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.clearSupersededSeeds(r.Context(), seed.Bucket, seed.Key); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "bucket": seed.Bucket, "key": seed.Key})
}

// handleS3ClearSeededObjects handles DELETE /v1/s3/seeded-objects. With ?key= it
// removes that seed, in ?bucket= or the "*" wildcard when bucket is omitted; with
// neither it removes all.
func (s *Server) handleS3ClearSeededObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")
	if bucket != "" || key != "" {
		if key == "" {
			http.Error(w, `{"error":"key is required when bucket is given"}`, http.StatusBadRequest)
			return
		}
		if err := s.state.Delete(r.Context(), s3CtrlNamespace, s3SeededObjectKey(bucket, key)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if bucket == "" {
			bucket = "*"
		}
		if err := s.clearSupersededSeeds(r.Context(), bucket, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), s3CtrlNamespace, s3CtrlSeededObjectPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), s3CtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	if err := s.clearSupersededSeeds(r.Context(), "*", ""); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
