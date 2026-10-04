package emulator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An ECR image digest is the SHA-256 of the manifest it names (#1283).
//
// Substrate minted a digest unrelated to the manifest, so two pushes of byte-identical manifest bytes
// stored two images, a caller could not verify a push by hashing what it sent, and
// ImageAlreadyExistsException could never fire. The assertions below are on the raw response bytes
// and on each other's digests, never on a decode into substrate's own types.

// ecrDigestOf is the digest ECR calculates for manifest: "sha256:" and its hex SHA-256.
func ecrDigestOf(manifest string) string {
	sum := sha256.Sum256([]byte(manifest))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ecrDigestHarness drives an ECR plugin over a caller's store, so a store fault can be armed.
type ecrDigestHarness struct {
	t   *testing.T
	p   *emulator.ECRPlugin
	ctx *emulator.RequestContext
}

func newECRDigestHarness(t *testing.T, state emulator.StateManager) *ecrDigestHarness {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.ECRPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	h := &ecrDigestHarness{t: t, p: p, ctx: &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ecr-digest", IDs: emulator.NewIDMint("req-ecr-digest"),
	}}
	h.ok("CreateRepository", map[string]any{"repositoryName": "digests"})
	return h
}

// call issues one operation, returning the body on success or the refusal's code.
func (h *ecrDigestHarness) call(op string, body map[string]any) (string, string, error) {
	h.t.Helper()
	if _, ok := body["repositoryName"]; !ok {
		body["repositoryName"] = "digests"
	}
	resp, err := h.p.HandleRequest(h.ctx, ecrRequest(h.t, op, body))
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		require.Equal(h.t, http.StatusBadRequest, awsErr.HTTPStatus, "%s %s: every ECR refusal is a 400", op, awsErr.Code)
		return "", awsErr.Code, nil
	}
	if err != nil {
		return "", "", err
	}
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return string(resp.Body), "", nil
}

func (h *ecrDigestHarness) ok(op string, body map[string]any) string {
	h.t.Helper()
	raw, code, err := h.call(op, body)
	require.NoError(h.t, err, "%s", op)
	require.Empty(h.t, code, "%s was refused", op)
	return raw
}

func (h *ecrDigestHarness) refused(op string, body map[string]any) string {
	h.t.Helper()
	raw, code, err := h.call(op, body)
	require.NoError(h.t, err, "%s", op)
	require.NotEmptyf(h.t, code, "%s was not refused: %s", op, raw)
	return code
}

// imageIDs reads ListImages' imageIds as digest/tag pairs.
func (h *ecrDigestHarness) imageIDs() []map[string]string {
	h.t.Helper()
	var out struct {
		ImageIDs []map[string]string `json:"imageIds"`
	}
	raw := h.ok("ListImages", map[string]any{})
	require.NoError(h.t, json.Unmarshal([]byte(raw), &out), "%s", raw)
	return out.ImageIDs
}

// putDigest pushes and returns the digest the response reports.
func (h *ecrDigestHarness) putDigest(manifest, tag string) string {
	h.t.Helper()
	body := map[string]any{"imageManifest": manifest}
	if tag != "" {
		body["imageTag"] = tag
	}
	var out struct {
		Image struct {
			ImageID struct {
				ImageDigest string `json:"imageDigest"`
			} `json:"imageId"`
		} `json:"image"`
	}
	raw := h.ok("PutImage", body)
	require.NoError(h.t, json.Unmarshal([]byte(raw), &out), "%s", raw)
	return out.Image.ImageID.ImageDigest
}

const ecrDigestManifest = `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`

func TestECRDigest_IsTheSHA256OfTheManifest(t *testing.T) {
	t.Parallel()
	h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
	want := ecrDigestOf(ecrDigestManifest)

	put := h.ok("PutImage", map[string]any{"imageManifest": ecrDigestManifest, "imageTag": "v1"})
	require.Contains(t, put, `"imageDigest":"`+want+`"`, "PutImage must report the manifest's SHA-256: %s", put)

	got := h.ok("BatchGetImage", map[string]any{"imageIds": []map[string]string{{"imageTag": "v1"}}})
	require.Contains(t, got, `"imageDigest":"`+want+`"`, "BatchGetImage: %s", got)
	described := h.ok("DescribeImages", map[string]any{})
	require.Contains(t, described, `"imageDigest":"`+want+`"`, "DescribeImages: %s", described)

	// A different manifest is a different image.
	require.NotEqual(t, want, h.putDigest(`{"schemaVersion":2,"other":true}`, "v2"))
}

func TestECRDigest_AManifestIsRequired(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"absent", map[string]any{"imageTag": "v1"}},
		{"empty", map[string]any{"imageTag": "v1", "imageManifest": ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
			assert.Equal(t, "InvalidParameterException", h.refused("PutImage", tc.body),
				"API_PutImage marks imageManifest Required, minimum length 1")
			assert.Empty(t, h.imageIDs(), "a refused push stores no image")
		})
	}
}

func TestECRDigest_OneManifestUnderTwoTagsIsOneImage(t *testing.T) {
	t.Parallel()
	h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
	first := h.putDigest(ecrDigestManifest, "a")
	second := h.putDigest(ecrDigestManifest, "b")
	require.Equal(t, first, second, "one manifest is one digest")

	ids := h.imageIDs()
	require.Len(t, ids, 2, "ListImages lists the image once per tag: %v", ids)
	for _, id := range ids {
		assert.Equal(t, first, id["imageDigest"], "every entry names the one image")
	}
	assert.Equal(t, "a", ids[0]["imageTag"])
	assert.Equal(t, "b", ids[1]["imageTag"])

	var described struct {
		ImageDetails []struct {
			ImageTags []string `json:"imageTags"`
		} `json:"imageDetails"`
	}
	raw := h.ok("DescribeImages", map[string]any{})
	require.NoError(t, json.Unmarshal([]byte(raw), &described), "%s", raw)
	require.Len(t, described.ImageDetails, 1, "DescribeImages answers one image: %s", raw)
	assert.Equal(t, []string{"a", "b"}, described.ImageDetails[0].ImageTags)

	// BatchGetImage answers the image under the tag it was asked by.
	got := h.ok("BatchGetImage", map[string]any{"imageIds": []map[string]string{{"imageTag": "b"}}})
	assert.Contains(t, got, `"imageTag":"b"`, "%s", got)
}

func TestECRDigest_APushThatChangesNothingIsAlreadyExists(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		first  string
		repeat string
	}{
		{"the same manifest under the same tag", "v1", "v1"},
		{"the same manifest with no tag, after a tagged push", "v1", ""},
		{"the same untagged manifest twice", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
			h.putDigest(ecrDigestManifest, tc.first)
			body := map[string]any{"imageManifest": ecrDigestManifest}
			if tc.repeat != "" {
				body["imageTag"] = tc.repeat
			}
			assert.Equal(t, "ImageAlreadyExistsException", h.refused("PutImage", body))
		})
	}

	// A push that moves a tag to another manifest is a change, not a repeat.
	t.Run("a tag moved to another manifest", func(t *testing.T) {
		t.Parallel()
		h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
		h.putDigest(ecrDigestManifest, "latest")
		moved := h.putDigest(`{"schemaVersion":2,"next":true}`, "latest")
		got := h.ok("BatchGetImage", map[string]any{"imageIds": []map[string]string{{"imageTag": "latest"}}})
		assert.Contains(t, got, `"imageDigest":"`+moved+`"`, "the tag names the newer image: %s", got)
	})
}

func TestECRDigest_ASuppliedDigestMustBeTheManifests(t *testing.T) {
	t.Parallel()
	h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
	assert.Equal(t, "ImageDigestDoesNotMatchException", h.refused("PutImage", map[string]any{
		"imageManifest": ecrDigestManifest, "imageTag": "v1",
		"imageDigest": "sha256:" + "11111111111111111111111111111111" + "11111111111111111111111111111111",
	}))
	assert.Empty(t, h.imageIDs(), "a refused push stores no image")

	put := h.ok("PutImage", map[string]any{
		"imageManifest": ecrDigestManifest, "imageTag": "v1", "imageDigest": ecrDigestOf(ecrDigestManifest),
	})
	assert.Contains(t, put, `"imageDigest":"`+ecrDigestOf(ecrDigestManifest)+`"`, "a matching digest is accepted: %s", put)
}

func TestECRDigest_BatchDeleteImageRemovesATagOrTheWholeImage(t *testing.T) {
	t.Parallel()
	type deleted struct {
		ImageIDs []map[string]string `json:"imageIds"`
		Failures []struct {
			FailureCode string `json:"failureCode"`
		} `json:"failures"`
	}
	del := func(h *ecrDigestHarness, id map[string]string) deleted {
		t.Helper()
		var out deleted
		raw := h.ok("BatchDeleteImage", map[string]any{"imageIds": []map[string]string{id}})
		require.NoError(t, json.Unmarshal([]byte(raw), &out), "%s", raw)
		return out
	}
	// gotImages counts the images BatchGetImage answers for digest. A failure entry names the digest
	// too, so a body match on the digest alone would pass for an image that is gone.
	gotImages := func(h *ecrDigestHarness, digest string) int {
		t.Helper()
		var out struct {
			Images []json.RawMessage `json:"images"`
		}
		raw := h.ok("BatchGetImage", map[string]any{"imageIds": []map[string]string{{"imageDigest": digest}}})
		require.NoError(t, json.Unmarshal([]byte(raw), &out), "%s", raw)
		return len(out.Images)
	}

	t.Run("by tag, the tag goes and the image stays until its last tag", func(t *testing.T) {
		t.Parallel()
		h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
		digest := h.putDigest(ecrDigestManifest, "a")
		h.putDigest(ecrDigestManifest, "b")

		out := del(h, map[string]string{"imageTag": "a"})
		require.Equal(t, []map[string]string{{"imageDigest": digest, "imageTag": "a"}}, out.ImageIDs)
		assert.Equal(t, 1, gotImages(h, digest), "the image survives while tag b names it")

		del(h, map[string]string{"imageTag": "b"})
		assert.Equal(t, 0, gotImages(h, digest), "removing the last tag deletes the image")
	})

	t.Run("by digest, the image and every tag go", func(t *testing.T) {
		t.Parallel()
		h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
		digest := h.putDigest(ecrDigestManifest, "a")
		h.putDigest(ecrDigestManifest, "b")

		out := del(h, map[string]string{"imageDigest": digest})
		assert.Equal(t, []map[string]string{
			{"imageDigest": digest, "imageTag": "a"},
			{"imageDigest": digest, "imageTag": "b"},
		}, out.ImageIDs, "one imageId per tag, as the published sample answers")
		assert.Empty(t, h.imageIDs())
	})

	t.Run("an unknown digest is a failure, not a deletion", func(t *testing.T) {
		t.Parallel()
		h := newECRDigestHarness(t, emulator.NewMemoryStateManager())
		out := del(h, map[string]string{"imageDigest": ecrDigestOf("nothing")})
		assert.Empty(t, out.ImageIDs)
		require.Len(t, out.Failures, 1)
		assert.Equal(t, "ImageNotFoundException", out.Failures[0].FailureCode)
	})
}

// A store fault at any read or write the new paths make is an error, never answered as a push, a
// refusal or a deletion.
func TestECRDigest_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body func(digest string) map[string]any
	}{
		{"PutImage, the existing-image read", func(m *cfFaultStateManager) { m.failGet = "ecrimage:" }, "PutImage",
			func(string) map[string]any { return map[string]any{"imageManifest": `{"n":2}`, "imageTag": "v2"} }},
		{"BatchDeleteImage, the image read", func(m *cfFaultStateManager) { m.failGet = "ecrimage:" }, "BatchDeleteImage",
			func(d string) map[string]any {
				return map[string]any{"imageIds": []map[string]string{{"imageDigest": d}}}
			}},
		{"BatchDeleteImage, the delete", func(m *cfFaultStateManager) { m.failDelete = "ecrimage:" }, "BatchDeleteImage",
			func(d string) map[string]any {
				return map[string]any{"imageIds": []map[string]string{{"imageDigest": d}}}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newECRDigestHarness(t, fault)
			digest := h.putDigest(ecrDigestManifest, "v1")

			tc.arm(fault)
			_, code, err := h.call(tc.op, tc.body(digest))
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			require.Empty(t, code, "%s answered a store fault as the published %s", tc.name, code)
		})
	}
}
