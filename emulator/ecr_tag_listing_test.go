package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tag immutability and the untagged image (#1379), asserted over the wire.
//
// imageTagMutability was stored and never read, so IMMUTABLE moved a tag like MUTABLE and the
// published ImageTagAlreadyExistsException had no site. Every listing was built from the tag index,
// so an image no tag named was in no listing, and the pages' filter.tagStatus could not be honored.

const (
	ecrListManifestA = `{"schemaVersion":2,"layers":[{"digest":"sha256:a"}]}`
	ecrListManifestB = `{"schemaVersion":2,"layers":[{"digest":"sha256:b"}]}`
	ecrListManifestC = `{"schemaVersion":2,"layers":[{"digest":"sha256:c"}]}`
)

func TestECRImmutability_AnImmutableTagCannotMove(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mutability string
		refused    bool
	}{
		{"MUTABLE", false},
		{"IMMUTABLE", true},
		// No exclusion filter is modeled, so no tag is excluded and the _WITH_EXCLUSION forms read as
		// their base setting.
		{"IMMUTABLE_WITH_EXCLUSION", true},
		{"MUTABLE_WITH_EXCLUSION", false},
		// A repository created with no setting takes the published MUTABLE default.
		{"", false},
	} {
		t.Run("mutability="+tc.mutability, func(t *testing.T) {
			t.Parallel()
			ts := newECRTestServer(t)
			create := map[string]any{"repositoryName": "imm"}
			if tc.mutability != "" {
				create["imageTagMutability"] = tc.mutability
			}
			status, raw := ecrCall(t, ts, "CreateRepository", create)
			require.Equal(t, http.StatusOK, status, "CreateRepository: %s", raw)
			status, raw = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "imm", "imageManifest": ecrListManifestA, "imageTag": "v1"})
			require.Equal(t, http.StatusOK, status, "first push: %s", raw)

			// The same tag on a different manifest would move it.
			status, raw = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "imm", "imageManifest": ecrListManifestB, "imageTag": "v1"})
			if tc.refused {
				require.Equal(t, http.StatusBadRequest, status, "moving v1 on %s: %s", tc.mutability, raw)
				require.Equal(t, "ImageTagAlreadyExistsException", ecrErrorCode(t, raw), "%s", raw)
				// The refused push stored nothing: v1 still names A, and B is not an image.
				_, list := ecrCall(t, ts, "ListImages", map[string]any{"repositoryName": "imm"})
				require.JSONEq(t, `{"imageIds":[{"imageDigest":"`+ecrDigestOf(ecrListManifestA)+`","imageTag":"v1"}]}`, list)
			} else {
				require.Equal(t, http.StatusOK, status, "moving v1 on %s: %s", tc.mutability, raw)
				require.Contains(t, raw, ecrDigestOf(ecrListManifestB), "%s", raw)
			}

			// A new tag on any manifest is never a move, and the same tag on the same manifest is the
			// published ImageAlreadyExistsException whatever the setting.
			status, raw = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "imm", "imageManifest": ecrListManifestC, "imageTag": "v2"})
			require.Equal(t, http.StatusOK, status, "new tag: %s", raw)
			status, raw = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "imm", "imageManifest": ecrListManifestC, "imageTag": "v2"})
			require.Equal(t, http.StatusBadRequest, status, "%s", raw)
			require.Equal(t, "ImageAlreadyExistsException", ecrErrorCode(t, raw), "%s", raw)
		})
	}
}

// An existing image re-pushed under a tag that names another image would move that tag too.
func TestECRImmutability_RepushingAnImageUnderAnotherImagesTagIsRefused(t *testing.T) {
	t.Parallel()
	ts := newECRTestServer(t)
	_, _ = ecrCall(t, ts, "CreateRepository", map[string]any{"repositoryName": "imm", "imageTagMutability": "IMMUTABLE"})
	for _, push := range []map[string]any{
		{"repositoryName": "imm", "imageManifest": ecrListManifestA, "imageTag": "a"},
		{"repositoryName": "imm", "imageManifest": ecrListManifestB, "imageTag": "b"},
	} {
		status, raw := ecrCall(t, ts, "PutImage", push)
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}
	status, raw := ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "imm", "imageManifest": ecrListManifestA, "imageTag": "b"})
	require.Equal(t, http.StatusBadRequest, status, "%s", raw)
	require.Equal(t, "ImageTagAlreadyExistsException", ecrErrorCode(t, raw), "%s", raw)
}

// ecrListingFixture builds a repository holding one image with two tags (A), one untagged image
// pushed without a tag (B), and one left untagged when its only tag moved to another image (C,
// whose tag "moved" now names A).
func ecrListingFixture(t *testing.T) (*httptest.Server, string, string, string) {
	t.Helper()
	ts := newECRTestServer(t)
	_, _ = ecrCall(t, ts, "CreateRepository", map[string]any{"repositoryName": "list"})
	for _, push := range []map[string]any{
		{"repositoryName": "list", "imageManifest": ecrListManifestA, "imageTag": "latest"},
		{"repositoryName": "list", "imageManifest": ecrListManifestA, "imageTag": "v1"},
		{"repositoryName": "list", "imageManifest": ecrListManifestB},
		{"repositoryName": "list", "imageManifest": ecrListManifestC, "imageTag": "moved"},
		{"repositoryName": "list", "imageManifest": ecrListManifestA, "imageTag": "moved"},
	} {
		status, raw := ecrCall(t, ts, "PutImage", push)
		require.Equal(t, http.StatusOK, status, "%v: %s", push, raw)
	}
	return ts, ecrDigestOf(ecrListManifestA), ecrDigestOf(ecrListManifestB), ecrDigestOf(ecrListManifestC)
}

// sortedDigests returns ds in the byte order the listings answer in.
func sortedDigests(ds ...string) []string {
	out := append([]string(nil), ds...)
	sort.Strings(out)
	return out
}

func TestECRListImages_ListsUntaggedImagesAndHonorsTagStatus(t *testing.T) {
	t.Parallel()
	ts, a, b, c := ecrListingFixture(t)

	// Every expected entry, per digest in sorted order: A carries three tags, B and C none.
	entries := func(tagged, untagged bool) []map[string]string {
		var out []map[string]string
		for _, d := range sortedDigests(a, b, c) {
			if d == a {
				if tagged {
					for _, tag := range []string{"latest", "moved", "v1"} {
						out = append(out, map[string]string{"imageDigest": a, "imageTag": tag})
					}
				}
				continue
			}
			if untagged {
				out = append(out, map[string]string{"imageDigest": d})
			}
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		filter map[string]any
		want   []map[string]string
	}{
		{"no filter lists every image, untagged included", nil, entries(true, true)},
		{"ANY", map[string]any{"tagStatus": "ANY"}, entries(true, true)},
		{"TAGGED lists every tag", map[string]any{"tagStatus": "TAGGED"}, entries(true, false)},
		{"UNTAGGED lists the images no tag names", map[string]any{"tagStatus": "UNTAGGED"}, entries(false, true)},
		{"imageStatus ACTIVE is every image", map[string]any{"imageStatus": "ACTIVE"}, entries(true, true)},
		{"imageStatus ARCHIVED is none: nothing is archived", map[string]any{"imageStatus": "ARCHIVED"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"repositoryName": "list"}
			if tc.filter != nil {
				body["filter"] = tc.filter
			}
			status, raw := ecrCall(t, ts, "ListImages", body)
			require.Equal(t, http.StatusOK, status, "%s", raw)
			var out struct {
				ImageIDs []map[string]string `json:"imageIds"`
			}
			require.NoError(t, json.Unmarshal([]byte(raw), &out), "%s", raw)
			if tc.want == nil {
				require.Empty(t, out.ImageIDs, "%s", raw)
				return
			}
			require.Equal(t, tc.want, out.ImageIDs, "%s", raw)
		})
	}
}

func TestECRDescribeImages_DescribesUntaggedImagesAndHonorsTagStatus(t *testing.T) {
	t.Parallel()
	ts, a, b, c := ecrListingFixture(t)
	for _, tc := range []struct {
		name      string
		tagStatus string
		want      []string
	}{
		{"no filter", "", sortedDigests(a, b, c)},
		{"TAGGED", "TAGGED", []string{a}},
		{"UNTAGGED", "UNTAGGED", sortedDigests(b, c)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"repositoryName": "list"}
			if tc.tagStatus != "" {
				body["filter"] = map[string]any{"tagStatus": tc.tagStatus}
			}
			status, raw := ecrCall(t, ts, "DescribeImages", body)
			require.Equal(t, http.StatusOK, status, "%s", raw)
			var out struct {
				ImageDetails []map[string]json.RawMessage `json:"imageDetails"`
			}
			require.NoError(t, json.Unmarshal([]byte(raw), &out), "%s", raw)
			var got []string
			for _, d := range out.ImageDetails {
				var digest string
				require.NoError(t, json.Unmarshal(d["imageDigest"], &digest))
				got = append(got, digest)
				// An untagged image's detail carries no imageTags member; a tagged one carries its tags.
				if digest == a {
					require.JSONEq(t, `["latest","moved","v1"]`, string(d["imageTags"]), "%s", raw)
				} else {
					require.NotContains(t, d, "imageTags", "an untagged image answers no imageTags: %s", raw)
				}
			}
			require.Equal(t, tc.want, got, "%s", raw)
		})
	}
}

func TestECRListings_RefuseAFilterValueOutsideItsEnum(t *testing.T) {
	t.Parallel()
	ts, _, _, _ := ecrListingFixture(t)
	for _, op := range []string{"ListImages", "DescribeImages"} {
		for _, filter := range []map[string]any{{"tagStatus": "tagged"}, {"imageStatus": "DELETED"}} {
			status, raw := ecrCall(t, ts, op, map[string]any{"repositoryName": "list", "filter": filter})
			require.Equalf(t, http.StatusBadRequest, status, "%s %v: %s", op, filter, raw)
			require.Equal(t, "InvalidParameterException", ecrErrorCode(t, raw), "%s", raw)
		}
	}
}

// A repository whose name is another's prefix ("list" and "list/sub") does not borrow its images.
func TestECRListings_ANestedRepositoryIsNotListedUnderItsParent(t *testing.T) {
	t.Parallel()
	ts, a, b, c := ecrListingFixture(t)
	status, raw := ecrCall(t, ts, "CreateRepository", map[string]any{"repositoryName": "list/sub"})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	other := `{"schemaVersion":2,"layers":[{"digest":"sha256:sub"}]}`
	status, raw = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "list/sub", "imageManifest": other})
	require.Equal(t, http.StatusOK, status, "%s", raw)

	_, raw = ecrCall(t, ts, "DescribeImages", map[string]any{"repositoryName": "list"})
	require.NotContains(t, raw, ecrDigestOf(other), "list must not answer list/sub's image: %s", raw)
	for _, d := range []string{a, b, c} {
		require.Contains(t, raw, d, "%s", raw)
	}
}

// ecrListFaultState faults List for prefixes under failList, the one store read the image
// enumeration adds; cfFaultStateManager faults Get, Put and Delete only.
type ecrListFaultState struct {
	emulator.StateManager
	failList string
}

func (m *ecrListFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if m.failList != "" && strings.HasPrefix(prefix, m.failList) {
		return nil, errCFFaultStore
	}
	return m.StateManager.List(ctx, namespace, prefix)
}

// A store fault while enumerating a repository's images, or while reading its mutability, is an
// error, never an empty listing or an unchecked push.
func TestECRListings_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"ListImages", "DescribeImages"} {
		t.Run(op+" enumeration", func(t *testing.T) {
			t.Parallel()
			fault := &ecrListFaultState{StateManager: emulator.NewMemoryStateManager()}
			h := newECRDigestHarness(t, fault)
			h.ok("PutImage", map[string]any{"imageManifest": ecrListManifestA})
			fault.failList = "ecrimage:"
			_, code, err := h.call(op, map[string]any{})
			require.Errorf(t, err, "%s must fail on a List fault, not answer %q", op, code)
		})
	}
	t.Run("PutImage mutability read", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		h := newECRDigestHarness(t, fault)
		h.ok("PutImage", map[string]any{"imageManifest": ecrListManifestA, "imageTag": "t"})
		fault.corruptGet = "ecrrepo:"
		_, code, err := h.call("PutImage", map[string]any{"imageManifest": ecrListManifestB, "imageTag": "t"})
		require.Errorf(t, err, "a corrupt repository record must fail the move check, not answer %q", code)
	})
}
