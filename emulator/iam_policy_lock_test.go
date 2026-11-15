package emulator_test

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyWrites_AreSerialized is review finding 7: every writer of a customer managed policy's
// record holds the one lock, so concurrent version creates cannot exceed the five-version limit and
// a tag write racing a version write loses neither change.
func TestPolicyWrites_AreSerialized(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)

	const writers = 12
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	for i := range writers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			resp := iamRequest(t, srv, "CreatePolicyVersion", map[string]any{
				"PolicyArn": iamVersionedPolicyARN, "PolicyDocument": iamDocAllowing("s3:Action" + strconv.Itoa(i)),
			})
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			resp := iamRequest(t, srv, "TagPolicy", map[string]any{
				"PolicyArn": iamVersionedPolicyARN,
				"Tags":      []map[string]string{{"Key": "k" + strconv.Itoa(i), "Value": "v"}},
			})
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()

	assert.Equal(t, 4, created, "only four versions fit beside v1")
	ids, _ := iamListedVersionIDs(t, srv)
	assert.Len(t, ids, 5)

	var out map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "ListPolicyTags", map[string]any{"PolicyArn": iamVersionedPolicyARN}), &out)
	tags, ok := out["Tags"].([]any)
	require.True(t, ok, "%v", out)
	assert.Len(t, tags, writers, "no tag write was lost to a version write")
}
