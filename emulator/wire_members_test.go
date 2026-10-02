package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The member walks the raw-bytes assertions in scripts/wire-bookkeeping-projected.txt are made with
// (#756), shared by the services whose wire tests were written after the first several.
//
// Each earlier wire test carries its own copy (emulator/backup_wire_test.go,
// emulator/codedeploy_wire_test.go, emulator/redshift_wire_test.go, emulator/ec2_wire_test.go and
// others); these are the same two walks, parameterized by the member list, so a new service names
// its members rather than copying a walker. Both compare a member name case-insensitively and as an
// equality, never as a substring: the account and the Region a record
// carries also appear inside published values such as ARNs, and a substring test would collide with
// them.

// wireAssertNoMemberJSON fails if any member of the JSON document body is named for one of members,
// at any depth, reporting the `$.a.b[0].c` path so a failure names the member rather than the
// service.
//
// The whole document rather than a record's subtree, which is strictly stronger: a member rendered on
// the envelope rather than on the record would fail here and pass a subtree walk.
func wireAssertNoMemberJSON(t *testing.T, site string, body []byte, members []string) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", site, body)
	wireWalkJSON(t, site, "$", doc, members)
}

// wireWalkJSON recurses through a decoded JSON document asserting on every member name it meets.
func wireWalkJSON(t *testing.T, site, path string, node any, members []string) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(key, member),
					"%s answered %s: %s is substrate's bookkeeping and no published shape carries it",
					site, child, member)
			}
			wireWalkJSON(t, site, child, value, members)
		}
	case []any:
		for i, value := range typed {
			wireWalkJSON(t, site, fmt.Sprintf("%s[%d]", path, i), value, members)
		}
	}
}

// wireAssertNoMemberXML fails if any element of the XML document body is named for one of members,
// at any depth, reporting the `/a/b/c` path.
//
// When keyText is set it also fails on the character data of an element named keyText. That is for
// the query services that render a string map as `<entry><key>…</key><value>…</value></entry>`: there
// a leaked member arrives as the *text* of a `<key>`, not as an element name, and an element walk
// alone would pass it.
func wireAssertNoMemberXML(t *testing.T, site string, body []byte, members []string, keyText string) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoErrorf(t, err, "%s: decode body: %s", site, body)
		switch el := tok.(type) {
		case xml.StartElement:
			path = append(path, el.Name.Local)
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(el.Name.Local, member),
					"%s answered %s: %s is substrate's bookkeeping and no published shape carries it",
					site, strings.Join(path, "/"), member)
			}
		case xml.CharData:
			if keyText == "" || len(path) == 0 || path[len(path)-1] != keyText {
				continue
			}
			text := strings.TrimSpace(string(el))
			for _, member := range members {
				assert.Falsef(t, strings.EqualFold(text, member),
					"%s answered %s=%q: %s is substrate's bookkeeping and no published shape carries it",
					site, strings.Join(path, "/"), text, member)
			}
		case xml.EndElement:
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
		}
	}
}
