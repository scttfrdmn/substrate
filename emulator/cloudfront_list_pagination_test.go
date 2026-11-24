package emulator_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListDistributions and ListOriginAccessControls page on Marker/MaxItems and always answer Marker,
// MaxItems, IsTruncated and Quantity, with NextMarker when truncated (API_ListDistributions,
// API_ListOriginAccessControls), over the wire.

// cfListDoc is the paging members of either list document, and the IDs it lists.
type cfListDoc struct {
	Marker      *string  `xml:"Marker"`
	MaxItems    *int     `xml:"MaxItems"`
	IsTruncated *bool    `xml:"IsTruncated"`
	Quantity    *int     `xml:"Quantity"`
	NextMarker  *string  `xml:"NextMarker"`
	IDs         []string `xml:"Items>DistributionSummary>Id"`
	OACIDs      []string `xml:"Items>OriginAccessControlSummary>Id"`
}

// ids is whichever list the document carries.
func (d cfListDoc) ids() []string { return append(append([]string{}, d.IDs...), d.OACIDs...) }

// cfListCase is one CloudFront listing and how to add an item to it.
type cfListCase struct {
	name, path string
	create     func(t *testing.T, ts *emulator.TestServer, i int) string
	remove     func(t *testing.T, ts *emulator.TestServer, id string)
}

// cfListCases are the two paginated listings.
func cfListCases() []cfListCase {
	return []cfListCase{
		{
			name: "ListDistributions", path: "/2020-05-31/distribution",
			create: func(t *testing.T, ts *emulator.TestServer, i int) string {
				id, _, _ := cfProgCreate(t, ts, fmt.Sprintf("page-%d", i))
				return id
			},
			remove: nil, // a distribution must be disabled first; the OAC case covers deletion.
		},
		{
			name: "ListOriginAccessControls", path: cfOACPath,
			create: func(t *testing.T, ts *emulator.TestServer, i int) string {
				body := strings.Replace(cfOACConfigXML, "substrate-oac", fmt.Sprintf("oac-%d", i), 1)
				code, raw, _ := cfProgCall(t, ts, http.MethodPost, cfOACPath, "", body)
				require.Equal(t, http.StatusCreated, code, "%s", raw)
				var doc struct {
					ID string `xml:"Id"`
				}
				require.NoError(t, xml.Unmarshal([]byte(raw), &doc))
				return doc.ID
			},
			remove: func(t *testing.T, ts *emulator.TestServer, id string) {
				code, raw, etag := cfProgCall(t, ts, http.MethodGet, cfOACPath+"/"+id, "", "")
				require.Equal(t, http.StatusOK, code, "%s", raw)
				code, raw, _ = cfProgCall(t, ts, http.MethodDelete, cfOACPath+"/"+id, etag, "")
				require.Equal(t, http.StatusNoContent, code, "%s", raw)
			},
		},
	}
}

// cfList reads one page, failing on anything but 200.
func cfList(t *testing.T, ts *emulator.TestServer, path string) cfListDoc {
	t.Helper()
	code, raw := cfWire(t, ts, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, code, "%s", raw)
	var doc cfListDoc
	require.NoError(t, xml.Unmarshal(raw, &doc), "%s", raw)
	require.NotNil(t, doc.Marker, "Marker is always answered: %s", raw)
	require.NotNil(t, doc.MaxItems, "MaxItems is always answered: %s", raw)
	require.NotNil(t, doc.IsTruncated, "IsTruncated is always answered: %s", raw)
	require.NotNil(t, doc.Quantity, "Quantity is always answered: %s", raw)
	assert.Equal(t, len(doc.ids()), *doc.Quantity, "Quantity counts the page")
	assert.Equal(t, *doc.IsTruncated, doc.NextMarker != nil, "NextMarker is present exactly when truncated: %s", raw)
	return doc
}

func TestCloudFrontListPagination_EchoesAndPages(t *testing.T) {
	t.Parallel()
	for _, tc := range cfListCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)

			empty := cfList(t, ts, tc.path)
			assert.Equal(t, "", *empty.Marker)
			assert.Equal(t, 100, *empty.MaxItems, "an absent MaxItems is substrate's default of 100")
			assert.False(t, *empty.IsTruncated)

			var created []string
			for i := range 5 {
				created = append(created, tc.create(t, ts, i))
			}
			sort.Strings(created)

			whole := cfList(t, ts, tc.path)
			assert.Equal(t, created, whole.ids(), "the listing is in ID order")

			var walked []string
			marker := ""
			for pages := 0; ; pages++ {
				require.Less(t, pages, 10)
				path := tc.path + "?MaxItems=2"
				if marker != "" {
					path += "&Marker=" + marker
				}
				pg := cfList(t, ts, path)
				assert.Equal(t, marker, *pg.Marker, "Marker echoes the request")
				assert.Equal(t, 2, *pg.MaxItems, "MaxItems echoes the request")
				walked = append(walked, pg.ids()...)
				if !*pg.IsTruncated {
					break
				}
				ids := pg.ids()
				assert.Equal(t, ids[len(ids)-1], *pg.NextMarker, "NextMarker is the page's last ID")
				marker = *pg.NextMarker
			}
			assert.Equal(t, created, walked, "a paged walk lists every item once, in order")

			// An unknown Marker resumes after where it sorts.
			after := cfList(t, ts, tc.path+"?Marker="+created[2]+"0")
			assert.Equal(t, created[3:], after.ids())

			if tc.remove != nil {
				// The cursor's own item deleted between pages resumes in place.
				first := cfList(t, ts, tc.path+"?MaxItems=2")
				tc.remove(t, ts, first.ids()[1])
				rest := cfList(t, ts, tc.path+"?Marker="+*first.NextMarker)
				assert.Equal(t, created[2:], rest.ids())
			}
		})
	}
}

func TestCloudFrontListPagination_RefusesAnUnreadableMaxItems(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, tc := range cfListCases() {
		for _, bad := range []string{"abc", "0", "-1", "1.5", ""} {
			code, raw := cfWire(t, ts, http.MethodGet, tc.path+"?MaxItems="+bad, "")
			assert.Equal(t, http.StatusBadRequest, code, "%s MaxItems=%q: %s", tc.name, bad, raw)
			assert.Contains(t, string(raw), "<Code>InvalidArgument</Code>", "%s MaxItems=%q", tc.name, bad)
		}
	}
}
