package emulator_test

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Redshift answers the Query protocol's three-level document (#1208).
//
// Every Redshift operation page's sample response is
//
//	<{Operation}Response xmlns="http://redshift.amazonaws.com/doc/2012-12-01/">
//	  <{Operation}Result>…</{Operation}Result>
//	  <ResponseMetadata><RequestId>…</RequestId></ResponseMetadata>
//	</{Operation}Response>
//
// and until #1208 substrate marshaled the result as the document root, with no namespace and no
// request ID, so no SDK could decode any Redshift response. These assertions are on the raw bytes, per
// iam_shape_members_test.go: a test that decodes into substrate's own structs cannot see a missing
// envelope, which is how it survived.

// redshiftXMLNS is the namespace every Redshift page's sample response declares.
const redshiftXMLNS = "http://redshift.amazonaws.com/doc/2012-12-01/"

// redshiftDocElement is one element of a decoded response document: its name, namespace, and the
// names of its direct children in order.
type redshiftDocElement struct {
	name, space string
	children    []string
}

// redshiftWalk returns the document's root and its direct children, and the text of every element
// named for each path asked for, so a test reads the raw structure rather than a struct's view of it.
func redshiftWalk(t *testing.T, body []byte) (root redshiftDocElement, elements map[string][]string) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	elements = map[string][]string{}
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "decode: %s", body)
		switch el := tok.(type) {
		case xml.StartElement:
			if len(path) == 0 {
				root = redshiftDocElement{name: el.Name.Local, space: el.Name.Space}
			}
			if len(path) == 1 {
				root.children = append(root.children, el.Name.Local)
			}
			path = append(path, el.Name.Local)
			elements[strings.Join(path, "/")] = append(elements[strings.Join(path, "/")], "")
		case xml.CharData:
			if len(path) > 0 {
				key := strings.Join(path, "/")
				vals := elements[key]
				vals[len(vals)-1] += strings.TrimSpace(string(el))
			}
		case xml.EndElement:
			path = path[:len(path)-1]
		}
	}
	return root, elements
}

func TestRedshiftEnvelope_EveryOperationAnswersThePublishedDocument(t *testing.T) {
	t.Parallel()
	p, ctx := setupRedshiftPlugin(t)
	call := func(op string, params map[string]string) []byte {
		t.Helper()
		resp, err := p.HandleRequest(ctx, redshiftRequest(t, op, params))
		require.NoError(t, err, "%s", op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
		return resp.Body
	}

	cluster := map[string]string{"ClusterIdentifier": "env-cluster", "NodeType": "dc2.large", "MasterUsername": "admin"}
	for _, tc := range []struct {
		op     string
		params map[string]string
		// item is the path, under the result, that a single record or a list member is published at.
		item string
	}{
		{"CreateCluster", cluster, "Cluster"},
		{"DescribeClusters", nil, "Clusters/Cluster"},
		{"DescribeClusters", map[string]string{"ClusterIdentifier": "env-cluster"}, "Clusters/Cluster"},
		{"ModifyCluster", map[string]string{"ClusterIdentifier": "env-cluster", "NodeType": "ra3.xlplus"}, "Cluster"},
		{"CreateClusterParameterGroup", map[string]string{"ParameterGroupName": "env-pg", "ParameterGroupFamily": "redshift-1.0", "Description": "d"}, "ClusterParameterGroup"},
		{"DescribeClusterParameterGroups", nil, "ParameterGroups/ClusterParameterGroup"},
		{"CreateClusterSubnetGroup", map[string]string{"ClusterSubnetGroupName": "env-sg", "Description": "d", "SubnetIds.SubnetIdentifier.1": "subnet-env"}, "ClusterSubnetGroup"},
		{"DescribeClusterSubnetGroups", nil, "ClusterSubnetGroups/ClusterSubnetGroup"},
		{"CreateClusterSnapshot", map[string]string{"ClusterIdentifier": "env-cluster", "SnapshotIdentifier": "env-snap"}, "Snapshot"},
		{"DescribeClusterSnapshots", nil, "Snapshots/Snapshot"},
		// Last: it removes the cluster the cases above read.
		{"DeleteCluster", map[string]string{"ClusterIdentifier": "env-cluster", "SkipFinalClusterSnapshot": "true"}, "Cluster"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := call(tc.op, tc.params)
			root, elements := redshiftWalk(t, body)
			require.Equal(t, tc.op+"Response", root.name, "the document root: %s", body)
			require.Equal(t, redshiftXMLNS, root.space, "the namespace: %s", body)
			require.Equal(t, []string{tc.op + "Result", "ResponseMetadata"}, root.children,
				"the result, then the metadata, as every sample shows: %s", body)
			require.Equal(t, []string{ctx.RequestID}, elements[tc.op+"Response/ResponseMetadata/RequestId"],
				"RequestId is the request's own ID, so a replayed body is byte-identical: %s", body)
			require.NotEmptyf(t, elements[tc.op+"Response/"+tc.op+"Result/"+tc.item],
				"the record is published at %s: %s", tc.item, body)
			require.NotContains(t, string(body), "<member>", "no list in these shapes publishes <member>: %s", body)
		})
	}
}

// The exact bytes of one success, so the whole document is pinned, not only its outline.
func TestRedshiftEnvelope_ASingleClusterIsClusterNotMember(t *testing.T) {
	t.Parallel()
	p, ctx := setupRedshiftPlugin(t)
	resp, err := p.HandleRequest(ctx, redshiftRequest(t, "CreateCluster", map[string]string{
		"ClusterIdentifier": "exact", "NodeType": "dc2.large", "MasterUsername": "admin",
	}))
	require.NoError(t, err)
	body := string(resp.Body)
	require.True(t, strings.HasPrefix(body, xml.Header+
		`<CreateClusterResponse xmlns="`+redshiftXMLNS+`"><CreateClusterResult><Cluster><ClusterIdentifier>exact</ClusterIdentifier>`),
		"CreateCluster opens with the envelope and a bare Cluster: %s", body)
	require.True(t, strings.HasSuffix(body,
		`</Cluster></CreateClusterResult><ResponseMetadata><RequestId>`+ctx.RequestID+`</RequestId></ResponseMetadata></CreateClusterResponse>`),
		"CreateCluster closes with the result, then the metadata: %s", body)
	assert.Equal(t, "text/xml; charset=UTF-8", resp.Headers["Content-Type"])
}

// redshiftWireCall posts one Query action to a test server and returns its status and body.
func redshiftWireCall(t *testing.T, baseURL string, params map[string]string) (int, []byte) {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "redshift.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// The refusals, over the wire: each code is the one Redshift's pages publish, at the published
// status, rendered in the Query error document.
func TestRedshiftEnvelope_RefusalsAnswerThePublishedCodes(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, body := redshiftWireCall(t, ts.URL, map[string]string{
		"Action": "CreateCluster", "Version": "2012-12-01",
		"ClusterIdentifier": "dup", "NodeType": "dc2.large", "MasterUsername": "admin",
	})
	require.Equal(t, http.StatusOK, status, "CreateCluster: %s", body)
	root, _ := redshiftWalk(t, body)
	require.Equal(t, "CreateClusterResponse", root.name, "the success over the wire is enveloped too: %s", body)

	for _, tc := range []struct {
		name   string
		params map[string]string
		status int
		code   string
	}{
		// API_CreateCluster: ClusterAlreadyExists, 400.
		{"a duplicate cluster", map[string]string{"Action": "CreateCluster", "ClusterIdentifier": "dup", "NodeType": "dc2.large", "MasterUsername": "admin"},
			http.StatusBadRequest, "ClusterAlreadyExists"},
		// API_DescribeClusters: ClusterNotFound, 404.
		{"a cluster that does not exist", map[string]string{"Action": "DescribeClusters", "ClusterIdentifier": "absent"},
			http.StatusNotFound, "ClusterNotFound"},
		{"deleting a cluster that does not exist", map[string]string{"Action": "DeleteCluster", "ClusterIdentifier": "absent"},
			http.StatusNotFound, "ClusterNotFound"},
		// Common Errors publishes no InvalidAction; an unknown Action is a parameter value that isn't valid.
		{"an action substrate does not route", map[string]string{"Action": "DescribeReservedNodes"},
			http.StatusBadRequest, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Version"] = "2012-12-01"
			status, body := redshiftWireCall(t, ts.URL, tc.params)
			require.Equal(t, tc.status, status, "%s: %s", tc.name, body)
			require.Contains(t, string(body), "<Code>"+tc.code+"</Code>", "%s: %s", tc.name, body)
			require.NotContains(t, string(body), "Fault</Code>", "the Fault suffix is the model's shape name, not the wire code: %s", body)
			require.NotContains(t, string(body), "InvalidAction", "Redshift's Common Errors page publishes no InvalidAction: %s", body)
		})
	}
}

// An absent Action is Common Errors' MissingAction, distinct from an unrecognized one.
func TestRedshiftEnvelope_AMissingActionIsMissingAction(t *testing.T) {
	t.Parallel()
	p, ctx := setupRedshiftPlugin(t)
	_, err := p.HandleRequest(ctx, redshiftRequest(t, "", nil))
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "MissingAction", awsErr.Code)
	assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
}
