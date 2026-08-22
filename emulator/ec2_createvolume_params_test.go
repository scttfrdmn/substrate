package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateVolume's two combination rules, and the size it will no longer invent (#712).
//
// AWS states both rules in its request parameters and neither with a Required: Yes. On Size:
// "You must specify either a snapshot ID or a volume size." On the zone pair, said twice, once
// on each member: "Either AvailabilityZone or AvailabilityZoneId must be specified, but not
// both." Substrate read neither, and the two silent defaults it used instead — 8 GiB and
// region+"a" — are the failure mode a refusal exists to prevent: the call reported success, so
// a volume appeared in a zone the caller never named, at a size they never asked for, and the
// first visible symptom was an attach failing later for no stated cause.
//
// The refusal code is substrate's reading and the tests assert it as such. CreateVolume's own
// Errors section is empty — it publishes no operation-specific error — so the only candidate is
// EC2's client-error table, where InvalidParameterCombination's gloss is the one that covers
// this shape: "The request includes an incorrect combination of parameters, or a missing
// parameter."

// ec2CreateVolumeParams builds a CreateVolume request, letting a caller drop a parameter the
// default request carries by naming it with an empty value.
func ec2CreateVolumeParams(extra map[string]string) map[string]string {
	params := map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "8",
	}
	for k, v := range extra {
		if v == "" {
			delete(params, k)
			continue
		}
		params[k] = v
	}
	return params
}

// ec2CreatedVolume is the part of a CreateVolume response these tests read.
type ec2CreatedVolume struct {
	VolumeID         string `xml:"volumeId"`
	Size             int    `xml:"size"`
	AvailabilityZone string `xml:"availabilityZone"`
}

// ec2CreateVolumeOK sends a CreateVolume expected to succeed.
func ec2CreateVolumeOK(t *testing.T, ts *httptest.Server, extra map[string]string) ec2CreatedVolume {
	t.Helper()
	var doc ec2CreatedVolume
	ec2FleetXML(t, ts, ec2CreateVolumeParams(extra), &doc)
	require.NotEmpty(t, doc.VolumeID)
	return doc
}

// TestEC2_CreateVolume_SizeOrSnapshotIsRequired pins the first combination rule.
func TestEC2_CreateVolume_SizeOrSnapshotIsRequired(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	t.Run("neither is refused", func(t *testing.T) {
		status, code, message := ec2ErrorDetail(t, ts,
			ec2CreateVolumeParams(map[string]string{"Size": ""}))
		assert.Equal(t, http.StatusBadRequest, status,
			"a request naming neither Size nor SnapshotId got a silent 8 GiB volume before #712")
		assert.Equal(t, "InvalidParameterCombination", code)
		assert.Contains(t, message, "snapshot",
			"the message must name what the caller could supply instead of a size")
	})

	t.Run("a size alone is enough", func(t *testing.T) {
		vol := ec2CreateVolumeOK(t, ts, map[string]string{"Size": "20"})
		assert.Equal(t, 20, vol.Size)
	})

	t.Run("a snapshot alone is enough", func(t *testing.T) {
		_, snapID := ec2SnapshotOfSize(t, ts, 30)
		vol := ec2CreateVolumeOK(t, ts, map[string]string{"Size": "", "SnapshotId": snapID})
		assert.Equal(t, 30, vol.Size,
			"the snapshot supplies the size, which is the whole reason Size is optional")
	})
}

// TestEC2_CreateVolume_SizeMustBeAPositiveInteger pins that an unusable Size is refused rather
// than replaced.
//
// Every row below produced an 8 GiB volume before #712, because the parse was
// `if n, err := strconv.Atoi(sizeStr); err == nil && n > 0` with no else — a discarded error
// and a discarded out-of-range value, which is exactly the shape the house rules forbid. `-1`
// is the sharpest case: a caller who meant to compute a size and got a negative number was
// told the volume was created.
func TestEC2_CreateVolume_SizeMustBeAPositiveInteger(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, size := range []string{"0", "-1", "8.5", "eight", "8 GiB", " 8", "0x8"} {
		t.Run(size, func(t *testing.T) {
			status, code, message := ec2ErrorDetail(t, ts,
				ec2CreateVolumeParams(map[string]string{"Size": size}))
			assert.Equal(t, http.StatusBadRequest, status,
				"Size=%q silently became %d GiB before #712", size, 8)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, size,
				"the message must quote the offending value back")
		})
	}

	// The boundary: 1 GiB is the smallest size any documented volume type accepts, and it is
	// accepted. Substrate does not enforce the per-type ranges (gp2 1-16384, io1 4-16384, …),
	// which is stated in the docs rather than left to be discovered.
	t.Run("1 GiB is accepted", func(t *testing.T) {
		vol := ec2CreateVolumeOK(t, ts, map[string]string{"Size": "1"})
		assert.Equal(t, 1, vol.Size)
	})
}

// TestEC2_CreateVolume_ZonePairIsExclusiveAndRequired pins the second combination rule, whose
// two halves fail in opposite directions.
func TestEC2_CreateVolume_ZonePairIsExclusiveAndRequired(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	t.Run("neither is refused", func(t *testing.T) {
		status, code, message := ec2ErrorDetail(t, ts,
			ec2CreateVolumeParams(map[string]string{"AvailabilityZone": ""}))
		assert.Equal(t, http.StatusBadRequest, status,
			"an absent zone silently became region+\"a\" before #712")
		assert.Equal(t, "InvalidParameterCombination", code)
		assert.Contains(t, message, "AvailabilityZoneId",
			"a caller who omitted both needs to be told both spellings exist")
	})

	t.Run("both is refused", func(t *testing.T) {
		status, code, _ := ec2ErrorDetail(t, ts, ec2CreateVolumeParams(map[string]string{
			"AvailabilityZone":   "us-east-1a",
			"AvailabilityZoneId": "use1-az1",
		}))
		assert.Equal(t, http.StatusBadRequest, status,
			"AWS says \"but not both\", and agreeing values are still both")
		assert.Equal(t, "InvalidParameterCombination", code)
	})

	t.Run("a zone name alone", func(t *testing.T) {
		vol := ec2CreateVolumeOK(t, ts, map[string]string{"AvailabilityZone": "us-east-1c"})
		assert.Equal(t, "us-east-1c", vol.AvailabilityZone)
	})
}

// TestEC2_CreateVolume_ZoneIDResolvesThroughDescribeAvailabilityZones pins that the zone ID
// substrate accepts is the one it publishes.
//
// AvailabilityZoneId was ignored outright before #712, so a request naming a zone by ID
// created a volume in zone a. It resolves now, and it resolves through the same derivation
// DescribeAvailabilityZones renders from — which is the point of the test: a caller who reads
// a zone ID out of one operation and hands it to the other must not be told it does not exist.
func TestEC2_CreateVolume_ZoneIDResolvesThroughDescribeAvailabilityZones(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	zones := ec2DescribeAZs(t, ts, map[string]string{"Action": "DescribeAvailabilityZones"})
	require.Len(t, zones, 3)

	for _, zone := range zones {
		t.Run(zone.ZoneID, func(t *testing.T) {
			require.NotEmpty(t, zone.ZoneID)
			vol := ec2CreateVolumeOK(t, ts, map[string]string{
				"AvailabilityZone":   "",
				"AvailabilityZoneId": zone.ZoneID,
			})
			assert.Equal(t, zone.ZoneName, vol.AvailabilityZone,
				"the record and the response carry the zone *name*, translated from the ID")
		})
	}

	// A zone ID naming nothing is refused rather than resolved to zone a, because unlike a
	// zone name it cannot be recorded as given — it has to be translated to be stored at all.
	t.Run("an unknown zone ID is refused", func(t *testing.T) {
		status, code, message := ec2ErrorDetail(t, ts, ec2CreateVolumeParams(map[string]string{
			"AvailabilityZone":   "",
			"AvailabilityZoneId": "use1-az9",
		}))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Contains(t, message, "use1-az9")
	})

	// The asymmetry, pinned so it is a decision and not an oversight: a zone *name* naming
	// nothing is recorded as given. Validating names is a wider change than the pair rule and
	// one substrate has never made on any zone-taking operation.
	t.Run("an unknown zone name is recorded as given", func(t *testing.T) {
		vol := ec2CreateVolumeOK(t, ts, map[string]string{"AvailabilityZone": "us-east-1z"})
		assert.Equal(t, "us-east-1z", vol.AvailabilityZone)
	})
}

// ec2ZoneIDsInRegion asks DescribeAvailabilityZones for one region's zone IDs, addressing the
// server by that region's host so the request context carries it.
func ec2ZoneIDsInRegion(t *testing.T, ts *httptest.Server, region string) []string {
	t.Helper()
	form := url.Values{"Action": []string{"DescribeAvailabilityZones"}}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "ec2." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var doc struct {
		XMLName xml.Name `xml:"DescribeAvailabilityZonesResponse"`
		Items   []struct {
			ZoneID string `xml:"zoneId"`
		} `xml:"availabilityZoneInfo>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&doc))
	ids := make([]string, 0, len(doc.Items))
	for _, item := range doc.Items {
		ids = append(ids, item.ZoneID)
	}
	return ids
}

// TestEC2_ZoneIDPrefixMatchesAWSPublishedTable pins the AZ-ID derivation against AWS's own
// table, which #712 is the first change to make load-bearing on the request side.
//
// Every row is copied from the AWS Availability Zones reference. Before #712 the derivation
// produced "ue11" for us-east-1 — a doubled digit and the wrong shape, refuted by AWS's table
// and by the code's own comment, which claimed "use1". Nothing pinned it because zone IDs were
// only ever *emitted*: a test that read one out of DescribeAvailabilityZones and filtered on
// it was self-consistent whatever the string was. CreateVolume now accepts one as an input,
// so a consumer's fixture carrying the real "use1-az1" would have been refused.
//
// ap-southeast-2 and ap-northeast-1 are the rows that matter most: they refute AWS's own
// summarizing sentence ("the first three letters of the Region code"), which would give "aps2"
// and "apn1". The published table wins over the sentence describing it.
func TestEC2_ZoneIDPrefixMatchesAWSPublishedTable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for region, prefix := range map[string]string{
		"us-east-1":      "use1",
		"us-east-2":      "use2",
		"us-west-1":      "usw1",
		"us-west-2":      "usw2",
		"ca-central-1":   "cac1",
		"mx-central-1":   "mxc1",
		"af-south-1":     "afs1",
		"ap-east-1":      "ape1",
		"ap-south-1":     "aps1",
		"ap-southeast-2": "apse2",
		"ap-northeast-1": "apne1",
		"eu-central-1":   "euc1",
		"eu-north-1":     "eun1",
		"eu-west-1":      "euw1",
		"eu-west-3":      "euw3",
		"il-central-1":   "ilc1",
		"me-south-1":     "mes1",
		"sa-east-1":      "sae1",
	} {
		t.Run(region, func(t *testing.T) {
			assert.Equal(t,
				[]string{prefix + "-az1", prefix + "-az2", prefix + "-az3"},
				ec2ZoneIDsInRegion(t, ts, region),
				"AWS publishes %s-az1 and friends for %s", prefix, region)
		})
	}
}

// TestEC2_CreateVolume_RefusalWritesNothing pins that a refused CreateVolume leaves no volume
// behind, which is the rule #673 established for RunInstances and #713 re-established for
// ReplaceRouteTableAssociation.
//
// The three refusals land before the state write, but that is an ordering a later change could
// break silently: a refusal that had already stored the volume would leave one at a size or in
// a zone the caller was told was invalid, and DescribeVolumes would report it.
func TestEC2_CreateVolume_RefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for name, extra := range map[string]map[string]string{
		"no size, no snapshot": {"Size": ""},
		"negative size":        {"Size": "-1"},
		"no zone":              {"AvailabilityZone": ""},
		"both zones":           {"AvailabilityZoneId": "use1-az1"},
		"unknown zone ID":      {"AvailabilityZone": "", "AvailabilityZoneId": "use1-az9"},
	} {
		t.Run(name, func(t *testing.T) {
			status, _, _ := ec2ErrorDetail(t, ts, ec2CreateVolumeParams(extra))
			require.Equal(t, http.StatusBadRequest, status)
		})
	}

	var doc struct {
		VolumeIDs []string `xml:"volumeSet>item>volumeId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeVolumes"}, &doc)
	assert.Empty(t, doc.VolumeIDs, "five refused requests must have created no volume")
}
