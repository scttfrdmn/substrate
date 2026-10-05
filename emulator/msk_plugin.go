package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MSKPlugin emulates the Amazon Managed Streaming for Apache Kafka (MSK) service.
// It supports cluster lifecycle operations using the MSK REST/JSON API.
type MSKPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// seedMu serializes the read-modify-write of a cluster's observation counter; see
	// [progression.observe].
	seedMu sync.Mutex
}

// Name returns the service name "msk".
func (p *MSKPlugin) Name() string { return mskNamespace }

// Initialize sets up the MSKPlugin with the provided configuration.
func (p *MSKPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for MSKPlugin.
func (p *MSKPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an MSK REST/JSON request to the appropriate handler.
func (p *MSKPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, clusterARN := parseKafkaOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateCluster":
		return p.createCluster(ctx, req)
	case "DescribeCluster":
		return p.describeCluster(ctx, req, clusterARN)
	case "GetBootstrapBrokers":
		return p.getBootstrapBrokers(ctx, req, clusterARN)
	case "ListClusters":
		return p.listClusters(ctx, req)
	case "DeleteCluster":
		return p.deleteCluster(ctx, req, clusterARN)
	case "CreateClusterV2":
		return p.createClusterV2(ctx, req)
	case "DescribeClusterV2":
		return p.describeClusterV2(ctx, req, clusterARN)
	case "ListClustersV2":
		return p.listClustersV2(ctx, req)
	case "ListNodes":
		return p.listNodes(ctx, req, clusterARN)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseKafkaOperation derives the MSK operation and optional cluster ARN from
// the HTTP method and request path.
//
// The path is matched as it arrives. It used to be normalised by strings.TrimRight(path, "/"), which
// folded an empty path parameter onto the collection route one case earlier in this switch:
// GET /v1/clusters/ became GET /v1/clusters and answered ListClusters, so a caller who built the URL
// from an empty variable read a full cluster list as success and three empty-ARN guards below could not
// be reached by any request (#1009). AWS publishes nothing about a trailing slash for MSK — as #950
// recorded, this is the weakest service in the tree to reason about from documentation, with no
// common-errors page and no Errors sections — so routing an empty parameter to the single-cluster
// operation is substrate's reading. It follows [parseEFSOperation], which never trimmed and whose nine
// empty-parameter guards are all reachable, on the ground that a refusal a caller can act on beats a
// different operation's success.
//
// # Anchoring (#1205)
//
// A cluster ARN contains slashes (`…:cluster/{name}/{uuid}`), and the router hands this function
// the decoded path, so the ARN cannot be taken as one slash-delimited segment. Each arm is anchored
// at both ends instead: the published prefix, the ARN, then the published suffix if there is one.
// The two sub-resource arms used to test only the suffix, so GET /anything/at/all/nodes was taken
// for ListNodes and refused 400 for a malformed ARN, telling the caller its input was wrong rather
// than that the path does not exist. MSK publishes /nodes and /bootstrap-brokers under /v1 only, so
// the same suffixes under /api/v2/clusters/ are refused as unrouted rather than read as part of a
// DescribeClusterV2 ARN.
func parseKafkaOperation(method, path string) (op, clusterARN string) {
	const (
		v1Prefix = "/v1/clusters/"
		v2Prefix = "/api/v2/clusters/"
	)
	subResource := func(prefix, suffix string) (string, bool) {
		if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) || len(path) < len(prefix)+len(suffix) {
			return "", false
		}
		return path[len(prefix) : len(path)-len(suffix)], true
	}

	switch {
	case path == "/v1/clusters" && method == http.MethodPost:
		return "CreateCluster", ""
	case path == "/v1/clusters" && method == http.MethodGet:
		return "ListClusters", ""
	case path == "/api/v2/clusters" && method == http.MethodPost:
		return "CreateClusterV2", ""
	case path == "/api/v2/clusters" && method == http.MethodGet:
		return "ListClustersV2", ""
	}

	if arn, ok := subResource(v1Prefix, "/bootstrap-brokers"); ok {
		if method == http.MethodGet {
			return "GetBootstrapBrokers", arn
		}
		return "", ""
	}
	if arn, ok := subResource(v1Prefix, "/nodes"); ok {
		if method == http.MethodGet {
			return "ListNodes", arn
		}
		return "", ""
	}
	if strings.HasPrefix(path, v1Prefix) {
		arn := strings.TrimPrefix(path, v1Prefix)
		switch method {
		case http.MethodGet:
			return "DescribeCluster", arn
		case http.MethodDelete:
			return "DeleteCluster", arn
		}
		return "", ""
	}
	if strings.HasPrefix(path, v2Prefix) {
		if strings.HasSuffix(path, "/nodes") || strings.HasSuffix(path, "/bootstrap-brokers") {
			return "", ""
		}
		if method == http.MethodGet {
			return "DescribeClusterV2", strings.TrimPrefix(path, v2Prefix)
		}
	}
	return "", ""
}

// mskBrokerNodeGroupInfoIn is brokerNodeGroupInfo as a create request carries it. ClientSubnets is
// read through a nil check, which tells an absent member (refused, Required: True) from an empty
// list (accepted: CloudFormation sends one, and the page's two-or-three-subnet rule is prose, not a
// constraint the model states).
type mskBrokerNodeGroupInfoIn struct {
	InstanceType     string               `json:"InstanceType"`
	ClientSubnets    []string             `json:"ClientSubnets"`
	SecurityGroups   []string             `json:"SecurityGroups"`
	StorageInfo      MSKStorageInfo       `json:"StorageInfo"`
	ConnectivityInfo *MSKConnectivityInfo `json:"ConnectivityInfo"`
}

// mskProvisionedIn holds the members CreateCluster takes at its top level and CreateClusterV2 takes
// under provisioned. Decoding is case-insensitive, so the published lowerCamel spellings
// (clusterName, brokerNodeGroupInfo, …) and the PascalCase ones CloudFormation sends both land.
type mskProvisionedIn struct {
	KafkaVersion         *string                   `json:"KafkaVersion"`
	NumberOfBrokerNodes  *int                      `json:"NumberOfBrokerNodes"`
	BrokerNodeGroupInfo  *mskBrokerNodeGroupInfoIn `json:"BrokerNodeGroupInfo"`
	EncryptionInfo       *MSKEncryptionInfo        `json:"EncryptionInfo"`
	ClientAuthentication *MSKClientAuthentication  `json:"ClientAuthentication"`
	EnhancedMonitoring   string                    `json:"EnhancedMonitoring"`
	StorageMode          string                    `json:"StorageMode"`
	ConfigurationInfo    *MSKConfigurationInfo     `json:"ConfigurationInfo"`
	LoggingInfo          *MSKLoggingInfo           `json:"LoggingInfo"`
	OpenMonitoring       *MSKOpenMonitoring        `json:"OpenMonitoring"`
	Rebalancing          *MSKRebalancing           `json:"Rebalancing"`
}

// mskCheckClusterName enforces clusterName, Required: True with MinLength 1 and MaxLength 64 on both
// create pages.
func mskCheckClusterName(name string) error {
	if name == "" {
		return mskBadRequest("clusterName", "clusterName is required")
	}
	if len(name) > 64 {
		return mskBadRequest("clusterName", "clusterName must be at most 64 characters")
	}
	return nil
}

// checkProvisioned validates a provisioned configuration. required says whether CreateCluster's
// Required: True members must be present: v1 marks kafkaVersion, numberOfBrokerNodes and
// brokerNodeGroupInfo required, and CreateClusterV2's ProvisionedRequest marks all three False, so a
// v2 request may omit them (#1211). The constraints on a member that is present apply either way.
func (in *mskProvisionedIn) check(required bool) error {
	if in.KafkaVersion == nil {
		if required {
			return mskBadRequest("kafkaVersion", "kafkaVersion is required")
		}
	} else if l := len(*in.KafkaVersion); l < 1 || l > 128 {
		return mskBadRequest("kafkaVersion", "kafkaVersion must be 1 to 128 characters")
	}
	if in.NumberOfBrokerNodes == nil {
		if required {
			return mskBadRequest("numberOfBrokerNodes", "numberOfBrokerNodes is required")
		}
	} else if *in.NumberOfBrokerNodes < 1 {
		// The page states no minimum; a cluster of no brokers is substrate's reading of "incorrect
		// input", refused rather than created.
		return mskBadRequest("numberOfBrokerNodes", "numberOfBrokerNodes must be at least 1")
	}
	if in.BrokerNodeGroupInfo == nil {
		if required {
			return mskBadRequest("brokerNodeGroupInfo", "brokerNodeGroupInfo is required")
		}
	} else {
		b := in.BrokerNodeGroupInfo
		if b.ClientSubnets == nil {
			return mskBadRequest("clientSubnets", "brokerNodeGroupInfo.clientSubnets is required")
		}
		if l := len(b.InstanceType); l == 0 {
			return mskBadRequest("instanceType", "brokerNodeGroupInfo.instanceType is required")
		} else if l < 5 || l > 32 {
			return mskBadRequest("instanceType", "brokerNodeGroupInfo.instanceType must be 5 to 32 characters")
		}
		if v := b.StorageInfo.EbsStorageInfo.VolumeSize; v != 0 && (v < 1 || v > 16384) {
			return mskBadRequest("volumeSize", "storageInfo.ebsStorageInfo.volumeSize must be 1 to 16384")
		}
		if err := mskCheckConnectivity(b.ConnectivityInfo); err != nil {
			return err
		}
	}
	if in.EncryptionInfo != nil {
		if at := in.EncryptionInfo.EncryptionAtRest; at != nil && at.DataVolumeKMSKeyID == "" {
			return mskBadRequest("dataVolumeKMSKeyId", "encryptionAtRest.dataVolumeKMSKeyId is required")
		}
		if it := in.EncryptionInfo.EncryptionInTransit; it != nil && it.ClientBroker != "" {
			switch it.ClientBroker {
			case "TLS", "TLS_PLAINTEXT", "PLAINTEXT":
			default:
				return mskBadRequest("clientBroker", "encryptionInTransit.clientBroker must be TLS, TLS_PLAINTEXT or PLAINTEXT")
			}
		}
	}
	if c := in.ConfigurationInfo; c != nil {
		if c.Arn == "" {
			return mskBadRequest("arn", "configurationInfo.arn is required")
		}
		if c.Revision < 1 {
			return mskBadRequest("revision", "configurationInfo.revision must be at least 1")
		}
	}
	return mskCheckLoggingAndMonitoring(in.LoggingInfo, in.OpenMonitoring, in.Rebalancing)
}

// mskCheckLoggingAndMonitoring enforces what both create pages state about loggingInfo,
// openMonitoring and rebalancing (#1386): brokerLogs is Required when loggingInfo is sent, each
// delivery target's enabled is Required when the target is sent, prometheus is Required when
// openMonitoring is sent, each exporter's enabledInBroker is Required when the exporter is sent, and
// rebalancing.status is one of its published values, PAUSED or ACTIVE. A destination's logGroup,
// deliveryStream or bucket is not checked: the pages mark each optional, and what an enabled target
// without one does is AWS's behavior, not something the model states.
func mskCheckLoggingAndMonitoring(l *MSKLoggingInfo, o *MSKOpenMonitoring, r *MSKRebalancing) error {
	if l != nil {
		if l.BrokerLogs == nil {
			return mskBadRequest("brokerLogs", "loggingInfo.brokerLogs is required")
		}
		for _, d := range []*MSKLogDestinations{l.BrokerLogs, l.AuthorizerLogs} {
			if err := mskCheckLogDestinations(d); err != nil {
				return err
			}
		}
	}
	if o != nil {
		if o.Prometheus == nil {
			return mskBadRequest("prometheus", "openMonitoring.prometheus is required")
		}
		for _, e := range []*MSKExporter{o.Prometheus.JmxExporter, o.Prometheus.NodeExporter} {
			if e != nil && e.EnabledInBroker == nil {
				return mskBadRequest("enabledInBroker", "openMonitoring.prometheus exporter enabledInBroker is required")
			}
		}
	}
	if r != nil && r.Status != "" && r.Status != "PAUSED" && r.Status != "ACTIVE" {
		return mskBadRequest("status", "rebalancing.status must be PAUSED or ACTIVE")
	}
	return nil
}

// mskCheckLogDestinations enforces enabled, Required on each delivery target a log set names.
func mskCheckLogDestinations(d *MSKLogDestinations) error {
	if d == nil {
		return nil
	}
	if (d.CloudWatchLogs != nil && d.CloudWatchLogs.Enabled == nil) ||
		(d.Firehose != nil && d.Firehose.Enabled == nil) ||
		(d.S3 != nil && d.S3.Enabled == nil) {
		return mskBadRequest("enabled", "a loggingInfo delivery target's enabled is required")
	}
	return nil
}

// mskCheckConnectivity enforces the values connectivityInfo's members publish (#1386): networkType
// is IPV4 or DUAL, and publicAccess.type is one of the two values its description names, DISABLED
// and SERVICE_PROVIDED_EIPS. Every member is optional, so an absent one is accepted.
func mskCheckConnectivity(c *MSKConnectivityInfo) error {
	if c == nil {
		return nil
	}
	if c.NetworkType != "" && c.NetworkType != "IPV4" && c.NetworkType != "DUAL" {
		return mskBadRequest("networkType", "connectivityInfo.networkType must be IPV4 or DUAL")
	}
	if p := c.PublicAccess; p != nil && p.Type != "" && p.Type != "DISABLED" && p.Type != "SERVICE_PROVIDED_EIPS" {
		return mskBadRequest("type", "connectivityInfo.publicAccess.type must be DISABLED or SERVICE_PROVIDED_EIPS")
	}
	return nil
}

// apply copies a validated provisioned configuration onto a record.
func (in *mskProvisionedIn) apply(c *MSKCluster) {
	if in.KafkaVersion != nil {
		c.KafkaVersion = *in.KafkaVersion
	}
	if in.NumberOfBrokerNodes != nil {
		c.NumberOfBrokerNodes = *in.NumberOfBrokerNodes
	}
	if b := in.BrokerNodeGroupInfo; b != nil {
		c.BrokerNodeGroupInfo = MSKBrokerNodeGroupInfo{
			InstanceType:     b.InstanceType,
			ClientSubnets:    b.ClientSubnets,
			SecurityGroups:   b.SecurityGroups,
			StorageInfo:      b.StorageInfo,
			ConnectivityInfo: b.ConnectivityInfo,
		}
	}
	c.LoggingInfo = in.LoggingInfo
	c.OpenMonitoring = in.OpenMonitoring
	c.Rebalancing = in.Rebalancing
	c.EncryptionInfo = in.EncryptionInfo
	c.ClientAuthentication = in.ClientAuthentication
	c.EnhancedMonitoring = in.EnhancedMonitoring
	c.StorageMode = in.StorageMode
	c.ConfigurationInfo = in.ConfigurationInfo
}

// createCluster handles CreateCluster (POST /v1/clusters). Every member clusters.html marks Required:
// True is checked before use and refused when absent, rather than defaulted (#1197): kafkaVersion
// used to default to "3.5.1", numberOfBrokerNodes to 2, and an absent brokerNodeGroupInfo was
// accepted, so a cluster was created from an empty body.
func (p *MSKPlugin) createCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ClusterName string            `json:"ClusterName"`
		Tags        map[string]string `json:"Tags"`
		mskProvisionedIn
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, mskInvalidBody()
		}
	}
	if err := mskCheckClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if err := input.check(true); err != nil {
		return nil, err
	}
	cluster := p.newCluster(reqCtx, input.ClusterName, input.Tags)
	cluster.ClusterType = mskClusterTypeProvisioned
	input.apply(&cluster)
	if err := p.storeNewCluster(reqCtx, &cluster); err != nil {
		return nil, err
	}
	obs, err := p.clusterObservation(&cluster, false)
	if err != nil {
		return nil, err
	}
	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"clusterArn":  cluster.ClusterARN,
		"clusterName": cluster.ClusterName,
		"state":       obs.State,
	})
}

// newCluster builds the record a create writes, minting its ARN.
func (p *MSKPlugin) newCluster(reqCtx *RequestContext, name string, tags map[string]string) MSKCluster {
	arn := "arn:aws:kafka:" + reqCtx.Region + ":" + reqCtx.AccountID + ":cluster/" + name + "/" + mskClusterUUID(reqCtx.IDs)
	return MSKCluster{
		ClusterName:    name,
		ClusterARN:     arn,
		CurrentVersion: mskMintCurrentVersion(reqCtx.IDs),
		State:          "ACTIVE",
		Tags:           tags,
		AccountID:      reqCtx.AccountID,
		Region:         reqCtx.Region,
		CreatedAt:      p.tc.Now(),
	}
}

// storeNewCluster writes a new cluster and its index entry, refusing a name already in use with the
// 409 both create pages publish for "This cluster name already exists".
func (p *MSKPlugin) storeNewCluster(reqCtx *RequestContext, cluster *MSKCluster) error {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	indexKey := "cluster_ids:" + scope
	names, err := loadStringIndex(context.Background(), p.state, mskNamespace, indexKey)
	if err != nil {
		return fmt.Errorf("msk create load index: %w", err)
	}
	for _, n := range names {
		if n == cluster.ClusterName {
			return mskConflict("clusterName", "Cluster "+cluster.ClusterName+" already exists.")
		}
	}
	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("msk create marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), mskNamespace, "cluster:"+scope+"/"+cluster.ClusterName, data); err != nil {
		return fmt.Errorf("msk create put: %w", err)
	}
	if err := updateStringIndex(context.Background(), p.state, mskNamespace, indexKey, cluster.ClusterName); err != nil {
		return fmt.Errorf("msk storeNewCluster index: %w", err)
	}
	return nil
}

func (p *MSKPlugin) describeCluster(_ *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	// Reachable since #1009: GET /v1/clusters/ routes here with an empty ARN rather than onto
	// ListClusters.
	if clusterARN == "" {
		return nil, mskBadRequest("clusterArn", "cluster ARN is required")
	}
	cluster, obs, err := p.observedCluster(clusterARN, true)
	if err != nil {
		return nil, err
	}
	shown := *cluster
	shown.State = obs.State
	info := mskClusterInfoWire(&shown)
	info.StateInfo = obs.StateInfo
	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"clusterInfo": info,
	})
}

// getBootstrapBrokers handles GetBootstrapBrokers. See [mskBootstrapBrokers] for which of the
// page's fourteen strings a cluster answers and in what host form.
func (p *MSKPlugin) getBootstrapBrokers(_ *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	if clusterARN == "" {
		return nil, mskBadRequest("clusterArn", "cluster ARN is required")
	}
	cluster, _, err := p.observedCluster(clusterARN, false)
	if err != nil {
		return nil, err
	}
	return mskJSONResponse(http.StatusOK, mskBootstrapBrokers(cluster))
}

// mskBrokerHosts answers each broker's host name, in the form GetBootstrapBrokers' page shows:
// b-{n}.{clusterName}.{id}.c2.kafka.{region}.amazonaws.com. The id is the first six hex digits of
// the ARN's UUID, so it is stable for a cluster and distinct between a cluster and a later one that
// reuses its name (#1204). Until #1199 the hosts were broker{n}.{name}.{region}.kafka.amazonaws.com,
// a form no MSK page uses.
func mskBrokerHosts(c *MSKCluster) []string {
	hosts := make([]string, 0, c.NumberOfBrokerNodes)
	for i := 1; i <= c.NumberOfBrokerNodes; i++ {
		hosts = append(hosts, fmt.Sprintf("b-%d.%s.%s.c2.kafka.%s.amazonaws.com", i, c.ClusterName, mskClusterHostID(c), c.Region))
	}
	return hosts
}

// mskClusterHostID is the per-cluster label in a broker host name.
func mskClusterHostID(c *MSKCluster) string {
	uuid := c.ClusterARN[strings.LastIndex(c.ClusterARN, "/")+1:]
	id := strings.ReplaceAll(uuid, "-", "")
	if len(id) > 6 {
		id = id[:6]
	}
	return id
}

// mskBootstrapBrokers answers the GetBootstrapBrokers members a cluster's configuration implies, each
// a comma-joined list of host:port pairs on the ports the developer guide's port-information page
// gives: 9092 plaintext, 9094 TLS, 9096 SASL/SCRAM, 9098 SASL/IAM.
//
//   - bootstrapBrokerString: client-broker encryption PLAINTEXT or TLS_PLAINTEXT, and unauthenticated
//     access.
//   - bootstrapBrokerStringTls: encryption TLS or TLS_PLAINTEXT, and unauthenticated or TLS client
//     authentication.
//   - bootstrapBrokerStringSaslScram / bootstrapBrokerStringSaslIam: encryption TLS or TLS_PLAINTEXT
//     (clusters.html: "To turn on SASL, you must also turn on EncryptionInTransit"), and that
//     mechanism enabled.
//
// The encryption default is TLS (clusters.html: "The default value is TLS"), so a cluster created
// with no encryptionInfo answers bootstrapBrokerStringTls, not bootstrapBrokerString. Absent client
// authentication reads as unauthenticated. A serverless cluster answers bootstrapBrokerStringSaslIam
// alone, at boot-{id}.c2.kafka-serverless.{region}.amazonaws.com:9098, since IAM is the only SASL
// mechanism a serverless cluster publishes. The page's other eight members — the public, VPC
// connectivity and IPv6 variants — are not answered: substrate models no connectivityInfo or network
// type.
func mskBootstrapBrokers(c *MSKCluster) map[string]string {
	out := map[string]string{}
	if mskClusterTypeOf(c) == mskClusterTypeServerless {
		out["bootstrapBrokerStringSaslIam"] = fmt.Sprintf("boot-%s.c2.kafka-serverless.%s.amazonaws.com:9098", mskClusterHostID(c), c.Region)
		return out
	}
	hosts := mskBrokerHosts(c)
	if len(hosts) == 0 {
		return out
	}
	join := func(port int) string {
		pairs := make([]string, len(hosts))
		for i, h := range hosts {
			pairs[i] = fmt.Sprintf("%s:%d", h, port)
		}
		return strings.Join(pairs, ",")
	}
	clientBroker, _ := mskEffectiveInTransit(c.EncryptionInfo)
	plaintext := clientBroker == "PLAINTEXT" || clientBroker == "TLS_PLAINTEXT"
	tls := clientBroker == "TLS" || clientBroker == "TLS_PLAINTEXT"
	a := c.ClientAuthentication
	iam := a != nil && a.Sasl != nil && a.Sasl.IAM != nil && a.Sasl.IAM.Enabled
	scram := a != nil && a.Sasl != nil && a.Sasl.Scram != nil && a.Sasl.Scram.Enabled
	tlsAuth := a != nil && a.TLS != nil && a.TLS.Enabled
	unauth := a == nil || (a.Unauthenticated != nil && a.Unauthenticated.Enabled) || (!iam && !scram && !tlsAuth)
	if plaintext && unauth {
		out["bootstrapBrokerString"] = join(9092)
	}
	if tls && (unauth || tlsAuth) {
		out["bootstrapBrokerStringTls"] = join(9094)
	}
	if tls && scram {
		out["bootstrapBrokerStringSaslScram"] = join(9096)
	}
	if tls && iam {
		out["bootstrapBrokerStringSaslIam"] = join(9098)
	}
	return out
}

// listClusters handles ListClusters (GET /v1/clusters).
//
// It reads the three published query parameters (#1195): clusterNameFilter, "a prefix of the name of
// the clusters", maxResults and nextToken (see [mskPage]). The v1 page describes ListClusters as
// returning "all the MSK clusters" while ListClustersV2's says it lists "all serverless and
// provisioned clusters", and v1's ClusterInfo has no member to describe a serverless one with, so a
// serverless cluster is listed by ListClustersV2 only — substrate's reading of the contrast.
func (p *MSKPlugin) listClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	listed, err := p.listedClusters(reqCtx, req.Params["clusterNameFilter"])
	if err != nil {
		return nil, err
	}
	clusters, stateInfos, err := p.observedListing(listed)
	if err != nil {
		return nil, err
	}
	infos := make([]mskClusterInfoOut, 0, len(clusters))
	for i, c := range clusters {
		if mskClusterTypeOf(c) == mskClusterTypeServerless {
			continue
		}
		info := mskClusterInfoWire(c)
		info.StateInfo = stateInfos[i]
		infos = append(infos, info)
	}
	page, next, err := mskPage(infos, req)
	if err != nil {
		return nil, err
	}
	resp := map[string]interface{}{"clusterInfoList": page}
	if next != "" {
		resp["nextToken"] = next
	}
	return mskJSONResponse(http.StatusOK, resp)
}

// listedClusters loads every cluster in the request's scope, in index order, keeping those whose name
// starts with nameFilter. A record that will not load is an error rather than a silently shorter list.
func (p *MSKPlugin) listedClusters(reqCtx *RequestContext, nameFilter string) ([]*MSKCluster, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	names, err := loadStringIndex(context.Background(), p.state, mskNamespace, "cluster_ids:"+scope)
	if err != nil {
		return nil, fmt.Errorf("msk list load index: %w", err)
	}
	out := make([]*MSKCluster, 0, len(names))
	for _, name := range names {
		if !strings.HasPrefix(name, nameFilter) {
			continue
		}
		data, err := p.state.Get(context.Background(), mskNamespace, "cluster:"+scope+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("msk list get %s: %w", name, err)
		}
		if data == nil {
			continue
		}
		var c MSKCluster
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("msk list unmarshal %s: %w", name, err)
		}
		out = append(out, &c)
	}
	return out, nil
}

// mskMaxResultsLimit is the largest page MSK's list operations answer. Each page gives maxResults as
// "default maximum 100 results per API call" and states no range, so 1 to 100 is substrate's reading:
// 100 is the ceiling the page names, and a page of no results is not a request anyone can mean.
const mskMaxResultsLimit = 100

// mskPage cuts one page out of a full listing, reading maxResults and nextToken from the query.
// maxResults defaults to the 100 the page names and is refused outside 1 to 100. nextToken is an
// offset token ([decodeOffsetPaginationToken]); one substrate did not issue is refused rather than
// answered as page one (#915's rule). MSK publishes no code for either, so both are
// BadRequestException/400, the status its pages give incorrect input, with invalidParameter naming
// the parameter. The returned token is empty on the last page, and the caller omits the member then
// rather than sending "" (#1195).
func mskPage[T any](items []T, req *AWSRequest) ([]T, string, error) {
	limit := mskMaxResultsLimit
	if raw, ok := req.Params["maxResults"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > mskMaxResultsLimit {
			return nil, "", mskBadRequest("maxResults", "maxResults must be an integer from 1 to 100")
		}
		limit = n
	}
	offset, ok := decodeOffsetPaginationToken(req.Params["nextToken"])
	if !ok {
		return nil, "", mskBadRequest("nextToken", "the nextToken is not one this operation issued")
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := offset + limit
	if end >= len(items) {
		return items[offset:], "", nil
	}
	return items[offset:end], encodeOffsetPaginationToken(end), nil
}

// deleteCluster handles DeleteCluster.
//
// It reads the page's one query parameter, currentVersion, and refuses a version the cluster is not
// at (see [mskCheckCurrentVersion]); until #1196 it was ignored. Unseeded, the record is removed at
// once, as it always was, and the response's DELETING is the one observation of the deleting cluster
// the call affords. Under a seed with pending observations the record is kept, marked DELETING, and
// its countdown restarted: that many describes report DELETING, and the one after answers
// NotFoundException. A second delete of a cluster already deleting answers DELETING again without
// restarting its countdown.
func (p *MSKPlugin) deleteCluster(_ *RequestContext, req *AWSRequest, clusterARN string) (*AWSResponse, error) {
	// Reachable since #1009: DELETE /v1/clusters/ routes here rather than answering unknownRouteError,
	// so a delete naming no cluster is refused for the reason it is wrong.
	if clusterARN == "" {
		return nil, mskBadRequest("clusterArn", "cluster ARN is required")
	}
	cluster, _, err := p.observedCluster(clusterARN, false)
	if err != nil {
		return nil, err
	}
	if err := mskCheckCurrentVersion(cluster, req.Params["currentVersion"]); err != nil {
		return nil, err
	}

	// DeleteClusterResponse declares only clusterArn and state; clusterName is not a member of it, so
	// it is not reported here.
	answer := map[string]interface{}{
		"clusterArn": cluster.ClusterARN,
		"state":      mskClusterTransitionDeleting,
	}
	if cluster.Transition == mskClusterTransitionDeleting {
		return mskJSONResponse(http.StatusOK, answer)
	}

	seed, _, err := mskClusterProgressions.peek(context.Background(), p.state, cluster.ClusterARN)
	if err != nil {
		return nil, fmt.Errorf("msk deleteCluster: %w", err)
	}
	if seed == nil || seed.PendingObservations == 0 {
		if err := p.removeCluster(cluster); err != nil {
			return nil, err
		}
		return mskJSONResponse(http.StatusOK, answer)
	}

	cluster.Transition = mskClusterTransitionDeleting
	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("msk deleteCluster marshal: %w", err)
	}
	scope := cluster.AccountID + "/" + cluster.Region
	if err := p.state.Put(context.Background(), mskNamespace, "cluster:"+scope+"/"+cluster.ClusterName, data); err != nil {
		return nil, fmt.Errorf("msk deleteCluster put: %w", err)
	}
	if err := mskClusterProgressions.reset(context.Background(), p.state, cluster.ClusterARN); err != nil {
		return nil, fmt.Errorf("msk deleteCluster: %w", err)
	}
	return mskJSONResponse(http.StatusOK, answer)
}

// loadClusterByARN finds and deserializes a cluster by its ARN.
// ARN format: arn:aws:kafka:{region}:{acct}:cluster/{name}/{uuid}.
func (p *MSKPlugin) loadClusterByARN(clusterARN string) (*MSKCluster, error) {
	// Parse region and account from ARN.
	parts := strings.SplitN(clusterARN, ":", 7)
	if len(parts) < 6 || parts[2] != "kafka" {
		return nil, mskBadRequest("clusterArn", "invalid MSK cluster ARN: "+clusterARN)
	}
	region := parts[3]
	acct := parts[4]
	// parts[5] is "cluster/{name}/{uuid}"
	resParts := strings.SplitN(parts[5], "/", 3)
	if len(resParts) < 2 {
		return nil, mskBadRequest("clusterArn", "invalid MSK cluster ARN resource: "+clusterARN)
	}
	name := resParts[1]

	scope := acct + "/" + region
	data, err := p.state.Get(context.Background(), mskNamespace, "cluster:"+scope+"/"+name)
	if err != nil {
		return nil, fmt.Errorf("msk loadClusterByARN get: %w", err)
	}
	notFound := mskNotFound("clusterArn", "Cluster not found: "+clusterARN)
	if data == nil {
		return nil, notFound
	}
	var cluster MSKCluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return nil, fmt.Errorf("msk loadClusterByARN unmarshal: %w", err)
	}
	// The record is found by name, and the ARN must then be the record's own, UUID included. The UUID
	// is what distinguishes a cluster from a later one reusing its name, so an ARN naming a deleted
	// cluster used to resolve to its replacement: a wrong-resource answer reported as success
	// (#1204). MSK publishes no error codes, so NotFoundException/404 is substrate's reading (#671),
	// the same answer a name that was never created gets.
	if cluster.ClusterARN != clusterARN {
		return nil, notFound
	}
	return &cluster, nil
}

// createClusterV2 handles CreateClusterV2 (POST /api/v2/clusters), checked against v2-clusters.html
// (#1211).
//
// The page's CreateClusterV2Request publishes clusterName (Required: True, 1 to 64), provisioned,
// serverless and tags, the last three Required: False. It publishes no rule for a request naming
// neither cluster kind, or both; a request with neither describes no cluster, and one with both
// describes two, so each is refused — substrate's reading, with invalidParameter naming the member.
// Inside provisioned the page marks every member Required: False, unlike CreateCluster's top level,
// so a v2 provisioned create may omit kafkaVersion, numberOfBrokerNodes and brokerNodeGroupInfo, and
// nothing is defaulted in their place. Inside serverless, vpcConfigs (each with subnetIds) and
// clientAuthentication are Required: True.
//
// Until #1211 serverless was decoded by nothing: a serverless create stored a provisioned cluster
// with no brokers, and DescribeClusterV2 reported clusterType PROVISIONED. The response is the
// page's CreateClusterV2Response, which carries clusterType beside the three members v1's has.
func (p *MSKPlugin) createClusterV2(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ClusterName string            `json:"ClusterName"`
		Tags        map[string]string `json:"Tags"`
		Provisioned *mskProvisionedIn `json:"Provisioned"`
		Serverless  *struct {
			VpcConfigs           []MSKVpcConfig                     `json:"VpcConfigs"`
			ClientAuthentication *MSKServerlessClientAuthentication `json:"ClientAuthentication"`
		} `json:"Serverless"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, mskInvalidBody()
		}
	}
	if err := mskCheckClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	switch {
	case input.Provisioned == nil && input.Serverless == nil:
		return nil, mskBadRequest("provisioned", "one of provisioned or serverless is required")
	case input.Provisioned != nil && input.Serverless != nil:
		return nil, mskBadRequest("serverless", "only one of provisioned or serverless may be given")
	}

	cluster := p.newCluster(reqCtx, input.ClusterName, input.Tags)
	if input.Provisioned != nil {
		if err := input.Provisioned.check(false); err != nil {
			return nil, err
		}
		cluster.ClusterType = mskClusterTypeProvisioned
		input.Provisioned.apply(&cluster)
	} else {
		sv := input.Serverless
		if sv.VpcConfigs == nil {
			return nil, mskBadRequest("vpcConfigs", "serverless.vpcConfigs is required")
		}
		for _, v := range sv.VpcConfigs {
			if v.SubnetIDs == nil {
				return nil, mskBadRequest("subnetIds", "serverless.vpcConfigs[].subnetIds is required")
			}
		}
		if sv.ClientAuthentication == nil {
			return nil, mskBadRequest("clientAuthentication", "serverless.clientAuthentication is required")
		}
		cluster.ClusterType = mskClusterTypeServerless
		cluster.Serverless = &MSKServerless{VpcConfigs: sv.VpcConfigs, ClientAuthentication: *sv.ClientAuthentication}
	}
	if err := p.storeNewCluster(reqCtx, &cluster); err != nil {
		return nil, err
	}
	obs, err := p.clusterObservation(&cluster, false)
	if err != nil {
		return nil, err
	}
	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"clusterArn":  cluster.ClusterARN,
		"clusterName": cluster.ClusterName,
		"clusterType": cluster.ClusterType,
		"state":       obs.State,
	})
}

// describeClusterV2 returns cluster details in the V2 Cluster shape, under clusterInfo, as
// v2-clusters-clusterarn.html publishes.
func (p *MSKPlugin) describeClusterV2(_ *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	// Reachable since #1009, as in describeCluster. getBootstrapBrokers and listNodes never had the
	// problem, because a literal segment follows the ARN and the empty parameter is interior.
	if clusterARN == "" {
		return nil, mskBadRequest("clusterArn", "cluster ARN is required")
	}
	cluster, obs, err := p.observedCluster(clusterARN, true)
	if err != nil {
		return nil, err
	}
	shown := *cluster
	shown.State = obs.State
	info := mskClusterWire(&shown)
	info.StateInfo = obs.StateInfo
	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"clusterInfo": info,
	})
}

// listClustersV2 handles ListClustersV2 (GET /api/v2/clusters). It reads the four query parameters
// v2-clusters.html publishes: clusterNameFilter ("Returns clusters starting with given name"),
// clusterTypeFilter ("Returns clusters with the given type", PROVISIONED or SERVERLESS; any other value
// is refused naming the parameter), maxResults and nextToken (see [mskPage]).
func (p *MSKPlugin) listClustersV2(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	typeFilter := req.Params["clusterTypeFilter"]
	switch typeFilter {
	case "", mskClusterTypeProvisioned, mskClusterTypeServerless:
	default:
		return nil, mskBadRequest("clusterTypeFilter", "clusterTypeFilter must be PROVISIONED or SERVERLESS")
	}
	listed, err := p.listedClusters(reqCtx, req.Params["clusterNameFilter"])
	if err != nil {
		return nil, err
	}
	clusters, stateInfos, err := p.observedListing(listed)
	if err != nil {
		return nil, err
	}
	infos := make([]mskClusterOut, 0, len(clusters))
	for i, c := range clusters {
		if typeFilter != "" && mskClusterTypeOf(c) != typeFilter {
			continue
		}
		info := mskClusterWire(c)
		info.StateInfo = stateInfos[i]
		infos = append(infos, info)
	}
	page, next, err := mskPage(infos, req)
	if err != nil {
		return nil, err
	}
	resp := map[string]interface{}{"clusterInfoList": page}
	if next != "" {
		resp["nextToken"] = next
	}
	return mskJSONResponse(http.StatusOK, resp)
}

// listNodes handles ListNodes: one node per broker, with each broker's endpoint the host
// GetBootstrapBrokers names, and brokers spread over the client subnets in order, the even
// distribution clusters.html describes. It pages with maxResults and nextToken (see [mskPage]). A
// serverless cluster has no brokers a caller manages, so it lists none — substrate's reading; the page
// does not address serverless clusters.
func (p *MSKPlugin) listNodes(_ *RequestContext, req *AWSRequest, clusterARN string) (*AWSResponse, error) {
	if clusterARN == "" {
		return nil, mskBadRequest("clusterArn", "cluster ARN is required")
	}
	cluster, _, err := p.observedCluster(clusterARN, false)
	if err != nil {
		return nil, err
	}

	// Extract UUID from the cluster ARN for node ARN construction.
	arnParts := strings.SplitN(clusterARN, "/", 3)
	uuid := ""
	if len(arnParts) >= 3 {
		uuid = arnParts[2]
	}

	out := []mskNodeInfoOut{}
	if mskClusterTypeOf(cluster) == mskClusterTypeProvisioned {
		subnets := cluster.BrokerNodeGroupInfo.ClientSubnets
		for i, host := range mskBrokerHosts(cluster) {
			clientSubnet := ""
			if len(subnets) > 0 {
				clientSubnet = subnets[i%len(subnets)]
			}
			out = append(out, mskNodeInfoWire(MSKNodeInfo{
				BrokerNodeInfo: MSKBrokerNodeInfo{
					BrokerID:     float64(i + 1),
					ClientSubnet: clientSubnet,
					CurrentBrokerSoftwareInfo: MSKBrokerSoftwareInfo{
						KafkaVersion: cluster.KafkaVersion,
					},
					Endpoints: []string{host},
				},
				InstanceType: cluster.BrokerNodeGroupInfo.InstanceType,
				NodeARN: fmt.Sprintf("arn:aws:kafka:%s:%s:broker/%s/%s/%d",
					cluster.Region, cluster.AccountID, cluster.ClusterName, uuid, i+1),
				NodeType: "BROKER",
			}))
		}
	}
	page, next, err := mskPage(out, req)
	if err != nil {
		return nil, err
	}
	resp := map[string]interface{}{"nodeInfoList": page}
	if next != "" {
		resp["nextToken"] = next
	}
	return mskJSONResponse(http.StatusOK, resp)
}

// mskClusterUUID mints the UUID a cluster ARN ends in: a version-4 UUID and a one-digit suffix,
// `…:cluster/{name}/{uuid}-{n}`, the form every ARN on MSK's pages takes (for example
// `…/6357e0b2-0e6a-4b86-a0b4-70df934c2e31-5`). MSK publishes no pattern for it.
//
// It used to be built from the name's length and the clock's nanoseconds, padded with
// `-0001-0001-0001-000000000001`, so on a frozen clock a deleted-then-recreated cluster reused its
// predecessor's ARN, and resolution ignored the UUID anyway (#1204). Derived from the request id
// now, as every minted identifier is (#856), so a replay mints the ARN its recording minted and two
// creates of one name mint two ARNs.
func mskClusterUUID(m *IDMint) string {
	return m.UUID() + "-" + m.Chars(1, "123456789")
}

// mskJSONResponse serializes v to JSON and returns an AWSResponse.
func mskJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("msk json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
