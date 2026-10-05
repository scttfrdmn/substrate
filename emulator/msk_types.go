package emulator

import "time"

// mskNamespace is the state namespace used by the MSK plugin.
const mskNamespace = "msk"

// MSKCluster represents an Amazon MSK (Managed Streaming for Apache Kafka) cluster.
type MSKCluster struct {
	// ClusterName is the name of the MSK cluster.
	ClusterName string `json:"ClusterName"`
	// ClusterARN is the Amazon Resource Name of the cluster.
	ClusterARN string `json:"ClusterArn"`
	// State is the current state of the cluster (e.g. "ACTIVE").
	State string `json:"State"`
	// BrokerNodeGroupInfo holds the broker node configuration.
	BrokerNodeGroupInfo MSKBrokerNodeGroupInfo `json:"BrokerNodeGroupInfo"`
	// NumberOfBrokerNodes is the number of broker nodes in the cluster.
	NumberOfBrokerNodes int `json:"NumberOfBrokerNodes"`
	// KafkaVersion is the version of Apache Kafka.
	KafkaVersion string `json:"KafkaVersion"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags,omitempty"`
	// AccountID is the AWS account that owns the cluster.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the cluster resides.
	Region string `json:"Region"`
	// CreatedAt is the time the cluster was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// The members below were added by #1199 and #1211. Each is omitempty, so a record written before
	// them decodes unchanged and reads as a provisioned cluster with none of them set.

	// CurrentVersion is the cluster's currentVersion, minted at create (#1196) and checked by
	// DeleteCluster. Empty on a record from before #1196, which accepts any version.
	CurrentVersion string `json:"CurrentVersion,omitempty"`
	// Transition is DELETING once a seeded DeleteCluster keeps the record for its countdown, and empty
	// otherwise. See emulator/msk_progression.go.
	Transition string `json:"Transition,omitempty"`

	// ClusterType is PROVISIONED or SERVERLESS, the published ClusterType values. Empty means
	// PROVISIONED, which is every cluster a record from before #1211 describes.
	ClusterType string `json:"ClusterType,omitempty"`
	// Serverless holds a serverless cluster's configuration, as CreateClusterV2 received it.
	Serverless *MSKServerless `json:"Serverless,omitempty"`
	// EncryptionInfo is the create request's encryptionInfo, as received.
	EncryptionInfo *MSKEncryptionInfo `json:"EncryptionInfo,omitempty"`
	// ClientAuthentication is the create request's clientAuthentication, as received.
	ClientAuthentication *MSKClientAuthentication `json:"ClientAuthentication,omitempty"`
	// EnhancedMonitoring is the create request's enhancedMonitoring, as received.
	EnhancedMonitoring string `json:"EnhancedMonitoring,omitempty"`
	// StorageMode is the create request's storageMode, as received.
	StorageMode string `json:"StorageMode,omitempty"`
	// ConfigurationInfo is the create request's configurationInfo, as received.
	ConfigurationInfo *MSKConfigurationInfo `json:"ConfigurationInfo,omitempty"`
}

// MSKServerless is a serverless cluster's configuration: the ServerlessRequest members of
// CreateClusterV2, both of which its page marks Required.
type MSKServerless struct {
	// VpcConfigs is the cluster's VPC configuration.
	VpcConfigs []MSKVpcConfig `json:"VpcConfigs"`
	// ClientAuthentication is the cluster's client authentication.
	ClientAuthentication MSKServerlessClientAuthentication `json:"ClientAuthentication"`
}

// MSKVpcConfig is one VpcConfig of a serverless cluster.
type MSKVpcConfig struct {
	// SubnetIDs is the subnets the cluster connects to; Required on the page.
	SubnetIDs []string `json:"SubnetIds"`
	// SecurityGroupIDs is the security groups attached to its ENIs.
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
}

// MSKServerlessClientAuthentication is a serverless cluster's client authentication.
type MSKServerlessClientAuthentication struct {
	// Sasl holds its SASL settings.
	Sasl *MSKServerlessSasl `json:"Sasl,omitempty"`
}

// MSKServerlessSasl is a serverless cluster's SASL settings; IAM is the only one published.
type MSKServerlessSasl struct {
	// IAM is SASL/IAM authentication.
	IAM *MSKEnabled `json:"Iam,omitempty"`
}

// MSKEnabled is the one-member {enabled} object MSK's authentication settings share.
type MSKEnabled struct {
	// Enabled reports whether the setting is on.
	Enabled bool `json:"Enabled"`
}

// MSKEncryptionInfo is a cluster's encryptionInfo.
type MSKEncryptionInfo struct {
	// EncryptionAtRest holds the data-volume KMS key.
	EncryptionAtRest *MSKEncryptionAtRest `json:"EncryptionAtRest,omitempty"`
	// EncryptionInTransit holds the in-transit settings.
	EncryptionInTransit *MSKEncryptionInTransit `json:"EncryptionInTransit,omitempty"`
}

// MSKEncryptionAtRest is a cluster's encryptionAtRest.
type MSKEncryptionAtRest struct {
	// DataVolumeKMSKeyID is the KMS key; Required on the page when the object is sent.
	DataVolumeKMSKeyID string `json:"DataVolumeKMSKeyId"`
}

// MSKEncryptionInTransit is a cluster's encryptionInTransit. Both members are optional, and the page
// states each default: clientBroker TLS, inCluster true.
type MSKEncryptionInTransit struct {
	// ClientBroker is TLS, TLS_PLAINTEXT or PLAINTEXT.
	ClientBroker string `json:"ClientBroker,omitempty"`
	// InCluster reports whether broker-to-broker traffic is encrypted.
	InCluster *bool `json:"InCluster,omitempty"`
}

// MSKClientAuthentication is a provisioned cluster's clientAuthentication.
type MSKClientAuthentication struct {
	// Sasl holds the SASL settings.
	Sasl *MSKSasl `json:"Sasl,omitempty"`
	// TLS holds TLS client authentication.
	TLS *MSKTLSAuthentication `json:"Tls,omitempty"`
	// Unauthenticated allows unauthenticated access.
	Unauthenticated *MSKEnabled `json:"Unauthenticated,omitempty"`
}

// MSKSasl is a provisioned cluster's SASL settings.
type MSKSasl struct {
	// IAM is SASL/IAM authentication.
	IAM *MSKEnabled `json:"Iam,omitempty"`
	// Scram is SASL/SCRAM authentication.
	Scram *MSKEnabled `json:"Scram,omitempty"`
}

// MSKTLSAuthentication is TLS client authentication.
type MSKTLSAuthentication struct {
	// CertificateAuthorityArnList is the private CAs that issue client certificates.
	CertificateAuthorityArnList []string `json:"CertificateAuthorityArnList,omitempty"`
	// Enabled reports whether TLS authentication is on.
	Enabled bool `json:"Enabled"`
}

// MSKConfigurationInfo is the configuration a cluster's brokers use. Both members are Required on the
// page when the object is sent.
type MSKConfigurationInfo struct {
	// Arn is the configuration's ARN.
	Arn string `json:"Arn"`
	// Revision is the configuration revision, at least 1.
	Revision int64 `json:"Revision"`
}

// MSKBrokerNodeGroupInfo holds configuration for MSK broker nodes.
type MSKBrokerNodeGroupInfo struct {
	// InstanceType is the Amazon EC2 instance type for the brokers.
	InstanceType string `json:"InstanceType"`
	// ClientSubnets holds the list of subnets for the brokers.
	ClientSubnets []string `json:"ClientSubnets"`
	// SecurityGroups holds the security group IDs for the brokers.
	SecurityGroups []string `json:"SecurityGroups"`
	// StorageInfo holds the storage configuration for the brokers.
	StorageInfo MSKStorageInfo `json:"StorageInfo"`
}

// MSKStorageInfo holds storage configuration for MSK broker nodes.
type MSKStorageInfo struct {
	// EbsStorageInfo holds the EBS storage configuration.
	EbsStorageInfo MSKEBSStorageInfo `json:"EbsStorageInfo"`
}

// MSKEBSStorageInfo holds EBS storage configuration for MSK broker nodes.
type MSKEBSStorageInfo struct {
	// VolumeSize is the size of the EBS volume in GiB.
	VolumeSize int `json:"VolumeSize"`
}

// MSKNodeInfo describes a single broker node returned by ListNodes.
type MSKNodeInfo struct {
	// BrokerNodeInfo holds broker-specific details.
	BrokerNodeInfo MSKBrokerNodeInfo `json:"BrokerNodeInfo"`
	// InstanceType is the EC2 instance type for this broker.
	InstanceType string `json:"InstanceType"`
	// NodeARN is the Amazon Resource Name of the broker node.
	NodeARN string `json:"NodeArn"`
	// NodeType is always "BROKER" for MSK clusters.
	NodeType string `json:"NodeType"`
}

// MSKBrokerNodeInfo holds broker-level details returned by ListNodes.
type MSKBrokerNodeInfo struct {
	// BrokerID is the numeric broker identifier (1-based).
	BrokerID float64 `json:"BrokerId"`
	// ClientSubnet is the subnet the broker is placed in.
	ClientSubnet string `json:"ClientSubnet"`
	// CurrentBrokerSoftwareInfo holds the software version running on the broker.
	CurrentBrokerSoftwareInfo MSKBrokerSoftwareInfo `json:"CurrentBrokerSoftwareInfo"`
	// Endpoints is the broker's host names, the same ones GetBootstrapBrokers answers.
	Endpoints []string `json:"Endpoints,omitempty"`
}

// MSKBrokerSoftwareInfo holds software version information for a broker node.
type MSKBrokerSoftwareInfo struct {
	// KafkaVersion is the Apache Kafka version running on the broker.
	KafkaVersion string `json:"KafkaVersion"`
}

// The wire is a different thing from the state, and the types below exist to keep
// them apart.
//
// The state types above are a persisted format: MemoryStateManager snapshots them
// and recorded runs replay from those bytes, so their PascalCase tags must not
// change. The kafka API's wire members are lowerCamel — every response member in
// the model carries a lowerCamel locationName, 579 of them, with no exceptions —
// and botocore matches a response key against that name case-sensitively. A
// PascalCase key therefore matches nothing and parses to nothing: `aws kafka
// list-clusters` returned HTTP 200 with an empty result and no error, because the
// state struct was marshaled straight onto the wire (#529).
//
// So each response gets its own element type, tagged from the model, projected
// from the state by a Wire function. Two consequences are deliberate: substrate's
// own AccountID and Region are simply absent from these types, so the leak cannot
// come back through a field someone adds later; and `omitempty` follows the model's
// optionality, because real MSK omits an unset member rather than sending null, and
// a caller distinguishing absent from null is reading a real observable.
//
// Do not "fix" a casing bug here by retagging a state type above. That conflates
// the two jobs again, and it silently changes the format of every recorded run.

// mskClusterInfoOut is the ClusterInfo element of the v1 DescribeCluster and ListClusters responses.
//
// It carries the published ClusterInfo members the record models (#1199): clusterArn, clusterName,
// state, creationTime, tags, brokerNodeGroupInfo, numberOfBrokerNodes, currentBrokerSoftwareInfo,
// encryptionInfo, clientAuthentication, enhancedMonitoring and storageMode, and since #1196
// currentVersion and — when a seeded cluster settles FAILED — stateInfo. Absent, because nothing in
// substrate holds them: activeOperationArn (no cluster operation is modeled), customerActionStatus,
// loggingInfo, openMonitoring, rebalancing, and the two zookeeper connect strings (no ZooKeeper is
// modeled). An absent member is omitted rather than sent empty.
type mskClusterInfoOut struct {
	ClusterARN                string                      `json:"clusterArn"`
	ClusterName               string                      `json:"clusterName"`
	State                     string                      `json:"state"`
	StateInfo                 *mskStateInfoOut            `json:"stateInfo,omitempty"`
	CurrentVersion            string                      `json:"currentVersion,omitempty"`
	BrokerNodeGroupInfo       *mskBrokerNodeGroupInfoOut  `json:"brokerNodeGroupInfo,omitempty"`
	CurrentBrokerSoftwareInfo mskBrokerSoftwareInfoOut    `json:"currentBrokerSoftwareInfo"`
	NumberOfBrokerNodes       int                         `json:"numberOfBrokerNodes,omitempty"`
	EncryptionInfo            *mskEncryptionInfoOut       `json:"encryptionInfo,omitempty"`
	ClientAuthentication      *mskClientAuthenticationOut `json:"clientAuthentication,omitempty"`
	EnhancedMonitoring        string                      `json:"enhancedMonitoring,omitempty"`
	StorageMode               string                      `json:"storageMode,omitempty"`
	Tags                      map[string]string           `json:"tags,omitempty"`
	CreationTime              time.Time                   `json:"creationTime"`
}

// mskClusterOut is the Cluster element of the v2 DescribeClusterV2 and ListClustersV2 responses,
// checked against v2-clusters.html and v2-clusters-clusterarn.html (#1211). A provisioned cluster
// answers provisioned; a serverless one answers serverless. The Cluster member absent here is
// activeOperationArn, for the reason [mskClusterInfoOut] gives; currentVersion and stateInfo are
// answered as there (#1196).
type mskClusterOut struct {
	ClusterARN     string             `json:"clusterArn"`
	ClusterName    string             `json:"clusterName"`
	ClusterType    string             `json:"clusterType"`
	State          string             `json:"state"`
	StateInfo      *mskStateInfoOut   `json:"stateInfo,omitempty"`
	CurrentVersion string             `json:"currentVersion,omitempty"`
	CreationTime   time.Time          `json:"creationTime"`
	Tags           map[string]string  `json:"tags,omitempty"`
	Provisioned    *mskProvisionedOut `json:"provisioned,omitempty"`
	Serverless     *mskServerlessOut  `json:"serverless,omitempty"`
}

// mskProvisionedOut is the Provisioned member of a v2 Cluster. Absent for the reasons
// [mskClusterInfoOut] gives: customerActionStatus, loggingInfo, openMonitoring, rebalancing and the
// zookeeper connect strings.
type mskProvisionedOut struct {
	BrokerNodeGroupInfo       *mskBrokerNodeGroupInfoOut  `json:"brokerNodeGroupInfo,omitempty"`
	CurrentBrokerSoftwareInfo mskBrokerSoftwareInfoOut    `json:"currentBrokerSoftwareInfo"`
	NumberOfBrokerNodes       int                         `json:"numberOfBrokerNodes,omitempty"`
	EncryptionInfo            *mskEncryptionInfoOut       `json:"encryptionInfo,omitempty"`
	ClientAuthentication      *mskClientAuthenticationOut `json:"clientAuthentication,omitempty"`
	EnhancedMonitoring        string                      `json:"enhancedMonitoring,omitempty"`
	StorageMode               string                      `json:"storageMode,omitempty"`
}

// mskServerlessOut is the Serverless member of a v2 Cluster. kafkaVersion is published and absent:
// a serverless create takes none, so substrate has none to report.
type mskServerlessOut struct {
	VpcConfigs           []mskVpcConfigOut                    `json:"vpcConfigs"`
	ClientAuthentication mskServerlessClientAuthenticationOut `json:"clientAuthentication"`
}

// mskVpcConfigOut is one VpcConfig of a serverless cluster.
type mskVpcConfigOut struct {
	SubnetIDs        []string `json:"subnetIds"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

// mskServerlessClientAuthenticationOut is a serverless cluster's clientAuthentication.
type mskServerlessClientAuthenticationOut struct {
	Sasl *mskServerlessSaslOut `json:"sasl,omitempty"`
}

// mskServerlessSaslOut is a serverless cluster's SASL settings.
type mskServerlessSaslOut struct {
	IAM *mskEnabledOut `json:"iam,omitempty"`
}

// mskEnabledOut is the one-member {enabled} object.
type mskEnabledOut struct {
	Enabled bool `json:"enabled"`
}

// mskEncryptionInfoOut is a cluster's encryptionInfo.
type mskEncryptionInfoOut struct {
	EncryptionAtRest    *mskEncryptionAtRestOut   `json:"encryptionAtRest,omitempty"`
	EncryptionInTransit mskEncryptionInTransitOut `json:"encryptionInTransit"`
}

// mskEncryptionAtRestOut is a cluster's encryptionAtRest.
type mskEncryptionAtRestOut struct {
	DataVolumeKMSKeyID string `json:"dataVolumeKMSKeyId"`
}

// mskEncryptionInTransitOut is a cluster's encryptionInTransit, always answered with the page's
// stated defaults filled in (clientBroker TLS, inCluster true), since those are what the cluster runs
// with when the create named neither.
type mskEncryptionInTransitOut struct {
	ClientBroker string `json:"clientBroker"`
	InCluster    bool   `json:"inCluster"`
}

// mskClientAuthenticationOut is a provisioned cluster's clientAuthentication.
type mskClientAuthenticationOut struct {
	Sasl            *mskSaslOut              `json:"sasl,omitempty"`
	TLS             *mskTLSAuthenticationOut `json:"tls,omitempty"`
	Unauthenticated *mskEnabledOut           `json:"unauthenticated,omitempty"`
}

// mskSaslOut is a provisioned cluster's SASL settings.
type mskSaslOut struct {
	IAM   *mskEnabledOut `json:"iam,omitempty"`
	Scram *mskEnabledOut `json:"scram,omitempty"`
}

// mskTLSAuthenticationOut is TLS client authentication.
type mskTLSAuthenticationOut struct {
	CertificateAuthorityArnList []string `json:"certificateAuthorityArnList,omitempty"`
	Enabled                     bool     `json:"enabled"`
}

// mskClusterTypeOf answers the published ClusterType of a stored cluster. A record from before #1211
// carries none, and every cluster it could describe was provisioned.
func mskClusterTypeOf(c *MSKCluster) string {
	if c.ClusterType == "" {
		return mskClusterTypeProvisioned
	}
	return c.ClusterType
}

const (
	mskClusterTypeProvisioned = "PROVISIONED"
	mskClusterTypeServerless  = "SERVERLESS"
)

// mskClusterInfoWire projects a stored cluster onto the v1 ClusterInfo wire shape.
func mskClusterInfoWire(c *MSKCluster) mskClusterInfoOut {
	out := mskClusterInfoOut{
		ClusterARN:                c.ClusterARN,
		ClusterName:               c.ClusterName,
		State:                     c.State,
		CurrentVersion:            c.CurrentVersion,
		CurrentBrokerSoftwareInfo: mskBrokerSoftwareInfoWire(c),
		NumberOfBrokerNodes:       c.NumberOfBrokerNodes,
		Tags:                      c.Tags,
		CreationTime:              c.CreatedAt,
	}
	if mskClusterTypeOf(c) == mskClusterTypeProvisioned {
		bng := mskBrokerNodeGroupInfoWire(c.BrokerNodeGroupInfo)
		out.BrokerNodeGroupInfo = &bng
		enc := mskEncryptionInfoWire(c.EncryptionInfo)
		out.EncryptionInfo = &enc
		out.ClientAuthentication = mskClientAuthenticationWire(c.ClientAuthentication)
		out.EnhancedMonitoring = c.EnhancedMonitoring
		out.StorageMode = c.StorageMode
	}
	return out
}

// mskClusterWire projects a stored cluster onto the v2 Cluster wire shape.
func mskClusterWire(c *MSKCluster) mskClusterOut {
	out := mskClusterOut{
		ClusterARN:     c.ClusterARN,
		ClusterName:    c.ClusterName,
		ClusterType:    mskClusterTypeOf(c),
		State:          c.State,
		CurrentVersion: c.CurrentVersion,
		CreationTime:   c.CreatedAt,
		Tags:           c.Tags,
	}
	if out.ClusterType == mskClusterTypeServerless && c.Serverless != nil {
		out.Serverless = mskServerlessWire(c.Serverless)
		return out
	}
	bng := mskBrokerNodeGroupInfoWire(c.BrokerNodeGroupInfo)
	enc := mskEncryptionInfoWire(c.EncryptionInfo)
	out.Provisioned = &mskProvisionedOut{
		BrokerNodeGroupInfo:       &bng,
		CurrentBrokerSoftwareInfo: mskBrokerSoftwareInfoWire(c),
		NumberOfBrokerNodes:       c.NumberOfBrokerNodes,
		EncryptionInfo:            &enc,
		ClientAuthentication:      mskClientAuthenticationWire(c.ClientAuthentication),
		EnhancedMonitoring:        c.EnhancedMonitoring,
		StorageMode:               c.StorageMode,
	}
	return out
}

// mskBrokerSoftwareInfoWire projects the brokers' software: the Kafka version and, when the create
// named one, the configuration and its revision.
func mskBrokerSoftwareInfoWire(c *MSKCluster) mskBrokerSoftwareInfoOut {
	out := mskBrokerSoftwareInfoOut{KafkaVersion: c.KafkaVersion}
	if c.ConfigurationInfo != nil {
		out.ConfigurationARN = c.ConfigurationInfo.Arn
		out.ConfigurationRevision = c.ConfigurationInfo.Revision
	}
	return out
}

// mskEncryptionInfoWire projects encryption, filling encryptionInTransit's published defaults.
func mskEncryptionInfoWire(e *MSKEncryptionInfo) mskEncryptionInfoOut {
	clientBroker, inCluster := mskEffectiveInTransit(e)
	out := mskEncryptionInfoOut{EncryptionInTransit: mskEncryptionInTransitOut{ClientBroker: clientBroker, InCluster: inCluster}}
	if e != nil && e.EncryptionAtRest != nil {
		out.EncryptionAtRest = &mskEncryptionAtRestOut{DataVolumeKMSKeyID: e.EncryptionAtRest.DataVolumeKMSKeyID}
	}
	return out
}

// mskEffectiveInTransit answers the in-transit settings a cluster runs with: what the create sent,
// or the page's stated default for whichever it did not — clientBroker "The default value is TLS",
// inCluster "The default value is true".
func mskEffectiveInTransit(e *MSKEncryptionInfo) (clientBroker string, inCluster bool) {
	clientBroker, inCluster = "TLS", true
	if e == nil || e.EncryptionInTransit == nil {
		return clientBroker, inCluster
	}
	if e.EncryptionInTransit.ClientBroker != "" {
		clientBroker = e.EncryptionInTransit.ClientBroker
	}
	if e.EncryptionInTransit.InCluster != nil {
		inCluster = *e.EncryptionInTransit.InCluster
	}
	return clientBroker, inCluster
}

// mskClientAuthenticationWire projects provisioned client authentication, or nil when none was sent.
func mskClientAuthenticationWire(a *MSKClientAuthentication) *mskClientAuthenticationOut {
	if a == nil {
		return nil
	}
	out := &mskClientAuthenticationOut{}
	if a.Sasl != nil {
		out.Sasl = &mskSaslOut{IAM: mskEnabledWire(a.Sasl.IAM), Scram: mskEnabledWire(a.Sasl.Scram)}
	}
	if a.TLS != nil {
		out.TLS = &mskTLSAuthenticationOut{CertificateAuthorityArnList: a.TLS.CertificateAuthorityArnList, Enabled: a.TLS.Enabled}
	}
	out.Unauthenticated = mskEnabledWire(a.Unauthenticated)
	return out
}

// mskServerlessWire projects a serverless configuration.
func mskServerlessWire(s *MSKServerless) *mskServerlessOut {
	out := &mskServerlessOut{VpcConfigs: make([]mskVpcConfigOut, 0, len(s.VpcConfigs))}
	for _, v := range s.VpcConfigs {
		out.VpcConfigs = append(out.VpcConfigs, mskVpcConfigOut(v))
	}
	if s.ClientAuthentication.Sasl != nil {
		out.ClientAuthentication.Sasl = &mskServerlessSaslOut{IAM: mskEnabledWire(s.ClientAuthentication.Sasl.IAM)}
	}
	return out
}

// mskEnabledWire projects an {enabled} object, or nil when absent.
func mskEnabledWire(e *MSKEnabled) *mskEnabledOut {
	if e == nil {
		return nil
	}
	return &mskEnabledOut{Enabled: e.Enabled}
}

// mskBrokerNodeGroupInfoOut is the brokerNodeGroupInfo member of a cluster.
// clientSubnets and instanceType are required in the model and so are always
// present; the rest are optional and omitted when unset.
type mskBrokerNodeGroupInfoOut struct {
	InstanceType   string             `json:"instanceType"`
	ClientSubnets  []string           `json:"clientSubnets"`
	SecurityGroups []string           `json:"securityGroups,omitempty"`
	StorageInfo    *mskStorageInfoOut `json:"storageInfo,omitempty"`
}

// mskStorageInfoOut is the storageInfo member of a broker node group.
type mskStorageInfoOut struct {
	EBSStorageInfo mskEBSStorageInfoOut `json:"ebsStorageInfo"`
}

// mskEBSStorageInfoOut is the ebsStorageInfo member of a storage configuration.
type mskEBSStorageInfoOut struct {
	VolumeSize int `json:"volumeSize,omitempty"`
}

// mskBrokerSoftwareInfoOut is the currentBrokerSoftwareInfo member of a cluster
// or a broker node.
type mskBrokerSoftwareInfoOut struct {
	ConfigurationARN      string `json:"configurationArn,omitempty"`
	ConfigurationRevision int64  `json:"configurationRevision,omitempty"`
	KafkaVersion          string `json:"kafkaVersion,omitempty"`
}

// mskNodeInfoOut is the nodeInfoList element of a ListNodes response. The model
// spells this member nodeARN, not nodeArn — the one MSK response member that is
// not the plain lowerCamel of its name.
type mskNodeInfoOut struct {
	BrokerNodeInfo mskBrokerNodeInfoOut `json:"brokerNodeInfo"`
	InstanceType   string               `json:"instanceType,omitempty"`
	NodeARN        string               `json:"nodeARN"`
	NodeType       string               `json:"nodeType"`
}

// mskBrokerNodeInfoOut is the brokerNodeInfo member of a node.
type mskBrokerNodeInfoOut struct {
	BrokerID                  float64                  `json:"brokerId"`
	ClientSubnet              string                   `json:"clientSubnet,omitempty"`
	CurrentBrokerSoftwareInfo mskBrokerSoftwareInfoOut `json:"currentBrokerSoftwareInfo"`
	Endpoints                 []string                 `json:"endpoints,omitempty"`
}

// mskBrokerNodeGroupInfoWire projects broker node configuration onto the wire.
// An unset storage size is omitted rather than reported as 0, which is a size the
// API rejects and real MSK never returns.
func mskBrokerNodeGroupInfoWire(b MSKBrokerNodeGroupInfo) mskBrokerNodeGroupInfoOut {
	out := mskBrokerNodeGroupInfoOut{
		InstanceType:   b.InstanceType,
		ClientSubnets:  b.ClientSubnets,
		SecurityGroups: b.SecurityGroups,
	}
	if b.StorageInfo.EbsStorageInfo.VolumeSize != 0 {
		out.StorageInfo = &mskStorageInfoOut{
			EBSStorageInfo: mskEBSStorageInfoOut{VolumeSize: b.StorageInfo.EbsStorageInfo.VolumeSize},
		}
	}
	return out
}

// mskNodeInfoWire projects a broker node onto the wire.
func mskNodeInfoWire(n MSKNodeInfo) mskNodeInfoOut {
	return mskNodeInfoOut{
		BrokerNodeInfo: mskBrokerNodeInfoOut{
			BrokerID:     n.BrokerNodeInfo.BrokerID,
			ClientSubnet: n.BrokerNodeInfo.ClientSubnet,
			CurrentBrokerSoftwareInfo: mskBrokerSoftwareInfoOut{
				KafkaVersion: n.BrokerNodeInfo.CurrentBrokerSoftwareInfo.KafkaVersion,
			},
			Endpoints: n.BrokerNodeInfo.Endpoints,
		},
		InstanceType: n.InstanceType,
		NodeARN:      n.NodeARN,
		NodeType:     n.NodeType,
	}
}
