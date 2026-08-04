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

// mskClusterInfoOut is the ClusterInfo element of the v1 DescribeCluster and
// ListClusters responses.
type mskClusterInfoOut struct {
	ClusterARN                string                    `json:"clusterArn"`
	ClusterName               string                    `json:"clusterName"`
	State                     string                    `json:"state"`
	BrokerNodeGroupInfo       mskBrokerNodeGroupInfoOut `json:"brokerNodeGroupInfo"`
	CurrentBrokerSoftwareInfo mskBrokerSoftwareInfoOut  `json:"currentBrokerSoftwareInfo"`
	NumberOfBrokerNodes       int                       `json:"numberOfBrokerNodes"`
	Tags                      map[string]string         `json:"tags,omitempty"`
	CreationTime              time.Time                 `json:"creationTime"`
}

// mskClusterOut is the Cluster element of the v2 DescribeClusterV2 and
// ListClustersV2 responses, which wraps the provisioned detail in a sub-object.
type mskClusterOut struct {
	ClusterARN   string            `json:"clusterArn"`
	ClusterName  string            `json:"clusterName"`
	ClusterType  string            `json:"clusterType"`
	State        string            `json:"state"`
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags,omitempty"`
	Provisioned  mskProvisionedOut `json:"provisioned"`
}

// mskProvisionedOut is the Provisioned member of a v2 Cluster.
type mskProvisionedOut struct {
	BrokerNodeGroupInfo       mskBrokerNodeGroupInfoOut `json:"brokerNodeGroupInfo"`
	CurrentBrokerSoftwareInfo mskBrokerSoftwareInfoOut  `json:"currentBrokerSoftwareInfo"`
	NumberOfBrokerNodes       int                       `json:"numberOfBrokerNodes"`
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
	KafkaVersion string `json:"kafkaVersion,omitempty"`
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
}

// mskClusterInfoWire projects a stored cluster onto the v1 ClusterInfo wire shape.
func mskClusterInfoWire(c *MSKCluster) mskClusterInfoOut {
	return mskClusterInfoOut{
		ClusterARN:                c.ClusterARN,
		ClusterName:               c.ClusterName,
		State:                     c.State,
		BrokerNodeGroupInfo:       mskBrokerNodeGroupInfoWire(c.BrokerNodeGroupInfo),
		CurrentBrokerSoftwareInfo: mskBrokerSoftwareInfoOut{KafkaVersion: c.KafkaVersion},
		NumberOfBrokerNodes:       c.NumberOfBrokerNodes,
		Tags:                      c.Tags,
		CreationTime:              c.CreatedAt,
	}
}

// mskClusterWire projects a stored cluster onto the v2 Cluster wire shape. Every
// cluster substrate creates is provisioned; serverless is not modeled, so
// clusterType is constant rather than derived.
func mskClusterWire(c *MSKCluster) mskClusterOut {
	return mskClusterOut{
		ClusterARN:   c.ClusterARN,
		ClusterName:  c.ClusterName,
		ClusterType:  "PROVISIONED",
		State:        c.State,
		CreationTime: c.CreatedAt,
		Tags:         c.Tags,
		Provisioned: mskProvisionedOut{
			BrokerNodeGroupInfo:       mskBrokerNodeGroupInfoWire(c.BrokerNodeGroupInfo),
			CurrentBrokerSoftwareInfo: mskBrokerSoftwareInfoOut{KafkaVersion: c.KafkaVersion},
			NumberOfBrokerNodes:       c.NumberOfBrokerNodes,
		},
	}
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
		},
		InstanceType: n.InstanceType,
		NodeARN:      n.NodeARN,
		NodeType:     n.NodeType,
	}
}
