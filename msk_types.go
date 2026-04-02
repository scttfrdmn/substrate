package substrate

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
	// BrokerId is the numeric broker identifier (1-based).
	BrokerId float64 `json:"BrokerId"`
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
