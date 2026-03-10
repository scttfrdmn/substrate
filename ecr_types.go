package substrate

import "time"

// ecrNamespace is the state namespace for ECR.
const ecrNamespace = "ecr"

// ECRRepository holds the persisted state of an ECR repository.
type ECRRepository struct {
	RepositoryName             string    `json:"repositoryName"`
	RepositoryArn              string    `json:"repositoryArn"`
	RegistryID                 string    `json:"registryId"`
	RepositoryURI              string    `json:"repositoryUri"`
	CreatedAt                  time.Time `json:"createdAt"`
	ImageScanningConfiguration struct {
		ScanOnPush bool `json:"scanOnPush"`
	} `json:"imageScanningConfiguration"`
	EncryptionConfiguration struct {
		EncryptionType string `json:"encryptionType"`
	} `json:"encryptionConfiguration"`
	Tags             map[string]string `json:"Tags,omitempty"`
	LifecyclePolicy  string            `json:"LifecyclePolicy,omitempty"`
	RepositoryPolicy string            `json:"RepositoryPolicy,omitempty"`
	AccountID        string            `json:"AccountID"`
	Region           string            `json:"Region"`
}

// ECRImage holds metadata for an image stored in an ECR repository.
type ECRImage struct {
	ImageDigest      string    `json:"imageDigest"`
	ImageTag         string    `json:"imageTag,omitempty"`
	ImagePushedAt    time.Time `json:"imagePushedAt"`
	ImageSizeInBytes int64     `json:"imageSizeInBytes"`
	ImageManifest    string    `json:"imageManifest,omitempty"`
	RepoName         string    `json:"repoName"`
}
