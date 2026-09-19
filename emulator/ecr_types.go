package emulator

import "time"

// ecrNamespace is the state namespace for ECR.
const ecrNamespace = "ecr"

// ecrImageTagMutabilityDefault is the setting a CreateRepository request that omits
// imageTagMutability takes.
//
// API_CreateRepository publishes it in as many words: "If this parameter is omitted, the
// default setting of MUTABLE will be used which will allow image tags to be
// overwritten." (#1090).
const ecrImageTagMutabilityDefault = "MUTABLE"

// ecrImageTagMutabilityValues is the Valid Values set API_Repository publishes for
// imageTagMutability.
//
// Four values, not the two the setting was introduced with: the two _WITH_EXCLUSION
// forms accompany imageTagMutabilityExclusionFilters, which substrate does not model.
// They are accepted anyway rather than refused, because those filters are Required: No on
// every page that publishes them — so a request naming one of the four without filters is
// a request AWS accepts, and refusing it would be substrate inventing a bound.
var ecrImageTagMutabilityValues = map[string]bool{
	"MUTABLE":                  true,
	"IMMUTABLE":                true,
	"IMMUTABLE_WITH_EXCLUSION": true,
	"MUTABLE_WITH_EXCLUSION":   true,
}

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

	// ImageTagMutability is the published setting CreateRepository accepts (#1090). A
	// record written before it existed holds "", which the wire projection reads as the
	// published default; see [ecrRepositoryToWire].
	ImageTagMutability string `json:"imageTagMutability,omitempty"`

	Tags             map[string]string `json:"Tags,omitempty"`
	LifecyclePolicy  string            `json:"LifecyclePolicy,omitempty"`
	RepositoryPolicy string            `json:"RepositoryPolicy,omitempty"`
	AccountID        string            `json:"AccountID"`
	Region           string            `json:"Region"`

	// EverTagged records that this repository has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
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
