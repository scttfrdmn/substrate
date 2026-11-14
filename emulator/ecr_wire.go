package emulator

// The wire is a different thing from the state, and the types below exist to keep them
// apart.
//
// ECRRepository (ecr_types.go) is a persisted state record, and it was handed straight
// to the caller at all three sites that answer a repository — createRepository,
// describeRepositories and deleteRepository. Six of its fields are substrate's own, so
// all three answered members API_Repository does not publish: AccountID and Region
// unconditionally, because neither carried omitempty, and Tags, LifecyclePolicy,
// RepositoryPolicy and ever_tagged from whichever of them had a value. A repository's
// tags are readable through ListTagsForResource and its two policies through
// GetLifecyclePolicy and GetRepositoryPolicy, which is where AWS puts them; a
// DescribeRepositories response publishes no policy member at all (#1013, #1090).
//
// So the repository gets its own type, tagged from the model, projected from the state.
// This is the pattern #529 established for API Gateway v1 in apigateway_wire.go and
// #1013 repeated for DynamoDB in dynamodb_wire.go, and it is here for the same reason:
// a state record grows fields for substrate's own bookkeeping, and a projection is what
// stops the next one from reaching a response.
//
// Do not "fix" a leak here by adding `json:"-"` to a state field. That works for the
// one field and leaves the next one to be remembered rather than prevented, and it
// changes the format of every recorded run, because MemoryStateManager snapshots those
// bytes and a replay reads them back. For the same reason CreatedAt stays a time.Time
// in the record and is converted on projection rather than being retyped in place.

// ecrImageScanningConfigurationOut is the imageScanningConfiguration member of a
// repository response. API_ImageScanningConfiguration publishes scanOnPush and nothing
// else, so this mirrors the shape exactly.
type ecrImageScanningConfigurationOut struct {
	ScanOnPush bool `json:"scanOnPush"`
}

// ecrEncryptionConfigurationOut is the encryptionConfiguration member of a repository
// response.
//
// API_EncryptionConfiguration publishes encryptionType and kmsKey. kmsKey is absent
// rather than present and empty because substrate does not model a customer managed
// key, and AWS's own CreateRepository sample response omits it — so its absence is a
// shape the page publishes.
type ecrEncryptionConfigurationOut struct {
	EncryptionType string `json:"encryptionType"`
}

// ecrRepositoryOut is the repository element of the ECR repository responses:
// CreateRepository, DeleteRepository, and — under the name repositories —
// DescribeRepositories.
//
// Member names and optionality follow API_Repository, whose nine members are all
// Required: No. The one substrate does not model,
// imageTagMutabilityExclusionFilters, is simply absent from the type rather than
// present and empty, so this reports nothing AWS would not (#1013's rule).
//
// createdAt is EpochSeconds rather than a time.Time because ECR speaks
// application/x-amz-json-1.1, whose timestamps are epoch seconds: AWS's own sample
// response answers 1.563223656E9, and the SDK v2 decoder calls ParseEpochSeconds on a
// timestamp member, which an RFC3339 string does not satisfy.
type ecrRepositoryOut struct {
	RepositoryName             string                           `json:"repositoryName"`
	RepositoryArn              string                           `json:"repositoryArn"`
	RegistryID                 string                           `json:"registryId"`
	RepositoryURI              string                           `json:"repositoryUri"`
	CreatedAt                  EpochSeconds                     `json:"createdAt"`
	ImageTagMutability         string                           `json:"imageTagMutability"`
	ImageScanningConfiguration ecrImageScanningConfigurationOut `json:"imageScanningConfiguration"`
	EncryptionConfiguration    ecrEncryptionConfigurationOut    `json:"encryptionConfiguration"`
}

// ecrRepositoryToWire projects a persisted repository onto the published shape.
//
// An empty ImageTagMutability becomes MUTABLE, which is both the default
// API_CreateRepository publishes for an omitted member and what a record written before
// #1090 holds — the member did not exist then, so a stored empty string means "the
// default was taken" rather than "no setting". Defaulting here as well as at create
// time is what keeps an already-recorded run reporting a published value.
func ecrRepositoryToWire(repo ECRRepository) ecrRepositoryOut {
	mutability := repo.ImageTagMutability
	if mutability == "" {
		mutability = ecrImageTagMutabilityDefault
	}
	return ecrRepositoryOut{
		RepositoryName:     repo.RepositoryName,
		RepositoryArn:      repo.RepositoryArn,
		RegistryID:         repo.RegistryID,
		RepositoryURI:      repo.RepositoryURI,
		CreatedAt:          EpochSeconds(repo.CreatedAt),
		ImageTagMutability: mutability,
		ImageScanningConfiguration: ecrImageScanningConfigurationOut{
			ScanOnPush: repo.ImageScanningConfiguration.ScanOnPush,
		},
		EncryptionConfiguration: ecrEncryptionConfigurationOut{
			EncryptionType: repo.EncryptionConfiguration.EncryptionType,
		},
	}
}

// ecrRepositoriesToWire projects a slice of persisted repositories, for
// DescribeRepositories.
func ecrRepositoriesToWire(repos []ECRRepository) []ecrRepositoryOut {
	out := make([]ecrRepositoryOut, 0, len(repos))
	for _, repo := range repos {
		out = append(out, ecrRepositoryToWire(repo))
	}
	return out
}

// --- Pull through cache rules (#1461) ----------------------------------------
//
// Five operations answer a rule, and API_PullThroughCacheRule is the only one of the five
// shapes that is a named type: the other four are each operation's own Response Syntax, and
// they do not agree. CreatePullThroughCacheRule answers no updatedAt; DeletePullThroughCacheRule
// answers neither updatedAt nor upstreamRegistry; UpdatePullThroughCacheRule answers neither
// createdAt, upstreamRegistry nor upstreamRegistryUrl; ValidatePullThroughCacheRule answers no
// date at all and adds isValid and failure. Each therefore gets its own struct, transcribed from
// its page, rather than one struct with the differences left to omitempty — a member present on
// one response and published on another is the near-miss #1013's rule exists to prevent.

// ecrPullThroughCacheRuleOut is one entry of DescribePullThroughCacheRules' pullThroughCacheRules,
// mirroring API_PullThroughCacheRule's nine members, all Required: No.
type ecrPullThroughCacheRuleOut struct {
	CreatedAt                EpochSeconds `json:"createdAt"`
	CredentialArn            string       `json:"credentialArn,omitempty"`
	CustomRoleArn            string       `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string       `json:"ecrRepositoryPrefix"`
	RegistryID               string       `json:"registryId"`
	UpdatedAt                EpochSeconds `json:"updatedAt"`
	UpstreamRegistry         string       `json:"upstreamRegistry"`
	UpstreamRegistryURL      string       `json:"upstreamRegistryUrl"`
	UpstreamRepositoryPrefix string       `json:"upstreamRepositoryPrefix,omitempty"`
}

// ecrPullThroughCacheRuleToWire projects a persisted rule onto API_PullThroughCacheRule.
func ecrPullThroughCacheRuleToWire(rule ECRPullThroughCacheRule) ecrPullThroughCacheRuleOut {
	return ecrPullThroughCacheRuleOut{
		CreatedAt:                EpochSeconds(rule.CreatedAt),
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		UpdatedAt:                EpochSeconds(rule.UpdatedAt),
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	}
}

// ecrCreatePullThroughCacheRuleOut is API_CreatePullThroughCacheRule's Response Syntax.
type ecrCreatePullThroughCacheRuleOut struct {
	CreatedAt                EpochSeconds `json:"createdAt"`
	CredentialArn            string       `json:"credentialArn,omitempty"`
	CustomRoleArn            string       `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string       `json:"ecrRepositoryPrefix"`
	RegistryID               string       `json:"registryId"`
	UpstreamRegistry         string       `json:"upstreamRegistry"`
	UpstreamRegistryURL      string       `json:"upstreamRegistryUrl"`
	UpstreamRepositoryPrefix string       `json:"upstreamRepositoryPrefix,omitempty"`
}

// ecrDeletePullThroughCacheRuleOut is API_DeletePullThroughCacheRule's Response Syntax, which
// publishes no upstreamRegistry and no updatedAt.
type ecrDeletePullThroughCacheRuleOut struct {
	CreatedAt                EpochSeconds `json:"createdAt"`
	CredentialArn            string       `json:"credentialArn,omitempty"`
	CustomRoleArn            string       `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string       `json:"ecrRepositoryPrefix"`
	RegistryID               string       `json:"registryId"`
	UpstreamRegistryURL      string       `json:"upstreamRegistryUrl"`
	UpstreamRepositoryPrefix string       `json:"upstreamRepositoryPrefix,omitempty"`
}

// ecrUpdatePullThroughCacheRuleOut is API_UpdatePullThroughCacheRule's Response Syntax, which
// publishes no createdAt, upstreamRegistry or upstreamRegistryUrl.
type ecrUpdatePullThroughCacheRuleOut struct {
	CredentialArn            string       `json:"credentialArn,omitempty"`
	CustomRoleArn            string       `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string       `json:"ecrRepositoryPrefix"`
	RegistryID               string       `json:"registryId"`
	UpdatedAt                EpochSeconds `json:"updatedAt"`
	UpstreamRepositoryPrefix string       `json:"upstreamRepositoryPrefix,omitempty"`
}

// ecrValidatePullThroughCacheRuleOut is API_ValidatePullThroughCacheRule's Response Syntax.
//
// failure is absent rather than empty on a valid rule: the page describes it only as "the reason
// the validation failed", so there is nothing to report when it did not.
type ecrValidatePullThroughCacheRuleOut struct {
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	Failure                  string `json:"failure,omitempty"`
	IsValid                  bool   `json:"isValid"`
	RegistryID               string `json:"registryId"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
}

// --- Repository creation templates (#1461) -----------------------------------

// ecrCreationTemplateEncryptionOut is a template's encryptionConfiguration, as
// API_EncryptionConfigurationForRepositoryCreationTemplate publishes it: encryptionType, and
// kmsKey only when one was given.
type ecrCreationTemplateEncryptionOut struct {
	EncryptionType string `json:"encryptionType"`
	KMSKey         string `json:"kmsKey,omitempty"`
}

// ecrRepositoryCreationTemplateOut is API_RepositoryCreationTemplate, the element all four
// template operations answer.
//
// resourceTags is always rendered, as `[]` when the template carries none, for the reason
// [ecrTagList] records; the other optional strings are absent when unset, as the
// DescribeRepositoryCreationTemplates sample response shows them.
type ecrRepositoryCreationTemplateOut struct {
	AppliedFor                         []string                               `json:"appliedFor"`
	CreatedAt                          EpochSeconds                           `json:"createdAt"`
	CustomRoleArn                      string                                 `json:"customRoleArn,omitempty"`
	Description                        string                                 `json:"description,omitempty"`
	EncryptionConfiguration            ecrCreationTemplateEncryptionOut       `json:"encryptionConfiguration"`
	ImageTagMutability                 string                                 `json:"imageTagMutability"`
	ImageTagMutabilityExclusionFilters []ECRImageTagMutabilityExclusionFilter `json:"imageTagMutabilityExclusionFilters,omitempty"`
	LifecyclePolicy                    string                                 `json:"lifecyclePolicy,omitempty"`
	Prefix                             string                                 `json:"prefix"`
	RepositoryPolicy                   string                                 `json:"repositoryPolicy,omitempty"`
	ResourceTags                       []ecrTag                               `json:"resourceTags"`
	UpdatedAt                          EpochSeconds                           `json:"updatedAt"`
}

// ecrCreationTemplateToWire projects a persisted template onto API_RepositoryCreationTemplate.
func ecrCreationTemplateToWire(tmpl ECRRepositoryCreationTemplate) ecrRepositoryCreationTemplateOut {
	return ecrRepositoryCreationTemplateOut{
		AppliedFor:                         append([]string{}, tmpl.AppliedFor...),
		CreatedAt:                          EpochSeconds(tmpl.CreatedAt),
		CustomRoleArn:                      tmpl.CustomRoleArn,
		Description:                        tmpl.Description,
		EncryptionConfiguration:            ecrCreationTemplateEncryptionOut{EncryptionType: tmpl.EncryptionType, KMSKey: tmpl.KMSKey},
		ImageTagMutability:                 tmpl.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: tmpl.ImageTagMutabilityExclusionFilters,
		LifecyclePolicy:                    tmpl.LifecyclePolicy,
		Prefix:                             tmpl.Prefix,
		RepositoryPolicy:                   tmpl.RepositoryPolicy,
		ResourceTags:                       ecrTagList(tmpl.ResourceTags),
		UpdatedAt:                          EpochSeconds(tmpl.UpdatedAt),
	}
}
