package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// Amazon ECR repository creation templates, and the repositories ECR creates on a caller's behalf
// (#1461).
//
// A template is a set of repository settings keyed by a namespace prefix, applied "only … during
// repository creation" to a repository Amazon ECR creates itself — through pull through cache,
// create on push, or replication — and never to one CreateRepository makes. Substrate models the
// four template operations and two of the three scenarios, each at the one API call that is
// observably the scenario's trigger:
//
//   - **PULL_THROUGH_CACHE — BatchGetImage.** "When an image is pulled, the BatchGetImage API is
//     called once to retrieve the image manifest", and BatchGetImage is the one operation that
//     publishes UnableToGetUpstreamImageException, "The image or images were unable to be pulled
//     using the pull through cache rule". So a BatchGetImage naming an absent repository that falls
//     under a pull through cache rule creates the repository — from the matching template, or with
//     the published defaults when there is none. See [ECRPlugin.pullThroughCacheRepository].
//   - **CREATE_ON_PUSH — PutImage.** "When an image is pushed and all new image layers have been
//     uploaded, the PutImage API is called once", and a template applies "when you push an image to
//     a repository that does not yet exist". Unlike pull through cache there is no default: "When
//     there isn't a repository creation template that matches the target repository for an image
//     push, Amazon ECR will not create a repository", so without a CREATE_ON_PUSH template PutImage
//     still answers RepositoryNotFoundException.
//   - **REPLICATION** is not modeled, because substrate models no replication configuration; a
//     template may name it and is stored and echoed, and nothing applies it.
//
// What a pull creates is the repository and nothing in it. Substrate does not reach an upstream
// registry, so the image a real pull would cache is not there to answer: BatchGetImage reports each
// requested image under failures, exactly as it does for any repository that lacks it, and a test
// that needs the manifest pushes it with PutImage. That is the scope rule — the repository's
// creation is observable through the API, the upstream's content is not.
//
// One published behavior is not modeled: the user guide says a template's repository creation
// role "must be provided when using repository tags and/or KMS in the template, otherwise the
// repository creation will fail". Substrate applies resourceTags without a customRoleArn, because it
// models no role assumption for ECR and no page publishes what the failure looks like.

// ecrAppliedForValues is the appliedFor Valid Values set API_CreateRepositoryCreationTemplate
// publishes.
var ecrAppliedForValues = map[string]bool{
	"REPLICATION":        true,
	"PULL_THROUGH_CACHE": true,
	"CREATE_ON_PUSH":     true,
}

// ecrTemplateEncryptionTypes is EncryptionConfigurationForRepositoryCreationTemplate's
// encryptionType Valid Values set.
var ecrTemplateEncryptionTypes = map[string]bool{"AES256": true, "KMS": true, "KMS_DSSE": true}

// ecrTemplateKMSKeyPattern is the published kmsKey pattern.
//
// The page prints it as `^$|arn:aws[a-z0-9-]*:kms:[a-z0-9-]+:[0-9]{12}:key\/[a-z0-9-]+`, whose `^`
// and `$` bind only the empty branch; read with Go's search semantics that accepts any string
// containing a key ARN. Both branches are anchored here, which is the whole-value match the
// constraint means.
var ecrTemplateKMSKeyPattern = regexp.MustCompile(`^(|arn:aws[a-z0-9-]*:kms:[a-z0-9-]+:[0-9]{12}:key\/[a-z0-9-]+)$`)

// ecrExclusionFilterPattern is the published ImageTagMutabilityExclusionFilter filter pattern.
var ecrExclusionFilterPattern = regexp.MustCompile(`^[0-9a-zA-Z._*-]{1,128}$`)

func ecrCreationTemplatePrefix(accountID, region string) string {
	return "ecrtmpl:" + accountID + "/" + region + "/"
}

func ecrCreationTemplateKey(accountID, region, prefix string) string {
	return ecrCreationTemplatePrefix(accountID, region) + ecrPrefixKey(prefix)
}

// ecrTemplateBody is the request body CreateRepositoryCreationTemplate and
// UpdateRepositoryCreationTemplate share. Every optional member is a pointer, so Update can tell an
// omitted member — which keeps its value — from one set to its zero.
type ecrTemplateBody struct {
	Prefix                             string                                  `json:"prefix"`
	Description                        *string                                 `json:"description"`
	AppliedFor                         *[]string                               `json:"appliedFor"`
	ImageTagMutability                 *string                                 `json:"imageTagMutability"`
	ImageTagMutabilityExclusionFilters *[]ECRImageTagMutabilityExclusionFilter `json:"imageTagMutabilityExclusionFilters"`
	EncryptionConfiguration            *struct {
		EncryptionType string `json:"encryptionType"`
		KMSKey         string `json:"kmsKey"`
	} `json:"encryptionConfiguration"`
	LifecyclePolicy  *string   `json:"lifecyclePolicy"`
	RepositoryPolicy *string   `json:"repositoryPolicy"`
	ResourceTags     *[]ecrTag `json:"resourceTags"`
	CustomRoleArn    *string   `json:"customRoleArn"`
}

// ecrMaxLen refuses a string member longer than its published maximum.
func ecrMaxLen(member string, value *string, maxLen int) *AWSError {
	if value != nil && len(*value) > maxLen {
		return ecrInvalidParameter(fmt.Sprintf(
			"Invalid parameter at '%s' failed to satisfy constraint: 'Member must have length less than or equal to %d'", member, maxLen))
	}
	return nil
}

// validate checks every member the body carries against its published constraint. It reads no
// state, so a malformed request is refused whether or not the prefix names a template.
func (b *ecrTemplateBody) validate() *AWSError {
	if b.Prefix == "" {
		return ecrInvalidParameter("prefix is required")
	}
	if awsErr := ecrValidatePrefix("prefix", b.Prefix, 1, 256); awsErr != nil {
		return awsErr
	}
	if b.AppliedFor != nil {
		// Required: Yes on create with no published minimum; a template that applies to no
		// scenario can never apply, so an empty list is refused rather than stored.
		if len(*b.AppliedFor) == 0 {
			return ecrInvalidParameter("appliedFor must name at least one of REPLICATION, PULL_THROUGH_CACHE, CREATE_ON_PUSH")
		}
		for _, v := range *b.AppliedFor {
			if !ecrAppliedForValues[v] {
				return ecrInvalidParameter("Invalid parameter at 'appliedFor' failed to satisfy constraint: " +
					"'Member must satisfy enum value set: [REPLICATION, PULL_THROUGH_CACHE, CREATE_ON_PUSH]'")
			}
		}
	}
	if b.ImageTagMutability != nil && !ecrImageTagMutabilityValues[*b.ImageTagMutability] {
		return ecrInvalidParameter("Invalid parameter at 'imageTagMutability' failed to satisfy constraint: " +
			"'Member must satisfy enum value set: [MUTABLE, IMMUTABLE, IMMUTABLE_WITH_EXCLUSION, MUTABLE_WITH_EXCLUSION]'")
	}
	if b.ImageTagMutabilityExclusionFilters != nil {
		filters := *b.ImageTagMutabilityExclusionFilters
		if len(filters) < 1 || len(filters) > 5 {
			return ecrInvalidParameter("Invalid parameter at 'imageTagMutabilityExclusionFilters' failed to satisfy constraint: 'Member must have length between 1 and 5'")
		}
		for _, f := range filters {
			if f.FilterType != "WILDCARD" || !ecrExclusionFilterPattern.MatchString(f.Filter) {
				return ecrInvalidParameter("Invalid parameter at 'imageTagMutabilityExclusionFilters': " +
					"filterType must be WILDCARD and filter must satisfy " + ecrExclusionFilterPattern.String())
			}
		}
	}
	if enc := b.EncryptionConfiguration; enc != nil {
		if !ecrTemplateEncryptionTypes[enc.EncryptionType] {
			return ecrInvalidParameter("Invalid parameter at 'encryptionConfiguration.encryptionType' failed to satisfy constraint: " +
				"'Member must satisfy enum value set: [AES256, KMS, KMS_DSSE]'")
		}
		if len(enc.KMSKey) > 2048 || !ecrTemplateKMSKeyPattern.MatchString(enc.KMSKey) {
			return ecrInvalidParameter("Invalid parameter at 'encryptionConfiguration.kmsKey' failed to satisfy constraint: " +
				"'Member must satisfy regular expression pattern: " + ecrTemplateKMSKeyPattern.String() + "'")
		}
	}
	for _, check := range []struct {
		member string
		value  *string
		max    int
	}{
		{"description", b.Description, 256},
		{"lifecyclePolicy", b.LifecyclePolicy, 30720},
		{"repositoryPolicy", b.RepositoryPolicy, 10240},
		{"customRoleArn", b.CustomRoleArn, 2048},
	} {
		if awsErr := ecrMaxLen(check.member, check.value, check.max); awsErr != nil {
			return awsErr
		}
	}
	if b.ResourceTags != nil {
		// "Tag keys can have a maximum character length of 128 characters, and tag values can
		// have a maximum length of 256 characters."
		for _, t := range *b.ResourceTags {
			if len(t.Key) > 128 || len(t.Value) > 256 {
				return ecrInvalidParameter("Invalid parameter at 'resourceTags': tag keys are at most 128 characters and values at most 256")
			}
		}
		if _, awsErr := ecrTagsToMap(*b.ResourceTags); awsErr != nil {
			return awsErr
		}
	}
	return nil
}

// apply writes every member the body carries onto tmpl, leaving the omitted ones as they are. It
// refuses only what [ecrTemplateBody.validate] would, so a caller that validated first gets nil.
func (b *ecrTemplateBody) apply(tmpl *ECRRepositoryCreationTemplate) *AWSError {
	if b.Description != nil {
		tmpl.Description = *b.Description
	}
	if b.AppliedFor != nil {
		tmpl.AppliedFor = append([]string{}, (*b.AppliedFor)...)
	}
	if b.ImageTagMutability != nil {
		tmpl.ImageTagMutability = *b.ImageTagMutability
	}
	if b.ImageTagMutabilityExclusionFilters != nil {
		tmpl.ImageTagMutabilityExclusionFilters = append([]ECRImageTagMutabilityExclusionFilter{}, (*b.ImageTagMutabilityExclusionFilters)...)
	}
	if b.EncryptionConfiguration != nil {
		tmpl.EncryptionType = b.EncryptionConfiguration.EncryptionType
		tmpl.KMSKey = b.EncryptionConfiguration.KMSKey
	}
	if b.LifecyclePolicy != nil {
		tmpl.LifecyclePolicy = *b.LifecyclePolicy
	}
	if b.RepositoryPolicy != nil {
		tmpl.RepositoryPolicy = *b.RepositoryPolicy
	}
	if b.ResourceTags != nil {
		tags, awsErr := ecrTagsToMap(*b.ResourceTags)
		if awsErr != nil {
			return awsErr
		}
		tmpl.ResourceTags = tags
	}
	if b.CustomRoleArn != nil {
		tmpl.CustomRoleArn = *b.CustomRoleArn
	}
	return nil
}

// ecrTemplateResponse is the response all four template operations but Describe answer.
type ecrTemplateResponse struct {
	RegistryID                 string                           `json:"registryId"`
	RepositoryCreationTemplate ecrRepositoryCreationTemplateOut `json:"repositoryCreationTemplate"`
}

func (p *ECRPlugin) createRepositoryCreationTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body ecrTemplateBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ecrInvalidBody()
	}
	if body.AppliedFor == nil {
		return nil, ecrInvalidParameter("appliedFor is required")
	}
	if awsErr := body.validate(); awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	key := ecrCreationTemplateKey(ctx.AccountID, ctx.Region, body.Prefix)
	if _, found, err := p.loadCreationTemplate(goCtx, key); err != nil {
		return nil, err
	} else if found {
		return nil, ecrTemplateAlreadyExists(body.Prefix)
	}

	// The two published defaults: MUTABLE when imageTagMutability is omitted ("the default setting
	// of MUTABLE will be used"), and AES256 for an omitted encryptionConfiguration, which is what
	// the DeleteRepositoryCreationTemplate sample response reports for a template that set none
	// and what the user guide names as the default for a repository ECR creates.
	now := p.tc.Now()
	tmpl := ECRRepositoryCreationTemplate{
		Prefix:             body.Prefix,
		ImageTagMutability: ecrImageTagMutabilityDefault,
		EncryptionType:     "AES256",
		CreatedAt:          now,
		UpdatedAt:          now,
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
	}
	if awsErr := body.apply(&tmpl); awsErr != nil {
		return nil, awsErr
	}
	if err := p.putCreationTemplate(goCtx, key, tmpl); err != nil {
		return nil, err
	}
	return ecrJSONResponse(http.StatusOK, ecrTemplateResponse{
		RegistryID:                 ctx.AccountID,
		RepositoryCreationTemplate: ecrCreationTemplateToWire(tmpl),
	})
}

func (p *ECRPlugin) updateRepositoryCreationTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body ecrTemplateBody
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ecrInvalidBody()
	}
	if awsErr := body.validate(); awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	key := ecrCreationTemplateKey(ctx.AccountID, ctx.Region, body.Prefix)
	tmpl, found, err := p.loadCreationTemplate(goCtx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ecrTemplateNotFound(body.Prefix)
	}
	// Omitted members keep their values. API_UpdateRepositoryCreationTemplate's own sample sends
	// only resourceTags and appliedFor and answers the template's description, encryption,
	// lifecycle policy and repository policy unchanged, which is a merge and not a replace — the
	// imageTagMutability entry's "if this parameter is omitted, the default setting of MUTABLE will
	// be used" is read as the create-time default it is copied from, not as an instruction to reset.
	if awsErr := body.apply(&tmpl); awsErr != nil {
		return nil, awsErr
	}
	tmpl.UpdatedAt = p.tc.Now()
	if err := p.putCreationTemplate(goCtx, key, tmpl); err != nil {
		return nil, err
	}
	return ecrJSONResponse(http.StatusOK, ecrTemplateResponse{
		RegistryID:                 ctx.AccountID,
		RepositoryCreationTemplate: ecrCreationTemplateToWire(tmpl),
	})
}

func (p *ECRPlugin) deleteRepositoryCreationTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ecrInvalidBody()
	}
	if body.Prefix == "" {
		return nil, ecrInvalidParameter("prefix is required")
	}
	if awsErr := ecrValidatePrefix("prefix", body.Prefix, 1, 256); awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	key := ecrCreationTemplateKey(ctx.AccountID, ctx.Region, body.Prefix)
	tmpl, found, err := p.loadCreationTemplate(goCtx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ecrTemplateNotFound(body.Prefix)
	}
	if err := p.state.Delete(goCtx, ecrNamespace, key); err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryCreationTemplate state.Delete: %w", err)
	}
	return ecrJSONResponse(http.StatusOK, ecrTemplateResponse{
		RegistryID:                 ctx.AccountID,
		RepositoryCreationTemplate: ecrCreationTemplateToWire(tmpl),
	})
}

func (p *ECRPlugin) describeRepositoryCreationTemplates(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Prefixes   []string `json:"prefixes"`
		MaxResults int      `json:"maxResults"`
		NextToken  string   `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecrInvalidBody()
		}
	}
	for _, prefix := range body.Prefixes {
		if awsErr := ecrValidatePrefix("prefixes", prefix, 1, 256); awsErr != nil {
			return nil, awsErr
		}
	}
	// No exclusion sentence is published on either pagination member, so the filtered form pages
	// too (ecr_pagination.go).
	pageReq, err := ecrDecodePage(body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	templates, err := p.listCreationTemplates(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	// A named prefix that matches no template is omitted rather than refused: the operation
	// publishes no TemplateNotFoundException, only InvalidParameterException, ValidationException
	// and ServerException.
	if len(body.Prefixes) > 0 {
		wanted := make(map[string]bool, len(body.Prefixes))
		for _, prefix := range body.Prefixes {
			wanted[ecrPrefixKey(prefix)] = true
		}
		filtered := templates[:0]
		for _, tmpl := range templates {
			if wanted[ecrPrefixKey(tmpl.Prefix)] {
				filtered = append(filtered, tmpl)
			}
		}
		templates = filtered
	}

	page, next := pageByOffsetToken(templates, pageReq.offset, pageReq.pageSize)
	out := make([]ecrRepositoryCreationTemplateOut, 0, len(page))
	for _, tmpl := range page {
		out = append(out, ecrCreationTemplateToWire(tmpl))
	}
	type response struct {
		NextToken                   string                             `json:"nextToken,omitempty"`
		RegistryID                  string                             `json:"registryId"`
		RepositoryCreationTemplates []ecrRepositoryCreationTemplateOut `json:"repositoryCreationTemplates"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		NextToken:                   next,
		RegistryID:                  ctx.AccountID,
		RepositoryCreationTemplates: out,
	})
}

// --- Persistence -------------------------------------------------------------

func (p *ECRPlugin) putCreationTemplate(goCtx context.Context, key string, tmpl ECRRepositoryCreationTemplate) error {
	data, err := json.Marshal(tmpl)
	if err != nil {
		return fmt.Errorf("ecr creation template marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, key, data); err != nil {
		return fmt.Errorf("ecr creation template state.Put: %w", err)
	}
	return nil
}

func (p *ECRPlugin) loadCreationTemplate(goCtx context.Context, key string) (ECRRepositoryCreationTemplate, bool, error) {
	data, err := p.state.Get(goCtx, ecrNamespace, key)
	if err != nil {
		return ECRRepositoryCreationTemplate{}, false, fmt.Errorf("ecr creation template state.Get: %w", err)
	}
	if data == nil {
		return ECRRepositoryCreationTemplate{}, false, nil
	}
	var tmpl ECRRepositoryCreationTemplate
	if err := json.Unmarshal(data, &tmpl); err != nil {
		return ECRRepositoryCreationTemplate{}, false, fmt.Errorf("ecr creation template unmarshal: %w", err)
	}
	return tmpl, true, nil
}

// listCreationTemplates returns every template in a registry, in prefix order.
func (p *ECRPlugin) listCreationTemplates(goCtx context.Context, accountID, region string) ([]ECRRepositoryCreationTemplate, error) {
	keys, err := p.state.List(goCtx, ecrNamespace, ecrCreationTemplatePrefix(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("ecr list creation templates: %w", err)
	}
	templates := make([]ECRRepositoryCreationTemplate, 0, len(keys))
	for _, key := range keys {
		tmpl, found, err := p.loadCreationTemplate(goCtx, key)
		if err != nil {
			return nil, err
		}
		if found {
			templates = append(templates, tmpl)
		}
	}
	return templates, nil
}

// matchingCreationTemplate returns the template that governs a new repository created for scenario,
// or nil when none does.
//
// The user guide's rule, applied to templates whose appliedFor names the scenario: a prefix P
// applies to repositories beginning `P/`; "if one template has the prefix "prod" and the other has
// the prefix "prod/team", the template with the prefix "prod/team" will be applied to all
// repositories whose names start with "prod/team/""; and ROOT applies "to all repositories in your
// registry that don't have an associated creation template".
func (p *ECRPlugin) matchingCreationTemplate(goCtx context.Context, accountID, region, name, scenario string) (*ECRRepositoryCreationTemplate, error) {
	templates, err := p.listCreationTemplates(goCtx, accountID, region)
	if err != nil {
		return nil, err
	}
	var best, root *ECRRepositoryCreationTemplate
	for i := range templates {
		if !slices.Contains(templates[i].AppliedFor, scenario) {
			continue
		}
		key := ecrPrefixKey(templates[i].Prefix)
		if key == ecrPrefixRoot {
			root = &templates[i]
			continue
		}
		if strings.HasPrefix(name, key+"/") {
			if best == nil || len(key) > len(ecrPrefixKey(best.Prefix)) {
				best = &templates[i]
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return root, nil
}

// --- Repositories created on the caller's behalf ------------------------------

// createRepositoryFromTemplate creates name in the caller's registry the way Amazon ECR does when it
// creates a repository itself, and returns it.
//
// With no template the settings are the user guide's defaults for a repository a pull through cache
// creates: mutable tags, AES256 encryption, and no repository policy, lifecycle policy or resource
// tags. A template replaces each with its own — every template member a repository holds:
// imageTagMutability, imageTagMutabilityExclusionFilters, encryptionType, kmsKey, repositoryPolicy,
// lifecyclePolicy and resourceTags. The template's description, appliedFor and customRoleArn are the
// template's, not the repository's.
func (p *ECRPlugin) createRepositoryFromTemplate(goCtx context.Context, ctx *RequestContext, name string, tmpl *ECRRepositoryCreationTemplate) (*ECRRepository, error) {
	repo := newECRRepositoryRecord(ctx, name, p.tc.Now())
	if tmpl != nil {
		repo.ImageTagMutability = tmpl.ImageTagMutability
		repo.ImageTagMutabilityExclusionFilters = slices.Clone(tmpl.ImageTagMutabilityExclusionFilters)
		repo.EncryptionConfiguration.EncryptionType = tmpl.EncryptionType
		repo.EncryptionConfiguration.KMSKey = tmpl.KMSKey
		repo.RepositoryPolicy = tmpl.RepositoryPolicy
		repo.LifecyclePolicy = tmpl.LifecyclePolicy
		if len(tmpl.ResourceTags) > 0 {
			repo.Tags = make(map[string]string, len(tmpl.ResourceTags))
			for k, v := range tmpl.ResourceTags {
				repo.Tags[k] = v
			}
			repo.EverTagged = taggingEverTagged(false, 0, len(repo.Tags))
		}
	}
	if err := p.storeNewRepository(goCtx, ctx, repo); err != nil {
		return nil, err
	}
	return &repo, nil
}

// pullThroughCacheRepository creates the repository a pull through cache rule names, when name is
// absent from the caller's registry and falls under a rule, and returns it — nil when no rule applies
// or the pull would find nothing upstream.
//
// Called by BatchGetImage only after the repository was found missing, so an existing repository —
// cached before, or created by CreateRepository under the same prefix — is never re-created or
// re-templated: "the settings in a repository creation template are only applied during repository
// creation and don't have any effect on existing repositories".
//
// **An ECR upstream is checked.** For an ECR-to-ECR rule the upstream registry is one substrate
// holds, so whether the pull finds anything is observable rather than unknowable: the repository is
// created only when the upstream repository the rule maps the name onto exists
// ([ecrUpstreamRepositoryName]). That is what keeps a ROOT rule — which ECR allows only for an ECR
// upstream, and which matches every name — from turning a mistyped BatchGetImage into a new
// repository. For any other upstream the content is outside the emulator, the rule's prefix is
// mandatory, and a name under it is created as AWS's first pull would.
func (p *ECRPlugin) pullThroughCacheRepository(goCtx context.Context, ctx *RequestContext, name string) (*ECRRepository, error) {
	rule, err := p.matchingPullThroughCacheRule(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil || rule == nil {
		return nil, err
	}
	if m := ecrUpstreamECRPattern.FindStringSubmatch(rule.UpstreamRegistryURL); m != nil {
		upstream := ecrUpstreamRepositoryName(*rule, name)
		data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(m[1], m[2], upstream))
		if err != nil {
			return nil, fmt.Errorf("ecr pull through cache upstream state.Get: %w", err)
		}
		if data == nil {
			return nil, nil
		}
	}
	tmpl, err := p.matchingCreationTemplate(goCtx, ctx.AccountID, ctx.Region, name, "PULL_THROUGH_CACHE")
	if err != nil {
		return nil, err
	}
	return p.createRepositoryFromTemplate(goCtx, ctx, name, tmpl)
}

// ecrUpstreamRepositoryName maps a cache repository name onto the upstream repository a rule pulls
// it from, per the user guide's mapping table for ECR to ECR pull through cache: the rule's
// ecrRepositoryPrefix is removed (nothing, for ROOT), and the upstreamRepositoryPrefix is prepended
// unless it is ROOT — `ecr-public/my-app/image1` → `my-app/image1` under (ecr-public, ROOT), and
// `my-app/image1` → `upstream-app/image1` under (my-app, upstream-app).
func ecrUpstreamRepositoryName(rule ECRPullThroughCacheRule, name string) string {
	rest := name
	if key := ecrPrefixKey(rule.EcrRepositoryPrefix); key != ecrPrefixRoot {
		rest = strings.TrimPrefix(name, key+"/")
	}
	if up := ecrPrefixKey(rule.UpstreamRepositoryPrefix); up != "" && up != ecrPrefixRoot {
		return up + "/" + rest
	}
	return rest
}

// createOnPushRepository creates the repository a PutImage names when it is absent and a
// CREATE_ON_PUSH template governs it, and returns it — nil when no such template does: "Amazon ECR
// will not create a repository with default settings" for a push.
func (p *ECRPlugin) createOnPushRepository(goCtx context.Context, ctx *RequestContext, name string) (*ECRRepository, error) {
	tmpl, err := p.matchingCreationTemplate(goCtx, ctx.AccountID, ctx.Region, name, "CREATE_ON_PUSH")
	if err != nil || tmpl == nil {
		return nil, err
	}
	return p.createRepositoryFromTemplate(goCtx, ctx, name, tmpl)
}

// ensureRepositoryOnBehalf is the repository lookup BatchGetImage and PutImage make. It returns the
// repository record when it exists or when create — the operation's on-behalf creation — makes it
// for this call, and the published RepositoryNotFoundException otherwise. Returning the record is
// what saves the caller a second read.
func (p *ECRPlugin) ensureRepositoryOnBehalf(goCtx context.Context, ctx *RequestContext, name string,
	create func(context.Context, *RequestContext, string) (*ECRRepository, error),
) (*ECRRepository, error) {
	data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("ecr repository state.Get: %w", err)
	}
	if data != nil {
		var repo ECRRepository
		if err := json.Unmarshal(data, &repo); err != nil {
			return nil, fmt.Errorf("ecr repository unmarshal: %w", err)
		}
		return &repo, nil
	}
	if create != nil {
		repo, err := create(goCtx, ctx, name)
		if err != nil {
			return nil, err
		}
		if repo != nil {
			return repo, nil
		}
	}
	return nil, ecrRepositoryNotFound(name)
}
