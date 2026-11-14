package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// Amazon ECR pull through cache rules (#1461).
//
// A rule maps a repository prefix in the caller's private registry onto an upstream registry, so
// that the first pull of `<prefix>/<upstream-repo>` creates that repository and caches the image.
// Substrate models the five rule operations — CreatePullThroughCacheRule,
// DescribePullThroughCacheRules, UpdatePullThroughCacheRule, DeletePullThroughCacheRule and
// ValidatePullThroughCacheRule — as records in the caller's registry, and the one API-observable
// effect of a pull: the repository it creates. See [ECRPlugin.pullThroughCacheRepository] for what
// triggers that and what it does not do.
//
// What substrate never does is reach an upstream. ValidatePullThroughCacheRule's published job is
// to "retrieve the contents of the AWS Secrets Manager secret, verify the syntax, and then validate
// that authentication to the upstream registry is successful"; here it answers isValid: true unless
// a test has seeded otherwise (POST /v1/ecr/pull-through-cache-validation), which is the
// deterministic-emulator rule for an outcome that depends on the world outside the API.

// ecrPrefixPattern is the pattern every pull-through-cache and creation-template prefix member
// publishes, verbatim: `ecrRepositoryPrefix`, `upstreamRepositoryPrefix`,
// `ecrRepositoryPrefixes`, `prefix` and `prefixes` all carry it.
var ecrPrefixPattern = regexp.MustCompile(`^([a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*\/?|ROOT)$`)

// ecrCredentialArnPattern is credentialArn's published pattern. The secret's name must begin
// `ecr-pullthroughcache/`, which the user guide states as a requirement in its own right.
var ecrCredentialArnPattern = regexp.MustCompile(`^arn:aws(-\w+)*:secretsmanager:[a-zA-Z0-9-:]+:secret:ecr\-pullthroughcache\/[a-zA-Z0-9\/_+=.@-]+$`)

// ecrRegistryIDPattern is registryId's published pattern on every ECR operation that carries it.
var ecrRegistryIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// ecrUpstreamECRPattern is the Amazon ECR upstream URL form, `<accountId>.dkr.ecr.<region>.amazonaws.com`.
var ecrUpstreamECRPattern = regexp.MustCompile(`^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$`)

// ecrPrefixRoot is the prefix value that matches every repository no more specific prefix does.
const ecrPrefixRoot = "ROOT"

// ecrUpstreamRegistries is the upstreamRegistry Valid Values set API_CreatePullThroughCacheRule and
// API_PullThroughCacheRule publish.
var ecrUpstreamRegistries = map[string]bool{
	"ecr": true, "ecr-public": true, "quay": true, "k8s": true, "docker-hub": true,
	"github-container-registry": true, "azure-container-registry": true,
	"gitlab-container-registry": true, "chainguard": true,
}

// ecrUpstreamAuth is what a supported upstream registry requires a rule to carry.
type ecrUpstreamAuth int

const (
	// ecrUpstreamAuthNone is an upstream that "doesn't require authentication": Amazon ECR Public,
	// the Kubernetes registry and Quay.
	ecrUpstreamAuthNone ecrUpstreamAuth = iota
	// ecrUpstreamAuthSecret is an upstream that "requires authentication with AWS Secrets Manager
	// secret": Docker Hub, Azure, GitHub, GitLab and Chainguard.
	ecrUpstreamAuthSecret
	// ecrUpstreamAuthRole is Amazon ECR, which "requires authentication with AWS IAM role" — and the
	// user guide narrows it: "you must create an IAM role if the upstream and downstream Amazon ECR
	// registries belong to different AWS account".
	ecrUpstreamAuthRole
)

// ecrClassifyUpstream maps an upstreamRegistryUrl onto its upstreamRegistry name and the
// authentication the user guide says it requires, reporting false for a URL outside the list.
//
// The URL list is API_CreatePullThroughCacheRule's own, entry by entry, plus `cgr.dev` for
// Chainguard: `chainguard` is in the page's upstreamRegistry Valid Values but its URL is missing from
// the page's list, and the user guide's create-pull-through-cache-rule example gives it. A scheme
// (`https://`) or path is not part of any published form and is refused.
func ecrClassifyUpstream(url string) (registry string, auth ecrUpstreamAuth, upstreamAccount string, ok bool) {
	switch url {
	case "public.ecr.aws":
		return "ecr-public", ecrUpstreamAuthNone, "", true
	case "registry.k8s.io":
		return "k8s", ecrUpstreamAuthNone, "", true
	case "quay.io":
		return "quay", ecrUpstreamAuthNone, "", true
	case "registry-1.docker.io":
		return "docker-hub", ecrUpstreamAuthSecret, "", true
	case "ghcr.io":
		return "github-container-registry", ecrUpstreamAuthSecret, "", true
	case "registry.gitlab.com":
		return "gitlab-container-registry", ecrUpstreamAuthSecret, "", true
	case "cgr.dev":
		return "chainguard", ecrUpstreamAuthSecret, "", true
	}
	if name, found := strings.CutSuffix(url, ".azurecr.io"); found && name != "" && !strings.ContainsAny(name, "./:") {
		return "azure-container-registry", ecrUpstreamAuthSecret, "", true
	}
	if m := ecrUpstreamECRPattern.FindStringSubmatch(url); m != nil {
		return "ecr", ecrUpstreamAuthRole, m[1], true
	}
	return "", ecrUpstreamAuthNone, "", false
}

// ecrPrefixKey is the identity of a prefix: the value with its assumed trailing `/` removed.
//
// "There is always an assumed `/` applied to the end of the prefix. If you specify `ecr-public` as
// the prefix, Amazon ECR treats that as `ecr-public/`." So `ecr-public` and `ecr-public/` are one
// prefix, and keying on the trimmed form is what makes a second create of either answer
// PullThroughCacheRuleAlreadyExistsException.
func ecrPrefixKey(prefix string) string {
	return strings.TrimSuffix(prefix, "/")
}

// ecrValidatePrefix checks a prefix member against its published length range and pattern.
func ecrValidatePrefix(member, value string, minLen, maxLen int) *AWSError {
	if len(value) < minLen || len(value) > maxLen {
		return ecrInvalidParameter(fmt.Sprintf(
			"Invalid parameter at '%s' failed to satisfy constraint: 'Member must have length between %d and %d'",
			member, minLen, maxLen))
	}
	if !ecrPrefixPattern.MatchString(value) {
		return ecrInvalidParameter(fmt.Sprintf(
			"Invalid parameter at '%s' failed to satisfy constraint: 'Member must satisfy regular expression pattern: %s'",
			member, ecrPrefixPattern.String()))
	}
	return nil
}

// ecrValidateRegistryID refuses a registryId outside its published `[0-9]{12}` pattern.
//
// A well-formed registryId is not otherwise read: every ECR operation substrate models acts on the
// caller's own registry, as the rest of the plugin does, because nothing here models registry
// permissions that would let one account act on another's.
func ecrValidateRegistryID(registryID string) *AWSError {
	if registryID != "" && !ecrRegistryIDPattern.MatchString(registryID) {
		return ecrInvalidParameter("Invalid parameter at 'registryId' failed to satisfy constraint: 'Member must satisfy regular expression pattern: [0-9]{12}'")
	}
	return nil
}

// ecrValidateCredentialArn checks credentialArn's published length range and pattern.
func ecrValidateCredentialArn(arn string) *AWSError {
	if arn == "" {
		return nil
	}
	if len(arn) < 50 || len(arn) > 612 || !ecrCredentialArnPattern.MatchString(arn) {
		return ecrInvalidParameter("Invalid parameter at 'credentialArn' failed to satisfy constraint: " +
			"'Member must have length between 50 and 612 and satisfy regular expression pattern: " +
			ecrCredentialArnPattern.String() + "'")
	}
	return nil
}

// ecrValidateCustomRoleArn checks customRoleArn's published maximum length, the only constraint any
// page publishes for it.
func ecrValidateCustomRoleArn(arn string) *AWSError {
	if len(arn) > 2048 {
		return ecrInvalidParameter("Invalid parameter at 'customRoleArn' failed to satisfy constraint: 'Member must have length less than or equal to 2048'")
	}
	return nil
}

// --- State keys --------------------------------------------------------------

func ecrPullThroughCacheRulePrefix(accountID, region string) string {
	return "ecrptc:" + accountID + "/" + region + "/"
}

func ecrPullThroughCacheRuleKey(accountID, region, prefix string) string {
	return ecrPullThroughCacheRulePrefix(accountID, region) + ecrPrefixKey(prefix)
}

// --- Handlers ----------------------------------------------------------------

func (p *ECRPlugin) createPullThroughCacheRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
		UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
		UpstreamRegistry         string `json:"upstreamRegistry"`
		UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix"`
		CredentialArn            string `json:"credentialArn"`
		CustomRoleArn            string `json:"customRoleArn"`
		RegistryID               string `json:"registryId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ecrInvalidBody()
	}

	// Each member against its own published constraint first, so the shape of the request is
	// refused whether or not the prefix is free.
	if body.EcrRepositoryPrefix == "" {
		return nil, ecrInvalidParameter("ecrRepositoryPrefix is required")
	}
	if awsErr := ecrValidatePrefix("ecrRepositoryPrefix", body.EcrRepositoryPrefix, 2, 30); awsErr != nil {
		return nil, awsErr
	}
	if body.UpstreamRepositoryPrefix != "" {
		if awsErr := ecrValidatePrefix("upstreamRepositoryPrefix", body.UpstreamRepositoryPrefix, 2, 30); awsErr != nil {
			return nil, awsErr
		}
	}
	if body.UpstreamRegistryURL == "" {
		return nil, ecrInvalidParameter("upstreamRegistryUrl is required")
	}
	if body.UpstreamRegistry != "" && !ecrUpstreamRegistries[body.UpstreamRegistry] {
		return nil, ecrInvalidParameter("Invalid parameter at 'upstreamRegistry' failed to satisfy constraint: " +
			"'Member must satisfy enum value set: [ecr, ecr-public, quay, k8s, docker-hub, github-container-registry, " +
			"azure-container-registry, gitlab-container-registry, chainguard]'")
	}
	if awsErr := ecrValidateCredentialArn(body.CredentialArn); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ecrValidateCustomRoleArn(body.CustomRoleArn); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ecrValidateRegistryID(body.RegistryID); awsErr != nil {
		return nil, awsErr
	}

	registry, auth, upstreamAccount, ok := ecrClassifyUpstream(body.UpstreamRegistryURL)
	if !ok {
		return nil, ecrUnsupportedUpstreamRegistry(body.UpstreamRegistryURL)
	}
	// The cross-member rules. Each is the user guide's own requirement; the code is substrate's
	// reading — see [ecrValidationException].
	if body.UpstreamRegistry != "" && body.UpstreamRegistry != registry {
		return nil, ecrValidationException(fmt.Sprintf(
			"upstreamRegistry '%s' does not match upstreamRegistryUrl '%s', which is a %s registry",
			body.UpstreamRegistry, body.UpstreamRegistryURL, registry))
	}
	switch auth {
	case ecrUpstreamAuthSecret:
		if body.CredentialArn == "" {
			return nil, ecrValidationException("The upstream registry '" + body.UpstreamRegistryURL +
				"' requires authentication: specify a credentialArn naming a Secrets Manager secret")
		}
	case ecrUpstreamAuthRole:
		if upstreamAccount != ctx.AccountID && body.CustomRoleArn == "" {
			return nil, ecrValidationException("The upstream Amazon ECR registry '" + body.UpstreamRegistryURL +
				"' belongs to another account: specify a customRoleArn")
		}
	case ecrUpstreamAuthNone:
	}

	goCtx := context.Background()
	key := ecrPullThroughCacheRuleKey(ctx.AccountID, ctx.Region, body.EcrRepositoryPrefix)
	existing, err := p.state.Get(goCtx, ecrNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("ecr createPullThroughCacheRule state.Get: %w", err)
	}
	if existing != nil {
		return nil, ecrPullThroughCacheRuleAlreadyExists(body.EcrRepositoryPrefix)
	}

	// "When this field isn't specified, Amazon ECR will use the ROOT." Reported only for an ECR
	// upstream, which is the only kind the user guide's upstream-namespace customization describes;
	// for the others the member stays absent, as the page's Docker Hub sample response has it.
	upstreamPrefix := body.UpstreamRepositoryPrefix
	if upstreamPrefix == "" && registry == "ecr" {
		upstreamPrefix = ecrPrefixRoot
	}

	now := p.tc.Now()
	rule := ECRPullThroughCacheRule{
		EcrRepositoryPrefix:      body.EcrRepositoryPrefix,
		UpstreamRegistryURL:      body.UpstreamRegistryURL,
		UpstreamRegistry:         registry,
		UpstreamRepositoryPrefix: upstreamPrefix,
		CredentialArn:            body.CredentialArn,
		CustomRoleArn:            body.CustomRoleArn,
		RegistryID:               ctx.AccountID,
		CreatedAt:                now,
		UpdatedAt:                now,
		AccountID:                ctx.AccountID,
		Region:                   ctx.Region,
	}
	if err := p.putPullThroughCacheRule(goCtx, key, rule); err != nil {
		return nil, err
	}
	return ecrJSONResponse(http.StatusOK, ecrCreatePullThroughCacheRuleOut{
		CreatedAt:                EpochSeconds(rule.CreatedAt),
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	})
}

func (p *ECRPlugin) describePullThroughCacheRules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		EcrRepositoryPrefixes []string `json:"ecrRepositoryPrefixes"`
		MaxResults            int      `json:"maxResults"`
		NextToken             string   `json:"nextToken"`
		RegistryID            string   `json:"registryId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecrInvalidBody()
		}
	}
	if awsErr := ecrValidateRegistryID(body.RegistryID); awsErr != nil {
		return nil, awsErr
	}
	// "Array Members: Minimum number of 1 item. Maximum number of 100 items." An absent member
	// decodes to nil and lists every rule; a present one outside the range is refused.
	if body.EcrRepositoryPrefixes != nil && (len(body.EcrRepositoryPrefixes) < 1 || len(body.EcrRepositoryPrefixes) > 100) {
		return nil, ecrInvalidParameter("Invalid parameter at 'ecrRepositoryPrefixes' failed to satisfy constraint: 'Member must have length between 1 and 100'")
	}
	for _, prefix := range body.EcrRepositoryPrefixes {
		if awsErr := ecrValidatePrefix("ecrRepositoryPrefixes", prefix, 2, 30); awsErr != nil {
			return nil, awsErr
		}
	}
	// API_DescribePullThroughCacheRules publishes no sentence excluding pagination from the
	// filtered form, unlike DescribeRepositories, so both forms page (ecr_pagination.go).
	pageReq, err := ecrDecodePage(body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	var rules []ECRPullThroughCacheRule
	if body.EcrRepositoryPrefixes != nil {
		// A named prefix with no rule is refused: the page publishes
		// PullThroughCacheRuleNotFoundException, and this is the only site it can fire from. The
		// answer is in prefix order rather than request order, so a cursor over it is stable.
		seen := make(map[string]bool, len(body.EcrRepositoryPrefixes))
		for _, prefix := range body.EcrRepositoryPrefixes {
			key := ecrPullThroughCacheRuleKey(ctx.AccountID, ctx.Region, prefix)
			if seen[key] {
				continue
			}
			seen[key] = true
			rule, found, err := p.loadPullThroughCacheRule(goCtx, key)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ecrPullThroughCacheRuleNotFound(prefix)
			}
			rules = append(rules, rule)
		}
		ecrSortRulesByPrefix(rules)
	} else {
		rules, err = p.listPullThroughCacheRules(goCtx, ctx.AccountID, ctx.Region)
		if err != nil {
			return nil, err
		}
	}

	page, next := pageByOffsetToken(rules, pageReq.offset, pageReq.pageSize)
	out := make([]ecrPullThroughCacheRuleOut, 0, len(page))
	for _, rule := range page {
		out = append(out, ecrPullThroughCacheRuleToWire(rule))
	}
	type response struct {
		PullThroughCacheRules []ecrPullThroughCacheRuleOut `json:"pullThroughCacheRules"`
		NextToken             string                       `json:"nextToken,omitempty"`
	}
	return ecrJSONResponse(http.StatusOK, response{PullThroughCacheRules: out, NextToken: next})
}

// ecrRuleRef is the request body the delete, update and validate operations share: the prefix that
// names the rule and the registry it lives in.
type ecrRuleRef struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	RegistryID          string `json:"registryId"`
	CredentialArn       string `json:"credentialArn"`
	CustomRoleArn       string `json:"customRoleArn"`
}

// resolvePullThroughCacheRule decodes and validates a rule reference and loads the rule it names,
// answering PullThroughCacheRuleNotFoundException when there is none.
func (p *ECRPlugin) resolvePullThroughCacheRule(ctx *RequestContext, req *AWSRequest) (ecrRuleRef, ECRPullThroughCacheRule, string, error) {
	var ref ecrRuleRef
	if err := json.Unmarshal(req.Body, &ref); err != nil {
		return ref, ECRPullThroughCacheRule{}, "", ecrInvalidBody()
	}
	if ref.EcrRepositoryPrefix == "" {
		return ref, ECRPullThroughCacheRule{}, "", ecrInvalidParameter("ecrRepositoryPrefix is required")
	}
	if awsErr := ecrValidatePrefix("ecrRepositoryPrefix", ref.EcrRepositoryPrefix, 2, 30); awsErr != nil {
		return ref, ECRPullThroughCacheRule{}, "", awsErr
	}
	if awsErr := ecrValidateRegistryID(ref.RegistryID); awsErr != nil {
		return ref, ECRPullThroughCacheRule{}, "", awsErr
	}
	key := ecrPullThroughCacheRuleKey(ctx.AccountID, ctx.Region, ref.EcrRepositoryPrefix)
	rule, found, err := p.loadPullThroughCacheRule(context.Background(), key)
	if err != nil {
		return ref, ECRPullThroughCacheRule{}, "", err
	}
	if !found {
		return ref, ECRPullThroughCacheRule{}, "", ecrPullThroughCacheRuleNotFound(ref.EcrRepositoryPrefix)
	}
	return ref, rule, key, nil
}

func (p *ECRPlugin) deletePullThroughCacheRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	_, rule, key, err := p.resolvePullThroughCacheRule(ctx, req)
	if err != nil {
		return nil, err
	}
	// Deleting a rule leaves the repositories it created where they are: they "are treated like
	// any other Amazon ECR repository", and nothing published ties their lifetime to the rule.
	if err := p.state.Delete(context.Background(), ecrNamespace, key); err != nil {
		return nil, fmt.Errorf("ecr deletePullThroughCacheRule state.Delete: %w", err)
	}
	return ecrJSONResponse(http.StatusOK, ecrDeletePullThroughCacheRuleOut{
		CreatedAt:                EpochSeconds(rule.CreatedAt),
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	})
}

func (p *ECRPlugin) updatePullThroughCacheRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ref, rule, key, err := p.resolvePullThroughCacheRule(ctx, req)
	if err != nil {
		return nil, err
	}
	if awsErr := ecrValidateCredentialArn(ref.CredentialArn); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ecrValidateCustomRoleArn(ref.CustomRoleArn); awsErr != nil {
		return nil, awsErr
	}

	// Only the two members the operation carries are updatable, and an omitted one keeps its value:
	// the page publishes no "omitted means cleared" sentence, and clearing the credential of an
	// authenticated upstream would leave a rule CreatePullThroughCacheRule refuses to make.
	if ref.CredentialArn != "" {
		rule.CredentialArn = ref.CredentialArn
	}
	if ref.CustomRoleArn != "" {
		rule.CustomRoleArn = ref.CustomRoleArn
	}
	rule.UpdatedAt = p.tc.Now()
	if err := p.putPullThroughCacheRule(context.Background(), key, rule); err != nil {
		return nil, err
	}
	return ecrJSONResponse(http.StatusOK, ecrUpdatePullThroughCacheRuleOut{
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		UpdatedAt:                EpochSeconds(rule.UpdatedAt),
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	})
}

func (p *ECRPlugin) validatePullThroughCacheRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	_, rule, _, err := p.resolvePullThroughCacheRule(ctx, req)
	if err != nil {
		return nil, err
	}
	// Nominal success unless seeded: substrate cannot reach an upstream or read a secret's value
	// as the upstream would, so "Amazon ECR was able to reach the upstream registry and
	// authentication was successful" is the default and the alternative is a test's to choose.
	isValid, failure := true, ""
	seed, err := ecrLoadValidationSeed(context.Background(), p.state, rule.EcrRepositoryPrefix)
	if err != nil {
		return nil, err
	}
	if seed != nil {
		isValid, failure = seed.IsValid, seed.Failure
	}
	return ecrJSONResponse(http.StatusOK, ecrValidatePullThroughCacheRuleOut{
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		Failure:                  failure,
		IsValid:                  isValid,
		RegistryID:               rule.RegistryID,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	})
}

// --- Persistence -------------------------------------------------------------

func (p *ECRPlugin) putPullThroughCacheRule(goCtx context.Context, key string, rule ECRPullThroughCacheRule) error {
	data, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("ecr pull through cache rule marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, key, data); err != nil {
		return fmt.Errorf("ecr pull through cache rule state.Put: %w", err)
	}
	return nil
}

func (p *ECRPlugin) loadPullThroughCacheRule(goCtx context.Context, key string) (ECRPullThroughCacheRule, bool, error) {
	data, err := p.state.Get(goCtx, ecrNamespace, key)
	if err != nil {
		return ECRPullThroughCacheRule{}, false, fmt.Errorf("ecr pull through cache rule state.Get: %w", err)
	}
	if data == nil {
		return ECRPullThroughCacheRule{}, false, nil
	}
	var rule ECRPullThroughCacheRule
	if err := json.Unmarshal(data, &rule); err != nil {
		return ECRPullThroughCacheRule{}, false, fmt.Errorf("ecr pull through cache rule unmarshal: %w", err)
	}
	return rule, true, nil
}

// listPullThroughCacheRules returns every rule in a registry, in prefix order — the order
// [StateManager.List] guarantees for the keys, which embed the prefix.
func (p *ECRPlugin) listPullThroughCacheRules(goCtx context.Context, accountID, region string) ([]ECRPullThroughCacheRule, error) {
	keys, err := p.state.List(goCtx, ecrNamespace, ecrPullThroughCacheRulePrefix(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("ecr list pull through cache rules: %w", err)
	}
	rules := make([]ECRPullThroughCacheRule, 0, len(keys))
	for _, key := range keys {
		rule, found, err := p.loadPullThroughCacheRule(goCtx, key)
		if err != nil {
			return nil, err
		}
		if found {
			rules = append(rules, rule)
		}
	}
	return rules, nil
}

// ecrSortRulesByPrefix orders rules by their prefix key, the order the registry-wide listing uses.
func ecrSortRulesByPrefix(rules []ECRPullThroughCacheRule) {
	sortTagsByKey(rules, func(r ECRPullThroughCacheRule) string { return ecrPrefixKey(r.EcrRepositoryPrefix) })
}

// matchingPullThroughCacheRule returns the rule whose prefix a repository name falls under, if any.
//
// A non-ROOT prefix P matches a name that begins `P/` and has an upstream repository after it —
// the `<prefix>/<upstream-repo>` naming scheme the user guide gives for every upstream. The longest
// matching prefix wins, which is the rule the user guide states for creation templates and the only
// unambiguous reading for nested rule prefixes. A ROOT rule matches any name no other rule does.
func (p *ECRPlugin) matchingPullThroughCacheRule(goCtx context.Context, accountID, region, name string) (*ECRPullThroughCacheRule, error) {
	rules, err := p.listPullThroughCacheRules(goCtx, accountID, region)
	if err != nil {
		return nil, err
	}
	var best, root *ECRPullThroughCacheRule
	for i := range rules {
		key := ecrPrefixKey(rules[i].EcrRepositoryPrefix)
		if key == ecrPrefixRoot {
			root = &rules[i]
			continue
		}
		if len(name) > len(key)+1 && strings.HasPrefix(name, key+"/") {
			if best == nil || len(key) > len(ecrPrefixKey(best.EcrRepositoryPrefix)) {
				best = &rules[i]
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return root, nil
}

// --- Validation seed ---------------------------------------------------------

// ecrCtrlNamespace is the state namespace for ECR's control-plane seeds.
const ecrCtrlNamespace = "ecr-ctrl"

// ecrValidationSeed is the body of POST /v1/ecr/pull-through-cache-validation: what
// ValidatePullThroughCacheRule answers for the rule with that prefix, or for every rule when the
// prefix is empty or "*".
type ecrValidationSeed struct {
	// EcrRepositoryPrefix is the rule the seed governs; empty or "*" governs every rule.
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`

	// IsValid is the isValid the operation answers.
	IsValid bool `json:"isValid"`

	// Failure is "the reason the validation failed", reported only when IsValid is false.
	Failure string `json:"failure,omitempty"`
}

func ecrValidationSeedKey(prefix string) string {
	if prefix == "" || prefix == "*" {
		return "ptc-validation:*"
	}
	return "ptc-validation:" + ecrPrefixKey(prefix)
}

// ecrLoadValidationSeed returns the seed governing prefix — its own, else the wildcard — or nil.
func ecrLoadValidationSeed(goCtx context.Context, state StateManager, prefix string) (*ecrValidationSeed, error) {
	for _, key := range []string{ecrValidationSeedKey(prefix), ecrValidationSeedKey("*")} {
		data, err := state.Get(goCtx, ecrCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("ecr validation seed state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ecrValidationSeed
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("ecr validation seed unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil
}

// handleECRSeedPullThroughCacheValidation handles POST /v1/ecr/pull-through-cache-validation.
//
// A failure on a seed that reports isValid: true is refused, because API_ValidatePullThroughCacheRule
// defines failure as "the reason the validation failed" and a valid rule has none to report.
func (s *Server) handleECRSeedPullThroughCacheValidation(w http.ResponseWriter, r *http.Request) {
	var seed ecrValidationSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.IsValid && seed.Failure != "" {
		http.Error(w, `{"error":"failure applies only when isValid is false"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := ecrValidationSeedKey(seed.EcrRepositoryPrefix)
	if err := s.state.Put(r.Context(), ecrCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "key": key})
}

// handleECRClearPullThroughCacheValidation handles DELETE /v1/ecr/pull-through-cache-validation.
// With ?ecrRepositoryPrefix=... it removes that rule's seed; without one it removes every seed.
func (s *Server) handleECRClearPullThroughCacheValidation(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("ecrRepositoryPrefix")
	var keys []string
	if prefix != "" {
		keys = []string{ecrValidationSeedKey(prefix)}
	} else {
		listed, err := s.state.List(r.Context(), ecrCtrlNamespace, "ptc-validation:")
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		keys = listed
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), ecrCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
