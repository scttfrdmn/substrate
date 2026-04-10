package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// PricingProvider resolves per-operation costs.
type PricingProvider interface {
	// PriceFor returns the per-unit cost for a service/operation in the given region.
	// An empty operation returns a service-level default price.
	PriceFor(service, operation, region string) float64
	// Refresh fetches fresh pricing data from the source.
	Refresh(ctx context.Context) error
	// Source returns a description of the pricing source (e.g., "static", "aws-cache").
	Source() string
	// CacheAge returns time since last refresh (zero for static).
	CacheAge() time.Duration
}

// StaticPricingProvider wraps the built-in defaultCostTable. It is the default
// provider when no AWS pricing is configured.
type StaticPricingProvider struct {
	table map[string]float64
}

// NewStaticPricingProvider creates a provider backed by the built-in pricing table.
func NewStaticPricingProvider() *StaticPricingProvider {
	return &StaticPricingProvider{table: defaultCostTable()}
}

// PriceFor looks up the price for service/operation.
func (p *StaticPricingProvider) PriceFor(service, operation, _ string) float64 {
	svc := strings.ToLower(service)
	if operation != "" {
		if price, ok := p.table[svc+"/"+operation]; ok {
			return price
		}
	}
	if price, ok := p.table[svc]; ok {
		return price
	}
	return 0.0
}

// Refresh is a no-op for the static provider.
func (p *StaticPricingProvider) Refresh(_ context.Context) error { return nil }

// Source returns "static".
func (p *StaticPricingProvider) Source() string { return "static" }

// CacheAge returns zero (static data never expires).
func (p *StaticPricingProvider) CacheAge() time.Duration { return 0 }

// PricingCache holds cached pricing data persisted to disk.
type PricingCache struct {
	Prices    map[string]float64 `json:"prices"`
	Source    string             `json:"source"`
	FetchedAt time.Time          `json:"fetchedAt"`
}

// awsPricingIndexURL is the AWS Price List API index.
const awsPricingIndexURL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"

// awsServiceCodeMap maps AWS Pricing service codes to substrate service names.
var awsServiceCodeMap = map[string]string{
	"AmazonS3":                "s3",
	"AmazonDynamoDB":          "dynamodb",
	"AWSLambda":               "lambda",
	"AmazonSQS":               "sqs",
	"AmazonSNS":               "sns",
	"AmazonEC2":               "ec2",
	"AmazonRDS":               "rds",
	"AmazonElastiCache":       "elasticache",
	"AmazonEFS":               "efs",
	"AmazonKinesis":           "kinesis",
	"AmazonKinesisFirehose":   "firehose",
	"AmazonCloudFront":        "cloudfront",
	"AmazonRoute53":           "route53",
	"AmazonCloudWatch":        "monitoring",
	"AmazonCloudWatchLogs":    "logs",
	"AmazonCognito":           "cognito-idp",
	"AWSStepFunctions":        "states",
	"AmazonECR":               "ecr",
	"AmazonECS":               "ecs",
	"AWSGlue":                 "glue",
	"AmazonAthena":            "athena",
	"AmazonOpenSearchService": "opensearch",
	"AmazonSES":               "sesv2",
	"AWSCodeBuild":            "codebuild",
	"AWSCodePipeline":         "codepipeline",
	"AWSBackup":               "backup",
	"AmazonMSK":               "msk",
	"AmazonRedshift":          "redshift",
	"AmazonTimestream":        "timestream",
	"AmazonSageMaker":         "sagemaker",
	"AmazonBedrockRuntime":    "bedrock-runtime",
	"AmazonQuickSight":        "quicksight",
	"ElasticLoadBalancing":    "elasticloadbalancing",
	"AWSKeyManagementService": "kms",
	"AWSSecretsManager":       "secretsmanager",
	"AmazonSSM":               "ssm",
}

// AWSPricingProvider fetches pricing from the AWS Price List API and caches
// results to disk. It falls back to the static table for operations not found
// in the cached AWS data.
type AWSPricingProvider struct {
	mu        sync.RWMutex
	prices    map[string]float64
	fetchedAt time.Time
	cachePath string
	ttl       time.Duration
	region    string
	fallback  *StaticPricingProvider
	client    *http.Client
}

// AWSPricingConfig holds configuration for the AWS pricing provider.
type AWSPricingConfig struct {
	CachePath     string
	CacheTTLHours int
	Region        string
}

// NewAWSPricingProvider creates a provider that fetches from the AWS Price List API.
// It loads cached data from disk if available and within TTL; otherwise falls
// back to static pricing. It never blocks on network I/O during construction.
func NewAWSPricingProvider(cfg AWSPricingConfig) *AWSPricingProvider {
	cachePath := cfg.CachePath
	if cachePath == "" {
		home, _ := os.UserHomeDir()
		cachePath = filepath.Join(home, ".substrate", "pricing-cache.json")
	}
	// Expand ~ prefix.
	if strings.HasPrefix(cachePath, "~/") {
		home, _ := os.UserHomeDir()
		cachePath = filepath.Join(home, cachePath[2:])
	}
	ttl := time.Duration(cfg.CacheTTLHours) * time.Hour
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	p := &AWSPricingProvider{
		prices:    make(map[string]float64),
		cachePath: cachePath,
		ttl:       ttl,
		region:    region,
		fallback:  NewStaticPricingProvider(),
		client:    &http.Client{Timeout: 30 * time.Second},
	}

	// Try loading cached data.
	if err := p.loadCache(); err == nil && !p.isCacheStale() {
		// Cache is fresh — use it.
		return p
	}

	// Cache missing or stale — use static fallback (no network on startup).
	return p
}

// PriceFor returns the cached AWS price, falling back to static if not found.
func (p *AWSPricingProvider) PriceFor(service, operation, _ string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	svc := strings.ToLower(service)
	if operation != "" {
		if price, ok := p.prices[svc+"/"+operation]; ok {
			return price
		}
	}
	if price, ok := p.prices[svc]; ok {
		return price
	}
	// Fall back to static.
	return p.fallback.PriceFor(service, operation, "")
}

// Refresh fetches fresh pricing data from AWS and updates the cache.
func (p *AWSPricingProvider) Refresh(ctx context.Context) error {
	index, err := p.fetchIndex(ctx)
	if err != nil {
		return fmt.Errorf("fetch pricing index: %w", err)
	}

	prices := make(map[string]float64)
	for awsCode, substrateSvc := range awsServiceCodeMap {
		offerURL, ok := index[awsCode]
		if !ok {
			continue
		}
		svcPrices, fetchErr := p.fetchServicePrices(ctx, offerURL)
		if fetchErr != nil {
			continue // Skip services that fail — partial data is better than none.
		}
		for op, price := range svcPrices {
			prices[substrateSvc+"/"+op] = price
		}
	}

	p.mu.Lock()
	p.prices = prices
	p.fetchedAt = time.Now()
	p.mu.Unlock()

	return p.saveCache()
}

// Source returns "aws-cache" if cached data is loaded, "aws-stale" if stale.
func (p *AWSPricingProvider) Source() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.fetchedAt.IsZero() {
		return "static-fallback"
	}
	if p.isCacheStale() {
		return "aws-stale"
	}
	return "aws-cache"
}

// CacheAge returns the time since the last successful refresh.
func (p *AWSPricingProvider) CacheAge() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.fetchedAt.IsZero() {
		return 0
	}
	return time.Since(p.fetchedAt)
}

func (p *AWSPricingProvider) isCacheStale() bool {
	return p.fetchedAt.IsZero() || time.Since(p.fetchedAt) > p.ttl
}

func (p *AWSPricingProvider) loadCache() error {
	data, err := os.ReadFile(p.cachePath)
	if err != nil {
		return err
	}
	var cache PricingCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return err
	}
	p.mu.Lock()
	p.prices = cache.Prices
	p.fetchedAt = cache.FetchedAt
	p.mu.Unlock()
	return nil
}

func (p *AWSPricingProvider) saveCache() error {
	p.mu.RLock()
	cache := PricingCache{
		Prices:    p.prices,
		Source:    "aws",
		FetchedAt: p.fetchedAt,
	}
	p.mu.RUnlock()

	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pricing cache: %w", err)
	}
	dir := filepath.Dir(p.cachePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	return os.WriteFile(p.cachePath, data, 0o600)
}

// fetchIndex downloads the AWS pricing index and returns a map of service code
// to offer file URL.
func (p *AWSPricingProvider) fetchIndex(ctx context.Context) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, awsPricingIndexURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pricing index returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var idx struct {
		Offers map[string]struct {
			CurrentVersionURL string `json:"currentVersionUrl"`
		} `json:"offers"`
	}
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("parse pricing index: %w", err)
	}

	result := make(map[string]string, len(idx.Offers))
	for code, offer := range idx.Offers {
		if offer.CurrentVersionURL != "" {
			result[code] = "https://pricing.us-east-1.amazonaws.com" + offer.CurrentVersionURL
		}
	}
	return result, nil
}

// fetchServicePrices downloads a service's offer file and extracts per-request
// pricing. Returns a map of operation-like keys to USD per request.
func (p *AWSPricingProvider) fetchServicePrices(ctx context.Context, offerURL string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, offerURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("offer file returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// AWS offer files are very large. We parse only the terms.OnDemand section
	// for request-based pricing (usageType containing "Requests" or "Request").
	var offer struct {
		Products map[string]struct {
			Attributes map[string]string `json:"attributes"`
		} `json:"products"`
		Terms struct {
			OnDemand map[string]map[string]struct {
				PriceDimensions map[string]struct {
					Unit         string            `json:"unit"`
					PricePerUnit map[string]string `json:"pricePerUnit"`
					Description  string            `json:"description"`
				} `json:"priceDimensions"`
			} `json:"OnDemand"`
		} `json:"terms"`
	}
	if err := json.Unmarshal(body, &offer); err != nil {
		return nil, fmt.Errorf("parse offer file: %w", err)
	}

	prices := make(map[string]float64)
	for sku, product := range offer.Products {
		usageType := product.Attributes["usagetype"]
		operation := product.Attributes["operation"]
		if operation == "" || !isRequestBased(usageType) {
			continue
		}
		// Find the OnDemand price for this SKU.
		skuTerms, ok := offer.Terms.OnDemand[sku]
		if !ok {
			continue
		}
		for _, term := range skuTerms {
			for _, dim := range term.PriceDimensions {
				usdStr, exists := dim.PricePerUnit["USD"]
				if !exists || usdStr == "0.0000000000" {
					continue
				}
				var usd float64
				if _, parseErr := fmt.Sscanf(usdStr, "%f", &usd); parseErr != nil {
					continue
				}
				if usd > 0 {
					prices[operation] = usd
				}
			}
		}
	}
	return prices, nil
}

// isRequestBased returns true if the usage type looks like per-request pricing.
func isRequestBased(usageType string) bool {
	u := strings.ToLower(usageType)
	return strings.Contains(u, "request") || strings.Contains(u, "invoke") ||
		strings.Contains(u, "api") || strings.Contains(u, "query")
}
