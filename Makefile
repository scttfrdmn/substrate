.PHONY: build build-substrate build-substratelocal test lint coverage clean tidy vet bench e2e iac-e2e iac-e2e-setup docker-build compose-up compose-down compose-logs python-test python-lint python-build vuln scan-fs scan-image scan-iac sast security docs-reference docs-reference-check docs-headings-check docs-versions docs-counts-check authz-reference authz-reference-check authz-reference-fetch cfn-schema cfn-schema-check cfn-schema-fetch scheduler-query-model operation-catalog operation-catalog-check operation-docs-check operation-docs-write version-check discarded-unmarshal-check discarded-marshal-check discarded-state-check wire-bookkeeping-check wire-bookkeeping-write tag-releases-check nondeterminism-check docs-anchor-fixtures comment-history-check doc-links-check

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X github.com/scttfrdmn/substrate/emulator.Version=$(VERSION)"

build: build-substrate build-substratelocal ## Build all binaries

build-substrate: ## Build the substrate binary
	go build $(LDFLAGS) -o bin/substrate ./cmd/substrate

build-substratelocal: ## Build the substratelocal wrapper binary
	go build -o bin/substratelocal ./cmd/substratelocal

docker-build: ## Build multi-arch Docker image (requires docker buildx)
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) \
		-t ghcr.io/scttfrdmn/substrate:$(VERSION) .

compose-up: ## Start Substrate locally with Docker Compose
	docker compose up -d

compose-down: ## Stop and remove Docker Compose containers
	docker compose down

compose-logs: ## Tail Docker Compose logs
	docker compose logs -f substrate

test: ## Run tests with race detector
	go test -race -count=1 ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

coverage: ## Generate coverage report
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	go tool cover -func=coverage.out | tail -1

tidy: ## Tidy and verify go modules
	go mod tidy
	go mod verify

docs-reference: ## Regenerate the coverage matrix in docs/services.md and the services sidebar from the plugin registry
	go run ./cmd/gen-service-reference

docs-reference-check: ## Fail if the matrix or sidebar is stale, a legacy /services# anchor no longer lands, or a docs link names a missing anchor
	go run ./cmd/gen-service-reference -check

docs-anchor-fixtures: ## Regenerate the VitePress anchor fixtures docs-reference-check is tested against (dev-only; run `npm ci` in docs/ first)
	node cmd/gen-service-reference/testdata/vitepress-anchors.mjs docs

authz-reference: ## Regenerate emulator/authz_reference_gen.go from the vendored AWS snapshots
	go run ./cmd/gen-authz-reference

authz-reference-check: ## Fail if authz_reference_gen.go is out of date with emulator/authzref (offline)
	go run ./cmd/gen-authz-reference -check

authz-reference-fetch: ## Refresh emulator/authzref/*.json from AWS (REQUIRES NETWORK; run by hand, never in CI)
	go run ./cmd/gen-authz-reference -fetch

cfn-schema: ## Regenerate emulator/cfn_schema_gen.go from the vendored CloudFormation resource specification
	go run ./cmd/gen-cfn-schema

cfn-schema-check: ## Fail if cfn_schema_gen.go is out of date with emulator/cfnschema (offline)
	go run ./cmd/gen-cfn-schema -check

cfn-schema-fetch: ## Refresh emulator/cfnschema from AWS (REQUIRES NETWORK; run by hand, never in CI)
	go run ./cmd/gen-cfn-schema -fetch

scheduler-query-model: ## Regenerate emulator/scheduler_query_model_gen.go from the AWS CLI v2's botocore models (needs `aws` on PATH)
	go run ./cmd/gen-scheduler-query-model

operation-catalog: ## Regenerate emulator/operation_catalog_gen.go from each plugin's dispatch switch
	go run ./cmd/gen-operation-catalog

operation-catalog-check: ## Fail if operation_catalog_gen.go is out of date with the plugins' dispatch
	go run ./cmd/gen-operation-catalog -check

operation-docs-check: ## Fail if a plugin has no docs/reference page (or a page no plugin), a routed operation no row, or a row names none
	go run ./cmd/check-operation-docs

operation-docs-write: ## Rewrite the undocumented-operations inventory (run by hand after writing rows)
	go run ./cmd/check-operation-docs -write

docs-headings-check: ## Fail if a reference page lacks one unique title or repeats a section
	./scripts/check-docs-headings.sh --self-test
	./scripts/check-docs-headings.sh

docs-versions: ## Fail if docs/README pin a stale version in prose
	./scripts/check-doc-versions.sh

docs-counts-check: ## Fail if docs/README pin a plugin, service or operation count outside the generated matrix
	./scripts/check-doc-counts.sh --self-test
	./scripts/check-doc-counts.sh

discarded-unmarshal-check: ## Fail if a plugin decodes a request body and discards the error
	./scripts/check-discarded-unmarshal.sh

discarded-marshal-check: ## Fail if non-test code encodes a value and discards the error
	./scripts/check-discarded-marshal.sh

discarded-state-check: ## Fail if non-test code writes to state and discards the error
	./scripts/check-discarded-state.sh

nondeterminism-check: ## Fail if emulator code reads the wall clock or a random source outside the allowlist
	./scripts/check-nondeterminism.sh --self-test
	./scripts/check-nondeterminism.sh

comment-history-check: ## Fail if this branch adds a Go comment narrating issue history (BASE=origin/main)
	./scripts/check-comment-history.sh --self-test
	./scripts/check-comment-history.sh

doc-links-check: ## Fail if a Go comment's [ident] doc link names no identifier
	go run ./cmd/check-doc-links

wire-bookkeeping-check: ## Fail if substrate's own bookkeeping fields gain new wire-visible ground
	./scripts/check-wire-bookkeeping.sh

wire-bookkeeping-write: ## Rewrite the bookkeeping baseline from the tree (run by hand after a deliberate change)
	./scripts/check-wire-bookkeeping.sh --write

version-check: ## Fail if the build's -ldflags do not reach the version the server reports
	./scripts/check-version-stamping.sh

tag-releases-check: ## Fail if a published version tag has no GitHub Release behind it
	./scripts/check-tag-releases.sh

bench: ## Run benchmarks
	go test -bench=. -benchmem -benchtime=5s ./...

e2e: ## Run end-to-end tests
	cd test/e2e && go test -v -race ./...

# The AWS provider the Terraform journey installs from. `iac-e2e-setup` fills it; the journey
# reads it as a filesystem mirror, so the test itself never contacts the registry.
TF_PLUGIN_CACHE_DIR ?= $(HOME)/.terraform.d/plugin-cache

iac-e2e-setup: ## Fetch what iac-e2e needs (REQUIRES NETWORK): the AWS provider into TF_PLUGIN_CACHE_DIR, and examples/cdk's node_modules
	mkdir -p "$(TF_PLUGIN_CACHE_DIR)"
	tfdata="$$(mktemp -d)" && TF_PLUGIN_CACHE_DIR="$(TF_PLUGIN_CACHE_DIR)" TF_DATA_DIR="$$tfdata" terraform -chdir=examples/terraform init -backend=false -input=false && rm -rf "$$tfdata"
	cd examples/cdk && npm ci --no-audit --no-fund

iac-e2e: ## Run the Terraform and CDK journeys against substrate and replay each (offline; run iac-e2e-setup first)
	cd test/iac && TF_PLUGIN_CACHE_DIR="$(TF_PLUGIN_CACHE_DIR)" SUBSTRATE_IAC=1 go test -v -race -count=1 -timeout 45m ./...

python-test: ## Run pytest-substrate unit tests and the worked example in python/examples
	cd python && .venv/bin/pytest tests/ examples/ -v

python-lint: ## Lint pytest-substrate with ruff (if available)
	cd python && .venv/bin/ruff check src/ tests/ examples/ 2>/dev/null || echo "ruff not installed; skipping"

python-build: ## Build pytest-substrate wheel
	cd python && .venv/bin/python -m build --wheel --outdir dist/ 2>/dev/null || \
		.venv/bin/pip wheel . --no-deps -w dist/

vuln: ## Run Go vulnerability check
	govulncheck ./...

scan-fs: ## Trivy filesystem scan for HIGH/CRITICAL vulnerabilities
	trivy fs --severity HIGH,CRITICAL .

scan-image: ## Trivy container image scan
	trivy image --severity HIGH,CRITICAL ghcr.io/scttfrdmn/substrate:$(VERSION)

scan-iac: ## Trivy IaC misconfiguration scan
	trivy config --ignorefile .trivyignore.yaml .

sast: ## Semgrep static analysis
	semgrep scan --config=auto --error .

security: vuln scan-fs scan-iac sast ## Run all security checks

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out coverage.html
	rm -rf python/build/ python/dist/ python/src/*.egg-info/

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
