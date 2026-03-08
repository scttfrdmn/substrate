.PHONY: build test lint coverage clean tidy vet bench e2e

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

build: ## Build the substrate binary
	go build $(LDFLAGS) -o bin/substrate ./cmd/substrate

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

bench: ## Run benchmarks
	go test -bench=. -benchmem -benchtime=5s ./...

e2e: ## Run end-to-end tests
	cd test/e2e && go test -v -race ./...

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out coverage.html

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
