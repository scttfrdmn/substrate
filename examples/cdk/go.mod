// This directory is a TypeScript CDK app, not Go. The module boundary is here only so that
// `go test ./...`, `go vet ./...` and golangci-lint at the repository root stop at it: once
// `npm ci` has run, node_modules/aws-cdk ships Go init templates whose file names
// ("%name%.template.go") the Go toolchain refuses.
module github.com/scttfrdmn/substrate/examples/cdk

go 1.26.0
