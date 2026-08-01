# Stage 1: build
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The emulator path segment is load-bearing: emulator.Version is what /health and
# /_localstack/{health,info} serve. -X against a package with no Go files (the root
# module path) is silently ignored by the linker, so a typo here fails at runtime,
# not at build time (#402). Keep in sync with LDFLAGS in the Makefile.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
    -ldflags "-X main.version=${VERSION} -X github.com/scttfrdmn/substrate/emulator.Version=${VERSION}" \
    -o /substrate ./cmd/substrate

# Stage 2: runtime
FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S substrate && adduser -S substrate -G substrate && \
    mkdir -p /var/lib/substrate && chown substrate:substrate /var/lib/substrate
COPY --from=builder /substrate /usr/local/bin/substrate
USER substrate
EXPOSE 4566
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:4566/health || exit 1
ENTRYPOINT ["/usr/local/bin/substrate"]
CMD ["server"]
