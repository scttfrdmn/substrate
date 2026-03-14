# Stage 1: build
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
    -ldflags "-X main.version=${VERSION} -X github.com/scttfrdmn/substrate.Version=${VERSION}" \
    -o /substrate ./cmd/substrate

# Stage 2: runtime
FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /substrate /usr/local/bin/substrate
EXPOSE 4566
ENTRYPOINT ["/usr/local/bin/substrate"]
CMD ["server"]
