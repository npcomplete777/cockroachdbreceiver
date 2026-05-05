.PHONY: all build test fmt vet clean install-tools generate

# Variables
GOPATH := $(shell go env GOPATH)
GOBIN := $(GOPATH)/bin
OCB := $(GOBIN)/ocb

all: fmt vet test build

build:
	go build -v ./...

test:
	go test -v -race -coverprofile=coverage.out ./...

test-integration:
	go test -tags=integration -v ./...

fmt:
	go fmt ./...
	gofmt -s -w .

vet:
	go vet ./...
	go mod tidy

clean:
	go clean -testcache
	rm -f coverage.out
	rm -rf ../otelcol-cockroachdb

install-tools:
	go install go.opentelemetry.io/collector/cmd/mdatagen@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install go.opentelemetry.io/collector/cmd/builder@latest

generate:
	go generate ./...

lint:
	golangci-lint run ./...

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

collector-build:
	cd .. && ocb --config builder-config-cockroach.yaml

collector-run: collector-build
	cd ../otelcol-cockroachdb && ./otelcol-cockroachdb --config ../config.yaml
