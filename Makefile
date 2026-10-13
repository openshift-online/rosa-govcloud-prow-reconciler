# Builds use the vendored modules only, so no network is needed.
export GOFLAGS := -mod=vendor
export GOPROXY := off

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE ?= rosa-govcloud-prow-reconciler:$(VERSION)

.PHONY: build test verify lint image clean

build:
	go build -ldflags '-X main.version=$(VERSION)' -o bin/ ./cmd/...

test:
	go test -race -count=1 -cover ./...

verify:
	@out=$$(gofmt -l cmd internal); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...

lint:
	golangci-lint run

image:
	podman build --build-arg BUILD_VERSION=$(VERSION) -t $(IMAGE) -f Containerfile .

clean:
	rm -rf bin
