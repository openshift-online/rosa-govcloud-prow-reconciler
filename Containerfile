# Built from vendor/ only: no module downloads, so the build needs no network.
FROM registry.access.redhat.com/ubi9/go-toolset:1.26 AS build
ARG BUILD_VERSION=dev
COPY . .
# FIPS: use the system OpenSSL for crypto, and refuse to run without it.
ENV GOFLAGS=-mod=vendor GOPROXY=off CGO_ENABLED=1 GOEXPERIMENT=strictfipsruntime
RUN go build -tags strictfipsruntime -ldflags "-X main.version=${BUILD_VERSION}" -o /tmp/bin/ ./cmd/...

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
COPY --from=build /tmp/bin/reconciler /tmp/bin/conncheck /usr/local/bin/
USER 1001
ENTRYPOINT ["/usr/local/bin/reconciler"]
