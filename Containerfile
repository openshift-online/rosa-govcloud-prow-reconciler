# Built from vendor/ only: no module downloads, so the build needs no network.
FROM registry.access.redhat.com/ubi9/go-toolset:9.8-1792671270 AS build
ARG BUILD_VERSION=dev
COPY . .
# FIPS: use the system OpenSSL for crypto, and refuse to run without it.
ENV GOFLAGS=-mod=vendor GOPROXY=off CGO_ENABLED=1 GOEXPERIMENT=strictfipsruntime
RUN go build -tags strictfipsruntime -ldflags "-X main.version=${BUILD_VERSION}" -o /tmp/bin/ ./cmd/...

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
ARG BUILD_VERSION=dev
LABEL name="rosa-govcloud-prow-reconciler" \
      com.redhat.component="rosa-govcloud-prow-reconciler" \
      description="ROSA GovCloud Prow reconciler" \
      io.k8s.description="ROSA GovCloud Prow reconciler" \
      distribution-scope="public" \
      vendor="Red Hat, Inc." \
      url="https://github.com/openshift-online/rosa-govcloud-prow-reconciler" \
      version="${BUILD_VERSION}" \
      release="${BUILD_VERSION}" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.source="https://github.com/openshift-online/rosa-govcloud-prow-reconciler" \
      org.opencontainers.image.title="ROSA GovCloud Prow Reconciler" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /tmp/bin/reconciler /tmp/bin/conncheck /usr/local/bin/
USER 1001
ENTRYPOINT ["/usr/local/bin/reconciler"]
