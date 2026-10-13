# ROSA GovCloud Prow Reconciler

The in-boundary half of the ROSA GovCloud CI bridge described in the enhancement
[ROSA GovCloud CI Bridge: Run Prow Jobs Against In-Boundary Jenkins via S3](https://github.com/openshift-online/rosa-enhancements/blob/main/enhancements/sre-tooling/rosa-govcloud-prow-jenkins-bridge.md)
(`enhancements/sre-tooling/rosa-govcloud-prow-jenkins-bridge.md`). It watches Prow's
public `prowjobs.js` feed, triggers the matching allowlisted Jenkins job inside the
boundary, and writes the build's artifacts and `result.json` to S3, where the Prow-side
CI shim picks them up. It is the **writer** half of the S3 contract
(`internal/contract`); the CI shim is the reader.

**Documentation:** [docs/DESIGN.md](docs/DESIGN.md) explains the architecture, job
lifecycle, retry rules and the `result.json` contract with diagrams.

```
Prow Deck (public)          Reconciler (in-boundary)          Jenkins (in-boundary)
prowjobs.js  <──GET (egress)──  poll ── trigger/poll/fetch ──>  build
                                  │
                                  └── PutObject ──> S3: {PROW_JOB_ID}/artifacts/*
                                                        {PROW_JOB_ID}/result.json
```

Every connection is outbound from the boundary. The reconciler **never writes back to
Prow**; Prow advances the ProwJob once the shim reads `result.json`.

> **Only a single replica may run.** There is no leader election or lease: two replicas
> would each see the same ProwJob in the feed and each trigger a Jenkins build for it
> (and race on `result.json`). Run exactly one instance (e.g. a Deployment with
> `replicas: 1` and the `Recreate` strategy, so the old pod is gone before a new one
> starts). Restarts are safe: on startup the reconciler rebuilds its state from Jenkins
> before triggering anything (see [Restarts](#restarts)).

## How it works

Every `POLL_INTERVAL` the loop in `internal/reconciler` runs one *tick*: discover, then
advance every in-flight job, then retry pending Jenkins stops. Mapped to the
enhancement's lifecycle and Reconciler State Machine:

| Step | State machine | What happens |
|------|---------------|--------------|
| Poll the feed | — | `GET prowjobs.js?omit=pod_spec,decoration_config`, stream-decoded; only ProwJobs with `metadata.labels.executor == EXECUTOR_LABEL` and a `govcloud.ci/jenkins-job` annotation are kept (`internal/prow`). A failed, truncated, or oversized poll is skipped entirely. |
| Match | `[*] --> Discovered` | Active (`triggered`/`pending`) ProwJobs not already tracked or done. Names that are not a canonical UUID are ignored (no safe S3 key). |
| Recover | — | At startup (before any trigger), and after an uncertain trigger, an existing Jenkins run carrying the ProwJob's `PROW_JOB_ID` is adopted instead of triggering again. |
| Validate | `Discovered --> Rejected --> [*]` | Jenkins job not in `JENKINS_JOB_ALLOWLIST`, or a ProwJob already older than `MAX_BUILD_TIME`: an `error` `result.json` is written and Jenkins is not called. |
| Trigger | `Discovered --> Triggered` | `POST {JENKINS_URL}/job/<job>/buildWithParameters` with basic auth and `PROW_JOB_ID=<uuid>` in the form body; `201` + `Location` gives the queue item. |
| Queue | `Triggered --> Monitoring` / `Triggered --> Error` | The queue item is polled until it names a build; a cancelled item writes an `error` result. |
| Monitor | `Monitoring --> Monitoring` / `--> Collecting` | `GET {build}/api/json` each tick until `building=false`. |
| Time out | `Monitoring --> Timeout --> [*]` | Past `MAX_BUILD_TIME` (counted from the ProwJob's start), the run is stopped in Jenkins, its log so far uploaded, and an `error` result ("timed out: ...") written. |
| Gone | (not in the state machine) | If the ProwJob turns terminal in the feed or is missing from `FEED_ABSENT_POLLS` successful polls, the run is stopped and an `error` result written. |
| Collect | `Collecting --> Uploading` / `Collecting --> Error` | `consoleText` becomes `artifacts/build-log.txt`; archived artifacts are downloaded with size caps. |
| Upload | `Uploading --> Complete` / `Uploading --> Error` | Each artifact is `PutObject`'d to `{PROW_JOB_ID}/artifacts/...`, **then** `result.json`. |
| Result | `Monitoring --> Error` / `Complete` | Status from the Jenkins result (table below) if every output reached S3; otherwise `error`. |
| Done | `Complete --> [*]` | The job leaves tracking; a *done* record stops it being triggered or written again while Prow still lists it as active, and is pruned once the feed shows it finished. |

Status mapping, per the enhancement (`internal/contract`):

| Jenkins result | `result.json` status |
|----------------|----------------------|
| `SUCCESS` | `success` |
| `FAILURE` / `UNSTABLE` | `failure` |
| `ABORTED` / `NOT_BUILT` | `error` |
| anything else, or none | `error` |
| any result, but outputs incomplete | `error` |

Transient Jenkins or S3 errors keep the job in flight and are retried on the next tick;
terminal errors and timeouts write an `error` `result.json` so the shim always resolves.

### Discovery and validation

The reconciler matches a ProwJob like this (ProwJob-level labels and annotations, so
the periodics are raw definitions in `openshift/release` rather than prowgen output):

```yaml
metadata:
  name: 0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c   # PROW_JOB_ID
  labels:
    executor: govcloud-jenkins                 # == EXECUTOR_LABEL
  annotations:
    govcloud.ci/jenkins-job: example-job       # must be in JENKINS_JOB_ALLOWLIST
```

The feed is public and untrusted (for example, rehearsal jobs from `openshift/release`
pull requests can set labels and annotations), so before acting on a ProwJob the
reconciler:

- ignores it unless `metadata.name` is a canonical lowercase UUID (no result is written,
  since there is no safe S3 key for it);
- rejects it with an `error` `result.json` if the Jenkins job is not in
  `JENKINS_JOB_ALLOWLIST`;
- sends Jenkins only `PROW_JOB_ID`; no other annotation is forwarded as a parameter.

**The feed.** `prowjobs.js` is the whole-instance firehose (~186 MB). The reconciler
asks Deck to leave out the pod spec and decoration config (`omit=pod_spec,decoration_config`;
Deck's `prowjobs.js` handler supports `omit`, and a Deck that does not ignores it) and
decodes the response as a stream, keeping only ProwJobs with the executor label, so
memory is bounded by the matches plus one ProwJob. A poll counts only if the whole
response was read and parsed as one complete JSON object with an `items` array within
`PROW_FEED_TIMEOUT` and `PROW_FEED_MAX_BYTES`. A non-200, truncated (including a
truncated gzip stream), malformed, or oversized response is a failed poll: nothing is
triggered from it and it is never taken as evidence that a ProwJob is gone.

### Collecting outputs

The Jenkins console log (`{build}/consoleText`) is uploaded as `artifacts/build-log.txt`,
as the contract requires, for every result that follows a started build (success,
failure, aborted, timeout, ProwJob gone) and listed in `result.json`'s `artifacts`. It is
streamed and only its last `MAX_LOG_BYTES` are kept: a longer log is cut to that tail
behind a `[build log truncated by the reconciler: ...]` line, since the end of the log is
what explains a failure.

Archived Jenkins artifacts keep their relative paths under `artifacts/` (e.g.
`artifacts/reports/junit_01.xml`). A path is skipped if it is absolute or contains empty,
`.` or `..` segments, backslashes, control characters, or invalid UTF-8, if the object
key would exceed S3's 1024-byte limit, or if it is `build-log.txt` (reserved for the
console log). Each artifact is read with a limit: one larger than `MAX_ARTIFACT_BYTES`,
or than what is left of `MAX_TOTAL_ARTIFACT_BYTES` for the build, is skipped without
being buffered. At most `MAX_ARTIFACTS` artifacts are copied per build; the rest are skipped.

**Incomplete outputs are an `error`.** This follows the state machine
(`Collecting --> Error: fetch failed`, `Uploading --> Error: S3 write failed`). If
listing or downloading an artifact or the console log from Jenkins, or uploading one to
S3, fails, the job stays in flight and collection is retried on the next tick, up to 3
attempts. If it still fails, `result.json` is written with `status: error` whatever
Jenkins reported, and `message` gives the Jenkins result and what is missing, e.g.
`Jenkins build 17 finished with SUCCESS, but 2 artifacts could not be fetched from
Jenkins: "a.xml", "b.xml"; build-log.txt could not be uploaded to S3` (up to 5 names per
problem). An artifact skipped on purpose (size cap, unsafe or reserved path) also makes
the result `error`, without retries, e.g. `... but 1 artifact skipped: "b.bin" (larger
than MAX_ARTIFACT_BYTES (104857600))`: the reconciler never reports a pass for a run
whose outputs did not all reach S3. Raise the caps or change what the job archives if
this happens. `artifacts` lists only what was actually uploaded. A truncated console
log is noted in `message` but does not change `status`. Results that are already
`error` (timeouts, a ProwJob that is gone) just have the missing outputs appended.

**S3.** The reconciler only calls `PutObject` (never Get/Head/List/Delete), and uploads
every artifact **before** `result.json`, which the shim treats as the completion signal.
The SDK's standard retryer retries transient S3 errors, and each upload is bounded by 2
minutes plus 1 second per MiB. A failed `result.json` write is retried on the next tick.
Objects get a `Content-Type` from their extension (`application/json`,
`application/xml`, `text/plain`, else `application/octet-stream`). No
server-side-encryption headers are sent, so the bucket's default encryption applies.
FIPS endpoints (`s3-fips.{region}.amazonaws.com`) are always used. Setting `S3_ENDPOINT`
replaces the endpoint entirely (the SDK does not combine FIPS with a custom endpoint), so
point it at a FIPS-capable endpoint, e.g. a FIPS VPC endpoint.
S3 errors are logged with bucket/key, AWS error code, HTTP status, and request ID only,
never request URLs, headers, or credentials.

### Timeouts and stopping builds

The reconciler's `MAX_BUILD_TIME` (default `2h`) is the give-up time per run; its
`error` result ("timed out: ...") is written to S3. The clock starts at the earlier of
the trigger and the ProwJob's `status.startTime`, so it includes Jenkins queue time and
any delay before the reconciler noticed the ProwJob. It is not checked against the CI
shim's own timeout: keep the shim's wait longer than `MAX_BUILD_TIME` if the timeout
result should reach it. A ProwJob already older than `MAX_BUILD_TIME` when first seen is
not triggered and gets an `error` result.

A timed-out run is **stopped in Jenkins**: a running build gets `POST {build}/stop`, a
queued item gets `POST {JENKINS_URL}/queue/cancelItem?id=N`. Since a queue item can start
just as it is cancelled and a build can ignore an abort, each attempt re-reads the run
first, and attempts repeat on later ticks (up to 10) until Jenkins shows it is no longer
queued or building. `result.json` is written right away and does not wait for that.

The same applies when nothing is waiting for the run any more: the ProwJob shows up in
the feed as `aborted` (or `failure`/`error`/`success`, e.g. after the shim timed out), or
it has been missing from `FEED_ABSENT_POLLS` consecutive **successful, non-empty** feed
polls. An `error` `result.json` ("ProwJob is aborted in Prow; stopping the Jenkins run")
is still written, so a shim that is in fact still polling fails fast. If the build
already finished, its real result is written instead.

Jenkins API requests (including reading the response) are bounded by 30s each, artifact
and console-log downloads by 15 minutes each, and a feed poll by `PROW_FEED_TIMEOUT`.

### Done records

A ProwJob stays `pending` in the feed until the shim reads `result.json` and Prow updates
it. Once the reconciler has written a result (or ignored or rejected the ProwJob) it keeps
a *done* record for that `PROW_JOB_ID` and never triggers Jenkins or rewrites
`result.json` for it again. Done records are dropped once the feed reports the ProwJob as
finished, or after it has been missing from the feed for `MAX_BUILD_TIME`.

### Restarts

State is in memory, and the reconciler's S3 identity is write-only (it cannot read what
it wrote), so on startup it rebuilds state from Jenkins **before triggering anything**:
for each allowlisted job it reads the queue and the most recent 100 builds, keyed by
their `PROW_JOB_ID` parameter. An active ProwJob with a matching run is adopted rather
than triggered: a queued or running build is monitored, and a build that already finished
has its outputs collected and its result written. A recovered run that is still queued or
building for a ProwJob the feed reports as finished is stopped. If Jenkins cannot be
read, the reconciler logs the error and retries with backoff (capped at 5 minutes)
instead of triggering blind. The same check guards retries: if a trigger request fails
(the build may have been queued even though the response was lost), Jenkins is queried
for that `PROW_JOB_ID` before triggering again.

### Shutdown

On SIGTERM or SIGINT the reconciler stops starting anything new (no feed poll, trigger,
or next job) and lets the step in progress finish (a Jenkins call, or one job's uploads
and `result.json` write) on a separate context for up to `SHUTDOWN_TIMEOUT`; keep the
pod's `terminationGracePeriodSeconds` above it. It then exits 0. If the step is still
running at `SHUTDOWN_TIMEOUT` it is cut off and the process exits 1. Because
`result.json` is always written after the artifacts, a cut-off can leave artifacts
without `result.json` (the next run completes them), never the reverse, and
`result.json` is never written for outputs that failed only because of the cut-off.
Jenkins builds are **not** stopped on shutdown: restart recovery adopts them.

## Configuration

All configuration is via environment variables; the only argument is
`reconciler -version`, which prints the build version to stderr. Every variable is
validated at startup: a malformed or out-of-range value (a bad duration,
integer, or boolean; a non-http(s) URL or one with credentials, a query, or a fragment;
an allowlist entry that is empty, duplicated, or contains whitespace) is an error, never
a silent fallback to the default. An unset or empty variable takes its default. All
problems are reported together, and secret values are never echoed. See
[`.env.example`](.env.example).

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `JENKINS_URL` | — | yes | Jenkins base URL, e.g. `https://jenkins.example.internal`; http(s), no credentials, query, or fragment. Credentials are only ever sent to this origin. |
| `JENKINS_USER` | — | yes | basic-auth user for every Jenkins call, including the trigger. Use a dedicated service account. |
| `JENKINS_API_TOKEN` | — | yes | API token for `JENKINS_USER`. Never logged. |
| `JENKINS_JOB_ALLOWLIST` | — | yes | comma-separated Jenkins job names that may be triggered (e.g. `example-job,another-example-job`); must contain the ProwJob's `govcloud.ci/jenkins-job` value |
| `JENKINS_JOB_TOKEN` | — | must be unset | not supported; startup fails if set. The enhancement triggers with basic auth, and a token-only `?token=` POST is rejected (403). |
| `S3_BUCKET` | — | yes | results bucket (written as `{PROW_JOB_ID}/...`) |
| `S3_REGION` | `us-gov-west-1` | no | AWS region |
| `S3_ENDPOINT` | (region FIPS endpoint) | no | http(s) URL overriding the S3 endpoint (e.g. a VPC endpoint or a local test server); no credentials, query, or fragment |
| `S3_USE_PATH_STYLE` | `false` | no | address objects as `{endpoint}/{bucket}/{key}`; usually needed with `S3_ENDPOINT` |
| `PROW_JOBS_URL` | `https://prow.ci.openshift.org/prowjobs.js` | no | Deck feed URL; http(s), no credentials, query, or fragment |
| `EXECUTOR_LABEL` | `govcloud-jenkins` | no | value of the ProwJob `executor` label to match (a Kubernetes label value) |
| `PROW_FEED_TIMEOUT` | `5m` | no | deadline for one feed request including the body (1s to 30m) |
| `PROW_FEED_MAX_BYTES` | `1073741824` (1 GiB) | no | a larger (decompressed) feed response is a failed poll |
| `POLL_INTERVAL` | `30s` | no | tick interval (1s to 10m) |
| `FEED_ABSENT_POLLS` | `10` | no | consecutive successful, non-empty polls an in-flight ProwJob must be missing from before its run is stopped (1 to 1000) |
| `MAX_BUILD_TIME` | `2h` | no | give-up time per run, from the ProwJob's start; see [Timeouts](#timeouts-and-stopping-builds) |
| `MAX_LOG_BYTES` | `52428800` (50 MiB) | no | cap on `build-log.txt`; a longer console log keeps its tail |
| `MAX_ARTIFACT_BYTES` | `104857600` (100 MiB) | no | larger archived artifacts are skipped (making the result `error`) |
| `MAX_TOTAL_ARTIFACT_BYTES` | `1073741824` (1 GiB) | no | cap on all archived artifacts of one build, console log excluded |
| `MAX_ARTIFACTS` | `500` | no | cap on archived artifacts copied per build; extras are skipped (making the result `error`) |
| `SHUTDOWN_TIMEOUT` | `30s` | no | how long the step in progress at SIGTERM/SIGINT may run (1s to 10m) |

AWS credentials and settings come from the AWS SDK's default chain, not from the table
above: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE` and shared config, web
identity (IRSA), or an instance role. Credentials are never logged.

`cmd/conncheck` reads the `PROW_*` and `EXECUTOR_LABEL` variables and, if `JENKINS_URL`
is set, `JENKINS_URL`, `JENKINS_USER`, `JENKINS_API_TOKEN`, and `JENKINS_JOB_ALLOWLIST`,
validated the same way.

## Requirements

### IAM

The reconciler's identity is **write-only** and **separate** from the shim's read-only
identity, as the enhancement requires. It needs exactly:

```json
{
  "Effect": "Allow",
  "Action": "s3:PutObject",
  "Resource": "arn:aws-us-gov:s3:::<S3_BUCKET>/*"
}
```

No `s3:GetObject`, `s3:ListBucket`, `s3:DeleteObject`, or bucket-level permission is
used. If the bucket's default encryption is SSE-KMS with a customer-managed key, the
identity also needs `kms:GenerateDataKey` on that key (uploads are single `PutObject`
calls, never multipart, so `kms:Decrypt` is not needed), and the key policy must allow it.

### Jenkins jobs

Each allowlisted job must:

- declare a **string parameter `PROW_JOB_ID`**. The reconciler passes the ProwJob UUID
  in it and relies on it to find its builds again after a restart;
- **archive** the files to surface in Spyglass (e.g. `junit_*.xml`) with Jenkins'
  artifact archiver, within the size caps. Only what the job archives, plus its console
  log, leaves the boundary, so archive only non-protected CI output;
- be buildable by `JENKINS_USER` through `buildWithParameters` with basic auth. Use a
  dedicated, least-privilege service account (read and build on the allowlisted jobs
  only), never a personal user's API token.

`cmd/conncheck` checks that each allowlisted job exists, is buildable, and declares
`PROW_JOB_ID`.

### Network

Egress only: HTTPS to Deck (`PROW_JOBS_URL`), to `JENKINS_URL`, and to the S3 endpoint.
Nothing connects inward.

## Validate connections (read-only)

`cmd/conncheck` probes both sides without triggering a build or writing to S3:

```bash
# Prow only:
go run ./cmd/conncheck

# Both sides:
JENKINS_URL=https://jenkins.example.internal \
JENKINS_USER=<JENKINS_USER> JENKINS_API_TOKEN=<JENKINS_API_TOKEN> \
JENKINS_JOB_ALLOWLIST=example-job \
  go run ./cmd/conncheck
```

It reports feed reachability and matching jobs, and for each allowlisted Jenkins job
whether it is buildable and declares `PROW_JOB_ID`. It exits 0 only if every check
passes.

## Build, test, run

Dependencies (`aws-sdk-go-v2` and its transitive modules) are vendored under `vendor/`,
so builds need no module proxy or network access:

```bash
make build    # bin/reconciler and bin/conncheck, version from git describe
make test     # go test -race -cover ./...
make verify   # gofmt and go vet
make lint     # golangci-lint (.golangci.yml)
make image    # podman build -f Containerfile
```

The Makefile sets `GOFLAGS=-mod=vendor GOPROXY=off`; set them yourself when calling
`go` directly. The `Containerfile` builds both binaries from `vendor/` (no network) with
Red Hat's Go toolset in FIPS mode (`CGO_ENABLED=1`, `GOEXPERIMENT=strictfipsruntime`,
system OpenSSL) on a UBI 9 minimal base; the entrypoint is `reconciler`, and
`conncheck` is in `/usr/local/bin` too.

After changing dependencies, run `go mod tidy && go mod vendor` and commit `vendor/`.
No runtime binaries are required.

```bash
JENKINS_URL=https://jenkins.example.internal \
JENKINS_USER=<JENKINS_USER> JENKINS_API_TOKEN=<JENKINS_API_TOKEN> \
JENKINS_JOB_ALLOWLIST=example-job \
S3_BUCKET=<S3_BUCKET> \
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID> AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> \
  go run ./cmd/reconciler
```

Logs are structured text on stderr.

## Layout

| Path | Role |
|------|------|
| `cmd/reconciler` | entrypoint: load config, wire clients, run the loop until SIGTERM/SIGINT |
| `cmd/conncheck` | read-only connection check for Prow and Jenkins |
| `internal/config` | strict environment parsing and validation; allowlists |
| `internal/prow` | streaming read and filter of the public ProwJob feed (read-only) |
| `internal/jenkins` | trigger, queue, build status, stop/cancel, artifacts, console log; credential scoping |
| `internal/s3` | `Writer` interface; `SDKWriter` uploads with `aws-sdk-go-v2` (`PutObject` only) |
| `internal/contract` | the `result.json` shape and status mapping shared with the shim |
| `internal/reconciler` | the control loop, in-flight/done tracking, collection, stops, startup recovery, shutdown |

## Security notes

- Jenkins credentials are only sent to the scheme/host/port of `JENKINS_URL`. URLs
  Jenkins hands back (queue `Location`, `executable.url`) are rebased onto `JENKINS_URL`
  when their path sits under its path (which also covers a Jenkins root URL naming a
  different host); anything else is rejected, and redirects off that origin are refused.
- Build parameters go in the form-encoded POST body, never the URL; URLs are scrubbed of
  query strings and userinfo before they appear in errors or logs.
- Artifact paths are validated so they cannot escape the build's `artifact/` tree or the
  S3 `artifacts/` prefix; each path segment is URL-escaped when fetched.
- Untrusted feed strings are truncated before they are logged or written to a (public)
  `result.json` message.

## Known limitations

- **Single replica only** (see above): there is no leader election.
- **Single-goroutine loop.** Feed polls and artifact transfers run inline, so a slow
  feed (up to `PROW_FEED_TIMEOUT`) or collecting a large build (up to 1 GiB of artifacts
  plus a 50 MiB log by default) delays the other jobs for that time.
- **Memory.** Each artifact is held in memory while it is uploaded (up to
  `MAX_ARTIFACT_BYTES`), as is the console log tail (up to `MAX_LOG_BYTES`).
- **`omit` support.** The feed request relies on Deck honouring `omit`; a Deck that
  ignores it still works, but serves (and the reconciler reads) the full feed.
- **CSRF crumbs** are not requested. Jenkins exempts API-token basic auth from crumb
  checks, which is the supported trigger path.
- **Stopping builds.** `POST {build}/stop` is a normal abort; a Pipeline build that
  ignores it would need `/term` or `/kill`, which are not sent. After 10 unconfirmed
  attempts the reconciler logs an error and gives up. Pending stops are not persisted
  across restarts (a recovered run is only stopped if its ProwJob is reported finished
  in the feed, not if it has vanished from it), and a trigger whose response was lost is
  not cancelled if its ProwJob ends before Jenkins is checked.
- **Recovery window.** Startup recovery scans the queue and the last 100 builds of each
  allowlisted job. A build older than that whose ProwJob is still active would be
  re-triggered.
- **Forged results.** Anyone holding the reconciler's write-only credentials can write
  any `result.json`; see the enhancement's Risks section.

## License

Apache License 2.0; see [LICENSE](LICENSE).
