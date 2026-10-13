// Package config holds the reconciler's runtime configuration and loads it from
// the environment. Secrets (the Jenkins API token, AWS credentials) come from the
// environment or the AWS SDK's credential chain; never hard-code them.
//
// Parsing is strict: a malformed or out-of-range value is a startup error, never
// a silent fallback to the default. An unset or empty variable takes its default.
// All problems are reported together, and secret values are never echoed.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

// Feed configures how the Prow ProwJob feed is read.
type Feed struct {
	ProwJobsURL   string // public Prow feed, e.g. https://prow.ci.openshift.org/prowjobs.js
	ExecutorLabel string // ProwJob executor label value we claim (default: govcloud-jenkins)
	// FeedTimeout bounds one feed request, including reading the body.
	FeedTimeout time.Duration
	// FeedMaxBytes caps the bytes read from one feed response.
	FeedMaxBytes int64
}

// Jenkins configures the in-boundary Jenkins connection.
type Jenkins struct {
	JenkinsURL      string   // base URL, e.g. https://jenkins.example.internal
	JenkinsUser     string   // basic-auth user (a dedicated service account)
	JenkinsAPIToken string   // basic-auth API token for JenkinsUser
	JobAllowlist    []string // Jenkins job names the reconciler is permitted to trigger
}

// Config is the fully-resolved runtime configuration.
type Config struct {
	Feed
	Jenkins

	PollInterval time.Duration // how often to poll Prow + in-flight builds
	// FeedAbsentPolls is how many consecutive successful, non-empty feed polls an
	// in-flight ProwJob must be missing from before its Jenkins run is stopped.
	FeedAbsentPolls int

	MaxBuildTime time.Duration // give up on an in-flight build after this; write an error Result
	// MaxLogBytes caps the Jenkins console log uploaded as artifacts/build-log.txt;
	// a longer log is cut to its last MaxLogBytes bytes.
	MaxLogBytes int64
	// MaxArtifactBytes caps each archived artifact; larger ones are skipped.
	MaxArtifactBytes int64
	// MaxTotalArtifactBytes caps all archived artifacts of one build together
	// (the console log is capped separately by MaxLogBytes).
	MaxTotalArtifactBytes int64
	// MaxArtifacts caps how many archived artifacts of one build are copied;
	// the rest are skipped.
	MaxArtifacts int

	// --- S3 (in-boundary GovCloud bucket) ---
	S3Bucket string
	S3Region string
	// S3Endpoint overrides the SDK's resolved S3 endpoint (e.g. a VPC endpoint or a
	// local test server). Empty uses the FIPS endpoint for S3Region.
	S3Endpoint string
	// S3UsePathStyle addresses objects as {endpoint}/{bucket}/{key} instead of
	// {bucket}.{endpoint}/{key}; usually needed with S3Endpoint.
	S3UsePathStyle bool

	// ShutdownTimeout bounds how long the step in progress at SIGTERM/SIGINT may
	// keep running before it is cut off.
	ShutdownTimeout time.Duration
}

// Defaults.
const (
	DefaultProwJobsURL   = "https://prow.ci.openshift.org/prowjobs.js"
	DefaultExecutorLabel = "govcloud-jenkins"
	DefaultPollInterval  = 30 * time.Second
	DefaultS3Region      = "us-gov-west-1"
	// DefaultShutdownTimeout matches Kubernetes' default 30s termination grace
	// period; set terminationGracePeriodSeconds above SHUTDOWN_TIMEOUT.
	DefaultShutdownTimeout = 30 * time.Second
	MaxShutdownTimeout     = 10 * time.Minute
)

// Bounds on POLL_INTERVAL. Below a second the loop would hammer Prow and
// Jenkins; above 10 minutes a ProwJob is noticed too late.
const (
	MinPollInterval = time.Second
	MaxPollInterval = 10 * time.Minute
)

// MaxFeedAbsentPolls bounds FEED_ABSENT_POLLS.
const MaxFeedAbsentPolls = 1000

// Feed request settings.
const (
	DefaultFeedTimeout  = prow.DefaultTimeout
	DefaultFeedMaxBytes = prow.DefaultMaxBytes
	MaxFeedTimeout      = 30 * time.Minute
)

// Load builds a Config from getenv, applying defaults and validating every
// value. The returned error lists all problems found.
func Load(getenv func(string) string) (*Config, error) {
	l := &loader{getenv: getenv}
	c := &Config{
		Feed:                  l.feed(),
		Jenkins:               l.jenkins(),
		PollInterval:          l.duration("POLL_INTERVAL", DefaultPollInterval, MinPollInterval, MaxPollInterval),
		FeedAbsentPolls:       l.integer("FEED_ABSENT_POLLS", DefaultFeedAbsentPolls, 1, MaxFeedAbsentPolls),
		MaxBuildTime:          l.duration("MAX_BUILD_TIME", DefaultMaxBuildTime, time.Nanosecond, 0),
		MaxLogBytes:           l.bytes("MAX_LOG_BYTES", DefaultMaxLogBytes),
		MaxArtifactBytes:      l.bytes("MAX_ARTIFACT_BYTES", DefaultMaxArtifactBytes),
		MaxTotalArtifactBytes: l.bytes("MAX_TOTAL_ARTIFACT_BYTES", DefaultMaxTotalArtifactBytes),
		MaxArtifacts:          l.integer("MAX_ARTIFACTS", DefaultMaxArtifacts, 1, 100000),
		S3Bucket:              l.required("S3_BUCKET"),
		S3Region:              l.str("S3_REGION", DefaultS3Region),
		S3Endpoint:            l.str("S3_ENDPOINT", ""),
		S3UsePathStyle:        l.boolean("S3_USE_PATH_STYLE"),
		ShutdownTimeout:       l.duration("SHUTDOWN_TIMEOUT", DefaultShutdownTimeout, time.Second, MaxShutdownTimeout),
	}
	if c.S3Bucket != "" && !validBucket(c.S3Bucket) {
		l.fail("S3_BUCKET must be a valid S3 bucket name (3-63 lowercase letters, digits, '.' or '-')")
	}
	if !regionPattern.MatchString(c.S3Region) {
		l.fail("S3_REGION must be an AWS region name such as %s", DefaultS3Region)
	}
	if c.S3Endpoint != "" {
		l.httpURL("S3_ENDPOINT", c.S3Endpoint)
	}
	if err := l.err(); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadFeed loads and validates only the Prow feed settings (for conncheck).
func LoadFeed(getenv func(string) string) (*Feed, error) {
	l := &loader{getenv: getenv}
	f := l.feed()
	if err := l.err(); err != nil {
		return nil, err
	}
	return &f, nil
}

// LoadJenkins loads and validates only the Jenkins settings (for conncheck).
func LoadJenkins(getenv func(string) string) (*Jenkins, error) {
	l := &loader{getenv: getenv}
	j := l.jenkins()
	if err := l.err(); err != nil {
		return nil, err
	}
	return &j, nil
}

func (l *loader) feed() Feed {
	f := Feed{
		ProwJobsURL:   l.str("PROW_JOBS_URL", DefaultProwJobsURL),
		ExecutorLabel: l.str("EXECUTOR_LABEL", DefaultExecutorLabel),
		FeedTimeout:   l.duration("PROW_FEED_TIMEOUT", DefaultFeedTimeout, time.Second, MaxFeedTimeout),
		FeedMaxBytes:  l.bytes("PROW_FEED_MAX_BYTES", DefaultFeedMaxBytes),
	}
	l.httpURL("PROW_JOBS_URL", f.ProwJobsURL)
	if !labelValuePattern.MatchString(f.ExecutorLabel) {
		l.fail("EXECUTOR_LABEL must be a Kubernetes label value (up to 63 letters, digits, '-', '_' or '.', starting and ending with a letter or digit)")
	}
	return f
}

func (l *loader) jenkins() Jenkins {
	j := Jenkins{
		JenkinsURL: l.required("JENKINS_URL"),
		// The enhancement triggers with basic auth (user:api-token); an
		// unauthenticated ?token= POST is rejected by the fronting proxy.
		JenkinsUser:     l.required("JENKINS_USER"),
		JenkinsAPIToken: l.required("JENKINS_API_TOKEN"),
		JobAllowlist:    l.list("JENKINS_JOB_ALLOWLIST"),
	}
	if j.JenkinsURL != "" {
		l.httpURL("JENKINS_URL", j.JenkinsURL)
	}
	if l.getenv("JENKINS_JOB_TOKEN") != "" {
		l.fail("JENKINS_JOB_TOKEN is not supported: Jenkins is triggered with JENKINS_USER/JENKINS_API_TOKEN basic auth; unset it")
	}
	return j
}

// DefaultMaxBuildTime is the enhancement's 2h build limit.
const DefaultMaxBuildTime = 2 * time.Hour

// DefaultFeedAbsentPolls is FeedAbsentPolls' default: 5 minutes at the default
// 30s poll interval, so a briefly incomplete feed does not stop a build.
const DefaultFeedAbsentPolls = 10

// Size cap defaults.
const (
	DefaultMaxLogBytes           = 50 << 20  // 50 MiB
	DefaultMaxArtifactBytes      = 100 << 20 // 100 MiB
	DefaultMaxTotalArtifactBytes = 1 << 30   // 1 GiB
	DefaultMaxArtifacts          = 500
)

// ReservedParam is the only build parameter the reconciler sends.
const ReservedParam = "PROW_JOB_ID"

var (
	// labelValuePattern is a Kubernetes label value (non-empty here).
	labelValuePattern = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`)
	bucketPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	regionPattern     = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`)
)

func validBucket(b string) bool {
	return bucketPattern.MatchString(b) && !strings.Contains(b, "..")
}

// loader reads variables through getenv and accumulates every problem found.
type loader struct {
	getenv   func(string) string
	problems []string
}

func (l *loader) fail(format string, args ...any) {
	l.problems = append(l.problems, fmt.Sprintf(format, args...))
}

func (l *loader) err() error {
	if len(l.problems) == 0 {
		return nil
	}
	return errors.New("invalid configuration: " + strings.Join(l.problems, "; "))
}

// str returns the variable, or def if it is unset or empty.
func (l *loader) str(key, def string) string {
	if v := l.getenv(key); v != "" {
		return v
	}
	return def
}

// required returns the variable, recording a problem if it is unset or empty.
func (l *loader) required(key string) string {
	v := l.getenv(key)
	if v == "" {
		l.fail("%s is required", key)
	}
	return v
}

// duration parses a Go duration within [min, max] (max <= 0: no upper bound).
func (l *loader) duration(key string, def, min, max time.Duration) time.Duration {
	v := l.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		l.fail("%s: want a Go duration such as 30s or 1h45m, got %q", key, contract.Truncate(v, 64))
	case d < min:
		l.fail("%s must be at least %s, got %s", key, min, d)
	case max > 0 && d > max:
		l.fail("%s must be at most %s, got %s", key, max, d)
	default:
		return d
	}
	return 0
}

// integer parses a decimal integer within [min, max].
func (l *loader) integer(key string, def, min, max int) int {
	v := l.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	switch {
	case err != nil:
		l.fail("%s: want an integer, got %q", key, contract.Truncate(v, 64))
	case n < min || n > max:
		l.fail("%s must be between %d and %d, got %d", key, min, max, n)
	default:
		return n
	}
	return 0
}

// bytes parses a positive byte count (a plain integer).
func (l *loader) bytes(key string, def int64) int64 {
	v := l.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		l.fail("%s: want a positive number of bytes, got %q", key, contract.Truncate(v, 64))
		return 0
	}
	return n
}

// boolean parses an optional boolean; unset means false.
func (l *loader) boolean(key string) bool {
	v := l.getenv(key)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail("%s: want true or false, got %q", key, contract.Truncate(v, 64))
	}
	return b
}

// list parses a comma-separated list. Entries must be non-empty, contain no
// whitespace, and be unique: an allowlist that does not say exactly what it
// means is refused rather than repaired.
func (l *loader) list(key string) []string {
	v := l.getenv(key)
	if v == "" {
		l.fail("%s is required", key)
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, e := range strings.Split(v, ",") {
		switch {
		case e == "":
			l.fail("%s has an empty entry", key)
		case strings.ContainsFunc(e, unicode.IsControl):
			l.fail("%s has an entry with control characters", key)
		case strings.ContainsFunc(e, unicode.IsSpace):
			l.fail("%s entry %q contains whitespace", key, contract.Truncate(e, 64))
		case seen[e]:
			l.fail("%s lists %q more than once", key, contract.Truncate(e, 64))
		default:
			seen[e] = true
			out = append(out, e)
		}
	}
	return out
}

// httpURL requires an absolute http(s) URL with a host and no userinfo, query,
// or fragment. The value is never echoed, since a URL can carry credentials.
// For JENKINS_URL this matters beyond hygiene: the Jenkins client only sends
// credentials to this origin and rebases URLs Jenkins returns onto it.
func (l *loader) httpURL(key, raw string) {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		l.fail("%s is not a valid URL", key)
	case (u.Scheme != "https" && u.Scheme != "http") || u.Host == "":
		l.fail("%s must be an absolute http(s) URL", key)
	case u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "":
		l.fail("%s must not contain credentials, a query, or a fragment", key)
	}
}
