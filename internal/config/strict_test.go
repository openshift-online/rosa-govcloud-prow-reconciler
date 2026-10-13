package config

import (
	"strings"
	"testing"
	"time"
)

// validEnv is a minimal valid environment.
func validEnv() map[string]string {
	return map[string]string{
		"JENKINS_URL":           "https://jenkins.example.internal",
		"JENKINS_USER":          "svc",
		"JENKINS_API_TOKEN":     "api-token",
		"JENKINS_JOB_ALLOWLIST": "example-job",
		"S3_BUCKET":             "example-bucket",
	}
}

func load(env map[string]string) (*Config, error) {
	return Load(func(k string) string { return env[k] })
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(validEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.ProwJobsURL != DefaultProwJobsURL || c.ExecutorLabel != DefaultExecutorLabel ||
		c.PollInterval != 30*time.Second || c.FeedAbsentPolls != 10 || c.S3Region != "us-gov-west-1" ||
		len(c.JobAllowlist) != 1 || c.ShutdownTimeout != 30*time.Second {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

// Every malformed or out-of-range value is a startup error: nothing silently
// falls back to its default.
func TestLoadRejects(t *testing.T) {
	for _, tc := range []struct {
		key, value, want string
	}{
		{"POLL_INTERVAL", "30", "POLL_INTERVAL: want a Go duration"},
		{"POLL_INTERVAL", "thirty seconds", "POLL_INTERVAL: want a Go duration"},
		{"POLL_INTERVAL", "0s", "POLL_INTERVAL must be at least 1s"},
		{"POLL_INTERVAL", "-30s", "POLL_INTERVAL must be at least 1s"},
		{"POLL_INTERVAL", "500ms", "POLL_INTERVAL must be at least 1s"},
		{"POLL_INTERVAL", "11m", "POLL_INTERVAL must be at most 10m0s"},
		{"MAX_BUILD_TIME", "1h45", "MAX_BUILD_TIME: want a Go duration"},
		{"MAX_BUILD_TIME", "0s", "MAX_BUILD_TIME must be at least"},
		{"FEED_ABSENT_POLLS", "ten", "FEED_ABSENT_POLLS: want an integer"},
		{"FEED_ABSENT_POLLS", "0", "FEED_ABSENT_POLLS must be between 1 and 1000"},
		{"FEED_ABSENT_POLLS", "1001", "FEED_ABSENT_POLLS must be between 1 and 1000"},
		{"MAX_LOG_BYTES", "50MiB", "MAX_LOG_BYTES: want a positive number of bytes"},
		{"MAX_ARTIFACT_BYTES", "-1", "MAX_ARTIFACT_BYTES: want a positive number of bytes"},
		{"MAX_TOTAL_ARTIFACT_BYTES", "0", "MAX_TOTAL_ARTIFACT_BYTES: want a positive number of bytes"},
		{"S3_USE_PATH_STYLE", "yes", "S3_USE_PATH_STYLE: want true or false"},
		{"PROW_JOBS_URL", "prow.ci.openshift.org/prowjobs.js", "PROW_JOBS_URL must be an absolute http(s) URL"},
		{"PROW_JOBS_URL", "file:///tmp/prowjobs.js", "PROW_JOBS_URL must be an absolute http(s) URL"},
		{"PROW_JOBS_URL", "https://prow.example.internal/prowjobs.js?var=allBuilds", "PROW_JOBS_URL must not contain"},
		{"JENKINS_URL", "jenkins.example.internal", "JENKINS_URL must be an absolute http(s) URL"},
		{"JENKINS_URL", "ftp://jenkins.example.internal", "JENKINS_URL must be an absolute http(s) URL"},
		{"JENKINS_URL", "https://svc:pw@jenkins.example.internal", "JENKINS_URL must not contain credentials"},
		{"JENKINS_URL", "https://jenkins.example.internal/#x", "JENKINS_URL must not contain"},
		{"JENKINS_URL", "https://jenkins.example.internal/%zz", "JENKINS_URL is not a valid URL"},
		{"S3_ENDPOINT", "127.0.0.1:9000", "S3_ENDPOINT"},
		{"S3_ENDPOINT", "https://s3.example.internal/?X-Amz-Signature=x", "S3_ENDPOINT must not contain"},
		{"EXECUTOR_LABEL", "govcloud jenkins", "EXECUTOR_LABEL must be a Kubernetes label value"},
		{"EXECUTOR_LABEL", "-govcloud", "EXECUTOR_LABEL must be a Kubernetes label value"},
		{"EXECUTOR_LABEL", strings.Repeat("a", 64), "EXECUTOR_LABEL must be a Kubernetes label value"},
		{"S3_BUCKET", "Example_Bucket", "S3_BUCKET must be a valid S3 bucket name"},
		{"S3_BUCKET", "ab", "S3_BUCKET must be a valid S3 bucket name"},
		{"S3_BUCKET", "a..b", "S3_BUCKET must be a valid S3 bucket name"},
		{"S3_REGION", "US-GOV-WEST-1", "S3_REGION must be an AWS region name"},
		{"JENKINS_JOB_ALLOWLIST", "example-job,", "JENKINS_JOB_ALLOWLIST has an empty entry"},
		{"JENKINS_JOB_ALLOWLIST", ",example-job", "JENKINS_JOB_ALLOWLIST has an empty entry"},
		{"JENKINS_JOB_ALLOWLIST", "example-job, other-job", `JENKINS_JOB_ALLOWLIST entry " other-job" contains whitespace`},
		{"JENKINS_JOB_ALLOWLIST", "example job", "contains whitespace"},
		{"JENKINS_JOB_ALLOWLIST", "example-job,example-job", `JENKINS_JOB_ALLOWLIST lists "example-job" more than once`},
		{"JENKINS_JOB_ALLOWLIST", "a\x00b", "JENKINS_JOB_ALLOWLIST has an entry with control characters"},
		{"JENKINS_JOB_TOKEN", "x", "JENKINS_JOB_TOKEN is not supported"},
		{"SHUTDOWN_TIMEOUT", "30", "SHUTDOWN_TIMEOUT: want a Go duration"},
		{"SHUTDOWN_TIMEOUT", "0s", "SHUTDOWN_TIMEOUT must be at least 1s"},
		{"SHUTDOWN_TIMEOUT", "11m", "SHUTDOWN_TIMEOUT must be at most 10m0s"},
		{"PROW_FEED_TIMEOUT", "5", "PROW_FEED_TIMEOUT: want a Go duration"},
		{"PROW_FEED_TIMEOUT", "0s", "PROW_FEED_TIMEOUT must be at least 1s"},
		{"PROW_FEED_TIMEOUT", "31m", "PROW_FEED_TIMEOUT must be at most 30m0s"},
		{"PROW_FEED_MAX_BYTES", "1GiB", "PROW_FEED_MAX_BYTES: want a positive number of bytes"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			env := validEnv()
			env[tc.key] = tc.value
			_, err := load(env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadRequired(t *testing.T) {
	for _, key := range []string{"JENKINS_URL", "JENKINS_USER", "JENKINS_API_TOKEN", "JENKINS_JOB_ALLOWLIST", "S3_BUCKET"} {
		env := validEnv()
		delete(env, key)
		_, err := load(env)
		if err == nil || !strings.Contains(err.Error(), key+" is required") {
			t.Errorf("without %s: err = %v", key, err)
		}
	}
}

// All problems are reported at once, so a misconfigured deployment is fixed in
// one round.
func TestLoadReportsAllProblems(t *testing.T) {
	_, err := load(map[string]string{
		"POLL_INTERVAL":     "soon",
		"FEED_ABSENT_POLLS": "0",
		"S3_USE_PATH_STYLE": "maybe",
	})
	if err == nil {
		t.Fatal("empty environment accepted")
	}
	for _, want := range []string{
		"JENKINS_URL is required", "JENKINS_USER is required", "JENKINS_API_TOKEN is required",
		"JENKINS_JOB_ALLOWLIST is required", "S3_BUCKET is required",
		"POLL_INTERVAL: want a Go duration", "FEED_ABSENT_POLLS must be between", "S3_USE_PATH_STYLE: want true or false",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// Secret values never appear in errors, even when they are what is wrong.
func TestLoadDoesNotEchoSecrets(t *testing.T) {
	const secret = "s3cr3t-value"
	env := validEnv()
	env["JENKINS_API_TOKEN"] = secret
	env["JENKINS_JOB_TOKEN"] = secret
	env["JENKINS_URL"] = "https://svc:" + secret + "@jenkins.example.internal"
	env["S3_ENDPOINT"] = "https://key:" + secret + "@s3.example.internal"
	env["PROW_JOBS_URL"] = "https://prow.example.internal/prowjobs.js?token=" + secret
	_, err := load(env)
	if err == nil {
		t.Fatal("accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error echoes a secret: %v", err)
	}
}

func TestLoadSections(t *testing.T) {
	env := validEnv()
	j, err := LoadJenkins(func(k string) string { return env[k] })
	if err != nil || j.JenkinsURL != env["JENKINS_URL"] || j.JenkinsUser != "svc" {
		t.Errorf("LoadJenkins = %+v, %v", j, err)
	}
	if _, err := LoadJenkins(func(string) string { return "" }); err == nil {
		t.Error("LoadJenkins accepted an empty environment")
	}
	f, err := LoadFeed(func(string) string { return "" })
	if err != nil || f.ProwJobsURL != DefaultProwJobsURL {
		t.Errorf("LoadFeed = %+v, %v", f, err)
	}
	if _, err := LoadFeed(func(k string) string { return map[string]string{"EXECUTOR_LABEL": "a b"}[k] }); err == nil {
		t.Error("LoadFeed accepted a bad label")
	}
}

func TestFeedSettings(t *testing.T) {
	c, err := load(validEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.FeedTimeout != 5*time.Minute || c.FeedMaxBytes != 1<<30 {
		t.Errorf("feed defaults: %+v", c.Feed)
	}
	env := validEnv()
	env["PROW_FEED_TIMEOUT"] = "2m"
	env["PROW_FEED_MAX_BYTES"] = "1000"
	if c, err = load(env); err != nil || c.FeedTimeout != 2*time.Minute || c.FeedMaxBytes != 1000 {
		t.Errorf("overrides: %+v, %v", c, err)
	}
}
