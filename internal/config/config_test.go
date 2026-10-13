package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Setenv("JENKINS_URL", "https://jenkins.example.internal/jenkins")
	t.Setenv("JENKINS_JOB_ALLOWLIST", "example-job")
	t.Setenv("S3_BUCKET", "example-bucket")
	t.Setenv("JENKINS_USER", "svc")
	t.Setenv("JENKINS_API_TOKEN", "api-token")
}

// The enhancement triggers Jenkins with basic auth; there is no token-only mode.
func TestJenkinsBasicAuthRequired(t *testing.T) {
	for _, key := range []string{"JENKINS_USER", "JENKINS_API_TOKEN"} {
		setRequired(t)
		t.Setenv(key, "")
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("config without %s accepted", key)
		}
	}
	setRequired(t)
	t.Setenv("JENKINS_JOB_TOKEN", "s3cr3t-job-token-value")
	_, err := Load(os.Getenv)
	if err == nil {
		t.Fatal("JENKINS_JOB_TOKEN accepted")
	}
	if strings.Contains(err.Error(), "s3cr3t") {
		t.Errorf("error echoes the token: %v", err)
	}
}

func TestLoadValidatesJenkinsURL(t *testing.T) {
	setRequired(t)
	if _, err := Load(os.Getenv); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for _, bad := range []string{
		"jenkins.example.internal",
		"ftp://jenkins.example.internal",
		"https://user:pw@jenkins.example.internal",
		"https://jenkins.example.internal/?token=x",
	} {
		t.Setenv("JENKINS_URL", bad)
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("JENKINS_URL=%q accepted, want error", bad)
		}
	}
}

func TestS3EndpointOptions(t *testing.T) {
	setRequired(t)
	c, err := Load(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if c.S3Region != "us-gov-west-1" || c.S3Endpoint != "" || c.S3UsePathStyle {
		t.Errorf("unexpected S3 defaults: %+v", c)
	}

	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("S3_USE_PATH_STYLE", "1")
	if c, err = Load(os.Getenv); err != nil || c.S3Endpoint != "http://127.0.0.1:9000" || !c.S3UsePathStyle {
		t.Errorf("endpoint override: cfg=%v err=%v", c, err)
	}

	t.Setenv("S3_USE_PATH_STYLE", "sometimes")
	if _, err := Load(os.Getenv); err == nil {
		t.Error("malformed S3_USE_PATH_STYLE accepted, want error")
	}
	t.Setenv("S3_USE_PATH_STYLE", "")

	for _, bad := range []string{
		"127.0.0.1:9000",
		"ftp://s3.example.internal",
		"https://access-key:secret@s3.example.internal",
		"https://s3.example.internal/?X-Amz-Signature=x",
	} {
		t.Setenv("S3_ENDPOINT", bad)
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("S3_ENDPOINT=%q accepted, want error", bad)
		}
	}
}

func TestMaxBuildTimeDefault(t *testing.T) {
	setRequired(t)
	c, err := Load(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxBuildTime != 2*time.Hour {
		t.Errorf("default MaxBuildTime=%s, want 2h", c.MaxBuildTime)
	}
}

func TestFeedAbsentPolls(t *testing.T) {
	setRequired(t)
	if c, err := Load(os.Getenv); err != nil || c.FeedAbsentPolls != 10 {
		t.Fatalf("default FeedAbsentPolls: cfg=%v err=%v", c, err)
	}
	t.Setenv("FEED_ABSENT_POLLS", "3")
	if c, err := Load(os.Getenv); err != nil || c.FeedAbsentPolls != 3 {
		t.Errorf("FEED_ABSENT_POLLS=3: cfg=%v err=%v", c, err)
	}
	for _, bad := range []string{"0", "-1", "ten"} {
		t.Setenv("FEED_ABSENT_POLLS", bad)
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("FEED_ABSENT_POLLS=%q accepted", bad)
		}
	}
}

func TestMaxLogBytes(t *testing.T) {
	setRequired(t)
	if c, err := Load(os.Getenv); err != nil || c.MaxLogBytes != 50<<20 {
		t.Fatalf("default MaxLogBytes: cfg=%v err=%v", c, err)
	}
	t.Setenv("MAX_LOG_BYTES", "1048576")
	if c, err := Load(os.Getenv); err != nil || c.MaxLogBytes != 1<<20 {
		t.Errorf("MAX_LOG_BYTES=1048576: cfg=%v err=%v", c, err)
	}
	for _, bad := range []string{"0", "-5", "50MiB"} {
		t.Setenv("MAX_LOG_BYTES", bad)
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("MAX_LOG_BYTES=%q accepted", bad)
		}
	}
}

func TestArtifactSizeCaps(t *testing.T) {
	setRequired(t)
	c, err := Load(os.Getenv)
	if err != nil || c.MaxArtifactBytes != 100<<20 || c.MaxTotalArtifactBytes != 1<<30 {
		t.Fatalf("defaults: cfg=%v err=%v", c, err)
	}
	t.Setenv("MAX_ARTIFACT_BYTES", "10")
	t.Setenv("MAX_TOTAL_ARTIFACT_BYTES", "20")
	if c, err = Load(os.Getenv); err != nil || c.MaxArtifactBytes != 10 || c.MaxTotalArtifactBytes != 20 {
		t.Errorf("overrides: cfg=%v err=%v", c, err)
	}
	if c.MaxArtifacts != DefaultMaxArtifacts {
		t.Errorf("MaxArtifacts = %d, want the default", c.MaxArtifacts)
	}
	t.Setenv("MAX_ARTIFACTS", "0")
	if _, err := Load(os.Getenv); err == nil {
		t.Error("MAX_ARTIFACTS=0 accepted")
	}
	t.Setenv("MAX_ARTIFACTS", "")
	for _, key := range []string{"MAX_ARTIFACT_BYTES", "MAX_TOTAL_ARTIFACT_BYTES"} {
		t.Setenv(key, "0")
		if _, err := Load(os.Getenv); err == nil {
			t.Errorf("%s=0 accepted", key)
		}
		t.Setenv(key, "")
	}
}
