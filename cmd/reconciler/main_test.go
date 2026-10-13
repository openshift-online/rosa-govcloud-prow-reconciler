package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testID = "0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c"

func TestRunConfigErrors(t *testing.T) {
	var logs bytes.Buffer
	err := run(context.Background(), nil, func(k string) string {
		return map[string]string{"POLL_INTERVAL": "often", "JENKINS_API_TOKEN": "s3cr3t-token"}[k]
	}, &logs)
	if err == nil {
		t.Fatal("run with an invalid environment succeeded")
	}
	for _, want := range []string{"invalid configuration", "JENKINS_URL is required", "S3_BUCKET is required", "POLL_INTERVAL: want a Go duration"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if strings.Contains(err.Error()+logs.String(), "s3cr3t-token") {
		t.Errorf("secret echoed: %v\n%s", err, logs.String())
	}
}

func TestRunPrintsVersion(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), []string{"-version"}, func(string) string { return "" }, &out); err != nil || out.String() != "reconciler dev\n" {
		t.Errorf("run -version = %q, %v", out.String(), err)
	}
}

func TestRunRejectsArguments(t *testing.T) {
	if err := run(context.Background(), []string{"-v"}, func(string) string { return "" }, io.Discard); err == nil {
		t.Error("run with arguments succeeded")
	}
}

// fakeS3 accepts path-style PutObject requests and records the keys.
type fakeS3 struct {
	mu   sync.Mutex
	keys []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	if r.Method != http.MethodPut {
		http.Error(w, "write-only", http.StatusForbidden)
		return
	}
	f.mu.Lock()
	f.keys = append(f.keys, r.URL.Path)
	f.mu.Unlock()
	w.Header().Set("ETag", `"etag"`)
}

func (f *fakeS3) written() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.keys...)
}

// fakeJenkins runs one build, which has already finished with SUCCESS once it
// leaves the queue.
func fakeJenkins(t *testing.T) *httptest.Server {
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /queue/api/json", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"items":[]}`)
	})
	mux.HandleFunc("GET /job/example-job/api/json", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"builds":[]}`)
	})
	mux.HandleFunc("POST /job/example-job/buildWithParameters", func(w http.ResponseWriter, r *http.Request) {
		if u, _, ok := r.BasicAuth(); !ok || u != "svc" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.FormValue("PROW_JOB_ID") != testID {
			t.Errorf("PROW_JOB_ID = %q", r.FormValue("PROW_JOB_ID"))
		}
		w.Header().Set("Location", srv.URL+"/queue/item/1/")
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /queue/item/1/api/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"executable": map[string]any{"url": srv.URL + "/job/example-job/1/"}})
	})
	mux.HandleFunc("GET /job/example-job/1/api/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"number": 1, "building": false, "result": "SUCCESS", "duration": 1000, "artifacts": []any{}})
	})
	mux.HandleFunc("GET /job/example-job/1/consoleText", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "Finished: SUCCESS\n")
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// End to end through the real clients: a ProwJob in the feed is triggered in
// Jenkins, its log and result.json land in S3 (log first), and the process
// shuts down cleanly when its context is cancelled.
func TestRunEndToEnd(t *testing.T) {
	for _, k := range []string{"AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_REGION", "AWS_DEFAULT_REGION"} {
		t.Setenv(k, "")
	}
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", dir+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "EXAMPLEACCESSKEYID00")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "example-secret-access-key")

	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"items":[{"metadata":{"name":%q,"labels":{"executor":"govcloud-jenkins"},`+
			`"annotations":{"govcloud.ci/jenkins-job":"example-job"}},"spec":{"job":"periodic-example"},"status":{"state":"pending"}}]}`, testID)
	}))
	defer feed.Close()
	jenkinsSrv := fakeJenkins(t)
	s3 := &fakeS3{}
	s3Srv := httptest.NewServer(s3)
	defer s3Srv.Close()

	env := map[string]string{
		"PROW_JOBS_URL":         feed.URL,
		"POLL_INTERVAL":         "1s",
		"JENKINS_URL":           jenkinsSrv.URL,
		"JENKINS_USER":          "svc",
		"JENKINS_API_TOKEN":     "api-token",
		"JENKINS_JOB_ALLOWLIST": "example-job",
		"S3_BUCKET":             "example-bucket",
		"S3_ENDPOINT":           s3Srv.URL,
		"S3_USE_PATH_STYLE":     "true",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	done := make(chan error, 1)
	go func() { done <- run(ctx, nil, func(k string) string { return env[k] }, &logs) }()

	want := []string{
		"/example-bucket/" + testID + "/artifacts/build-log.txt",
		"/example-bucket/" + testID + "/result.json",
	}
	deadline := time.Now().Add(15 * time.Second)
	for len(s3.written()) < len(want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run = %v, want a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if got := s3.written(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("S3 writes = %v, want %v\nlogs:\n%s", got, want, logs.String())
	}
	if strings.Contains(logs.String(), "api-token") || strings.Contains(logs.String(), "example-secret-access-key") {
		t.Errorf("logs contain a credential:\n%s", logs.String())
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
