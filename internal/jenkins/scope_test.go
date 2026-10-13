package jenkins

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// foreignServer counts requests and fails the test if any carries credentials.
func foreignServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("credentials sent to foreign host: %s %s", r.Method, r.URL)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// Jenkins hands back queue/build URLs on a foreign host. The client must rebase
// them onto BaseURL (path preserved, under a /jenkins prefix) and never contact
// the foreign host.
func TestReturnedURLsAreRebasedOntoBaseURL(t *testing.T) {
	foreign, foreignHits := foreignServer(t)

	var sawAuthOnFollowups atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/jenkins/job/example-job/buildWithParameters", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", foreign.URL+"/jenkins/queue/item/7/")
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/jenkins/queue/item/7/api/json", func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); ok {
			sawAuthOnFollowups.Add(1)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"executable": map[string]any{"url": foreign.URL + "/jenkins/job/example-job/3/"}})
	})
	mux.HandleFunc("/jenkins/job/example-job/3/api/json", func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); ok {
			sawAuthOnFollowups.Add(1)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"number": 3, "building": false, "result": "SUCCESS"})
	})
	jenkinsSrv := httptest.NewServer(mux)
	defer jenkinsSrv.Close()

	c := New(jenkinsSrv.URL+"/jenkins/", "svc", "api-token")
	ctx := context.Background()
	q, err := c.TriggerBuild(ctx, "example-job", map[string]string{"PROW_JOB_ID": "id"})
	if err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	if want := jenkinsSrv.URL + "/jenkins/queue/item/7/"; q != want {
		t.Errorf("queue URL = %q, want %q", q, want)
	}
	b, err := c.ResolveQueueItem(ctx, q)
	if err != nil {
		t.Fatalf("ResolveQueueItem: %v", err)
	}
	if want := jenkinsSrv.URL + "/jenkins/job/example-job/3/"; b != want {
		t.Errorf("build URL = %q, want %q", b, want)
	}
	if _, err := c.GetBuild(ctx, b); err != nil {
		t.Fatalf("GetBuild: %v", err)
	}
	if n := foreignHits.Load(); n != 0 {
		t.Errorf("foreign host contacted %d times", n)
	}
	if sawAuthOnFollowups.Load() != 2 {
		t.Errorf("expected authenticated follow-ups against BaseURL")
	}
}

// Even if a caller passes a foreign URL directly, credentials only go to BaseURL.
func TestGetBuildForeignURLNeverSendsCredentialsAbroad(t *testing.T) {
	foreign, foreignHits := foreignServer(t)
	jenkinsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"number":1}`))
	}))
	defer jenkinsSrv.Close()

	c := New(jenkinsSrv.URL, "svc", "api-token")
	if _, err := c.GetBuild(context.Background(), foreign.URL+"/job/example-job/1/"); err != nil {
		t.Fatalf("GetBuild: %v", err)
	}
	if _, err := c.DownloadArtifact(context.Background(), foreign.URL+"/job/example-job/1/", "junit.xml", 1<<20); err != nil {
		t.Fatalf("DownloadArtifact: %v", err)
	}
	if n := foreignHits.Load(); n != 0 {
		t.Errorf("foreign host contacted %d times", n)
	}
}

func TestRebaseRejectsPathsOutsideRoot(t *testing.T) {
	c := New("https://jenkins.example.internal/jenkins", "", "")
	for _, raw := range []string{
		"https://other.example.internal/admin/",
		"https://jenkins.example.internal/jenkinsx/job/a/",
		"/queue/item/1/",
		"https://jenkins.example.internal/jenkins/../admin/",
		"https://jenkins.example.internal/jenkins/%2e%2e/admin/",
		"mailto:someone",
	} {
		if got, err := c.Rebase(raw); err == nil {
			t.Errorf("Rebase(%q) = %q, want error", raw, got)
		}
	}
	for raw, want := range map[string]string{
		"queue/item/1/": "https://jenkins.example.internal/jenkins/queue/item/1/",
		"http://jenkins-internal:8080/jenkins/job/a/2/":               "https://jenkins.example.internal/jenkins/job/a/2/",
		"https://user:pw@jenkins.example.internal/jenkins/job/a/2/#x": "https://jenkins.example.internal/jenkins/job/a/2/",
	} {
		got, err := c.Rebase(raw)
		if err != nil || got != want {
			t.Errorf("Rebase(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
}

// A redirect off the Jenkins origin is refused before any request reaches it.
func TestCrossOriginRedirectRefused(t *testing.T) {
	foreign, foreignHits := foreignServer(t)
	jenkinsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL+r.URL.Path, http.StatusFound)
	}))
	defer jenkinsSrv.Close()

	c := New(jenkinsSrv.URL, "svc", "api-token")
	if _, err := c.GetBuild(context.Background(), jenkinsSrv.URL+"/job/example-job/1/"); err == nil {
		t.Fatal("expected cross-origin redirect to be refused")
	}
	if n := foreignHits.Load(); n != 0 {
		t.Errorf("foreign host contacted %d times", n)
	}
}

func TestArtifactPathTraversalRejected(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if strings.Contains(r.URL.Path, "..") {
			t.Errorf("traversal reached server: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := New(srv.URL, "svc", "api-token")
	build := srv.URL + "/job/example-job/1/"
	for _, p := range []string{"../../../credentials.xml", "/etc/passwd", ""} {
		if _, err := c.DownloadArtifact(context.Background(), build, p, 1<<20); err == nil {
			t.Errorf("DownloadArtifact(%q) succeeded, want error", p)
		}
	}
	if hits.Load() != 0 {
		t.Errorf("server contacted for invalid paths")
	}
	if _, err := c.DownloadArtifact(context.Background(), build, "reports/junit 01.xml", 1<<20); err != nil {
		t.Errorf("valid nested path rejected: %v", err)
	}
}

// Each segment is escaped, so characters that mean something in a URL reach
// Jenkins as part of the file name.
func TestArtifactPathSegmentsAreEscaped(t *testing.T) {
	var gotPath, gotRaw, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotRaw, gotQuery = r.URL.Path, r.URL.EscapedPath(), r.URL.RawQuery
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := New(srv.URL, "svc", "api-token")
	name := "reports/a b#c?d=e%41;f/ü.xml"
	if _, err := c.DownloadArtifact(context.Background(), srv.URL+"/job/example-job/1/", name, 1<<20); err != nil {
		t.Fatal(err)
	}
	if want := "/job/example-job/1/artifact/" + name; gotPath != want {
		t.Errorf("decoded path = %q, want %q", gotPath, want)
	}
	if gotQuery != "" || !strings.Contains(gotRaw, "a%20b%23c%3Fd=e%2541%3Bf/%C3%BC.xml") {
		t.Errorf("escaped path = %q, query = %q", gotRaw, gotQuery)
	}
}
