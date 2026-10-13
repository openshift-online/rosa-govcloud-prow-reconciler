package jenkins

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A short global http.Client.Timeout would also cut off large artifact bodies;
// timeouts are per request instead.
func TestNoGlobalClientTimeout(t *testing.T) {
	c := New("https://jenkins.example.internal", "", "")
	if c.HTTP.Timeout != 0 {
		t.Errorf("http.Client.Timeout = %s, want 0 (per-request deadlines)", c.HTTP.Timeout)
	}
	if c.APITimeout != DefaultAPITimeout || c.DownloadTimeout != DefaultDownloadTimeout {
		t.Errorf("timeouts = %s/%s", c.APITimeout, c.DownloadTimeout)
	}
}

// slowBody writes n chunks with a delay between them.
func slowBody(n int, delay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < n; i++ {
			if _, err := w.Write([]byte("chunk\n")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
	}
}

// An artifact that takes longer than the API timeout to stream still downloads;
// an API call that stalls past the API timeout fails.
func TestDownloadsUseTheDownloadTimeout(t *testing.T) {
	srv := httptest.NewServer(slowBody(6, 50*time.Millisecond)) // ~300ms
	defer srv.Close()
	c := New(srv.URL, "svc", "api-token")
	c.APITimeout = 100 * time.Millisecond
	c.DownloadTimeout = 10 * time.Second

	data, err := c.DownloadArtifact(context.Background(), srv.URL+"/job/example-job/1/", "build.log", 1<<20)
	if err != nil {
		t.Fatalf("slow download failed under the API timeout: %v", err)
	}
	if strings.Count(string(data), "chunk") != 6 {
		t.Errorf("got %q", data)
	}

	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"number":`)) // valid so far, then nothing
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second): // so a missing timeout fails instead of hanging
		}
	}))
	defer stalled.Close()
	c = New(stalled.URL, "svc", "api-token")
	c.APITimeout = 100 * time.Millisecond
	start := time.Now()
	// The reconciler's pass-stopping check (noteTimeout) relies on a net.Error timeout.
	var ne net.Error
	if _, err := c.GetBuild(context.Background(), stalled.URL+"/job/example-job/1/"); !errors.As(err, &ne) || !ne.Timeout() {
		t.Errorf("stalled API read: err = %v, want a net.Error timeout", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("API call took %s, want about APITimeout", d)
	}
}

func TestDownloadTimeoutBoundsDownloads(t *testing.T) {
	srv := httptest.NewServer(slowBody(100, 50*time.Millisecond))
	defer srv.Close()
	c := New(srv.URL, "svc", "api-token")
	c.DownloadTimeout = 150 * time.Millisecond
	start := time.Now()
	if _, err := c.DownloadArtifact(context.Background(), srv.URL+"/job/example-job/1/", "build.log", 1<<20); err == nil {
		t.Error("download succeeded past DownloadTimeout")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("download took %s, want about DownloadTimeout", d)
	}
}
