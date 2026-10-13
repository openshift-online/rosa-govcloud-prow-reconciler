package jenkins

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type seenRequest struct {
	method, path, query string
	auth                bool
}

func recordingServer(t *testing.T, status int) (*httptest.Server, func() []seenRequest) {
	var mu sync.Mutex
	var seen []seenRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, ok := r.BasicAuth()
		mu.Lock()
		seen = append(seen, seenRequest{r.Method, r.URL.Path, r.URL.RawQuery, ok})
		mu.Unlock()
		if status == http.StatusFound {
			http.Redirect(w, r, "/somewhere/", status)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []seenRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]seenRequest(nil), seen...)
	}
}

func TestStopBuild(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusFound) // Jenkins redirects to the build page
	c := New(srv.URL+"/ci", "svc", "api-token")
	if err := c.StopBuild(context.Background(), srv.URL+"/ci/job/example-job/7/"); err != nil {
		t.Fatalf("StopBuild: %v", err)
	}
	got := seen()
	if len(got) != 1 || got[0] != (seenRequest{"POST", "/ci/job/example-job/7/stop", "", true}) {
		t.Errorf("requests = %+v, want one authenticated POST .../7/stop (redirect not followed)", got)
	}
}

func TestCancelQueueItem(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusNoContent)
	c := New(srv.URL+"/ci", "svc", "api-token")
	if err := c.CancelQueueItem(context.Background(), srv.URL+"/ci/queue/item/42/"); err != nil {
		t.Fatalf("CancelQueueItem: %v", err)
	}
	got := seen()
	if len(got) != 1 || got[0] != (seenRequest{"POST", "/ci/queue/cancelItem", "id=42", true}) {
		t.Errorf("requests = %+v, want one authenticated POST /ci/queue/cancelItem?id=42", got)
	}
}

func TestStopAndCancelErrors(t *testing.T) {
	srv, _ := recordingServer(t, http.StatusForbidden)
	c := New(srv.URL, "svc", "api-token")
	ctx := context.Background()
	if err := c.StopBuild(ctx, srv.URL+"/job/example-job/7/"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("StopBuild on 403: err = %v", err)
	}
	if err := c.CancelQueueItem(ctx, srv.URL+"/queue/item/1/"); err == nil {
		t.Error("CancelQueueItem on 403 succeeded")
	}
}

func TestCancelQueueItemRejectsNonQueueURLs(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusNoContent)
	c := New(srv.URL+"/ci", "svc", "api-token")
	for _, q := range []string{
		srv.URL + "/ci/job/example-job/7/",
		srv.URL + "/ci/queue/item/",
		srv.URL + "/ci/queue/item/1x/",
		srv.URL + "/ci/queue/item/1/2/",
		srv.URL + "/other/queue/item/1/",
		srv.URL + "/ci/queue/item/99999999999999999999/",
	} {
		if err := c.CancelQueueItem(context.Background(), q); err == nil {
			t.Errorf("CancelQueueItem(%q) accepted", q)
		}
	}
	if n := len(seen()); n != 0 {
		t.Errorf("%d requests sent for invalid queue URLs", n)
	}
}

// Stop and cancel go through the credential-scoped request path: a foreign host
// in a build or queue URL is rebased onto BaseURL, never contacted.
func TestStopAndCancelNeverContactForeignHost(t *testing.T) {
	foreign, foreignHits := foreignServer(t)
	srv, seen := recordingServer(t, http.StatusOK)
	c := New(srv.URL, "svc", "api-token")
	ctx := context.Background()
	if err := c.StopBuild(ctx, foreign.URL+"/job/example-job/7/"); err != nil {
		t.Fatalf("StopBuild: %v", err)
	}
	if err := c.CancelQueueItem(ctx, foreign.URL+"/queue/item/3/"); err != nil {
		t.Fatalf("CancelQueueItem: %v", err)
	}
	if n := foreignHits.Load(); n != 0 {
		t.Errorf("foreign host contacted %d times", n)
	}
	if n := len(seen()); n != 2 {
		t.Errorf("Jenkins saw %d requests, want 2", n)
	}
}
