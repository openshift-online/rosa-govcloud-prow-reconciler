package jenkins

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// JSON bodies are read to EOF before close, so the keep-alive connection is
// reused instead of one new connection per poll.
func TestJSONResponsesReuseConnection(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.(http.Flusher).Flush() // chunked: EOF only arrives after the decoder has stopped reading
		_, _ = w.Write([]byte(`{"number":1}`))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("\n"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	c := New(srv.URL, "svc", "api-token")
	for i := 0; i < 3; i++ {
		if _, err := c.GetBuild(context.Background(), srv.URL+"/job/example-job/1/"); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("%d connections for 3 requests, want 1", n)
	}
}
