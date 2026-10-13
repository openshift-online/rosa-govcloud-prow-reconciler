package jenkins

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDownloadArtifactSizeLimit(t *testing.T) {
	body := strings.Repeat("x", 100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/chunked.bin") {
			w.(http.Flusher).Flush() // no Content-Length: the limit must hold while reading
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	c := New(srv.URL, "svc", "api-token")
	build := srv.URL + "/job/example-job/1/"
	ctx := context.Background()

	for _, name := range []string{"sized.bin", "chunked.bin"} {
		if data, err := c.DownloadArtifact(ctx, build, name, 100); err != nil || len(data) != 100 {
			t.Errorf("%s at exactly the limit: len=%d err=%v", name, len(data), err)
		}
		if _, err := c.DownloadArtifact(ctx, build, name, 99); !errors.Is(err, ErrArtifactTooLarge) {
			t.Errorf("%s over the limit: err=%v, want ErrArtifactTooLarge", name, err)
		}
		if data, err := c.DownloadArtifact(ctx, build, name, math.MaxInt64); err != nil || len(data) != 100 {
			t.Errorf("%s with the largest limit: len=%d err=%v", name, len(data), err)
		}
	}
	if _, err := c.DownloadArtifact(ctx, build, "sized.bin", 0); !errors.Is(err, ErrArtifactTooLarge) {
		t.Errorf("zero limit: err=%v", err)
	}
}
