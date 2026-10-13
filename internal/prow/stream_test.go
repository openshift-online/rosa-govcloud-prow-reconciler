package prow

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// A final Read that returns data and io.EOF together must still be rejected when
// that data is over the cap.
func TestCapReaderRejectsOverCapDataWithEOF(t *testing.T) {
	r := &capReader{r: iotest.DataErrReader(strings.NewReader("012345")), left: 5}
	if _, err := io.ReadAll(r); !errors.Is(err, ErrFeedTooLarge) {
		t.Errorf("err = %v, want ErrFeedTooLarge", err)
	}
}

// The largest cap reads everything instead of overflowing.
func TestCapReaderMaxInt64(t *testing.T) {
	r := &capReader{r: strings.NewReader("012345"), left: math.MaxInt64}
	if b, err := io.ReadAll(r); err != nil || string(b) != "012345" {
		t.Errorf("got %q, %v; want all data", b, err)
	}
}

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func matchingItem(name, state string) string {
	return fmt.Sprintf(`{"metadata":{"name":%q,"labels":{"executor":"govcloud-jenkins"},"annotations":{"govcloud.ci/jenkins-job":"example-job"}},"spec":{"job":"periodic-example"},"status":{"state":%q}}`, name, state)
}

// A large feed of mostly unrelated ProwJobs is decoded as a stream: only the
// matching ones are kept, and every item is counted.
func TestListFeedStreamsLargeFeed(t *testing.T) {
	const n = 20000
	pad := strings.Repeat("x", 512)
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"kind":"ProwJobList","metadata":{"resourceVersion":"1"},"items":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			if i%5000 == 0 {
				fmt.Fprint(w, matchingItem(fmt.Sprintf("match-%d", i), "pending"))
				continue
			}
			fmt.Fprintf(w, `{"metadata":{"name":"other-%d","labels":{"executor":"other"},"annotations":{"a":%q}},"spec":{"job":"x","pod_spec":{"containers":[{"args":[%q]}]}},"status":{"state":"success"}}`, i, pad, pad)
		}
		fmt.Fprint(w, `]}`)
	})
	f, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins")
	if err != nil {
		t.Fatal(err)
	}
	if f.Items != n || len(f.Jobs) != 4 {
		t.Fatalf("Items=%d Jobs=%d, want %d and 4", f.Items, len(f.Jobs), n)
	}
	for i, j := range f.Jobs {
		if want := fmt.Sprintf("match-%d", i*5000); j.ID != want || j.JenkinsJob != "example-job" {
			t.Errorf("job %d = %+v, want %s", i, j, want)
		}
	}
}

// Deck's omit parameter is sent so the pod spec and decoration config are left
// out; labels and annotations, which matching needs, are kept.
func TestListFeedSendsOmit(t *testing.T) {
	var got string
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.RawQuery
		fmt.Fprint(w, `{"items":[]}`)
	})
	c := New(srv.URL)
	if _, err := c.ListFeed(context.Background(), "govcloud-jenkins"); err != nil {
		t.Fatal(err)
	}
	if got != "omit=pod_spec%2Cdecoration_config" {
		t.Errorf("query = %q, want omit=pod_spec,decoration_config", got)
	}
	c.Omit = nil
	if _, err := c.ListFeed(context.Background(), "govcloud-jenkins"); err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("query = %q with no omit list, want none", got)
	}
}

// Anything but a complete feed object is a failed poll, never a (partial or
// empty) result: the reconciler relies on that to detect ProwJobs that are gone.
func TestListFeedRejectsIncompleteOrWrongContent(t *testing.T) {
	full := `{"items":[` + matchingItem("a", "pending") + `,` + matchingItem("b", "pending") + `]}`
	for name, body := range map[string]string{
		"truncated mid-item":     full[:len(full)/2],
		"truncated after item":   `{"items":[` + matchingItem("a", "pending") + `,`,
		"truncated before close": `{"items":[` + matchingItem("a", "pending") + `]`,
		"empty body":             ``,
		"html":                   `<html><body>Service Unavailable</body></html>`,
		"javascript":             `var allBuilds = {"items":[]};`,
		"top-level array":        `[` + matchingItem("a", "pending") + `]`,
		"no items":               `{"kind":"ProwJobList"}`,
		"items not an array":     `{"items":{"a":1}}`,
		"duplicate items":        `{"items":[],"items":[]}`,
		"trailing data":          full + `{}`,
		"bad item":               `{"items":[{"metadata":"nope"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
			f, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins")
			if err == nil {
				t.Errorf("accepted: %+v", f)
			}
		})
	}
}

func TestListFeedAcceptsNullItemsAndOtherFields(t *testing.T) {
	for body, wantItems := range map[string]int{
		`{"items":null}`: 0,
		`{"apiVersion":"v1","items":[` + matchingItem("a", "pending") + `],"metadata":{"x":[1,{"y":[]}]}}`: 1,
	} {
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
		f, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins")
		if err != nil || f.Items != wantItems {
			t.Errorf("%s: feed=%+v err=%v, want %d items", body, f, err, wantItems)
		}
	}
}

func TestListFeedNon200(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"items":[]}`, http.StatusServiceUnavailable)
	})
	if _, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins"); err == nil || !strings.Contains(err.Error(), "503") {
		t.Errorf("err = %v, want a 503 failure", err)
	}
}

func gzipped(s string) []byte {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, _ = io.WriteString(zw, s)
	_ = zw.Close()
	return b.Bytes()
}

// A gzip-encoded response is decompressed transparently; a truncated gzip
// stream is a failed poll.
func TestListFeedGzip(t *testing.T) {
	body := gzipped(`{"items":[` + matchingItem("a", "pending") + `]}`)
	for name, tc := range map[string]struct {
		data []byte
		ok   bool
	}{
		"complete":  {body, true},
		"truncated": {body[:len(body)-10], false},
	} {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
					t.Errorf("request did not accept gzip")
				}
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(tc.data)
			})
			f, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins")
			if tc.ok && (err != nil || len(f.Jobs) != 1) {
				t.Errorf("feed=%+v err=%v, want one job", f, err)
			}
			if !tc.ok && err == nil {
				t.Errorf("truncated gzip accepted: %+v", f)
			}
		})
	}
}

// A feed that stalls is cut off by the per-request timeout.
func TestListFeedTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[`+matchingItem("a", "pending"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	c := New(srv.URL)
	c.Timeout = 100 * time.Millisecond
	start := time.Now()
	_, err := c.ListFeed(context.Background(), "govcloud-jenkins")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want a deadline error", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %s", d)
	}
}

// A response larger than MaxBytes is a failed poll.
func TestListFeedSizeCap(t *testing.T) {
	body := `{"items":[` + matchingItem("a", "pending") + `]}`
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
	c := New(srv.URL)
	c.MaxBytes = int64(len(body))
	if _, err := c.ListFeed(context.Background(), "govcloud-jenkins"); err != nil {
		t.Errorf("feed of exactly MaxBytes rejected: %v", err)
	}
	c.MaxBytes = int64(len(body)) - 1
	if _, err := c.ListFeed(context.Background(), "govcloud-jenkins"); !errors.Is(err, ErrFeedTooLarge) {
		t.Errorf("err = %v, want ErrFeedTooLarge", err)
	}
}
