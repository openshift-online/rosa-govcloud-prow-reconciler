package jenkins

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadTail(t *testing.T) {
	input := []byte(strings.Repeat("0123456789", 10_000)) // 100,000 bytes
	for _, max := range []int64{1, 7, 10, 32 << 10, 99_999, 100_000, 1 << 20} {
		for name, r := range map[string]io.Reader{
			"whole":    bytes.NewReader(input),
			"one-byte": iotest.OneByteReader(bytes.NewReader(input)),
			"half":     iotest.HalfReader(bytes.NewReader(input)),
		} {
			got, total, err := readTail(r, max)
			if err != nil {
				t.Fatalf("max=%d %s: %v", max, name, err)
			}
			want := input
			if int64(len(want)) > max {
				want = want[int64(len(want))-max:]
			}
			if total != int64(len(input)) || !bytes.Equal(got, want) {
				t.Errorf("max=%d %s: total=%d len=%d, want total=%d len=%d (tail match %v)",
					max, name, total, len(got), len(input), len(want), bytes.Equal(got, want))
			}
		}
	}
}

func TestReadTailPropagatesErrors(t *testing.T) {
	r := io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(io.ErrUnexpectedEOF))
	if _, _, err := readTail(r, 4); err == nil {
		t.Error("read error swallowed")
	}
}

func TestConsoleText(t *testing.T) {
	var gotPath string
	var gotAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _, gotAuth = r.BasicAuth()
		_, _ = w.Write([]byte("line 1\nline 2\nline 3\n"))
	}))
	defer srv.Close()
	c := New(srv.URL, "svc", "api-token")
	tail, total, err := c.ConsoleText(context.Background(), srv.URL+"/job/example-job/4/", 7)
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/job/example-job/4/consoleText" || !gotAuth {
		t.Errorf("request path=%q auth=%v", gotPath, gotAuth)
	}
	if string(tail) != "line 3\n" || total != 21 {
		t.Errorf("tail=%q total=%d", tail, total)
	}
}
