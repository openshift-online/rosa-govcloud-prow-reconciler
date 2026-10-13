package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

const (
	testID     = "0d4c3b2a-1f0e-4d9c-8b7a-6e5f4d3c2b1a"
	testBucket = "example-bucket"
	testAKID   = "EXAMPLEACCESSKEYID00"
	testSecret = "example/secret/access/key/do-not-leak"
)

// gotRequest is one request seen by the fake S3 server.
type gotRequest struct {
	Method string
	Path   string
	Query  string
	Header http.Header
	Body   []byte
}

// fakeS3Server is an httptest server standing in for an S3 endpoint with
// path-style addressing. handle, if set, decides the response; the default is
// 200 OK like a successful PutObject.
type fakeS3Server struct {
	mu     sync.Mutex
	reqs   []gotRequest
	handle func(n int, w http.ResponseWriter, r *http.Request) bool // true = handled
	srv    *httptest.Server
}

func newFakeS3Server(t *testing.T) *fakeS3Server {
	f := &fakeS3Server{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, gotRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone(), Body: body})
		n := len(f.reqs)
		h := f.handle
		f.mu.Unlock()
		if h != nil && h(n, w, r) {
			return
		}
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeS3Server) requests() []gotRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]gotRequest(nil), f.reqs...)
}

// isolateEnv keeps the developer's AWS environment and shared config out of tests.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
		"AWS_USE_FIPS_ENDPOINT", "AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_ROLE_ARN",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_EC2_METADATA_DISABLED",
	} {
		t.Setenv(k, "")
	}
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", dir+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/credentials")
}

// testLoadOpts injects static credentials and a zero-delay standard retryer.
func testLoadOpts(extra ...func(*awsconfig.LoadOptions) error) []func(*awsconfig.LoadOptions) error {
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: testAKID, SecretAccessKey: testSecret, Source: "test"}, nil
	})
	return append([]func(*awsconfig.LoadOptions) error{
		awsconfig.WithCredentialsProvider(creds),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		}),
	}, extra...)
}

func newTestWriter(t *testing.T, f *fakeS3Server) *SDKWriter {
	t.Helper()
	isolateEnv(t)
	w, err := newWriter(context.Background(), Options{
		Bucket: testBucket, Region: "us-gov-west-1", Endpoint: f.srv.URL, UsePathStyle: true,
	}, testLoadOpts())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestPutResultKeyHeadersAndBody(t *testing.T) {
	f := newFakeS3Server(t)
	w := newTestWriter(t, f)
	want := contract.Result{Status: contract.StatusSuccess, Duration: "15.7s", Artifacts: []string{"junit_01.xml"}, Message: "ok"}
	if err := w.PutResult(context.Background(), testID, want); err != nil {
		t.Fatal(err)
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPut || r.Path != "/"+testBucket+"/"+testID+"/result.json" {
		t.Errorf("got %s %s, want PUT /%s/%s/result.json", r.Method, r.Path, testBucket, testID)
	}
	if !strings.Contains(r.Query, "x-id=PutObject") {
		t.Errorf("query %q is not a PutObject", r.Query)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential="+testAKID+"/") ||
		!strings.Contains(auth, "/us-gov-west-1/s3/aws4_request") {
		t.Errorf("request not SigV4-signed for us-gov-west-1: %q", auth)
	}
	for k := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-server-side-encryption") {
			t.Errorf("unexpected SSE header %s: bucket default encryption should apply", k)
		}
	}
	var got contract.Result
	if err := json.Unmarshal(r.Body, &got); err != nil {
		t.Fatalf("body is not result JSON: %v: %q", err, r.Body)
	}
	if got.Status != want.Status || got.Duration != want.Duration || got.Message != want.Message ||
		len(got.Artifacts) != 1 || got.Artifacts[0] != "junit_01.xml" {
		t.Errorf("body = %+v, want %+v", got, want)
	}
}

func TestPutArtifactKeysAndContentTypes(t *testing.T) {
	f := newFakeS3Server(t)
	w := newTestWriter(t, f)
	cases := map[string]string{
		"junit_01.xml":  "application/xml",
		"build-log.txt": "text/plain; charset=utf-8",
		"console.LOG":   "text/plain; charset=utf-8",
		"summary.json":  "application/json",
		"bundle.tar.gz": "application/octet-stream",
		"noext":         "application/octet-stream",
	}
	for name := range cases {
		if err := w.PutArtifact(context.Background(), testID, name, []byte("data:"+name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	reqs := f.requests()
	if len(reqs) != len(cases) {
		t.Fatalf("got %d requests, want %d", len(reqs), len(cases))
	}
	for _, r := range reqs {
		name := strings.TrimPrefix(r.Path, "/"+testBucket+"/"+testID+"/artifacts/")
		wantCT, ok := cases[name]
		if !ok || r.Method != http.MethodPut {
			t.Errorf("unexpected request %s %s", r.Method, r.Path)
			continue
		}
		if ct := r.Header.Get("Content-Type"); ct != wantCT {
			t.Errorf("%s: Content-Type = %q, want %q", name, ct, wantCT)
		}
		if string(r.Body) != "data:"+name {
			t.Errorf("%s: body = %q", name, r.Body)
		}
	}
}

func TestPutRejectsUnsafeKeys(t *testing.T) {
	f := newFakeS3Server(t)
	w := newTestWriter(t, f)
	ctx := context.Background()
	for _, id := range []string{"", "../other", testID + "/x", "not-a-uuid"} {
		if err := w.PutResult(ctx, id, contract.Result{}); err == nil {
			t.Errorf("PutResult(%q) accepted", id)
		}
		if err := w.PutArtifact(ctx, id, "a.txt", nil); err == nil {
			t.Errorf("PutArtifact(id=%q) accepted", id)
		}
	}
	for _, name := range []string{
		"", "../x", strings.Repeat("a", 1024-len(testID+"/artifacts/")+1), // full table: contract
	} {
		if err := w.PutArtifact(ctx, testID, name, nil); err == nil {
			t.Errorf("PutArtifact(name=%q) accepted", name)
		}
	}
	if n := len(f.requests()); n != 0 {
		t.Errorf("%d requests sent for invalid keys", n)
	}
}

// Nested relative paths are kept: the shim's recursive copy recreates them.
func TestPutArtifactNestedPaths(t *testing.T) {
	f := newFakeS3Server(t)
	w := newTestWriter(t, f)
	longest := strings.Repeat("a", 1024-len(testID+"/artifacts/"))
	for _, name := range []string{"reports/junit_01.xml", "a/b/c/d.log", "with space/ü.txt", "x..y/z", longest} {
		if err := w.PutArtifact(context.Background(), testID, name, []byte("x")); err != nil {
			t.Errorf("PutArtifact(%q): %v", name, err)
		}
	}
	reqs := f.requests()
	if len(reqs) != 5 {
		t.Fatalf("got %d requests, want 5", len(reqs))
	}
	if want := "/" + testBucket + "/" + testID + "/artifacts/reports/junit_01.xml"; reqs[0].Path != want {
		t.Errorf("path = %q, want %q", reqs[0].Path, want)
	}
	if want := "/" + testBucket + "/" + testID + "/artifacts/with space/ü.txt"; reqs[2].Path != want {
		t.Errorf("path = %q, want %q", reqs[2].Path, want)
	}
}

func TestPutRetriesTransientErrors(t *testing.T) {
	f := newFakeS3Server(t)
	f.handle = func(n int, w http.ResponseWriter, _ *http.Request) bool {
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`)
			return true
		}
		return false
	}
	w := newTestWriter(t, f)
	if err := w.PutArtifact(context.Background(), testID, "build-log.txt", []byte("log")); err != nil {
		t.Fatalf("want success after retry, got %v", err)
	}
	reqs := f.requests()
	if len(reqs) != 2 {
		t.Fatalf("got %d attempts, want 2", len(reqs))
	}
	if string(reqs[1].Body) != "log" {
		t.Errorf("retried body = %q, want full payload", reqs[1].Body)
	}
}

func TestPutErrorIsSanitized(t *testing.T) {
	f := newFakeS3Server(t)
	f.handle = func(_ int, w http.ResponseWriter, _ *http.Request) bool {
		w.Header().Set("x-amz-request-id", "REQ123")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>REQ123</RequestId></Error>`)
		return true
	}
	w := newTestWriter(t, f)
	err := w.PutResult(context.Background(), testID, contract.Result{Status: contract.StatusError})
	if err == nil {
		t.Fatal("want error on 403")
	}
	msg := err.Error()
	for _, want := range []string{"s3://" + testBucket + "/" + testID + "/result.json", "AccessDenied", "403", "REQ123"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q missing %q", msg, want)
		}
	}
	t.Logf("error: %s", msg)
	assertNoSecrets(t, msg, f.srv.URL)
	if n := len(f.requests()); n != 1 {
		t.Errorf("403 retried: %d attempts", n)
	}
}

func TestPutTransportErrorIsSanitized(t *testing.T) {
	f := newFakeS3Server(t)
	f.handle = func(_ int, w http.ResponseWriter, _ *http.Request) bool {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close() // drop the connection mid-request
		}
		return true
	}
	w := newTestWriter(t, f)
	err := w.PutArtifact(context.Background(), testID, "build-log.txt", []byte("log"))
	if err == nil {
		t.Fatal("want transport error")
	}
	t.Logf("error: %s", err)
	assertNoSecrets(t, err.Error(), f.srv.URL)
	if n := len(f.requests()); n < 2 {
		t.Errorf("transport error not retried: %d attempts", n)
	}
}

func TestPutHonorsContextCancellation(t *testing.T) {
	f := newFakeS3Server(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.handle = func(_ int, _ http.ResponseWriter, r *http.Request) bool {
		select {
		case <-r.Context().Done():
		case <-release:
		}
		return true
	}
	w := newTestWriter(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := w.PutResult(ctx, testID, contract.Result{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("cancellation took %s", time.Since(start))
	}
	t.Logf("error: %s", err)
	assertNoSecrets(t, err.Error(), f.srv.URL)
}

// TestEndpointResolution checks virtual-hosted and FIPS endpoints for GovCloud
// without any network: a stub HTTP client captures the outgoing request.
func TestEndpointResolution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     Options
		wantHost string
		wantPath string
	}{
		{"default is fips", Options{Bucket: testBucket, Region: "us-gov-west-1"},
			testBucket + ".s3-fips.us-gov-west-1.amazonaws.com", "/" + testID + "/result.json"},
		{"fips path-style", Options{Bucket: testBucket, Region: "us-gov-west-1", UsePathStyle: true},
			"s3-fips.us-gov-west-1.amazonaws.com", "/" + testBucket + "/" + testID + "/result.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateEnv(t)
			var got *http.Request
			stub := stubDoer(func(r *http.Request) (*http.Response, error) {
				got = r
				if r.Body != nil {
					_, _ = io.Copy(io.Discard, r.Body) // the SDK computes a trailing checksum as the body is read
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})
			w, err := newWriter(context.Background(), tc.opts, testLoadOpts(awsconfig.WithHTTPClient(stub)))
			if err != nil {
				t.Fatal(err)
			}
			if err := w.PutResult(context.Background(), testID, contract.Result{}); err != nil {
				t.Fatal(err)
			}
			if got.URL.Scheme != "https" || got.URL.Host != tc.wantHost || got.URL.Path != tc.wantPath {
				t.Errorf("request to %s, want https://%s%s", got.URL, tc.wantHost, tc.wantPath)
			}
		})
	}
}

type stubDoer func(*http.Request) (*http.Response, error)

func (s stubDoer) Do(r *http.Request) (*http.Response, error) { return s(r) }

func TestNewRequiresBucketAndRegion(t *testing.T) {
	isolateEnv(t)
	if _, err := New(context.Background(), Options{Region: "us-gov-west-1"}); err == nil {
		t.Error("missing bucket accepted")
	}
	if _, err := New(context.Background(), Options{Bucket: testBucket}); err == nil {
		t.Error("missing region accepted")
	}
}

func assertNoSecrets(t *testing.T, msg, endpoint string) {
	t.Helper()
	for _, bad := range []string{testAKID, testSecret, "Authorization", "AWS4-HMAC-SHA256", "X-Amz-", endpoint, "x-id="} {
		if strings.Contains(msg, bad) {
			t.Errorf("error %q leaks %q", msg, bad)
		}
	}
}

// Compile-time check that the SDK writer satisfies the reconciler's seam.
var _ Writer = (*SDKWriter)(nil)

func TestPutTimeoutScalesWithSize(t *testing.T) {
	if got := putTimeout(0); got != 2*time.Minute {
		t.Errorf("putTimeout(0) = %s", got)
	}
	if got := putTimeout(100 << 20); got != 2*time.Minute+100*time.Second {
		t.Errorf("putTimeout(100MiB) = %s", got)
	}
}
