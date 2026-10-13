package jenkins

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const testAPIToken = "s3cr3t-api-token-value"

// Parameters travel in the POST body, never the URL, and the trigger carries
// basic auth.
func TestTriggerBuildParamsInBodyWithBasicAuth(t *testing.T) {
	var gotQuery, gotParam, gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		if r.PostForm.Has("token") {
			t.Errorf("trigger sent a job token")
		}
		gotParam = r.PostForm.Get("PROW_JOB_ID")
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Location", "http://"+r.Host+"/queue/item/1/")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := New(srv.URL, "svc", testAPIToken)
	if _, err := c.TriggerBuild(context.Background(), "example-job", map[string]string{"PROW_JOB_ID": "id"}); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	if gotQuery != "" {
		t.Errorf("trigger URL has a query string %q; params must be in the body", gotQuery)
	}
	if gotParam != "id" {
		t.Errorf("form PROW_JOB_ID = %q, want id", gotParam)
	}
	if gotUser != "svc" || gotPass != testAPIToken {
		t.Errorf("basic auth = %q:%q, want the configured credentials", gotUser, gotPass)
	}
}

// Without basic-auth credentials nothing is sent: the enhancement's trigger
// path is basic auth, and an unauthenticated POST would only be rejected.
func TestTriggerBuildRequiresBasicAuth(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	for _, creds := range [][2]string{{"", ""}, {"svc", ""}, {"", testAPIToken}} {
		c := New(srv.URL, creds[0], creds[1])
		if _, err := c.TriggerBuild(context.Background(), "example-job", nil); err == nil {
			t.Errorf("TriggerBuild with user=%q token set=%t succeeded", creds[0], creds[1] != "")
		}
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("%d requests sent without credentials", n)
	}
}

// A transport failure must not surface the request URL's query or userinfo.
func TestTransportErrorRedactsURL(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	base := srv.URL
	srv.Close() // connection refused from here on

	c := New(base, "svc", testAPIToken)
	_, err := c.GetBuild(context.Background(), base+"/job/example-job/1/?token=secret-query")
	if err == nil {
		t.Fatal("expected an error from a closed server")
	}
	if strings.Contains(err.Error(), "secret-query") || strings.Contains(err.Error(), testAPIToken) {
		t.Fatalf("error leaks a secret: %v", err)
	}
}

func TestRedactURL(t *testing.T) {
	got := redactURL("https://user:pw@jenkins.example.internal/job/x/build?token=abc#frag")
	want := "https://jenkins.example.internal/job/x/build"
	if got != want {
		t.Errorf("redactURL = %q, want %q", got, want)
	}
}
