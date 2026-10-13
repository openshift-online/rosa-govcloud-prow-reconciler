package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const feedBody = `{"items":[{"metadata":{"name":"0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c","labels":{"executor":"govcloud-jenkins"},` +
	`"annotations":{"govcloud.ci/jenkins-job":"example-job"}},"spec":{"job":"periodic-example"},"status":{"state":"pending"}}]}`

// fakeJenkins serves job info for the jobs in params (name -> declared
// parameters) and requires basic auth svc:api-token.
func fakeJenkins(t *testing.T, params map[string][]string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("conncheck sent %s %s; it must be read-only", r.Method, r.URL.Path)
		}
		if u, p, ok := r.BasicAuth(); !ok || u != "svc" || p != "api-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		job := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/job/"), "/api/json")
		names, ok := params[job]
		if !ok {
			http.NotFound(w, r)
			return
		}
		var defs []map[string]string
		for _, n := range names {
			defs = append(defs, map[string]string{"name": n})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"buildable": true,
			"property":  []map[string]any{{"parameterDefinitions": defs}},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fakeFeed(t *testing.T, status int) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("omit"); got != "pod_spec,decoration_config" {
			t.Errorf("omit = %q", got)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(feedBody))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRun(t *testing.T) {
	feed := fakeFeed(t, http.StatusOK)
	jenkinsSrv := fakeJenkins(t, map[string][]string{"example-job": {"PROW_JOB_ID"}, "no-param-job": {"OTHER"}})
	base := func() map[string]string {
		return map[string]string{
			"PROW_JOBS_URL":         feed.URL,
			"JENKINS_URL":           jenkinsSrv.URL,
			"JENKINS_USER":          "svc",
			"JENKINS_API_TOKEN":     "api-token",
			"JENKINS_JOB_ALLOWLIST": "example-job",
		}
	}
	for _, tc := range []struct {
		name   string
		env    func(map[string]string)
		ok     bool
		output []string
	}{
		{"success", nil, true, []string{
			"[prow] OK: feed reachable; 1 ProwJob(s), 1 matching, 1 active",
			"0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c",
			`[jenkins] OK "example-job": buildable=true params=[PROW_JOB_ID]`,
			"RESULT: PASS",
		}},
		{"prow only", func(e map[string]string) {
			for _, k := range []string{"JENKINS_URL", "JENKINS_USER", "JENKINS_API_TOKEN", "JENKINS_JOB_ALLOWLIST"} {
				delete(e, k)
			}
		}, true, []string{"[jenkins] SKIP: JENKINS_URL not set", "RESULT: PASS"}},
		{"auth failure", func(e map[string]string) { e["JENKINS_API_TOKEN"] = "wrong-token" }, false, []string{
			`[jenkins] FAIL "example-job": GET`, "401 Unauthorized", "RESULT: FAIL",
		}},
		{"allowlisted job missing", func(e map[string]string) { e["JENKINS_JOB_ALLOWLIST"] = "example-job,missing-job" }, false, []string{
			`[jenkins] OK "example-job"`, `[jenkins] FAIL "missing-job": GET`, "404 Not Found", "RESULT: FAIL",
		}},
		{"job without PROW_JOB_ID", func(e map[string]string) { e["JENKINS_JOB_ALLOWLIST"] = "no-param-job" }, false, []string{
			`[jenkins] FAIL "no-param-job": no PROW_JOB_ID parameter declared`, "RESULT: FAIL",
		}},
		{"jenkins config error", func(e map[string]string) { delete(e, "JENKINS_USER"); e["JENKINS_JOB_ALLOWLIST"] = "a,a" }, false, []string{
			"[jenkins] FAIL: invalid configuration: JENKINS_USER is required", `JENKINS_JOB_ALLOWLIST lists "a" more than once`,
		}},
		{"feed config error", func(e map[string]string) { e["PROW_FEED_TIMEOUT"] = "soon" }, false, []string{
			"[prow] FAIL: invalid configuration: PROW_FEED_TIMEOUT", `[jenkins] OK "example-job"`, "RESULT: FAIL",
		}},
		{"feed unavailable", func(e map[string]string) { e["PROW_JOBS_URL"] = fakeFeed(t, http.StatusBadGateway).URL }, false, []string{
			"[prow] FAIL: prow feed returned 502", "RESULT: FAIL",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := base()
			if tc.env != nil {
				tc.env(env)
			}
			var out bytes.Buffer
			err := run(context.Background(), nil, func(k string) string { return env[k] }, &out)
			if (err == nil) != tc.ok {
				t.Errorf("run = %v, want ok=%v", err, tc.ok)
			}
			for _, want := range tc.output {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output does not contain %q:\n%s", want, out.String())
				}
			}
			if strings.Contains(out.String(), "api-token") || strings.Contains(out.String(), "wrong-token") {
				t.Errorf("output contains the API token:\n%s", out.String())
			}
		})
	}
}

// Feed strings are untrusted: control characters must not reach the terminal.
func TestFeedStringsQuoted(t *testing.T) {
	body := strings.Replace(feedBody, `"example-job"`, `"bad\u001b[2Jjob"`, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	t.Cleanup(srv.Close)
	var out bytes.Buffer
	env := map[string]string{"PROW_JOBS_URL": srv.URL}
	_ = run(context.Background(), nil, func(k string) string { return env[k] }, &out)
	if strings.Contains(out.String(), "\x1b") || !strings.Contains(out.String(), `jenkinsJob="bad\x1b[2Jjob"`) {
		t.Errorf("feed string not quoted:\n%q", out.String())
	}
}

func TestRunRejectsArguments(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--jenkins"}, func(string) string { return "" }, &out); err == nil || out.Len() != 0 {
		t.Errorf("run with arguments = %v, output %q", err, out.String())
	}
}
