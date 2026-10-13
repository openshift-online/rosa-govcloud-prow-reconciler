package prow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const feed = `{
  "items": [
    {
      "metadata": {"name": "uuid-active", "labels": {"executor": "govcloud-jenkins"},
        "annotations": {"govcloud.ci/jenkins-job": "example-job"}},
      "spec": {"job": "periodic-rosa-govcloud-bridge-test"},
      "status": {"state": "triggered", "startTime": "2026-01-01T00:00:00Z"}
    },
    {
      "metadata": {"name": "uuid-done", "labels": {"executor": "govcloud-jenkins"},
        "annotations": {"govcloud.ci/jenkins-job": "example-job"}},
      "spec": {"job": "periodic-rosa-govcloud-bridge-test"},
      "status": {"state": "success"}
    },
    {
      "metadata": {"name": "uuid-noannotation", "labels": {"executor": "govcloud-jenkins"}},
      "spec": {"job": "x"}, "status": {"state": "triggered"}
    },
    {
      "metadata": {"name": "uuid-otherexecutor", "labels": {"executor": "something-else"},
        "annotations": {"govcloud.ci/jenkins-job": "example-job"}},
      "spec": {"job": "x"}, "status": {"state": "triggered"}
    }
  ]
}`

func TestListFeedMatching(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(feed))
	}))
	defer srv.Close()

	f, err := New(srv.URL).ListFeed(context.Background(), "govcloud-jenkins")
	if err != nil {
		t.Fatalf("ListFeed: %v", err)
	}
	jobs := f.Jobs
	// Excludes the wrong-executor job and the missing-annotation job, but counts
	// them, so an empty feed can be told apart from one with no matching jobs.
	if len(jobs) != 2 || f.Items != 4 {
		t.Fatalf("want 2 matched jobs of 4 items, got %d of %d: %+v", len(jobs), f.Items, jobs)
	}

	var active *Job
	for i := range jobs {
		if jobs[i].ID == "uuid-active" {
			active = &jobs[i]
		}
	}
	if active == nil {
		t.Fatal("expected uuid-active in results")
	}
	if !active.Active() {
		t.Errorf("uuid-active should be Active(), state=%q", active.State)
	}
	if active.JenkinsJob != "example-job" {
		t.Errorf("JenkinsJob = %q", active.JenkinsJob)
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC); !active.StartTime.Equal(want) {
		t.Errorf("StartTime = %v, want %v", active.StartTime, want)
	}
}

func TestValidProwJobID(t *testing.T) {
	for id, want := range map[string]bool{
		"0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c":   true,
		"0B5E3C1A-7D2F-4E8B-9A6C-1F2D3E4A5B6C":   false, // not canonical
		"0b5e3c1a7d2f4e8b9a6c1f2d3e4a5b6c":       false,
		"0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c/":  false,
		"../0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6": false,
		"":                                       false,
	} {
		if got := ValidProwJobID(id); got != want {
			t.Errorf("ValidProwJobID(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestParseTime(t *testing.T) {
	for _, raw := range []string{"", "yesterday", "2026-01-01"} {
		if got := parseTime(raw); !got.IsZero() {
			t.Errorf("parseTime(%q) = %v, want zero", raw, got)
		}
	}
}
