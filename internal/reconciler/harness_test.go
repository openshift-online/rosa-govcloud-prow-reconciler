package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

// fakeS3 records every write; it is the s3.Writer seam used by all loop tests.
type fakeS3 struct {
	mu             sync.Mutex
	results        map[string][]contract.Result // every PutResult, in order, per id
	artifacts      map[string]map[string][]byte
	ops            []string // successful writes in order: "artifact:{id}/{name}" or "result:{id}"
	failPuts       bool
	failResult     bool   // only PutResult fails
	artifactPuts   int    // successful PutArtifact calls
	resultAttempts int    // PutResult calls, successful or not
	failArtifact   string // PutArtifact of this name always fails
	// beforeArtifact, if set, runs (without the lock) at the start of every
	// PutArtifact, e.g. to block it.
	beforeArtifact func(ctx context.Context)
}

func newFakeS3() *fakeS3 {
	return &fakeS3{results: map[string][]contract.Result{}, artifacts: map[string]map[string][]byte{}}
}

func (f *fakeS3) PutResult(ctx context.Context, id string, r contract.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resultAttempts++
	if err := ctx.Err(); err != nil { // like the SDK, a done context fails the write
		return err
	}
	if f.failPuts || f.failResult {
		return fmt.Errorf("injected S3 failure")
	}
	f.results[id] = append(f.results[id], r)
	f.ops = append(f.ops, "result:"+id)
	return nil
}

func (f *fakeS3) PutArtifact(ctx context.Context, id, name string, data []byte) error {
	f.mu.Lock()
	hook := f.beforeArtifact
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.failPuts || name == f.failArtifact {
		return fmt.Errorf("injected S3 failure")
	}
	if f.artifacts[id] == nil {
		f.artifacts[id] = map[string][]byte{}
	}
	f.artifacts[id][name] = data
	f.artifactPuts++
	f.ops = append(f.ops, "artifact:"+id+"/"+name)
	return nil
}

func (f *fakeS3) artifact(id, name string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.artifacts[id][name]
	return string(d), ok
}

func (f *fakeS3) setFailPuts(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPuts = v
}

func (f *fakeS3) opsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

func (f *fakeS3) resultsFor(id string) []contract.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]contract.Result(nil), f.results[id]...)
}

// feedJob is one ProwJob served by fakeFeed.
type feedJob struct {
	ID          string
	State       string
	JenkinsJob  string
	ParamsJSON  string    // govcloud.ci/jenkins-params annotation, which must never reach Jenkins; "" = absent
	ExecutorOff bool      // true = wrong executor label
	StartTime   time.Time // status.startTime; zero = absent
}

// fakeFeed serves a mutable prowjobs.js.
type fakeFeed struct {
	mu   sync.Mutex
	jobs []feedJob
	// mode: "" = serve jobs, "fail" = 500, "truncated" = cut-off JSON, "empty" = no
	// items, "cut" = every job but the first, with the closing "]}" missing (a
	// truncation that would look like the first job vanished if it were accepted).
	mode string
	srv  *httptest.Server
}

func newFakeFeed(t *testing.T, jobs ...feedJob) *fakeFeed {
	f := &fakeFeed{jobs: jobs}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch f.mode {
		case "fail":
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		case "empty":
			_, _ = w.Write([]byte(`{"items":[]}`))
			return
		}
		type item struct {
			Metadata struct {
				Name        string            `json:"name"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
			Spec struct {
				Job string `json:"job"`
			} `json:"spec"`
			Status struct {
				State     string `json:"state"`
				StartTime string `json:"startTime,omitempty"`
			} `json:"status"`
		}
		var out struct {
			Items []item `json:"items"`
		}
		for _, j := range f.jobs {
			var it item
			it.Metadata.Name = j.ID
			it.Metadata.Labels = map[string]string{"executor": "govcloud-jenkins"}
			if j.ExecutorOff {
				it.Metadata.Labels["executor"] = "other"
			}
			it.Metadata.Annotations = map[string]string{prow.AnnotationJenkinsJob: j.JenkinsJob}
			if j.ParamsJSON != "" {
				it.Metadata.Annotations["govcloud.ci/jenkins-params"] = j.ParamsJSON
			}
			it.Spec.Job = "periodic-example"
			it.Status.State = j.State
			if !j.StartTime.IsZero() {
				it.Status.StartTime = j.StartTime.Format(time.RFC3339)
			}
			out.Items = append(out.Items, it)
		}
		switch f.mode {
		case "truncated":
			b, _ := json.Marshal(out)
			_, _ = w.Write(b[:len(b)/2])
			return
		case "cut":
			out.Items = out.Items[1:]
			b, _ := json.Marshal(out)
			_, _ = w.Write(b[:len(b)-2])
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeFeed) setMode(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
}

func (f *fakeFeed) set(jobs ...feedJob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = jobs
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// fakeClock is an injectable, manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type harness struct {
	fj    *fakeJenkins
	feed  *fakeFeed
	r     *Reconciler
	s3    *fakeS3
	logs  *syncBuffer
	clock *fakeClock
}

func testConfig(feedURL, jenkinsURL string) *config.Config {
	return &config.Config{
		Feed: config.Feed{
			ProwJobsURL:   feedURL,
			ExecutorLabel: "govcloud-jenkins",
		},
		Jenkins: config.Jenkins{
			JenkinsURL:   jenkinsURL,
			JobAllowlist: []string{"example-job"},
		},
		PollInterval:          time.Second,
		MaxBuildTime:          config.DefaultMaxBuildTime,
		FeedAbsentPolls:       config.DefaultFeedAbsentPolls,
		MaxLogBytes:           config.DefaultMaxLogBytes,
		MaxArtifactBytes:      config.DefaultMaxArtifactBytes,
		MaxTotalArtifactBytes: config.DefaultMaxTotalArtifactBytes,
		MaxArtifacts:          config.DefaultMaxArtifacts,
		S3Bucket:              "example-bucket",
		ShutdownTimeout:       config.DefaultShutdownTimeout,
	}
}

// newHarness wires a Reconciler to a new fake Jenkins and a feed serving jobs
// (default: one pending testID job on example-job; pass []feedJob{}... for none).
// mutate, if set, tweaks the config before construction.
func newHarness(t *testing.T, mutate func(*config.Config), jobs ...feedJob) *harness {
	t.Helper()
	if jobs == nil {
		jobs = []feedJob{{ID: testID, State: "pending", JenkinsJob: "example-job"}}
	}
	fj, feed := newFakeJenkins(t), newFakeFeed(t, jobs...)
	feedURL, jc := feed.srv.URL, fj.client()
	cfg := testConfig(feedURL, jc.BaseURL)
	if mutate != nil {
		mutate(cfg)
	}
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newFakeS3()
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	r := New(cfg, prow.New(feedURL), jc, w, log)
	r.now = clock.now
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("reconciler logs:\n%s", logs.String())
		}
	})
	return &harness{fj: fj, feed: feed, r: r, s3: w, logs: logs, clock: clock}
}
