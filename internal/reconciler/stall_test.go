package reconciler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

// The first Jenkins timeout ends a discover pass: one slow trigger is not
// followed by a second timeout per remaining job.
func TestDiscoverStopsAtJenkinsTimeout(t *testing.T) {
	h := newHarness(t, nil,
		feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"},
		feedJob{ID: otherID, State: "pending", JenkinsJob: "example-job"})
	var calls atomic.Int32
	h.fj.set(func(fj *fakeJenkins) {
		fj.beforeTrigger = func() { calls.Add(1); time.Sleep(300 * time.Millisecond) }
	})
	h.r.jenkins.APITimeout = 50 * time.Millisecond

	h.r.tick(context.Background())
	if n := calls.Load(); n != 1 {
		t.Errorf("trigger attempts in one pass = %d, want 1", n)
	}
}

// Likewise an advance pass stops at the first timeout.
func TestAdvanceStopsAtJenkinsTimeout(t *testing.T) {
	h := newHarness(t, nil,
		feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"},
		feedJob{ID: otherID, State: "pending", JenkinsJob: "example-job"})
	h.r.tick(context.Background()) // both triggered and running

	var calls atomic.Int32
	h.fj.set(func(fj *fakeJenkins) {
		fj.beforeBuild = func() { calls.Add(1); time.Sleep(300 * time.Millisecond) }
	})
	h.r.jenkins.APITimeout = 50 * time.Millisecond
	h.r.advance(context.Background())
	if n := calls.Load(); n != 1 {
		t.Errorf("build polls in one pass = %d, want 1", n)
	}
}

// And so does the stop pass.
func TestStopsPassStopsAtJenkinsTimeout(t *testing.T) {
	h := newHarness(t, nil, []feedJob{}...)
	fj := h.fj
	for _, id := range []string{testID, otherID} {
		n := fj.addBuild("example-job", id, true, "")
		h.r.stopping[id] = &stopTask{tracked: &tracked{buildURL: fmt.Sprintf("%s/job/example-job/%d/", fj.srv.URL, n)}, reason: "test"}
	}

	var calls atomic.Int32
	fj.set(func(fj *fakeJenkins) {
		fj.beforeBuild = func() { calls.Add(1); time.Sleep(300 * time.Millisecond) }
	})
	h.r.jenkins.APITimeout = 50 * time.Millisecond
	h.r.advanceStops(context.Background())
	if n := calls.Load(); n != 1 {
		t.Errorf("Jenkins calls in one stop pass = %d, want 1", n)
	}
}

// After a tick that overran POLL_INTERVAL the next feed poll waits a full
// interval rather than starting at once.
func TestNoImmediatePollAfterOverrun(t *testing.T) {
	const interval = 100 * time.Millisecond
	var mu sync.Mutex
	var lastEnd time.Time
	var gaps []time.Duration
	feedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		if !lastEnd.IsZero() {
			gaps = append(gaps, time.Since(lastEnd))
		}
		mu.Unlock()
		time.Sleep(2 * interval) // every tick overruns
		_, _ = w.Write([]byte(`{"items":[]}`))
		mu.Lock()
		lastEnd = time.Now()
		mu.Unlock()
	}))
	defer feedSrv.Close()
	h := newHarness(t, nil)
	h.r.prow = prow.New(feedSrv.URL)
	h.r.cfg.PollInterval = interval

	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	_ = h.r.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(gaps) < 2 {
		t.Fatalf("only %d polls", len(gaps)+1)
	}
	for _, g := range gaps {
		if g < interval/2 {
			t.Errorf("next poll started %s after the previous finished, want about %s", g, interval)
		}
	}
}

// A build whose polls always time out must still hit its deadline.
func TestStalledBuildStillTimesOut(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.r.tick(ctx) // triggered and running

	h.fj.set(func(fj *fakeJenkins) { fj.beforeBuild = func() { time.Sleep(300 * time.Millisecond) } })
	h.r.jenkins.APITimeout = 50 * time.Millisecond
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.advance(ctx)
	if !slices.ContainsFunc(h.s3.resultsFor(testID), func(r contract.Result) bool { return strings.Contains(r.Message, "timed out") }) {
		t.Errorf("results = %+v, want a timed out error", h.s3.resultsFor(testID))
	}
}
