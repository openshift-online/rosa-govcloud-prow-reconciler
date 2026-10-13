package reconciler

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

func wantOneError(t *testing.T, h *harness, substr string) {
	t.Helper()
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, substr) {
		t.Errorf("results = %+v, want one error containing %q", res, substr)
	}
}

// A build still running at the deadline is stopped in Jenkins, not left to run.
func TestTimeoutStopsRunningBuild(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj

	h.r.tick(ctx) // triggered and running
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx) // times out: stop requested, result written
	h.r.tick(ctx) // stop confirmed

	if got := fj.stopCalls(); !slices.Equal(got, []string{"example-job/1"}) {
		t.Errorf("stop calls = %v, want [example-job/1]", got)
	}
	if fj.building("example-job", 1) {
		t.Error("build still running after timeout")
	}
	wantOneError(t, h, "timed out")
	if res := h.s3.resultsFor(testID); len(res) == 1 && res[0].JenkinsBuild != fj.srv.URL+"/job/example-job/1/" {
		t.Errorf("jenkins_build = %q", res[0].JenkinsBuild)
	}
	if len(h.r.stopping) != 0 {
		t.Errorf("stopping = %v, want empty once Jenkins confirms", h.r.stopping)
	}
}

// A run still waiting in the queue at the deadline has its queue item cancelled.
func TestTimeoutCancelsQueuedItem(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.autoStart = false

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	h.r.tick(ctx)

	if got := fj.cancelCalls(); !slices.Equal(got, []int{1}) {
		t.Errorf("cancel calls = %v, want [1]", got)
	}
	if got := fj.stopCalls(); len(got) != 0 {
		t.Errorf("stop calls = %v, want none", got)
	}
	wantOneError(t, h, "timed out")
	if len(h.r.stopping) != 0 {
		t.Errorf("stopping = %v, want empty", h.r.stopping)
	}
}

// The queue item can start a build just as it is cancelled. The next attempt
// sees the build and stops it.
func TestCancelRaceStopsTheStartedBuild(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.autoStart = false
	fj.startOnCancel = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	for i := 0; i < 3; i++ {
		h.r.tick(ctx)
	}
	if got := fj.stopCalls(); !slices.Equal(got, []string{"example-job/1"}) {
		t.Errorf("stop calls = %v, want the build that started during cancel", got)
	}
	if fj.building("example-job", 1) {
		t.Error("build still running")
	}
	wantOneError(t, h, "timed out")
}

// A build that ignores the stop request is retried for a bounded number of ticks;
// result.json is still written exactly once, right away.
func TestStopRetriedThenAbandoned(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.ignoreStop = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	for i := 0; i < maxStopAttempts+5; i++ {
		h.r.tick(ctx)
	}
	if n := len(fj.stopCalls()); n != maxStopAttempts {
		t.Errorf("stop calls = %d, want %d", n, maxStopAttempts)
	}
	if len(h.r.stopping) != 0 {
		t.Errorf("stopping = %v, want empty after giving up", h.r.stopping)
	}
	wantOneError(t, h, "timed out")
	if n := fj.triggerCount(); n != 1 {
		t.Errorf("triggered %d times, want 1", n)
	}
}

// Abandoning a queued run whose queue item Jenkins has already expired (404)
// still finds the running build by PROW_JOB_ID and stops it.
func TestAbandonStopsBuildOfExpiredQueueItem(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.autoStart = false

	h.r.tick(ctx) // queued
	fj.set(func(fj *fakeJenkins) { fj.queue = map[int]*fjQueueItem{} })
	fj.addBuild("example-job", testID, true, "")
	h.r.abandon(ctx, testID, h.r.inflight[testID], "timed out")

	if got := fj.stopCalls(); !slices.Equal(got, []string{"example-job/1"}) {
		t.Errorf("stop calls = %v, want the found build stopped", got)
	}
}

// A queue item Jenkins has expired with no run left for the PROW_JOB_ID has
// nothing to stop, so no stop is kept retrying.
func TestAbandonVanishedQueueItemWithNoRun(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.set(func(fj *fakeJenkins) { fj.autoStart = false })

	h.r.tick(ctx) // queued
	h.fj.set(func(fj *fakeJenkins) { fj.queue = map[int]*fjQueueItem{} })
	h.r.abandon(ctx, testID, h.r.inflight[testID], "timed out")

	if len(h.r.stopping) != 0 {
		t.Errorf("stopping = %v, want empty: there is no run to stop", h.r.stopping)
	}
}

// While S3 keeps failing after a timeout, the run is stopped once (bounded by
// maxStopAttempts) and the console log is downloaded once. The result write is
// retried until the ProwJob is no longer active, then given up.
func TestAbandonWithS3DownStopsAndFetchesOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj, feed := h.fj, h.feed
	fj.ignoreStop = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.s3.setFailPuts(true)
	for range 3 * maxStopAttempts {
		h.r.tick(ctx)
	}
	if n := len(fj.stopCalls()); n != maxStopAttempts {
		t.Errorf("stop calls = %d, want %d", n, maxStopAttempts)
	}
	if fj.consoleReads != 1 {
		t.Errorf("console log downloaded %d times, want 1", fj.consoleReads)
	}
	if len(h.r.stopping) != 0 {
		t.Errorf("stopping=%v, want empty after the stop attempts ran out", h.r.stopping)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Fatal("gave up on the result while the ProwJob is still active")
	}
	feed.set(feedJob{ID: testID, State: "aborted", JenkinsJob: "example-job"})
	h.r.tick(ctx)
	h.r.tick(ctx)
	if len(h.r.inflight) != 0 {
		t.Errorf("inflight=%v, want given up once the ProwJob is inactive", h.r.inflight)
	}
}

// If the timeout result cannot be written the job stays in flight; the retry
// writes it once and does not trigger again.
func TestTimeoutResultRetriedAfterS3Failure(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.ignoreStop = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.s3.setFailPuts(true)
	h.r.tick(ctx)
	if _, ok := h.r.inflight[testID]; !ok {
		t.Fatal("job dropped after a failed result write")
	}
	h.s3.setFailPuts(false)
	h.r.tick(ctx)
	h.r.tick(ctx)
	wantOneError(t, h, "timed out")
	if n := h.fj.triggerCount(); n != 1 {
		t.Errorf("triggered %d times, want 1", n)
	}
}

// A timeout's error result is the shim's only answer, so it keeps being retried
// through a longer S3 outage, and stops once the ProwJob is no longer active.
func TestTimeoutResultRetriedThroughLongS3Outage(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.ignoreStop = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.s3.setFailPuts(true)
	for i := 0; i < 5; i++ {
		h.r.tick(ctx)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Fatal("gave up on the timeout result during the S3 outage")
	}
	h.s3.setFailPuts(false)
	h.r.tick(ctx)
	wantOneError(t, h, "timed out")
}
