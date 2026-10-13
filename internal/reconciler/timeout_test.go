package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// The shim's 2h timeout starts with the ProwJob, not with our trigger. If the
// trigger is late (feed lag, reconciler outage), the deadline still counts from
// the ProwJob's start so the timeout result lands while the shim is polling.
func TestTimeoutCountsFromProwJobStart(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, []feedJob{}...)
	fj, feed := h.fj, h.feed
	fj.autoStart = false
	feed.set(feedJob{ID: testID, State: "pending", JenkinsJob: "example-job", StartTime: h.clock.now().Add(-time.Hour)})

	h.r.tick(ctx) // triggered an hour after the ProwJob started
	if n := fj.triggerCount(); n != 1 {
		t.Fatalf("triggered %d times, want 1", n)
	}
	h.clock.advance(h.r.cfg.MaxBuildTime - time.Hour - time.Minute)
	h.r.tick(ctx)
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Fatalf("result written before the deadline: %+v", res)
	}
	h.clock.advance(2 * time.Minute)
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, "timed out") {
		t.Errorf("results = %+v, want one timeout error", res)
	}
}

// A ProwJob start time in the future (clock skew, or a bogus feed) does not
// extend the deadline past MaxBuildTime from the trigger.
func TestFutureProwJobStartIgnored(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, []feedJob{}...)
	fj, feed := h.fj, h.feed
	fj.autoStart = false
	feed.set(feedJob{ID: testID, State: "pending", JenkinsJob: "example-job", StartTime: h.clock.now().Add(24 * time.Hour)})

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	if res := h.s3.resultsFor(testID); len(res) != 1 || !strings.Contains(res[0].Message, "timed out") {
		t.Errorf("results = %+v, want one timeout error", res)
	}
}

// A ProwJob already older than MaxBuildTime is not worth a Jenkins build: the
// shim is about to give up. It gets an error result and no trigger.
func TestStaleProwJobNotTriggered(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, []feedJob{}...)
	fj, feed := h.fj, h.feed
	feed.set(feedJob{ID: testID, State: "pending", JenkinsJob: "example-job", StartTime: h.clock.now().Add(-h.r.cfg.MaxBuildTime - time.Minute)})

	h.r.tick(ctx)
	h.r.tick(ctx)
	if n := fj.triggerCount(); n != 0 {
		t.Errorf("triggered %d builds for a stale ProwJob", n)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, "not triggered") {
		t.Errorf("results = %+v, want one error", res)
	}
}

// A stale ProwJob whose build already ran (found after a restart) still gets
// that build's real result.
func TestStaleProwJobWithRecoveredBuildReportsResult(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, []feedJob{}...)
	fj, feed := h.fj, h.feed
	fj.addBuild("example-job", testID, false, "SUCCESS")
	feed.set(feedJob{ID: testID, State: "pending", JenkinsJob: "example-job", StartTime: h.clock.now().Add(-3 * time.Hour)})
	recovered(t, h)

	h.r.tick(ctx)
	if res := h.s3.resultsFor(testID); len(res) != 1 || res[0].Status != contract.StatusSuccess {
		t.Errorf("results = %+v, want the recovered build's success", res)
	}
}
