package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// After result.json is written the ProwJob stays pending until the shim reads it.
// Those later ticks must not re-trigger Jenkins or overwrite the result.
func TestNoRetriggerAfterResultWhileProwJobStillActive(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)

	h.r.tick(ctx) // trigger + resolve queue -> running
	h.fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // collect + write result
	for i := 0; i < 3; i++ {
		h.r.tick(ctx) // ProwJob still pending in the feed
	}

	if n := h.fj.triggerCount(); n != 1 {
		t.Errorf("Jenkins triggered %d times, want 1", n)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess {
		t.Errorf("results = %+v, want exactly one success", res)
	}
}

func TestTimeoutDoesNotRetrigger(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.autoStart = false // stays in the queue forever

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx) // times out, writes error result
	h.r.tick(ctx)
	h.r.tick(ctx)

	if n := h.fj.triggerCount(); n != 1 {
		t.Errorf("Jenkins triggered %d times, want 1", n)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, "timed out") {
		t.Errorf("results = %+v, want exactly one timeout error", res)
	}
}

func TestRejectedJobWrittenOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "not-allowlisted"})

	for i := 0; i < 4; i++ {
		h.r.tick(ctx)
	}
	if n := len(h.s3.resultsFor(testID)); n != 1 {
		t.Errorf("rejection written %d times, want 1", n)
	}
	if n := strings.Count(h.logs.String(), "not in allowlist; rejecting"); n != 1 {
		t.Errorf("rejection logged %d times, want 1", n)
	}
}

// A failed S3 write is transient: the job stays unresolved and is retried, and
// only marked done once the write succeeds.
func TestRejectionRetriedAfterS3Failure(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "not-allowlisted"})

	h.s3.failPuts = true
	h.r.tick(ctx)
	h.s3.failPuts = false
	h.r.tick(ctx)
	h.r.tick(ctx)
	if n := len(h.s3.resultsFor(testID)); n != 1 {
		t.Errorf("rejection written %d times, want 1", n)
	}
}

func TestDoneRecordsArePruned(t *testing.T) {
	ctx := context.Background()
	other := "11111111-2222-4333-8444-555555555555"
	h := newHarness(t, nil,
		feedJob{ID: testID, State: "pending", JenkinsJob: "not-allowlisted"},
		feedJob{ID: other, State: "pending", JenkinsJob: "not-allowlisted"},
	)
	feed := h.feed
	h.r.tick(ctx)
	if len(h.r.done) != 2 {
		t.Fatalf("done = %v, want 2 records", h.r.done)
	}

	// testID completes in Prow; other drops out of the feed entirely.
	feed.set(feedJob{ID: testID, State: "failure", JenkinsJob: "not-allowlisted"})
	h.r.tick(ctx)
	if _, ok := h.r.done[testID]; ok {
		t.Error("done record kept for a ProwJob the feed reports as finished")
	}
	if _, ok := h.r.done[other]; !ok {
		t.Error("done record for a briefly absent ProwJob pruned before the grace period")
	}

	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	if len(h.r.done) != 0 {
		t.Errorf("done = %v, want empty after grace period", h.r.done)
	}
}

func TestInvalidIDLoggedOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: "not-a-uuid", State: "triggered", JenkinsJob: "example-job"})
	for i := 0; i < 3; i++ {
		h.r.tick(ctx)
	}
	if n := strings.Count(h.logs.String(), "not a canonical UUID"); n != 1 {
		t.Errorf("invalid ID logged %d times, want 1", n)
	}
}

// Enhancement state machine: Triggered --> Error on a cancelled queue item. The
// shim should get an error promptly, not wait for MAX_BUILD_TIME, and a failed
// write of it is retried.
func TestCancelledQueueItemWritesErrorOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.autoStart = false

	h.r.tick(ctx)
	fj.cancelQueued(1)
	h.s3.failResult = true
	h.r.tick(ctx)
	h.s3.failResult = false
	h.r.tick(ctx)
	h.r.tick(ctx)

	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, "cancelled") {
		t.Errorf("results = %+v, want one cancellation error", res)
	}
	if n := fj.triggerCount(); n != 1 {
		t.Errorf("Jenkins triggered %d times, want 1", n)
	}
}
