package reconciler

import (
	"context"
	"slices"
	"testing"
)

// otherID is a second ProwJob that keeps the feed non-empty.
const otherID = "11111111-2222-4333-8444-555555555555"

func otherJob() feedJob {
	return feedJob{ID: otherID, State: "success", JenkinsJob: "example-job"}
}

// An aborted ProwJob (or one that failed because the shim gave up) has nobody
// waiting for its result: the Jenkins run is stopped and an error result written.
func TestTerminalProwJobStopsRun(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		queued      bool
		wantStops   []string
		wantCancels []int
	}{
		{"aborted while running", "aborted", false, []string{"example-job/1"}, nil},
		{"aborted while queued", "aborted", true, nil, []int{1}},
		{"failed (shim timed out)", "failure", false, []string{"example-job/1"}, nil},
		{"error", "error", false, []string{"example-job/1"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, nil)
			fj, feed := h.fj, h.feed
			fj.autoStart = !tc.queued

			h.r.tick(ctx)
			feed.set(feedJob{ID: testID, State: tc.state, JenkinsJob: "example-job"})
			h.r.tick(ctx)
			h.r.tick(ctx)

			if got := fj.stopCalls(); !slices.Equal(got, tc.wantStops) {
				t.Errorf("stop calls = %v, want %v", got, tc.wantStops)
			}
			if got := fj.cancelCalls(); !slices.Equal(got, tc.wantCancels) {
				t.Errorf("cancel calls = %v, want %v", got, tc.wantCancels)
			}
			wantOneError(t, h, "ProwJob is "+tc.state)
			if n := fj.triggerCount(); n != 1 {
				t.Errorf("triggered %d times, want 1", n)
			}
			if len(h.r.inflight) != 0 || len(h.r.stopping) != 0 {
				t.Errorf("inflight=%v stopping=%v, want both empty", h.r.inflight, h.r.stopping)
			}
		})
	}
}

// If the build finished by the time the abort is seen, its real result is
// written and nothing is stopped.
func TestTerminalProwJobWithFinishedBuildReportsResult(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj, feed := h.fj, h.feed

	h.r.tick(ctx)
	fj.finish("example-job", 1, "SUCCESS")
	feed.set(feedJob{ID: testID, State: "aborted", JenkinsJob: "example-job"})
	h.r.tick(ctx)
	if got := fj.stopCalls(); len(got) != 0 {
		t.Errorf("stop calls = %v, want none", got)
	}
	if res := h.s3.resultsFor(testID); len(res) != 1 || res[0].Status != "success" {
		t.Errorf("results = %+v, want the build's success", res)
	}
}

// A ProwJob that disappears from the feed is only given up on after
// FeedAbsentPolls consecutive successful polls without it; reappearing resets
// the count.
func TestDisappearedProwJobStopsRunAfterConsecutivePolls(t *testing.T) {
	ctx := context.Background()
	active := feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"}
	h := newHarness(t, nil, active, otherJob())
	fj, feed := h.fj, h.feed
	n := h.r.cfg.FeedAbsentPolls

	h.r.tick(ctx)
	feed.set(otherJob())
	for i := 0; i < n-1; i++ {
		h.r.tick(ctx)
	}
	feed.set(active, otherJob()) // back again: count resets
	h.r.tick(ctx)
	feed.set(otherJob())
	for i := 0; i < n-1; i++ {
		h.r.tick(ctx)
	}
	if got := fj.stopCalls(); len(got) != 0 || len(h.s3.resultsFor(testID)) != 0 {
		t.Fatalf("acted before %d consecutive absent polls: stops=%v results=%v", n, got, h.s3.resultsFor(testID))
	}
	h.r.tick(ctx) // n-th consecutive absence
	h.r.tick(ctx)
	if got := fj.stopCalls(); !slices.Equal(got, []string{"example-job/1"}) {
		t.Errorf("stop calls = %v, want [example-job/1]", got)
	}
	wantOneError(t, h, "missing from the Prow feed")
}

// An empty feed in the middle breaks the run of absences: "consecutive" means
// consecutive successful polls that positively lacked the ProwJob.
func TestEmptyFeedBreaksAbsenceStreak(t *testing.T) {
	ctx := context.Background()
	active := feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"}
	h := newHarness(t, nil, active, otherJob())
	fj, feed := h.fj, h.feed
	n := h.r.cfg.FeedAbsentPolls

	h.r.tick(ctx)
	feed.set(otherJob())
	for i := 0; i < n-1; i++ {
		h.r.tick(ctx)
	}
	feed.set() // empty
	h.r.tick(ctx)
	feed.set(otherJob())
	h.r.tick(ctx) // would be the n-th absence if the empty poll had not broken the streak
	h.r.tick(ctx)
	if got := fj.stopCalls(); len(got) != 0 || len(h.s3.resultsFor(testID)) != 0 {
		t.Errorf("acted despite an empty feed in the streak: stops=%v results=%v", got, h.s3.resultsFor(testID))
	}
}

// Failed, truncated, or empty feed polls are not evidence that a ProwJob is gone:
// however many there are, nothing is stopped and nothing is written.
func TestUnusableFeedNeverStopsRuns(t *testing.T) {
	for _, mode := range []string{"fail", "truncated", "cut", "empty"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, nil, feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"}, otherJob())
			fj, feed := h.fj, h.feed

			h.r.tick(ctx)
			feed.setMode(mode)
			for i := 0; i < 3*h.r.cfg.FeedAbsentPolls; i++ {
				h.r.tick(ctx)
			}
			if got, c := fj.stopCalls(), fj.cancelCalls(); len(got) != 0 || len(c) != 0 {
				t.Errorf("stops=%v cancels=%v during %s feed polls", got, c, mode)
			}
			if res := h.s3.resultsFor(testID); len(res) != 0 {
				t.Errorf("results = %+v, want none", res)
			}
			tr, ok := h.r.inflight[testID]
			if !ok || tr.absentPolls != 0 || tr.abandon != "" {
				t.Errorf("tracked = %+v, want still in flight with no absence counted", tr)
			}

			// Once the feed is usable again the run proceeds normally.
			feed.setMode("")
			fj.finish("example-job", 1, "SUCCESS")
			h.r.tick(ctx)
			if res := h.s3.resultsFor(testID); len(res) != 1 || res[0].Status != "success" {
				t.Errorf("results = %+v, want the build's success once the feed recovers", res)
			}
		})
	}
}

// After a restart, a run that is still building for a ProwJob that has already
// finished in Prow is an orphan and is stopped.
func TestRecoveredOrphanIsStopped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "aborted", JenkinsJob: "example-job"}, otherJob())
	fj := h.fj
	n := fj.addBuild("example-job", testID, true, "")
	fj.addBuild("example-job", otherID, false, "SUCCESS")
	recovered(t, h)

	h.r.tick(ctx)
	h.r.tick(ctx)
	if got := fj.stopCalls(); !slices.Equal(got, []string{"example-job/1"}) {
		t.Errorf("stop calls = %v, want only the orphaned build", got)
	}
	if fj.building("example-job", n) {
		t.Error("orphaned build still running")
	}
	if len(h.s3.opsLog()) != 0 {
		t.Errorf("writes = %v, want none for a finished ProwJob", h.s3.opsLog())
	}
}
