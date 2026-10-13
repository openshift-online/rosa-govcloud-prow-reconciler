package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
)

// Entries for ProwJobs that left the feed are pruned once absent for MaxBuildTime;
// otherwise recovered runs and failed triggers would pile up over weeks.
func TestRecoveredAndFailedTriggersPrunedWhenJobGone(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj, feed := h.fj, h.feed
	fj.addBuild("example-job", otherID, false, "SUCCESS")
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus = 500 })
	if err := h.r.recoverState(ctx); err != nil {
		t.Fatal(err)
	}
	h.r.tick(ctx)
	if h.r.failedTriggers[testID] == nil || len(h.r.recovered) != 1 {
		t.Fatalf("setup: failedTriggers=%v recovered=%v, want one each", h.r.failedTriggers, h.r.recovered)
	}

	feed.set()
	h.r.tick(ctx)
	if h.r.failedTriggers[testID] == nil || len(h.r.recovered) != 1 {
		t.Fatal("entries pruned before the grace period")
	}
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	if len(h.r.failedTriggers) != 0 || len(h.r.recovered) != 0 {
		t.Errorf("failedTriggers=%v recovered=%v, want both pruned", h.r.failedTriggers, h.r.recovered)
	}
}

// The 3-in-a-row 401/403 count starts over after a different error.
func TestAuthFailCountResetsAfterOtherError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	for _, status := range []int{401, 401, 500, 401, 401} {
		fj.set(func(fj *fakeJenkins) { fj.triggerStatus = status })
		h.r.tick(ctx)
	}
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Errorf("results = %+v; 401 401 500 401 401 must not count as 3 consecutive", res)
	}
}

// A retry subtracts what earlier attempts already uploaded from
// MAX_TOTAL_ARTIFACT_BYTES, so the total cannot be exceeded across attempts.
func TestRetryCountsSentArtifactsAgainstTotalBudget(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.MaxTotalArtifactBytes = 15 })
	fj := h.fj
	h.s3.failArtifact = "b"

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"a": "12345678", "b": "12345", "c": "123"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // a uploaded, b fails in S3, c not tried
	h.s3.failArtifact = ""
	h.r.tick(ctx) // b fits again (8 + 5), c does not (8 + 5 + 3 > 15)

	if _, ok := h.s3.artifact(testID, "c"); ok {
		t.Error("c uploaded on the retry; total budget exceeded across attempts")
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || !strings.Contains(res[0].Message, "2 bytes left") {
		t.Errorf("results = %+v, want c skipped for the 2 bytes left", res)
	}
}

// A context cut off by the shutdown timeout must not even attempt result.json:
// the outputs only failed because of the cut-off. (fakeS3 refuses writes on a
// done context, so count the attempts instead of looking at what was stored.)
func TestShutdownCutOffWritesNoResult(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []string{"finish", "abandon"} {
		t.Run(tc, func(t *testing.T) {
			h := newHarness(t, nil)
			h.r.tick(ctx)
			h.r.tick(ctx) // build started
			tr := h.r.inflight[testID]
			if tr == nil || tr.buildURL == "" {
				t.Fatalf("setup: tracked = %+v", tr)
			}
			cut, cancel := context.WithCancel(ctx)
			cancel()
			if tc == "finish" {
				tr.collectAttempts = maxCollectAttempts - 1 // a failed collection would now write an error result
				h.r.finish(cut, testID, tr, &jenkins.Build{Result: "SUCCESS"})
			} else {
				h.r.abandon(cut, testID, tr, "timed out")
			}
			if n := h.s3.resultAttempts; n != 0 || tr.result != nil {
				t.Errorf("result attempts = %d, held result = %+v, want none", n, tr.result)
			}
			if _, ok := h.r.inflight[testID]; !ok {
				t.Error("cut-off job dropped from tracking")
			}
		})
	}
}

// A done record's grace period counts from the last poll that listed the job,
// not from when it was resolved: a ProwJob still pending long after its result
// must not be re-triggered when it drops out of one poll and comes back.
func TestDoneRecordGraceCountsFromLastSeen(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.r.tick(ctx) // triggered, building
	h.fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // result written, done
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx) // still pending in Prow

	h.feed.set(feedJob{ID: otherID, State: "success", JenkinsJob: "example-job"})
	h.r.tick(ctx)
	h.feed.set(feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"})
	h.r.tick(ctx)

	if n := h.fj.triggerCount(); n != 1 {
		t.Errorf("triggered %d times, want 1", n)
	}
	if n := len(h.s3.resultsFor(testID)); n != 1 {
		t.Errorf("%d results written, want 1", n)
	}
}
