package reconciler

import (
	"context"
	"strings"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// A 4xx on trigger (other than 401/403/408/429) ends the run at once with only the
// code in the result; 5xx, 408 and 429 stay uncertain and are retried after a
// Jenkins check. A 401/403 queued nothing, so it is not uncertain.
func TestTriggerStatusClasses(t *testing.T) {
	for _, tc := range []struct {
		code     int
		terminal bool
	}{{400, true}, {404, true}, {408, false}, {429, false}, {500, false}, {503, false}} {
		h := newHarness(t, nil)
		fj := h.fj
		fj.set(func(fj *fakeJenkins) { fj.triggerStatus = tc.code })
		h.r.tick(context.Background())

		res := h.s3.resultsFor(testID)
		if !tc.terminal {
			if f := h.r.failedTriggers[testID]; len(res) != 0 || f == nil || !f.unsure {
				t.Errorf("%d: results=%+v failedTrigger=%+v, want no result and unsure", tc.code, res, f)
			}
			continue
		}
		if len(res) != 1 || res[0].Status != contract.StatusError ||
			!strings.HasPrefix(res[0].Message, "Jenkins rejected the trigger with HTTP ") ||
			strings.Contains(res[0].Message, "injected") {
			t.Errorf("%d: results = %+v, want one error with the code only", tc.code, res)
		}
	}
}

// A terminal 4xx whose error result cannot be written is not re-triggered while
// the write is retried.
func TestTerminalTriggerNotRepostedWhileResultFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	posts := 0
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus = 400; fj.beforeTrigger = func() { posts++ } })
	h.s3.setFailPuts(true)
	h.r.tick(ctx)
	h.r.tick(ctx)
	h.s3.setFailPuts(false)
	h.r.tick(ctx)
	if posts != 1 {
		t.Errorf("trigger POSTs = %d, want 1", posts)
	}
	wantOneError(t, h, "HTTP 400")
}

// A 401/403 may be a token being rotated: the trigger is retried on later ticks,
// and only the third consecutive rejection fails the ProwJob.
func TestTriggerAuthRetries(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus = 403 })
	h.r.tick(ctx)
	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus = 0 })
	h.r.tick(ctx)
	if n := fj.triggerCount(); n != 1 || len(h.s3.resultsFor(testID)) != 0 {
		t.Errorf("403 twice then ok: triggers=%d results=%+v, want one trigger and no result", n, h.s3.resultsFor(testID))
	}

	h = newHarness(t, nil)
	fj = h.fj
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus = 403 })
	for range 3 {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Message != "Jenkins rejected the trigger with HTTP 403 after 3 attempts" {
		t.Errorf("403 three times: results = %+v, want one error after 3 attempts", res)
	}
}

// A bad token is refused on reads too; the third 401 must still fail the ProwJob.
func TestTriggerAuthFailsWhenReadsRefusedToo(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.triggerStatus, fj.readStatus = 401, 401 })
	for range maxAuthFails {
		h.r.tick(ctx)
	}
	wantOneError(t, h, "HTTP 401 after 3 attempts")
	if f := h.r.failedTriggers[testID]; f != nil && f.unsure {
		t.Errorf("failedTrigger = %+v, want not unsure: a 401 queued nothing", f)
	}
}

// Jenkins forgets finished queue items (404). The build is found by PROW_JOB_ID;
// with none, the run fails after maxQueueMisses lookups instead of waiting for
// the deadline.
func TestQueueItemVanished(t *testing.T) {
	for _, withBuild := range []bool{true, false} {
		ctx := context.Background()
		h := newHarness(t, nil)
		fj := h.fj
		fj.set(func(fj *fakeJenkins) { fj.autoStart = withBuild })
		h.r.tick(ctx)
		fj.set(func(fj *fakeJenkins) { fj.queue = map[int]*fjQueueItem{} }) // every queue URL now 404s

		if withBuild {
			fj.finish("example-job", 1, "SUCCESS")
			h.r.tick(ctx)
			res := h.s3.resultsFor(testID)
			if len(res) != 1 || res[0].Status != contract.StatusSuccess {
				t.Errorf("with build: results = %+v, want one success", res)
			}
			continue
		}
		for i := 1; i <= maxQueueMisses; i++ {
			if n := len(h.s3.resultsFor(testID)); n != 0 {
				t.Fatalf("result after %d misses, want none before %d", i-1, maxQueueMisses)
			}
			h.r.tick(ctx)
		}
		res := h.s3.resultsFor(testID)
		if len(res) != 1 || res[0].Status != contract.StatusError || !strings.Contains(res[0].Message, "queue item disappeared") {
			t.Errorf("no build: results = %+v, want one error", res)
		}
	}
}

// Finding the run still queued under a new queue URL resets the miss count, so
// later misses start again from zero.
func TestQueueMissesResetOnNewQueueURL(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.autoStart = false })
	gone := func() { fj.set(func(fj *fakeJenkins) { fj.queue = map[int]*fjQueueItem{} }) }

	h.r.tick(ctx) // queue item 1
	gone()
	for range maxQueueMisses - 1 {
		h.r.tick(ctx)
	}
	fj.addQueued("example-job", testID) // the run reappears as queue item 2
	h.r.tick(ctx)
	if q := h.r.inflight[testID].queueURL; q != fj.srv.URL+"/queue/item/2/" {
		t.Fatalf("queueURL = %q, want queue item 2", q)
	}
	gone()
	for range maxQueueMisses - 1 {
		h.r.tick(ctx)
	}
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Errorf("results = %+v, want none: misses before the new URL must not count", res)
	}
}
