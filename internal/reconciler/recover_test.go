package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

func recovered(t *testing.T, h *harness) {
	t.Helper()
	if err := h.r.recoverState(context.Background()); err != nil {
		t.Fatalf("recoverState: %v", err)
	}
}

// A build started before a restart is monitored, not re-triggered.
func TestRecoveryResumesRunningBuild(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	n := fj.addBuild("example-job", testID, true, "")
	recovered(t, h)

	h.r.tick(ctx)
	if c := fj.triggerCount(); c != 0 {
		t.Fatalf("re-triggered %d builds after restart", c)
	}
	fj.finish("example-job", n, "SUCCESS")
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess || res[0].JenkinsBuild != fj.srv.URL+"/job/example-job/1/" {
		t.Errorf("results = %+v", res)
	}
}

// A recovered run without a start time (Jenkins reported 0) is timed from
// adoption, not from 1970, so it is not timed out at once.
func TestRecoveredRunWithoutStartTime(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.addBuild("example-job", testID, true, "")
	h.fj.set(func(fj *fakeJenkins) { fj.builds["example-job"][0].timestamp = 0 })
	recovered(t, h)

	h.r.tick(ctx)
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Errorf("results = %+v, want none while the build runs", res)
	}
}

// The build finished while the reconciler was down, possibly long ago. Write that
// build's real result instead of re-triggering or timing out.
func TestRecoveryWritesResultForFinishedBuild(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.addBuild("example-job", "11111111-2222-4333-8444-555555555555", false, "SUCCESS") // other ProwJob
	fj.addBuild("example-job", testID, false, "FAILURE")
	h.clock.advance(3 * time.Hour) // past MaxBuildTime since the build started
	recovered(t, h)

	h.r.tick(ctx)
	h.r.tick(ctx)
	if c := fj.triggerCount(); c != 0 {
		t.Fatalf("re-triggered %d builds after restart", c)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusFailure {
		t.Errorf("results = %+v, want one failure from the existing build", res)
	}
}

func TestRecoveryAdoptsQueuedItem(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job"})
	fj := h.fj
	fj.addQueued("example-job", testID)
	recovered(t, h)

	h.r.tick(ctx)
	if c := fj.triggerCount(); c != 0 {
		t.Fatalf("re-triggered %d builds after restart", c)
	}
	tr, ok := h.r.inflight[testID]
	if !ok || tr.buildURL != "" || tr.queueURL != fj.srv.URL+"/queue/item/1/" {
		t.Errorf("tracked = %+v, want queued at item 1", tr)
	}
}

// Run must not trigger anything until Jenkins state has been read successfully.
func TestRunDoesNotTriggerUntilRecoverySucceeds(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 5 * time.Millisecond }, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job"})
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.failReads = true })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.r.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(100 * time.Millisecond)
	if c := fj.triggerCount(); c != 0 {
		t.Fatalf("triggered %d builds while Jenkins state was unreadable", c)
	}
	fj.set(func(fj *fakeJenkins) { fj.failReads = false })
	deadline := time.Now().Add(5 * time.Second)
	for fj.triggerCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c := fj.triggerCount(); c != 1 {
		t.Fatalf("want exactly 1 trigger after recovery, got %d", c)
	}
}

// If a trigger fails after Jenkins already queued the build, the retry must find
// and adopt that build rather than start a second one.
func TestUncertainTriggerIsCheckedBeforeRetry(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job"})
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.lostTrigger = true })

	h.r.tick(ctx) // build queued, response lost
	fj.set(func(fj *fakeJenkins) { fj.lostTrigger = false; fj.failReads = true })
	h.r.tick(ctx) // Jenkins unreadable: must not trigger blind
	fj.set(func(fj *fakeJenkins) { fj.failReads = false })
	h.r.tick(ctx) // finds and adopts the build

	if c := fj.triggerCount(); c != 1 {
		t.Fatalf("triggered %d times, want 1", c)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Fatal("existing build was not adopted")
	}
}

// A trigger that definitely did not queue anything is retried once Jenkins
// confirms there is no run.
func TestFailedTriggerRetriedWhenJenkinsHasNoRun(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job"})
	fj := h.fj
	fj.set(func(fj *fakeJenkins) { fj.failTrigger = true })

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) { fj.failTrigger = false })
	h.r.tick(ctx)
	if c := fj.triggerCount(); c != 1 {
		t.Fatalf("successful triggers = %d, want 1", c)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Fatal("retried trigger not tracked")
	}
}

// An adopted queued run whose queue item later expires is looked up by job name.
func TestAdoptedQueuedItemVanishes(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job"})
	fj := h.fj
	fj.addQueued("example-job", testID)
	recovered(t, h)
	h.r.tick(ctx) // adopted, queued
	fj.set(func(fj *fakeJenkins) { fj.queue = map[int]*fjQueueItem{} })

	for i := 0; i < maxQueueMisses; i++ {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || !strings.Contains(res[0].Message, "queue item disappeared") {
		t.Errorf("results = %+v, want queue-item-disappeared error", res)
	}
}
