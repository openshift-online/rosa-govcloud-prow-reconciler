package reconciler

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// runAsync starts h.r.Run and returns a cancel func and a channel with Run's
// result.
func runAsync(h *harness) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.r.Run(ctx) }()
	return cancel, done
}

func waitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown")
		return nil
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A trigger in progress at shutdown completes and its run is tracked, but no
// further ProwJob is triggered.
func TestShutdownStopsNewTriggers(t *testing.T) {
	h := newHarness(t, nil,
		feedJob{ID: testID, State: "pending", JenkinsJob: "example-job"},
		feedJob{ID: otherID, State: "pending", JenkinsJob: "example-job"})
	fj := h.fj
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fj.beforeTrigger = func() { once.Do(func() { close(entered); <-release }) }

	cancel, done := runAsync(h)
	<-entered
	cancel()
	close(release)
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want a clean shutdown", err)
	}
	if n := fj.triggerCount(); n != 1 {
		t.Errorf("triggered %d builds, want only the one in progress", n)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Errorf("in-progress trigger not tracked: %v", h.r.inflight)
	}
	if got := fj.stopCalls(); len(got) != 0 {
		t.Errorf("stop calls = %v; shutdown must not stop Jenkins builds", got)
	}
}

// recoveredFinishedBuild sets up a finished build with an artifact for testID,
// which Run adopts and collects on its first tick.
func recoveredFinishedBuild(t *testing.T, mutate func(*config.Config)) *harness {
	h := newHarness(t, mutate)
	h.fj.addBuild("example-job", testID, false, "SUCCESS")
	h.fj.builds["example-job"][0].artifacts = map[string]string{"junit_01.xml": "<testsuite/>"}
	return h
}

// An upload in progress at shutdown finishes, and so does the rest of that
// job's step: result.json is written, after the artifacts.
func TestShutdownDrainsCurrentStep(t *testing.T) {
	h := recoveredFinishedBuild(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.s3.beforeArtifact = func(context.Context) { once.Do(func() { close(entered); <-release }) }

	cancel, done := runAsync(h)
	<-entered
	cancel()
	close(release)
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want a clean shutdown", err)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess {
		t.Fatalf("results = %+v, want the drained step's success", res)
	}
	ops := h.s3.opsLog()
	if !slices.Equal(ops[len(ops)-1:], []string{"result:" + testID}) || len(ops) != 3 {
		t.Errorf("ops = %v, want both artifacts, then result.json", ops)
	}
}

// A step that outlasts SHUTDOWN_TIMEOUT is cut off. Uploads may be left without
// result.json (restart recovery completes them), but result.json is never
// written for outputs that failed only because of the cut-off.
func TestShutdownTimeoutCutsOffStep(t *testing.T) {
	h := recoveredFinishedBuild(t, func(c *config.Config) { c.ShutdownTimeout = 50 * time.Millisecond })
	entered := make(chan struct{})
	var once sync.Once
	h.s3.beforeArtifact = func(ctx context.Context) {
		once.Do(func() { close(entered); <-ctx.Done() })
	}

	cancel, done := runAsync(h)
	<-entered
	cancel()
	if err := waitRun(t, done); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Run = %v, want ErrShutdownTimeout", err)
	}
	if ops := h.s3.opsLog(); slices.Contains(ops, "result:"+testID) {
		t.Errorf("ops = %v: result.json written after a cut-off collection", ops)
	}
	if _, ok := h.r.inflight[testID]; !ok {
		t.Error("cut-off job dropped from tracking")
	}
}

// Shutting down while idle returns promptly and leaves running builds alone.
func TestShutdownLeavesBuildsRunning(t *testing.T) {
	h := newHarness(t, nil)
	fj := h.fj

	cancel, done := runAsync(h)
	waitFor(t, "the trigger", func() bool { return fj.triggerCount() == 1 })
	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want a clean shutdown", err)
	}
	if got := fj.stopCalls(); len(got) != 0 || !fj.building("example-job", 1) {
		t.Errorf("stop calls = %v; the build must keep running for restart recovery", got)
	}
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Errorf("results = %+v, want none", res)
	}
}
