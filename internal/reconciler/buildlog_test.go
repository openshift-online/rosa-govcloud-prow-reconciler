package reconciler

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// The contract puts the build log at artifacts/build-log.txt. It is uploaded
// for every result that follows a started build, listed in result.json, and
// written before result.json.
func TestConsoleLogUploadedAsBuildLog(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     string // "" = time out while building
		wantStatus string
	}{
		{"success", "SUCCESS", contract.StatusSuccess},
		{"failure", "FAILURE", contract.StatusFailure},
		{"aborted", "ABORTED", contract.StatusError},
		{"timeout", "", contract.StatusError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, nil)
			fj := h.fj

			h.r.tick(ctx)
			fj.set(func(fj *fakeJenkins) { fj.builds["example-job"][0].console = "step 1\nstep 2 FAILED\n" })
			if tc.result != "" {
				fj.finish("example-job", 1, tc.result)
			} else {
				h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
			}
			h.r.tick(ctx)

			res := h.s3.resultsFor(testID)
			if len(res) != 1 || res[0].Status != tc.wantStatus {
				t.Fatalf("results = %+v, want one %s", res, tc.wantStatus)
			}
			if !slices.Contains(res[0].Artifacts, "build-log.txt") {
				t.Errorf("artifacts = %v, want build-log.txt listed", res[0].Artifacts)
			}
			if got, _ := h.s3.artifact(testID, "build-log.txt"); got != "step 1\nstep 2 FAILED\n" {
				t.Errorf("build-log.txt = %q", got)
			}
			ops := h.s3.opsLog()
			if ops[len(ops)-1] != "result:"+testID || !slices.Contains(ops, "artifact:"+testID+"/build-log.txt") {
				t.Errorf("ops = %v, want build-log.txt before result.json", ops)
			}
		})
	}
}

// A log over MAX_LOG_BYTES keeps its tail (where failures are) behind a notice.
func TestConsoleLogTruncatedToTail(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.MaxLogBytes = 16 })
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].console = strings.Repeat("noise\n", 100) + "FATAL: it broke\n"
	})
	fj.finish("example-job", 1, "FAILURE")
	h.r.tick(ctx)

	got, _ := h.s3.artifact(testID, "build-log.txt")
	want := "[build log truncated by the reconciler: showing the last 16 of 616 bytes]\nFATAL: it broke\n"
	if got != want {
		t.Errorf("build-log.txt = %q, want %q", got, want)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || !strings.Contains(res[0].Message, "build log truncated") {
		t.Errorf("results = %+v, want truncation noted in the message", res)
	}
}

// The truncation note survives a collection retry, when the log is already in S3.
func TestTruncationNoteKeptOnRetry(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.MaxLogBytes = 16 })
	fj := h.fj
	fj.failArtifacts = map[string]int{"a.xml": 1}

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].console = strings.Repeat("noise\n", 100)
		fj.builds["example-job"][0].artifacts = map[string]string{"a.xml": "a"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // log uploaded (truncated), artifact fetch fails
	h.r.tick(ctx)

	res := h.s3.resultsFor(testID)
	if len(res) != 1 || !strings.Contains(res[0].Message, "build log truncated") {
		t.Errorf("results = %+v, want truncation noted after the retry", res)
	}
}

// build-log.txt is part of the contract: a log download that keeps failing
// makes the result error (Collecting --> Error: fetch failed), and
// build-log.txt is not listed.
func TestConsoleLogFailureRecorded(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.failConsole = true

	h.r.tick(ctx)
	h.fj.finish("example-job", 1, "SUCCESS")
	for i := 0; i < maxCollectAttempts; i++ {
		h.r.tick(ctx)
	}

	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError {
		t.Fatalf("results = %+v, want one error", res)
	}
	if slices.Contains(res[0].Artifacts, "build-log.txt") {
		t.Errorf("artifacts = %v lists a log that was not uploaded", res[0].Artifacts)
	}
	if res[0].Message != "Jenkins build 1 finished with SUCCESS, but build-log.txt could not be fetched from Jenkins" {
		t.Errorf("message = %q, want the failure recorded", res[0].Message)
	}
}

// A Jenkins artifact named build-log.txt would overwrite the console log; it is
// skipped, which leaves the outputs incomplete, so the result is error.
func TestJenkinsArtifactCannotOverwriteBuildLog(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"build-log.txt": "archived by the job"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx)

	if got, _ := h.s3.artifact(testID, "build-log.txt"); got != defaultConsole {
		t.Errorf("build-log.txt = %q, want the console log", got)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError ||
		!strings.Contains(res[0].Message, `1 artifact skipped: "build-log.txt" (name reserved for the console log)`) {
		t.Errorf("results = %+v, want the skip recorded", res)
	}
	if n := strings.Count(strings.Join(h.s3.opsLog(), ","), "build-log.txt"); n != 1 {
		t.Errorf("build-log.txt written %d times, want 1", n)
	}
}
