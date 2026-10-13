package reconciler

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// A transient download failure is retried on the next tick, and the result
// reflects the successful retry.
func TestTransientArtifactFailureRetried(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.failArtifacts = map[string]int{"junit_01.xml": 1}

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"junit_01.xml": "<testsuite/>"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // download fails: no result yet
	if res := h.s3.resultsFor(testID); len(res) != 0 {
		t.Fatalf("result written despite a retryable failure: %+v", res)
	}
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Message != "Jenkins build 1 finished with SUCCESS" ||
		!slices.Equal(slices.Sorted(slices.Values(res[0].Artifacts)), []string{"build-log.txt", "junit_01.xml"}) {
		t.Errorf("results = %+v, want a clean success with both artifacts", res)
	}
}

// After S3 fails on one artifact, only artifacts not yet in S3 are reported as
// not uploaded; ones sent by an earlier attempt stay listed as uploaded.
func TestUploadFailureKeepsEarlierUploads(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.failArtifacts = map[string]int{"a.xml": 1} // attempt 1: a is fetched late, b and c go up
	h.s3.failArtifact = "a.xml"                   // from attempt 2 on, uploading a fails

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"a.xml": "a", "b.xml": "b", "c.xml": "c"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	for range maxCollectAttempts {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 {
		t.Fatalf("results = %+v, want exactly one", res)
	}
	if got := slices.Sorted(slices.Values(res[0].Artifacts)); !slices.Equal(got, []string{"b.xml", "c.xml"}) {
		t.Errorf("artifacts = %v, want b.xml and c.xml (sent earlier)", got)
	}
	if !strings.Contains(res[0].Message, `1 artifact could not be uploaded to S3: "a.xml"`) {
		t.Errorf("message = %q, want only a.xml reported as not uploaded", res[0].Message)
	}
}

// Persistent fetch failures stop being retried after maxCollectAttempts: the
// result is error (Collecting --> Error: fetch failed) even though Jenkins
// passed, the message gives the Jenkins result and the failures, and
// "artifacts" lists only uploaded files.
func TestPersistentArtifactFailuresRecorded(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	arts := map[string]string{"ok.xml": "<ok/>"}
	fj.failArtifacts = map[string]int{}
	for i := 0; i < 7; i++ {
		p := fmt.Sprintf("broken-%d.xml", i)
		arts[p] = "x"
		fj.failArtifacts[p] = -1
	}

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) { fj.builds["example-job"][0].artifacts = arts })
	fj.finish("example-job", 1, "SUCCESS")
	for i := 0; i < maxCollectAttempts+2; i++ {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 {
		t.Fatalf("results = %+v, want exactly one", res)
	}
	if res[0].Status != contract.StatusError {
		t.Errorf("status = %q, want error", res[0].Status)
	}
	if got := slices.Sorted(slices.Values(res[0].Artifacts)); !slices.Equal(got, []string{"build-log.txt", "ok.xml"}) {
		t.Errorf("artifacts = %v, want only what was uploaded", got)
	}
	want := `Jenkins build 1 finished with SUCCESS, but 7 artifacts could not be fetched from Jenkins: ` +
		`"broken-0.xml", "broken-1.xml", "broken-2.xml", "broken-3.xml", "broken-4.xml", and 2 more`
	if res[0].Message != want {
		t.Errorf("message = %q\nwant      %q", res[0].Message, want)
	}
}

// Persistent S3 upload failures also make the result error
// (Uploading --> Error: S3 write failed), whatever Jenkins reported.
func TestArtifactUploadFailureRecorded(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	h.s3.failArtifact = "reports/junit.xml"

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"reports/junit.xml": "<j/>", "other.txt": "o"}
	})
	fj.finish("example-job", 1, "FAILURE")
	for i := 0; i < maxCollectAttempts; i++ {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError {
		t.Fatalf("results = %+v, want one error", res)
	}
	if !strings.Contains(res[0].Message, `Jenkins build 1 finished with FAILURE, but 1 artifact could not be uploaded to S3: "reports/junit.xml"`) {
		t.Errorf("message = %q", res[0].Message)
	}
	if slices.Contains(res[0].Artifacts, "reports/junit.xml") {
		t.Errorf("artifacts = %v lists a failed upload", res[0].Artifacts)
	}
}

// Failures are retried before they count: a build whose outputs all copy on
// the last allowed attempt still reports its Jenkins status.
func TestFailuresRecoveredOnLastAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.failArtifacts = map[string]int{"junit_01.xml": maxCollectAttempts - 1}

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"junit_01.xml": "<testsuite/>"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	for i := 0; i < maxCollectAttempts; i++ {
		h.r.tick(ctx)
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess {
		t.Errorf("results = %+v, want one success", res)
	}
}

// A timed-out run is already an error; missing outputs are added to its message.
func TestAbandonNotesMissingLog(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fj.failConsole = true

	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError ||
		!strings.HasSuffix(res[0].Message, "; build-log.txt could not be fetched from Jenkins") {
		t.Errorf("results = %+v, want one error noting the missing log", res)
	}
}

// When only PutResult fails, later ticks retry just that write (no re-upload)
// and succeed once S3 recovers.
func TestPutResultRetriedAlone(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) { fj.builds["example-job"][0].artifacts = map[string]string{"a.xml": "<a/>"} })
	fj.finish("example-job", 1, "SUCCESS")
	h.s3.failResult = true
	for i := 0; i < maxCollectAttempts+3; i++ {
		h.r.tick(ctx)
	}
	if len(h.s3.resultsFor(testID)) != 0 {
		t.Fatal("result written while PutResult fails")
	}
	h.s3.failResult = false
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess {
		t.Fatalf("results = %+v, want one success after S3 recovers", res)
	}
	if n := h.s3.artifactPuts; n != 2 { // a.xml and build-log.txt, once each
		t.Errorf("artifact uploads = %d, want 2 (no re-upload on result retries)", n)
	}
}

// If PutResult keeps failing past the deadline, the job is given up on.
func TestPutResultGivesUpAtDeadline(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)

	h.r.tick(ctx)
	h.fj.finish("example-job", 1, "SUCCESS")
	h.s3.failResult = true
	h.r.tick(ctx)
	h.clock.advance(h.r.cfg.MaxBuildTime + time.Minute)
	h.r.tick(ctx)
	if _, ok := h.r.inflight[testID]; ok {
		t.Fatal("still in flight after the deadline")
	}
	if !strings.Contains(h.logs.String(), "giving up on result.json") {
		t.Error("no error logged")
	}
}

// A retry after a partial failure uploads only what is missing: artifacts that
// already reached S3 are not put again.
func TestRetrySkipsUploadedArtifacts(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	fj.failArtifacts = map[string]int{"b.xml": 1}

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"a.xml": "<a/>", "b.xml": "<b/>"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx) // b.xml download fails; a.xml and the log are uploaded
	h.r.tick(ctx)
	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusSuccess ||
		!slices.Equal(slices.Sorted(slices.Values(res[0].Artifacts)), []string{"a.xml", "b.xml", "build-log.txt"}) {
		t.Fatalf("results = %+v, want a clean success with all three artifacts", res)
	}
	if n := h.s3.artifactPuts; n != 3 {
		t.Errorf("artifact uploads = %d, want 3 (each once)", n)
	}
}

// S3 failing (or hanging) on one artifact must not cost a wait per remaining
// artifact: uploads stop at the first error, and the rest count as not uploaded.
func TestUploadStopsAtFirstS3Error(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj
	h.s3.failPuts = true
	calls := 0
	h.s3.beforeArtifact = func(context.Context) { calls++ }

	h.r.tick(ctx)
	arts := map[string]string{}
	for i := 0; i < 50; i++ {
		arts[fmt.Sprintf("f%02d.txt", i)] = "x"
	}
	fj.set(func(fj *fakeJenkins) { fj.builds["example-job"][0].artifacts = arts })
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx)
	if calls != 1 {
		t.Errorf("PutArtifact calls in one attempt = %d, want 1", calls)
	}
	if len(h.s3.resultsFor(testID)) != 0 {
		t.Error("result written after the first attempt; the transient retry path should apply")
	}
}
