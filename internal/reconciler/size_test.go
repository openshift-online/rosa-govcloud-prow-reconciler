package reconciler

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// Oversized artifacts are skipped, without retries, and the result is error
// since its outputs are incomplete. The per-build total caps the sum: once it is
// used up, later artifacts are skipped too.
func TestArtifactSizeCaps(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) {
		c.MaxArtifactBytes = 10
		c.MaxTotalArtifactBytes = 15
	})
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{
			"a-huge.bin":  "12345678901", // 11: over the per-artifact cap
			"b-small.txt": "12345678",    // 8: fits (7 left)
			"c-mid.txt":   "12345678",    // 8: over the 7 left of the total
			"d-tiny.txt":  "1234567",     // 7: fits exactly (0 left)
			"e-last.txt":  "1",           // budget used up
		}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx)

	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError {
		t.Fatalf("results = %+v, want one error", res)
	}
	got := slices.Sorted(slices.Values(res[0].Artifacts))
	if want := []string{"b-small.txt", "build-log.txt", "d-tiny.txt"}; !slices.Equal(got, want) {
		t.Errorf("artifacts = %v, want %v", got, want)
	}
	for _, want := range []string{
		`Jenkins build 1 finished with SUCCESS, but 3 artifacts skipped: `,
		`"a-huge.bin" (larger than MAX_ARTIFACT_BYTES (10))`,
		`"c-mid.txt" (larger than the 7 bytes left of MAX_TOTAL_ARTIFACT_BYTES (15))`,
		`"e-last.txt" (MAX_TOTAL_ARTIFACT_BYTES (15) reached)`,
	} {
		if !strings.Contains(res[0].Message, want) {
			t.Errorf("message = %q, want it to contain %q", res[0].Message, want)
		}
	}
	if _, ok := h.s3.artifact(testID, "a-huge.bin"); ok {
		t.Error("oversized artifact uploaded")
	}
}

// Over MAX_ARTIFACTS the extras are skipped (no retries) and the result is error.
func TestArtifactCountCap(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.MaxArtifacts = 2 })
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx)

	res := h.s3.resultsFor(testID)
	if len(res) != 1 || res[0].Status != contract.StatusError {
		t.Fatalf("results = %+v, want one error", res)
	}
	if got := slices.Sorted(slices.Values(res[0].Artifacts)); !slices.Equal(got, []string{"a", "b", "build-log.txt"}) {
		t.Errorf("artifacts = %v, want the first two plus the log", got)
	}
	if want := `"2 artifacts beyond the first 2" (MAX_ARTIFACTS (2) reached)`; !strings.Contains(res[0].Message, want) {
		t.Errorf("message = %q, want it to contain %q", res[0].Message, want)
	}
}
