package reconciler

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// Archived artifacts keep their relative paths, so same-named files in
// different directories no longer overwrite each other; unsafe paths are
// skipped and recorded.
func TestArtifactPathsPreserved(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	fj := h.fj

	h.r.tick(ctx)
	fj.set(func(fj *fakeJenkins) {
		fj.builds["example-job"][0].artifacts = map[string]string{
			"suite-a/junit.xml":   "<a/>",
			"suite-b/junit.xml":   "<b/>",
			"odd #name?.txt":      "odd",
			"../escape.txt":       "no",
			"dir/\x01control.txt": "no",
		}
	})
	fj.finish("example-job", 1, "SUCCESS")
	h.r.tick(ctx)

	for name, want := range map[string]string{"suite-a/junit.xml": "<a/>", "suite-b/junit.xml": "<b/>", "odd #name?.txt": "odd"} {
		if got, ok := h.s3.artifact(testID, name); !ok || got != want {
			t.Errorf("artifact %q = %q, %v; want %q", name, got, ok, want)
		}
	}
	res := h.s3.resultsFor(testID)
	if len(res) != 1 {
		t.Fatalf("results = %+v", res)
	}
	got := slices.Sorted(slices.Values(res[0].Artifacts))
	want := []string{"build-log.txt", "odd #name?.txt", "suite-a/junit.xml", "suite-b/junit.xml"}
	if !slices.Equal(got, want) {
		t.Errorf("artifacts = %v, want %v", got, want)
	}
	if n := strings.Count(res[0].Message, "(unsafe path)"); n != 2 || !strings.Contains(res[0].Message, "2 artifacts skipped") {
		t.Errorf("message = %q, want 2 unsafe paths recorded", res[0].Message)
	}
	if res[0].Status != contract.StatusError {
		t.Errorf("status = %q, want error: skipped artifacts leave the outputs incomplete", res[0].Status)
	}
}
