package reconciler

import (
	"context"
	"slices"
	"testing"
)

// The shim treats result.json as the completion signal, so every artifact must be
// in S3 before result.json is written — including when the first attempt fails
// and the job is retried on a later tick.
func TestArtifactsUploadedBeforeResult(t *testing.T) {
	for _, tc := range []struct {
		name        string
		failFirstUp bool
	}{
		{"first attempt", false},
		{"after failed upload", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, nil)
			fj := h.fj

			h.r.tick(ctx) // trigger + resolve queue -> running
			fj.finish("example-job", 1, "SUCCESS")
			fj.set(func(fj *fakeJenkins) {
				fj.builds["example-job"][0].artifacts = map[string]string{
					"junit_01.xml":         "<testsuite/>",
					"logs/build-log.txt":   "log",
					"reports/summary.json": "{}",
				}
			})
			if tc.failFirstUp {
				h.s3.setFailPuts(true)
				h.r.tick(ctx) // every write fails; job stays in flight
				h.s3.setFailPuts(false)
				if n := len(h.s3.opsLog()); n != 0 {
					t.Fatalf("%d writes recorded while S3 was failing", n)
				}
			}
			h.r.tick(ctx) // collect + write

			ops := h.s3.opsLog()
			if len(ops) != 5 {
				t.Fatalf("ops = %v, want 3 artifacts and the console log, then result", ops)
			}
			if ops[4] != "result:"+testID {
				t.Errorf("last write = %q, want result.json; ops = %v", ops[4], ops)
			}
			arts := slices.Sorted(slices.Values(ops[:4]))
			want := []string{
				"artifact:" + testID + "/build-log.txt",
				"artifact:" + testID + "/junit_01.xml",
				"artifact:" + testID + "/logs/build-log.txt",
				"artifact:" + testID + "/reports/summary.json",
			}
			if !slices.Equal(arts, want) {
				t.Errorf("artifact writes = %v, want %v", arts, want)
			}
			res := h.s3.resultsFor(testID)
			if len(res) != 1 || len(res[0].Artifacts) != 4 {
				t.Errorf("results = %+v, want one result listing 4 artifacts", res)
			}
		})
	}
}
