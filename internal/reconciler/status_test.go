package reconciler

import (
	"context"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// Every Jenkins result, as seen through the build API, maps to the status in the
// enhancement's table (The S3 Contract); anything outside it is an error.
func TestJenkinsResultMapping(t *testing.T) {
	for _, tc := range []struct {
		result, wantStatus, wantMsg string
	}{
		{"SUCCESS", contract.StatusSuccess, "Jenkins build 1 finished with SUCCESS"},
		{"FAILURE", contract.StatusFailure, "Jenkins build 1 finished with FAILURE"},
		{"UNSTABLE", contract.StatusFailure, "Jenkins build 1 finished with UNSTABLE"},
		{"ABORTED", contract.StatusError, "Jenkins build 1 finished with ABORTED"},
		{"NOT_BUILT", contract.StatusError, "Jenkins build 1 finished with NOT_BUILT"},
		{"", contract.StatusError, "Jenkins build 1 finished without a result"}, // null
		{"success", contract.StatusError, `Jenkins build 1 finished with unrecognized result "success"`},
		{"SOMETHING_NEW", contract.StatusError, `Jenkins build 1 finished with unrecognized result "SOMETHING_NEW"`},
	} {
		t.Run(tc.result, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, nil)

			h.r.tick(ctx)
			h.fj.finish("example-job", 1, tc.result)
			h.r.tick(ctx)

			res := h.s3.resultsFor(testID)
			if len(res) != 1 {
				t.Fatalf("results = %+v, want one", res)
			}
			if res[0].Status != tc.wantStatus || res[0].Message != tc.wantMsg {
				t.Errorf("result = {status %q, message %q}, want {%q, %q}", res[0].Status, res[0].Message, tc.wantStatus, tc.wantMsg)
			}
		})
	}
}

// Jenkins reporting a 0 ms duration must not become "0s" in result.json.
func TestDurationOmittedWhenZero(t *testing.T) {
	for _, tc := range []struct {
		zero bool
		want string
	}{{false, "1.5s"}, {true, ""}} {
		ctx := context.Background()
		h := newHarness(t, nil)
		h.fj.zeroDuration = tc.zero

		h.r.tick(ctx)
		h.fj.finish("example-job", 1, "SUCCESS")
		h.r.tick(ctx)

		if res := h.s3.resultsFor(testID); len(res) != 1 || res[0].Duration != tc.want {
			t.Errorf("zero=%v: results = %+v, want duration %q", tc.zero, res, tc.want)
		}
	}
}
