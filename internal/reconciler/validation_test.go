package reconciler

import (
	"context"
	"strings"
	"testing"
)

const testID = "0b5e3c1a-7d2f-4e8b-9a6c-1f2d3e4a5b6c"

func TestInvalidProwJobIDIsIgnored(t *testing.T) {
	h := newHarness(t, nil,
		feedJob{ID: "../../other-prefix", State: "triggered", JenkinsJob: "example-job"},
		feedJob{ID: strings.ToUpper(testID), State: "triggered", JenkinsJob: "example-job"},
		feedJob{ID: "not-a-uuid", State: "pending", JenkinsJob: "not-allowlisted"},
	)
	h.r.tick(context.Background())

	if n := h.fj.triggerCount(); n != 0 {
		t.Errorf("triggered %d builds for invalid ProwJob IDs", n)
	}
	if len(h.s3.results) != 0 {
		t.Errorf("wrote results for invalid IDs: %v", h.s3.results)
	}
}

// A params annotation from the (untrusted) feed is ignored: only PROW_JOB_ID is sent.
func TestFeedParamsAreNotForwarded(t *testing.T) {
	h := newHarness(t, nil, feedJob{ID: testID, State: "triggered", JenkinsJob: "example-job", ParamsJSON: `{"OPENSHIFT_VERSION":"4.17"}`})
	h.r.tick(context.Background())

	form := h.fj.lastTrigger()
	if form == nil {
		t.Fatal("expected a trigger")
	}
	if form.Get("PROW_JOB_ID") != testID || len(form) != 1 {
		t.Errorf("trigger form = %v", form)
	}
}
