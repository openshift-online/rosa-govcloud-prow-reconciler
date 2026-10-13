package reconciler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
)

// A failed trigger must not write the Jenkins API token to the logs.
func TestTriggerFailureDoesNotLogToken(t *testing.T) {
	const token = "s3cr3t-api-token-value"
	h := newHarness(t, nil)

	dead := httptest.NewServer(http.NotFoundHandler())
	base := dead.URL
	dead.Close() // every Jenkins call fails at the transport layer

	h.r.jenkins = jenkins.New(base, "svc", token)
	h.r.tick(context.Background())

	logs := h.logs.String()
	if !strings.Contains(logs, "trigger jenkins") {
		t.Fatalf("expected a trigger failure to be logged, got:\n%s", logs)
	}
	if strings.Contains(logs, token) {
		t.Fatalf("API token leaked into logs:\n%s", logs)
	}
}
