package jenkins

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListRuns(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ci/queue/api/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"items":[
		  {"url":"queue/item/9/","inQueueSince":1767225600000,"task":{"name":"example-job"},
		   "actions":[{},{"parameters":[{"name":"DRY_RUN","value":true},{"name":"PROW_JOB_ID","value":"q-id"}]}]},
		  {"url":"queue/item/10/","task":{"name":"other-job"},
		   "actions":[{"parameters":[{"name":"PROW_JOB_ID","value":"other"}]}]}
		]}`))
	})
	mux.HandleFunc("/ci/job/example-job/api/json", func(w http.ResponseWriter, r *http.Request) {
		if tree := r.URL.Query().Get("tree"); !strings.HasSuffix(tree, "]{0,5}") {
			t.Errorf("tree = %q, want a {0,5} range", tree)
		}
		_, _ = w.Write([]byte(`{"builds":[
		  {"url":"https://jenkins-root.example.internal/ci/job/example-job/4/","building":true,"timestamp":1767225600000,
		   "actions":[{"parameters":[{"name":"PROW_JOB_ID","value":"b-id"}]}]},
		  {"url":"https://jenkins-root.example.internal/ci/job/example-job/3/","building":false,"actions":[{}]}
		]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	runs, err := New(srv.URL+"/ci", "svc", "api-token").ListRuns(context.Background(), "example-job", "PROW_JOB_ID", 5)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %+v, want 2", runs)
	}
	if runs[0].ProwJobID != "q-id" || runs[0].QueueURL != srv.URL+"/ci/queue/item/9/" || runs[0].BuildURL != "" {
		t.Errorf("queue run = %+v", runs[0])
	}
	if runs[1].ProwJobID != "b-id" || runs[1].BuildURL != srv.URL+"/ci/job/example-job/4/" || !runs[1].Building {
		t.Errorf("build run = %+v", runs[1])
	}
}
