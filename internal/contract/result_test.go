package contract

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

// The result.json keys are defined by the ROSA enhancement (The S3 Contract) and
// read by the Prow-side CI shim. Renaming one silently breaks the shim.
func TestResultJSONKeys(t *testing.T) {
	b, err := json.Marshal(Result{
		Status:       StatusSuccess,
		Duration:     "15.7s",
		JenkinsBuild: "https://jenkins.example.internal/job/example-job/17/",
		Artifacts:    []string{"junit_01.xml", "build-log.txt"},
		Message:      "Jenkins build 17 finished with SUCCESS",
	})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	var got []string
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"artifacts", "duration", "jenkins_build", "message", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("result.json keys = %v, want %v", got, want)
	}
}

// Status mapping table from the enhancement (The S3 Contract). Values outside the
// table must never produce a pass.
func TestStatusForJenkinsResult(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"SUCCESS", StatusSuccess},
		{"FAILURE", StatusFailure},
		{"UNSTABLE", StatusFailure},
		{"ABORTED", StatusError},
		{"NOT_BUILT", StatusError},
		{"", StatusError}, // JSON null decodes to ""
		{"success", StatusError},
		{" SUCCESS", StatusError},
		{"SUCCESS\n", StatusError},
		{"UNKNOWN", StatusError},
	} {
		if got := StatusForJenkinsResult(tc.in); got != tc.want {
			t.Errorf("StatusForJenkinsResult(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
