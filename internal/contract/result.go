// Package contract defines the data shared between the reconciler (writer) and
// the Prow-side CI shim (reader). This MUST stay in sync with "The S3 Contract" in
// the ROSA enhancement enhancements/sre-tooling/rosa-govcloud-prow-jenkins-bridge.md
// (https://github.com/openshift-online/rosa-enhancements/blob/main/enhancements/sre-tooling/rosa-govcloud-prow-jenkins-bridge.md).
package contract

// Status values written to result.json. The shim maps success -> exit 0 and
// everything else -> exit 1.
const (
	StatusSuccess = "success" // Jenkins build passed
	StatusFailure = "failure" // Jenkins build ran but tests failed
	StatusError   = "error"   // build aborted/not built, or a reconciler-side error (rejection, timeout, incomplete outputs)
)

// BuildLogArtifact is the artifact name the Jenkins console log is uploaded as
// (s3://{bucket}/{PROW_JOB_ID}/artifacts/build-log.txt), per The S3 Contract.
const BuildLogArtifact = "build-log.txt"

// StatusForJenkinsResult maps a finished Jenkins build's result to a result.json
// status, per the enhancement's status mapping table:
//
//	SUCCESS             -> success
//	FAILURE / UNSTABLE  -> failure
//	ABORTED / NOT_BUILT -> error
//
// The table does not cover any other value. Anything else, including an empty or
// null result and a lowercase or unknown value, maps to error: the reconciler
// must never report a pass it cannot confirm.
func StatusForJenkinsResult(jenkinsResult string) string {
	switch jenkinsResult {
	case "SUCCESS":
		return StatusSuccess
	case "FAILURE", "UNSTABLE":
		return StatusFailure
	default:
		return StatusError
	}
}

// Result is written to s3://{bucket}/{PROW_JOB_ID}/result.json.
//
// The shim requires `status`; every other field is optional. Always write a
// Result even on error so the shim fails fast instead of timing out.
type Result struct {
	Status       string   `json:"status"`
	Duration     string   `json:"duration,omitempty"`
	JenkinsBuild string   `json:"jenkins_build,omitempty"`
	Artifacts    []string `json:"artifacts,omitempty"`
	Message      string   `json:"message,omitempty"`
}

// Truncate shortens an untrusted string to n bytes before it is logged, echoed in
// an error, or written to a (publicly visible) result message.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
