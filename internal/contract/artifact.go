package contract

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxArtifactNameBytes is what S3's 1024-byte key limit leaves for an artifact
// name after "{PROW_JOB_ID}/artifacts/" (the ID is a 36-byte canonical UUID).
const maxArtifactNameBytes = 1024 - 36 - len("/artifacts/")

// ValidateArtifactName checks that name is a safe relative path for the object
// {PROW_JOB_ID}/artifacts/{name}. Nested paths ("reports/junit_01.xml") are allowed,
// since the shim copies the prefix recursively, recreating them as directories.
// Rejected: empty names or segments (leading, trailing, or doubled "/"), "." and
// ".." segments, backslashes, control characters, invalid UTF-8, and names that
// make the key longer than S3's 1024-byte limit. These keep a Jenkins-supplied
// path from escaping the artifacts/ prefix or from writing a file outside the
// shim's download directory.
func ValidateArtifactName(name string) error {
	bad := func(why string) error {
		return fmt.Errorf("invalid artifact name %q: %s", Truncate(name, 100), why)
	}
	switch {
	case name == "":
		return bad("empty")
	case !utf8.ValidString(name):
		return bad("not valid UTF-8")
	case strings.ContainsFunc(name, unicode.IsControl):
		return bad("contains control characters")
	case strings.Contains(name, "\\"):
		return bad("contains a backslash")
	case len(name) > maxArtifactNameBytes:
		return bad("object key would exceed 1024 bytes")
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return bad("must be a relative path without empty, \".\", or \"..\" segments")
		}
	}
	return nil
}
