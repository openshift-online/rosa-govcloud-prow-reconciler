package contract

import (
	"strings"
	"testing"
)

func TestValidateArtifactName(t *testing.T) {
	longest := strings.Repeat("a", maxArtifactNameBytes)
	for _, name := range []string{"reports/junit_01.xml", "a/b/c/d.log", "with space/ü.txt", "x..y/z", longest} {
		if err := ValidateArtifactName(name); err != nil {
			t.Errorf("ValidateArtifactName(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range []string{
		"", ".", "..", `a\b.txt`, "/abs.txt", "a//b.txt", "a/", "../x", "a/../../x", "a/./b",
		"new\nline.txt", "tab\t.txt", "nul\x00.txt", "del\x7f.txt", "bad\xffutf8",
		longest + "a",
	} {
		if err := ValidateArtifactName(name); err == nil {
			t.Errorf("ValidateArtifactName(%q) = nil, want error", name)
		}
	}
}
