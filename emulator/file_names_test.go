package emulator_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// releaseNamedFile matches a file named after a release (cfn_resources_v19.go,
// ec2_v0126_review_test.go) rather than the domain it holds.
var releaseNamedFile = regexp.MustCompile(`_v[0-9]`)

// TestFileNames_AreNamedByDomainNotRelease refuses a Go file in this package whose name records
// the release that added it. Code is found by what it does — cfn_resources_<service>.go for a
// CloudFormation resource type, <service>_<area>.go elsewhere — and the release belongs in git
// and the CHANGELOG (#1609).
func TestFileNames_AreNamedByDomainNotRelease(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if releaseNamedFile.MatchString(name) {
			t.Errorf("%s is named after a release; name it by domain (cfn_resources_<service>.go for a CloudFormation type)", name)
		}
	}
}
