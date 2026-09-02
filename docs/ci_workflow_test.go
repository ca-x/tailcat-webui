package docs

import (
	"os"
	"strings"
	"testing"
)

func TestCIVerifiesCommittedWebdistBeforeBuilding(t *testing.T) {
	workflowBytes, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowBytes)
	previous := -1
	for _, fragment := range []string{
		"pnpm build",
		"diff -qr web/dist webdist/dist",
		"go build -trimpath -o /tmp/tailcat-webui ./cmd/tailcat-webui",
	} {
		index := strings.Index(workflow, fragment)
		if index <= previous {
			t.Fatalf("CI fragment %q is missing or out of order", fragment)
		}
		previous = index
	}
	for _, forbidden := range []string{"rm -rf webdist/dist", "cp -R web/dist webdist/dist"} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("CI must not refresh committed assets with %q", forbidden)
		}
	}
	if !strings.Contains(workflow, "push:\n    branches: [main]") {
		t.Fatal("CI push scope must remain main-only")
	}
	if !strings.Contains(workflow, "target-compile:\n    name: Compile ${{ matrix.platform }}\n    needs: verify") {
		t.Fatal("target compile matrix must depend on the parity-verifying job")
	}
	if !strings.Contains(workflow, "go test -count=1 ./internal/privatefs ./internal/transfer") {
		t.Fatal("Windows runtime job must exercise private filesystem and transfer storage behavior")
	}
}

func TestWorkflowsUseNativeArmRunnersAndCurrentActions(t *testing.T) {
	workflows := make(map[string]string)
	for _, name := range []string{"ci.yml", "docker.yml", "release.yml"} {
		content, err := os.ReadFile("../.github/workflows/" + name)
		if err != nil {
			t.Fatal(err)
		}
		workflows[name] = string(content)
	}
	for name, workflow := range workflows {
		if !strings.Contains(workflow, "actions/checkout@v7") {
			t.Fatalf("%s must use actions/checkout@v7", name)
		}
		for _, stale := range []string{
			"actions/checkout@v4",
			"pnpm/action-setup@v4",
			"actions/setup-node@v4",
			"actions/setup-go@v6",
			"actions/upload-artifact@v4",
			"docker/setup-qemu-action",
			"docker/setup-buildx-action@v3",
			"docker/login-action@v3",
			"docker/metadata-action@v5",
			"docker/build-push-action@v6",
			"softprops/action-gh-release@v2",
		} {
			if strings.Contains(workflow, stale) {
				t.Fatalf("%s retains stale action %q", name, stale)
			}
		}
	}
	for _, name := range []string{"docker.yml", "release.yml"} {
		workflow := workflows[name]
		for _, required := range []string{
			"runner: ubuntu-24.04-arm",
			"platform: linux/arm64",
			"push-by-digest=true",
			"actions/download-artifact@v8",
			"docker buildx imagetools create",
		} {
			if !strings.Contains(workflow, required) {
				t.Fatalf("%s is missing native multi-architecture fragment %q", name, required)
			}
		}
	}
}
