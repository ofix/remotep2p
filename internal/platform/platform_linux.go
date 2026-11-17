//go:build linux && !android
package platform

import (
	"os/exec"
)

func implOpenFolder(path string) error {
	return exec.Command("xdg-open", path).Start()
}
