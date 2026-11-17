//go:build darwin
package platform

import (
	"os/exec"
)

func implOpenFolder(path string) error {
	return exec.Command("open", path).Start()
}
