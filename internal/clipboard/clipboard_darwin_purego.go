//go:build darwin && !cgo
package clipboard

import (
	"bytes"
	"fmt"
	"os/exec"
)

func newImpl() Clip { return &DarwinPureClip{} }
type DarwinPureClip struct{}

func (d *DarwinPureClip) ReadText() (string, error) {
	cmd := exec.Command("osascript", "-e", "the clipboard")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}
func (d *DarwinPureClip) WriteText(s string) error {
	script := fmt.Sprintf("set the clipboard to %q", s)
	return exec.Command("osascript", "-e", script).Run()
}
func (d *DarwinPureClip) ReadImage() ([]byte, error) {
	return nil, fmt.Errorf("purego darwin image clipboard not supported")
}
func (d *DarwinPureClip) ReadFileList() ([]string, error) {
	return nil, fmt.Errorf("purego darwin file clipboard not supported")
}
