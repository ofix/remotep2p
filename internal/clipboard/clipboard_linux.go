//go:build linux && !android
package clipboard

import (
	"bytes"
	"os/exec"
	"strings"
)

func newImpl() Clip { return &LinuxClip{} }
type LinuxClip struct{}

func (l *LinuxClip) ReadText() (string, error) {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-o")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}
func (l *LinuxClip) WriteText(s string) error {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-i")
	cmd.Stdin = bytes.NewBufferString(s)
	return cmd.Run()
}
func (l *LinuxClip) ReadImage() ([]byte, error) {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-o", "-t", "image/png")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func (l *LinuxClip) ReadFileList() ([]string, error) {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-o", "-t", "text/uri-list")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	var list []string
	for _, line := range strings.Split(out.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// file:// URI 转本地路径，简单版
		list = append(list, strings.TrimPrefix(line, "file://"))
	}
	return list, nil
}
