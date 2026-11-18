//go:build linux && !android

package clipboard

import (
	"bytes"
	"os/exec"
	"strings"
)

func newImpl() Clipboard { return &LinuxClipboard{} }

type LinuxClipboard struct{}

func (l *LinuxClipboard) ReadText() (string, error) {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-o")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}

func (l *LinuxClipboard) WriteText(s string) error {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-i")
	cmd.Stdin = bytes.NewBufferString(s)
	return cmd.Run()
}

// 直接返回 PNG bytes，和接口对齐
func (l *LinuxClipboard) ReadImage() ([]byte, error) {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-o", "-t", "image/png")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// 写入 PNG bytes
func (l *LinuxClipboard) WriteImage(pngData []byte) error {
	cmd := exec.Command("xclip", "-sel", "clipboard", "-i", "-t", "image/png")
	cmd.Stdin = bytes.NewBuffer(pngData)
	return cmd.Run()
}

func (l *LinuxClipboard) ReadFileList() ([]string, error) {
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
		line = strings.TrimPrefix(line, "file://")
		list = append(list, line)
	}
	return list, nil
}
