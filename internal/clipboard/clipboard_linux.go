//go:build linux
package clipboard

func newImpl() Clip { return &LinuxClip{} }
type LinuxClip struct{}
func (l *LinuxClip) ReadText() (string, error) { return "", nil }
func (l *LinuxClip) WriteText(s string) error { return nil }
func (l *LinuxClip) ReadImage() ([]byte, error) { return nil, nil }
func (l *LinuxClip) ReadFileList() ([]string, error) { return nil, nil }
