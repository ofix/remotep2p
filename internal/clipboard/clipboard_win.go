//go:build windows
package clipboard

func newImpl() Clip { return &WinClip{} }

type WinClip struct{}
func (w *WinClip) ReadText() (string, error) { return "", nil }
func (w *WinClip) WriteText(s string) error { return nil }
func (w *WinClip) ReadImage() ([]byte, error) { return nil, nil }
func (w *WinClip) ReadFileList() ([]string, error) { return nil, nil }
