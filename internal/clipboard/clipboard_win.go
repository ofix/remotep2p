//go:build windows
package clipboard

import (
	"bytes"
	"image"
	"image/png"
	"syscall"
	"unsafe"

	"github.com/atotto/clipboard"
	"golang.org/x/sys/windows"
)

func newImpl() Clip { return &WinClip{} }

type WinClip struct{}

func (w *WinClip) ReadText() (string, error) {
	return clipboard.ReadAll()
}
func (w *WinClip) WriteText(s string) error {
	return clipboard.WriteAll(s)
}

func (w *WinClip) ReadImage() ([]byte, error) {
	user32 := syscall.NewLazyDLL("user32.dll")
	gdi32 := syscall.NewLazyDLL("gdi32.dll")
	OpenClipboard := user32.NewProc("OpenClipboard")
	CloseClipboard := user32.NewProc("CloseClipboard")
	GetClipboardData := user32.NewProc("GetClipboardData")
	CF_BITMAP := uintptr(2)

	ok, _, _ := OpenClipboard.Call(0)
	if ok == 0 {
		return nil, nil
	}
	defer CloseClipboard.Call()

	hBmp, _, _ := GetClipboardData.Call(CF_BITMAP)
	if hBmp == 0 {
		return nil, nil
	}

	GetObject := gdi32.NewProc("GetObjectW")
	var bm windows.BITMAP
	_, _, _ = GetObject.Call(hBmp, unsafe.Sizeof(bm), uintptr(unsafe.Pointer(&bm)))

	CreateCompatibleDC := gdi32.NewProc("CreateCompatibleDC")
	DeleteDC := gdi32.NewProc("DeleteDC")
	SelectObject := gdi32.NewProc("SelectObject")
	GetDIBits := gdi32.NewProc("GetDIBits")

	hdc, _, _ := CreateCompatibleDC.Call(0)
	defer DeleteDC.Call(hdc)
	old, _, _ := SelectObject.Call(hdc, hBmp)
	defer SelectObject.Call(hdc, old)

	width := int32(bm.BmWidth)
	height := int32(bm.BmHeight)
	bitCount := uint16(24)
	rowLen := ((width * int32(bitCount) + 31) >> 5) << 2

	info := windows.BITMAPINFO{
		BmiHeader: windows.BITMAPINFOHEADER{
			BiSize:        uint32(unsafe.Sizeof(windows.BITMAPINFOHEADER{})),
			BiWidth:       width,
			BiHeight:      -height, // top-down
			BiPlanes:      1,
			BiBitCount:    bitCount,
			BiCompression: 0,
		},
	}
	buf := make([]byte, rowLen*height)
	GetDIBits.Call(hdc, hBmp, 0, uintptr(height), uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&info)), 0)

	img := image.NewRGBA(image.Rect(0, 0, int(width), int(height)))
	idx := 0
	for y := 0; y < int(height); y++ {
		for x := 0; x < int(width); x++ {
			b := buf[idx]
			g := buf[idx+1]
			r := buf[idx+2]
			img.Set(x, y, image.RGBA{R: r, G: g, B: b, A: 0xff})
			idx += 3
		}
	}
	var out bytes.Buffer
	err := png.Encode(&out, img)
	return out.Bytes(), err
}

func (w *WinClip) ReadFileList() ([]string, error) {
	user32 := syscall.NewLazyDLL("user32.dll")
	shell32 := syscall.NewLazyDLL("shell32.dll")
	OpenClipboard := user32.NewProc("OpenClipboard")
	CloseClipboard := user32.NewProc("CloseClipboard")
	GetClipboardData := user32.NewProc("GetClipboardData")
	DragQueryFile := shell32.NewProc("DragQueryFileW")
	CF_HDROP := uintptr(15)
	ok, _, _ := OpenClipboard.Call(0)
	if ok == 0 {
		return nil, nil
	}
	defer CloseClipboard.Call()
	hDrop, _, _ := GetClipboardData.Call(CF_HDROP)
	if hDrop == 0 {
		return nil, nil
	}
	var count uint32
	DragQueryFile.Call(hDrop, 0xFFFFFFFF, 0, 0)
	out := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		var buf [512]uint16
		_, _, _ = DragQueryFile.Call(hDrop, uintptr(i), uintptr(unsafe.Pointer(&buf[0])), 512)
		out = append(out, syscall.UTF16ToString(buf[:]))
	}
	return out, nil
}
