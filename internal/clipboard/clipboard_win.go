//go:build windows
// +build windows

package clipboard

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"unsafe"

	"golang.org/x/sys/windows"
)

type BITMAPINFOHEADER struct {
	BiSize          uint32
	BiWidth         int32
	BiHeight        int32
	BiPlanes        uint16
	BiBitCount      uint16
	BiCompression   uint32
	BiSizeImage     uint32
	BiXPelsPerMeter int32
	BiYPelsPerMeter int32
	BiClrUsed       uint32
	BiClrImportant  uint32
}

type BITMAPINFO struct {
	BmiHeader BITMAPINFOHEADER
}

const (
	cfUnicodeText = 13
	cfDib         = 8
	cfHDrop       = 15

	LMEM_FIXED    = 0x0000
	LMEM_ZEROINIT = 0x0040
)

var (
	user32   = windows.NewLazySystemDLL("user32.dll")
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")

	procOpenClipboard              = user32.NewProc("OpenClipboard")
	procCloseClipboard             = user32.NewProc("CloseClipboard")
	procEmptyClipboard             = user32.NewProc("EmptyClipboard")
	procSetClipboardData           = user32.NewProc("SetClipboardData")
	procGetClipboardData           = user32.NewProc("GetClipboardData")
	procIsClipboardFormatAvailable = user32.NewProc("IsClipboardFormatAvailable")

	procLocalAlloc = kernel32.NewProc("LocalAlloc")
)

func newImpl() Clipboard { return &winClipboard{} }

type winClipboard struct{}

func (w *winClipboard) ReadText() (string, error) {
	ret, _, err := procOpenClipboard.Call(0)
	if ret == 0 {
		return "", err
	}
	defer procCloseClipboard.Call()

	avail, _, _ := procIsClipboardFormatAvailable.Call(uintptr(cfUnicodeText))
	if avail == 0 {
		return "", nil
	}
	hData, _, err := procGetClipboardData.Call(uintptr(cfUnicodeText))
	if hData == 0 {
		return "", err
	}
	return windows.UTF16PtrToString((*uint16)(unsafe.Pointer(hData))), nil
}

func (w *winClipboard) WriteText(s string) error {
	ret, _, err := procOpenClipboard.Call(0)
	if ret == 0 {
		return err
	}
	defer procCloseClipboard.Call()
	procEmptyClipboard.Call()

	u16, err := windows.UTF16FromString(s)
	if err != nil {
		return err
	}
	h, _, err := procLocalAlloc.Call(uintptr(LMEM_FIXED|LMEM_ZEROINIT), uintptr(len(u16)*2))
	if h == 0 {
		return err
	}
	copy((*[1 << 30]uint16)(unsafe.Pointer(h))[:], u16)
	_, _, err = procSetClipboardData.Call(uintptr(cfUnicodeText), h)
	return err
}

// ReadImage：读DIB → 转RGBA画布 → 编码PNG返回[]byte
func (w *winClipboard) ReadImage() ([]byte, error) {
	ret, _, err := procOpenClipboard.Call(0)
	if ret == 0 {
		return nil, err
	}
	defer procCloseClipboard.Call()

	avail, _, _ := procIsClipboardFormatAvailable.Call(uintptr(cfDib))
	if avail == 0 {
		return nil, nil
	}
	hData, _, err := procGetClipboardData.Call(uintptr(cfDib))
	if hData == 0 {
		return nil, err
	}

	info := (*BITMAPINFO)(unsafe.Pointer(hData))
	header := info.BmiHeader
	wid := int(header.BiWidth)
	hgt := int(header.BiHeight)
	topDown := false
	if hgt < 0 {
		hgt = -hgt
		topDown = true
	}
	if header.BiBitCount != 32 {
		return nil, nil
	}
	pixStart := uintptr(hData) + unsafe.Sizeof(info.BmiHeader)
	img := image.NewRGBA(image.Rect(0, 0, wid, hgt))
	stride := wid * 4
	for y := 0; y < hgt; y++ {
		srcY := y
		if !topDown {
			srcY = hgt - 1 - y
		}
		src := (*[1 << 30]byte)(unsafe.Pointer(pixStart + uintptr(srcY*stride)))
		for x := 0; x < wid; x++ {
			b := src[x*4+0]
			g := src[x*4+1]
			r := src[x*4+2]
			a := src[x*4+3]
			img.SetRGBA(x, y, color.RGBA{R: r, G: g, B: b, A: a})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteImage：接收PNG []byte → 解码 → 转DIB BGRA写入剪贴板
func (w *winClipboard) WriteImage(pngData []byte) error {
	ret, _, err := procOpenClipboard.Call(0)
	if ret == 0 {
		return err
	}
	defer procCloseClipboard.Call()
	procEmptyClipboard.Call()

	img, _, err := image.Decode(bytes.NewReader(pngData))
	if err != nil {
		return err
	}
	rgbaImg, ok := img.(*image.RGBA)
	if !ok {
		b := img.Bounds()
		rgbaImg = image.NewRGBA(b)
		for y := b.Min.Y; y < b.Max.Y; y++ {
			for x := b.Min.X; x < b.Max.X; x++ {
				rgbaImg.Set(x, y, img.At(x, y))
			}
		}
	}
	bounds := rgbaImg.Bounds()
	width := int32(bounds.Dx())
	height := int32(bounds.Dy())

	info := BITMAPINFO{
		BmiHeader: BITMAPINFOHEADER{
			BiSize:          uint32(unsafe.Sizeof(BITMAPINFOHEADER{})),
			BiWidth:         width,
			BiHeight:        -height,
			BiPlanes:        1,
			BiBitCount:      32,
			BiCompression:   0,
			BiSizeImage:     uint32(width * height * 4),
			BiXPelsPerMeter: 2835,
			BiYPelsPerMeter: 2835,
			BiClrUsed:       0,
			BiClrImportant:  0,
		},
	}
	totalSize := unsafe.Sizeof(info.BmiHeader) + uintptr(width*height*4)
	hMem, _, err := procLocalAlloc.Call(uintptr(LMEM_FIXED|LMEM_ZEROINIT), totalSize)
	if hMem == 0 {
		return err
	}
	copy((*[1 << 30]byte)(unsafe.Pointer(hMem))[:], (*[1 << 30]byte)(unsafe.Pointer(&info.BmiHeader))[:unsafe.Sizeof(info.BmiHeader)])
	pixDst := hMem + unsafe.Sizeof(info.BmiHeader)
	dst := (*[1 << 30]byte)(unsafe.Pointer(pixDst))
	for y := 0; y < int(height); y++ {
		for x := 0; x < int(width); x++ {
			c := rgbaImg.RGBAAt(x, y)
			off := y*int(width)*4 + x*4
			dst[off+0] = c.B
			dst[off+1] = c.G
			dst[off+2] = c.R
			dst[off+3] = c.A
		}
	}
	_, _, err = procSetClipboardData.Call(uintptr(cfDib), hMem)
	return err
}

// ReadFileList 后续再补 CF_HDROP
func (w *winClipboard) ReadFileList() ([]string, error) {
	return nil, nil
}
