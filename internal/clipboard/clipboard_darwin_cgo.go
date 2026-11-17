//go:build darwin && cgo
package clipboard

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Cocoa
#include <Cocoa/Cocoa.h>
#include <stdlib.h>

static NSString* getClipText() {
    NSPasteboard *pb = [NSPasteboard generalPasteboard];
    return [pb stringForType:NSPasteboardTypeString];
}
static void setClipText(NSString *s) {
    NSPasteboard *pb = [NSPasteboard generalPasteboard];
    [pb clearContents];
    [pb setString:s forType:NSPasteboardTypeString];
}
static NSData* getClipPNG() {
    NSPasteboard *pb = [NSPasteboard generalPasteboard];
    NSImage *img = [pb readImageForType:NSPasteboardTypePNG];
    if (!img) return nil;
    return [img TIFFRepresentation];
}
static NSArray* getClipFileURLs() {
    NSPasteboard *pb = [NSPasteboard generalPasteboard];
    return [pb propertyListForType:NSPasteboardTypeFileURL];
}
*/
import "C"
import (
	"bytes"
	"image"
	"image/png"
	"unsafe"
)

func newImpl() Clip { return &DarwinCGOClip{} }
type DarwinCGOClip struct{}

func (d *DarwinCGOClip) ReadText() (string, error) {
	s := C.getClipText()
	if s == nil {
		return "", nil
	}
	return C.GoString(C.UTF8String(s)), nil
}
func (d *DarwinCGOClip) WriteText(s string) error {
	cs := C.NSStringFromString(C.CString(s))
	defer C.free(unsafe.Pointer(C.CString(s)))
	C.setClipText(cs)
	return nil
}
func (d *DarwinCGOClip) ReadImage() ([]byte, error) {
	data := C.getClipPNG()
	if data == nil {
		return nil, nil
	}
	length := C.length(data)
	ptr := C.bytes(data)
	b := C.GoBytes(unsafe.Pointer(ptr), C.int(length))
	img, _, err := image.Decode(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	err = png.Encode(&out, img)
	return out.Bytes(), err
}
func (d *DarwinCGOClip) ReadFileList() ([]string, error) {
	arr := C.getClipFileURLs()
	if arr == nil {
		return nil, nil
	}
	count := C.arrayCount(arr)
	var list []string
	for i := C.int(0); i < count; i++ {
		urlStr := C.arrayObjectAtIndex(arr, i)
		if urlStr == nil {
			continue
		}
		list = append(list, C.GoString(C.UTF8String(urlStr)))
	}
	return list, nil
}
