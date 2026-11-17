//go:build darwin && cgo
package clipboard

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Cocoa
#include <Cocoa/Cocoa.h>
*/
import "C"
import (
	"fmt"
)

func newImpl() Clip { return &DarwinCGOClip{} }
type DarwinCGOClip struct{}
func (d *DarwinCGOClip) ReadText() (string, error) { return "", nil }
func (d *DarwinCGOClip) WriteText(s string) error { return nil }
func (d *DarwinCGOClip) ReadImage() ([]byte, error) { return nil, nil }
func (d *DarwinCGOClip) ReadFileList() ([]string, error) { return nil, nil }
