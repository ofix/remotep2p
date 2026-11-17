//go:build windows
package platform

import (
	"syscall"
	"unsafe"
)

func implOpenFolder(path string) error {
	shell32 := syscall.NewLazyDLL("shell32.dll")
	proc := shell32.NewProc("ShellExecuteW")
	cmd := syscall.StringToUTF16Ptr("open")
	p := syscall.StringToUTF16Ptr(path)
	r, _, err := proc.Call(0, uintptr(unsafe.Pointer(cmd)), uintptr(unsafe.Pointer(p)), 0, 0, 1)
	if r <= 32 {
		return err
	}
	return nil
}
