package transfer

import (
	"os"
	"path/filepath"
)

type FileEntry struct {
	RelPath string
	Size    int64
}

func WalkFolder(root string) ([]FileEntry, error) {
	var list []FileEntry
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		list = append(list, FileEntry{RelPath: rel, Size: info.Size()})
		return nil
	})
	return list, err
}
