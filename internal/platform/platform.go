package platform

type Platform interface {
	OpenFolder(path string) error
}

func OpenFolder(path string) error {
	return implOpenFolder(path)
}
