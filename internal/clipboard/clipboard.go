package clipboard

// Clipboard 抽象接口
type Clipboard interface {
	ReadText() (string, error)
	WriteText(string) error

	// ReadImage 返回 PNG 二进制
	ReadImage() ([]byte, error)
	// WriteImage 接收 PNG 二进制
	WriteImage([]byte) error

	ReadFileList() ([]string, error)
}

var Instance Clipboard

func init() {
	Instance = newImpl()
}
