package clipboard

type Clip interface {
	ReadText() (string, error)
	WriteText(string) error
	ReadImage() ([]byte, error)
	ReadFileList() ([]string, error)
}

var Instance Clip

func init() {
	Instance = newImpl()
}
