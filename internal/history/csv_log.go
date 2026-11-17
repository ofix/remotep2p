package history

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

type Record struct {
	TS       int64
	BatchID  string
	Type     string
	Name     string
	SavePath string
	Size     int64
	Sha256   string
	Status   string
}

type Log struct {
	path string
	mu   sync.Mutex
}

func NewLog(path string) *Log {
	return &Log{path: path}
}

func (l *Log) Append(r *Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	return w.Write([]string{
		fmt.Sprintf("%d", r.TS),
		r.BatchID,
		r.Type,
		r.Name,
		r.SavePath,
		fmt.Sprintf("%d", r.Size),
		r.Sha256,
		r.Status,
	})
}
