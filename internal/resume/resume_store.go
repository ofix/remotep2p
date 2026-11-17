package resume

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Meta struct {
	BatchID    [4]byte `json:"batch_id"`
	IsFolder   bool    `json:"is_folder"`
	TotalSize  int64   `json:"total_size"`
	Offset     int64   `json:"offset"`
	RecvPath   string  `json:"recv_path"`
	CreateTime int64   `json:"ts"`
	Sha256     string  `json:"sha256"`
}

type Store struct {
	dir string
}

func NewStore(dir string) *Store {
	_ = os.MkdirAll(dir, 0755)
	return &Store{dir: dir}
}

func (s *Store) path(bid [4]byte) string {
	return filepath.Join(s.dir, fmt.Sprintf("%x.resume", bid))
}

func (s *Store) Save(m *Meta) error {
	d, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(m.BatchID) + ".tmp"
	if err := os.WriteFile(tmp, d, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(m.BatchID))
}

func (s *Store) Load(bid [4]byte) (*Meta, error) {
	d, err := os.ReadFile(s.path(bid))
	if err != nil {
		return nil, err
	}
	var meta Meta
	if err := json.Unmarshal(d, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *Store) Delete(bid [4]byte) error {
	return os.Remove(s.path(bid))
}
