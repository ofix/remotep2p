package node

import (
	"encoding/binary"
	"sync"
)

type Task struct {
	BatchID [4]byte
	Offset  int64
	Done    bool
}

type TaskManager struct {
	mu    sync.Mutex
	tasks map[[4]byte]*Task
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[[4]byte]*Task),
	}
}

func (tm *TaskManager) OnAck(bid [4]byte, body []byte) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tasks[bid]; ok {
		off := binary.LittleEndian.Uint64(body)
		t.Offset = int64(off)
	}
}

func (tm *TaskManager) MarkDone(bid [4]byte) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, bid)
}
