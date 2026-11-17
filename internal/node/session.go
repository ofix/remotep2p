package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/quic-go/quic-go"
	"remotep2p/internal/codec"
	"remotep2p/internal/resume"
	"remotep2p/internal/transfer"
)

type PeerSession struct {
	ctx      context.Context
	conn     quic.Connection
	ctrlStr  quic.Stream
	token    string
	resumeSt *resume.Store
	taskMgr  *TaskManager
	recvDir  string
	thumbDir string
}

func NewPeerSession(ctx context.Context, conn quic.Connection, ctrlStr quic.Stream, token string, rs *resume.Store, tm *TaskManager, recvDir, thumbDir string) *PeerSession {
	return &PeerSession{
		ctx:      ctx,
		conn:     conn,
		ctrlStr:  ctrlStr,
		token:    token,
		resumeSt: rs,
		taskMgr:  tm,
		recvDir:  recvDir,
		thumbDir: thumbDir,
	}
}

func (s *PeerSession) Handshake() bool {
	frame := &codec.Frame{
		FrameType: codec.FrameHandshake,
		BatchID:   [4]byte{},
		BodyLen:   uint64(len(s.token)),
		Flags:     0,
		Body:      []byte(s.token),
	}
	if err := codec.WriteFrame(s.ctrlStr, frame); err != nil {
		return false
	}
	rsp, err := codec.ReadFrame(s.ctrlStr)
	if err != nil || rsp.FrameType != codec.FrameHandshake || string(rsp.Body) != s.token {
		return false
	}
	return true
}

func (s *PeerSession) Run() {
	defer s.conn.CloseWithError(0, "session exit")
	for {
		f, err := codec.ReadFrame(s.ctrlStr)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("ctrl read err:", err)
			return
		}
		s.onCtrlFrame(f)
	}
}

func (s *PeerSession) onCtrlFrame(f *codec.Frame) {
	switch f.FrameType {
	case codec.FrameClipText:
		fmt.Printf("recv clip text: %s
", string(f.Body))
	case codec.FrameTaskReq:
		go s.startRecvTask(f)
	case codec.FrameTaskResume:
		go s.startRecvTask(f)
	case codec.FrameTaskAck:
		s.taskMgr.OnAck(f.BatchID, f.Body)
	case codec.FrameTaskDone:
		s.taskMgr.MarkDone(f.BatchID)
		_ = s.resumeSt.Delete(f.BatchID)
	case codec.FrameTaskErr:
		fmt.Printf("task %x error: %s
", f.BatchID, string(f.Body))
	}
}

type TaskReqPayload struct {
	Name     string `json:"name"`
	Total    int64  `json:"total"`
	Sha256   string `json:"sha256"`
	IsFolder bool   `json:"is_folder"`
}

func (s *PeerSession) startRecvTask(f *codec.Frame) {
	var req TaskReqPayload
	if err := json.Unmarshal(f.Body, &req); err != nil {
		fmt.Println("parse task req err:", err)
		return
	}
	bid := f.BatchID
	meta := &resume.Meta{
		BatchID:    bid,
		IsFolder:   req.IsFolder,
		TotalSize:  req.Total,
		Offset:     0,
		RecvPath:   filepath.Join(s.recvDir, req.Name),
		CreateTime: time.Now().Unix(),
		Sha256:     req.Sha256,
	}
	if (f.Flags & codec.FlagResume) != 0 {
		old, err := s.resumeSt.Load(bid)
		if err == nil {
			meta.Offset = old.Offset
		}
	}
	if err := s.resumeSt.Save(meta); err != nil {
		fmt.Println("save resume meta err:", err)
		return
	}
	dataStream, err := s.conn.AcceptStream(s.ctx)
	if err != nil {
		fmt.Println("accept data stream err:", err)
		return
	}
	defer dataStream.Close()
	var r io.Reader = dataStream
	if (f.Flags & codec.FlagCompressed) != 0 {
		zr, err := transfer.NewZstdReader(r)
		if err != nil {
			fmt.Println("zstd reader err:", err)
			return
		}
		defer zr.Close()
		r = zr
	}
	mode := os.O_CREATE | os.O_WRONLY
	if meta.Offset > 0 {
		mode |= os.O_APPEND
	}
	_ = os.MkdirAll(filepath.Dir(meta.RecvPath), 0755)
	out, err := os.OpenFile(meta.RecvPath, mode, 0644)
	if err != nil {
		fmt.Println("open recv file err:", err)
		return
	}
	defer out.Close()
	if meta.Offset > 0 {
		if _, err := out.Seek(meta.Offset, io.SeekStart); err != nil {
			fmt.Println("seek err:", err)
			return
		}
	}
	buf := make([]byte, 65536)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				fmt.Println("write chunk err:", err)
				return
			}
			meta.Offset += int64(n)
			_ = s.resumeSt.Save(meta)
			ack := make([]byte, 8)
			binary.LittleEndian.PutUint64(ack, uint64(meta.Offset))
			ackFrame := &codec.Frame{
				FrameType: codec.FrameTaskAck,
				BatchID:   bid,
				BodyLen:   8,
				Body:      ack,
			}
			_ = codec.WriteFrame(s.ctrlStr, ackFrame)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("read chunk err:", err)
			errFrame := &codec.Frame{
				FrameType: codec.FrameTaskErr,
				BatchID:   bid,
				BodyLen:   uint64(len(err.Error())),
				Body:      []byte(err.Error()),
			}
			_ = codec.WriteFrame(s.ctrlStr, errFrame)
			return
		}
	}
	// final checksum
	gotHash, err := transfer.FileSha256(meta.RecvPath)
	if err != nil || gotHash != meta.Sha256 {
		errFrame := &codec.Frame{
			FrameType: codec.FrameTaskErr,
			BatchID:   bid,
			BodyLen:   uint64(len("sha256 mismatch")),
			Body:      []byte("sha256 mismatch"),
		}
		_ = codec.WriteFrame(s.ctrlStr, errFrame)
		return
	}
	// try thumbnail
	thumbPath := transfer.ThumbPath(s.thumbDir, fmt.Sprintf("%x", bid))
	_ = transfer.GenThumbnail(meta.RecvPath, thumbPath)
	doneFrame := &codec.Frame{
		FrameType: codec.FrameTaskDone,
		BatchID:   bid,
		BodyLen:   0,
		Body:      nil,
	}
	_ = codec.WriteFrame(s.ctrlStr, doneFrame)
}
