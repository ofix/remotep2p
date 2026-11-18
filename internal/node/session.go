package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"remotep2p/internal/codec"
	"remotep2p/internal/resume"
	"remotep2p/internal/transfer"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type PeerSession struct {
	ctx           context.Context
	conn          quic.Connection
	ctrlStream    quic.Stream
	token         string
	resumeStore   *resume.Store
	taskMgr       *TaskManager
	recvDir       string
	thumbDir      string
	eventHub      *EventHub
	peerBucket    *TokenBucket
	globalBucket  *TokenBucket
	traffic       *TrafficStat
	globalTraffic *TrafficStat

	resumeWaitCh map[[4]byte]chan int64
	resumeWaitMu sync.Mutex
}

func NewPeerSession(
	ctx context.Context,
	conn quic.Connection,
	ctrlStream quic.Stream,
	token string,
	rs *resume.Store,
	tm *TaskManager,
	recvDir, thumbDir string,
	eh *EventHub,
	peerBucket, globalBucket *TokenBucket,
	globalTraffic *TrafficStat,
) *PeerSession {
	return &PeerSession{
		ctx:           ctx,
		conn:          conn,
		ctrlStream:    ctrlStream,
		token:         token,
		resumeStore:   rs,
		taskMgr:       tm,
		recvDir:       recvDir,
		thumbDir:      thumbDir,
		eventHub:      eh,
		peerBucket:    peerBucket,
		globalBucket:  globalBucket,
		traffic:       &TrafficStat{},
		globalTraffic: globalTraffic,
		resumeWaitCh:  make(map[[4]byte]chan int64),
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
	if err := codec.WriteFrame(s.ctrlStream, frame); err != nil {
		return false
	}
	rsp, err := codec.ReadFrame(s.ctrlStream)
	if err != nil || rsp.FrameType != codec.FrameHandshake || string(rsp.Body) != s.token {
		return false
	}
	return true
}

func (s *PeerSession) onCtrlFrame(f *codec.Frame) {
	from := s.conn.RemoteAddr().String()
	switch f.FrameType {
	case codec.FrameClipText:
		fmt.Printf("[ClipText] peer: %s\n%s\n", from, string(f.Body))
		raw, _ := json.Marshal(map[string]any{"text": string(f.Body)})
		s.eventHub.Publish(&Event{Type: EvClipText, From: from, Data: json.RawMessage(raw)})

	case codec.FrameClipImage:
		fmt.Printf("[ClipImage] peer: %s, png len: %d\n", from, len(f.Body))
		raw, _ := json.Marshal(map[string]any{"size": len(f.Body)})
		s.eventHub.Publish(&Event{Type: EvClipImage, From: from, Data: json.RawMessage(raw)})

	case codec.FrameClipFiles:
		var list []string
		if err := json.Unmarshal(f.Body, &list); err == nil {
			fmt.Printf("[ClipFiles] peer: %s, files: %+v\n", from, list)
			raw, _ := json.Marshal(map[string]any{"files": list})
			s.eventHub.Publish(&Event{Type: EvClipFiles, From: from, Data: json.RawMessage(raw)})
		}

	case codec.FrameTaskReq:
		go s.onTaskReq(f)

	case codec.FrameTaskResume:
		s.handleResumeResp(f)

	case codec.FrameTaskAck:
		s.taskMgr.OnAck(f.BatchID, f.Body)

	case codec.FrameTaskDone:
		s.taskMgr.MarkDone(f.BatchID)
		_ = s.resumeStore.Delete(f.BatchID)
		raw, _ := json.Marshal(map[string]any{"batch_id": fmt.Sprintf("%x", f.BatchID)})
		s.eventHub.Publish(&Event{Type: EvTaskDone, From: from, Data: json.RawMessage(raw)})

	case codec.FrameTaskErr:
		fmt.Printf("task %x error: %s\n", f.BatchID, string(f.Body))
		raw, _ := json.Marshal(map[string]any{"batch_id": fmt.Sprintf("%x", f.BatchID), "err": string(f.Body)})
		s.eventHub.Publish(&Event{Type: EvTaskError, From: from, Data: json.RawMessage(raw)})
	}
}

// WriteFrame 发送前限流 + 统计tx，适配 codec.WriteFrame 接口
func (s *PeerSession) WriteFrame(f *codec.Frame) error {
	var buf bytes.Buffer
	if err := codec.WriteFrame(&buf, f); err != nil {
		return err
	}
	n := int64(buf.Len())
	if s.peerBucket != nil {
		s.peerBucket.Take(n)
	}
	if s.globalBucket != nil {
		s.globalBucket.Take(n)
	}
	_, err := s.ctrlStream.Write(buf.Bytes())
	s.traffic.AddTx(n)
	s.globalTraffic.AddTx(n)
	return err
}

func (s *PeerSession) Run() {
	defer s.conn.CloseWithError(0, "session exit")
	for {
		f, err := codec.ReadFrame(s.ctrlStream)
		if err != nil {
			if err != io.EOF {
				fmt.Println("read frame err:", err)
			}
			return
		}
		// 计算收到帧的大小用于RX统计
		var buf bytes.Buffer
		if err := codec.WriteFrame(&buf, f); err == nil {
			rxN := int64(buf.Len())
			s.traffic.AddRx(rxN)
			s.globalTraffic.AddRx(rxN)
		}
		s.onCtrlFrame(f)
	}
}

func (s *PeerSession) onTaskReq(f *codec.Frame) {
	var req codec.TaskReqPayload
	if err := json.Unmarshal(f.Body, &req); err != nil {
		log.Printf("onTaskReq unmarshal err: %v", err)
		return
	}
	bid := f.BatchID

	if (f.Flags & codec.FlagResume) != 0 {
		localMeta, hasLocal, err := s.loadResumeMetaOnlyLoad(bid, &req)
		if err != nil {
			log.Printf("loadResumeMetaOnlyLoad err: %v bid=%x", err, bid)
			hasLocal = false
		}
		safeOffset := req.ClaimOffset
		if hasLocal && localMeta.Offset < safeOffset {
			safeOffset = localMeta.Offset
		}
		if safeOffset > req.Total {
			safeOffset = 0
		}

		respPayload := codec.TaskResumePayload{SafeOffset: safeOffset}
		respBody, err := json.Marshal(&respPayload)
		if err != nil {
			log.Printf("marshal TaskResumePayload err: %v", err)
			return
		}
		respFrame := &codec.Frame{
			FrameType: codec.FrameTaskResume,
			BatchID:   bid,
			Body:      respBody,
			BodyLen:   uint64(len(respBody)),
		}
		if err := codec.WriteFrame(s.ctrlStream, respFrame); err != nil {
			log.Printf("send FrameTaskResume err: %v bid=%x", err, bid)
			return
		}
		s.startRecvTask(f, safeOffset)
		return
	}
	s.startRecvTask(f, 0)
}

func (s *PeerSession) handleResumeResp(f *codec.Frame) {
	var payload codec.TaskResumePayload
	if err := json.Unmarshal(f.Body, &payload); err != nil {
		log.Printf("handleResumeResp unmarshal err: %v", err)
		return
	}
	bid := f.BatchID

	s.resumeWaitMu.Lock()
	ch, ok := s.resumeWaitCh[bid]
	if ok {
		delete(s.resumeWaitCh, bid)
	}
	s.resumeWaitMu.Unlock()

	if ok {
		ch <- payload.SafeOffset
		close(ch)
	} else {
		log.Printf("recv resume resp but no waiting sender bid=%x", bid)
	}
}

// loadResumeMetaOnlyLoad 仅加载校验本地断点，协商阶段使用，不写盘
func (s *PeerSession) loadResumeMetaOnlyLoad(bid [4]byte, req *codec.TaskReqPayload) (*resume.Meta, bool, error) {
	old, err := s.resumeStore.Load(bid)
	if err != nil {
		return nil, false, nil
	}
	if old.Sha256 == req.Sha256 && old.TotalSize == req.Total {
		return old, true, nil
	}
	log.Printf("local resume meta mismatch bid=%x, invalid", bid)
	return nil, false, nil
}

// startRecvTask 接收任务入口，negotiatedSafeOffset 为协商后的权威起点
func (s *PeerSession) startRecvTask(f *codec.Frame, negotiatedSafeOffset int64) {
	var req codec.TaskReqPayload
	if err := json.Unmarshal(f.Body, &req); err != nil {
		log.Printf("startRecvTask parse req err: %v", err)
		return
	}
	bid := f.BatchID

	meta := &resume.Meta{
		BatchID:    bid,
		IsFolder:   req.IsFolder,
		TotalSize:  req.Total,
		Offset:     negotiatedSafeOffset,
		RecvPath:   filepath.Join(s.recvDir, req.Name),
		CreateTime: time.Now().Unix(),
		Sha256:     req.Sha256,
	}
	if err := s.resumeStore.Save(meta); err != nil {
		log.Printf("save recv meta err: %v bid=%x", err, bid)
		return
	}

	// ========== 下面是你原有 DataChunk 接收循环，zstd、文件写入、低频刷断点、回复TaskAck ==========
	// 这里保留留给你原有逻辑，只保证入参对齐
	log.Printf("start recv task %x, safeOffset=%d, save to %s", bid, negotiatedSafeOffset, meta.RecvPath)
}

func (s *PeerSession) shouldTriggerResumeFlag(bid [4]byte, srcPath string) (bool, int64, error) {
	old, err := s.resumeStore.Load(bid)
	if err != nil {
		return false, 0, nil
	}
	stat, err := os.Stat(srcPath)
	if err != nil {
		log.Printf("src file stat err %v", err)
		return false, 0, nil
	}
	if stat.Size() != old.TotalSize {
		log.Printf("source file size changed, discard resume bid=%x", bid)
		return false, 0, nil
	}
	hash, err := transfer.FileSha256(srcPath)
	if err != nil {
		log.Printf("calc src hash err %v", err)
		return false, 0, nil
	}
	if hash != old.Sha256 {
		log.Printf("source file hash changed, discard resume bid=%x", bid)
		return false, 0, nil
	}
	if old.Offset <= 0 {
		return false, 0, nil
	}
	return true, old.Offset, nil
}

// SendFile 对外暴露：发送单个文件
func (s *PeerSession) SendFile(srcPath string, compress bool) error {
	return s.startSendTask(srcPath, compress)
}

func (s *PeerSession) startSendTask(srcPath string, compress bool) error {
	var bid [4]byte
	if _, err := rand.Read(bid[:]); err != nil {
		return fmt.Errorf("gen batch id failed: %w", err)
	}

	needResume, claimOffset, err := s.shouldTriggerResumeFlag(bid, srcPath)
	if err != nil {
		return err
	}
	stat, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	hashStr, err := transfer.FileSha256(srcPath)
	if err != nil {
		return err
	}

	req := codec.TaskReqPayload{
		Name:        filepath.Base(srcPath),
		IsFolder:    false,
		Total:       stat.Size(),
		Sha256:      hashStr,
		ClaimOffset: claimOffset,
	}
	body, err := json.Marshal(&req)
	if err != nil {
		return err
	}
	taskFrame := &codec.Frame{
		FrameType: codec.FrameTaskReq,
		BatchID:   bid,
		Body:      body,
		BodyLen:   uint64(len(body)),
	}
	if needResume {
		taskFrame.Flags |= codec.FlagResume
	}
	if compress {
		taskFrame.Flags |= codec.FlagCompressed
	}
	if err := codec.WriteFrame(s.ctrlStream, taskFrame); err != nil {
		return err
	}

	var startOffset int64 = 0
	if needResume {
		ch := make(chan int64, 1)
		s.resumeWaitMu.Lock()
		s.resumeWaitCh[bid] = ch
		s.resumeWaitMu.Unlock()

		select {
		case startOffset = <-ch:
			log.Printf("negotiated safe offset bid=%x offset=%d", bid, startOffset)
		case <-s.ctx.Done():
			s.resumeWaitMu.Lock()
			delete(s.resumeWaitCh, bid)
			s.resumeWaitMu.Unlock()
			return fmt.Errorf("wait resume ack canceled")
		}
	}

	// 从 startOffset 开始发送 DataChunk
	log.Printf("sender ready, startOffset=%d", startOffset)
	return nil
}

// GenBatchID 生成4字节 batch id
// GenBatchID 生成4字节 BatchID
func GenBatchID() [4]byte {
	var bid [4]byte
	_, err := rand.Read(bid[:])
	if err != nil {
		panic(err)
	}
	return bid
}
