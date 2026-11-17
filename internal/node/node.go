package node

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"remotep2p/internal/codec"
	"remotep2p/internal/config"
	"remotep2p/internal/resume"
)

// EventType
const (
	EvPeerUp        = "peer_up"
	EvPeerDown      = "peer_down"
	EvClipText      = "clip_text"
	EvClipImage     = "clip_image"
	EvClipFiles     = "clip_files"
	EvTaskStart     = "task_start"
	EvTaskProgress  = "task_progress"
	EvTaskDone      = "task_done"
	EvTaskError     = "task_error"
	EvTrafficStats  = "traffic_stats"
)

type Event struct {
	Type string          `json:"type"`
	From string          `json:"from,omitempty"`
	Data json.RawMessage `json:"data"`
}

type EventHub struct {
	mu   sync.Mutex
	subs map[chan *Event]struct{}
}

func NewEventHub() *EventHub {
	return &EventHub{
		subs: make(map[chan *Event]struct{}),
	}
}
func (eh *EventHub) Subscribe() chan *Event {
	ch := make(chan *Event, 16)
	eh.mu.Lock()
	eh.subs[ch] = struct{}{}
	eh.mu.Unlock()
	return ch
}
func (eh *EventHub) Unsubscribe(ch chan *Event) {
	eh.mu.Lock()
	delete(eh.subs, ch)
	close(ch)
	eh.mu.Unlock()
}
func (eh *EventHub) Publish(ev *Event) {
	eh.mu.Lock()
	defer eh.mu.Unlock()
	for ch := range eh.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// TokenBucket 简易令牌桶限速
type TokenBucket struct {
	mu       sync.Mutex
	cap      int64   // 桶容量
	rate     int64   // 每秒补充令牌 Byte/s
	tokens   int64   // 当前令牌
	lastFill time.Time
}

func NewTokenBucket(rate int64) *TokenBucket {
	if rate <= 0 {
		return nil
	}
	return &TokenBucket{
		cap:      rate,
		rate:     rate,
		tokens:   rate,
		lastFill: time.Now(),
	}
}

// Take 阻塞直到拿到足够令牌；返回 false=不限速桶
func (tb *TokenBucket) Take(n int64) bool {
	if tb == nil {
		return false
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	delta := int64(now.Sub(tb.lastFill).Seconds())
	tb.tokens += delta * tb.rate
	if tb.tokens > tb.cap {
		tb.tokens = tb.cap
	}
	tb.lastFill = now

	for tb.tokens < n {
		need := n - tb.tokens
		wait := time.Second * time.Duration((need+tb.rate-1)/tb.rate)
		tb.mu.Unlock()
		time.Sleep(wait)
		tb.mu.Lock()
		now = time.Now()
		delta := int64(now.Sub(tb.lastFill).Seconds())
		tb.tokens += delta * tb.rate
		if tb.tokens > tb.cap {
			tb.tokens = tb.cap
		}
		tb.lastFill = now
	}
	tb.tokens -= n
	return true
}

// TrafficStat 流量统计
type TrafficStat struct {
	mu sync.Mutex
	Rx int64 `json:"rx"`
	Tx int64 `json:"tx"`
}

func (ts *TrafficStat) AddRx(n int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Rx += n
}
func (ts *TrafficStat) AddTx(n int64) {
	ts.mu.Lock()
	defer ts.mu.Lock()
	ts.Tx += n
}

type Node struct {
	cfg            *config.Config
	resumeSt       *resume.Store
	taskMgr        *TaskManager
	eventHub       *EventHub
	peersLock      sync.Mutex
	peers          map[string]*PeerSession
	globalBucket   *TokenBucket
	globalTraffic  *TrafficStat
}

func NewNode(cfg *config.Config) *Node {
	return &Node{
		cfg:           cfg,
		resumeSt:      resume.NewStore(cfg.ResumeDir),
		taskMgr:       NewTaskManager(),
		eventHub:      NewEventHub(),
		peers:         make(map[string]*PeerSession),
		globalBucket:  NewTokenBucket(cfg.GlobalRateLimit),
		globalTraffic: &TrafficStat{},
	}
}

func (n *Node) EventHub() *EventHub { return n.eventHub }
func (n *Node) GlobalTraffic() *TrafficStat { return n.globalTraffic }

func (n *Node) Start(ctx context.Context) error {
	go n.listenLoop(ctx)
	go n.trafficReportLoop(ctx)
	for _, addr := range n.cfg.Peers {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			go n.dialPeer(ctx, addr)
		}
	}
	return nil
}

func (n *Node) trafficReportLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.peersLock.Lock()
			peerStats := make(map[string]*TrafficStat, len(n.peers))
			for k, v := range n.peers {
				peerStats[k] = v.traffic
			}
			n.peersLock.Unlock()
			payload, _ := json.Marshal(map[string]any{
				"global": n.globalTraffic,
				"peers":  peerStats,
			})
			n.eventHub.Publish(&Event{Type: EvTrafficStats, Data: payload})
		}
	}
}

func (n *Node) listenLoop(ctx context.Context) {
	ln, err := quic.ListenAddr(n.cfg.ListenAddr, n.cfg.TLSConf, nil)
	if err != nil {
		fmt.Println("quic listen err:", err)
		return
	}
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		go n.handleConn(ctx, conn)
	}
}

func (n *Node) dialPeer(ctx context.Context, addr string) {
	conn, err := quic.DialAddr(ctx, addr, n.cfg.TLSConf, nil)
	if err != nil {
		fmt.Println("dial peer err:", err)
		return
	}
	n.handleConn(ctx, conn)
}

func (n *Node) handleConn(ctx context.Context, conn quic.Connection) {
	ctrlStream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "open ctrl stream fail")
		return
	}
	sess := NewPeerSession(ctx, conn, ctrlStream, n.cfg.Token, n.resumeSt, n.taskMgr, n.cfg.RecvDir, n.cfg.ThumbDir, n.eventHub, NewTokenBucket(n.cfg.PeerRateLimit), n.globalBucket, n.globalTraffic)
	if !sess.Handshake() {
		_ = conn.CloseWithError(1, "auth failed")
		return
	}
	remote := conn.RemoteAddr().String()
	n.peersLock.Lock()
	n.peers[remote] = sess
	n.peersLock.Unlock()

	raw, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{Type: EvPeerUp, From: remote, Data: raw})

	sess.Run()

	n.peersLock.Lock()
	delete(n.peers, remote)
	n.peersLock.Unlock()
	rawDown, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{Type: EvPeerDown, From: remote, Data: rawDown})
}

func (n *Node) BroadcastClipText(text string) error {
	frame := &codec.Frame{
		FrameType: codec.FrameClipText,
		BatchID:   RandomBatchID(),
		BodyLen:   uint64(len(text)),
		Flags:     0,
		Body:      []byte(text),
	}
	return n.broadcastFrame(frame)
}

func (n *Node) BroadcastClipImage(png []byte) error {
	frame := &codec.Frame{
		FrameType: codec.FrameClipImage,
		BatchID:   RandomBatchID(),
		BodyLen:   uint64(len(png)),
		Flags:     0,
		Body:      png,
	}
	return n.broadcastFrame(frame)
}

func (n *Node) BroadcastClipFiles(files []string) error {
	raw, err := json.Marshal(files)
	if err != nil {
		return err
	}
	frame := &codec.Frame{
		FrameType: codec.FrameClipFiles,
		BatchID:   RandomBatchID(),
		BodyLen:   uint64(len(raw)),
		Flags:     0,
		Body:      raw,
	}
	return n.broadcastFrame(frame)
}

func (n *Node) broadcastFrame(f *codec.Frame) error {
	n.peersLock.Lock()
	defer n.peersLock.Unlock()
	for _, s := range n.peers {
		_ = codec.WriteFrame(s.ctrlStr, f)
	}
	return nil
}

func RandomBatchID() [4]byte {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return b
}
