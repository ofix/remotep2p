package node

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"remotep2p/internal/codec"
	"remotep2p/internal/config"
	"remotep2p/internal/discovery"
	"remotep2p/internal/resume"

	"github.com/grandcat/zeroconf"
	"github.com/quic-go/quic-go"
)

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
	EvPeerDiscovery = "discovery_peer"
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

type TokenBucket struct {
	mu       sync.Mutex
	cap      int64
	rate     int64
	tokens   int64
	lastFill time.Time
}

type TrafficStat struct {
	mu sync.Mutex
	Rx int64 `json:"rx"`
	Tx int64 `json:"tx"`
}

// PeerMetaInfo 仅用来记录peer发现来源，不侵入PeerSession
type PeerMetaInfo struct {
	Addr   string `json:"addr"`
	Source string `json:"source"` // static / zeroconf / incoming
}

type Node struct {
	cfg             *config.Config
	resumeStore     *resume.Store
	taskMgr         *TaskManager
	eventHub        *EventHub
	peersLock       sync.Mutex
	peers           map[string]*PeerSession
	discoveryLock   sync.Mutex
	discoveredPeers map[string]PeerMetaInfo
	globalBucket    *TokenBucket
	globalTraffic   *TrafficStat

	zcServer *zeroconf.Server
	zcCancel context.CancelFunc
	mdns     *discovery.Mdns // 新增，外部mdns实例
}

func NewNode(cfg *config.Config) *Node {
	return &Node{
		cfg:             cfg,
		resumeStore:     resume.NewStore(cfg.ResumeDir),
		taskMgr:         NewTaskManager(),
		eventHub:        NewEventHub(),
		peers:           make(map[string]*PeerSession),
		discoveredPeers: make(map[string]PeerMetaInfo),
		globalBucket:    NewTokenBucket(cfg.GlobalRateLimit),
		globalTraffic:   &TrafficStat{},
	}
}

func (ts *TrafficStat) AddRx(n int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Rx += n
}

func (ts *TrafficStat) AddTx(n int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Tx += n
}

func (n *Node) Start(ctx context.Context) error {
	go n.listenLoop(ctx)
	go n.trafficReportLoop(ctx)

	// 静态Peer 保留在Node内部，这部分属于Node启动拨号逻辑
	for _, addr := range n.cfg.Peers {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			go func(a string) {
				_, err := n.DialPeerIfNotExistWithSource(a, "static")
				if err != nil {
					fmt.Printf("[static] dial %s err: %v\n", a, err)
				}
			}(addr)
		}
	}

	// mDNS 交给外部 discovery.Mdns，Node只订阅事件
	if n.cfg.EnableZeroconf {
		mdnsSvc, err := discovery.NewMdns(n.cfg.Token, n.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("init mdns: %w", err)
		}
		if err := mdnsSvc.Start(ctx); err != nil {
			return fmt.Errorf("start mdns: %w", err)
		}
		// 保存到node，方便Stop
		n.mdns = mdnsSvc

		// 单独协程消费mdns事件
		go func() {
			for evt := range mdnsSvc.Events() {
				switch evt.Type {
				case discovery.MdnsEventFound:
					fmt.Printf("[zeroconf] discovered peer: %s\n", evt.QUICAddr)
					go func(a string) {
						_, err := n.DialPeerIfNotExistWithSource(a, "zeroconf")
						if err != nil {
							fmt.Printf("[zeroconf] dial %s err: %v\n", a, err)
						}
					}(evt.QUICAddr)
				case discovery.MdnsEventLost:
					fmt.Printf("[zeroconf] peer lost: %s\n", evt.QUICAddr)
					// 可选：标记discoveredPeers为下线，由Dial逻辑判断重连
				}
			}
		}()
	}

	return nil
}

// SendFileToPeer 发送文件到指定远端peer地址
func (n *Node) SendFileToPeer(remoteAddr string, filePath string, compress bool) error {
	n.peersLock.Lock()
	sess, ok := n.peers[remoteAddr]
	n.peersLock.Unlock()
	if !ok {
		return fmt.Errorf("peer %s not connected", remoteAddr)
	}
	return sess.SendFile(filePath, compress)
}

// SendFileToAllPeers 广播发送给所有已连接peer
func (n *Node) SendFileToAllPeers(filePath string, compress bool) {
	n.peersLock.Lock()
	// 拷贝快照，避免长时间持有锁
	all := make([]*PeerSession, 0, len(n.peers))
	for _, v := range n.peers {
		all = append(all, v)
	}
	n.peersLock.Unlock()

	for _, sess := range all {
		s := sess
		go func() {
			if err := s.SendFile(filePath, compress); err != nil {
				log.Printf("broadcast send file err: %v", err)
			}
		}()
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

func parseTxtKV(txt []string) map[string]string {
	out := make(map[string]string)
	for _, s := range txt {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) == 2 {
			out[parts[0]] = parts[1]
		}
	}
	return out
}

func (n *Node) addPeer(remoteAddr string, sess *PeerSession) {
	n.peersLock.Lock()
	n.peers[remoteAddr] = sess
	n.peersLock.Unlock()
}

func (n *Node) removePeer(remoteAddr string) {
	n.peersLock.Lock()
	delete(n.peers, remoteAddr)
	n.peersLock.Unlock()
}

func (n *Node) handleConn(ctx context.Context, conn quic.Connection) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("handleConn panic recovered: %v", r)
			_ = conn.CloseWithError(99, "panic")
		}
	}()

	// incoming 连接：必须 AcceptStream，而不是 OpenStreamSync
	ctrlStream, err := conn.AcceptStream(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "accept ctrl stream fail")
		return
	}

	sess := NewPeerSession(ctx, conn, ctrlStream, n.cfg.Token, n.resumeStore, n.taskMgr,
		n.cfg.RecvDir, n.cfg.ThumbDir, n.eventHub,
		NewTokenBucket(n.cfg.PeerRateLimit), n.globalBucket, n.globalTraffic)

	// 握手超时保护
	hsCtx, hsCancel := context.WithTimeout(ctx, 5*time.Second)
	defer hsCancel()
	handshakeOK := make(chan bool, 1)
	go func() {
		handshakeOK <- sess.Handshake()
	}()
	select {
	case ok := <-handshakeOK:
		if !ok {
			_ = conn.CloseWithError(1, "auth failed")
			return
		}
	case <-hsCtx.Done():
		_ = conn.CloseWithError(2, "handshake timeout")
		return
	}

	remote := conn.RemoteAddr().String()

	n.addPeer(remote, sess)

	rawUp, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{
		Type: EvPeerUp,
		From: remote,
		Data: json.RawMessage(rawUp),
	})
	n.publishPeerDiscovery(remote, "incoming")

	// 阻塞运行会话，直到连接断开
	sess.Run()

	// 会话退出后清理在线peer
	n.removePeer(remote)

	rawDown, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{
		Type: EvPeerDown,
		From: remote,
		Data: json.RawMessage(rawDown),
	})
	n.publishPeerDown(remote)
}

func (n *Node) dialPeer(ctx context.Context, addr string) (*PeerSession, error) {
	conn, err := quic.DialAddr(ctx, addr, n.cfg.TLSConf, nil)
	if err != nil {
		fmt.Println("dial peer err:", err)
		return nil, err
	}
	ctrlStream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "open ctrl stream fail")
		return nil, err
	}
	sess := NewPeerSession(ctx, conn, ctrlStream, n.cfg.Token, n.resumeStore, n.taskMgr,
		n.cfg.RecvDir, n.cfg.ThumbDir, n.eventHub,
		NewTokenBucket(n.cfg.PeerRateLimit), n.globalBucket, n.globalTraffic)
	if !sess.Handshake() {
		_ = conn.CloseWithError(1, "auth failed")
		return nil, fmt.Errorf("handshake failed")
	}
	remote := conn.RemoteAddr().String()
	n.addPeer(remote, sess)

	rawUp, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{
		Type: EvPeerUp,
		From: remote,
		Data: json.RawMessage(rawUp),
	})
	go sess.Run()
	return sess, nil
}

func (n *Node) DialPeerIfNotExistWithSource(addr, source string) (*PeerSession, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("empty addr")
	}
	n.peersLock.Lock()
	_, exists := n.peers[addr]
	n.peersLock.Unlock()
	if exists {
		return nil, fmt.Errorf("peer already exists")
	}
	sess, err := n.dialPeer(context.Background(), addr)
	if err == nil {
		n.discoveryLock.Lock()
		n.discoveredPeers[addr] = PeerMetaInfo{
			Addr:   addr,
			Source: source,
		}
		n.discoveryLock.Unlock()
		n.publishPeerDiscovery(addr, source)
	}
	return sess, err
}

func (n *Node) Stop() {
	if n.zcServer != nil {
		n.zcServer.Shutdown()
	}
	if n.zcCancel != nil {
		n.zcCancel()
	}
	if n.mdns != nil {
		n.mdns.Stop()
	}
}

func (n *Node) EventHub() *EventHub { return n.eventHub }

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

func (n *Node) publishPeerDiscovery(addr, source string) {
	payload, _ := json.Marshal(map[string]any{
		"addr":   addr,
		"source": source,
	})
	n.eventHub.Publish(&Event{
		Type: EvPeerDiscovery,
		Data: json.RawMessage(payload),
	})
}

func (n *Node) publishPeerDown(addr string) {
	payload, _ := json.Marshal(map[string]any{"addr": addr})
	n.eventHub.Publish(&Event{
		Type: EvPeerDown,
		Data: json.RawMessage(payload),
	})
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
		if err := s.WriteFrame(f); err != nil {
			fmt.Printf("broadcast write frame err: %v\n", err)
		}
	}
	return nil
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

func (n *Node) GlobalTraffic() *TrafficStat { return n.globalTraffic }

func (n *Node) trafficReportLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
			n.eventHub.Publish(&Event{
				Type: EvTrafficStats,
				Data: json.RawMessage(payload),
			})
		}
	}
}

func RandomBatchID() [4]byte {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return b
}
