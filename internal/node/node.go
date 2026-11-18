package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"remotep2p/internal/codec"
	"remotep2p/internal/config"
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

	// 静态Peer
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

	// Zeroconf mDNS
	if n.cfg.EnableZeroconf {
		h := sha256.New()
		h.Write([]byte(n.cfg.Token))
		tokenHash := fmt.Sprintf("%x", h.Sum(nil))

		_, portStr, err := net.SplitHostPort(n.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("parse listen addr: %w", err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("parse port: %w", err)
		}

		rid := RandomBatchID()
		instanceName := "rp2p-" + fmt.Sprintf("%x", rid[:])
		txtRecords := []string{
			fmt.Sprintf("token_hash=%s", tokenHash),
			fmt.Sprintf("quic_addr=%s", n.cfg.ListenAddr),
		}

		server, err := zeroconf.Register(
			instanceName,
			"_rp2p._udp",
			"local.",
			port,
			txtRecords,
			nil,
		)
		if err != nil {
			return fmt.Errorf("zeroconf register failed: %w", err)
		}
		n.zcServer = server

		browseCtx, cancel := context.WithCancel(context.Background())
		n.zcCancel = cancel

		entries := make(chan *zeroconf.ServiceEntry)
		go func() {
			resolver, err := zeroconf.NewResolver(nil)
			if err != nil {
				fmt.Printf("[zeroconf] new resolver err: %v\n", err)
				return
			}
			err = resolver.Browse(browseCtx, "_rp2p._udp", "local.", entries)
			if err != nil {
				fmt.Printf("[zeroconf] browse err: %v\n", err)
			}
		}()

		go func() {
			for entry := range entries {
				txt := parseTxtKV(entry.Text)
				peerTokenHash, ok := txt["token_hash"]
				if !ok {
					continue
				}
				peerQUICAddr, ok := txt["quic_addr"]
				if !ok {
					continue
				}
				if peerTokenHash != tokenHash {
					continue
				}
				if peerQUICAddr == n.cfg.ListenAddr {
					continue
				}
				if len(entry.AddrIPv4) == 0 {
					fmt.Printf("[zeroconf] peer gone: %s\n", peerQUICAddr)
					continue
				}

				fmt.Printf("[zeroconf] discovered peer: %s\n", peerQUICAddr)
				go func(a string) {
					_, err := n.DialPeerIfNotExistWithSource(a, "zeroconf")
					if err != nil {
						fmt.Printf("[zeroconf] dial %s err: %v\n", a, err)
					}
				}(peerQUICAddr)
			}
		}()
	}

	return nil
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

func (n *Node) handleConn(ctx context.Context, conn quic.Connection) {
	ctrlStream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "open ctrl stream fail")
		return
	}
	sess := NewPeerSession(ctx, conn, ctrlStream, n.cfg.Token, n.resumeStore, n.taskMgr,
		n.cfg.RecvDir, n.cfg.ThumbDir, n.eventHub,
		NewTokenBucket(n.cfg.PeerRateLimit), n.globalBucket, n.globalTraffic)
	if !sess.Handshake() {
		_ = conn.CloseWithError(1, "auth failed")
		return
	}
	remote := conn.RemoteAddr().String()

	n.peersLock.Lock()
	n.peers[remote] = sess
	n.peersLock.Unlock()

	n.discoveryLock.Lock()
	n.discoveredPeers[remote] = PeerMetaInfo{
		Addr:   remote,
		Source: "incoming",
	}
	n.discoveryLock.Unlock()

	rawUp, _ := json.Marshal(map[string]string{"addr": remote})
	n.eventHub.Publish(&Event{
		Type: EvPeerUp,
		From: remote,
		Data: json.RawMessage(rawUp),
	})
	n.publishPeerDiscovery(remote, "incoming")

	sess.Run()

	n.peersLock.Lock()
	delete(n.peers, remote)
	n.peersLock.Unlock()
	n.discoveryLock.Lock()
	delete(n.discoveredPeers, remote)
	n.discoveryLock.Unlock()

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
	n.peersLock.Lock()
	n.peers[remote] = sess
	n.peersLock.Unlock()

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
