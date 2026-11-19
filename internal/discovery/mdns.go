package discovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
)

// MdnsEventType mDNS事件类型
type MdnsEventType string

const (
	MdnsEventFound MdnsEventType = "found" // 发现匹配的rp2p节点
	MdnsEventLost  MdnsEventType = "lost"  // 节点下线
)

// MdnsEvent 向外输出事件，不耦合QUIC、Node逻辑
type MdnsEvent struct {
	Type      MdnsEventType
	TokenHash string
	QUICAddr  string
}

// Mdns mDNS服务：发布自身服务 + 局域网浏览发现
type Mdns struct {
	instanceName string
	tokenHash    string
	port         int

	server  *zeroconf.Server
	cancel  context.CancelFunc
	eventCh chan *MdnsEvent
	
	localIPSet map[string]bool 
	
	// 新增字段用于同步和状态管理
	mu        sync.Mutex      
	lastSeen  map[string]time.Time 
	closeOnce sync.Once       
}

// NewMdns 新建mDNS实例
func NewMdns(token string, quicPort int) (*Mdns, error) {
	h := sha256.New()
	h.Write([]byte(token))
	tokenHash := fmt.Sprintf("%x", h.Sum(nil))

	var rid [4]byte
	if _, err := rand.Read(rid[:]); err != nil {
		return nil, fmt.Errorf("generate random instance id: %w", err)
	}
	instanceName := "rp2p-" + fmt.Sprintf("%x", rid[:])
	localIPSet, err := collectLocalIPv4()
	if err != nil {
		return nil, err
	}
	return &Mdns{
		instanceName: instanceName,
		tokenHash:    tokenHash,
		port:         quicPort,
		eventCh:      make(chan *MdnsEvent, 16),
		localIPSet:   localIPSet,
		lastSeen:     make(map[string]time.Time),
	}, nil
}

func collectLocalIPv4() (map[string]bool, error) {
	set := make(map[string]bool)
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok || ipNet.IP.IsLoopback() {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 != nil {
				set[ip4.String()] = true
			}
		}
	}
	return set, nil
}

// Start 发布本机服务、启动浏览
func (m *Mdns) Start(ctx context.Context) error {
	txtRecords := []string{
		fmt.Sprintf("token_hash=%s", m.tokenHash),
	}

	server, err := zeroconf.Register(
		m.instanceName,
		"_rp2p._udp",
		"local.",
		m.port,
		txtRecords,
		nil,
	)
	if err != nil {
		return fmt.Errorf("zeroconf register fail: %w", err)
	}
	m.server = server
	fmt.Printf("[mdns] ✅ 自身服务发布成功, instance=%s, port=%d\n", m.instanceName, m.port)

	browseCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	entries := make(chan *zeroconf.ServiceEntry, 8)

	// 浏览协程
	go func() {
		resolver, err := zeroconf.NewResolver(nil)
		if err != nil {
			fmt.Printf("[mdns] ❌ resolver init err: %v\n", err)
			return
		}
		
		// Browse 是异步的，注册成功后立即返回
		if err := resolver.Browse(browseCtx, "_rp2p._udp", "local.", entries); err != nil {
			fmt.Printf("[mdns] ❌ browse err: %v\n", err)
			return
		}
		fmt.Println("[mdns] ✅ 浏览器已启动，等待局域网节点广播...")
		
		// 等待 ctx 取消，保持 goroutine 存活直到明确停止
		<-browseCtx.Done()
		fmt.Println("[mdns] 浏览协程退出")
	}()

	// 消费发现事件协程
	go func() {
		defer func() {
			// 当浏览上下文结束时，关闭事件通道
			// 注意：这里依赖 browseCtx  Done 来触发关闭，或者由 Stop() 显式关闭
			// 为了安全，我们在 Stop() 中统一关闭，这里只做循环退出
		}()
		
		for {
			select {
			case <-browseCtx.Done():
				return
			case entry := <-entries:
				m.handle(entry)
			}
		}
	}()
	
	// 启动定期清理协程，检测节点下线
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-browseCtx.Done():
				return
			case <-ticker.C:
				m.checkLostNodes()
			}
		}
	}()

	return nil
}

// handle 处理单个 mDNS 服务发现条目
func (m *Mdns) handle(entry *zeroconf.ServiceEntry) {
	// 1. 过滤自身
	if entry.Instance == m.instanceName {
		for _, ip := range entry.AddrIPv4 {
			if m.localIPSet[ip.String()] {
				return 
			}
		}
	}

	// 2. 解析 TXT 记录
	txt := parseTxtKV(entry.Text)
	peerTokenHash, ok := txt["token_hash"]
	if !ok {
		return
	}

	// 3. 校验 Token
	if peerTokenHash != m.tokenHash {
		return
	}

	// 4. 更新最后可见时间
	m.mu.Lock()
	m.lastSeen[entry.Instance] = time.Now()
	m.mu.Unlock()

	// 5. 构建对端地址
	if len(entry.AddrIPv4) == 0 {
		return
	}
	
	ip := entry.AddrIPv4[0].String()
	// 使用 entry.Port 而不是 m.port
	peerQUICAddr := net.JoinHostPort(ip, strconv.Itoa(entry.Port))

	// 6. 发送 Found 事件 (非阻塞)
	select {
	case m.eventCh <- &MdnsEvent{
		Type:      MdnsEventFound,
		QUICAddr:  peerQUICAddr,
		TokenHash: peerTokenHash,
	}:
	default:
		// 通道满则丢弃，避免阻塞
	}
}

func (m *Mdns) checkLostNodes() {
	now := time.Now()
	timeout := 30 * time.Second 
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	for instance, lastTime := range m.lastSeen {
		if now.Sub(lastTime) > timeout {
			delete(m.lastSeen, instance)
			
			// 发送 Lost 事件 (非阻塞)
			select {
			case m.eventCh <- &MdnsEvent{
				Type:      MdnsEventLost,
				TokenHash: m.tokenHash, 
			}:
			default:
			}
		}
	}
}

// Events 返回只读事件通道
func (m *Mdns) Events() <-chan *MdnsEvent {
	return m.eventCh
}

// Stop 停止发布、停止浏览、关闭通道
func (m *Mdns) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	if m.server != nil {
		m.server.Shutdown()
	}
	
	m.closeOnce.Do(func() {
		close(m.eventCh)
	})
}

func parseTxtKV(txt []string) map[string]string {
	out := make(map[string]string)
	for _, s := range txt {
		kv := strings.SplitN(s, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}