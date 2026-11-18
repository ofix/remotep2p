package discovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net"
	"strconv"
	"strings"

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
}

// NewMdns 新建mDNS实例
// token: 验证密钥
// quicPort: QUIC监听端口，例如8443
func NewMdns(token string, quicPort int) (*Mdns, error) {
	h := sha256.New()
	h.Write([]byte(token))
	tokenHash := fmt.Sprintf("%x", h.Sum(nil))

	var rid [4]byte
	if _, err := rand.Read(rid[:]); err != nil {
		return nil, fmt.Errorf("generate random instance id: %w", err)
	}
	instanceName := "rp2p-" + fmt.Sprintf("%x", rid[:])

	return &Mdns{
		instanceName: instanceName,
		tokenHash:    tokenHash,
		port:         quicPort,
		eventCh:      make(chan *MdnsEvent, 16),
	}, nil
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
		fmt.Println("[mdns] ✅ 浏览器已启动，等待局域网节点广播...")
		err = resolver.Browse(browseCtx, "_rp2p._udp", "local.", entries)
		if err != nil {
			fmt.Printf("[mdns] ❌ browse err: %v\n", err)
		}
		fmt.Println("[mdns] 浏览协程退出")
	}()

	// 消费发现事件协程
	go func() {
		for entry := range entries {
			fmt.Printf("[mdns raw] 收到广播包 | instance=%s | IPv4=%v | txt=%v\n",
				entry.Instance, entry.AddrIPv4, entry.Text)

			// 过滤自己
			if entry.Instance == m.instanceName {
				fmt.Println("[mdns] ↩️ 跳过自己")
				continue
			}
			txt := parseTxtKV(entry.Text)
			peerTokenHash, ok := txt["token_hash"]
			if !ok {
				fmt.Println("[mdns] ⚠️ 广播包缺少token_hash，忽略")
				continue
			}
			// token hash不匹配直接跳过
			if peerTokenHash != m.tokenHash {
				fmt.Printf("[mdns] ⚠️ token_hash不匹配，本地:%s 对端:%s\n", m.tokenHash, peerTokenHash)
				continue
			}

			if len(entry.AddrIPv4) == 0 {
				// 下线事件：缺少IP
				m.eventCh <- &MdnsEvent{
					Type:      MdnsEventLost,
					QUICAddr:  "",
					TokenHash: peerTokenHash,
				}
				fmt.Println("[mdns] 📌 收到节点下线事件")
				continue
			}
			ip := entry.AddrIPv4[0].String()
			peerQUICAddr := net.JoinHostPort(ip, strconv.Itoa(m.port))

			m.eventCh <- &MdnsEvent{
				Type:      MdnsEventFound,
				QUICAddr:  peerQUICAddr,
				TokenHash: peerTokenHash,
			}
			fmt.Printf("[mdns] 🎯 发现对端节点！QUIC地址=%s\n", peerQUICAddr)
		}
		fmt.Println("[mdns] entries通道关闭，事件消费协程退出")
	}()

	return nil
}

// Events 返回只读事件通道，给Node订阅
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
		fmt.Println("[mdns] 服务注销，发送goodbye下线包")
	}
	close(m.eventCh)
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
