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
	quicAddr     string

	server  *zeroconf.Server
	cancel  context.CancelFunc
	eventCh chan *MdnsEvent
}

// NewMdns 新建mDNS实例
// token: 验证密钥
// listenQUICAddr: 本机监听地址，格式 "0.0.0.0:8443"
func NewMdns(token string, listenQUICAddr string) (*Mdns, error) {
	h := sha256.New()
	h.Write([]byte(token))
	tokenHash := fmt.Sprintf("%x", h.Sum(nil))

	_, portStr, err := net.SplitHostPort(listenQUICAddr)
	if err != nil {
		return nil, fmt.Errorf("split host port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("parse port: %w", err)
	}

	var rid [4]byte
	if _, err := rand.Read(rid[:]); err != nil {
		return nil, fmt.Errorf("generate random instance id: %w", err)
	}
	instanceName := "rp2p-" + fmt.Sprintf("%x", rid[:])

	return &Mdns{
		instanceName: instanceName,
		tokenHash:    tokenHash,
		port:         port,
		quicAddr:     listenQUICAddr,
		eventCh:      make(chan *MdnsEvent, 16),
	}, nil
}

// Start 发布本机服务、启动浏览
func (m *Mdns) Start(ctx context.Context) error {
	txtRecords := []string{
		fmt.Sprintf("token_hash=%s", m.tokenHash),
		fmt.Sprintf("quic_addr=%s", m.quicAddr),
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
		return err
	}
	m.server = server

	browseCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	entries := make(chan *zeroconf.ServiceEntry)

	// 浏览协程
	go func() {
		resolver, err := zeroconf.NewResolver(nil)
		if err != nil {
			fmt.Printf("[mdns] resolver init err: %v\n", err)
			return
		}
		err = resolver.Browse(browseCtx, "_rp2p._udp", "local.", entries)
		if err != nil {
			fmt.Printf("[mdns] browse err: %v\n", err)
		}
	}()

	// 消费发现事件协程
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
			// 过滤自己
			if peerQUICAddr == m.quicAddr {
				continue
			}
			// token hash不匹配直接跳过
			if peerTokenHash != m.tokenHash {
				continue
			}

			if len(entry.AddrIPv4) == 0 {
				m.eventCh <- &MdnsEvent{
					Type:      MdnsEventLost,
					QUICAddr:  peerQUICAddr,
					TokenHash: peerTokenHash,
				}
				continue
			}

			m.eventCh <- &MdnsEvent{
				Type:      MdnsEventFound,
				QUICAddr:  peerQUICAddr,
				TokenHash: peerTokenHash,
			}
		}
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
