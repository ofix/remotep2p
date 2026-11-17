package node

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/quic-go/quic-go"
	"remotep2p/config"
	"remotep2p/internal/resume"
)

type Node struct {
	cfg       *config.Config
	resumeSt  *resume.Store
	taskMgr   *TaskManager
	peersLock sync.Mutex
	peers     map[string]*PeerSession
}

func NewNode(cfg *config.Config) *Node {
	return &Node{
		cfg:      cfg,
		resumeSt: resume.NewStore(cfg.ResumeDir),
		taskMgr:  NewTaskManager(),
		peers:    make(map[string]*PeerSession),
	}
}

func (n *Node) Start(ctx context.Context) error {
	go n.listenLoop(ctx)
	for _, addr := range n.cfg.Peers {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			go n.dialPeer(ctx, addr)
		}
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
	sess := NewPeerSession(ctx, conn, ctrlStream, n.cfg.Token, n.resumeSt, n.taskMgr, n.cfg.RecvDir, n.cfg.ThumbDir)
	if !sess.Handshake() {
		_ = conn.CloseWithError(1, "auth failed")
		return
	}
	n.peersLock.Lock()
	n.peers[conn.RemoteAddr().String()] = sess
	n.peersLock.Unlock()
	sess.Run()
}

func RandomBatchID() [4]byte {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return b
}
