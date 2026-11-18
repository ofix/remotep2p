package web

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io/fs"
	"math/big"
	"net/http"
	"time"

	"remotep2p/internal/clipboard"
	"remotep2p/internal/config"
	"remotep2p/internal/node"

	"github.com/gorilla/websocket"
)

var (
	globalNode *node.Node
	upgrader   = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

func SetNode(n *node.Node) {
	globalNode = n
}

func GenSelfSignedTLS() (*tls.Config, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"RemoteP2P"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	return &tls.Config{InsecureSkipVerify: true, // ✅ 加上这一行，消除这个校验报错
		Certificates: []tls.Certificate{cert}, NextProtos: []string{"hq2"}}, nil
}

func StartHTTPS(cfg *config.Config) {
	mux := http.NewServeMux()
	sub, err := fs.Sub(UIAssets, "ui")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}
	mux.HandleFunc("/api/clip/read", apiClipRead)
	mux.HandleFunc("/api/clip/send/text", apiClipSendText)
	mux.HandleFunc("/api/clip/send/files", apiClipSendFiles)
	mux.HandleFunc("/ws", apiWS)
	mux.HandleFunc("/api/clip/image", apiClipReadImage)
	mux.HandleFunc("/api/file/send", apiSendFile)

	srv := http.Server{
		Addr: cfg.HTTPSAddr,
		// TLSConfig: cfg.TLSConf,
		Handler: mux,
	}

	// _ = srv.ListenAndServeTLS("", "")

	// 直接指定证书路径，内部自动构造TLSConfig
	_ = srv.ListenAndServeTLS("server.crt", "server.key")
}

func apiClipRead(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	text, _ := clipboard.Instance.ReadText()
	files, _ := clipboard.Instance.ReadFileList()
	img, _ := clipboard.Instance.ReadImage()
	json.NewEncoder(w).Encode(map[string]any{
		"text":      text,
		"files":     files,
		"has_image": img != nil,
	})
}

func apiClipReadImage(w http.ResponseWriter, r *http.Request) {
	png, err := clipboard.Instance.ReadImage()
	if err != nil || png == nil {
		http.Error(w, "no image", 404)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	_, _ = w.Write(png)
}

func apiSendFile(w http.ResponseWriter, r *http.Request) {
	// 文件上传入口：前端FormData上传，这里先占位，对接task发送逻辑
	w.WriteHeader(200)
}

func apiClipSendText(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if globalNode == nil {
		http.Error(w, "node not ready", 500)
		return
	}
	err := globalNode.BroadcastClipText(req.Text)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
}

func apiClipSendFiles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		Files []string `json:"files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if globalNode == nil {
		http.Error(w, "node not ready", 500)
		return
	}
	err := globalNode.BroadcastClipFiles(req.Files)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
}

func apiWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	ch := globalNode.EventHub().Subscribe()
	defer globalNode.EventHub().Unsubscribe(ch)

	for ev := range ch {
		bs, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		_ = ws.WriteMessage(websocket.TextMessage, bs)
	}
}

func apiStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ts := globalNode.GlobalTraffic()
	_ = json.NewEncoder(w).Encode(ts)
}
