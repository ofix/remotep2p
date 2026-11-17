package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"remotep2p/internal/config"
	"remotep2p/internal/node"
	"remotep2p/internal/web"
)

func main() {
	var (
		listen    = flag.String("listen", ":8443", "quic listen addr")
		peerList  = flag.String("peer", "", "peer addr, comma split")
		token     = flag.String("token", "default123", "auth token")
		recvDir   = flag.String("recv", "./recv", "receive dir")
		csvPath   = flag.String("csv", "./history.csv", "history csv path")
		resumeDir = flag.String("resume", "./resume", "resume meta dir")
		thumbDir  = flag.String("thumb", "./thumb", "thumbnail cache dir")
		httpsAddr = flag.String("https", ":8444", "web https listen")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	tlsConf, err := web.GenSelfSignedTLS()
	if err != nil {
		panic(err)
	}
	var peers []string
	if *peerList != "" {
		for _, s := range strings.Split(*peerList, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				peers = append(peers, s)
			}
		}
	}
	cfg := &config.Config{
		ListenAddr: *listen,
		Peers:      peers,
		Token:      *token,
		RecvDir:    *recvDir,
		CSVPath:    *csvPath,
		ResumeDir:  *resumeDir,
		ThumbDir:   *thumbDir,
		HTTPSAddr:  *httpsAddr,
		TLSConf:    tlsConf,
	}

	n := node.NewNode(cfg)
	web.SetNode(n)
	go web.StartHTTPS(cfg)

	if err := n.Start(ctx); err != nil {
		fmt.Println("node start err:", err)
	}
	<-ctx.Done()
	fmt.Println("exit")
}
