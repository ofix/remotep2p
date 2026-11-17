package web

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io/fs"
	"math/big"
	"net/http"
	"time"

	"remotep2p/config"
)

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
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
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
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"hq2"}}, nil
}

func StartHTTPS(cfg *config.Config) {
	mux := http.NewServeMux()
	sub, err := fs.Sub(UIAssets, "ui")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}
	srv := http.Server{
		Addr:      cfg.HTTPSAddr,
		TLSConfig: cfg.TLSConf,
		Handler:   mux,
	}
	_ = srv.ListenAndServeTLS("", "")
}
