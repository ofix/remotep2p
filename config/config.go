package config

import (
	"crypto/tls"
)

type Config struct {
	ListenAddr string
	Peers      []string
	Token      string
	RecvDir    string
	CSVPath    string
	ResumeDir  string
	ThumbDir   string
	HTTPSAddr  string
	TLSConf    *tls.Config
}
