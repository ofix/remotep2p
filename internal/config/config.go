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

	// 限速：单位 Byte/s；0=不限速
	GlobalRateLimit int64 `json:"global_rate_limit"`
	PeerRateLimit   int64 `json:"peer_rate_limit"`
	EnableZeroconf bool `json:"enable_zeroconf"`
}
