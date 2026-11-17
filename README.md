# RemoteP2P
QUIC P2P 跨平台传输工具
✅ 特性
- 对等节点架构，无中心化服务端
- 二进制帧协议 + Zstd 流式压缩
- 断点续传 + 偏移持久化
- SHA256 文件完整性校验
- 图片自动缩略图缓存
- 内置 WebUI（go:embed），剪贴板发送、任务面板、平台能力提示
- CSV 传输历史记录
- 剪贴板：文本 / 图片 / 文件列表跨设备同步
- 跨平台：Win11 x86_64、银河麒麟V10 arm64、macOS amd64/arm64、Linux

📌 编译规则
- Windows / 麒麟 arm64：CGO_ENABLED=0，静态独立二进制
- macOS 默认 CGO_ENABLED=1：支持 Cocoa NSPasteboard（图片+文件剪贴板）；可加 tag purego 只支持文本剪贴板

📦 编译
make win
make linux-arm64
make darwin-arm64

🚀 启动示例
./remotep2p -listen :8443 -token abc123 -recv ./recv
./remotep2p -listen :8443 -peer 192.168.1.10:8443 -token abc123
