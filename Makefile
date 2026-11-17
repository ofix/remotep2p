# RemoteP2P Makefile
# Windows / Linux arm64: CGO_ENABLED=0 static
# macOS: CGO_ENABLED=1 for full Cocoa clipboard

.PHONY: all tidy win linux-arm64 darwin-amd64 darwin-arm64 clean

all: tidy win linux-arm64 darwin-amd64 darwin-arm64

tidy:
	go mod tidy

win:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o build/remotep2p.exe

linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o build/remotep2p-kylin-arm64

darwin-amd64:
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o build/remotep2p-darwin-amd64

darwin-arm64:
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o build/remotep2p-darwin-arm64

clean:
	rm -rf build recv resume thumb history.csv
