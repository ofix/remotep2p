package web

import (
	"embed"
)

//go:embed ui/*
var UIAssets embed.FS
