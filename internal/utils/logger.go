package utils

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fatih/color"
)

// 预定义颜色笔刷
var (
	ColorGreen  = color.New(color.FgGreen)
	ColorYellow = color.New(color.FgYellow)
	ColorRed    = color.New(color.FgRed)
	ColorBlue   = color.New(color.FgBlue)
	ColorGray   = color.New(color.FgHiBlack)
	ColorBold   = color.New(color.Bold)
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

type Logger struct {
	mu    sync.Mutex
	Level LogLevel
}

var GlobalLogger = &Logger{Level: DEBUG}

func SetLevel(level LogLevel) {
	GlobalLogger.Level = level
}

// logf 核心打印方法
func (l *Logger) logf(level LogLevel, prefix string, colorFunc *color.Color, format string, v ...interface{}) {
	if level < l.Level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf(format, v...)
	timestamp := time.Now().Format("15:04:05")
	
	// 关键：只给前缀 [INF] 上色，消息内容保持默认白色，除非你在 msg 里手动加了颜色
	coloredPrefix := colorFunc.Sprintf("[%s]", prefix)
	
	// 输出格式: [TIME] [PREFIX] Message
	fmt.Fprintf(os.Stdout, "[%s] %s %s\n", timestamp, coloredPrefix, msg)
}

func Debug(format string, v ...interface{}) {
	GlobalLogger.logf(DEBUG, "DBG", ColorGray, format, v...)
}

func Info(format string, v ...interface{}) {
	GlobalLogger.logf(INFO, "INF", ColorGreen, format, v...)
}

func Warn(format string, v ...interface{}) {
	GlobalLogger.logf(WARN, "WRN", ColorYellow, format, v...)
}

func Error(format string, v ...interface{}) {
	GlobalLogger.logf(ERROR, "ERR", ColorRed, format, v...)
}

// Helper: 用于在消息中高亮特定字段
func HighlightHex(data []byte) string {
	return ColorYellow.Sprintf("%x", data)
}

func HighlightText(text string) string {
	return ColorBlue.Sprintf("\"%s\"", text)
}