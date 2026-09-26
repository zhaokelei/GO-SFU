// logger.go
// 分级日志模块（INFO/WARN/ERROR）
// 全局变量 log 供其他文件使用，格式如 log.Infof("...")
//
// 【重要设计说明】——为解决「必须先在服务器黑框按回车才能连接、通话两秒卡死」的问题
//
// Windows 控制台的「快速编辑模式」会在鼠标点击黑框后进入文本选择状态，
// 此时进程写 stdout/stderr 会被操作系统阻塞挂起。若日志是同步写控制台，
// 整个 SFU 就会被日志一起卡死（信令不响应、媒体不转发）。
//
// 因此本模块采用「异步非阻塞」日志：
//   - 业务线程调用 Infof/Warnf/Errorf 时只把日志投递进带缓冲的 channel，
//     投递永不阻塞（队列满时直接丢弃并计数），保证音视频转发与信令不被日志拖住；
//   - 后台 goroutine 负责真正写入；控制台与日志文件各有独立的后台队列，
//     控制台被冻结时日志文件仍然完整写入，方便事后排查。
//
// 另外 console_windows.go 会在启动时直接关闭控制台的快速编辑模式，从根源上避免冻结。

package main

import (
	"io"
	stdlog "log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// logQueueSize 每个输出目标（控制台 / 文件）的异步日志队列长度
const logQueueSize = 4096

// ============================================================
// 异步非阻塞写入器
// ============================================================

// asyncWriter 异步写入器：实现 io.Writer，Write 仅做入队，绝不阻塞调用方
type asyncWriter struct {
	ch     chan []byte   // 日志缓冲队列
	inner  io.Writer     // 真正的底层写入目标（控制台或文件）
	done   chan struct{} // 后台协程退出信号
	closed uint32        // 是否已关闭（1=已关闭）
	drops  uint64        // 因队列满而丢弃的日志条数
}

// newAsyncWriter 创建异步写入器并启动后台写协程
func newAsyncWriter(inner io.Writer, bufSize int) *asyncWriter {
	aw := &asyncWriter{
		ch:    make(chan []byte, bufSize),
		inner: inner,
		done:  make(chan struct{}),
	}
	go aw.run()
	return aw
}

// run 后台协程：持续把队列中的日志写入底层目标
func (aw *asyncWriter) run() {
	defer close(aw.done)
	for b := range aw.ch {
		_, _ = aw.inner.Write(b)
	}
}

// Write 实现 io.Writer：拷贝后入队，队列满则丢弃，永不阻塞
func (aw *asyncWriter) Write(p []byte) (int, error) {
	if atomic.LoadUint32(&aw.closed) == 1 {
		return len(p), nil
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	select {
	case aw.ch <- cp:
	default:
		// 队列已满（例如控制台被长时间冻结），丢弃本条日志并计数，
		// 宁可丢日志也绝不阻塞业务线程
		atomic.AddUint64(&aw.drops, 1)
	}
	return len(p), nil
}

// close 关闭写入器并等待后台协程排空队列（带超时，避免底层写入被冻结时永久等待）
func (aw *asyncWriter) close(timeout time.Duration) {
	if !atomic.CompareAndSwapUint32(&aw.closed, 0, 1) {
		return
	}
	close(aw.ch)
	select {
	case <-aw.done:
	case <-time.After(timeout):
	}
}

// ============================================================
// 分级日志器
// ============================================================

// levelLogger 同一日志级别的多个输出目标（控制台 + 文件）
type levelLogger struct {
	targets []*stdlog.Logger
}

// Printf 依次写入所有输出目标
func (ll *levelLogger) Printf(format string, args ...interface{}) {
	for _, t := range ll.targets {
		t.Printf(format, args...)
	}
}

// Logger 分级日志器
type Logger struct {
	info    *levelLogger
	warn    *levelLogger
	err     *levelLogger
	level   int            // 0=INFO 输出所有, 1=WARN 只输出WARN+ERROR, 2=ERROR 只输出ERROR
	writers []*asyncWriter // 所有异步写入器，退出时统一刷新
	file    *os.File       // 日志文件句柄（未启用文件时为 nil）
	mu      sync.Mutex
	closed  bool
}

// log 全局日志实例（包级变量，其他文件直接使用 log.Infof 等）
var log *Logger

// initLogger 初始化全局日志器
// level：日志级别 INFO/WARN/ERROR
// logFile：日志文件路径，为空则只输出到控制台
func initLogger(level string, logFile string) {
	lvl := 0
	switch level {
	case "INFO":
		lvl = 0
	case "WARN":
		lvl = 1
	case "ERROR":
		lvl = 2
	default:
		lvl = 0
	}

	// 控制台异步写入器（底层为 os.Stdout）
	console := newAsyncWriter(os.Stdout, logQueueSize)
	writers := []*asyncWriter{console}
	targets := []io.Writer{console}

	// 日志文件异步写入器（可选，独立队列，不受控制台冻结影响）
	var f *os.File
	if logFile != "" {
		opened, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			f = opened
			fw := newAsyncWriter(f, logQueueSize)
			writers = append(writers, fw)
			targets = append(targets, fw)
		}
	}

	// 为每个级别构建「控制台 + 文件」双目标日志器
	build := func(prefix string) *levelLogger {
		var ls []*stdlog.Logger
		for _, w := range targets {
			ls = append(ls, stdlog.New(w, prefix, stdlog.LstdFlags|stdlog.Lmicroseconds))
		}
		return &levelLogger{targets: ls}
	}

	log = &Logger{
		info:    build("[INFO]  "),
		warn:    build("[WARN]  "),
		err:     build("[ERROR] "),
		level:   lvl,
		writers: writers,
		file:    f,
	}
}

// Infof 打印 INFO 级别日志
func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level <= 0 {
		l.info.Printf(format, args...)
	}
}

// Warnf 打印 WARN 级别日志
func (l *Logger) Warnf(format string, args ...interface{}) {
	if l.level <= 1 {
		l.warn.Printf(format, args...)
	}
}

// Errorf 打印 ERROR 级别日志
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.err.Printf(format, args...)
}

// Close 刷新并关闭日志（程序退出前调用，确保已入队日志尽量落盘）
func (l *Logger) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.mu.Unlock()

	// 依次关闭各个异步写入器（带超时）
	for _, w := range l.writers {
		w.close(2 * time.Second)
	}
	if l.file != nil {
		_ = l.file.Close()
	}
}
