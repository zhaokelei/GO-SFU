// console_windows.go
// Windows 专用：控制台「快速编辑模式」自动关闭
//
// 背景（本次卡死问题的根因）：
//   Windows 控制台（cmd.exe / PowerShell）默认开启「快速编辑模式(QuickEdit Mode)」。
//   一旦用户在服务器黑框里点了一下鼠标，控制台就进入「文本选择」状态，
//   此时进程向 stdout / stderr 的任何一次写入都会被操作系统阻塞挂起。
//   由于 SFU 的日志是同步写控制台的，写入被卡住就会连锁冻结整个进程：
//     - 前端点击「连接」后信令无法被处理，表现为「连不上」；
//     - 在控制台里按一下回车，选择状态被取消，写入立刻恢复，
//       于是表现为「按回车瞬间连接成功」；
//     - 通话过程中黑框被点一下，媒体转发线程被日志卡住，表现为「两秒就卡死」。
//
// 解决方案：进程启动时调用 Windows API 关闭标准输入的快速编辑模式，
//           从根源上让「点击控制台」不再冻结进程。
//
// 本文件通过 build tag 只在 Windows 下编译，使用 syscall 直接调用 kernel32，
// 不引入任何第三方依赖，也不影响 CGO_ENABLED=0 的静态编译。

//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

// Windows 控制台相关常量与 API
var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	procGetStdHandle   = kernel32.NewProc("GetStdHandle")
	procGetConsoleMode = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode = kernel32.NewProc("SetConsoleMode")
)

const (
	// 标准输入句柄编号：STD_INPUT_HANDLE = -10
	stdInputHandle = ^uintptr(9)
	// ENABLE_QUICK_EDIT_MODE：快速编辑模式（鼠标选择会阻塞输出），需要清除
	enableQuickEditMode = 0x0040
	// ENABLE_INSERT_MODE：插入模式，顺带清除，避免选择状态残留
	enableInsertMode = 0x0020
	// ENABLE_EXTENDED_FLAGS：修改扩展标志位时必须同时置位，否则部分系统上不生效
	enableExtendedFlags = 0x0080
)

// disableQuickEdit 关闭 Windows 控制台的快速编辑模式
// 返回 true 表示成功关闭（即成功拿到控制台并修改了模式）
// 返回 false 表示当前不是真实控制台（例如被重定向到文件/管道），无需也无法修改
func disableQuickEdit() bool {
	// 获取标准输入句柄
	h, _, _ := procGetStdHandle.Call(stdInputHandle)
	if h == 0 || h == uintptr(syscall.InvalidHandle) {
		return false
	}

	// 读取当前控制台模式
	var mode uint32
	r, _, _ := procGetConsoleMode.Call(h, uintptr(unsafe.Pointer(&mode)))
	if r == 0 {
		return false
	}

	// 清除快速编辑模式与插入模式，并置位扩展标志
	mode &^= enableQuickEditMode
	mode &^= enableInsertMode
	mode |= enableExtendedFlags

	// 写回控制台模式
	r, _, _ = procSetConsoleMode.Call(h, uintptr(mode))
	return r != 0
}
