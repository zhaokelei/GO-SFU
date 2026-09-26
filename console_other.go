// console_other.go
// 非 Windows 平台：控制台快速编辑模式不存在，提供空实现以保证跨平台编译通过

//go:build !windows

package main

// disableQuickEdit 非 Windows 平台无需处理，直接返回 false
func disableQuickEdit() bool {
	return false
}
