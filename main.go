// main.go
// SFU 主入口
// 负责：
//   - 命令行参数解析
//   - 日志初始化（分级 INFO/WARN/ERROR）
//   - WebRTC API 配置（NAT1To1IPs、STUN/TURN）
//   - HTTP 服务（内嵌 index.html、WebSocket 信令路由）
//   - 房间管理器初始化
//   - 统计监控定时打印

package main

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/turn/v2"
	"github.com/pion/webrtc/v3"
)

// ============================================================
// 全局变量（供其他模块使用）
// ============================================================

// 命令行参数
var (
	addr           = flag.String("addr", ":2033", "监听地址")
	publicIP       = flag.String("public-ip", "", "服务器公网IP（必填）")
	stunURL        = flag.String("stun", "stun:stun.chat.bilibili.com:3478,stun:stun.cloudflare.com:3478,stun:stun.miwifi.com:3478", "客户端STUN地址（多个用逗号分隔，仅下发给前端；服务端自身已不再使用）")
	turnURL        = flag.String("turn-url", "", "外部 TURN 地址（可选，仅供服务端自身使用）")
	turnUser       = flag.String("turn-user", "ysp", "TURN 用户名（内嵌 TURN 与外部 TURN 共用）")
	turnPass       = flag.String("turn-pass", "", "TURN 密码（内嵌 TURN：留空则每次启动随机生成；外部 TURN：需显式填写）")
	turnPort       = flag.Int("turn-port", 3478, "内嵌 TURN 服务监听端口(UDP)，设为 0 表示禁用内嵌 TURN")
	turnRealm      = flag.String("turn-realm", "ysp-sfu", "内嵌 TURN 认证域(realm)")
	jitterBufferMs = flag.Int("jitter-buffer-ms", 50, "抖动缓冲时间(ms)")
	nackCacheSize  = flag.Int("nack-cache-size", 1024, "NACK缓存包数量")
	maxSessions    = flag.Int("max-sessions", 100, "单个房间最大Session数")
	logLevel       = flag.String("log-level", "INFO", "日志级别(INFO/WARN/ERROR)")
	logFile        = flag.String("log-file", "sfu.log", "日志文件路径（为空则只输出到控制台）")
)

// webrtcAPI 全局 WebRTC API 实例
var webrtcAPI *webrtc.API

// 内嵌 TURN 服务端相关状态
// 说明：TURN 中继直接跑在本 exe 进程内，不依赖外部 coturn。
// 前端页面由服务端渲染时会把下面的地址/账号密码注入进去，无需手动配置。
var (
	turnServer   *turn.Server // 内嵌 TURN 服务实例（nil 表示未启用）
	turnAddr     string       // 下发给前端的 TURN URL，形如 turn:1.2.3.4:3478
	turnCredUser string       // 下发给前端的 TURN 用户名
	turnCredPass string       // 下发给前端的 TURN 密码
)

// ============================================================
// go:embed 内嵌前端页面
// ============================================================

//go:embed index.html
var indexHTML embed.FS

// ============================================================
// 初始化 WebRTC API
// ============================================================

func initWebRTC(pubIP string) (*webrtc.API, error) {
	settingEngine := webrtc.SettingEngine{}

	// 设置 NAT1To1IPs（公网地址映射）。
	//
	// 这里必须用 ICECandidateTypeSrflx 而不是 ICECandidateTypeHost，原因是：
	//   1) Host 模式：pion 会用公网 IP 【替换】服务器的内网 host 候选，
	//      私网地址对外完全隐藏（见 pion webrtc settingengine.go 说明）。
	//      结果服务器只对外暴露一个公网 IP，没有任何内网候选。
	//   2) 当两台手机连在同一个 WiFi（同一个 NAT 出口）时，它们访问服务器的
	//      公网 IP 需要路由器支持「NAT 回流 / hairpin」。绝大多数家用路由器
	//      默认不支持回流，于是 ICE 直连失败 → 同一 WiFi 下无法通话；
	//      而一台 4G、一台 WiFi 时走的是正常公网路径，所以能通。
	//   3) Srflx 模式：内网 host 候选会【保留】，同时额外追加一条公网 srflx 候选。
	//      - 同一 WiFi 的手机：可用内网地址直连服务器，绕开回流限制；
	//      - 跨网（4G）的手机：走公网 srflx 候选。
	if pubIP != "" {
		settingEngine.SetNAT1To1IPs([]string{pubIP}, webrtc.ICECandidateTypeSrflx)
		log.Infof("已设置 NAT1To1IPs(Srflx): %s", pubIP)
	} else {
		log.Warnf("未设置 --public-ip，仅能本地测试，公网部署必须设置！")
	}

	// 设置 ICE UDP 端口范围（大范围 UDP 用于 WebRTC 媒体）
	settingEngine.SetEphemeralUDPPortRange(49152, 65535)

	// 放宽 ICE 超时阈值：默认值（断连 5s / 失败 25s）在弱网或重协商期间偏激进，
	// 实测日志里出现过「ICE 连接状态变更: failed」把本来还能恢复的会话提前关掉。
	// 这里放宽为：断连 10s、失败 30s、保活 2s，给网络抖动留出恢复余量。
	settingEngine.SetICETimeouts(10*time.Second, 30*time.Second, 2*time.Second)

	// 创建媒体引擎，注册常用编解码器
	mediaEngine := webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("注册默认编解码器失败: %w", err)
	}

	// 创建拦截器链：注册 pion 默认拦截器（NACK 重传、RTCP 反馈、TWCC 拥塞控制）
	// 注意：必须先建注册表，再调用 RegisterDefaultInterceptors 才会真正挂载拦截器；
	// 只创建空的 interceptor.Registry{} 且不注册，则不会加载任何拦截器，会导致
	// 浏览器端无法做基于 TWCC 的带宽估计，弱网时不能及时降低码率而持续丢包。
	interceptorRegistry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(&mediaEngine, interceptorRegistry); err != nil {
		return nil, fmt.Errorf("注册默认拦截器失败: %w", err)
	}

	// 创建 API
	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(settingEngine),
		webrtc.WithMediaEngine(&mediaEngine),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
	)

	return api, nil
}

// ============================================================
// 内嵌 TURN 服务端
// ============================================================

// turnConfigPlaceholder 是 index.html 中的占位符，
// 页面渲染时会被替换成内嵌 TURN 的 JSON 配置（未启用时为 null）。
const turnConfigPlaceholder = "__YSP_TURN_CONFIG_PLACEHOLDER__"

// startEmbeddedTURN 在本进程内启动一个 TURN 中继服务。
//
// 为什么需要 TURN：
//
//	WebRTC 的 ICE 打洞在以下场景会失败，直连不通就永远收不到对方画面：
//	  1) 两台设备在同一个 WiFi（同一 NAT 出口），访问服务器公网 IP 需要
//	     路由器支持 NAT 回流(hairpin)，绝大多数家用路由器默认不支持；
//	  2) 部分运营商 4G/5G 使用对称型 NAT，连公网 STUN 也拿不到可用映射。
//	此时唯一的兜底就是让媒体经 TURN 服务器中继转发。
//
// 本实现直接把 pion 的 TURN 服务端跑在 exe 内部，不依赖外部 coturn：
//   - 控制端口：--turn-port（UDP，默认 3478）
//   - 中继端口：49152-65535/UDP（与 WebRTC 媒体端口范围一致，需一并放行）
//   - 中继对外地址：--public-ip
//   - 账号密码：--turn-user / --turn-pass（密码留空则每次启动随机生成）
func startEmbeddedTURN(pubIP string) error {
	if *turnPort <= 0 {
		log.Infof("内嵌 TURN: 已禁用（--turn-port=0）")
		return nil
	}
	if pubIP == "" {
		log.Warnf("内嵌 TURN: 未设置 --public-ip，无法确定中继对外地址，已跳过启动")
		return nil
	}

	relayIP := net.ParseIP(pubIP)
	if relayIP == nil {
		return fmt.Errorf("--public-ip 不是合法 IP 地址: %s", pubIP)
	}

	// 密码留空则随机生成 24 位十六进制串，避免使用弱口令
	user := *turnUser
	pass := *turnPass
	if pass == "" {
		buf := make([]byte, 12)
		if _, err := rand.Read(buf); err != nil {
			return fmt.Errorf("生成内嵌 TURN 随机密码失败: %w", err)
		}
		pass = hex.EncodeToString(buf)
	}

	realm := *turnRealm
	authKey := turn.GenerateAuthKey(user, realm, pass)

	listenAddr := fmt.Sprintf("0.0.0.0:%d", *turnPort)
	conn, err := net.ListenPacket("udp4", listenAddr)
	if err != nil {
		return fmt.Errorf("内嵌 TURN 监听 %s 失败: %w", listenAddr, err)
	}

	// 创建日志工厂（默认级别 Error，避免中继转发时刷屏）
	loggerFactory := logging.NewDefaultLoggerFactory()

	srv, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		// 认证回调：只接受预设的用户名，比对 md5 认证密钥
		AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
			if username != user {
				return nil, false
			}
			return authKey, true
		},
		// UDP 监听配置：中继地址对外用公网 IP，实际监听 0.0.0.0
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn: conn,
				RelayAddressGenerator: &turn.RelayAddressGeneratorPortRange{
					RelayAddress: relayIP,
					Address:      "0.0.0.0",
					MinPort:      49152,
					MaxPort:      65535,
				},
				PermissionHandler: turn.DefaultPermissionHandler,
			},
		},
		LoggerFactory: loggerFactory,
	})
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("启动内嵌 TURN 失败: %w", err)
	}

	turnServer = srv
	turnAddr = fmt.Sprintf("turn:%s:%d", pubIP, *turnPort)
	turnCredUser = user
	turnCredPass = pass

	log.Infof("内嵌 TURN 已启动: %s (realm=%s, user=%s)", turnAddr, realm, user)
	log.Infof("内嵌 TURN 中继端口范围: 49152-65535/UDP（需在防火墙/云安全组放行）")
	return nil
}

// embeddedTURNJSON 把内嵌 TURN 配置序列化成 JSON，供页面渲染时注入。
// 未启用内嵌 TURN 时返回 "null"。
func embeddedTURNJSON() string {
	if turnServer == nil || turnAddr == "" {
		return "null"
	}
	cfg := map[string]interface{}{
		"urls":       []string{turnAddr},
		"username":   turnCredUser,
		"credential": turnCredPass,
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Warnf("序列化内嵌 TURN 配置失败: %v", err)
		return "null"
	}
	return string(b)
}

// ============================================================
// HTTP 路由
// ============================================================

// handleIndex 返回内嵌的 index.html（并把内嵌 TURN 配置注入页面）
func handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := indexHTML.ReadFile("index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusInternalServerError)
		return
	}

	// 把占位符替换为内嵌 TURN 配置：前端 initPC() 会读取 window.__YSP_TURN__
	// 来配置 ICE 中继，因此启用内嵌 TURN 后客户端无需任何手动配置。
	page := strings.Replace(string(data), turnConfigPlaceholder, embeddedTURNJSON(), 1)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write([]byte(page))
}

// ============================================================
// 统计监控
// ============================================================

// statsLoop 定时打印统计信息
func statsLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		roomManager.PrintAllStats()
		log.Infof("[Stats] 当前 goroutine 数量: %d", runtime.NumGoroutine())
	}
}

// ============================================================
// 主函数
// ============================================================

func main() {
	// 解析命令行参数
	flag.Parse()

	// 初始化日志（异步非阻塞，支持同时写日志文件）
	initLogger(*logLevel, *logFile)

	// 关键修复：关闭 Windows 控制台的「快速编辑模式」
	// 否则一旦有人在服务器黑框里点了鼠标，控制台进入文本选择状态，
	// 进程写日志会被阻塞挂起，导致前端连不上、通话卡死（按回车才恢复）。
	if disableQuickEdit() {
		log.Infof("已自动关闭 Windows 控制台快速编辑模式（点击黑框不会再卡死）")
	}

	// 打印启动配置信息
	log.Infof("========================================")
	log.Infof("  YSP SFU 服务器启动")
	log.Infof("========================================")
	log.Infof("监听地址: %s", *addr)
	log.Infof("公网 IP: %s", *publicIP)
	log.Infof("STUN(下发给客户端): %s", *stunURL)
	if *turnURL != "" {
		log.Infof("外部 TURN(仅服务端自身): %s (user=%s)", *turnURL, *turnUser)
	} else {
		log.Infof("外部 TURN(仅服务端自身): 未配置")
	}
	if *turnPort > 0 {
		log.Infof("内嵌 TURN: 启用，控制端口 UDP %d（密码为空则随机生成）", *turnPort)
	} else {
		log.Infof("内嵌 TURN: 禁用")
	}
	log.Infof("抖动缓冲: %d ms", *jitterBufferMs)
	log.Infof("NACK 缓存大小: %d 包", *nackCacheSize)
	log.Infof("单房间最大 Session: %d", *maxSessions)
	log.Infof("日志级别: %s", *logLevel)
	if *logFile != "" {
		log.Infof("日志文件: %s", *logFile)
	} else {
		log.Infof("日志文件: 未启用（仅控制台输出）")
	}
	log.Infof("========================================")

	if *publicIP == "" {
		log.Warnf("警告: 未设置 --public-ip，公网部署将无法建立 WebRTC 连接！")
	}

	// 初始化 WebRTC API
	api, err := initWebRTC(*publicIP)
	if err != nil {
		log.Errorf("初始化 WebRTC 失败: %v", err)
		log.Close()
		os.Exit(1)
	}
	webrtcAPI = api

	// 启动内嵌 TURN 中继：客户端直连（打洞）失败时由此兜底转发媒体。
	// 启动失败不影响直连通话，因此只记录错误、降级继续运行。
	if err := startEmbeddedTURN(*publicIP); err != nil {
		log.Errorf("启动内嵌 TURN 失败（将降级为仅直连）: %v", err)
	}

	// 初始化房间管理器
	initRoomManager(RoomConfig{
		MaxSessions:    *maxSessions,
		NackCacheSize:  *nackCacheSize,
		JitterBufferMs: *jitterBufferMs,
	})

	// 设置 HTTP 路由
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/ws", handleWebSocket)

	// 启动统计监控
	go statsLoop()

	// 启动 HTTP 服务器
	log.Infof("HTTP 服务器启动，监听 %s", *addr)
	log.Infof("访问 http://<服务器IP>:2033 打开前端页面")
	log.Infof("WebSocket 信令: ws://<服务器IP>:2033/ws")

	server := &http.Server{
		Addr:         *addr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 优雅关闭
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Infof("收到退出信号，正在关闭服务器...")

		rooms := roomManager.ListRooms()
		for _, room := range rooms {
			room.Destroy()
		}

		_ = server.Close()

		// 关闭内嵌 TURN 服务
		if turnServer != nil {
			_ = turnServer.Close()
		}

		log.Infof("服务器已关闭")
		log.Close()
		os.Exit(0)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Errorf("HTTP 服务器错误: %v", err)
		log.Close()
		os.Exit(1)
	}
}
