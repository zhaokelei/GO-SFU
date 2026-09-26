# YSP SFU - 选择性转发单元

[English](README.en.md) | 中文

基于 Go + pion/webrtc v3 实现的标准 SFU（Selective Forwarding Unit）。

所有音视频媒体流量全部经过服务器转发，浏览器之间不建立任何直接 P2P 连接。

## 特性

- **纯 RTP 包转发**：不解码、不编码、不转码，复制 RTP 包转发给订阅者
- **NACK 重传**：滑动窗口缓存最近 RTP 包，收到 NACK 请求时重传丢失包
- **PLI 关键帧请求**：订阅端 PLI 透传给发布端，解决丢包后花屏/绿屏
- **RTCP 双向透传**：SenderReport / ReceiverReport 双向转发，保障音视频同步
- **序列号校验**：过滤重复包、乱序包，避免花屏
- **抖动缓冲**：平滑 RTP 到达时间差，缓解卡顿/爆音
- **异常包过滤**：检测非法版本号、长度异常的 RTP 包并丢弃
- **SSRC 隔离**：同一 Session 内音视频 SSRC 隔离，避免混流
- **NAT 穿透**：支持 `--public-ip` 设置 NAT1To1IPs，内置 STUN，可选 TURN
- **连接状态监听**：ICE / PeerConnection 状态变更自动触发资源清理
- **WebSocket 心跳**：定时 ping/pong，超时断连
- **自动资源清理**：Session 下线时关闭所有 sender/receiver、清空缓存，杜绝 goroutine 泄漏
- **事件有序分发**：房间内事件通过单 goroutine 队列顺序处理，避免并发乱序
- **单 EXE 部署**：前端页面通过 `go:embed` 嵌入二进制，静态编译无外部依赖
- **非阻塞日志**：日志异步写入（控制台 + 文件双通道各带缓冲队列），某一输出被阻塞也不会拖垮 SFU
- **控制台防冻结**：Windows 下启动即自动关闭控制台「快速编辑模式」，避免点击黑框导致进程被挂起

## 项目结构

```
ysp/
├── main.go          # 主入口：命令行参数、HTTP服务、WebRTC初始化
├── room.go          # 房间管理：Session生命周期、事件有序分发、统计监控
├── session.go       # 会话管理：PeerConnection封装、Track发布订阅、RTCP透传
├── signaling.go     # WebSocket信令：消息处理、心跳、消息过滤
├── rtp_buffer.go    # RTP处理：滑动窗口缓存、序列号校验、抖动缓冲、SSRC映射
├── logger.go        # 分级日志（INFO/WARN/ERROR），异步非阻塞、控制台+文件双通道
├── console_windows.go # Windows：启动时关闭控制台快速编辑模式，防止点击黑框冻结进程
├── console_other.go   # 非 Windows 平台：空实现，保证跨平台编译
├── index.html       # 前端测试页面（嵌入二进制）
├── go.mod
├── README.md        # 中文文档
└── README.en.md     # 英文文档
```

## 编译

### 1. 初始化模块（首次）

```bash
go mod init ysp-sfu
```

### 2. 拉取依赖

```bash
go mod tidy
```

### 3. 静态编译生成单 EXE

**Windows（在 Windows 上编译）：**

```powershell
$env:CGO_ENABLED=0
$env:GOOS="windows"
$env:GOARCH="amd64"
go build -o sfu.exe .
```

**Linux 交叉编译 Windows EXE：**

```bash
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o sfu.exe .
```

编译产物 `sfu.exe` 为单独立文件，无需附带 html、dll 等任何外部文件，拷贝到任意 Windows 机器双击即可运行。

## 运行

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--addr` | `:2033` | 监听地址 |
| `--public-ip` | （必填） | 服务器公网 IP，用于 NAT1To1IPs |
| `--stun` | `stun:stun.chat.bilibili.com:3478,stun:stun.cloudflare.com:3478,stun:stun.miwifi.com:3478` | STUN 地址（多个用逗号分隔） |
| `--turn-url` | 空 | TURN 地址（可选） |
| `--turn-user` | 空 | TURN 用户名（可选） |
| `--turn-pass` | 空 | TURN 密码（可选） |
| `--jitter-buffer-ms` | `50` | 抖动缓冲时间（毫秒） |
| `--nack-cache-size` | `512` | NACK 缓存包数量 |
| `--max-sessions` | `100` | 单个房间最大 Session 数 |
| `--log-level` | `INFO` | 日志级别（INFO/WARN/ERROR） |
| `--log-file` | `sfu.log` | 日志文件路径（为空则只输出到控制台） |

### 本地测试

```bash
sfu.exe --public-ip 127.0.0.1
```

浏览器打开 `http://127.0.0.1:2033`，输入房间 ID 和会话 ID，点击连接即可测试。

### 公网部署

```bash
sfu.exe --public-ip <你的公网IP>
```

如需 TURN 中继兜底：

```bash
sfu.exe --public-ip <公网IP> --turn-url turn:<turn服务器>:3478 --turn-user <用户名> --turn-pass <密码>
```

## 常见问题：点击服务器黑框后连不上 / 通话卡死

**现象**：在服务器上点击 cmd / PowerShell 窗口后，前端点「连接」一直连不上；在窗口里按一下回车，连接瞬间成功；通话中如果点了黑框，画面两秒就卡死。

**原因**：Windows 控制台默认开启「快速编辑模式（QuickEdit Mode）」。鼠标在黑框内点击后会进入「文本选择」状态，此时进程向控制台写入会被操作系统阻塞挂起；由于 SFU 的日志原本是同步写控制台，日志被卡住就会连锁冻结整个进程（信令不响应、媒体停止转发）。按回车取消选择后写入恢复，所以表现为「按回车瞬间连上」。

**已内置的修复**（本版本）：

1. 程序启动时自动调用 Windows API 关闭控制台「快速编辑模式」，从根源上让点击黑框不再冻结进程；
2. 日志改为**异步非阻塞**，并同时写入 `--log-file` 指定的文件（默认 `sfu.log`）。即使控制台被冻结，SFU 的信令与媒体转发也完全不受影响，日志仍会完整落盘。

**手动兜底**（若需在旧版本上临时规避）：cmd 标题栏右键 → 属性 → 选项 → 取消勾选「快速编辑模式」；或用重定向启动，例如 `sfu.exe --public-ip <IP> > run.log 2>&1`。

## HTTPS 部署（Nginx 反向代理）

浏览器（尤其 iOS Safari / Android Chrome）只有在**安全上下文**下才允许调用摄像头和麦克风，
所以手机端测试必须走 HTTPS。推荐做法：**Nginx 负责 HTTPS（TLS 终止），后端 SFU 保持纯 HTTP**，
无需在程序里处理证书。

### 架构说明

```
手机 / 电脑浏览器
    │  https://sfu.example.com      ← 页面 + 信令（TCP 443）
    ▼
Nginx（TLS 终止）
    │  http://127.0.0.1:2033       ← 反向代理，含 WebSocket 升级
    ▼
SFU 后端（HTTP + WS）
    ▲
    │  WebRTC 媒体（UDP，不经过 Nginx）
    └────────────────────────────  浏览器直连服务器 UDP
```

> **关键点**：Nginx 只反代 HTTP / WebSocket（TCP），**WebRTC 的媒体流量是 UDP，不经过 Nginx**。
> 因此：
> 1. `--public-ip` 仍必须设置为浏览器能连到的服务器地址；
> 2. UDP 媒体端口范围仍需对外放行（见「防火墙说明」）。

### 1. 准备证书

**方式 A：有公网域名（推荐，证书天然受信任）**

```bash
# Ubuntu / Debian
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d sfu.example.com
```

**方式 B：纯局域网 / 无域名（自签名）**

```bash
sudo mkdir -p /etc/nginx/ssl
openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
  -keyout /etc/nginx/ssl/key.pem \
  -out /etc/nginx/ssl/cert.pem \
  -subj "/CN=192.168.1.100" \
  -addext "subjectAltName=IP:192.168.1.100,IP:127.0.0.1,DNS:localhost"
```

把 `192.168.1.100` 换成服务器实际 IP。若已安装 mkcert，可一步生成并自动信任：

```bash
mkcert -install
mkcert 192.168.1.100 localhost 127.0.0.1
```

### 2. 配置 Nginx

先在 `http { }` 块中加入这段 `map`（用于正确判断 WebSocket 升级）：

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
```

再新建 `/etc/nginx/conf.d/sfu.conf`：

```nginx
server {
    listen 443 ssl;
    http2 on;                      # nginx < 1.25.1 请写成：listen 443 ssl http2;
    server_name sfu.example.com;   # 局域网自签名场景填 IP，如 192.168.1.100

    ssl_certificate     /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_session_cache   shared:SSL:10m;

    location / {
        proxy_pass http://127.0.0.1:2033;
        proxy_http_version 1.1;

        # WebSocket 升级必需
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection $connection_upgrade;

        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # 信令是长连接，超时必须放大，否则会频繁掉线重连
        proxy_read_timeout    3600s;
        proxy_send_timeout    3600s;
        proxy_connect_timeout 10s;

        # 关闭响应缓冲，降低信令延迟
        proxy_buffering off;
    }
}

# HTTP 自动跳转 HTTPS
server {
    listen 80;
    server_name sfu.example.com;
    return 301 https://$host$request_uri;
}
```

检查并重载：

```bash
sudo nginx -t && sudo nginx -s reload
```

### 2.1 宝塔面板（BT Panel）配置

宝塔添加站点时会自动生成站点主配置（`listen` / `server_name` / `root` / SSL / HTTP 跳转等），
**不要整体替换**。只需在站点「配置文件」里，把下面这一段粘进 `server { }` 内部任意位置
（例如 `#SSL-INFO-END` 之后），保存并在面板点「重载配置」即可：

```nginx
    # ==== YSP SFU 后端反代：页面 + 信令 WebSocket ====
    location / {
        proxy_pass http://127.0.0.1:2033;
        proxy_http_version 1.1;

        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 升级必需：缺这两行 /ws 握手会失败（后端报 upgrade token not found）
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 信令是长连接，超时必须放大，否则会频繁掉线重连
        proxy_read_timeout    3600s;
        proxy_send_timeout    3600s;
        proxy_connect_timeout 10s;

        # 关闭响应缓冲，降低信令延迟
        proxy_buffering off;
    }
```

> **说明**：
> 1. 前端页面已内嵌在 `sfu.exe` 中，所以用 `location /` 一条即可把**页面和 `/ws` 信令**一起转给后端，
>    不必再把文件放进 `wwwroot`；
> 2. 若你只想反代信令、页面另放宝塔静态目录，把 `location /` 改成 `location /ws`；
> 3. 宝塔自带的 `#PHP-INFO` / `#REWRITE` / `#redirect` / `#static-cache` 等 `include` 保留面板默认内容即可；
> 4. 若面板的 `static_cache` 里对 `\.(js|css|png|jpg...)` 定义了带 `expires` 的 `location`，
>    它会优先于 `location /` 而拦截这些请求，必要时把该 `include` 注释掉。

### 3. 启动后端

Nginx 已做 TLS 终止，后端继续用 HTTP 启动即可：

```bash
sfu.exe --addr 127.0.0.1:2033 --public-ip 192.168.1.100
```

- `--addr 127.0.0.1:2033`：只允许本机 Nginx 访问，避免绕过 HTTPS 直连（更安全）
- `--public-ip`：填浏览器实际能连到的服务器地址（局域网填局域网 IP，公网填公网 IP）

### 4. 手机访问

用手机浏览器打开 `https://192.168.1.100`（或你的域名）：

- **域名 + Let's Encrypt**：证书受信任，直接可用。
- **自签名证书**：必须先把证书安装并**信任**，否则浏览器会禁用摄像头/麦克风：
  - **iOS**：把 `cert.pem` 传到手机 → 设置 → 已下载描述文件 → 安装 → 然后进入 设置 → 通用 → 关于本机 → 证书信任设置 → 打开完全信任
  - **Android**：设置 → 安全 → 加密与凭据 → 安装证书 → CA 证书
  - **Windows / macOS**：双击证书 → 安装到「受信任的根证书颁发机构」

> 仅在证书警告页点击「继续访问」**不够**：浏览器不会把它视作安全上下文，`getUserMedia` 依然会被拒绝。

### 5. 验证

- 页面能打开，地址栏显示锁头且无警告
- 前端日志显示 `正在连接 wss://<IP>/ws` 且提示「信令已连接」
- 浏览器控制台无 `Mixed Content` 报错（前端已按页面协议自动选择 `ws` / `wss`）
- 能正常调用摄像头和麦克风

## 防火墙说明

### 必须开放的端口

| 端口 | 协议 | 用途 |
|------|------|------|
| 443 | TCP | HTTPS 页面 + WSS 信令（经 Nginx，对外暴露） |
| 80 | TCP | HTTP 自动跳转 HTTPS（可选） |
| 2033 | TCP | 后端 HTTP + WebSocket（Nginx 方案下建议仅监听 127.0.0.1，无需对外暴露） |
| 49152-65535 | UDP | WebRTC 媒体（ICE/DTLS/SRTP），**必须对外暴露** |

### 配置示例（Linux iptables）

```bash
# 允许信令和网页
iptables -A INPUT -p tcp --dport 2033 -j ACCEPT

# 允许 WebRTC 媒体 UDP 端口范围
iptables -A INPUT -p udp --dport 49152:65535 -j ACCEPT
```

### Windows 防火墙

在「Windows  Defender 防火墙」中添加入站规则：
- TCP 2033
- UDP 49152-65535

## 跨境部署注意事项

1. **公网 IP 必须设置**：`--public-ip` 为必填项，否则浏览器无法连接服务器。
2. **STUN 服务器选择**：跨境网络下建议使用离服务器较近的 STUN，或自建 STUN/TURN 服务器。
3. **TURN 兜底**：跨境网络丢包率高，建议配置 TURN 服务器作为中继兜底，保证连通性。
4. **抖动缓冲调整**：跨境网络抖动大，可适当调大 `--jitter-buffer-ms`（如 100-200ms），牺牲延迟换流畅度。
5. **NACK 缓存**：跨境丢包多，可适当调大 `--nack-cache-size`（如 50-100），提高重传成功率。
6. **云厂商安全组**：务必在云厂商控制台的安全组中放行上述 TCP/UDP 端口。
7. **带宽规划**：SFU 模式下服务器出口带宽 = 订阅人数 × 发布码率，需提前规划带宽。

## 信令协议

所有信令通过 WebSocket 传输，消息格式为 JSON：

```json
{ "type": "消息类型", "data": { ... } }
```

### 客户端 → 服务器

| type | 说明 | data |
|------|------|------|
| `join` | 加入房间 | `{ roomId, sessionId, name }`（`sessionId` 为唯一连接标识，`name` 为显示昵称，可重复） |
| `offer` | 推送 SDP Offer | `{ sdp }` |
| `answer` | 回复 SDP Answer | `{ sdp }` |
| `ice_candidate` | Trickle ICE 候选 | `{ candidate, sdpMid, sdpMLineIndex }` |
| `subscribe` | 订阅轨道 | `{ publisherId, trackId }` |
| `unsubscribe` | 取消订阅 | `{ publisherId, trackId }` |
| `list_tracks` | 获取房间轨道列表 | - |
| `ping` | 心跳 | - |

### 服务器 → 客户端

| type | 说明 |
|------|------|
| `join_success` | 加入成功 |
| `sessions_list` | 房间内已有会话列表（每项含 `sessionId` 与 `name`） |
| `tracks_list` | 房间轨道列表 |
| `track_published` | 有新轨道发布 |
| `track_unpublished` | 有轨道取消发布 |
| `session_join` | 有会话加入（含 `sessionId` 与 `name`） |
| `session_leave` | 有会话离开（含 `sessionId` 与 `name`） |
| `offer` | 服务器发起重协商 Offer |
| `answer` | 服务器回复 Answer |
| `ice_candidate` | 服务器 ICE 候选 |
| `error` | 错误通知 |
| `pong` | 心跳回复 |

## 设计说明

- **不定义 Peer 结构体**：业务层只有 Room 和 Session，直接使用 pion 的 `webrtc.PeerConnection` 作为浏览器↔服务器的 WebRTC 连接对象。
- **纯包转发**：服务器收到 RTP 包后，经序列号校验、异常过滤后，复制转发给所有订阅者的 `TrackLocalStaticRTP`。
- **RTCP 透传**：订阅端的 PLI/NACK 通过发布者的 `PeerConnection.WriteRTCP` 透传给发布端浏览器；发布端的 SR/RR 由 pion 拦截器自动处理。
- **资源清理**：Session 关闭时通过 `context.CancelFunc` 通知所有读包 goroutine 退出，关闭 PeerConnection 释放所有 sender/receiver，清空 RTP 缓存。

## 限制

- 不做 simulcast、SVC、转码
- 不做录制
- 内部测试用，无 JWT 鉴权
