// signaling.go
// WebSocket 信令模块
// 负责：
//   - WebSocket 连接建立与管理
//   - 信令消息处理（join/offer/answer/ice_candidate/subscribe/unsubscribe）
//   - 心跳检测（ping/pong，超时断连）
//   - 非法消息过滤，防止恶意消息导致 panic
//   - 客户端连接映射（sessionID -> WebSocket）

package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

// SignalingMessage 信令消息统一格式
type SignalingMessage struct {
	Type string      `json:"type"` // 消息类型
	Data interface{} `json:"data"` // 消息数据
}

// ClientInfo 客户端连接信息
type ClientInfo struct {
	conn       *websocket.Conn
	sessionID  string
	name       string
	roomID     string
	session    *Session
	mu         sync.Mutex
	negMu      sync.Mutex // 重协商串行化锁，避免同一个 PC 并发发起 offer
	lastPong   time.Time
	closed     bool
	clientBusy int32 // 客户端是否正在协商（1=忙碌），用于避免双方同时发起 offer

	// pendingCandidates 暂存「远端描述尚未设置」时提前到达的 ICE 候选
	// Trickle ICE 下浏览器可能在 Offer/Answer 处理完成前就发来候选，
	// 此时 PC 尚无远端描述，直接 AddICECandidate 会失败。先暂存，
	// 等 SetRemoteDescription 成功后再统一补加，避免候选丢失导致连通失败
	pendingCandidates []webrtc.ICECandidateInit
}

// 全局客户端连接映射：sessionID -> *ClientInfo
var (
	clientsMu sync.RWMutex
	clients   = make(map[string]*ClientInfo)
)

// WebSocket 升级器
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// 允许跨域（内部测试用）
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// handleWebSocket 处理 WebSocket 连接
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("WebSocket 升级失败: %v", err)
		return
	}

	// 创建客户端信息
	client := &ClientInfo{
		conn:     conn,
		lastPong: time.Now(),
	}

	// 启动心跳检测
	go client.heartbeatLoop()

	// 启动消息读取循环
	client.readLoop()
}

// readLoop 消息读取循环
func (c *ClientInfo) readLoop() {
	defer func() {
		c.cleanup()
	}()

	// 设置 pong 处理函数
	c.conn.SetPongHandler(func(appData string) error {
		c.mu.Lock()
		c.lastPong = time.Now()
		c.mu.Unlock()
		return nil
	})

	// 设置读取超时
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Warnf("[Signaling] WebSocket 读取异常 session=%s err=%v", c.sessionID, err)
			} else {
				log.Infof("[Signaling] WebSocket 连接关闭 session=%s", c.sessionID)
			}
			return
		}

		// 重置读取超时
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// 处理消息
		c.handleMessage(msg)
	}
}

// handleMessage 处理信令消息
func (c *ClientInfo) handleMessage(raw []byte) {
	// 防御性编程：消息长度限制，防止恶意大包
	if len(raw) > 64*1024 {
		log.Warnf("[Signaling] 消息过大(%d bytes)，忽略", len(raw))
		return
	}

	var msg SignalingMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Warnf("[Signaling] JSON 解析失败: %v", err)
		return
	}

	// 消息类型过滤
	switch msg.Type {
	case "join":
		c.handleJoin(msg.Data)
	case "offer":
		c.handleOffer(msg.Data)
	case "answer":
		c.handleAnswer(msg.Data)
	case "ice_candidate":
		c.handleICECandidate(msg.Data)
	case "subscribe":
		c.handleSubscribe(msg.Data)
	case "unsubscribe":
		c.handleUnsubscribe(msg.Data)
	case "list_tracks":
		c.handleListTracks()
	case "call_request":
		c.handleCallRequest(msg.Data)
	case "call_accept":
		c.handleCallAccept(msg.Data)
	case "call_reject":
		c.handleCallReject(msg.Data)
	case "call_end":
		c.handleCallEnd(msg.Data)
	case "negotiation_state":
		c.handleNegotiationState(msg.Data)
	case "renegotiate_retry":
		c.handleRenegotiateRetry(msg.Data)
	case "ping":
		// 客户端心跳，回复 pong
		c.sendPong()
	default:
		log.Warnf("[Signaling] 未知消息类型: %s", msg.Type)
	}
}

// handleJoin 处理加入房间请求
func (c *ClientInfo) handleJoin(data interface{}) {
	// 解析参数
	joinData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("join 参数格式错误")
		return
	}

	roomID, _ := joinData["roomId"].(string)
	sessionID, _ := joinData["sessionId"].(string)
	// name 为显示昵称，允许重复；为空时回退为 sessionId，保证界面始终有名称可展示
	name, _ := joinData["name"].(string)
	if name == "" {
		name = sessionID
	}

	if roomID == "" || sessionID == "" {
		c.sendError("roomId 和 sessionId 不能为空")
		return
	}

	// 防止 sessionID / name 过长
	if len(sessionID) > 128 || len(roomID) > 128 || len(name) > 64 {
		c.sendError("roomId、sessionId 或昵称过长")
		return
	}

	// 断线重连幂等处理：
	// 1) 若同一 sessionId 的旧连接仍存在，则顶替旧连接（旧连接会被异步关闭）
	// 2) 若房间内仍残留同名 Session（旧连接尚未完全清理），先移除，避免加入失败
	clientsMu.Lock()
	oldClient := clients[sessionID]
	if oldClient != nil {
		delete(clients, sessionID)
	}
	clientsMu.Unlock()

	if oldClient != nil && oldClient != c {
		log.Warnf("[Signaling] sessionId %s 已存在，顶替旧连接", sessionID)
		go oldClient.cleanup()
	}

	if oldRoom, ok := roomManager.GetRoom(roomID); ok {
		if stale, ok2 := oldRoom.GetSession(sessionID); ok2 {
			log.Warnf("[Signaling] 房间 %s 存在残留同名 Session %s，先移除", roomID, sessionID)
			oldRoom.RemoveSessionIf(sessionID, stale)
			go stale.Close()
		}
	}

	c.roomID = roomID
	c.sessionID = sessionID
	c.name = name

	// 创建 Session（sessionID 为唯一连接标识，name 为显示昵称）
	session, err := NewSession(sessionID, name, roomID, webrtcAPI,
		*nackCacheSize, *jitterBufferMs, c.onSessionEvent)
	if err != nil {
		log.Errorf("[Signaling] 创建 Session 失败: %v", err)
		c.sendError("创建会话失败: " + err.Error())
		return
	}
	c.session = session

	// 获取或创建房间，加入 Session
	room := roomManager.GetOrCreateRoom(roomID)
	if err := room.AddSession(session); err != nil {
		log.Errorf("[Signaling] 加入房间失败: %v", err)
		session.Close()
		c.sendError("加入房间失败: " + err.Error())
		return
	}

	// 注册客户端连接
	clientsMu.Lock()
	clients[sessionID] = c
	clientsMu.Unlock()

	log.Infof("[Signaling] Session %s 加入房间 %s", sessionID, roomID)

	// 返回加入成功
	c.sendMessage(SignalingMessage{
		Type: "join_success",
		Data: map[string]interface{}{
			"roomId":    roomID,
			"sessionId": sessionID,
			"name":      name,
		},
	})

	// 返回房间内已有的轨道列表
	tracks := room.GetAllTracks()
	c.sendMessage(SignalingMessage{
		Type: "tracks_list",
		Data: map[string]interface{}{
			"tracks": tracks,
		},
	})

	// 返回房间内已有的其他 Session 列表（排除自己）
	// 用于前端展示在线联系人，保证后加入者也能看到先加入者
	// 每项包含唯一 sessionId（拨号用）与显示昵称 name（展示用）
	others := make([]map[string]string, 0)
	for _, s := range room.GetAllSessions() {
		if s.ID != sessionID {
			others = append(others, map[string]string{
				"sessionId": s.ID,
				"name":      s.Name,
			})
		}
	}
	c.sendMessage(SignalingMessage{
		Type: "sessions_list",
		Data: map[string]interface{}{
			"sessions": others,
		},
	})
}

// handleOffer 处理浏览器发来的 Offer SDP
func (c *ClientInfo) handleOffer(data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	// 解析 SDP
	sdpData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("offer 参数格式错误")
		return
	}

	sdpStr, _ := sdpData["sdp"].(string)
	if sdpStr == "" {
		c.sendError("SDP 不能为空")
		return
	}

	// 会话或 PC 已关闭时直接丢弃 Offer：
	// 例如 ICE 断开会话已被销毁、连接被新连接顶替等场景，此时再进入等待
	// 队列毫无意义，只会白等 6 秒并刷出「等待 stable 超时」的无用日志
	if c.isClosed() {
		log.Warnf("[Signaling] Session %s 已关闭，丢弃本次 Offer", c.sessionID)
		return
	}
	if pc := c.session.GetPeerConnection(); pc.SignalingState() == webrtc.SignalingStateClosed {
		log.Warnf("[Signaling] Session %s PC 已关闭，丢弃本次 Offer", c.sessionID)
		return
	}

	// 协商冲突（glare）防护：
	// 服务端可能正等待自己发出的 Offer 的 Answer（have-local-offer），
	// 此时直接 SetRemoteDescription(offer) 会抛 InvalidModificationError，
	// 导致客户端的发布协商丢失。处理策略：暂存客户端 Offer，
	// 等服务端 PC 回到 stable 后再处理，保证协商意图不丢失（自愈）
	if pc := c.session.GetPeerConnection(); pc.SignalingState() != webrtc.SignalingStateStable {
		log.Warnf("[Signaling] Session %s 收到 Offer 但 PC 状态=%v，暂存并延迟处理",
			c.sessionID, pc.SignalingState())
		go c.applyOfferWhenStable(sdpStr)
		return
	}

	c.applyClientOffer(sdpStr)
}

// applyOfferWhenStable 等待服务端 PC 回到 stable 后再处理客户端 Offer
// 用于解决协商冲突：服务端上一步 Offer 尚未完成时收到的客户端 Offer 不能被丢弃
func (c *ClientInfo) applyOfferWhenStable(sdpStr string) {
	for i := 0; i < 60; i++ { // 最多等待 6 秒
		if c.isClosed() || c.session == nil {
			return
		}
		if c.session.GetPeerConnection().SignalingState() == webrtc.SignalingStateStable {
			c.applyClientOffer(sdpStr)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Warnf("[Signaling] Session %s 等待 PC stable 超时（6s），本次 Offer 被丢弃", c.sessionID)
}

// applyClientOffer 实际执行"设置远端 Offer -> 创建 Answer -> 设置本地 Answer -> 回传"
// 通过 negMu 与服务器主动重协商（negotiateAsync）互斥，避免同一 PC 上并发协商
func (c *ClientInfo) applyClientOffer(sdpStr string) {
	if c.session == nil || c.isClosed() {
		return
	}

	c.negMu.Lock()
	defer c.negMu.Unlock()

	pc := c.session.GetPeerConnection()
	if pc.SignalingState() != webrtc.SignalingStateStable {
		log.Warnf("[Signaling] Session %s 处理 Offer 时 PC 状态=%v，放弃本次",
			c.sessionID, pc.SignalingState())
		return
	}

	// 设置远端描述
	if err := c.session.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdpStr,
	}); err != nil {
		log.Errorf("[Signaling] 设置远端描述失败: %v", err)
		c.sendError("设置远端描述失败: " + err.Error())
		return
	}

	// 远端描述已就绪，补加此前提前到达并暂存的 ICE 候选
	c.flushPendingCandidates()

	// 创建应答
	answer, err := c.session.CreateAnswer()
	if err != nil {
		log.Errorf("[Signaling] 创建应答失败: %v", err)
		c.sendError("创建应答失败: " + err.Error())
		return
	}

	// 设置本地描述
	if err := c.session.SetLocalDescription(answer); err != nil {
		log.Errorf("[Signaling] 设置本地描述失败: %v", err)
		c.sendError("设置本地描述失败: " + err.Error())
		return
	}

	// 发送应答给浏览器
	c.sendMessage(SignalingMessage{
		Type: "answer",
		Data: map[string]interface{}{
			"sdp": answer.SDP,
		},
	})

	log.Infof("[Signaling] Session %s 完成 Offer/Answer 交换", c.sessionID)
}

// handleAnswer 处理浏览器发来的 Answer SDP（服务器发起重协商时）
func (c *ClientInfo) handleAnswer(data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	sdpData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("answer 参数格式错误")
		return
	}

	sdpStr, _ := sdpData["sdp"].(string)
	if sdpStr == "" {
		c.sendError("SDP 不能为空")
		return
	}

	// 仅在服务端处于 have-local-offer（等待自己发出 Offer 的 Answer）时才处理，
	// 否则属于迟到或多余的 Answer，直接忽略，避免状态机报错
	if pc := c.session.GetPeerConnection(); pc.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
		log.Warnf("[Signaling] Session %s PC 状态=%v，忽略本次 Answer", c.sessionID, pc.SignalingState())
		return
	}

	err := c.session.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdpStr,
	})
	if err != nil {
		log.Errorf("[Signaling] 设置 Answer 远端描述失败: %v", err)
		c.sendError("设置 Answer 失败: " + err.Error())
		return
	}

	// 远端描述已就绪，补加此前提前到达并暂存的 ICE 候选
	c.flushPendingCandidates()

	log.Infof("[Signaling] Session %s Answer 处理完成", c.sessionID)
}

// handleICECandidate 处理浏览器发来的 ICE 候选
func (c *ClientInfo) handleICECandidate(data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	candData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("ice_candidate 参数格式错误")
		return
	}

	candidateStr, _ := candData["candidate"].(string)
	if candidateStr == "" {
		c.sendError("candidate 不能为空")
		return
	}

	// 构建 ICE 候选
	candidate := webrtc.ICECandidateInit{
		Candidate: candidateStr,
	}

	// 可选的 sdpMid 和 sdpMLineIndex
	if sdpMid, ok := candData["sdpMid"].(string); ok {
		candidate.SDPMid = &sdpMid
	}
	if sdpMLineIndex, ok := candData["sdpMLineIndex"].(float64); ok {
		idx := uint16(sdpMLineIndex)
		candidate.SDPMLineIndex = &idx
	}

	// 若远端描述尚未设置（Trickle ICE 候选早于 Offer/Answer 到达），
	// 直接 AddICECandidate 会抛 "remote description is not set"。
	// 此处先暂存，待远端描述就绪后再统一补加，避免候选丢失导致连通失败
	if c.session.GetPeerConnection().RemoteDescription() == nil {
		c.mu.Lock()
		c.pendingCandidates = append(c.pendingCandidates, candidate)
		pending := len(c.pendingCandidates)
		c.mu.Unlock()
		log.Infof("[Signaling] Session %s 远端描述未就绪，暂存 ICE 候选(共 %d 个)",
			c.sessionID, pending)
		return
	}

	if err := c.session.AddICECandidate(candidate); err != nil {
		log.Warnf("[Signaling] 添加 ICE 候选失败: %v", err)
		// 不返回错误，避免影响流程
	}
}

// flushPendingCandidates 将暂存的 ICE 候选补加到 PC
// 必须在 SetRemoteDescription（远端描述就绪）成功之后调用
func (c *ClientInfo) flushPendingCandidates() {
	c.mu.Lock()
	cands := c.pendingCandidates
	c.pendingCandidates = nil
	c.mu.Unlock()

	if len(cands) == 0 {
		return
	}

	log.Infof("[Signaling] Session %s 远端描述已就绪，补加暂存的 ICE 候选 %d 个",
		c.sessionID, len(cands))

	for _, cand := range cands {
		if c.isClosed() || c.session == nil {
			return
		}
		if err := c.session.AddICECandidate(cand); err != nil {
			log.Warnf("[Signaling] 补加 ICE 候选失败: %v", err)
		}
	}
}

// handleSubscribe 处理订阅轨道请求
func (c *ClientInfo) handleSubscribe(data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	subData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("subscribe 参数格式错误")
		return
	}

	publisherID, _ := subData["publisherId"].(string)
	trackID, _ := subData["trackId"].(string)

	if publisherID == "" || trackID == "" {
		c.sendError("publisherId 和 trackId 不能为空")
		return
	}

	room, ok := roomManager.GetRoom(c.roomID)
	if !ok {
		c.sendError("房间不存在")
		return
	}

	if err := room.SubscribeTrack(c.sessionID, publisherID, trackID); err != nil {
		log.Errorf("[Signaling] 订阅失败: %v", err)
		c.sendError("订阅失败: " + err.Error())
		return
	}

	// 订阅成功后，需要服务器发起重协商（发送 Offer）
	// 因为 AddTrack 会触发 negotiationneeded
	c.negotiate()

	log.Infof("[Signaling] Session %s 订阅成功 publisher=%s track=%s",
		c.sessionID, publisherID, trackID)
}

// handleUnsubscribe 处理取消订阅
func (c *ClientInfo) handleUnsubscribe(data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	subData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("unsubscribe 参数格式错误")
		return
	}

	publisherID, _ := subData["publisherId"].(string)
	trackID, _ := subData["trackId"].(string)

	room, ok := roomManager.GetRoom(c.roomID)
	if !ok {
		return
	}

	room.UnsubscribeTrack(c.sessionID, publisherID, trackID)

	// 取消订阅后重协商
	c.negotiate()
}

// handleListTracks 获取房间内所有轨道
func (c *ClientInfo) handleListTracks() {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	room, ok := roomManager.GetRoom(c.roomID)
	if !ok {
		c.sendError("房间不存在")
		return
	}

	tracks := room.GetAllTracks()
	c.sendMessage(SignalingMessage{
		Type: "tracks_list",
		Data: map[string]interface{}{
			"tracks": tracks,
		},
	})
}

// negotiate 服务器发起重协商（发送 Offer）
// 当 AddTrack/RemoveTrack 后需要重新协商
// 异步执行 + 串行化，避免同一个 PC 上并发发起多次 offer 导致协商冲突
func (c *ClientInfo) negotiate() {
	go c.negotiateAsync(false)
}

// negotiateICERestart 服务器发起 ICE 重启重协商
// 用于 ICE 长时间断开后的兜底恢复：生成携带新 ICE 用户名/密码的 Offer，
// 让两端重新收集候选并重新打洞，从而尽量在不中断会话的情况下恢复连接
func (c *ClientInfo) negotiateICERestart() {
	go c.negotiateAsync(true)
}

// handleNegotiationState 记录客户端的协商状态
// 客户端在开始协商前上报 busy=true，协商结束后上报 busy=false；
// 服务器据此避免在客户端协商过程中发起 offer，从根本上避免 glare（协商冲突）
func (c *ClientInfo) handleNegotiationState(data interface{}) {
	stateData, ok := data.(map[string]interface{})
	if !ok {
		return
	}
	busy, _ := stateData["busy"].(bool)
	if busy {
		atomic.StoreInt32(&c.clientBusy, 1)
	} else {
		atomic.StoreInt32(&c.clientBusy, 0)
	}
}

// handleRenegotiateRetry 客户端请求稍后重试重协商
// 客户端本地正在协商时无法接受服务器 Offer，会请求服务器延迟重试，
// 这样服务器无需回滚，只需等客户端空闲后重新发起，避免协商冲突
func (c *ClientInfo) handleRenegotiateRetry(data interface{}) {
	log.Infof("[Signaling] Session %s 请求延迟重试重协商", c.sessionID)
	go func() {
		time.Sleep(600 * time.Millisecond)
		c.negotiate()
	}()
}

// negotiateAsync 重协商的实际执行体
// 关键：必须同时满足"服务端 PC 空闲"和"客户端空闲"才发起 offer，
// 否则双方同时发 offer 会导致 DTLS 角色冲突（Failed to set SSL role for the transport）
// iceRestart 为 true 时生成 ICE 重启 Offer（携带新 ICE 凭据）
func (c *ClientInfo) negotiateAsync(iceRestart bool) {
	if c.session == nil {
		return
	}

	// 串行化：同一时刻只允许一个重协商流程
	c.negMu.Lock()
	defer c.negMu.Unlock()

	pc := c.session.GetPeerConnection()

	// 等待双方都回到空闲状态（最多 10 秒）
	for i := 0; i < 100; i++ {
		if c.isClosed() {
			return
		}
		selfIdle := pc.SignalingState() == webrtc.SignalingStateStable
		clientIdle := atomic.LoadInt32(&c.clientBusy) == 0
		if selfIdle && clientIdle {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if c.isClosed() {
		return
	}
	if pc.SignalingState() != webrtc.SignalingStateStable {
		log.Warnf("[Signaling] Session %s 跳过本次重协商，PC 状态=%v",
			c.sessionID, pc.SignalingState())
		return
	}

	// 创建 Offer
	// iceRestart=true 时携带 ICE 重启选项，生成新的 ICE 用户名/密码，
	// 让两端重新收集候选并重新打洞，用于恢复已断开的连接
	var offer webrtc.SessionDescription
	var err error
	if iceRestart {
		offer, err = pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	} else {
		offer, err = pc.CreateOffer(nil)
	}
	if err != nil {
		log.Errorf("[Signaling] 创建 Offer 失败: %v", err)
		return
	}

	// 设置本地描述
	if err := pc.SetLocalDescription(offer); err != nil {
		log.Errorf("[Signaling] 设置本地描述失败: %v", err)
		return
	}

	// 发送 Offer 给浏览器
	c.sendMessage(SignalingMessage{
		Type: "offer",
		Data: map[string]interface{}{
			"sdp": offer.SDP,
		},
	})

	if iceRestart {
		log.Infof("[Signaling] Session %s 发起 ICE 重启重协商 Offer", c.sessionID)
	} else {
		log.Infof("[Signaling] Session %s 发起重协商 Offer", c.sessionID)
	}
}

// isClosed 读取 closed 标志（加锁）
func (c *ClientInfo) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// onSessionEvent Session 事件回调
// 当 Session 有状态变化时（如 ICE candidate、发布/取消发布轨道），通过此回调处理
func (c *ClientInfo) onSessionEvent(event SessionEvent, sessionID string, data interface{}) {
	switch event {
	case "ice_candidate":
		// ICE 候选直接发给本客户端
		candidate, ok := data.(webrtc.ICECandidateInit)
		if !ok {
			return
		}
		c.sendMessage(SignalingMessage{
			Type: "ice_candidate",
			Data: candidate,
		})

	case EventTrackPublished, EventTrackUnpublished:
		// 轨道发布/取消发布：交给房间做有序分发，通知房间内其他 Session
		if roomManager != nil {
			if room, ok := roomManager.GetRoom(c.roomID); ok {
				room.EnqueueSessionEvent(event, sessionID, data)
			}
		}

	case EventIceRestartNeeded:
		// 会话 ICE 长时间断开：由服务端兜底发起一次 ICE 重启重协商
		if c.session == nil || c.isClosed() {
			return
		}
		log.Warnf("[Signaling] Session %s 触发服务端兜底 ICE 重启", c.sessionID)
		c.negotiateICERestart()
	}
}

// heartbeatLoop 心跳检测循环
// 服务端定时 ping 客户端，超时未收到 pong 则断连
func (c *ClientInfo) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			// 检查是否超时（30 秒未收到 pong）
			if time.Since(c.lastPong) > 30*time.Second {
				c.mu.Unlock()
				log.Warnf("[Signaling] Session %s 心跳超时，断连", c.sessionID)
				c.conn.Close()
				return
			}
			c.mu.Unlock()

			// 发送 ping
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				log.Warnf("[Signaling] 发送 ping 失败 session=%s err=%v", c.sessionID, err)
				return
			}
		}
	}
}

// sendPong 回复客户端 ping
func (c *ClientInfo) sendPong() {
	c.sendMessage(SignalingMessage{
		Type: "pong",
		Data: nil,
	})
}

// sendMessage 向客户端发送消息
func (c *ClientInfo) sendMessage(msg SignalingMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.conn == nil {
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Warnf("[Signaling] 消息序列化失败: %v", err)
		return
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Warnf("[Signaling] 发送消息失败 session=%s type=%s err=%v",
			c.sessionID, msg.Type, err)
	}
}

// sendError 向客户端发送错误消息
func (c *ClientInfo) sendError(errMsg string) {
	c.sendMessage(SignalingMessage{
		Type: "error",
		Data: map[string]interface{}{
			"message": errMsg,
		},
	})
}

// cleanup 清理客户端连接
func (c *ClientInfo) cleanup() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	// 从客户端映射中移除
	// 仅当映射中登记的仍然是本连接时才删除，
	// 避免"顶替旧连接"场景下旧连接的异步清理误删已接管的新连接
	if c.sessionID != "" {
		clientsMu.Lock()
		if cur, ok := clients[c.sessionID]; ok && cur == c {
			delete(clients, c.sessionID)
		}
		clientsMu.Unlock()
	}

	// 关闭 Session
	if c.session != nil {
		c.session.Close()
	}

	// 关闭 WebSocket 连接
	if c.conn != nil {
		_ = c.conn.Close()
	}

	log.Infof("[Signaling] 客户端连接已清理 session=%s", c.sessionID)
}

// sendToClient 向指定 sessionID 的客户端发送消息
// 供 room.go 的事件通知调用
func sendToClient(sessionID string, msg SignalingMessage) {
	clientsMu.RLock()
	client, ok := clients[sessionID]
	clientsMu.RUnlock()

	if !ok {
		return
	}
	client.sendMessage(msg)
}

// ============================================================
// 通话信令处理（视频通话 / 语音通话）
// ============================================================

// forwardCallMessage 通用通话消息转发
// 从 data 中解析 to 字段，将消息转发给目标用户，并附加 from 字段
func (c *ClientInfo) forwardCallMessage(msgType string, data interface{}) {
	if c.session == nil {
		c.sendError("请先加入房间")
		return
	}

	callData, ok := data.(map[string]interface{})
	if !ok {
		c.sendError("通话消息参数格式错误")
		return
	}

	to, _ := callData["to"].(string)
	if to == "" {
		c.sendError("缺少目标用户 to")
		return
	}

	// 构造转发消息，附加 from（主叫/被叫自己）
	forwardData := make(map[string]interface{})
	for k, v := range callData {
		forwardData[k] = v
	}
	forwardData["from"] = c.sessionID
	// 附带主叫显示昵称，方便被叫界面直接展示（sessionID 仅用于寻址）
	forwardData["fromName"] = c.name

	log.Infof("[Signaling] 通话信令 %s: %s -> %s data=%v", msgType, c.sessionID, to, forwardData)

	sendToClient(to, SignalingMessage{
		Type: msgType,
		Data: forwardData,
	})
}

// handleCallRequest 处理通话请求（主叫方发起）
func (c *ClientInfo) handleCallRequest(data interface{}) {
	c.forwardCallMessage("call_request", data)
}

// handleCallAccept 处理通话接听（被叫方同意）
func (c *ClientInfo) handleCallAccept(data interface{}) {
	c.forwardCallMessage("call_accept", data)
}

// handleCallReject 处理通话拒接（被叫方拒绝）
func (c *ClientInfo) handleCallReject(data interface{}) {
	c.forwardCallMessage("call_reject", data)
}

// handleCallEnd 处理通话挂断（任一方挂断）
func (c *ClientInfo) handleCallEnd(data interface{}) {
	c.forwardCallMessage("call_end", data)
}
