// room.go
// 房间管理模块
// Room 管理房间内的所有 Session，负责：
//   - Session 上线/下线通知
//   - Track 发布/取消发布通知
//   - 事件有序分发（单 goroutine 处理事件队列，避免并发乱序）
//   - 房间自动销毁（无 Session 时）
//   - 单个房间最大 Session 数量限制
//   - 实时统计监控

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RoomConfig 房间配置
type RoomConfig struct {
	MaxSessions    int // 单个房间最大 Session 数量
	NackCacheSize  int // NACK 缓存包数量
	JitterBufferMs int // 抖动缓冲时间（毫秒）
}

// emptyRoomGrace 空房销毁宽限期。
// 用于跨越「断线重连/顶替旧连接」造成的极短空房瞬间，
// 避免房间被反复销毁后再重建。
const emptyRoomGrace = 3 * time.Second

// RoomEvent 房间事件（用于有序分发队列）
type RoomEvent struct {
	Type      SessionEvent // 事件类型
	SessionID string       // 触发事件的 Session ID
	Data      interface{}  // 事件数据
}

// Room 房间结构体
type Room struct {
	ID       string
	config   RoomConfig
	mu       sync.RWMutex
	sessions map[string]*Session // sessionID -> Session

	eventQueue chan *RoomEvent // 事件队列（有序分发）
	stopCh     chan struct{}   // 停止信号

	destroyOnce sync.Once // 保证房间只销毁一次

	// destroyTimer 空房延迟销毁定时器。
	// 断线重连/顶替旧连接时，服务端会先移除旧 Session 再加入新 Session，
	// 中间存在极短的空房瞬间（实测日志里出现过仅 423ms 的空房）。
	// 若一空就立刻销毁，会出现「销毁 → 立即重建」的抖动，事件循环与统计采样被反复重启。
	// 引入宽限期后可跨越这个瞬间，只在真正长时间无人时才销毁。
	destroyTimer *time.Timer

	// 统计
	createdAt      time.Time
	forwardPackets uint64 // 转发包计数
	lostPackets    uint64 // 丢包统计
}

// NewRoom 创建新房间
func NewRoom(id string, config RoomConfig) *Room {
	r := &Room{
		ID:         id,
		config:     config,
		sessions:   make(map[string]*Session),
		eventQueue: make(chan *RoomEvent, 1024),
		stopCh:     make(chan struct{}),
		createdAt:  time.Now(),
	}

	// 启动事件分发 goroutine
	go r.eventLoop()

	log.Infof("[Room %s] 房间已创建", id)
	return r
}

// eventLoop 事件处理循环（单 goroutine，保证事件有序分发）
func (r *Room) eventLoop() {
	log.Infof("[Room %s] 事件分发循环启动", r.ID)
	defer log.Infof("[Room %s] 事件分发循环结束", r.ID)

	for {
		select {
		case event := <-r.eventQueue:
			r.handleEvent(event)
		case <-r.stopCh:
			return
		}
	}
}

// handleEvent 处理单个事件
func (r *Room) handleEvent(event *RoomEvent) {
	// 通知房间内所有 Session（除了触发者自己）
	r.mu.RLock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.mu.RUnlock()

	for _, s := range sessions {
		if s.ID == event.SessionID {
			continue
		}
		// 通过 Session 的 onEvent 回调发送通知
		// 这里直接调用 Session 的信令发送方法
		r.notifySession(s, event)
	}

	// 事件处理完毕，检查房间是否需要销毁
	r.checkAndDestroyIfEmpty()
}

// notifySession 通知单个 Session 某个事件
func (r *Room) notifySession(s *Session, event *RoomEvent) {
	// 构造通知消息
	switch event.Type {
	case EventSessionJoin:
		// 通知有新 Session 加入，附带显示昵称（name 允许重复）
		name := ""
		if sess, ok := event.Data.(*Session); ok {
			name = sess.Name
		}
		msg := SignalingMessage{
			Type: "session_join",
			Data: map[string]interface{}{
				"sessionId": event.SessionID,
				"name":      name,
			},
		}
		sendToClient(s.ID, msg)
		// 关键兜底：紧接着再向该客户端下发一份「房间内除自己外的完整成员列表」。
		// 增量事件一旦出现时序错乱、消息丢失，就会导致某些客户端漏显成员
		// （典型现象：后加入的人看不到先加入的人，或在安卓端不显示）。
		// 全量列表是权威数据，前端收到后整体覆盖，可彻底消除这类不一致。
		sendToClient(s.ID, r.buildSessionsListMsg(s.ID))

	case EventSessionLeave:
		// 离开事件携带 name（由移除方在删除前取出）
		name, _ := event.Data.(string)
		msg := SignalingMessage{
			Type: "session_leave",
			Data: map[string]interface{}{
				"sessionId": event.SessionID,
				"name":      name,
			},
		}
		sendToClient(s.ID, msg)
		// 同上：有人离开后也下发一次全量列表兜底，保证列表与服务器状态一致
		sendToClient(s.ID, r.buildSessionsListMsg(s.ID))

	case EventTrackPublished:
		trackInfo, _ := event.Data.(TrackInfo)
		msg := SignalingMessage{
			Type: "track_published",
			Data: map[string]interface{}{
				"sessionId": trackInfo.SessionID,
				"name":      r.sessionName(trackInfo.SessionID),
				"trackId":   trackInfo.TrackID,
				"streamId":  trackInfo.StreamID,
				"kind":      trackInfo.Kind,
				"ssrc":      trackInfo.SSRC,
			},
		}
		sendToClient(s.ID, msg)

	case EventTrackUnpublished:
		trackInfo, _ := event.Data.(TrackInfo)
		msg := SignalingMessage{
			Type: "track_unpublished",
			Data: map[string]interface{}{
				"sessionId": trackInfo.SessionID,
				"trackId":   trackInfo.TrackID,
			},
		}
		sendToClient(s.ID, msg)
	}
}

// enqueueEvent 将事件放入队列（有序分发）
func (r *Room) enqueueEvent(event *RoomEvent) {
	select {
	case r.eventQueue <- event:
	default:
		// 队列已满，丢弃并告警
		log.Warnf("[Room %s] 事件队列已满，丢弃事件 type=%s session=%s",
			r.ID, event.Type, event.SessionID)
	}
}

// EnqueueSessionEvent 供 Session 回调使用：把 Session 事件送入房间的有序分发队列
// 由房间统一分发给房间内其他 Session，保证事件顺序一致
func (r *Room) EnqueueSessionEvent(event SessionEvent, sessionID string, data interface{}) {
	r.enqueueEvent(&RoomEvent{
		Type:      event,
		SessionID: sessionID,
		Data:      data,
	})
}

// AddSession 添加 Session 到房间
func (r *Room) AddSession(session *Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查房间最大 Session 数
	if len(r.sessions) >= r.config.MaxSessions {
		return fmt.Errorf("房间 %s 已达到最大 Session 数量 %d", r.ID, r.config.MaxSessions)
	}

	// sessionID 冲突检查
	if _, exists := r.sessions[session.ID]; exists {
		return fmt.Errorf("sessionID %s 已存在于房间 %s", session.ID, r.ID)
	}

	session.room = r
	r.sessions[session.ID] = session

	// 有 Session 加入：取消可能存在的空房销毁倒计时，
	// 避免「刚加入就被上一轮倒计时销毁」的竞态
	if r.destroyTimer != nil {
		r.destroyTimer.Stop()
		r.destroyTimer = nil
	}

	log.Infof("[Room %s] Session %s 加入，当前在线 %d 人",
		r.ID, session.ID, len(r.sessions))

	// 入队事件：通知其他 Session
	r.enqueueEvent(&RoomEvent{
		Type:      EventSessionJoin,
		SessionID: session.ID,
		Data:      session,
	})

	return nil
}

// RemoveSession 从房间移除 Session
func (r *Room) RemoveSession(sessionID string) {
	r.removeSession(sessionID, nil)
}

// RemoveSessionIf 仅当房间内 sessionID 对应的确实是 s 时才移除。
// 用于断线重连场景：新连接可能已复用同一 sessionID，
// 此时旧 Session 的异步 Close 不能误删新 Session
func (r *Room) RemoveSessionIf(sessionID string, s *Session) {
	r.removeSession(sessionID, s)
}

// removeSession 移除 Session 的内部实现
// only 非 nil 时表示"仅当房间内该 ID 对应的对象就是 only 时才移除"
func (r *Room) removeSession(sessionID string, only *Session) {
	r.mu.Lock()
	cur, exists := r.sessions[sessionID]
	// ID 已被新会话复用：不做任何处理，避免误删新会话
	if exists && only != nil && cur != only {
		exists = false
	}
	// 在移除前取出显示昵称，供离开事件广播使用
	leaveName := ""
	if exists {
		if cur != nil {
			leaveName = cur.Name
		}
		delete(r.sessions, sessionID)
	}
	count := len(r.sessions)
	r.mu.Unlock()

	if !exists {
		return
	}

	log.Infof("[Room %s] Session %s 离开，当前在线 %d 人", r.ID, sessionID, count)

	// 入队事件：通知其他 Session（携带离开者的显示昵称）
	r.enqueueEvent(&RoomEvent{
		Type:      EventSessionLeave,
		SessionID: sessionID,
		Data:      leaveName,
	})

	// 检查是否需要销毁房间
	r.checkAndDestroyIfEmpty()
}

// checkAndDestroyIfEmpty 房间为空时「延迟销毁」。
//
// 为什么不立即销毁：断线重连或顶替旧连接时，服务端会先移除旧 Session
// 再加入新 Session，中间存在极短的空房瞬间。若一空就立刻销毁，
// 会造成房间被反复销毁/重建（日志中可见 423ms 的极短生命周期）。
// 这里给出空房宽限期：宽限期内只要有 Session 加入即取消销毁；
// 到期仍为空才真正移除房间。
func (r *Room) checkAndDestroyIfEmpty() {
	r.mu.Lock()

	if len(r.sessions) > 0 {
		// 已有人：撤销待执行的销毁倒计时
		if r.destroyTimer != nil {
			r.destroyTimer.Stop()
			r.destroyTimer = nil
		}
		r.mu.Unlock()
		return
	}

	if r.destroyTimer != nil {
		// 已有销毁倒计时在跑，无需重复启动
		r.mu.Unlock()
		return
	}

	r.destroyTimer = time.AfterFunc(emptyRoomGrace, func() {
		r.mu.Lock()
		stillEmpty := len(r.sessions) == 0
		r.destroyTimer = nil
		r.mu.Unlock()

		if stillEmpty {
			roomManager.RemoveRoom(r.ID)
		}
	})
	r.mu.Unlock()
}

// GetSession 获取房间内的 Session
func (r *Room) GetSession(sessionID string) (*Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[sessionID]
	return s, ok
}

// sessionName 获取房间内某 Session 的显示昵称（不存在时返回空串）
func (r *Room) sessionName(sessionID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.sessions[sessionID]; ok && s != nil {
		return s.Name
	}
	return ""
}

// GetAllSessions 获取房间内所有 Session
func (r *Room) GetAllSessions() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// buildSessionsListMsg 构造「房间内除 exceptID 之外所有成员」的完整列表消息。
// 每项包含唯一 sessionId（信令寻址/拨号用）与显示昵称 name（界面展示用）。
// 这是在线联系人列表的权威数据来源：客户端收到后整体覆盖本地列表即可，
// 从而不依赖任何增量事件的到达顺序，彻底避免漏显成员的问题。
func (r *Room) buildSessionsListMsg(exceptID string) SignalingMessage {
	others := make([]map[string]string, 0)
	for _, s := range r.GetAllSessions() {
		if s == nil || s.ID == exceptID {
			continue
		}
		others = append(others, map[string]string{
			"sessionId": s.ID,
			"name":      s.Name,
		})
	}
	return SignalingMessage{
		Type: "sessions_list",
		Data: map[string]interface{}{
			"sessions": others,
		},
	}
}

// SessionCount 获取房间内 Session 数量
func (r *Room) SessionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

// GetAllTracks 获取房间内所有已发布的轨道
func (r *Room) GetAllTracks() []TrackInfo {
	r.mu.RLock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.mu.RUnlock()

	var tracks []TrackInfo
	for _, s := range sessions {
		tracks = append(tracks, s.GetPublishedTracks()...)
	}
	return tracks
}

// SubscribeTrack 订阅房间内某条轨道
// subscriberID: 订阅者 sessionID
// publisherID: 发布者 sessionID
// trackID: 轨道 ID
func (r *Room) SubscribeTrack(subscriberID, publisherID, trackID string) error {
	publisher, ok := r.GetSession(publisherID)
	if !ok {
		return fmt.Errorf("发布者 Session 不存在: %s", publisherID)
	}

	subscriber, ok := r.GetSession(subscriberID)
	if !ok {
		return fmt.Errorf("订阅者 Session 不存在: %s", subscriberID)
	}

	if publisherID == subscriberID {
		return fmt.Errorf("不能订阅自己发布的轨道")
	}

	// 调用发布者的 AddSubscriber
	localTrack, ssrc, err := publisher.AddSubscriber(trackID, subscriber)
	if err != nil {
		return fmt.Errorf("订阅失败: %w", err)
	}

	log.Infof("[Room %s] Session %s 订阅了 %s 的轨道 %s ssrc=%d",
		r.ID, subscriberID, publisherID, trackID, ssrc)

	// 通知订阅者订阅成功，需要重新协商（发送 offer）
	// 由于 pion 的 AddTrack 会触发 negotiationneeded，由信令模块处理
	_ = localTrack
	return nil
}

// UnsubscribeTrack 取消订阅
func (r *Room) UnsubscribeTrack(subscriberID, publisherID, trackID string) {
	publisher, ok := r.GetSession(publisherID)
	if !ok {
		return
	}
	publisher.RemoveSubscriber(trackID, subscriberID)
	log.Infof("[Room %s] Session %s 取消订阅 %s 的轨道 %s",
		r.ID, subscriberID, publisherID, trackID)
}

// GetStats 获取房间统计信息
func (r *Room) GetStats() RoomStats {
	r.mu.RLock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.mu.RUnlock()

	var totalPacketsSent, totalPacketsRecv, totalBytesSent, totalBytesRecv uint64
	for _, s := range sessions {
		bs, br, ps, pr := s.GetStats()
		totalBytesSent += bs
		totalBytesRecv += br
		totalPacketsSent += ps
		totalPacketsRecv += pr
	}

	return RoomStats{
		RoomID:         r.ID,
		SessionCount:   len(sessions),
		PacketsSent:    totalPacketsSent,
		PacketsRecv:    totalPacketsRecv,
		BytesSent:      totalBytesSent,
		BytesRecv:      totalBytesRecv,
		ForwardPackets: atomic.LoadUint64(&r.forwardPackets),
		CreatedAt:      r.createdAt,
	}
}

// RoomStats 房间统计信息
type RoomStats struct {
	RoomID         string
	SessionCount   int
	PacketsSent    uint64
	PacketsRecv    uint64
	BytesSent      uint64
	BytesRecv      uint64
	ForwardPackets uint64
	CreatedAt      time.Time
}

// Destroy 销毁房间
func (r *Room) Destroy() {
	// 保证只销毁一次，避免重复 close(stopCh) 导致 panic
	r.destroyOnce.Do(func() {
		log.Infof("[Room %s] 销毁房间", r.ID)

		// 停掉可能存在的空房销毁倒计时，避免定时器在销毁后再触发
		r.mu.Lock()
		if r.destroyTimer != nil {
			r.destroyTimer.Stop()
			r.destroyTimer = nil
		}
		r.mu.Unlock()

		// 停止事件循环
		close(r.stopCh)

		// 关闭所有 Session
		r.mu.Lock()
		sessions := r.sessions
		r.sessions = make(map[string]*Session)
		r.mu.Unlock()

		for _, s := range sessions {
			s.Close()
		}
	})
}

// ============================================================
// RoomManager 房间管理器
// ============================================================

// RoomManager 管理所有房间
type RoomManager struct {
	mu     sync.RWMutex
	rooms  map[string]*Room
	config RoomConfig

	// 实时速率采样：记录每个房间上一次统计时的累计字节与时间戳，
	// 两次采样做差即可得到该时间窗口内的平均速率（kbps）。
	rateMu      sync.Mutex
	rateSamples map[string]*rateSample
}

// rateSample 某个房间在上一次统计采样时的字节数与时间
type rateSample struct {
	bytesSent uint64    // 上次采样时房间累计发送字节
	bytesRecv uint64    // 上次采样时房间累计接收字节
	ts        time.Time // 上次采样时间
}

// NewRoomManager 创建房间管理器
func NewRoomManager(config RoomConfig) *RoomManager {
	return &RoomManager{
		rooms:       make(map[string]*Room),
		config:      config,
		rateSamples: make(map[string]*rateSample),
	}
}

// GetOrCreateRoom 获取或创建房间
func (rm *RoomManager) GetOrCreateRoom(roomID string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	room, ok := rm.rooms[roomID]
	if !ok {
		room = NewRoom(roomID, rm.config)
		rm.rooms[roomID] = room
	}
	return room
}

// GetRoom 获取房间
func (rm *RoomManager) GetRoom(roomID string) (*Room, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	room, ok := rm.rooms[roomID]
	return room, ok
}

// RemoveRoom 移除房间
func (rm *RoomManager) RemoveRoom(roomID string) {
	rm.mu.Lock()
	room, ok := rm.rooms[roomID]
	if ok {
		delete(rm.rooms, roomID)
	}
	rm.mu.Unlock()

	if ok {
		room.Destroy()
		log.Infof("[Room %s] 房间已从管理器中移除", roomID)
	}
}

// ListRooms 列出所有房间
func (rm *RoomManager) ListRooms() []*Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	rooms := make([]*Room, 0, len(rm.rooms))
	for _, r := range rm.rooms {
		rooms = append(rooms, r)
	}
	return rooms
}

// calcRates 根据两次采样之间的字节差，计算房间实时速率（单位 kbps）
// 说明：以服务器为观察点，
//   - 上行 = 服务器接收到的字节增量（即客户端上传的流量）
//   - 下行 = 服务器发送出去的字节增量（即下发给客户端的流量）
//
// 首次采样没有历史数据，速率返回 0；房间被重建导致计数回退时也按 0 处理。
func (rm *RoomManager) calcRates(roomID string, bytesSent, bytesRecv uint64, now time.Time) (float64, float64) {
	rm.rateMu.Lock()
	defer rm.rateMu.Unlock()

	if rm.rateSamples == nil {
		rm.rateSamples = make(map[string]*rateSample)
	}

	prev, ok := rm.rateSamples[roomID]
	rm.rateSamples[roomID] = &rateSample{bytesSent: bytesSent, bytesRecv: bytesRecv, ts: now}
	if !ok {
		return 0, 0
	}

	elapsed := now.Sub(prev.ts).Seconds()
	if elapsed <= 0 {
		return 0, 0
	}

	// 防止房间重建导致累计值回退，无符号相减出现巨大数字
	var upDelta, downDelta uint64
	if bytesRecv >= prev.bytesRecv {
		upDelta = bytesRecv - prev.bytesRecv
	}
	if bytesSent >= prev.bytesSent {
		downDelta = bytesSent - prev.bytesSent
	}

	// 字节 * 8 / 秒 / 1000 = kbps
	upKbps := float64(upDelta) * 8 / elapsed / 1000
	downKbps := float64(downDelta) * 8 / elapsed / 1000
	return upKbps, downKbps
}

// cleanupRateSamples 清理已销毁房间的历史采样，避免长期运行时 map 无限增长
func (rm *RoomManager) cleanupRateSamples(rooms []*Room) {
	rm.rateMu.Lock()
	defer rm.rateMu.Unlock()

	if len(rm.rateSamples) == 0 {
		return
	}
	active := make(map[string]struct{}, len(rooms))
	for _, room := range rooms {
		active[room.ID] = struct{}{}
	}
	for id := range rm.rateSamples {
		if _, ok := active[id]; !ok {
			delete(rm.rateSamples, id)
		}
	}
}

// PrintAllStats 打印所有房间的统计信息
func (rm *RoomManager) PrintAllStats() {
	rooms := rm.ListRooms()
	if len(rooms) == 0 {
		log.Infof("[Stats] 当前无活跃房间")
		rm.cleanupRateSamples(nil)
		return
	}

	log.Infof("[Stats] ====== 实时统计 ======")
	now := time.Now()
	for _, room := range rooms {
		stats := room.GetStats()

		// 计算该房间自上次采样以来的实时上下行速率（kbps）
		upKbps, downKbps := rm.calcRates(room.ID, stats.BytesSent, stats.BytesRecv, now)

		log.Infof("[Stats] 房间=%s 在线人数=%d 上行=%.1fkbps 下行=%.1fkbps 转发包=%d 发送包=%d 接收包=%d 发送字节=%d 接收字节=%d 存活时间=%s",
			stats.RoomID, stats.SessionCount, upKbps, downKbps, stats.ForwardPackets,
			stats.PacketsSent, stats.PacketsRecv,
			stats.BytesSent, stats.BytesRecv,
			time.Since(stats.CreatedAt).Round(time.Second))
	}
	log.Infof("[Stats] ======================")

	rm.cleanupRateSamples(rooms)
}

// roomManager 全局房间管理器
var roomManager *RoomManager

// initRoomManager 初始化全局房间管理器
func initRoomManager(config RoomConfig) {
	roomManager = NewRoomManager(config)
}
