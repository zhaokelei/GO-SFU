// session.go
// Session 表示一个浏览器客户端与 SFU 服务器之间的 WebRTC 会话
// 封装 pion/webrtc 的 PeerConnection，负责：
//   - 接收浏览器推流（发布 Track）
//   - 向浏览器转发其他 Session 的 Track（订阅）
//   - RTCP 双向透传（SR/RR/PLI/NACK）
//   - SSRC 映射与隔离
//   - 连接状态监听与资源清理

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

// SessionEvent 房间内事件类型
type SessionEvent string

const (
	EventSessionJoin      SessionEvent = "session_join"      // Session 加入房间
	EventSessionLeave     SessionEvent = "session_leave"     // Session 离开房间
	EventTrackPublished   SessionEvent = "track_published"   // 发布了新 Track
	EventTrackUnpublished SessionEvent = "track_unpublished" // 取消发布 Track

	// EventIceRestartNeeded 会话 ICE 长时间处于断开状态，
	// 请求信令层主动发起一次 ICE 重启（携带新 ICE 凭据的重协商 Offer）。
	EventIceRestartNeeded SessionEvent = "ice_restart_needed"
)

// NACK 未命中限流参数
// 弱网下客户端会对同一丢失区间反复发送 NACK，若缓存已无对应包则永远无法恢复，
// 反而形成风暴。cooldown 内的重复未命中被抑制，窗口阈值用于判定「同一区间」。
const (
	nackMissCooldown = 2 * time.Second // 同一丢失区间的冷却时间
	nackMissWindow   = 128             // 判定同一丢失区间的序列号跨度

	// ICE disconnected 宽限期：disconnected 在弱网下常可自愈，
	// 宽限期内恢复为 connected/completed 则取消关闭，避免误杀正常连接
	iceDisconnectGrace = 15 * time.Second

	// ICE 断开后服务端兜底发起 ICE 重启的延时。
	// 前端在检测到断开时也会自行 restartIce，此处留出时间让前端先尝试恢复；
	// 若宽限期内仍然后端断开，才由服务端兜底发起一次 ICE 重启重协商。
	iceRestartFallbackDelay = 6 * time.Second
)

// SessionInfo 会话信息（通知前端用）
type SessionInfo struct {
	SessionID string      `json:"sessionId"`
	RoomID    string      `json:"roomId"`
	Tracks    []TrackInfo `json:"tracks"`
}

// TrackInfo 轨道信息
type TrackInfo struct {
	TrackID   string `json:"trackId"`
	StreamID  string `json:"streamId"`
	Kind      string `json:"kind"` // audio / video
	SSRC      uint32 `json:"ssrc"`
	SessionID string `json:"sessionId"` // 发布者 sessionId
}

// PublishedTrack 发布的轨道（本 Session 推流上来的轨道）
type PublishedTrack struct {
	track       *webrtc.TrackRemote    // 远端轨道（浏览器推上来的）
	receiver    *webrtc.RTPReceiver    // 对应的 RTPReceiver
	publisherPC *webrtc.PeerConnection // 发布者的 PeerConnection（用于透传 PLI 给发布者）
	trackID     string                 // 轨道 ID
	streamID    string                 // 流 ID
	kind        webrtc.RTPCodecType    // 音频或视频
	ssrc        uint32                 // 原始 SSRC
	payloadType uint8                  // 载荷类型
	buffer      *RTPBuffer             // RTP 滑动窗口缓存（NACK 用）
	validator   *SeqValidator          // 序列号校验器
	nackLimiter *NackMissLimiter       // NACK 未命中限流器（防止 NACK 风暴）
	mu          sync.RWMutex
	subscribers map[string]*subscriberInfo // 订阅该轨道的 Session
	closed      bool
}

// subscriberInfo 订阅者信息
type subscriberInfo struct {
	session    *Session
	localTrack *webrtc.TrackLocalStaticRTP // 订阅端的本地转发轨道
	sender     *webrtc.RTPSender           // 对应的 RTPSender
	ssrc       uint32                      // 订阅端分配的 SSRC（与原始不同，用于映射）
}

// Session 会话结构体
// 不定义 Peer，直接使用 pion 的 webrtc.PeerConnection
type Session struct {
	ID     string                 // 唯一 sessionId（连接标识，非显示名）
	Name   string                 // 显示昵称（允许重复，仅用于界面展示）
	RoomID string                 // 所属房间 ID
	pc     *webrtc.PeerConnection // pion 的 WebRTC 连接对象
	api    *webrtc.API            // webrtc API 配置
	room   *Room                  // 所属房间（用于累计房间级转发统计）

	ctx    context.Context    // 退出信号
	cancel context.CancelFunc // 取消函数

	mu               sync.RWMutex
	publishedTracks  map[string]*PublishedTrack   // 本 Session 发布的轨道 trackID -> PublishedTrack
	subscribedTracks map[string]*SubscriptionInfo // 本 Session 订阅的轨道 trackID -> SubscriptionInfo
	ssrcMap          *SSRCMap                     // SSRC 映射表

	// 配置
	nackCacheSize  int // NACK 缓存大小
	jitterBufferMs int // 抖动缓冲时间

	// 统计
	bytesSent     uint64
	bytesReceived uint64
	packetsSent   uint64
	packetsRecv   uint64

	// 事件回调（通知房间）
	onEvent func(event SessionEvent, sessionID string, data interface{})

	once   sync.Once
	closed bool
}

// SubscriptionInfo 订阅信息（本 Session 订阅其他发布者的轨道）
type SubscriptionInfo struct {
	publisher  *Session                    // 发布者
	trackID    string                      // 轨道 ID
	localTrack *webrtc.TrackLocalStaticRTP // 本地转发轨道
	sender     *webrtc.RTPSender           // 对应的 RTPSender
	ssrc       uint32                      // 本端分配的 SSRC
	pubSSRC    uint32                      // 发布者原始 SSRC
	pubPT      uint8                       // 发布者载荷类型
}

// NewSession 创建新的 Session
func NewSession(id, name, roomID string, api *webrtc.API, nackCacheSize, jitterBufferMs int,
	onEvent func(event SessionEvent, sessionID string, data interface{})) (*Session, error) {

	// 创建 PeerConnection 配置。
	//
	// 【重要修复：服务端不再向公网 STUN 发起请求】
	// 之前这里会把 --stun 指定的公网 STUN（stun.chat.bilibili.com 等）也加入
	// 服务端 PeerConnection 的 ICEServers。服务端本身就在公网出口，去访问这些
	// 第三方 STUN 经常超时/不可达，导致服务端侧的 ICE 连通性检查长时间停留在
	// checking 状态（日志表现为「ICE 候选收集超时(15s)」「ICE failed」）。
	// 结果就是：客户端明明完成了 Offer/Answer 交换，却始终收不到服务端转发来的
	// 媒体 —— 也就是「第一次、第二次连接不出对方画面，刷新页面后再点就正常」。
	//
	// 服务端持有公网 IP，通过 main.go 的 SetNAT1To1IPs(Srflx) 已经能生成
	// host + srflx 候选，完全不需要外部 STUN，因此这里默认不配置任何 STUN。
	config := webrtc.Configuration{}

	// 仅当显式配置了外部 TURN 时才追加（服务端本身有公网 IP，通常无需）。
	// 真正给【客户端】兜底用的是内嵌 TURN，见 main.go 的 startEmbeddedTURN。
	if *turnURL != "" && *turnUser != "" && *turnPass != "" {
		config.ICEServers = append(config.ICEServers, webrtc.ICEServer{
			URLs:       []string{*turnURL},
			Username:   *turnUser,
			Credential: *turnPass,
		})
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("创建 PeerConnection 失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Session{
		ID:               id,
		Name:             name,
		RoomID:           roomID,
		pc:               pc,
		api:              api,
		ctx:              ctx,
		cancel:           cancel,
		publishedTracks:  make(map[string]*PublishedTrack),
		subscribedTracks: make(map[string]*SubscriptionInfo),
		ssrcMap:          NewSSRCMap(),
		nackCacheSize:    nackCacheSize,
		jitterBufferMs:   jitterBufferMs,
		onEvent:          onEvent,
	}

	// 注册事件监听
	s.registerHandlers()

	return s, nil
}

// registerHandlers 注册 PeerConnection 的事件回调
func (s *Session) registerHandlers() {
	pc := s.pc

	// 监听 ICE 连接状态变化
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Infof("[Session %s] ICE 连接状态变更: %s", s.ID, state.String())

		// 根据 ICE 状态决定是否关闭会话
		switch state {
		case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			// failed / closed 属于不可恢复状态，直接关闭会话
			log.Warnf("[Session %s] ICE 状态异常(%s)，关闭会话", s.ID, state.String())
			go s.Close()

		case webrtc.ICEConnectionStateDisconnected:
			// disconnected 在弱网下很常见（丢包、短暂拥塞），往往能自行恢复，
			// 因此不能立即关闭，否则会把只是瞬时抖动的正常连接误杀。
			// 这里给一个宽限期：期间若恢复为 connected/completed 则取消关闭；
			// 宽限期结束仍未恢复（或进一步变为 failed/closed）才真正关闭。
			log.Warnf("[Session %s] ICE 断开(disconnected)，进入 %s 宽限期等待恢复",
				s.ID, iceDisconnectGrace)
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()
				deadline := time.After(iceDisconnectGrace)
				// 兜底 ICE 重启定时器：只触发一次，触发后置 nil 以便 select 永久阻塞该分支
				restartTimer := time.After(iceRestartFallbackDelay)
				for {
					select {
					case <-s.ctx.Done():
						// 会话已被其他地方关闭，直接退出
						return
					case <-deadline:
						// 宽限期结束，仍未恢复则关闭会话
						st := s.pc.ICEConnectionState()
						if st == webrtc.ICEConnectionStateDisconnected ||
							st == webrtc.ICEConnectionStateFailed ||
							st == webrtc.ICEConnectionStateClosed {
							log.Warnf("[Session %s] ICE 宽限期(%s)结束仍为 %s，关闭会话",
								s.ID, iceDisconnectGrace, st.String())
							s.Close()
						}
						return
					case <-restartTimer:
						// 宽限期内仍未恢复：由服务端兜底发起一次 ICE 重启重协商，
						// 主动重新收集候选、重新打洞（前端也会自行 restartIce，
						// 此分支仅当前端侧恢复失败时才真正发挥作用）
						restartTimer = nil
						st := s.pc.ICEConnectionState()
						if st == webrtc.ICEConnectionStateDisconnected ||
							st == webrtc.ICEConnectionStateFailed {
							log.Warnf("[Session %s] ICE 断开已达 %s，服务端兜底发起 ICE 重启",
								s.ID, iceRestartFallbackDelay)
							if s.onEvent != nil {
								s.onEvent(EventIceRestartNeeded, s.ID, nil)
							}
						}
					case <-ticker.C:
						// 期间轮询：只要恢复为 connected/completed 就取消关闭
						st := s.pc.ICEConnectionState()
						if st == webrtc.ICEConnectionStateConnected ||
							st == webrtc.ICEConnectionStateCompleted {
							log.Infof("[Session %s] ICE 已在宽限期内恢复为 %s，取消关闭",
								s.ID, st.String())
							return
						}
					}
				}
			}()
		}

		// ICE 长时间收集不到候选，输出警告
		if state == webrtc.ICEConnectionStateChecking {
			go func() {
				select {
				case <-time.After(15 * time.Second):
					if s.pc.ICEConnectionState() == webrtc.ICEConnectionStateChecking {
						log.Warnf("[Session %s] ICE 候选收集超时(15s)，可能存在网络问题", s.ID)
					}
				case <-s.ctx.Done():
				}
			}()
		}
	})

	// 监听 PeerConnection 状态（包含 DTLS 握手状态）
	// pion v3 将 DTLS 状态聚合到 PeerConnectionState 中
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Infof("[Session %s] PeerConnection 状态变更: %s", s.ID, state.String())
		// PeerConnectionStateFailed 包含 DTLS 握手失败等情况
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed {
			go s.Close()
		}
	})

	// 监听 ICE 候选（Trickle ICE）
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			// 候选收集完成
			log.Infof("[Session %s] 本端 ICE 候选收集完成", s.ID)
			return
		}
		// 打印候选详情（SDP candidate 行含 类型/协议/地址/端口），
		// 用于排查 NAT 穿透问题：若只有 host 而没有 srflx/relay，
		// 说明 STUN/TURN 未生效，同网络（同一 NAT 出口）下极易打洞失败导致无法通话
		cand := c.ToJSON()
		log.Infof("[Session %s] 本端 ICE 候选: %s", s.ID, cand.Candidate)
		// 通过信令发送给浏览器
		if s.onEvent != nil {
			s.onEvent("ice_candidate", s.ID, cand)
		}
	})

	// 监听远端 Track（浏览器推流上来）
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		s.handlePublishedTrack(track, receiver)
	})

	// 监听 DataChannel（暂不使用，但注册避免告警）
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		log.Infof("[Session %s] 收到 DataChannel: %s", s.ID, dc.Label())
	})
}

// handlePublishedTrack 处理浏览器推流上来的 Track
func (s *Session) handlePublishedTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	trackID := track.ID()
	streamID := track.StreamID()
	kind := track.Kind()
	ssrc := uint32(track.SSRC())
	pt := uint8(track.PayloadType())

	// 兜底：部分浏览器在 SDP 中不回传 msid（或为空），会导致 track.ID() 为空。
	// 若为空则用 "kind-ssrc" 生成稳定且唯一的替代 ID，
	// 保证广播出去的 trackId 非空，对端订阅时不会因空 ID 被拒绝
	if trackID == "" {
		trackID = fmt.Sprintf("%s-%d", kind.String(), ssrc)
	}
	if streamID == "" {
		streamID = "stream-" + s.ID
	}

	log.Infof("[Session %s] 收到发布 Track: trackID=%s streamID=%s kind=%s ssrc=%d pt=%d",
		s.ID, trackID, streamID, kind.String(), ssrc, pt)

	// 创建发布轨道信息
	pubTrack := &PublishedTrack{
		track:       track,
		receiver:    receiver,
		publisherPC: s.pc,
		trackID:     trackID,
		streamID:    streamID,
		kind:        kind,
		ssrc:        ssrc,
		payloadType: pt,
		buffer:      NewRTPBuffer(ssrc, s.nackCacheSize),
		validator:   NewSeqValidator(3000),
		nackLimiter: NewNackMissLimiter(),
		subscribers: make(map[string]*subscriberInfo),
	}

	s.mu.Lock()
	s.publishedTracks[trackID] = pubTrack
	s.ssrcMap.Register(ssrc, trackID)
	s.mu.Unlock()

	// 通知房间：新 Track 发布
	if s.onEvent != nil {
		s.onEvent(EventTrackPublished, s.ID, TrackInfo{
			TrackID:   trackID,
			StreamID:  streamID,
			Kind:      kind.String(),
			SSRC:      ssrc,
			SessionID: s.ID,
		})
	}

	// 启动 RTP 读取循环
	go s.readRTPLoop(pubTrack)

	// 启动 RTCP 读取循环（处理订阅端发来的 PLI/NACK）
	go s.readPublisherRTCPLoop(pubTrack)
}

// readRTPLoop 从发布轨道读取 RTP 包，转发给所有订阅者
func (s *Session) readRTPLoop(pub *PublishedTrack) {
	log.Infof("[Session %s] 启动 RTP 读取循环 trackID=%s ssrc=%d", s.ID, pub.trackID, pub.ssrc)

	defer func() {
		log.Infof("[Session %s] RTP 读取循环结束 trackID=%s", s.ID, pub.trackID)
		// 读包循环结束说明该轨道已结束（客户端取消发布或重协商移除），
		// 清理发布记录、缓存与订阅者，并通知房间内其他 Session
		s.cleanupPublishedTrack(pub.trackID)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 读取 RTP 包
		pkt, _, err := pub.track.ReadRTP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Infof("[Session %s] RTP 读取 EOF trackID=%s", s.ID, pub.trackID)
				return
			}
			// 其他错误
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Warnf("[Session %s] RTP 读取错误 trackID=%s err=%v", s.ID, pub.trackID, err)
				// 短暂休眠避免 CPU 空转
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		// 1. 异常包检测
		if !ValidateRTP(pkt) {
			log.Warnf("[Session %s] 丢弃异常 RTP 包 trackID=%s seq=%d ssrc=%d",
				s.ID, pub.trackID, pkt.SequenceNumber, pkt.SSRC)
			continue
		}

		// 2. 序列号校验（过滤重复包）
		if !pub.validator.Check(pkt.SequenceNumber) {
			// 重复包，丢弃
			continue
		}

		// 3. 存入滑动窗口缓存（NACK 重传用）
		pub.buffer.Add(pkt)

		// 4. 转发给所有订阅者
		atomic.AddUint64(&s.packetsRecv, 1)
		atomic.AddUint64(&s.bytesReceived, uint64(len(pkt.Payload)))

		s.forwardToSubscribers(pub, pkt)
	}
}

// forwardToSubscribers 将 RTP 包转发给所有订阅者
func (s *Session) forwardToSubscribers(pub *PublishedTrack, pkt *rtp.Packet) {
	pub.mu.RLock()
	defer pub.mu.RUnlock()

	for subID, sub := range pub.subscribers {
		select {
		case <-sub.session.ctx.Done():
			continue
		default:
		}

		// 写入订阅者的本地轨道
		if err := sub.localTrack.WriteRTP(pkt); err != nil {
			log.Warnf("[Session %s] 转发 RTP 到订阅者 %s 失败 trackID=%s err=%v",
				s.ID, subID, pub.trackID, err)
			continue
		}

		atomic.AddUint64(&sub.session.packetsSent, 1)
		atomic.AddUint64(&sub.session.bytesSent, uint64(len(pkt.Payload)))

		// 累计房间级转发包计数（供监控统计使用）
		if s.room != nil {
			atomic.AddUint64(&s.room.forwardPackets, 1)
		}
	}
}

// requestKeyFrame 向发布者请求一个关键帧（发送 PLI）
// 当 NACK 未命中（目标包已不在缓存、无法重传）时调用，
// 这是让订阅端画面最快恢复的方式（关键帧到达后不再依赖丢失的历史包）
func (s *Session) requestKeyFrame(pub *PublishedTrack, reason string) {
	if pub.publisherPC == nil {
		return
	}
	if err := pub.publisherPC.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: pub.ssrc},
	}); err != nil {
		log.Warnf("[Session %s] 请求关键帧失败 trackID=%s: %v", s.ID, pub.trackID, err)
		return
	}
	log.Warnf("[Session %s] NACK 无法重传(%s)，已向发布者请求关键帧 trackID=%s",
		s.ID, reason, pub.trackID)
}

// readPublisherRTCPLoop 读取发布轨道的 RTCP 包
// 这里处理订阅端通过 RTCP 发来的 PLI 和 NACK
func (s *Session) readPublisherRTCPLoop(pub *PublishedTrack) {
	log.Infof("[Session %s] 启动发布端 RTCP 读取循环 trackID=%s", s.ID, pub.trackID)

	defer func() {
		log.Infof("[Session %s] 发布端 RTCP 读取循环结束 trackID=%s", s.ID, pub.trackID)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 从 receiver 读取 RTCP（返回 []rtcp.Packet, interceptor.Attributes, error）
		pkts, _, err := pub.receiver.ReadRTCP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-s.ctx.Done():
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		for _, pkt := range pkts {
			switch p := pkt.(type) {
			case *rtcp.PictureLossIndication:
				// PLI 请求关键帧，透传给发布者（这里就是浏览器推流端，由浏览器自动响应）
				log.Infof("[Session %s] 收到 PLI 请求 trackID=%s mediaSSRC=%d",
					s.ID, pub.trackID, p.MediaSSRC)
				// pion 会自动处理 PLI，这里只需记录日志

			case *rtcp.TransportLayerNack:
				// NACK 请求重传丢失的包
				lostSeqs := expandNackPairs(p.Nacks)

				// 从缓存中查找并重传
				for _, seq := range lostSeqs {
					cached, ok := pub.buffer.Get(seq)
					if !ok {
						// 未命中：目标包已不在缓存窗口内，反复请求也无法恢复。
						// 对同一丢失区间做冷却限流，冷却期内静默丢弃（防 NACK 风暴），
						// 冷却结束后触发一次 PLI，请求发布者发送关键帧以尽快恢复画面
						if pub.nackLimiter != nil &&
							pub.nackLimiter.ShouldTrigger(seq, nackMissCooldown, nackMissWindow) {
							s.requestKeyFrame(pub, fmt.Sprintf("发布端 NACK seq=%d", seq))
						}
						continue
					}
					// 重传给所有订阅者
					s.forwardToSubscribers(pub, cached)
				}

			case *rtcp.SenderReport:
				// 发布者发送的 SR，转发给订阅者（在订阅端的 RTCP 读取循环处理）
				// 这里不需要处理

			case *rtcp.ReceiverReport:
				// RR 报告，记录丢包率等
				for _, block := range p.Reports {
					if block.SSRC == pub.ssrc {
						fractionLost := float64(block.FractionLost) / 256.0
						if block.FractionLost > 0 {
							log.Warnf("[Session %s] 丢包报告 trackID=%s 丢包率=%.2f%% 累计丢包=%d",
								s.ID, pub.trackID, fractionLost*100, block.TotalLost)
						}
					}
				}
			}
		}
	}
}

// AddSubscriber 为发布轨道添加订阅者
// publisherTrackID: 发布轨道 ID
// subscriber: 订阅者 Session
// 返回订阅者的本地轨道和 SSRC
func (s *Session) AddSubscriber(publisherTrackID string, subscriber *Session) (*webrtc.TrackLocalStaticRTP, uint32, error) {
	s.mu.RLock()
	pub, ok := s.publishedTracks[publisherTrackID]
	s.mu.RUnlock()

	if !ok {
		return nil, 0, fmt.Errorf("发布轨道不存在: %s", publisherTrackID)
	}

	// 幂等保护：同一订阅者重复订阅同一轨道时，直接复用已存在的转发轨道。
	// 若不去重，会在订阅者 PeerConnection 上重复 AddTrack，导致对方收到多路相同音轨
	// 叠加播放：音量翻倍、相位错位，从而显著加重回声/啸叫。
	pub.mu.RLock()
	if existing, exists := pub.subscribers[subscriber.ID]; exists {
		pub.mu.RUnlock()
		return existing.localTrack, existing.ssrc, nil
	}
	pub.mu.RUnlock()

	// 创建转发轨道（静态 RTP 轨道，pion 会分配 SSRC）
	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		pub.track.Codec().RTPCodecCapability,
		pub.trackID,
		pub.streamID,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("创建转发轨道失败: %w", err)
	}

	// 添加到订阅者的 PeerConnection
	sender, err := subscriber.pc.AddTrack(localTrack)
	if err != nil {
		return nil, 0, fmt.Errorf("添加轨道到订阅者失败: %w", err)
	}

	// 记录订阅端分配的 SSRC（用于映射与日志）。Encodings 可能因实现差异为空，做防御
	var subSSRC uint32
	if params := sender.GetParameters(); len(params.Encodings) > 0 {
		subSSRC = uint32(params.Encodings[0].SSRC)
	}
	subInfo := &subscriberInfo{
		session:    subscriber,
		localTrack: localTrack,
		sender:     sender,
		ssrc:       subSSRC,
	}

	pub.mu.Lock()
	pub.subscribers[subscriber.ID] = subInfo
	pub.mu.Unlock()

	// 启动订阅端的 RTCP 读取循环（处理订阅者发来的 PLI/NACK，透传给发布者）
	go subscriber.readSubscriberRTCPLoop(pub, localTrack, sender)

	// 向订阅者记录订阅信息
	subscriber.mu.Lock()
	subscriber.subscribedTracks[publisherTrackID] = &SubscriptionInfo{
		publisher:  s,
		trackID:    publisherTrackID,
		localTrack: localTrack,
		sender:     sender,
		ssrc:       subSSRC,
		pubSSRC:    pub.ssrc,
		pubPT:      pub.payloadType,
	}
	subscriber.mu.Unlock()

	log.Infof("[Session %s] 添加订阅者 %s 到轨道 %s ssrc=%d",
		s.ID, subscriber.ID, publisherTrackID, subInfo.ssrc)

	return localTrack, subInfo.ssrc, nil
}

// readSubscriberRTCPLoop 读取订阅端的 RTCP 包
// 订阅者可能发送 PLI（请求关键帧）和 NACK（请求重传）
// 需要将这些 RTCP 透传给发布者
func (s *Session) readSubscriberRTCPLoop(pub *PublishedTrack, localTrack *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender) {
	log.Infof("[Session %s] 启动订阅端 RTCP 读取循环（订阅轨道 %s）", s.ID, pub.trackID)

	defer func() {
		log.Infof("[Session %s] 订阅端 RTCP 读取循环结束（轨道 %s）", s.ID, pub.trackID)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-s.ctx.Done():
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		for _, pkt := range pkts {
			switch p := pkt.(type) {
			case *rtcp.PictureLossIndication:
				// 订阅者请求关键帧，透传给发布者
				log.Infof("[Session %s] 订阅者请求 PLI，透传给发布者轨道 %s", s.ID, pub.trackID)
				// 通过发布者的 PeerConnection 发送 PLI 给发布者浏览器
				if err := pub.publisherPC.WriteRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{
						MediaSSRC: pub.ssrc,
					},
				}); err != nil {
					log.Warnf("[Session %s] 透传 PLI 失败: %v", s.ID, err)
				}

			case *rtcp.TransportLayerNack:
				// 订阅者请求重传丢失的包
				lostSeqs := expandNackPairs(p.Nacks)

				// 从发布者的缓存中查找并重传
				for _, seq := range lostSeqs {
					cached, ok := pub.buffer.Get(seq)
					if !ok {
						// 未命中：缓存中已无该包，重复请求无意义。做冷却限流避免
						// NACK 风暴，冷却结束后触发一次 PLI 请求关键帧恢复画面
						if pub.nackLimiter != nil &&
							pub.nackLimiter.ShouldTrigger(seq, nackMissCooldown, nackMissWindow) {
							s.requestKeyFrame(pub, fmt.Sprintf("订阅端 NACK seq=%d", seq))
						}
						continue
					}
					// 直接写入订阅者的本地轨道
					if err := localTrack.WriteRTP(cached); err != nil {
						log.Warnf("[Session %s] NACK 重传失败 seq=%d: %v", s.ID, seq, err)
					}
				}

			case *rtcp.ReceiverReport:
				// 订阅者的 RR，记录丢包率
				for _, block := range p.Reports {
					fractionLost := float64(block.FractionLost) / 256.0
					if block.FractionLost > 0 {
						log.Warnf("[Session %s] 订阅端丢包报告 轨道=%s 丢包率=%.2f%%",
							s.ID, pub.trackID, fractionLost*100)
					}
				}
			}
		}
	}
}

// RemoveSubscriber 移除订阅者
func (s *Session) RemoveSubscriber(publisherTrackID string, subscriberID string) {
	s.mu.RLock()
	pub, ok := s.publishedTracks[publisherTrackID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	pub.mu.Lock()
	sub, exists := pub.subscribers[subscriberID]
	if exists {
		delete(pub.subscribers, subscriberID)
	}
	pub.mu.Unlock()

	if exists && sub != nil {
		// 从订阅者的 PeerConnection 中移除轨道
		if sub.session != nil {
			if err := sub.session.pc.RemoveTrack(sub.sender); err != nil {
				log.Warnf("[Session %s] 移除订阅轨道失败: %v", subscriberID, err)
			}
			// 从订阅者的订阅列表中移除
			sub.session.mu.Lock()
			delete(sub.session.subscribedTracks, publisherTrackID)
			sub.session.mu.Unlock()
		}
		log.Infof("[Session %s] 移除订阅者 %s 从轨道 %s", s.ID, subscriberID, publisherTrackID)
	}
}

// RemoveAllSubscribers 移除某轨道的所有订阅者
func (s *Session) RemoveAllSubscribers(publisherTrackID string) {
	s.mu.RLock()
	pub, ok := s.publishedTracks[publisherTrackID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	pub.mu.Lock()
	subs := make([]*subscriberInfo, 0, len(pub.subscribers))
	for _, sub := range pub.subscribers {
		subs = append(subs, sub)
	}
	pub.subscribers = make(map[string]*subscriberInfo)
	pub.mu.Unlock()

	for _, sub := range subs {
		if sub.session != nil {
			_ = sub.session.pc.RemoveTrack(sub.sender)
			sub.session.mu.Lock()
			delete(sub.session.subscribedTracks, publisherTrackID)
			sub.session.mu.Unlock()
		}
	}
}

// UnpublishTrack 取消发布轨道
func (s *Session) UnpublishTrack(trackID string) {
	s.cleanupPublishedTrack(trackID)
}

// cleanupPublishedTrack 清理已结束的发布轨道
// 触发场景：客户端取消发布、重协商移除轨道、读包循环结束
// 负责移除订阅者、清空缓存、解除 SSRC 映射，并通知房间内其他 Session
// 通过 map 存在性判断保证幂等，重复调用不会重复通知
func (s *Session) cleanupPublishedTrack(trackID string) {
	s.mu.Lock()
	pub, ok := s.publishedTracks[trackID]
	if ok {
		delete(s.publishedTracks, trackID)
		s.ssrcMap.Unregister(pub.ssrc)
	}
	s.mu.Unlock()

	if !ok {
		// 已被清理过（例如 Session 关闭时已统一清理），直接返回
		return
	}

	// 移除所有订阅者
	s.RemoveAllSubscribers(trackID)

	// 清空缓存
	pub.buffer.Clear()

	// 通知房间：轨道取消发布
	if s.onEvent != nil {
		s.onEvent(EventTrackUnpublished, s.ID, TrackInfo{
			TrackID:   trackID,
			SessionID: s.ID,
		})
	}

	log.Infof("[Session %s] 发布轨道已结束并清理 trackID=%s", s.ID, trackID)
}

// GetPublishedTracks 获取本 Session 发布的所有轨道信息
func (s *Session) GetPublishedTracks() []TrackInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tracks := make([]TrackInfo, 0, len(s.publishedTracks))
	for _, pub := range s.publishedTracks {
		tracks = append(tracks, TrackInfo{
			TrackID:   pub.trackID,
			StreamID:  pub.streamID,
			Kind:      pub.kind.String(),
			SSRC:      pub.ssrc,
			SessionID: s.ID,
		})
	}
	return tracks
}

// GetPublishedTrack 获取指定发布轨道
func (s *Session) GetPublishedTrack(trackID string) (*PublishedTrack, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pub, ok := s.publishedTracks[trackID]
	return pub, ok
}

// GetStats 获取统计信息
func (s *Session) GetStats() (bytesSent, bytesReceived, packetsSent, packetsRecv uint64) {
	return atomic.LoadUint64(&s.bytesSent),
		atomic.LoadUint64(&s.bytesReceived),
		atomic.LoadUint64(&s.packetsSent),
		atomic.LoadUint64(&s.packetsRecv)
}

// SetRemoteDescription 设置远端 SDP
func (s *Session) SetRemoteDescription(sdp webrtc.SessionDescription) error {
	return s.pc.SetRemoteDescription(sdp)
}

// CreateAnswer 创建应答 SDP
func (s *Session) CreateAnswer() (webrtc.SessionDescription, error) {
	return s.pc.CreateAnswer(nil)
}

// SetLocalDescription 设置本地 SDP
func (s *Session) SetLocalDescription(sdp webrtc.SessionDescription) error {
	return s.pc.SetLocalDescription(sdp)
}

// AddICECandidate 添加 ICE 候选
func (s *Session) AddICECandidate(candidate webrtc.ICECandidateInit) error {
	return s.pc.AddICECandidate(candidate)
}

// Close 关闭 Session，释放所有资源
func (s *Session) Close() {
	s.once.Do(func() {
		log.Infof("[Session %s] 开始关闭会话，释放资源", s.ID)
		s.closed = true
		s.cancel()

		// 取消所有发布的轨道
		s.mu.Lock()
		pubs := make([]*PublishedTrack, 0, len(s.publishedTracks))
		for _, pub := range s.publishedTracks {
			pubs = append(pubs, pub)
		}
		s.publishedTracks = make(map[string]*PublishedTrack)

		// 取消所有订阅
		subs := make([]*SubscriptionInfo, 0, len(s.subscribedTracks))
		for _, sub := range s.subscribedTracks {
			subs = append(subs, sub)
		}
		s.subscribedTracks = make(map[string]*SubscriptionInfo)
		s.mu.Unlock()

		// 移除所有订阅者（从发布者的订阅列表中移除本 session）
		for _, sub := range subs {
			if sub.publisher != nil {
				sub.publisher.RemoveSubscriber(sub.trackID, s.ID)
			}
		}

		// 对每个发布轨道，通知订阅者并清理
		for _, pub := range pubs {
			s.RemoveAllSubscribers(pub.trackID)
			pub.buffer.Clear()
			if s.onEvent != nil {
				s.onEvent(EventTrackUnpublished, s.ID, TrackInfo{
					TrackID:   pub.trackID,
					SessionID: s.ID,
				})
			}
		}

		// 关闭 PeerConnection
		if err := s.pc.Close(); err != nil {
			log.Warnf("[Session %s] 关闭 PeerConnection 失败: %v", s.ID, err)
		}

		// 从房间中移除自己：触发房间内其他 Session 的下线通知，
		// 并在房间无任何 Session 时自动销毁房间（释放资源，避免泄漏）
		if roomManager != nil {
			if room, ok := roomManager.GetRoom(s.RoomID); ok {
				// 仅当房间内该 ID 仍对应本 Session 时才移除，
				// 避免断线重连时旧会话误删同 ID 的新会话
				room.RemoveSessionIf(s.ID, s)
			}
		}

		log.Infof("[Session %s] 会话已关闭，资源已释放", s.ID)
	})
}

// IsClosed 是否已关闭
func (s *Session) IsClosed() bool {
	return s.closed
}

// GetPeerConnection 获取底层 PeerConnection（信令模块用）
func (s *Session) GetPeerConnection() *webrtc.PeerConnection {
	return s.pc
}

// expandNackPairs 将 NackPair 列表展开为丢失的序列号列表
// NackPair.PacketID 是第一个丢失的序列号
// NackPair.LostPackets 是 16 位 bitmask，bit i 表示 PacketID + i + 1 是否丢失
func expandNackPairs(nacks []rtcp.NackPair) []uint16 {
	var seqs []uint16
	for _, nack := range nacks {
		// 第一个丢失的序列号
		seqs = append(seqs, nack.PacketID)
		// 解析 bitmask
		for i := 0; i < 16; i++ {
			if nack.LostPackets&(1<<uint(i)) != 0 {
				seqs = append(seqs, nack.PacketID+uint16(i+1))
			}
		}
	}
	return seqs
}
