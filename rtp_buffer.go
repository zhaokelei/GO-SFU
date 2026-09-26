// rtp_buffer.go
// RTP 包处理工具：滑动窗口缓存（NACK重传）、序列号校验、抖动缓冲、异常包过滤、SSRC映射
// 所有媒体转发前都经过这里，保障弱网下的音视频质量

package main

import (
	"container/list"
	"sync"
	"time"

	"github.com/pion/rtp"
)

// ============================================================
// 1. 异常 RTP 包检测
// ============================================================

// ValidateRTP 检测异常 RTP 包，返回是否合法
// 检测项：版本号必须为2
func ValidateRTP(pkt *rtp.Packet) bool {
	if pkt == nil {
		return false
	}
	// RTP 版本号固定为 2
	if pkt.Version != 2 {
		return false
	}
	return true
}

// ============================================================
// 2. RTP 序列号校验器（过滤重复包、乱序包）
// ============================================================

// SeqValidator RTP 序列号校验器
// 16 位序列号会回绕，需要处理回绕情况
// 策略：维护最大已接收序列号，超过一定阈值的乱序包丢弃
type SeqValidator struct {
	mu              sync.Mutex
	maxSeq          uint16 // 已接收的最大序列号
	hasInit         bool   // 是否初始化
	maxDropout      uint16 // 最大允许乱序跨度，超过视为新流或丢包
	duplicateCount  uint64 // 重复包计数
	outOfOrderCount uint64 // 乱序包计数
	totalCount      uint64 // 总包计数
}

// NewSeqValidator 创建序列号校验器
// maxDropout: 允许的最大乱序跨度，默认 3000
func NewSeqValidator(maxDropout uint16) *SeqValidator {
	if maxDropout == 0 {
		maxDropout = 3000
	}
	return &SeqValidator{
		maxDropout: maxDropout,
	}
}

// Check 校验序列号，返回是否应该转发该包
// 返回 true 表示正常包，false 表示重复应丢弃
func (s *SeqValidator) Check(seq uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalCount++

	if !s.hasInit {
		s.maxSeq = seq
		s.hasInit = true
		return true
	}

	// 计算序列号差值（考虑回绕）
	diff := int32(seq) - int32(s.maxSeq)

	// 正常递增（包括回绕后从 0 开始的情况）
	if diff > 0 && diff <= int32(s.maxDropout) {
		s.maxSeq = seq
		return true
	}

	// 重复包
	if diff == 0 {
		s.duplicateCount++
		return false
	}

	// 乱序包：diff < 0 但在允许范围内，允许通过
	if diff < 0 && -diff <= int32(s.maxDropout) {
		s.outOfOrderCount++
		return true
	}

	// 跨度太大，可能是新流或严重丢包，重置
	s.maxSeq = seq
	return true
}

// GetStats 返回统计信息
func (s *SeqValidator) GetStats() (total, duplicate, outOfOrder uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalCount, s.duplicateCount, s.outOfOrderCount
}

// ============================================================
// 3. RTP 滑动窗口缓存（用于 NACK 重传）
// ============================================================

// RTPBuffer 滑动窗口缓存，按 SSRC 分别管理
// 收到 NACK 请求时，从缓存中查找对应的 RTP 包并重传
//
// 实现说明（接收性能关键）：
// 采用「哈希表 + 双向链表」实现 O(1) 的插入与淘汰：
//   - packets：序列号 -> 链表节点，负责 O(1) 查找（NACK 重传命中）
//   - order：按到达顺序保存各包，表头（Front）是最早到达的包，淘汰时直接摘除
//
// 旧的实现用一张 map 保存，每次插入后都要全量遍历 map 找最旧的包来淘汰，
// 相当于每个包都要 O(n) 扫描一次。当缓存深度加大（如 1024）且多路高码率并发时，
// 这个开销会成为接收侧 CPU 热点。改为 O(1) 后，接收与转发开销与缓存深度无关，
// 服务器可以稳定承接更高的总码率。
type RTPBuffer struct {
	mu      sync.Mutex
	packets map[uint16]*list.Element // 序列号 -> 链表节点（节点值为 *rtp.Packet）
	order   *list.List               // 到达顺序链表，Front 为最早到达（优先淘汰）
	maxSize int                      // 最大缓存包数量
	ssrc    uint32                   // 所属 SSRC
	hits    uint64                   // NACK 命中次数
	misses  uint64                   // NACK 未命中次数
}

// NewRTPBuffer 创建 RTP 缓存
// maxSize: 最大缓存包数量，默认 512
func NewRTPBuffer(ssrc uint32, maxSize int) *RTPBuffer {
	if maxSize <= 0 {
		maxSize = 512
	}
	return &RTPBuffer{
		packets: make(map[uint16]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
		ssrc:    ssrc,
	}
}

// Add 向缓存中添加 RTP 包
// 超过最大容量时按到达顺序淘汰最早的包（O(1)）
func (b *RTPBuffer) Add(pkt *rtp.Packet) {
	if pkt == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	seq := pkt.SequenceNumber

	// 已存在则不重复添加
	if _, exists := b.packets[seq]; exists {
		return
	}

	// 追加到链表尾部（尾部为最新到达的包），并登记进哈希表 —— 均为 O(1)
	el := b.order.PushBack(pkt)
	b.packets[seq] = el

	// 超出上限时，从链表头部摘除「最早到达」的包 —— O(1)
	// 正常有序到达时，最早到达的包即序列号最旧的包，语义与原实现一致。
	for b.order.Len() > b.maxSize {
		front := b.order.Front()
		if front == nil {
			// 缓存已空，避免死循环
			break
		}
		oldPkt := front.Value.(*rtp.Packet)
		b.order.Remove(front)
		delete(b.packets, oldPkt.SequenceNumber)
	}
}

// Get 根据序列号获取缓存的 RTP 包（用于 NACK 重传）
func (b *RTPBuffer) Get(seq uint16) (*rtp.Packet, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	el, exists := b.packets[seq]
	if exists {
		b.hits++
		return el.Value.(*rtp.Packet), true
	}
	b.misses++
	return nil, false
}

// GetStats 返回 NACK 命中统计
func (b *RTPBuffer) GetStats() (hits, misses uint64, size int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits, b.misses, b.order.Len()
}

// Clear 清空缓存
func (b *RTPBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.packets = make(map[uint16]*list.Element)
	b.order = list.New()
}

// ============================================================
// 3.5 NACK 未命中限流器（防止 NACK 风暴）
// ============================================================

// NackMissLimiter 对 NACK 未命中做去重限流
// 当请求重传的包已不在缓存中（超出窗口）时，客户端往往会在极短时间内对
// 同一丢失区间反复发送 NACK，形成「NACK 风暴」：既无法恢复数据，又会
// 打满 CPU 与日志。这里对同一丢失区间做冷却限流，冷却期内只触发一次
// 关键帧请求（PLI），从而以最快方式让画面恢复。
type NackMissLimiter struct {
	mu        sync.Mutex
	lastMiss  time.Time // 上次触发关键帧请求的时间
	lastSeq   uint16    // 上次未命中区间的代表序列号
	hasInit   bool      // 是否已初始化
	missCount uint64    // 被限流抑制的未命中次数（统计用）
}

// NewNackMissLimiter 创建 NACK 未命中限流器
func NewNackMissLimiter() *NackMissLimiter {
	return &NackMissLimiter{}
}

// ShouldTrigger 判断本次未命中是否需要触发关键帧请求
// seq: 本次未命中的序列号
// cooldown: 同一丢失区间的冷却时间
// winSeqs: 判断「同一区间」的序列号跨度阈值
// 返回 true 表示应触发（并已刷新时间戳），false 表示处于冷却期被抑制
func (l *NackMissLimiter) ShouldTrigger(seq uint16, cooldown time.Duration, winSeqs int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	if l.hasInit {
		// 计算与上次未命中序列号的跨度（考虑回绕，取绝对值）
		diff := int32(seq) - int32(l.lastSeq)
		if diff < 0 {
			diff = -diff
		}
		// 属于同一丢失区间且仍在冷却期内 → 抑制
		if diff <= int32(winSeqs) && now.Sub(l.lastMiss) < cooldown {
			l.missCount++
			return false
		}
	}

	l.lastMiss = now
	l.lastSeq = seq
	l.hasInit = true
	return true
}

// SuppressedCount 返回被限流抑制的未命中次数
func (l *NackMissLimiter) SuppressedCount() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.missCount
}

// ============================================================
// 4. 抖动缓冲（Jitter Buffer）
// ============================================================

// JitterBuffer 抖动缓冲器
// 平滑 RTP 包到达时间差，缓解网络抖动带来的卡顿、爆音
type JitterBuffer struct {
	mu         sync.Mutex
	packets    []*jitterPacket // 按时间戳排序的包队列
	bufferMs   int             // 缓冲时间（毫秒）
	maxPackets int             // 最大缓冲包数，防止内存暴涨
}

// jitterPacket 内部包结构
type jitterPacket struct {
	pkt       *rtp.Packet
	timestamp time.Time // 到达时间
}

// NewJitterBuffer 创建抖动缓冲器
// bufferMs: 缓冲时间，默认 50ms；maxPackets: 最大缓冲包数，默认 100
func NewJitterBuffer(bufferMs, maxPackets int) *JitterBuffer {
	if bufferMs <= 0 {
		bufferMs = 50
	}
	if maxPackets <= 0 {
		maxPackets = 100
	}
	return &JitterBuffer{
		bufferMs:   bufferMs,
		maxPackets: maxPackets,
	}
}

// Push 推入一个 RTP 包
func (j *JitterBuffer) Push(pkt *rtp.Packet) {
	if pkt == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	// 超过最大包数，丢弃最旧的
	if len(j.packets) >= j.maxPackets {
		j.packets = j.packets[1:]
	}

	j.packets = append(j.packets, &jitterPacket{
		pkt:       pkt,
		timestamp: time.Now(),
	})

	// 按 RTP 时间戳排序（稳定排序，保持到达顺序）
	for i := len(j.packets) - 1; i > 0; i-- {
		if j.packets[i].pkt.Timestamp < j.packets[i-1].pkt.Timestamp {
			j.packets[i], j.packets[i-1] = j.packets[i-1], j.packets[i]
		} else {
			break
		}
	}
}

// Pop 取出已经可以发送的包（到达时间超过缓冲阈值）
// 返回 nil 表示暂时没有可发送的包
func (j *JitterBuffer) Pop() *rtp.Packet {
	j.mu.Lock()
	defer j.mu.Unlock()

	if len(j.packets) == 0 {
		return nil
	}

	// 检查队首包是否已到达缓冲时间
	now := time.Now()
	elapsed := now.Sub(j.packets[0].timestamp)
	if elapsed < time.Duration(j.bufferMs)*time.Millisecond {
		return nil
	}

	pkt := j.packets[0].pkt
	j.packets = j.packets[1:]
	return pkt
}

// Len 返回当前缓冲包数
func (j *JitterBuffer) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.packets)
}

// Clear 清空缓冲
func (j *JitterBuffer) Clear() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.packets = nil
}

// ============================================================
// 5. SSRC 隔离映射（同一 session 内音视频 SSRC 隔离）
// ============================================================

// SSRCMap 管理 SSRC 到轨道的映射，避免浏览器混流
type SSRCMap struct {
	mu     sync.RWMutex
	ssrcs  map[uint32]string // SSRC -> trackID
	tracks map[string]uint32 // trackID -> SSRC
}

// NewSSRCMap 创建 SSRC 映射
func NewSSRCMap() *SSRCMap {
	return &SSRCMap{
		ssrcs:  make(map[uint32]string),
		tracks: make(map[string]uint32),
	}
}

// Register 注册 SSRC 与 trackID 的映射
func (s *SSRCMap) Register(ssrc uint32, trackID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ssrcs[ssrc] = trackID
	s.tracks[trackID] = ssrc
}

// Unregister 移除 SSRC 映射
func (s *SSRCMap) Unregister(ssrc uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if trackID, ok := s.ssrcs[ssrc]; ok {
		delete(s.ssrcs, ssrc)
		delete(s.tracks, trackID)
	}
}

// GetTrackID 根据 SSRC 获取 trackID
func (s *SSRCMap) GetTrackID(ssrc uint32) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	trackID, ok := s.ssrcs[ssrc]
	return trackID, ok
}

// GetSSRC 根据 trackID 获取 SSRC
func (s *SSRCMap) GetSSRC(trackID string) (uint32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ssrc, ok := s.tracks[trackID]
	return ssrc, ok
}

// ListSSRCs 返回所有已注册的 SSRC
func (s *SSRCMap) ListSSRCs() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]uint32, 0, len(s.ssrcs))
	for ssrc := range s.ssrcs {
		result = append(result, ssrc)
	}
	return result
}
