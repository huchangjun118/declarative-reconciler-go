package reconcile

import (
	"sync"
	"time"
)

// BackoffPolicy 描述重试退避策略。
type BackoffPolicy struct {
	BaseDelay time.Duration
	Factor    float64
	MaxDelay  time.Duration
}

// Delay 计算第 attempt 次（从 1 开始）重试前的等待时长。
// 第 n 次重试等待 BaseDelay * Factor^(n-1)，封顶 MaxDelay。
func (b BackoffPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := b.BaseDelay
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2
	}
	d := float64(base)
	for i := 1; i < attempt; i++ {
		d *= factor
		if b.MaxDelay > 0 && time.Duration(d) >= b.MaxDelay {
			return b.MaxDelay
		}
	}
	if b.MaxDelay > 0 && time.Duration(d) > b.MaxDelay {
		return b.MaxDelay
	}
	return time.Duration(d)
}

// QueueConfig 是工作队列配置。
type QueueConfig struct {
	// Concurrency 是并发工作协程数；慢资源只占用自己的 worker，
	// 不阻塞其他资源的调和。
	Concurrency int
	// MaxRetries 是单个条目的最大重试次数，超过后丢弃，
	// 避免单个资源的持续失败拖垮整个队列。
	MaxRetries int
	Backoff    BackoffPolicy
}

func (c QueueConfig) withDefaults() QueueConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	return c
}

// WorkQueue 是去重、带退避重试的并发工作队列。
// 同一键在队列中只保留一份；处理期间再次入队会在 Done 后重新排队，
// 保证同一对象的多次变更被合并，且同一时刻只有一个 worker 处理该键。
type WorkQueue struct {
	cfg QueueConfig

	mu         sync.Mutex
	cond       *sync.Cond
	items      []Key
	dirty      map[Key]bool
	processing map[Key]bool
	retries    map[Key]int
	shutDown   bool
}

// NewWorkQueue 创建工作队列。
func NewWorkQueue(cfg QueueConfig) *WorkQueue {
	q := &WorkQueue{
		cfg:        cfg.withDefaults(),
		dirty:      make(map[Key]bool),
		processing: make(map[Key]bool),
		retries:    make(map[Key]int),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Config 返回队列生效的配置。
func (q *WorkQueue) Config() QueueConfig { return q.cfg }

// Add 将键入队；同一键在队列中只保留一份（合并去重）。
func (q *WorkQueue) Add(k Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutDown {
		return
	}
	if q.dirty[k] {
		return
	}
	q.dirty[k] = true
	if q.processing[k] {
		// 正在处理中：Done 时会重新排队。
		return
	}
	q.items = append(q.items, k)
	q.cond.Signal()
}

// AddAfter 延迟入队。
func (q *WorkQueue) AddAfter(k Key, d time.Duration) {
	if d <= 0 {
		q.Add(k)
		return
	}
	time.AfterFunc(d, func() { q.Add(k) })
}

// Get 阻塞取出一个待处理键；ok 为 false 表示队列已关闭且排空。
func (q *WorkQueue) Get() (k Key, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.shutDown {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return Key{}, false
	}
	k = q.items[0]
	q.items = q.items[1:]
	delete(q.dirty, k)
	q.processing[k] = true
	return k, true
}

// Done 标记键处理完成；若处理期间又有变更则重新排队。
func (q *WorkQueue) Done(k Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, k)
	if q.dirty[k] && !q.shutDown {
		q.items = append(q.items, k)
		q.cond.Signal()
	}
}

// Requeue 处理失败后按退避策略重新入队；超过 MaxRetries 则丢弃。
// 返回 false 表示该键已被丢弃。
func (q *WorkQueue) Requeue(k Key) bool {
	q.mu.Lock()
	q.retries[k]++
	n := q.retries[k]
	over := q.cfg.MaxRetries > 0 && n > q.cfg.MaxRetries
	if over {
		delete(q.retries, k)
		delete(q.dirty, k)
	}
	q.mu.Unlock()
	if over {
		q.Done(k)
		return false
	}
	delay := q.cfg.Backoff.Delay(n)
	q.Done(k)
	q.AddAfter(k, delay)
	return true
}

// Forget 清除键的重试计数（处理成功后调用）。
func (q *WorkQueue) Forget(k Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.retries, k)
}

// Len 返回当前排队中的键数量。
func (q *WorkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// ShutDown 关闭队列。
func (q *WorkQueue) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutDown = true
	q.cond.Broadcast()
}
