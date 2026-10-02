package reconcile

import (
	"context"
	"errors"
	"sync"
)

// Reconciler 是用户实现的调和逻辑。
type Reconciler interface {
	Reconcile(ctx context.Context, req Key) error
}

// ReconcilerFunc 让函数充当 Reconciler。
type ReconcilerFunc func(ctx context.Context, req Key) error

func (f ReconcilerFunc) Reconcile(ctx context.Context, req Key) error { return f(ctx, req) }

// Controller 监听存储事件，把变更键入队并驱动调和。
//
// 热循环抑制：控制器记录每个键最近入队时的 Generation 与删除标记，
// 仅当 Generation 推进或进入删除流程时才再次入队；
// 自身状态写回不推进 Generation，因此不会反复触发调和。
type Controller struct {
	Name  string
	Store Store
	// Cache 是共享读取缓存；为 nil 时直接读 Store。
	Cache *Cache
	Queue *WorkQueue
	Do    Reconciler
	// ListGVKs 启动时需要全量入队的资源种类（用于重启后恢复删除流程等）。
	ListGVKs []GroupVersionKind

	mu     sync.Mutex
	seen   map[Key]seenState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type seenState struct {
	generation int64
	deleting   bool
}

// Reader 返回控制器读取资源使用的接口。
func (c *Controller) Reader() Reader {
	if c.Cache != nil {
		return c.Cache
	}
	return c.Store
}

// Run 启动事件监听与 worker 协程，直到 ctx 取消后返回。
func (c *Controller) Run(ctx context.Context) {
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Lock()
	if c.seen == nil {
		c.seen = make(map[Key]seenState)
	}
	c.mu.Unlock()

	events, err := c.Store.Watch(ctx)
	if err != nil {
		return
	}

	// 启动时全量入队：崩溃重启后处于删除流程中的对象能继续清理，
	// 既不跳过已完成的步骤，也不重复执行已生效的动作。
	for _, gvk := range c.ListGVKs {
		objs, err := c.Store.List(ctx, gvk)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			c.enqueue(Event{Type: EventAdded, Object: obj})
		}
	}

	n := c.Queue.Config().Concurrency
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go c.worker(ctx)
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for ev := range events {
			c.enqueue(ev)
		}
	}()

	<-ctx.Done()
	c.Queue.ShutDown()
	c.wg.Wait()
}

// Stop 停止控制器。
func (c *Controller) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

// enqueue 从事件推导键并入队；仅状态变化的事件会被过滤以避免热循环。
func (c *Controller) enqueue(ev Event) {
	obj := ev.Object
	if obj == nil {
		return
	}
	k := KeyOf(obj)
	if c.Cache != nil {
		c.Cache.Invalidate(k)
	}
	deleting := obj.Meta.DeletionTimestamp != nil
	c.mu.Lock()
	last, ok := c.seen[k]
	advanced := !ok || obj.Meta.Generation > last.generation || (deleting && !last.deleting)
	if ev.Type == EventDeleted {
		// 对象已真正移除，清理记录，无需调和。
		delete(c.seen, k)
		c.mu.Unlock()
		return
	}
	if advanced {
		c.seen[k] = seenState{generation: obj.Meta.Generation, deleting: deleting}
	}
	c.mu.Unlock()
	if advanced {
		c.Queue.Add(k)
	}
}

func (c *Controller) worker(ctx context.Context) {
	defer c.wg.Done()
	for {
		k, ok := c.Queue.Get()
		if !ok {
			return
		}
		err := c.Do.Reconcile(ctx, k)
		if err != nil {
			// 失败：按退避策略重试；超过上限则丢弃，
			// 单个资源的持续失败不会拖垮整个队列。
			c.Queue.Requeue(k)
		} else {
			c.Queue.Forget(k)
			c.Queue.Done(k)
		}
	}
}

// maxStatusRetries 是状态写回冲突时的最大重试次数。
const maxStatusRetries = 8

// UpdateStatusWithRetry 带版本校验地写回状态：
// 每次先读取最新对象，应用 mutate 后做 CAS 写回；
// 遇到并发冲突重新读取并重试，既不覆盖其他写入者的更新，
// 也不会回退已经生效的期望状态。
func UpdateStatusWithRetry(ctx context.Context, r Reader, s Store, k Key, mutate func(obj *Resource)) error {
	var lastErr error
	for i := 0; i < maxStatusRetries; i++ {
		obj, err := r.Get(ctx, k.GVK, k.Namespace, k.Name)
		if err != nil {
			return err
		}
		mutate(obj)
		obj.Status.ObservedGeneration = obj.Meta.Generation
		err = s.UpdateStatus(ctx, obj)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		lastErr = err
	}
	return lastErr
}
