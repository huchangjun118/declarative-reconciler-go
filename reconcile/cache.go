package reconcile

import (
	"context"
	"sync"
	"time"
)

// Cache 是多个控制器与执行单元共享的读取缓存。
//
// 回退策略：
//   - 缓存命中且未过期：直接返回缓存副本；
//   - 未命中或已过期：回退到直接读取底层存储，并刷新缓存；
//   - 直接读取失败但手头有过期副本：降级返回过期副本（可用性优先），
//     否则返回错误。
//
// watch 事件通过 Invalidate 主动失效对应键，保证不会长期读到旧值。
type Cache struct {
	store Store
	ttl   time.Duration
	now   func() time.Time

	mu      sync.RWMutex
	entries map[Key]cacheEntry
}

type cacheEntry struct {
	obj       *Resource
	fetchedAt time.Time
}

// NewCache 创建共享读取缓存；ttl <= 0 表示不过期（仅靠事件失效）。
func NewCache(store Store, ttl time.Duration) *Cache {
	return &Cache{store: store, ttl: ttl, now: time.Now, entries: make(map[Key]cacheEntry)}
}

// Get 读取资源；缓存过期或未命中时回退到直接读取存储。
func (c *Cache) Get(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Resource, error) {
	k := Key{GVK: gvk, Namespace: namespace, Name: name}
	c.mu.RLock()
	e, hit := c.entries[k]
	c.mu.RUnlock()
	if hit && !c.expired(e) {
		return e.obj.DeepCopy(), nil
	}
	// 回退：直接读取底层存储。
	obj, err := c.store.Get(ctx, gvk, namespace, name)
	if err != nil {
		if hit {
			// 存储暂时不可用：降级返回过期副本（可用性优先）。
			return e.obj.DeepCopy(), nil
		}
		return nil, err
	}
	c.mu.Lock()
	c.entries[k] = cacheEntry{obj: obj.DeepCopy(), fetchedAt: c.now()}
	c.mu.Unlock()
	return obj, nil
}

func (c *Cache) expired(e cacheEntry) bool {
	if c.ttl <= 0 {
		return false
	}
	return c.now().Sub(e.fetchedAt) > c.ttl
}

// Invalidate 使某个键的缓存失效（由 watch 事件驱动）。
func (c *Cache) Invalidate(k Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, k)
}

// Reader 是控制器读取资源时使用的接口，可直接用 Cache 或 Store 实现。
type Reader interface {
	Get(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Resource, error)
}
