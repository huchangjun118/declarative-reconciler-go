package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"
)

// flakyStore 可控制 Get 是否失败的存储包装。
type flakyStore struct {
	Store
	failGet bool
	gets    int
}

func (f *flakyStore) Get(ctx context.Context, gvk GroupVersionKind, ns, name string) (*Resource, error) {
	f.gets++
	if f.failGet {
		return nil, errors.New("store unavailable")
	}
	return f.Store.Get(ctx, gvk, ns, name)
}

func TestCacheFallback(t *testing.T) {
	base := NewMemoryStore()
	store := &flakyStore{Store: base}
	ctx := context.Background()
	if err := base.Create(ctx, newWidget("cached")); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	c := NewCache(store, time.Minute)
	c.now = func() time.Time { return now }

	// 第一次读取：未命中，回退到直接读并填充缓存。
	obj, err := c.Get(ctx, widgetGVK, "default", "cached")
	if err != nil {
		t.Fatal(err)
	}
	if store.gets != 1 {
		t.Fatalf("expected 1 direct read, got %d", store.gets)
	}

	// 未过期：命中缓存，不再访问存储。
	if _, err := c.Get(ctx, widgetGVK, "default", "cached"); err != nil {
		t.Fatal(err)
	}
	if store.gets != 1 {
		t.Fatalf("cache not used, direct reads = %d", store.gets)
	}

	// 过期后：回退到直接读取并刷新。
	now = now.Add(2 * time.Minute)
	obj.Meta.Labels = map[string]string{"fresh": "yes"}
	if err := base.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	fresh, err := c.Get(ctx, widgetGVK, "default", "cached")
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Meta.Labels["fresh"] != "yes" {
		t.Fatal("expected fresh read after expiry")
	}
	if store.gets != 2 {
		t.Fatalf("expected fallback read after expiry, direct reads = %d", store.gets)
	}

	// 存储故障 + 有过期副本：降级返回过期副本。
	now = now.Add(2 * time.Minute)
	store.failGet = true
	stale, err := c.Get(ctx, widgetGVK, "default", "cached")
	if err != nil {
		t.Fatalf("expected stale fallback, got err=%v", err)
	}
	if stale.Meta.Labels["fresh"] != "yes" {
		t.Fatal("stale copy corrupted")
	}

	// 失效后无副本可用：返回错误。
	c.Invalidate(KeyOf(obj))
	if _, err := c.Get(ctx, widgetGVK, "default", "cached"); err == nil {
		t.Fatal("expected error when store down and no cache entry")
	}
}

func TestCacheSharedAcrossControllers(t *testing.T) {
	base := NewMemoryStore()
	store := &flakyStore{Store: base}
	ctx := context.Background()
	if err := base.Create(ctx, newWidget("shared")); err != nil {
		t.Fatal(err)
	}
	c := NewCache(store, 0) // 不过期，仅靠事件失效

	// 两个“控制器”共享同一缓存。
	if _, err := c.Get(ctx, widgetGVK, "default", "shared"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, widgetGVK, "default", "shared"); err != nil {
		t.Fatal(err)
	}
	if store.gets != 1 {
		t.Fatalf("shared cache not reused, direct reads = %d", store.gets)
	}
	// 事件驱动失效后重新读取。
	c.Invalidate(KeyOf(newWidget("shared")))
	if _, err := c.Get(ctx, widgetGVK, "default", "shared"); err != nil {
		t.Fatal(err)
	}
	if store.gets != 2 {
		t.Fatalf("expected refresh after invalidation, direct reads = %d", store.gets)
	}
}

func TestCacheReturnsIndependentCopies(t *testing.T) {
	base := NewMemoryStore()
	ctx := context.Background()
	if err := base.Create(ctx, newWidget("copies")); err != nil {
		t.Fatal(err)
	}
	c := NewCache(base, 0)
	a, err := c.Get(ctx, widgetGVK, "default", "copies")
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Get(ctx, widgetGVK, "default", "copies")
	if err != nil {
		t.Fatal(err)
	}
	a.Spec["size"] = 999
	if b.Spec["size"] != 1 {
		t.Fatal("cached copies share mutable state")
	}
}
