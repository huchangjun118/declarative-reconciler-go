package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReconcile 验证并发调和、去重与幂等：
// 同一版本被重复调和不得产生重复副作用。
func TestConcurrentReconcile(t *testing.T) {
	store := NewMemoryStore()
	q := NewWorkQueue(QueueConfig{Concurrency: 8, MaxRetries: 5,
		Backoff: BackoffPolicy{BaseDelay: time.Millisecond}})

	var mu sync.Mutex
	reconciled := map[Key][]int64{} // key -> 调和时观察到的 generation 序列
	sideEffects := map[string]int{} // key/generation -> 副作用次数
	c := &Controller{
		Name:     "test",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			obj, err := store.Get(ctx, k.GVK, k.Namespace, k.Name)
			if err != nil {
				return nil // 已删除
			}
			mu.Lock()
			defer mu.Unlock()
			reconciled[k] = append(reconciled[k], obj.Meta.Generation)
			// 幂等副作用：以 (key, generation) 为幂等键。
			idem := fmt.Sprintf("%s/%d", k.String(), obj.Meta.Generation)
			sideEffects[idem]++
			return nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	const n = 20
	for i := 0; i < n; i++ {
		obj := newWidget(fmt.Sprintf("w-%d", i))
		if err := store.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "all objects reconciled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reconciled) == n
	})

	// 每个 (key, generation) 的副作用必须恰好一次（幂等去重生效）。
	mu.Lock()
	for idem, count := range sideEffects {
		if count != 1 {
			t.Fatalf("side effect %s ran %d times, want 1", idem, count)
		}
	}
	mu.Unlock()
}

// TestStatusWriteDoesNotHotLoop 验证状态写回不会反复触发调和。
func TestStatusWriteDoesNotHotLoop(t *testing.T) {
	store := NewMemoryStore()
	q := NewWorkQueue(QueueConfig{Concurrency: 2, MaxRetries: 5,
		Backoff: BackoffPolicy{BaseDelay: time.Millisecond}})

	var mu sync.Mutex
	count := map[Key]int{}
	c := &Controller{
		Name:     "status",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			mu.Lock()
			count[k]++
			mu.Unlock()
			// 调和逻辑：写回状态（模拟 Ready 条件）。
			return UpdateStatusWithRetry(ctx, store, store, k, func(obj *Resource) {
				obj.Status.Conditions = []Condition{{Type: "Ready", Status: "True"}}
			})
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	if err := store.Create(ctx, newWidget("hot")); err != nil {
		t.Fatal(err)
	}
	k := testKey("hot")
	waitFor(t, "status written", func() bool {
		obj, err := store.Get(ctx, widgetGVK, "default", "hot")
		return err == nil && len(obj.Status.Conditions) == 1
	})
	// 稳定后调和次数应保持很小（不形成热循环）。
	before := func() int { mu.Lock(); defer mu.Unlock(); return count[k] }()
	time.Sleep(200 * time.Millisecond)
	after := func() int { mu.Lock(); defer mu.Unlock(); return count[k] }()
	if after != before {
		t.Fatalf("hot loop detected: reconciles grew from %d to %d", before, after)
	}
	if after > 3 {
		t.Fatalf("too many reconciles: %d", after)
	}
}

// TestConflictRetry 验证并发状态写回的冲突重试不丢更新。
func TestConflictRetry(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.Create(ctx, newWidget("c1")); err != nil {
		t.Fatal(err)
	}
	k := testKey("c1")

	const writers = 16
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			err := UpdateStatusWithRetry(ctx, store, store, k, func(obj *Resource) {
				n, _ := obj.Status.Extra["counter"].(int)
				if obj.Status.Extra == nil {
					obj.Status.Extra = map[string]any{}
				}
				obj.Status.Extra["counter"] = n + 1
			})
			if err != nil {
				t.Errorf("writer %d: %v", w, err)
			}
		}(w)
	}
	wg.Wait()
	obj, err := store.Get(ctx, widgetGVK, "default", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if got := obj.Status.Extra["counter"]; got != writers {
		t.Fatalf("counter = %v, want %d (lost updates under conflict)", got, writers)
	}
	if obj.Status.ObservedGeneration != obj.Meta.Generation {
		t.Fatalf("observedGeneration %d != generation %d",
			obj.Status.ObservedGeneration, obj.Meta.Generation)
	}
}

// TestStatusRetryDoesNotRollbackSpec 验证冲突重试期间不覆盖他人的 spec 更新。
func TestStatusRetryDoesNotRollbackSpec(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.Create(ctx, newWidget("c2")); err != nil {
		t.Fatal(err)
	}
	k := testKey("c2")

	// 状态写回循环与 spec 更新并发进行。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = UpdateStatusWithRetry(ctx, store, store, k, func(obj *Resource) {
				obj.Status.Conditions = []Condition{{Type: "Ready", Status: "True"}}
			})
		}
	}()

	for i := 0; i < 10; i++ {
		// 与状态写入并发：冲突时重读重试（客户端既定模式）。
		err := updateMetaWithRetry(ctx, store, k, func(latest *Resource) {
			latest.Spec["size"] = i + 2
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	obj, err := store.Get(ctx, widgetGVK, "default", "c2")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Spec["size"] != 11 {
		t.Fatalf("spec rolled back: size = %v, want 11", obj.Spec["size"])
	}
	if obj.Meta.Generation != 11 {
		t.Fatalf("generation = %d, want 11 (only spec changes bump it)", obj.Meta.Generation)
	}
}

// TestFailedReconcileRetriesWithBackoff 验证调和失败按退避重试，
// 且持续失败在超过上限后被丢弃，不拖垮队列。
func TestFailedReconcileRetriesWithBackoff(t *testing.T) {
	store := NewMemoryStore()
	// MaxRetries=2：首次 + 2 次重试都失败后丢弃。
	q := NewWorkQueue(QueueConfig{
		Concurrency: 1,
		MaxRetries:  2,
		Backoff:     BackoffPolicy{BaseDelay: time.Millisecond, Factor: 2, MaxDelay: 20 * time.Millisecond},
	})

	var mu sync.Mutex
	attempts := map[Key]int{}
	c := &Controller{
		Name:     "fail",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			mu.Lock()
			attempts[k]++
			mu.Unlock()
			return errors.New("boom")
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	if err := store.Create(ctx, newWidget("fail")); err != nil {
		t.Fatal(err)
	}
	// 等待重试耗尽、键被丢弃。
	waitFor(t, "retries exhausted and dropped", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts[testKey("fail")] >= 3 && q.Len() == 0
	})
	mu.Lock()
	n := attempts[testKey("fail")]
	mu.Unlock()
	if n != 3 {
		t.Fatalf("attempts = %d, want 3 (1 initial + 2 retries)", n)
	}
}

// TestSpecChangeTriggersNewGenerationReconcile 验证 spec 真正变化时
// 才以新版本重新入队调和。
func TestSpecChangeTriggersNewGenerationReconcile(t *testing.T) {
	store := NewMemoryStore()
	q := NewWorkQueue(QueueConfig{Concurrency: 1, Backoff: BackoffPolicy{BaseDelay: time.Millisecond}})

	var mu sync.Mutex
	gens := map[Key][]int64{}
	c := &Controller{
		Name:     "gen",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			obj, err := store.Get(ctx, k.GVK, k.Namespace, k.Name)
			if err != nil {
				return nil
			}
			mu.Lock()
			gens[k] = append(gens[k], obj.Meta.Generation)
			mu.Unlock()
			return nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	obj := newWidget("gen")
	if err := store.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	k := testKey("gen")
	waitFor(t, "first reconcile", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(gens[k]) >= 1
	})

	// 更新 spec（size 变化）：应推进 generation 并再次调和。
	got, err := store.Get(ctx, widgetGVK, "default", "gen")
	if err != nil {
		t.Fatal(err)
	}
	got.Spec["size"] = 42
	if err := store.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "second reconcile on spec change", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, g := range gens[k] {
			if g == 2 {
				return true
			}
		}
		return false
	})

	// 仅更新元数据（不改 spec）：不推进 generation，不再调和。
	before := func() int { mu.Lock(); defer mu.Unlock(); return len(gens[k]) }()
	got2, _ := store.Get(ctx, widgetGVK, "default", "gen")
	got2.Meta.Labels = map[string]string{"x": "y"}
	if err := store.Update(ctx, got2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	after := func() int { mu.Lock(); defer mu.Unlock(); return len(gens[k]) }()
	if after != before {
		t.Fatalf("metadata-only change triggered reconcile: %d -> %d", before, after)
	}
}
