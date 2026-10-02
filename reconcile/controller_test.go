package reconcile

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

var widgetGVK = GroupVersionKind{Group: "test.io", Version: "v1", Kind: "Widget"}

func newWidget(name string) *Resource {
	return &Resource{
		APIVersion: "test.io/v1",
		Kind:       "Widget",
		Meta:       ObjectMeta{Namespace: "default", Name: name},
		Spec:       map[string]any{"size": 1},
	}
}

// waitFor 轮询直到条件满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

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
