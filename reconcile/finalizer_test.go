package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestDeletionCleanupCrashRecovery 验证：
// 清理过程中崩溃后重启，已完成的步骤不重复执行，
// 未完成的步骤继续执行，最终对象被真正删除。
func TestDeletionCleanupCrashRecovery(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	obj := newWidget("doomed")
	if err := store.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	fin := &Finalizer{Name: "test.io/cleanup", Store: store}

	// 调和时确保终结器存在。
	changed, err := fin.Ensure(ctx, obj)
	if err != nil || !changed {
		t.Fatalf("Ensure = %v, %v", changed, err)
	}

	// 删除：对象带终结器，只标记 DeletionTimestamp。
	if err := store.Delete(ctx, widgetGVK, "default", "doomed"); err != nil {
		t.Fatal(err)
	}
	cur, err := store.Get(ctx, widgetGVK, "default", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Meta.DeletionTimestamp == nil {
		t.Fatal("expected DeletionTimestamp to be set")
	}

	var mu sync.Mutex
	var executed []string
	crashAt := 2 // 第 2 步（0 起）首次执行时模拟进程崩溃
	crashed := false
	steps := func(crash bool) []CleanupStep {
		names := []string{"detach-volume", "release-ip", "notify-owner"}
		out := make([]CleanupStep, len(names))
		for i, n := range names {
			i, n := i, n
			out[i] = CleanupStep{
				Name: n,
				Run: func(ctx context.Context, obj *Resource) error {
					if crash && i == crashAt && !crashed {
						crashed = true
						return errors.New("simulated crash")
					}
					mu.Lock()
					executed = append(executed, n)
					mu.Unlock()
					return nil
				},
			}
		}
		return out
	}

	// 第一次运行：在第 3 步前“崩溃”。
	fin.Steps = steps(true)
	done, err := fin.HandleDeletion(ctx, cur)
	if err == nil || done {
		t.Fatalf("expected crash error, got done=%v err=%v", done, err)
	}
	mu.Lock()
	if len(executed) != 2 {
		t.Fatalf("expected 2 steps before crash, got %v", executed)
	}
	mu.Unlock()

	// 模拟进程重启：新的 Finalizer 实例，从存储中的检查点恢复。
	fin2 := &Finalizer{Name: "test.io/cleanup", Store: store, Steps: steps(false)}
	cur2, err := store.Get(ctx, widgetGVK, "default", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	done, err = fin2.HandleDeletion(ctx, cur2)
	if err != nil || !done {
		t.Fatalf("recovery failed: done=%v err=%v", done, err)
	}

	mu.Lock()
	got := append([]string(nil), executed...)
	mu.Unlock()
	// 崩溃发生在 notify-owner 生效之前，因此重启后它重新执行一次；
	// 已完成并记录检查点的 detach-volume / release-ip 不得重复执行。
	want := []string{"detach-volume", "release-ip", "notify-owner"}
	if len(got) != len(want) {
		t.Fatalf("unexpected execution sequence: %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected execution sequence: %v, want %v", got, want)
		}
	}

	// 终结器已移除，对象被真正删除。
	if _, err := store.Get(ctx, widgetGVK, "default", "doomed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected object gone, got err=%v", err)
	}
}

// TestDeletionCleanupViaControllerRestart 验证通过控制器重启恢复删除流程。
func TestDeletionCleanupViaControllerRestart(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	obj := newWidget("restart")
	obj.Meta.Finalizers = []string{"test.io/cleanup"}
	if err := store.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, widgetGVK, "default", "restart"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var executed []string
	fin := &Finalizer{
		Name:  "test.io/cleanup",
		Store: store,
		Steps: []CleanupStep{
			{Name: "s1", Run: func(ctx context.Context, o *Resource) error {
				mu.Lock()
				executed = append(executed, "s1")
				mu.Unlock()
				return nil
			}},
		},
	}
	reconciler := ReconcilerFunc(func(ctx context.Context, k Key) error {
		o, err := store.Get(ctx, k.GVK, k.Namespace, k.Name)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = fin.HandleDeletion(ctx, o)
		return err
	})

	// 第一个控制器实例运行后“进程退出”（cancel）。
	q1 := NewWorkQueue(QueueConfig{Concurrency: 1, Backoff: BackoffPolicy{BaseDelay: 5 * time.Millisecond}})
	c1 := &Controller{Name: "c1", Store: store, Queue: q1, Do: reconciler,
		ListGVKs: []GroupVersionKind{widgetGVK}}
	ctx1, cancel1 := context.WithCancel(ctx)
	done1 := make(chan struct{})
	go func() { c1.Run(ctx1); close(done1) }()
	waitFor(t, "cleanup step executed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(executed) > 0
	})
	cancel1()
	<-done1

	// 对象应已被真正删除；若没有，第二个实例（重启）必须能继续完成。
	q2 := NewWorkQueue(QueueConfig{Concurrency: 1, Backoff: BackoffPolicy{BaseDelay: 5 * time.Millisecond}})
	c2 := &Controller{Name: "c2", Store: store, Queue: q2, Do: reconciler,
		ListGVKs: []GroupVersionKind{widgetGVK}}
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go c2.Run(ctx2)
	waitFor(t, "object fully deleted", func() bool {
		_, err := store.Get(ctx, widgetGVK, "default", "restart")
		return errors.Is(err, ErrNotFound)
	})
}

// TestEnsureFinalizerIdempotent 验证 Ensure 幂等：重复调用不重复添加终结器。
func TestEnsureFinalizerIdempotent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	obj := newWidget("ensure")
	if err := store.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	fin := &Finalizer{Name: "test.io/cleanup", Store: store}
	changed, err := fin.Ensure(ctx, obj)
	if err != nil || !changed {
		t.Fatalf("first Ensure = %v, %v", changed, err)
	}
	got, err := store.Get(ctx, widgetGVK, "default", "ensure")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Meta.Finalizers) != 1 {
		t.Fatalf("expected 1 finalizer, got %v", got.Meta.Finalizers)
	}
	// 第二次：已存在，不再修改。
	changed, err = fin.Ensure(ctx, got)
	if err != nil || changed {
		t.Fatalf("second Ensure = %v, %v, want false", changed, err)
	}
	// 删除流程中也不再添加。
	if err := store.Delete(ctx, widgetGVK, "default", "ensure"); err != nil {
		t.Fatal(err)
	}
	got, _ = store.Get(ctx, widgetGVK, "default", "ensure")
	changed, err = fin.Ensure(ctx, got)
	if err != nil || changed {
		t.Fatalf("Ensure during deletion = %v, %v, want false", changed, err)
	}
}
