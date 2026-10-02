package reconcile

import (
	"context"
	"testing"
	"time"
)

// TestConditionsAndObservedGeneration 验证：
// 调和完成后写回条件列表与 ObservedGeneration；
// 只有 Spec 真正推进时代谢才更新。
func TestConditionsAndObservedGeneration(t *testing.T) {
	store := NewMemoryStore()
	q := NewWorkQueue(QueueConfig{Concurrency: 1, Backoff: BackoffPolicy{BaseDelay: time.Millisecond}})

	reconciled := make(chan Key, 8)
	c := &Controller{
		Name:     "cond",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			return UpdateStatusWithRetry(ctx, store, store, k, func(obj *Resource) {
				obj.Status.Conditions = []Condition{
					{Type: "Ready", Status: "True", Reason: "Reconciled",
						LastTransitionTime: time.Unix(0, 0)},
				}
			})
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	if err := store.Create(ctx, newWidget("cond")); err != nil {
		t.Fatal(err)
	}
	k := testKey("cond")
	reconciled <- k
	waitFor(t, "condition written", func() bool {
		obj, err := store.Get(ctx, widgetGVK, "default", "cond")
		return err == nil && len(obj.Status.Conditions) == 1 &&
			obj.Status.ObservedGeneration == obj.Meta.Generation
	})

	obj, err := store.Get(ctx, widgetGVK, "default", "cond")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration = %d, want 1", obj.Status.ObservedGeneration)
	}
	if obj.Status.Conditions[0].Type != "Ready" || obj.Status.Conditions[0].Status != "True" {
		t.Fatalf("condition wrong: %+v", obj.Status.Conditions[0])
	}

	// 再次更新 spec（推进到 generation 2），调和后观测代数应为 2。
	obj.Spec["size"] = 7
	if err := store.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "observed generation advances", func() bool {
		got, err := store.Get(ctx, widgetGVK, "default", "cond")
		return err == nil && got.Meta.Generation == 2 && got.Status.ObservedGeneration == 2
	})
}

// TestSlowResourceDoesNotBlockOthers 验证：
// 单个慢资源只占用自己的 worker，不阻塞其他资源调和。
func TestSlowResourceDoesNotBlockOthers(t *testing.T) {
	store := NewMemoryStore()
	// 并发度 2：一个慢资源 + 一个快资源。
	q := NewWorkQueue(QueueConfig{Concurrency: 2, Backoff: BackoffPolicy{BaseDelay: time.Millisecond}})

	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})
	fastDone := make(chan Key, 1)
	c := &Controller{
		Name:     "slow",
		Store:    store,
		Queue:    q,
		ListGVKs: []GroupVersionKind{widgetGVK},
		Do: ReconcilerFunc(func(ctx context.Context, k Key) error {
			if k.Name == "slow" {
				close(slowStarted)
				<-slowRelease // 阻塞直到测试释放
				return nil
			}
			fastDone <- k
			return nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	if err := store.Create(ctx, newWidget("slow")); err != nil {
		t.Fatal(err)
	}
	<-slowStarted // 确认慢资源已被 worker 取走

	if err := store.Create(ctx, newWidget("fast")); err != nil {
		t.Fatal(err)
	}
	select {
	case k := <-fastDone:
		if k.Name != "fast" {
			t.Fatalf("unexpected key: %v", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow resource blocked fast resource: no free worker")
	}
	close(slowRelease)
}
