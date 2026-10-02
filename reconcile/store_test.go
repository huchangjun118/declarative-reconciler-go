package reconcile

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreCASAndGeneration(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	obj := newWidget("cas")
	if err := s.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, newWidget("cas")); !errors.Is(err, ErrAlreadyExist) {
		t.Fatalf("expected ErrAlreadyExist, got %v", err)
	}

	got, err := s.Get(ctx, widgetGVK, "default", "cas")
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta.Generation != 1 || got.Meta.ResourceVersion == 0 {
		t.Fatalf("initial gen/rv wrong: %+v", got.Meta)
	}

	// 仅元数据变化：不推进代数。
	v1 := got.DeepCopy()
	v1.Meta.Labels = map[string]string{"a": "b"}
	if err := s.Update(ctx, v1); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, widgetGVK, "default", "cas")
	if got.Meta.Generation != 1 {
		t.Fatalf("metadata-only update bumped generation to %d", got.Meta.Generation)
	}

	// spec 变化：推进代数。
	v2 := got.DeepCopy()
	v2.Spec["size"] = 9
	if err := s.Update(ctx, v2); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, widgetGVK, "default", "cas")
	if got.Meta.Generation != 2 {
		t.Fatalf("spec update did not bump generation: %d", got.Meta.Generation)
	}

	// 旧版本写入：冲突。
	if err := s.Update(ctx, v2); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for stale write, got %v", err)
	}

	// 状态写回不推进代数。
	if err := s.UpdateStatus(ctx, got); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Get(ctx, widgetGVK, "default", "cas")
	if after.Meta.Generation != 2 {
		t.Fatalf("status update bumped generation to %d", after.Meta.Generation)
	}
	// 状态写回不影响 spec。
	if after.Spec["size"] != 9 {
		t.Fatalf("status write clobbered spec: %v", after.Spec)
	}
}

func TestMemoryStoreWatchEvents(t *testing.T) {
	s := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, newWidget("watched")); err != nil {
		t.Fatal(err)
	}
	ev := <-events
	if ev.Type != EventAdded || ev.Object.Meta.Name != "watched" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if err := s.Delete(ctx, widgetGVK, "default", "watched"); err != nil {
		t.Fatal(err)
	}
	ev = <-events
	if ev.Type != EventDeleted {
		t.Fatalf("expected Deleted, got %v", ev.Type)
	}
}

func TestMemoryStoreFinalizerDeleteFlow(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	obj := newWidget("fin")
	obj.Meta.Finalizers = []string{"test.io/cleanup"}
	if err := s.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	// 删除只打标记。
	if err := s.Delete(ctx, widgetGVK, "default", "fin"); err != nil {
		t.Fatal(err)
	}
	cur, err := s.Get(ctx, widgetGVK, "default", "fin")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Meta.DeletionTimestamp == nil {
		t.Fatal("expected deletion timestamp")
	}
	// 移除终结器后对象被真正删除。
	cur.Meta.Finalizers = nil
	if err := s.Update(ctx, cur); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, widgetGVK, "default", "fin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected object removed, got %v", err)
	}
}
