package reconcile

import (
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

func testKey(name string) Key {
	return Key{
		GVK:       GroupVersionKind{Group: "test.io", Version: "v1", Kind: "Widget"},
		Namespace: "default",
		Name:      name,
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
