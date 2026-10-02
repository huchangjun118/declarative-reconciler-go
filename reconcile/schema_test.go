package reconcile

import (
	"errors"
	"reflect"
	"testing"
)

// 测试用两个版本：v1（旧，字段名 size）与 v2（枢纽，字段名 replicas + 新增 mode）。
func testRegistry(t *testing.T) *Registry {
	t.Helper()
	reg := NewRegistry()
	v1 := &Schema{
		GVK: GroupVersionKind{Group: "test.io", Version: "v1", Kind: "Widget"},
		Default: func(r *Resource) {
			if _, ok := r.Spec["size"]; !ok {
				r.Spec["size"] = 1
			}
		},
		Validate: func(r *Resource) error {
			if _, ok := r.Spec["image"]; !ok {
				return errors.New("spec.image is required")
			}
			return nil
		},
		ConvertToHub: func(r *Resource) (*Resource, error) {
			out := r.DeepCopy()
			out.Spec = map[string]any{
				"replicas": r.Spec["size"],
				"image":    r.Spec["image"],
			}
			return out, nil
		},
		ConvertFromHub: func(r *Resource) (*Resource, error) {
			out := r.DeepCopy()
			out.Spec = map[string]any{
				"size":  r.Spec["replicas"],
				"image": r.Spec["image"],
			}
			return out, nil
		},
	}
	v2 := &Schema{
		GVK: GroupVersionKind{Group: "test.io", Version: "v2", Kind: "Widget"},
		Hub: true,
		Default: func(r *Resource) {
			if _, ok := r.Spec["replicas"]; !ok {
				r.Spec["replicas"] = 1
			}
			if _, ok := r.Spec["mode"]; !ok {
				r.Spec["mode"] = "standard"
			}
		},
		Validate: func(r *Resource) error {
			if _, ok := r.Spec["image"]; !ok {
				return errors.New("spec.image is required")
			}
			return nil
		},
		ImmutableFields: []string{"image"},
	}
	if err := reg.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(v2); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestConversionRoundTrip(t *testing.T) {
	reg := testRegistry(t)
	v1obj := &Resource{
		APIVersion: "test.io/v1",
		Kind:       "Widget",
		Meta:       ObjectMeta{Namespace: "default", Name: "w", ResourceVersion: 7, Generation: 3},
		Spec:       map[string]any{"size": 5, "image": "nginx:1.25"},
	}
	hub, err := reg.ToHub(v1obj)
	if err != nil {
		t.Fatal(err)
	}
	if hub.APIVersion != "test.io/v2" {
		t.Fatalf("hub version = %s", hub.APIVersion)
	}
	if hub.Spec["replicas"] != 5 || hub.Spec["image"] != "nginx:1.25" {
		t.Fatalf("hub spec = %v", hub.Spec)
	}
	// 元数据与状态在转换中原样保留。
	if hub.Meta.ResourceVersion != 7 || hub.Meta.Generation != 3 {
		t.Fatalf("meta changed in conversion: %+v", hub.Meta)
	}
	back, err := reg.FromHub(hub, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Spec, v1obj.Spec) {
		t.Fatalf("round trip lost fields: got %v want %v", back.Spec, v1obj.Spec)
	}
	// 枢纽版本自身转换是恒等的。
	same, err := reg.ToHub(hub)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(same.Spec, hub.Spec) {
		t.Fatalf("hub identity conversion changed spec: %v -> %v", hub.Spec, same.Spec)
	}
}

func TestDefaultsAndValidation(t *testing.T) {
	reg := testRegistry(t)
	obj := &Resource{
		APIVersion: "test.io/v2",
		Kind:       "Widget",
		Spec:       map[string]any{"image": "nginx"},
	}
	if err := reg.ApplyDefaultsAndValidate(obj); err != nil {
		t.Fatal(err)
	}
	if obj.Spec["replicas"] != 1 || obj.Spec["mode"] != "standard" {
		t.Fatalf("defaults not applied: %v", obj.Spec)
	}
	bad := &Resource{APIVersion: "test.io/v2", Kind: "Widget", Spec: map[string]any{}}
	if err := reg.ApplyDefaultsAndValidate(bad); err == nil {
		t.Fatal("expected validation error for missing image")
	}
}

func TestImmutableFieldRejected(t *testing.T) {
	reg := testRegistry(t)
	oldObj := &Resource{
		APIVersion: "test.io/v2", Kind: "Widget",
		Spec: map[string]any{"image": "nginx", "replicas": 1},
	}
	// 修改可变字段：允许。
	okObj := oldObj.DeepCopy()
	okObj.Spec["replicas"] = 3
	if err := reg.CheckImmutable(oldObj, okObj); err != nil {
		t.Fatalf("mutable change rejected: %v", err)
	}
	// 修改不可变字段：拒绝。
	badObj := oldObj.DeepCopy()
	badObj.Spec["image"] = "redis"
	if err := reg.CheckImmutable(oldObj, badObj); !errors.Is(err, ErrImmutable) {
		t.Fatalf("expected ErrImmutable, got %v", err)
	}
	// 跨版本比较：v1 旧对象 vs v2 新对象，同样拒绝。
	v1Old := &Resource{
		APIVersion: "test.io/v1", Kind: "Widget",
		Spec: map[string]any{"size": 1, "image": "nginx"},
	}
	if err := reg.CheckImmutable(v1Old, badObj); !errors.Is(err, ErrImmutable) {
		t.Fatalf("cross-version: expected ErrImmutable, got %v", err)
	}
}
