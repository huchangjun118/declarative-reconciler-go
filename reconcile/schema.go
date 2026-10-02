package reconcile

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// ErrImmutable 表示更新试图修改不可变字段。
var ErrImmutable = errors.New("reconcile: immutable field changed")

// Schema 描述资源某一个存储版本的规则。
type Schema struct {
	GVK GroupVersionKind
	// Hub 为 true 表示这是当前（枢纽）版本。
	Hub bool
	// Default 在写入前填充默认值。
	Default func(r *Resource)
	// Validate 在写入前做必填校验。
	Validate func(r *Resource) error
	// ImmutableFields 是 spec 中不可变的顶层字段名。
	ImmutableFields []string
	// ConvertToHub 把本版本对象转换到枢纽版本。
	// 转换必须保留所有字段信息，保证来回转换不丢字段。
	ConvertToHub func(r *Resource) (*Resource, error)
	// ConvertFromHub 把枢纽版本对象转换回本版本。
	ConvertFromHub func(r *Resource) (*Resource, error)
}

// Registry 按种类注册多个存储版本，并负责版本转换。
type Registry struct {
	mu     sync.RWMutex
	byKind map[string]map[string]*Schema // group/kind -> version -> schema
}

// NewRegistry 创建空的版本注册表。
func NewRegistry() *Registry {
	return &Registry{byKind: make(map[string]map[string]*Schema)}
}

func kindKey(g GroupVersionKind) string { return g.Group + "/" + g.Kind }

// Register 注册一个版本的模式。每种最多一个枢纽版本。
func (reg *Registry) Register(s *Schema) error {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	kk := kindKey(s.GVK)
	if reg.byKind[kk] == nil {
		reg.byKind[kk] = make(map[string]*Schema)
	}
	if _, dup := reg.byKind[kk][s.GVK.Version]; dup {
		return fmt.Errorf("reconcile: schema %s already registered", s.GVK)
	}
	if s.Hub {
		for _, other := range reg.byKind[kk] {
			if other.Hub {
				return fmt.Errorf("reconcile: hub for %s already registered as %s", kk, other.GVK.Version)
			}
		}
	}
	reg.byKind[kk][s.GVK.Version] = s
	return nil
}

// SchemaFor 返回指定版本的模式。
func (reg *Registry) SchemaFor(gvk GroupVersionKind) (*Schema, error) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	if m, ok := reg.byKind[kindKey(gvk)]; ok {
		if s, ok := m[gvk.Version]; ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("reconcile: no schema for %s", gvk)
}

// HubSchema 返回该种类的枢纽版本模式。
func (reg *Registry) HubSchema(gvk GroupVersionKind) (*Schema, error) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	if m, ok := reg.byKind[kindKey(gvk)]; ok {
		for _, s := range m {
			if s.Hub {
				return s, nil
			}
		}
	}
	return nil, fmt.Errorf("reconcile: no hub schema for %s", gvk)
}

// ToHub 把任意已注册版本的对象转换到枢纽版本。
// 转换只改 APIVersion 与 Spec，元数据与状态原样保留。
func (reg *Registry) ToHub(r *Resource) (*Resource, error) {
	s, err := reg.SchemaFor(r.GVK())
	if err != nil {
		return nil, err
	}
	hub, err := reg.HubSchema(r.GVK())
	if err != nil {
		return nil, err
	}
	if s.GVK.Version == hub.GVK.Version {
		out := r.DeepCopy()
		out.APIVersion = hub.GVK.Group + "/" + hub.GVK.Version
		return out, nil
	}
	if s.ConvertToHub == nil {
		return nil, fmt.Errorf("reconcile: no ConvertToHub for %s", s.GVK)
	}
	out, err := s.ConvertToHub(r.DeepCopy())
	if err != nil {
		return nil, err
	}
	out.APIVersion = hub.GVK.Group + "/" + hub.GVK.Version
	out.Kind = hub.GVK.Kind
	return out, nil
}

// FromHub 把枢纽版本对象转换到目标版本。
func (reg *Registry) FromHub(r *Resource, version string) (*Resource, error) {
	hub, err := reg.HubSchema(r.GVK())
	if err != nil {
		return nil, err
	}
	if r.GVK().Version != hub.GVK.Version {
		return nil, fmt.Errorf("reconcile: FromHub expects hub version %s, got %s", hub.GVK.Version, r.GVK().Version)
	}
	target, err := reg.SchemaFor(GroupVersionKind{Group: hub.GVK.Group, Version: version, Kind: hub.GVK.Kind})
	if err != nil {
		return nil, err
	}
	if target.GVK.Version == hub.GVK.Version {
		return r.DeepCopy(), nil
	}
	if target.ConvertFromHub == nil {
		return nil, fmt.Errorf("reconcile: no ConvertFromHub for %s", target.GVK)
	}
	out, err := target.ConvertFromHub(r.DeepCopy())
	if err != nil {
		return nil, err
	}
	out.APIVersion = target.GVK.Group + "/" + target.GVK.Version
	out.Kind = target.GVK.Kind
	return out, nil
}

// ApplyDefaultsAndValidate 在写入前填充默认值并做必填校验。
func (reg *Registry) ApplyDefaultsAndValidate(r *Resource) error {
	s, err := reg.SchemaFor(r.GVK())
	if err != nil {
		return err
	}
	if s.Default != nil {
		s.Default(r)
	}
	if s.Validate != nil {
		if err := s.Validate(r); err != nil {
			return fmt.Errorf("reconcile: validation failed for %s: %w", s.GVK, err)
		}
	}
	return nil
}

// CheckImmutable 校验更新没有修改不可变字段。
// 新旧对象先各自转换到枢纽版本再比较，避免跨版本表示差异造成误判。
func (reg *Registry) CheckImmutable(oldObj, newObj *Resource) error {
	if oldObj == nil || newObj == nil {
		return nil
	}
	hub, err := reg.HubSchema(oldObj.GVK())
	if err != nil {
		return err
	}
	if len(hub.ImmutableFields) == 0 {
		return nil
	}
	oldHub, err := reg.ToHub(oldObj)
	if err != nil {
		return err
	}
	newHub, err := reg.ToHub(newObj)
	if err != nil {
		return err
	}
	for _, f := range hub.ImmutableFields {
		if !reflect.DeepEqual(oldHub.Spec[f], newHub.Spec[f]) {
			return fmt.Errorf("%w: spec.%s", ErrImmutable, f)
		}
	}
	return nil
}
