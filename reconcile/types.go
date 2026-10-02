// Package reconcile 提供声明式资源的控制器调和运行时：
// 把资源的期望状态（Spec）持续调和成实际状态（Status）。
// 底层资源存储可替换（Store 接口），本地使用内存实现即可跑通全流程，
// 不依赖任何外部服务。
package reconcile

import "time"

// GroupVersionKind 标识一个资源的组、版本与种类。
type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

// String 返回 GVK 的可读表示。
func (g GroupVersionKind) String() string {
	return g.Group + "/" + g.Version + ", Kind=" + g.Kind
}

// ObjectMeta 是所有资源共有的元数据。
type ObjectMeta struct {
	Namespace         string
	Name              string
	ResourceVersion   int64
	Generation        int64
	DeletionTimestamp *time.Time
	Finalizers        []string
	Labels            map[string]string
	Annotations       map[string]string
}

// Condition 描述资源状态的一个条件项。
type Condition struct {
	Type               string
	Status             string // "True" / "False" / "Unknown"
	Reason             string
	Message            string
	ObservedGeneration int64
	LastTransitionTime time.Time
}

// ResourceStatus 是资源的状态子资源。
type ResourceStatus struct {
	ObservedGeneration int64
	Conditions         []Condition
	Extra              map[string]any
}

// Resource 是一个声明式资源对象。
// Spec 使用 map 以支持多存储版本之间的转换。
type Resource struct {
	APIVersion string
	Kind       string
	Meta       ObjectMeta
	Spec       map[string]any
	Status     ResourceStatus
}

// Key 是资源在队列与缓存中的唯一键。
type Key struct {
	GVK       GroupVersionKind
	Namespace string
	Name      string
}

// String 返回键的可读表示。
func (k Key) String() string {
	return k.GVK.String() + " " + k.Namespace + "/" + k.Name
}

// GVK 返回资源的 GroupVersionKind。
func (r *Resource) GVK() GroupVersionKind {
	g, v := splitAPIVersion(r.APIVersion)
	return GroupVersionKind{Group: g, Version: v, Kind: r.Kind}
}

// KeyOf 返回资源的键。
func KeyOf(r *Resource) Key {
	return Key{GVK: r.GVK(), Namespace: r.Meta.Namespace, Name: r.Meta.Name}
}

// HasFinalizer 报告资源是否带有指定终结器。
func (r *Resource) HasFinalizer(f string) bool {
	for _, x := range r.Meta.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}

// DeepCopy 返回资源的深拷贝。
func (r *Resource) DeepCopy() *Resource {
	out := *r
	out.Spec = deepCopyMap(r.Spec)
	out.Meta.Labels = deepCopyStringMap(r.Meta.Labels)
	out.Meta.Annotations = deepCopyStringMap(r.Meta.Annotations)
	if r.Meta.Finalizers != nil {
		out.Meta.Finalizers = append([]string(nil), r.Meta.Finalizers...)
	}
	if r.Meta.DeletionTimestamp != nil {
		t := *r.Meta.DeletionTimestamp
		out.Meta.DeletionTimestamp = &t
	}
	out.Status = *r.Status.DeepCopy()
	return &out
}

// DeepCopy 返回状态的深拷贝。
func (s *ResourceStatus) DeepCopy() *ResourceStatus {
	out := *s
	if s.Conditions != nil {
		out.Conditions = append([]Condition(nil), s.Conditions...)
	}
	out.Extra = deepCopyMap(s.Extra)
	return &out
}

func splitAPIVersion(apiVersion string) (group, version string) {
	for i := len(apiVersion) - 1; i >= 0; i-- {
		if apiVersion[i] == '/' {
			return apiVersion[:i], apiVersion[i+1:]
		}
	}
	return "", apiVersion
}

func deepCopyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		switch tv := v.(type) {
		case map[string]any:
			out[k] = deepCopyMap(tv)
		case []any:
			out[k] = deepCopySlice(tv)
		default:
			out[k] = v
		}
	}
	return out
}

func deepCopySlice(in []any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		if m, ok := v.(map[string]any); ok {
			out[i] = deepCopyMap(m)
		} else {
			out[i] = v
		}
	}
	return out
}

func deepCopyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
