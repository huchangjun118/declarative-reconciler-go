package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrForbidden 表示越权写入被拒绝。
var ErrForbidden = errors.New("reconcile: forbidden")

// Identity 是发起变更的主体身份。
type Identity struct {
	User   string
	Groups []string
}

type ctxKeyIdentity struct{}

// WithIdentity 把身份放入上下文。
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKeyIdentity{}, id)
}

// IdentityFrom 从上下文取身份；没有则返回匿名。
func IdentityFrom(ctx context.Context) Identity {
	if id, ok := ctx.Value(ctxKeyIdentity{}).(Identity); ok {
		return id
	}
	return Identity{User: "system:anonymous"}
}

// Verb 是变更动作。
type Verb string

const (
	VerbCreate Verb = "create"
	VerbUpdate Verb = "update"
	VerbDelete Verb = "delete"
)

// Authorizer 决定一次变更是否被允许。
type Authorizer interface {
	Allow(id Identity, verb Verb, gvk GroupVersionKind, namespace, name string) bool
}

// AllowAll 允许一切（默认）。
type AllowAll struct{}

func (AllowAll) Allow(Identity, Verb, GroupVersionKind, string, string) bool { return true }

// StaticAuthorizer 按 "user|verb|group|kind" 规则表授权，便于测试。
type StaticAuthorizer struct {
	Rules map[string]bool
}

func (s StaticAuthorizer) Allow(id Identity, verb Verb, gvk GroupVersionKind, _, _ string) bool {
	return s.Rules[id.User+"|"+string(verb)+"|"+gvk.Group+"|"+gvk.Kind]
}

// AuditEntry 是一条审计记录。
type AuditEntry struct {
	Time      time.Time
	User      string
	Verb      Verb
	GVK       GroupVersionKind
	Namespace string
	Name      string
	Allowed   bool
	Reason    string
}

// AuditSink 接收审计记录。
type AuditSink interface {
	Record(e AuditEntry)
}

// MemoryAuditSink 把审计记录保存在内存中。
type MemoryAuditSink struct {
	mu      sync.Mutex
	Entries []AuditEntry
}

func (m *MemoryAuditSink) Record(e AuditEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Entries = append(m.Entries, e)
}

// SecureStore 在 Store 之上叠加权限校验与审计：
// 所有变更先经 Authorizer 判定，越权写入被拒绝；
// 每一次变更与拒绝都会写入审计记录。
type SecureStore struct {
	Store  Store
	Auth   Authorizer
	Audits AuditSink
	now    func() time.Time
}

// NewSecureStore 创建带权限与审计的存储包装；auth/audits 为 nil 时使用默认值。
func NewSecureStore(s Store, auth Authorizer, audits AuditSink) *SecureStore {
	if auth == nil {
		auth = AllowAll{}
	}
	if audits == nil {
		audits = &MemoryAuditSink{}
	}
	return &SecureStore{Store: s, Auth: auth, Audits: audits, now: time.Now}
}

func (s *SecureStore) check(ctx context.Context, verb Verb, gvk GroupVersionKind, ns, name string) error {
	id := IdentityFrom(ctx)
	allowed := s.Auth.Allow(id, verb, gvk, ns, name)
	e := AuditEntry{
		Time:      s.now(),
		User:      id.User,
		Verb:      verb,
		GVK:       gvk,
		Namespace: ns,
		Name:      name,
		Allowed:   allowed,
	}
	if !allowed {
		e.Reason = "denied by authorizer"
	}
	s.Audits.Record(e)
	if !allowed {
		return fmt.Errorf("%w: user %q cannot %s %s %s/%s", ErrForbidden, id.User, verb, gvk, ns, name)
	}
	return nil
}

func (s *SecureStore) Create(ctx context.Context, obj *Resource) error {
	if err := s.check(ctx, VerbCreate, obj.GVK(), obj.Meta.Namespace, obj.Meta.Name); err != nil {
		return err
	}
	return s.Store.Create(ctx, obj)
}

func (s *SecureStore) Update(ctx context.Context, obj *Resource) error {
	if err := s.check(ctx, VerbUpdate, obj.GVK(), obj.Meta.Namespace, obj.Meta.Name); err != nil {
		return err
	}
	return s.Store.Update(ctx, obj)
}

func (s *SecureStore) UpdateStatus(ctx context.Context, obj *Resource) error {
	if err := s.check(ctx, VerbUpdate, obj.GVK(), obj.Meta.Namespace, obj.Meta.Name); err != nil {
		return err
	}
	return s.Store.UpdateStatus(ctx, obj)
}

func (s *SecureStore) Delete(ctx context.Context, gvk GroupVersionKind, ns, name string) error {
	if err := s.check(ctx, VerbDelete, gvk, ns, name); err != nil {
		return err
	}
	return s.Store.Delete(ctx, gvk, ns, name)
}

func (s *SecureStore) Get(ctx context.Context, gvk GroupVersionKind, ns, name string) (*Resource, error) {
	return s.Store.Get(ctx, gvk, ns, name)
}

func (s *SecureStore) List(ctx context.Context, gvk GroupVersionKind) ([]*Resource, error) {
	return s.Store.List(ctx, gvk)
}

func (s *SecureStore) Watch(ctx context.Context) (<-chan Event, error) {
	return s.Store.Watch(ctx)
}
