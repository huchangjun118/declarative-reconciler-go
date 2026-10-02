package reconcile

import (
	"context"
	"errors"
	"testing"
)

func TestSecureStoreAuthorizationAndAudit(t *testing.T) {
	base := NewMemoryStore()
	audits := &MemoryAuditSink{}
	auth := StaticAuthorizer{Rules: map[string]bool{
		"alice|create|test.io|Widget": true,
		"alice|update|test.io|Widget": true,
		// alice 无 delete 权限；bob 无任何权限。
	}}
	s := NewSecureStore(base, auth, audits)
	ctx := context.Background()

	// 允许：alice 创建。
	aliceCtx := WithIdentity(ctx, Identity{User: "alice"})
	if err := s.Create(aliceCtx, newWidget("sec")); err != nil {
		t.Fatalf("alice create denied: %v", err)
	}
	// 拒绝：bob 创建（越权）。
	bobCtx := WithIdentity(ctx, Identity{User: "bob"})
	if err := s.Create(bobCtx, newWidget("sec2")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden for bob, got %v", err)
	}
	// 拒绝：alice 删除（越权）。
	if err := s.Delete(aliceCtx, widgetGVK, "default", "sec"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden for alice delete, got %v", err)
	}
	// 越权写入确实未生效。
	if _, err := base.Get(ctx, widgetGVK, "default", "sec2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("denied write took effect: %v", err)
	}
	if _, err := base.Get(ctx, widgetGVK, "default", "sec"); err != nil {
		t.Fatalf("denied delete took effect: %v", err)
	}

	// 审计：3 次变更/拒绝都有记录，且可追溯。
	if len(audits.Entries) != 3 {
		t.Fatalf("expected 3 audit entries, got %d", len(audits.Entries))
	}
	type want struct {
		user    string
		verb    Verb
		allowed bool
	}
	wants := []want{
		{"alice", VerbCreate, true},
		{"bob", VerbCreate, false},
		{"alice", VerbDelete, false},
	}
	for i, w := range wants {
		e := audits.Entries[i]
		if e.User != w.user || e.Verb != w.verb || e.Allowed != w.allowed {
			t.Fatalf("audit[%d] = %+v, want %+v", i, e, w)
		}
		if e.GVK != widgetGVK || e.Namespace != "default" {
			t.Fatalf("audit[%d] missing resource identity: %+v", i, e)
		}
	}
}

func TestSecureStoreDefaultAllowAll(t *testing.T) {
	base := NewMemoryStore()
	s := NewSecureStore(base, nil, nil) // 默认 AllowAll + MemoryAuditSink
	ctx := context.Background()
	if err := s.Create(ctx, newWidget("def")); err != nil {
		t.Fatalf("default allow-all denied: %v", err)
	}
	if _, err := s.Get(ctx, widgetGVK, "default", "def"); err != nil {
		t.Fatalf("created object not readable: %v", err)
	}
}

func TestIdentityFromContext(t *testing.T) {
	ctx := context.Background()
	if id := IdentityFrom(ctx); id.User != "system:anonymous" {
		t.Fatalf("expected anonymous, got %q", id.User)
	}
	ctx = WithIdentity(ctx, Identity{User: "carol", Groups: []string{"admins"}})
	if id := IdentityFrom(ctx); id.User != "carol" || len(id.Groups) != 1 {
		t.Fatalf("identity round-trip wrong: %+v", id)
	}
}
