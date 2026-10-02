package reconcile

import (
	"context"
	"errors"
)

// CleanupStep 是删除前的一个清理步骤。步骤必须幂等：
// 进程在步骤之间崩溃后，已完成的步骤不会重复执行，
// 未完成的步骤会被重新执行。
type CleanupStep struct {
	Name string
	// Run 执行清理动作；返回 nil 表示该步骤完成。
	Run func(ctx context.Context, obj *Resource) error
}

// Finalizer 管理带检查点的删除清理流程。
// 每个步骤完成后先把进度持久化到对象状态，再执行下一步，
// 因此崩溃重启后可以从断点继续：既不跳过已完成的步骤，
// 也不重复执行已经生效的清理动作。
type Finalizer struct {
	Name  string
	Store Store
	Steps []CleanupStep
}

// cleanupProgressKey 是记录在 Status.Extra 中的进度键，
// 值为已完成步骤的数量。
const cleanupProgressKey = "reconcile.cleanupProgress"

func progressOf(obj *Resource) int {
	if obj.Status.Extra == nil {
		return 0
	}
	switch v := obj.Status.Extra[cleanupProgressKey].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

// Ensure 在对象未处于删除流程时确保终结器存在。
// 返回 true 表示本次调用修改了对象（调用方应停止本轮调和，等下一事件）。
func (f *Finalizer) Ensure(ctx context.Context, obj *Resource) (bool, error) {
	if obj.Meta.DeletionTimestamp != nil || f.hasFinalizer(obj) {
		return false, nil
	}
	err := updateMetaWithRetry(ctx, f.Store, KeyOf(obj), func(latest *Resource) {
		latest.Meta.Finalizers = append(latest.Meta.Finalizers, f.Name)
	})
	return true, err
}

func (f *Finalizer) hasFinalizer(obj *Resource) bool {
	return obj.HasFinalizer(f.Name)
}

// HandleDeletion 在对象处于删除流程时执行清理；
// 全部完成后移除终结器（对象随后被存储真正删除）。
// 返回 true 表示清理已全部完成。
func (f *Finalizer) HandleDeletion(ctx context.Context, obj *Resource) (bool, error) {
	if obj.Meta.DeletionTimestamp == nil || !f.hasFinalizer(obj) {
		return true, nil
	}
	done := progressOf(obj)
	for i := done; i < len(f.Steps); i++ {
		step := f.Steps[i]
		if err := step.Run(ctx, obj); err != nil {
			return false, err
		}
		// 步骤生效后立刻持久化检查点（CAS 重读，避免覆盖他人更新）。
		completed := i + 1
		err := UpdateStatusWithRetry(ctx, f.Store, f.Store, KeyOf(obj), func(latest *Resource) {
			if latest.Status.Extra == nil {
				latest.Status.Extra = map[string]any{}
			}
			latest.Status.Extra[cleanupProgressKey] = completed
		})
		if err != nil {
			return false, err
		}
	}
	// 全部清理完成：移除终结器，对象将被真正删除。
	err := updateMetaWithRetry(ctx, f.Store, KeyOf(obj), func(latest *Resource) {
		out := latest.Meta.Finalizers[:0]
		for _, x := range latest.Meta.Finalizers {
			if x != f.Name {
				out = append(out, x)
			}
		}
		latest.Meta.Finalizers = out
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// updateMetaWithRetry 以 CAS 重试方式修改对象元数据/规格。
func updateMetaWithRetry(ctx context.Context, s Store, k Key, mutate func(obj *Resource)) error {
	var lastErr error
	for i := 0; i < maxStatusRetries; i++ {
		obj, err := s.Get(ctx, k.GVK, k.Namespace, k.Name)
		if err != nil {
			return err
		}
		mutate(obj)
		err = s.Update(ctx, obj)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		lastErr = err
	}
	return lastErr
}
