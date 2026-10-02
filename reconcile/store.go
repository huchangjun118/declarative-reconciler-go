package reconcile

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

// 存储层错误。
var (
	ErrNotFound     = errors.New("reconcile: resource not found")
	ErrConflict     = errors.New("reconcile: resource version conflict")
	ErrAlreadyExist = errors.New("reconcile: resource already exists")
)

// EventType 是存储变更事件的类型。
type EventType string

const (
	EventAdded    EventType = "Added"
	EventModified EventType = "Modified"
	EventDeleted  EventType = "Deleted"
)

// Event 是一次存储变更通知。
type Event struct {
	Type   EventType
	Object *Resource
}

// Store 是可替换的资源存储接口。
type Store interface {
	Create(ctx context.Context, obj *Resource) error
	// Update 使用 ResourceVersion 做乐观并发校验（CAS），
	// 仅当 Spec 发生变化时递增 Generation。
	Update(ctx context.Context, obj *Resource) error
	// UpdateStatus 只写回状态子资源，同样做版本校验，不推进 Generation。
	UpdateStatus(ctx context.Context, obj *Resource) error
	Get(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Resource, error)
	// Delete 在对象带终结器时只设置 DeletionTimestamp；
	// 终结器清空后对象才被真正移除。
	Delete(ctx context.Context, gvk GroupVersionKind, namespace, name string) error
	List(ctx context.Context, gvk GroupVersionKind) ([]*Resource, error)
	// Watch 返回一个事件通道；ctx 取消后通道被关闭并注销。
	Watch(ctx context.Context) (<-chan Event, error)
}

// MemoryStore 是基于内存的 Store 实现，本地即可跑通全流程。
// 所有变更在锁内完成并广播事件，watch 订阅者各自拥有独立队列，
// 慢订阅者不会阻塞广播或其他订阅者。
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[Key]*Resource
	rv      int64

	subsMu sync.Mutex
	subs   map[int]*subscription
	nextID int
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[Key]*Resource),
		subs:    make(map[int]*subscription),
	}
}

func (s *MemoryStore) allocRV() int64 {
	s.rv++
	return s.rv
}

func (s *MemoryStore) Create(ctx context.Context, obj *Resource) error {
	s.mu.Lock()
	k := KeyOf(obj)
	if _, exists := s.objects[k]; exists {
		s.mu.Unlock()
		return ErrAlreadyExist
	}
	stored := obj.DeepCopy()
	stored.Meta.ResourceVersion = s.allocRV()
	if stored.Meta.Generation == 0 {
		stored.Meta.Generation = 1
	}
	s.objects[k] = stored
	s.mu.Unlock()
	s.broadcast(Event{Type: EventAdded, Object: stored.DeepCopy()})
	return nil
}

func (s *MemoryStore) Update(ctx context.Context, obj *Resource) error {
	s.mu.Lock()
	k := KeyOf(obj)
	cur, exists := s.objects[k]
	if !exists {
		s.mu.Unlock()
		return ErrNotFound
	}
	if obj.Meta.ResourceVersion != cur.Meta.ResourceVersion {
		s.mu.Unlock()
		return ErrConflict
	}
	stored := obj.DeepCopy()
	stored.Meta.ResourceVersion = s.allocRV()
	// 只有期望状态（Spec）真正推进时才更新代数；
	// 元数据或状态写回不推进。
	if !reflect.DeepEqual(cur.Spec, stored.Spec) {
		stored.Meta.Generation = cur.Meta.Generation + 1
	} else {
		stored.Meta.Generation = cur.Meta.Generation
	}
	// 删除流程中终结器被清空：对象被真正移除。
	if stored.Meta.DeletionTimestamp != nil && len(stored.Meta.Finalizers) == 0 {
		delete(s.objects, k)
		s.mu.Unlock()
		s.broadcast(Event{Type: EventDeleted, Object: stored.DeepCopy()})
		return nil
	}
	s.objects[k] = stored
	s.mu.Unlock()
	s.broadcast(Event{Type: EventModified, Object: stored.DeepCopy()})
	return nil
}

func (s *MemoryStore) UpdateStatus(ctx context.Context, obj *Resource) error {
	s.mu.Lock()
	k := KeyOf(obj)
	cur, exists := s.objects[k]
	if !exists {
		s.mu.Unlock()
		return ErrNotFound
	}
	if obj.Meta.ResourceVersion != cur.Meta.ResourceVersion {
		s.mu.Unlock()
		return ErrConflict
	}
	stored := cur.DeepCopy()
	stored.Status = *obj.Status.DeepCopy()
	stored.Meta.ResourceVersion = s.allocRV()
	s.objects[k] = stored
	s.mu.Unlock()
	s.broadcast(Event{Type: EventModified, Object: stored.DeepCopy()})
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, gvk GroupVersionKind, namespace, name string) (*Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[Key{GVK: gvk, Namespace: namespace, Name: name}]
	if !ok {
		return nil, ErrNotFound
	}
	return obj.DeepCopy(), nil
}

func (s *MemoryStore) Delete(ctx context.Context, gvk GroupVersionKind, namespace, name string) error {
	s.mu.Lock()
	k := Key{GVK: gvk, Namespace: namespace, Name: name}
	cur, exists := s.objects[k]
	if !exists {
		s.mu.Unlock()
		return ErrNotFound
	}
	if len(cur.Meta.Finalizers) > 0 && cur.Meta.DeletionTimestamp == nil {
		// 有终结器：只标记删除时间，等待清理完成。
		now := time.Now()
		stored := cur.DeepCopy()
		stored.Meta.DeletionTimestamp = &now
		stored.Meta.ResourceVersion = s.allocRV()
		s.objects[k] = stored
		s.mu.Unlock()
		s.broadcast(Event{Type: EventModified, Object: stored.DeepCopy()})
		return nil
	}
	// 无终结器（或已在删除流程中且终结器为空）：真正移除。
	delete(s.objects, k)
	s.mu.Unlock()
	s.broadcast(Event{Type: EventDeleted, Object: cur.DeepCopy()})
	return nil
}

func (s *MemoryStore) List(ctx context.Context, gvk GroupVersionKind) ([]*Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Resource
	for k, v := range s.objects {
		if k.GVK == gvk {
			out = append(out, v.DeepCopy())
		}
	}
	return out, nil
}

// subscription 是一个订阅者的内部事件队列，
// 由专属派发协程消费，避免慢订阅者阻塞广播或其他订阅者。
type subscription struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []Event
	closed bool
}

func newSubscription() *subscription {
	sub := &subscription{}
	sub.cond = sync.NewCond(&sub.mu)
	return sub
}

func (sub *subscription) push(ev Event) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.closed {
		return
	}
	sub.queue = append(sub.queue, ev)
	sub.cond.Signal()
}

func (sub *subscription) close() {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.closed = true
	sub.cond.Broadcast()
}

func (s *MemoryStore) Watch(ctx context.Context) (<-chan Event, error) {
	sub := newSubscription()
	out := make(chan Event, 64)
	s.subsMu.Lock()
	id := s.nextID
	s.nextID++
	s.subs[id] = sub
	s.subsMu.Unlock()

	// 派发协程：独占 out 的所有权，ctx 取消后关闭通道。
	go func() {
		defer close(out)
		for {
			sub.mu.Lock()
			for len(sub.queue) == 0 && !sub.closed {
				sub.cond.Wait()
			}
			if len(sub.queue) == 0 && sub.closed {
				sub.mu.Unlock()
				return
			}
			ev := sub.queue[0]
			sub.queue = sub.queue[1:]
			sub.mu.Unlock()
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	// 注销协程：ctx 取消后从订阅表移除并关闭订阅队列。
	go func() {
		<-ctx.Done()
		s.subsMu.Lock()
		delete(s.subs, id)
		s.subsMu.Unlock()
		sub.close()
	}()
	return out, nil
}

func (s *MemoryStore) broadcast(ev Event) {
	s.subsMu.Lock()
	subs := make([]*subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.subsMu.Unlock()
	for _, sub := range subs {
		sub.push(ev)
	}
}
