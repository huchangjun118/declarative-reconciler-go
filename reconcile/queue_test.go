package reconcile

import (
	"sync"
	"testing"
	"time"
)

func testKey(name string) Key {
	return Key{
		GVK:       GroupVersionKind{Group: "test.io", Version: "v1", Kind: "Widget"},
		Namespace: "default",
		Name:      name,
	}
}

func TestQueueDeduplicates(t *testing.T) {
	q := NewWorkQueue(QueueConfig{Concurrency: 1})
	defer q.ShutDown()
	k := testKey("a")
	for i := 0; i < 10; i++ {
		q.Add(k)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("expected 1 queued item after dedup, got %d", got)
	}
	got, ok := q.Get()
	if !ok || got != k {
		t.Fatalf("Get = %v,%v want %v,true", got, ok, k)
	}
	// 处理期间再次入队：Done 后应重新排队。
	q.Add(k)
	q.Done(k)
	if q.Len() != 1 {
		t.Fatalf("expected requeue after Done, len=%d", q.Len())
	}
}

func TestQueueBackoffAndMaxRetries(t *testing.T) {
	q := NewWorkQueue(QueueConfig{
		Concurrency: 1,
		MaxRetries:  3,
		Backoff:     BackoffPolicy{BaseDelay: time.Millisecond, Factor: 2, MaxDelay: 10 * time.Millisecond},
	})
	defer q.ShutDown()
	k := testKey("b")
	q.Add(k)
	for i := 1; i <= 3; i++ {
		got, ok := q.Get()
		if !ok || got != k {
			t.Fatalf("attempt %d: Get = %v,%v", i, got, ok)
		}
		if !q.Requeue(k) {
			t.Fatalf("attempt %d: unexpected drop", i)
		}
		// 等待退避到期后重新入队。
		deadline := time.Now().Add(2 * time.Second)
		for q.Len() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	got, ok := q.Get()
	if !ok || got != k {
		t.Fatalf("final Get = %v,%v", got, ok)
	}
	if q.Requeue(k) {
		t.Fatalf("expected drop after MaxRetries")
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty queue after drop, len=%d", q.Len())
	}
}

func TestBackoffDelayMonotonicAndCapped(t *testing.T) {
	b := BackoffPolicy{BaseDelay: 10 * time.Millisecond, Factor: 2, MaxDelay: 50 * time.Millisecond}
	d1 := b.Delay(1)
	d2 := b.Delay(2)
	d10 := b.Delay(10)
	if !(d1 < d2) {
		t.Fatalf("expected increasing delays: %v !< %v", d1, d2)
	}
	if d10 != 50*time.Millisecond {
		t.Fatalf("expected cap at 50ms, got %v", d10)
	}
}

func TestQueueConcurrentProducersConsumers(t *testing.T) {
	q := NewWorkQueue(QueueConfig{Concurrency: 8})
	var wg sync.WaitGroup
	const producers = 8
	const perProducer = 200
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				q.Add(testKey(string(rune('a'+p)) + "-" + string(rune('0'+i%10))))
			}
		}(p)
	}
	wg.Wait()
	q.ShutDown()
	seen := map[Key]int{}
	var mu sync.Mutex
	var consumers sync.WaitGroup
	for i := 0; i < 8; i++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				k, ok := q.Get()
				if !ok {
					return
				}
				mu.Lock()
				seen[k]++
				mu.Unlock()
				q.Done(k)
			}
		}()
	}
	consumers.Wait()
	total := 0
	for _, n := range seen {
		total += n
	}
	if total == 0 {
		t.Fatal("no items consumed")
	}
}
