package observer

import (
	"context"
	"github.com/TrueGameover/gstream/src/types"
	"sync"
	"sync/atomic"
	"time"
)

type ObserverImpl[T interface{}] struct {
	subscribers        sync.Map
	subscribersCounter atomic.Uint64
	repeatInterval     time.Duration
}

func NewObserverImpl[T interface{}](repeatInterval time.Duration) types.Observer[T] {
	return &ObserverImpl[T]{
		repeatInterval: repeatInterval,
	}
}

func (h *ObserverImpl[T]) Publish(element T) bool {
	ack := false

	h.subscribers.Range(func(key, value any) bool {
		s := value.(types.Subscriber[T])

		if s.Received(element) {
			ack = true
		}

		return !ack
	})

	return ack
}

func (h *ObserverImpl[T]) PublishWithWaiting(ctx context.Context, element T) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if h.Publish(element) {
			return
		}

		time.Sleep(h.repeatInterval)
	}
}

func (h *ObserverImpl[T]) Subscribe(s types.Subscriber[T]) uint64 {
	id := h.subscribersCounter.Add(1)

	h.subscribers.Store(id, s)

	return id
}

func (h *ObserverImpl[T]) Unsubscribe(id uint64) {
	h.subscribers.Delete(id)
}

func (h *ObserverImpl[T]) Release() {
	h.subscribers.Range(func(key, value any) bool {
		h.subscribers.Delete(key)
		return true
	})
}
