package observer

import (
	"context"
	"github.com/TrueGameover/gstream/src/types"
	"time"
)

type ObserverImpl[T interface{}] struct {
	subscribers    []types.Subscriber[T]
	repeatInterval time.Duration
}

func NewObserverImpl[T interface{}](repeatInterval time.Duration) types.Observer[T] {
	return &ObserverImpl[T]{
		repeatInterval: repeatInterval,
	}
}

func (h *ObserverImpl[T]) Publish(element T) bool {
	for _, subscriber := range h.subscribers {
		if subscriber.Received(element) {
			return true
		}
	}

	return false
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

func (h *ObserverImpl[T]) Subscribe(s types.Subscriber[T]) {
	h.subscribers = append(h.subscribers, s)
}

func (h *ObserverImpl[T]) Release() {
	h.subscribers = []types.Subscriber[T]{}
}
