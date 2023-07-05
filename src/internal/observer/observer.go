package observer

import (
	"github.com/TrueGameover/gstream/src/types"
)

type ObserverImpl[T interface{}] struct {
	subscribers []types.Subscriber[T]
}

func NewObserverImpl[T interface{}]() types.Observer[T] {
	return &ObserverImpl[T]{}
}

func (h *ObserverImpl[T]) Publish(element T) {
	for _, subscriber := range h.subscribers {
		if subscriber.Received(element) {
			return
		}
	}
}

func (h *ObserverImpl[T]) Subscribe(s types.Subscriber[T]) {
	h.subscribers = append(h.subscribers, s)
}

func (h *ObserverImpl[T]) Release() {
	h.subscribers = []types.Subscriber[T]{}
}
