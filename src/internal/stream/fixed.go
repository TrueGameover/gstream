package stream

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type FixedSizeObserver[T interface{}] struct {
	maxSize                 int
	elementsList            *list.List
	mutex                   *sync.Mutex
	channelsBag             *list.List
	waitElementsInterval    time.Duration
	waitSubscribersInterval time.Duration
	skipElements            bool
	skipWithoutSubscribers  bool
}

type channelBag[T interface{}] struct {
	ch  chan T
	ctx context.Context
}

func NewFixedSizeObserver[T interface{}](
	ctx context.Context,
	maxSize int,
	waitElementsInterval time.Duration,
	waitSubscribersInterval time.Duration,
	skipElements bool,
	skipWithoutSubscribers bool,
) *FixedSizeObserver[T] {
	l := list.New()
	l.Init()

	channelsBagList := list.New()
	channelsBagList.Init()

	m := sync.Mutex{}

	obj := FixedSizeObserver[T]{
		maxSize:                 maxSize,
		elementsList:            l,
		mutex:                   &m,
		channelsBag:             channelsBagList,
		waitElementsInterval:    waitElementsInterval,
		waitSubscribersInterval: waitSubscribersInterval,
		skipElements:            skipElements,
		skipWithoutSubscribers:  skipWithoutSubscribers,
	}

	go obj.dispatchToChannels(ctx)

	return &obj
}

func (q *FixedSizeObserver[T]) Publish(element T) {
	if q.channelsBag.Len() == 0 && q.skipWithoutSubscribers {
		return
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	actualSize := q.elementsList.Len()

	if actualSize >= q.maxSize {
		diff := actualSize - q.maxSize + 1
		for i := 0; i < diff; i++ {
			_ = q.catchHead()
		}
	}

	q.elementsList.PushBack(element)
}

func (q *FixedSizeObserver[T]) pop() *T {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	element := q.catchHead()

	return element
}

func (q *FixedSizeObserver[T]) catchHead() *T {
	head := q.elementsList.Front()

	if head == nil {
		return nil
	}

	q.elementsList.Remove(head)

	element, ok := head.Value.(T)

	if !ok {
		return nil
	}

	return &element
}

func (q *FixedSizeObserver[T]) removeBagAndGoNext(element *list.Element, value *channelBag[T]) *list.Element {
	if value != nil {
		close(value.ch)
	}

	nextElement := element.Next()
	q.channelsBag.Remove(element)
	return nextElement
}

func (q *FixedSizeObserver[T]) dispatchToChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if q.channelsBag.Len() == 0 {
			time.Sleep(q.waitSubscribersInterval)
			continue
		}

		head := q.pop()

		if head == nil {
			time.Sleep(q.waitElementsInterval)
			continue
		}

		element := q.channelsBag.Front()
		hasReceivers := false
		for element != nil {
			bag, ok := element.Value.(channelBag[T])

			if !ok {
				element = q.removeBagAndGoNext(element, nil)
				continue
			}

			select {
			case <-bag.ctx.Done():
				element = q.removeBagAndGoNext(element, &bag)
				continue
			default:
			}

			// skip on sending problems
			select {
			case bag.ch <- *head:
				hasReceivers = true
			default:
			}

			element = element.Next()
		}

		if !hasReceivers && !q.skipElements {
			// message not delivered, push it again
			q.Publish(*head)
		}
	}
}

func (q *FixedSizeObserver[T]) Subscribe(ctx context.Context) <-chan T {
	elementsChannel := make(chan T, q.maxSize)
	q.channelsBag.PushBack(channelBag[T]{
		ch:  elementsChannel,
		ctx: ctx,
	})

	return elementsChannel
}

func (q *FixedSizeObserver[T]) GetLength() int {
	return q.elementsList.Len()
}
