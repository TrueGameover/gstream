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
	subscribersBag          *list.List
	waitElementsInterval    time.Duration
	waitSubscribersInterval time.Duration
	skipElements            bool
	skipWithoutSubscribers  bool
	terminationFunc         context.CancelFunc
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
	internalCtx, cancel := context.WithCancel(ctx)

	obj := FixedSizeObserver[T]{
		maxSize:                 maxSize,
		elementsList:            l,
		mutex:                   &m,
		subscribersBag:          channelsBagList,
		waitElementsInterval:    waitElementsInterval,
		waitSubscribersInterval: waitSubscribersInterval,
		skipElements:            skipElements,
		skipWithoutSubscribers:  skipWithoutSubscribers,
		terminationFunc:         cancel,
	}

	go obj.dispatchToChannels(internalCtx)

	return &obj
}

func (q *FixedSizeObserver[T]) Publish(element T) {
	if q.subscribersBag.Len() == 0 && q.skipWithoutSubscribers {
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
	q.subscribersBag.Remove(element)
	return nextElement
}

func (q *FixedSizeObserver[T]) dispatchToChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if q.subscribersBag.Len() == 0 {
			time.Sleep(q.waitSubscribersInterval)
			continue
		}

		head := q.pop()

		if head == nil {
			time.Sleep(q.waitElementsInterval)
			continue
		}

		subscriber := q.subscribersBag.Front()
		hasReceivers := false
		for subscriber != nil {
			bag, ok := subscriber.Value.(channelBag[T])

			if !ok {
				subscriber = q.removeBagAndGoNext(subscriber, nil)
				continue
			}

			select {
			case <-bag.ctx.Done():
				subscriber = q.removeBagAndGoNext(subscriber, &bag)
				continue
			default:
			}

			// skip on sending problems
			select {
			case bag.ch <- *head:
				hasReceivers = true
			default:
			}

			subscriber = subscriber.Next()
		}

		if !hasReceivers && !q.skipElements {
			// message not delivered, push it again
			q.Publish(*head)
		}
	}
}

func (q *FixedSizeObserver[T]) Subscribe(ctx context.Context) <-chan T {
	elementsChannel := make(chan T, q.maxSize)
	q.subscribersBag.PushBack(channelBag[T]{
		ch:  elementsChannel,
		ctx: ctx,
	})

	return elementsChannel
}

func (q *FixedSizeObserver[T]) GetLength() int {
	return q.elementsList.Len()
}

func (q *FixedSizeObserver[T]) Release() {
	q.terminationFunc()

	subscriber := q.subscribersBag.Front()
	for subscriber != nil {
		bag, ok := subscriber.Value.(channelBag[T])

		if ok {
			subscriber = q.removeBagAndGoNext(subscriber, &bag)
		}
	}
}
