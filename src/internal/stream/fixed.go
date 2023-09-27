package stream

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
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
	messagesCounter         atomic.Uint64
	ackRequired             bool
	ackMessagesChannel      chan uint64

	deliveryRetriesMaxCount int
	deliveryRetries         map[uint64]int
}

type channelBag[T interface{}] struct {
	ch  chan T
	ctx context.Context
}

type identifiedMessage[T interface{}] struct {
	id      uint64
	payload T
}

func NewFixedSizeObserver[T interface{}](
	ctx context.Context,
	maxSize int,
	waitElementsInterval time.Duration,
	waitSubscribersInterval time.Duration,
	skipElements bool,
	skipWithoutSubscribers bool,
	acquiringRequired bool,
	retriesMaxCount int,
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
		ackRequired:             acquiringRequired,
		ackMessagesChannel:      make(chan uint64, maxSize),
		deliveryRetriesMaxCount: retriesMaxCount,
		deliveryRetries:         map[uint64]int{},
	}

	go obj.dispatchToChannels(internalCtx)

	return &obj
}

func (q *FixedSizeObserver[T]) Publish(element T) uint64 {
	if q.subscribersBag.Len() == 0 && q.skipWithoutSubscribers {
		return 0
	}

	msgId := q.messagesCounter.Add(1)

	q.internalPublish(element, msgId)

	return msgId
}

func (q *FixedSizeObserver[T]) internalPublish(element T, msgId uint64) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	actualSize := q.elementsList.Len()

	if actualSize >= q.maxSize {
		diff := actualSize - q.maxSize + 1
		for i := 0; i < diff; i++ {
			_ = q.catchHead()
		}
	}

	q.elementsList.PushBack(identifiedMessage[T]{
		id:      msgId,
		payload: element,
	})
}

func (q *FixedSizeObserver[T]) pop() *identifiedMessage[T] {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	element := q.catchHead()

	return element
}

func (q *FixedSizeObserver[T]) catchHead() *identifiedMessage[T] {
	head := q.elementsList.Front()

	if head == nil {
		return nil
	}

	q.elementsList.Remove(head)

	element, ok := head.Value.(identifiedMessage[T])

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

		internalMsg := q.pop()

		if internalMsg == nil {
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
			case bag.ch <- internalMsg.payload:
				hasReceivers = true
			default:
			}

			subscriber = subscriber.Next()
		}

		if !hasReceivers && !q.skipElements {
			retriesCount := q.deliveryRetries[internalMsg.id]
			retriesCount++

			// repeating to max retries count
			if retriesCount < q.deliveryRetriesMaxCount {
				// message not delivered, push it again
				q.internalPublish(internalMsg.payload, internalMsg.id)

				q.deliveryRetries[internalMsg.id] = retriesCount

			} else {
				delete(q.deliveryRetries, internalMsg.id)
			}
		} else {
			delete(q.deliveryRetries, internalMsg.id)
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

	close(q.ackMessagesChannel)
}

func (q *FixedSizeObserver[T]) Ack(msgId uint64) {
	//q.ackMessagesChannel <- msgId
	panic("not implemented")
}

func (q *FixedSizeObserver[T]) WaitAck(msgId uint64) {
	panic("not implemented")
}
