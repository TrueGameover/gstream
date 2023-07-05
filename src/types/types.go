package types

import (
	"context"
	"github.com/google/uuid"
)

type FixedSizeObserver[T interface{}] interface {
	Publish(element T) uint64
	Subscribe(ctx context.Context) <-chan T
	GetLength() int
	Release()
	Ack(msgId uint64)
	WaitAck(msgId uint64)
}

type GrpcClient[T interface{}] interface {
	GetId() uuid.UUID
	SetId(id uuid.UUID)
	HasId() bool
	Stop()
	GetContext() context.Context
	Listen() error
}

type GrpcStreamDecorator[I interface{}, O interface{}] interface {
	Fetch() (<-chan O, error)
	Release()
}

type Observer[T interface{}] interface {
	Publish(element T)
	Subscribe(subscriber Subscriber[T])
	Release()
}

type Subscriber[T interface{}] interface {
	Received(T) bool
}
