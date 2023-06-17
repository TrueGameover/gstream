package types

import (
	"context"
	"github.com/google/uuid"
)

type FixedSizeObserver[T interface{}] interface {
	Publish(element T)
	Subscribe(ctx context.Context) <-chan T
	GetLength() int
	Release()
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
