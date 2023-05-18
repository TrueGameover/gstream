package receive

import (
	"context"
	"errors"
)

type GrpcStreamDecorator[T interface{}] struct {
	stream          IMessageReceive
	ctx             context.Context
	streamChannel   *chan T
	channelSize     int
	terminationFunc context.CancelFunc
}

type IMessageReceive interface {
	RecvMsg(m interface{}) error
}

func NewGrpcStreamDecorator[T interface{}](
	ctx context.Context,
	stream IMessageReceive,
	channelSize int,
) *GrpcStreamDecorator[T] {
	internalCtx, cancelFunc := context.WithCancel(ctx)

	return &GrpcStreamDecorator[T]{
		stream:          stream,
		ctx:             internalCtx,
		channelSize:     channelSize,
		terminationFunc: cancelFunc,
	}
}

func (w *GrpcStreamDecorator[T]) Fetch() (<-chan T, error) {
	if w.streamChannel != nil {
		return nil, errors.New("stream already has listener")
	}

	channel := make(chan T, w.channelSize)
	w.streamChannel = &channel

	go func() {
		defer close(channel)

		for {
			var msg T
			err := w.stream.RecvMsg(&msg)

			if err != nil {
				return
			}

			channel <- msg

			select {
			case <-w.ctx.Done():
				return
			default:
			}
		}
	}()

	return channel, nil
}

func (w *GrpcStreamDecorator[T]) Release() {
	w.terminationFunc()
}
