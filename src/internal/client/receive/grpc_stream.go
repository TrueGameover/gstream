package receive

import (
	"context"
	"errors"
	"google.golang.org/grpc"
)

type GrpcStreamDecorator[T interface{}] struct {
	grpcClientStream grpc.ClientStream
	grpcServerStream grpc.ServerStream
	ctx              context.Context
	streamChannel    *chan T
	channelSize      int
	terminationFunc  context.CancelFunc
}

type recvMessage interface {
	RecvMsg(m interface{}) error
}

func NewGrpcStreamDecorator[T interface{}](
	ctx context.Context,
	channelSize int,
	grpcClientStream grpc.ClientStream,
	grpcServerStream grpc.ServerStream,
) (*GrpcStreamDecorator[T], error) {
	if grpcClientStream == nil && grpcServerStream == nil {
		return nil, errors.New("client or server stream expected")
	}

	internalCtx, cancelFunc := context.WithCancel(ctx)

	return &GrpcStreamDecorator[T]{
		grpcClientStream: grpcClientStream,
		grpcServerStream: grpcServerStream,
		ctx:              internalCtx,
		streamChannel:    nil,
		channelSize:      channelSize,
		terminationFunc:  cancelFunc,
	}, nil
}

func (w *GrpcStreamDecorator[T]) Fetch() (<-chan T, error) {
	if w.streamChannel != nil {
		return nil, errors.New("stream already has listener")
	}

	channel := make(chan T, w.channelSize)
	w.streamChannel = &channel

	var recv recvMessage
	if w.grpcServerStream != nil {
		recv = w.grpcServerStream
	} else {
		recv = w.grpcClientStream
	}

	go func() {
		defer close(channel)

		for {
			var msg T
			err := recv.RecvMsg(&msg)

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
