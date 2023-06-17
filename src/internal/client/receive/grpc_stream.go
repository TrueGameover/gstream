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
	mapFunc          func(msg interface{}) T
	errorCallback    func(err error) error
}

type recvMessage interface {
	RecvMsg(m interface{}) error
}

func NewGrpcStreamDecorator[T interface{}](
	ctx context.Context,
	channelSize int,
	grpcClientStream grpc.ClientStream,
	grpcServerStream grpc.ServerStream,
	mappingFunc func(msg interface{}) T,
	errorCallback func(err error) error,
) (*GrpcStreamDecorator[T], error) {
	if grpcClientStream == nil && grpcServerStream == nil {
		return nil, errors.New("client or server stream expected")
	}

	internalCtx, cancelFunc := context.WithCancel(ctx)

	if errorCallback == nil {
		errorCallback = func(err error) error {
			return err
		}
	}

	return &GrpcStreamDecorator[T]{
		grpcClientStream: grpcClientStream,
		grpcServerStream: grpcServerStream,
		ctx:              internalCtx,
		streamChannel:    nil,
		channelSize:      channelSize,
		terminationFunc:  cancelFunc,
		mapFunc:          mappingFunc,
		errorCallback:    errorCallback,
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
				err = w.errorCallback(err)
				if err != nil {
					return
				}
			}

			mappedMsg := w.mapFunc(msg)

			channel <- mappedMsg

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
