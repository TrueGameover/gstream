package receive

import (
	"context"
	"errors"
	"google.golang.org/grpc"
)

type GrpcStreamDecorator[I interface{}, O interface{}] struct {
	grpcClientStream grpc.ClientStream
	grpcServerStream grpc.ServerStream
	ctx              context.Context
	streamChannel    *chan O
	channelSize      int
	terminationFunc  context.CancelFunc
	mapFunc          func(msg I) O
	errorCallback    func(err error) error
}

type recvMessage interface {
	RecvMsg(m interface{}) error
}

func NewGrpcStreamDecorator[I interface{}, O interface{}](
	ctx context.Context,
	channelSize int,
	grpcClientStream grpc.ClientStream,
	grpcServerStream grpc.ServerStream,
	mappingFunc func(msg I) O,
	errorCallback func(err error) error,
) (*GrpcStreamDecorator[I, O], error) {
	if grpcClientStream == nil && grpcServerStream == nil {
		return nil, errors.New("client or server stream expected")
	}

	internalCtx, cancelFunc := context.WithCancel(ctx)

	if errorCallback == nil {
		errorCallback = func(err error) error {
			return err
		}
	}

	return &GrpcStreamDecorator[I, O]{
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

func (w *GrpcStreamDecorator[T, O]) Fetch() (<-chan O, error) {
	if w.streamChannel != nil {
		return nil, errors.New("stream already has listener")
	}

	channel := make(chan O, w.channelSize)
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
			if w.mapFunc != nil {
				var msg T
				err := recv.RecvMsg(&msg)

				if err != nil {
					err = w.errorCallback(err)
					if err != nil {
						return
					}
				}

				channel <- w.mapFunc(msg)

			} else {
				var msg O
				err := recv.RecvMsg(&msg)

				if err != nil {
					err = w.errorCallback(err)
					if err != nil {
						return
					}
				}

				channel <- msg
			}

			select {
			case <-w.ctx.Done():
				return
			default:
			}
		}
	}()

	return channel, nil
}

func (w *GrpcStreamDecorator[I, O]) Release() {
	w.terminationFunc()
}
