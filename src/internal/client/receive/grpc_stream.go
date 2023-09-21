package receive

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"sync"
)

type GrpcStreamDecorator[I interface{}, O interface{}] struct {
	grpcClientStreamProvider func() (grpc.ClientStream, error)
	grpcServerStream         grpc.ServerStream
	ctx                      context.Context
	streamChannel            *chan O
	streamCtx                context.Context
	channelSize              int
	terminationFunc          context.CancelFunc
	mapFunc                  func(msg *I) O
	errorCallback            func(err error) error
}

type recvMessage interface {
	RecvMsg(m interface{}) error
}

func NewGrpcStreamDecorator[I interface{}, O interface{}](
	ctx context.Context,
	channelSize int,
	grpcClientStreamProvider func() (grpc.ClientStream, error),
	grpcServerStream grpc.ServerStream,
	mappingFunc func(msg *I) O,
	errorCallback func(err error) error,
) (*GrpcStreamDecorator[I, O], error) {
	if grpcClientStreamProvider == nil && grpcServerStream == nil {
		return nil, errors.New("client or server stream expected")
	}

	internalCtx, cancelFunc := context.WithCancel(ctx)

	if errorCallback == nil {
		errorCallback = func(err error) error {
			return err
		}
	}

	return &GrpcStreamDecorator[I, O]{
		grpcClientStreamProvider: grpcClientStreamProvider,
		grpcServerStream:         grpcServerStream,
		ctx:                      internalCtx,
		streamChannel:            nil,
		channelSize:              channelSize,
		terminationFunc:          cancelFunc,
		mapFunc:                  mappingFunc,
		errorCallback:            errorCallback,
	}, nil
}

func (w *GrpcStreamDecorator[T, O]) Fetch() (<-chan O, error) {
	if w.streamChannel != nil {
		return nil, errors.New("stream already has listener")
	}

	channel := make(chan O, w.channelSize)
	w.streamChannel = &channel

	wGroup := sync.WaitGroup{}
	wGroup.Add(1)
	go func() {
		defer close(channel)

		var recv recvMessage

		if w.grpcServerStream != nil {
			recv = w.grpcServerStream
			w.streamCtx = w.grpcServerStream.Context()

		} else {
			clientStream, err := w.grpcClientStreamProvider()
			if err != nil {
				_ = w.errorCallback(err)
				wGroup.Done()
				return
			}

			w.streamCtx = clientStream.Context()
			recv = clientStream
		}

		wGroup.Done()

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

				channel <- w.mapFunc(&msg)

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

	// wait stream init
	wGroup.Wait()

	return channel, nil
}

func (w *GrpcStreamDecorator[I, O]) Release() {
	w.terminationFunc()
}

func (w *GrpcStreamDecorator[I, O]) GetStreamContext() context.Context {
	return w.streamCtx
}
