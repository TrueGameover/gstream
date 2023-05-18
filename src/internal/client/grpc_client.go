package client

import (
	"context"
	"github.com/TrueGameover/gstream/src/internal/client/receive"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"sync"
)

type GrpcClient[T interface{}] struct {
	grpcServerStream           grpc.ServerStream
	grpcClientStream           grpc.ClientStream
	messageReceived            func(ctx context.Context, grpcClient *GrpcClient[T], msg *T) error
	errorHandler               func(grpcClient *GrpcClient[T], err error) error
	cancelCtx                  context.CancelFunc
	clientContext              context.Context
	clientId                   *uuid.UUID
	skipMessagesWhileWithoutId bool
	messagesChannelSize        int
}

func NewGrpcClient[T interface{}](
	ctx context.Context,
	grpcServerStream grpc.ServerStream,
	grpcClientStream grpc.ClientStream,
	messageCallback func(ctx context.Context, grpcClient *GrpcClient[T], msg *T) error,
	errorCallback func(grpcClient *GrpcClient[T], err error) error,
	skipMessagesUntilClientIdNotSet bool,
	messagesChannelSize int,
) *GrpcClient[T] {
	internalCtx, cancel := context.WithCancel(ctx)

	return &GrpcClient[T]{
		grpcServerStream:           grpcServerStream,
		grpcClientStream:           grpcClientStream,
		messageReceived:            messageCallback,
		errorHandler:               errorCallback,
		cancelCtx:                  cancel,
		clientContext:              internalCtx,
		clientId:                   nil,
		skipMessagesWhileWithoutId: skipMessagesUntilClientIdNotSet,
		messagesChannelSize:        messagesChannelSize,
	}
}

func (gc *GrpcClient[T]) GetId() uuid.UUID {
	return *gc.clientId
}

func (gc *GrpcClient[T]) SetId(id uuid.UUID) {
	gc.clientId = &id
}

func (gc *GrpcClient[T]) HasId() bool {
	return gc.clientId != nil
}

func (gc *GrpcClient[T]) Stop() {
	gc.cancelCtx()
}

func (gc *GrpcClient[T]) GetContext() context.Context {
	return gc.clientContext
}

func (gc *GrpcClient[T]) Listen() error {
	waitGroup := sync.WaitGroup{}

	streamWrapper, err := receive.NewGrpcStreamDecorator[T](gc.clientContext, gc.messagesChannelSize, gc.grpcClientStream, gc.grpcServerStream)
	if err != nil {
		return err
	}

	messagesChannel, err := streamWrapper.Fetch()
	if err != nil {
		return err
	}

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		var streamCtx context.Context

		if gc.grpcServerStream != nil {
			streamCtx = gc.grpcServerStream.Context()
		} else {
			streamCtx = gc.grpcClientStream.Context()
		}

		for {
			select {
			case <-gc.clientContext.Done():
				return
			case <-streamCtx.Done():
				return
			case msg, ok := <-messagesChannel:
				if !ok {
					return
				}

				if gc.skipMessagesWhileWithoutId && gc.clientId == nil {
					break
				}

				err = gc.messageReceived(gc.clientContext, gc, &msg)
				if err != nil {
					err = gc.errorHandler(gc, err)
					if err != nil {
						return
					}
				}
			}

			select {
			case <-gc.clientContext.Done():
				return
			case <-streamCtx.Done():
				return
			default:
			}
		}
	}()

	waitGroup.Wait()
	return err
}
