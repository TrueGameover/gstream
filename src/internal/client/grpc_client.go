package client

import (
	"context"
	"github.com/TrueGameover/gstream/src/internal/client/receive"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"sync"
)

type GrpcClient[T interface{}] struct {
	grpcStream                 grpc.ServerStream
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
	stream grpc.ServerStream,
	messageCallback func(ctx context.Context, grpcClient *GrpcClient[T], msg *T) error,
	errorCallback func(grpcClient *GrpcClient[T], err error) error,
	skipMessagesUntilClientIdNotSet bool,
	messagesChannelSize int,

) *GrpcClient[T] {
	internalCtx, cancel := context.WithCancel(ctx)

	return &GrpcClient[T]{
		grpcStream:                 stream,
		messageReceived:            messageCallback,
		cancelCtx:                  cancel,
		clientContext:              internalCtx,
		errorHandler:               errorCallback,
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

	streamWrapper := receive.NewGrpcStreamDecorator[T](gc.clientContext, gc.grpcStream, gc.messagesChannelSize)
	messagesChannel, err := streamWrapper.Fetch()
	if err != nil {
		return err
	}

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		grpcStreamCtx := gc.grpcStream.Context()

		for {
			select {
			case <-gc.clientContext.Done():
				return
			case <-grpcStreamCtx.Done():
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
			case <-grpcStreamCtx.Done():
				return
			default:
			}
		}
	}()

	waitGroup.Wait()
	return err
}
