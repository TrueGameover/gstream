package client

import (
	"context"
	"errors"
	"github.com/TrueGameover/gstream/src/internal/client/receive"
	"github.com/TrueGameover/gstream/src/types"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"sync"
)

type GrpcClient[T interface{}] struct {
	grpcServerStream           grpc.ServerStream
	grpcClientStreamProvider   func() (grpc.ClientStream, error)
	messageReceived            func(ctx context.Context, grpcClient types.GrpcClient[T], msg *T) error
	errorHandler               func(grpcClient types.GrpcClient[T], err error) error
	cancelCtx                  context.CancelFunc
	clientContext              context.Context
	clientId                   *uuid.UUID
	skipMessagesWhileWithoutId bool
	messagesChannelSize        int
	perMessageAck              bool
}

func NewGrpcClient[T interface{}](
	ctx context.Context,
	grpcServerStream grpc.ServerStream,
	grpcClientStreamProvider func() (grpc.ClientStream, error),
	messageCallback func(ctx context.Context, grpcClient types.GrpcClient[T], msg *T) error,
	errorCallback func(grpcClient types.GrpcClient[T], err error) error,
	skipMessagesUntilClientIdNotSet bool,
	messagesChannelSize int,
	generateId bool,
	perMessageAck bool,
) *GrpcClient[T] {
	internalCtx, cancel := context.WithCancel(ctx)

	var id *uuid.UUID
	if generateId {
		d := uuid.New()
		id = &d
	}

	return &GrpcClient[T]{
		grpcServerStream:           grpcServerStream,
		grpcClientStreamProvider:   grpcClientStreamProvider,
		messageReceived:            messageCallback,
		errorHandler:               errorCallback,
		cancelCtx:                  cancel,
		clientContext:              internalCtx,
		clientId:                   id,
		skipMessagesWhileWithoutId: skipMessagesUntilClientIdNotSet,
		messagesChannelSize:        messagesChannelSize,
		perMessageAck:              perMessageAck,
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

	streamWrapper, err := receive.NewGrpcStreamDecorator[T, T](
		gc.clientContext,
		gc.messagesChannelSize,
		gc.perMessageAck,
		gc.grpcClientStreamProvider,
		gc.grpcServerStream,
		nil,
		func(err error) error {
			return gc.errorHandler(gc, err)
		},
	)
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

		streamCtx := streamWrapper.GetStreamContext()
		if streamCtx == nil {
			err := gc.errorHandler(gc, errors.New("stream context is nil"))
			if err != nil {
				return
			}
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
