package gstream

import (
	"context"
	"errors"
	"github.com/TrueGameover/gstream/src/internal/client"
	"github.com/TrueGameover/gstream/src/internal/client/receive"
	"github.com/TrueGameover/gstream/src/internal/stream"
	"google.golang.org/grpc"
	"time"
)

type FixedSizeObserverConfiguration struct {
	Ctx                      context.Context
	SubscribersChannelLength *int
	// SkipOnFail skip not delivered messages or retry delivery them unlimited
	SkipOnFail *bool
	// SkipPublishWithoutSubscribers skip element publishing without subscribers
	SkipPublishWithoutSubscribers *bool
	// ElementsCheckInterval sleep time between checking of elements existence
	ElementsCheckInterval *time.Duration
	// SubscribersCheckInterval sleep time between checking of subscribers
	SubscribersCheckInterval *time.Duration
}

//goland:noinspection GoUnusedExportedFunction
func NewFixedSizeObserver[T interface{}](config FixedSizeObserverConfiguration) (*stream.FixedSizeObserver[T], error) {
	size := 100
	if config.SubscribersChannelLength != nil {
		size = *config.SubscribersChannelLength
	}

	if size < 1 {
		return nil, errors.New("size should be greater than zero")
	}

	skipElements := false
	if config.SkipOnFail != nil {
		skipElements = *config.SkipOnFail
	}

	elementsWait := time.Millisecond * 10
	if config.ElementsCheckInterval != nil {
		elementsWait = *config.ElementsCheckInterval
	}

	subscribersWait := time.Second * 1
	if config.SubscribersCheckInterval != nil {
		subscribersWait = *config.SubscribersCheckInterval
	}

	skipWithoutSubscribers := false
	if config.SkipPublishWithoutSubscribers != nil {
		skipWithoutSubscribers = *config.SkipPublishWithoutSubscribers
	}

	return stream.NewFixedSizeObserver[T](
		config.Ctx,
		size,
		elementsWait,
		subscribersWait,
		skipElements,
		skipWithoutSubscribers,
	), nil
}

type GrpcStreamDecoratorConfiguration struct {
	Ctx          context.Context
	ServerStream *grpc.ServerStream
	ClientStream *grpc.ClientStream
	ChannelSize  *int
}

//goland:noinspection GoUnusedExportedFunction
func NewGrpcStreamDecorator[T interface{}](config GrpcStreamDecoratorConfiguration) (*receive.GrpcStreamDecorator[T], error) {
	size := 100
	if config.ChannelSize != nil {
		size = *config.ChannelSize
	}

	if size < 1 {
		return nil, errors.New("channel size should be greater than zero")
	}

	if config.ServerStream == nil && config.ClientStream == nil {
		return nil, errors.New("server or client stream expected")
	}

	var source receive.IMessageReceive
	if config.ServerStream != nil {
		source = *config.ServerStream
	}

	if config.ClientStream != nil {
		source = *config.ClientStream
	}

	return receive.NewGrpcStreamDecorator[T](
		config.Ctx,
		source,
		size,
	), nil
}

type GrpcClientConfiguration[T interface{}] struct {
	Ctx                           context.Context
	ServerStream                  grpc.ServerStream
	MessagesCallback              func(ctx context.Context, grpcClient *client.GrpcClient[T], msg *T) error
	ErrorsCallback                *func(grpcClient *client.GrpcClient[T], err error) error
	SkipMessagesIfClientWithoutId *bool
	MessagesChannelSize           *int
}

//goland:noinspection GoUnusedExportedFunction
func NewGrpcClient[T interface{}](config GrpcClientConfiguration[T]) (*client.GrpcClient[T], error) {
	errCallback := func(grpcClient *client.GrpcClient[T], err error) error {
		return err
	}
	if config.ErrorsCallback != nil {
		errCallback = *config.ErrorsCallback
	}

	skip := false
	if config.SkipMessagesIfClientWithoutId != nil {
		skip = *config.SkipMessagesIfClientWithoutId
	}

	size := 100
	if config.MessagesChannelSize != nil {
		size = *config.MessagesChannelSize
	}

	return client.NewGrpcClient(
		config.Ctx,
		config.ServerStream,
		config.MessagesCallback,
		errCallback,
		skip,
		size,
	), nil
}
