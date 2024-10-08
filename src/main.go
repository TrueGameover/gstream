package gstream

import (
	"context"
	"errors"
	"github.com/TrueGameover/gstream/src/internal/client"
	"github.com/TrueGameover/gstream/src/internal/client/receive"
	"github.com/TrueGameover/gstream/src/internal/observer"
	"github.com/TrueGameover/gstream/src/internal/stream"
	"github.com/TrueGameover/gstream/src/types"
	"google.golang.org/grpc"
	"time"
)

type FixedSizeObserverConfiguration struct {
	Ctx                      context.Context
	SubscribersChannelLength *int
	// SkipOnFail skip not delivered messages or retry delivery them unlimited
	SkipOnFail bool
	// SkipPublishWithoutSubscribers skip element publishing without subscribers
	SkipPublishWithoutSubscribers bool
	// ElementsCheckInterval sleep time between checking of elements existence
	ElementsCheckInterval *time.Duration
	// SubscribersCheckInterval sleep time between checking of subscribers
	SubscribersCheckInterval *time.Duration
	// SkipAfterDeliveryRetriesCount drop messages after retries count
	SkipAfterDeliveryRetriesCount *int
}

//goland:noinspection GoUnusedExportedFunction
func NewFixedSizeObserver[T interface{}](config FixedSizeObserverConfiguration) (types.FixedSizeObserver[T], error) {
	size := 100
	if config.SubscribersChannelLength != nil {
		size = *config.SubscribersChannelLength
	}

	if size < 0 {
		return nil, errors.New("size should be greater or equal to zero")
	}

	elementsWait := time.Millisecond * 10
	if config.ElementsCheckInterval != nil {
		elementsWait = *config.ElementsCheckInterval
	}

	subscribersWait := time.Second * 1
	if config.SubscribersCheckInterval != nil {
		subscribersWait = *config.SubscribersCheckInterval
	}

	deliveryRetries := 1000
	if config.SkipAfterDeliveryRetriesCount != nil {
		deliveryRetries = *config.SkipAfterDeliveryRetriesCount
	}

	o := stream.NewFixedSizeObserver[T](
		config.Ctx,
		size,
		elementsWait,
		subscribersWait,
		config.SkipOnFail,
		config.SkipPublishWithoutSubscribers,
		false,
		deliveryRetries,
	)

	return o, nil
}

type GrpcStreamDecoratorConfiguration[I interface{}, O interface{}] struct {
	Ctx                  context.Context
	ServerStream         grpc.ServerStream
	ClientStreamProvider func() (grpc.ClientStream, error)
	ChannelSize          *int
	MappingFunc          func(msg *I) O
	ErrorCallback        func(err error) error
	PerMessageAck        bool
}

//goland:noinspection GoUnusedExportedFunction
func NewGrpcStreamDecorator[I interface{}, O interface{}](config GrpcStreamDecoratorConfiguration[I, O]) (types.GrpcStreamDecorator[I, O], error) {
	size := 100
	if config.ChannelSize != nil {
		size = *config.ChannelSize
	}

	if size < 0 {
		return nil, errors.New("channel size should be greater or equal to zero")
	}

	streamDec, err := receive.NewGrpcStreamDecorator[I, O](
		config.Ctx,
		size,
		config.PerMessageAck,
		config.ClientStreamProvider,
		config.ServerStream,
		config.MappingFunc,
		config.ErrorCallback,
	)
	if err != nil {
		return nil, err
	}

	return streamDec, nil
}

type GrpcClientConfiguration[T interface{}] struct {
	Ctx                           context.Context
	ServerStream                  grpc.ServerStream
	ClientStreamProvider          func() (grpc.ClientStream, error)
	MessagesCallback              func(ctx context.Context, grpcClient types.GrpcClient[T], msg *T) error
	ErrorsCallback                func(grpcClient types.GrpcClient[T], err error) error
	SkipMessagesIfClientWithoutId bool
	MessagesChannelSize           *int
	GenerateId                    bool
	PerMessageAck                 bool
}

//goland:noinspection GoUnusedExportedFunction
func NewGrpcClient[T interface{}](config GrpcClientConfiguration[T]) (types.GrpcClient[T], error) {
	errCallback := func(grpcClient types.GrpcClient[T], err error) error {
		return err
	}
	if config.ErrorsCallback != nil {
		errCallback = config.ErrorsCallback
	}

	size := 100
	if config.MessagesChannelSize != nil {
		size = *config.MessagesChannelSize
	}

	cl := client.NewGrpcClient(
		config.Ctx,
		config.ServerStream,
		config.ClientStreamProvider,
		config.MessagesCallback,
		errCallback,
		config.SkipMessagesIfClientWithoutId,
		size,
		config.GenerateId,
		config.PerMessageAck,
	)

	return cl, nil
}

type ObserverConfiguration struct {
	WaitingRepeatInterval time.Duration
}

//goland:noinspection GoUnusedExportedFunction
func NewObserver[T interface{}](config ObserverConfiguration) (types.Observer[T], error) {
	return observer.NewObserverImpl[T](config.WaitingRepeatInterval), nil
}
