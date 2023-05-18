package gstream

import (
	"context"
	"errors"
	"github.com/TrueGameover/gstream/src/internal/stream"
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
