package manager

import (
	"context"
	"os"
	"time"
)

type Option func(o *options)

type options struct {
	ctx  context.Context
	sigs []os.Signal

	stopTimeout time.Duration
	servers     []Server

	beforeStart []func(context.Context) error
	beforeStop  []func(context.Context) error
	afterStart  []func(context.Context) error
	afterStop   []func(context.Context) error
}

func Context(ctx context.Context) Option {
	return func(o *options) { o.ctx = ctx }
}

func AddServer(srv ...Server) Option {
	return func(o *options) { o.servers = srv }
}

func Signal(sigs ...os.Signal) Option {
	return func(o *options) { o.sigs = sigs }
}

func StopTimeout(t time.Duration) Option {
	return func(o *options) { o.stopTimeout = t }
}

func BeforeStart(fn func(context.Context) error) Option {
	return func(o *options) {
		o.beforeStart = append(o.beforeStart, fn)
	}
}

func BeforeStop(fn func(context.Context) error) Option {
	return func(o *options) {
		o.beforeStop = append(o.beforeStop, fn)
	}
}

func AfterStart(fn func(context.Context) error) Option {
	return func(o *options) {
		o.afterStart = append(o.afterStart, fn)
	}
}

func AfterStop(fn func(context.Context) error) Option {
	return func(o *options) {
		o.afterStop = append(o.afterStop, fn)
	}
}
