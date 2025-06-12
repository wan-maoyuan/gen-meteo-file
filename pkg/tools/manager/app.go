package manager

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

type App struct {
	name   string
	opts   options
	ctx    context.Context
	cancel context.CancelFunc
}

func New(name string, opts ...Option) *App {
	o := options{
		ctx:         context.Background(),
		sigs:        []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT},
		stopTimeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(&o)
	}

	ctx, cancel := context.WithCancel(o.ctx)

	return &App{
		name:   name,
		ctx:    ctx,
		cancel: cancel,
		opts:   o,
	}
}

func (a *App) Run() (err error) {
	eg, ctx := errgroup.WithContext(a.ctx)
	wg := sync.WaitGroup{}

	for _, fn := range a.opts.beforeStart {
		if err = fn(a.ctx); err != nil {
			return err
		}
	}

	for _, srv := range a.opts.servers {
		srv := srv
		eg.Go(func() error {
			<-ctx.Done()
			stopCtx, cancel := context.WithTimeout(a.opts.ctx, a.opts.stopTimeout)
			defer cancel()
			return srv.Stop(stopCtx)
		})
		wg.Add(1)
		eg.Go(func() error {
			wg.Done()
			return srv.Start(a.ctx)
		})
	}
	wg.Wait()

	for _, fn := range a.opts.afterStart {
		if err = fn(a.ctx); err != nil {
			return err
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, a.opts.sigs...)
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case <-c:
			return a.Stop()
		}
	})
	if err = eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	err = nil
	for _, fn := range a.opts.afterStop {
		err = fn(a.ctx)
	}
	return err
}

func (a *App) Stop() (err error) {
	for _, fn := range a.opts.beforeStop {
		err = fn(a.ctx)
	}

	if a.cancel != nil {
		a.cancel()
	}

	return err
}
