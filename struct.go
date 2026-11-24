package main

type Waker = func()

type Poll[T any] struct {
	Value T
	Ready bool
}

func (p *Poll[T]) IsReady() bool {
	return p.Ready
}

func Pending[T any]() Poll[T] {
	return Poll[T]{Ready: false}
}

func Ready[T any](v T) Poll[T] {
	return Poll[T]{Value: v, Ready: true}
}

type Cancellable interface {
	Cancel()
}

func tryCancel(f any) {
	if c, ok := f.(Cancellable); ok {
		c.Cancel()
	}
}
