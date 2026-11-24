package main

import (
	"sync"
	"sync/atomic"
)

type Future[T any] interface {
	Poll(waker Waker) Poll[T]
}

type task struct {
	mu     sync.Mutex
	poll   func(waker Waker) bool
	queued bool
	done   bool
}

type Executor struct {
	mu    sync.Mutex
	cond  sync.Cond
	queue []*task
}

func NewExecutor() *Executor {
	e := &Executor{}
	e.cond = *sync.NewCond(&e.mu)
	return e
}

func (e *Executor) enqueue(t *task) {
	e.mu.Lock()
	if !t.queued {
		t.queued = true
		e.queue = append(e.queue, t)
		e.cond.Signal()
	}
	e.mu.Unlock()
}

func BlockOn[T any](e *Executor, root Future[T]) T {
	var (
		result T
		done   atomic.Bool
	)
	t := &task{
		poll: func(waker Waker) bool {
			res := root.Poll(waker)
			if !res.IsReady() {
				return false
			}
			result = res.Value
			done.Store(true)
			return true
		},
	}
	e.enqueue(t)

	for !done.Load() {
		e.mu.Lock()
		for len(e.queue) == 0 && !done.Load() {
			e.cond.Wait()
		}
		if done.Load() {
			e.mu.Unlock()
			break
		}
		curr := e.queue[0]
		e.queue = e.queue[1:]
		curr.queued = false
		e.mu.Unlock()

		curr.mu.Lock()
		if !curr.done {
			waker := func() {
				e.enqueue(curr)
			}
			if curr.poll(waker) {
				curr.done = true
			}
		}
		curr.mu.Unlock()
	}
	return result
}
