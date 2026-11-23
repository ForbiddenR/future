package main

import "sync"

type Executor struct {
	mu     sync.Mutex
	cond   sync.Cond
	queue  []*Task
	closed bool
}

func NewExecutor() *Executor {
	e := &Executor{}
	e.cond = *sync.NewCond(&e.mu)
	return e
}

func (e *Executor) enqueue(t *Task) {
	e.mu.Lock()
	if !e.closed {
		e.queue = append(e.queue, t)
		e.cond.Signal()
	}
	e.mu.Unlock()
}

func (e *Executor) Spawn(f Future) {
	e.enqueue(&Task{future: f})
}

func (e *Executor) Stop() {
	e.mu.Lock()
	e.closed = true
	e.cond.Broadcast()
	e.mu.Unlock()
}

func (e *Executor) Run() {
	for {
		e.mu.Lock()
		for len(e.queue) == 0 && !e.closed {
			e.cond.Wait()
		}
		if len(e.queue) == 0 && e.closed {
			e.mu.Unlock()
			return
		}
		task := e.queue[0]
		e.queue = e.queue[1:]
		e.mu.Unlock()

		task.mu.Lock()
		if !task.done {
			waker := func() {
				e.enqueue(task)
			}
			if task.future.Poll(waker) == Ready {
				task.done = true
			}
		}
		task.mu.Unlock()
	}
}
