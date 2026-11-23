package main

import (
	"log"
	"sync"
)

type PollStatus int

const (
	Pending PollStatus = iota
	Ready
)

type Waker = func()

type Future interface {
	Poll(Waker) PollStatus
}

type Task struct {
	mu     sync.Mutex
	future Future
	done   bool
}

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

func main() {
	const port = 18080
	listenFd, err := createNonBlockListener(port)
	if err != nil {
		log.Fatalf("failed to create listen socket: %v", err)
	}

	ioReactor, err := NewIoReactor()
	if err != nil {
		log.Fatalf("failed to create ioReactor: %v", err)
	}

	if err := ioReactor.Register(listenFd); err != nil {
		log.Fatalf("failed to register the listener fd: %v", err)
	}

	exec := NewExecutor()
	defer exec.Stop()

	listenerTask := &ListenerTask{
		listenFd: listenFd,
		reactor:  ioReactor,
		exec:     exec,
	}

	exec.Spawn(listenerTask)

	exec.Run()
}
