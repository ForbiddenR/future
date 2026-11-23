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
