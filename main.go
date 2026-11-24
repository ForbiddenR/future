package main

import (
)

func main() {
	// const port = 18080
	// listenFd, err := createNonBlockListener(port)
	// if err != nil {
	// 	log.Fatalf("failed to create listen socket: %v", err)
	// }

	// ioReactor, err := NewIoReactor()
	// if err != nil {
	// 	log.Fatalf("failed to create ioReactor: %v", err)
	// }

	// if err := ioReactor.Register(listenFd); err != nil {
	// 	log.Fatalf("failed to register the listener fd: %v", err)
	// }

	// exec := NewExecutor()
	// defer exec.Stop()

	// listenerTask := &ListenerTask{
	// 	listenFd: listenFd,
	// 	reactor:  ioReactor,
	// 	exec:     exec,
	// }

	// exec.Spawn(listenerTask)

	// exec.Run()
}
