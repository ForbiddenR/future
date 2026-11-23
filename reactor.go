package main

import (
	"errors"
	"log"
	"sync"
	"syscall"
)

type fdWakers struct {
	readWaker  Waker
	writeWaker Waker
}

type IoReactor struct {
	epfd int
	mu   sync.Mutex
	fds  map[int]*fdWakers
}

func NewIoReactor() (*IoReactor, error) {
	epdf, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	r := &IoReactor{
		epfd: epdf,
		fds:  make(map[int]*fdWakers),
	}
	go r.eventLoop()
	return r, nil
}

func (r *IoReactor) Register(fd int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.fds[fd] = &fdWakers{}
	ev := &syscall.EpollEvent{
		Events: syscall.EPOLLONESHOT,
		Fd:     int32(fd),
	}
	return syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_ADD, fd, ev)
}

func (r *IoReactor) Unregister(fd int) {
	r.mu.Lock()
	delete(r.fds, fd)
	_ = syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
	r.mu.Unlock()
}

func (r *IoReactor) WaitReadable(fd int, waker Waker) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	st, ok := r.fds[fd]
	if !ok {
		return syscall.EBADF
	}
	st.readWaker = waker
	return r.armLockd(fd, st)
}

func (r *IoReactor) WaitWritable(fd int, waker Waker) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	st, ok := r.fds[fd]
	if !ok {
		return syscall.EBADF
	}
	st.writeWaker = waker
	return r.armLockd(fd, st)
}

func (r *IoReactor) armLockd(fd int, st *fdWakers) error {
	var events uint32 = syscall.EPOLLONESHOT
	if st.readWaker != nil {
		events |= syscall.EPOLLIN
	}
	if st.writeWaker != nil {
		events |= syscall.EPOLLOUT
	}
	ev := &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}
	return syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_MOD, fd, ev)
}

func (r *IoReactor) eventLoop() {
	events := make([]syscall.EpollEvent, 128)
	for {
		n, err := syscall.EpollWait(r.epfd, events, -1)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			log.Printf("EpollWait quit: %v", err)
			return
		}

		var toWake []Waker
		r.mu.Lock()
		for i := range n {
			fd := int(events[i].Fd)
			mask := events[i].Events

			st, ok := r.fds[fd]
			if !ok {
				continue
			}

			isErr := mask&(syscall.EPOLLERR|syscall.EPOLLHUP) != 0
			if (mask&syscall.EPOLLIN != 0 || isErr) && st.readWaker != nil {
				toWake = append(toWake, st.readWaker)
				st.readWaker = nil
			}
			if (mask&syscall.EPOLLOUT != 0 || isErr) && st.writeWaker != nil {
				toWake = append(toWake, st.writeWaker)
				st.writeWaker = nil
			}

			if st.writeWaker != nil || st.readWaker != nil {
				_ = r.armLockd(fd, st)
			}
		}
		r.mu.Unlock()

		for _, wake := range toWake {
			wake()
		}
	}
}
