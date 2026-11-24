package main

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrSemaphoreClosed = errors.New("semaphore closed")

type semWaiter struct {
	needed   int
	assigned int
	waker    Waker
	inQueue  bool
	granted  bool
	closed   bool
}

type Semaphore struct {
	mu      sync.Mutex
	permits int
	closed  bool
	waiters []*semWaiter
}

func NewSemaphore(permits int) *Semaphore {
	if permits < 0 {
		panic("semaphore permits cannot be negative")
	}
	return &Semaphore{permits: permits}
}

func (s *Semaphore) AvailablePermits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.permits
}

func (s *Semaphore) AddPermits(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	toWake := s.addPermitsLockd(n)
	s.mu.Unlock()

	for _, wake := range toWake {
		wake()
	}
}

func (s *Semaphore) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	toWake := make([]Waker, 0, len(s.waiters))
	for _, w := range s.waiters {
		w.inQueue = false
		w.closed = true
		w.assigned = 0
		if w.waker != nil {
			toWake = append(toWake, w.waker)
			w.waker = nil
		}
	}
	s.waiters = nil
	s.permits = 0
	s.mu.Unlock()

	for _, wake := range toWake {
		wake()
	}
}

func (s *Semaphore) addPermitsLockd(n int) []Waker {
	if n <= 0 || s.closed {
		return nil
	}
	var toWake []Waker
	for n > 0 && len(s.waiters) > 0 {
		head := s.waiters[0]
		rem := head.needed - head.assigned
		if n > rem {
			head.assigned = head.needed
			n -= rem
			s.waiters = s.waiters[1:]
			head.inQueue = false
			head.granted = true
			if head.waker != nil {
				toWake = append(toWake, head.waker)
				head.waker = nil
			}
		} else {
			head.assigned += n
			n = 0
		}
	}
	if n > 0 {
		s.permits += n
	}
	return toWake
}

func (s *Semaphore) removeWaiterLocked(target *semWaiter) {
	for i, w := range s.waiters {
		if w == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

func (s *Semaphore) TryAcquire(n int) (*SemaphorePermit, bool) {
	if n < 0 {
		panic("cannot acquire negative permits")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.waiters) > 0 || s.permits < n {
		return nil, false
	}
	s.permits -= n
	return &SemaphorePermit{sem: s, count: n}, true
}

type SemaphorePermit struct {
	sem      *Semaphore
	count    int
	released atomic.Bool
}

func (p *SemaphorePermit) Release() {
	if p == nil || p.count <= 0 {
		return
	}
	if p.released.CompareAndSwap(false, true) {
		p.sem.AddPermits(p.count)
	}
}

type AcquireFuture struct {
	sem       *Semaphore
	needed    int
	waiter    *semWaiter
	taken     bool
	cancelled bool
}

func (f *AcquireFuture) Poll(waker Waker) Poll[Result[*SemaphorePermit]] {
	f.sem.mu.Lock()
	if f.cancelled {
		f.sem.mu.Unlock()
		return Pending[Result[*SemaphorePermit]]()
	}
	if f.taken {
		f.sem.mu.Unlock()
		panic("AcquireFuture polled after completion")
	}

	if f.waiter == nil {
		if f.sem.closed {
			f.taken = true
			f.sem.mu.Unlock()
			return Ready(Result[*SemaphorePermit]{Err: ErrSemaphoreClosed})
		}

		if f.needed == 0 {
			f.taken = true
			f.sem.mu.Unlock()
			return Ready(Result[*SemaphorePermit]{Value: &SemaphorePermit{sem: f.sem, count: 0}})
		}
		if len(f.sem.waiters) == 0 && f.sem.permits >= f.needed {
			f.sem.permits -= f.needed
			f.taken = true
			f.sem.mu.Unlock()
			return Ready(Result[*SemaphorePermit]{Value: &SemaphorePermit{sem: f.sem, count: f.needed}})

		}

		w := &semWaiter{
			needed:  f.needed,
			waker:   waker,
			inQueue: true,
		}
		if len(f.sem.waiters) == 0 && f.sem.permits > 0 {
			w.assigned = f.sem.permits
			f.sem.permits = 0
		}
		f.sem.waiters = append(f.sem.waiters, w)
		f.waiter = w
		f.sem.mu.Unlock()
		return Pending[Result[*SemaphorePermit]]()
	}

	w := f.waiter
	if w.granted {
		f.taken = true
		f.waiter = nil
		n := w.needed
		f.sem.mu.Unlock()
		return Ready(Result[*SemaphorePermit]{Value: &SemaphorePermit{sem: f.sem, count: n}})
	}

	if w.closed {
		f.taken = true
		f.waiter = nil
		f.sem.mu.Unlock()
		return Ready(Result[*SemaphorePermit]{Err: ErrSemaphoreClosed})
	}

	w.waker = waker
	f.sem.mu.Unlock()
	return Ready(Result[*SemaphorePermit]{})
}

func (f *AcquireFuture) Cancel() {
	f.sem.mu.Lock()
	if f.taken || f.cancelled {
		f.sem.mu.Unlock()
		return
	}
	f.cancelled = true
	w := f.waiter
	f.waiter = nil
	if w == nil {
		f.sem.mu.Unlock()
		return
	}

	var reclaim int
	if w.inQueue {
		f.sem.removeWaiterLocked(w)
		w.inQueue = false
		reclaim = w.assigned
		w.assigned = 0
	} else {
		reclaim = w.needed
		w.granted = false
	}

	var toWake []Waker
	if reclaim > 0 {
		toWake = f.sem.addPermitsLockd(reclaim)
	}
	f.sem.mu.Unlock()

	for _, wake := range toWake {
		wake()
	}
}
