package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Pair[A, B any] struct {
	First  A
	Second B
}

type Join2Future[A, B any] struct {
	f1    Future[A]
	f2    Future[B]
	res1  A
	res2  B
	done1 bool
	done2 bool
}

func Join2[A, B any](f1 Future[A], f2 Future[B]) *Join2Future[A, B] {
	return &Join2Future[A, B]{f1: f1, f2: f2}
}

func (j *Join2Future[A, B]) Poll(waker Waker) Poll[Pair[A, B]] {
	if !j.done1 {
		if p := j.f1.Poll(waker); p.IsReady() {
			j.res1 = p.Value
			j.done1 = true
		}
	}

	if !j.done2 {
		if p := j.f2.Poll(waker); p.IsReady() {
			j.res2 = p.Value
			j.done2 = true
		}
	}

	if j.done1 && j.done2 {
		return Ready(Pair[A, B]{j.res1, j.res2})
	}
	return Pending[Pair[A, B]]()
}

func (j *Join2Future[A, B]) Cancel() {
	if !j.done1 {
		tryCancel(j.f1)
	}
	if !j.done2 {
		tryCancel(j.f2)
	}
}

type JoinAllFuture[T any] struct {
	futures   []Future[T]
	results   []T
	done      []bool
	remaining int
}

func JoinAll[T any](fs ...Future[T]) *JoinAllFuture[T] {
	return &JoinAllFuture[T]{
		futures:   fs,
		results:   make([]T, len(fs)),
		done:      make([]bool, len(fs)),
		remaining: len(fs),
	}
}

func (j *JoinAllFuture[T]) Poll(waker Waker) Poll[[]T] {
	for i, f := range j.futures {
		if !j.done[i] {
			if p := f.Poll(waker); p.IsReady() {
				j.results[i] = p.Value
				j.done[i] = true
				j.remaining--
			}
		}
	}
	if j.remaining == 0 {
		return Ready(j.results)
	}
	return Pending[[]T]()
}

func (j *JoinAllFuture[T]) Cancel() {
	for i, f := range j.futures {
		if !j.done[i] {
			tryCancel(f)
		}
	}
}

type Either[A, B any] struct {
	Index  int
	First  A
	Second B
}

type Select2Future[A, B any] struct {
	f1   Future[A]
	f2   Future[B]
	done bool
}

func Select2[A, B any](f1 Future[A], f2 Future[B]) *Select2Future[A, B] {
	return &Select2Future[A, B]{f1: f1, f2: f2}
}

func (s *Select2Future[A, B]) Poll(waker Waker) Poll[Either[A, B]] {
	pollFirst := func() (Poll[Either[A, B]], bool) {
		if p := s.f1.Poll(waker); p.IsReady() {
			s.done = true
			tryCancel(s.f2)
			return Ready(Either[A, B]{Index: 0, First: p.Value}), true
		}
		return Pending[Either[A, B]](), false
	}
	pollSecond := func() (Poll[Either[A, B]], bool) {
		if p := s.f2.Poll(waker); p.IsReady() {
			s.done = true
			tryCancel(s.f1)
			return Ready(Either[A, B]{Index: 1, Second: p.Value}), true
		}
		return Pending[Either[A, B]](), false
	}

	if rand.IntN(2) == 0 {
		if res, ok := pollFirst(); ok {
			return res
		}
		if res, ok := pollSecond(); ok {
			return res
		}
	} else {
		if res, ok := pollSecond(); ok {
			return res
		}
		if res, ok := pollFirst(); ok {
			return res
		}
	}
	return Pending[Either[A, B]]()
}

func (s *Select2Future[A, B]) Cancel() {
	if !s.done {
		s.done = true
		tryCancel(s.f1)
		tryCancel(s.f2)
	}
}

type DelayValue[T any] struct {
	name      string
	delay     time.Duration
	val       T
	mu        sync.Mutex
	timer     *time.Timer
	ready     bool
	cancelled bool
}

func NewDelayValue[T any](name string, val T, delay time.Duration) *DelayValue[T] {
	return &DelayValue[T]{name: name, val: val, delay: delay}
}

func (d *DelayValue[T]) Poll(waker Waker) Poll[T] {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ready {
		return Ready(d.val)
	}
	if d.cancelled {
		return Pending[T]()
	}
	if d.timer == nil {
		d.timer = time.AfterFunc(d.delay, func() {
			d.mu.Lock()
			if d.cancelled {
				d.mu.Unlock()
				return
			}
			d.ready = true
			d.mu.Unlock()
			waker()
		})
	}
	return Pending[T]()
}

func (d *DelayValue[T]) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.cancelled || d.ready {
		return
	}
	d.cancelled = true
	if d.timer != nil && d.timer.Stop() {
		fmt.Printf("[Cancel] %s is not ready, the timer is released\n", d.name)
	}
}
