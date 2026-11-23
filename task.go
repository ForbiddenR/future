package main

import (
	"errors"
	"fmt"
	"log"
	"syscall"
)

type ListenerTask struct {
	listenFd int
	reactor  *IoReactor
	exec     *Executor
}

func (l *ListenerTask) Stop() {
	l.reactor.Unregister(l.listenFd)
	_ = syscall.Close(l.listenFd)
}

func (l *ListenerTask) Poll(waker Waker) PollStatus {
	for {
		nfd, sa, err := syscall.Accept(l.listenFd)
		if err != nil {
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.ECONNABORTED) {
				continue
			}

			if isWouldBlock(err) {
				if err := l.reactor.WaitReadable(l.listenFd, waker); err != nil {
					log.Printf("Listener WaitReadable failed: %v", err)
					l.Stop()
					return Ready
				}
				return Pending
			}
			log.Printf("Accept failed: %v", err)
			l.Stop()
			return Ready
		}

		if err := syscall.SetNonblock(nfd, true); err != nil {
			_ = syscall.Close(nfd)
			continue
		}
		if err := l.reactor.Register(nfd); err != nil {
			_ = syscall.Close(nfd)
			continue
		}

		addr := formatAddr(sa)
		log.Printf("[fd=%d] new connection: %s", nfd, addr)
		l.exec.Spawn(&EchoTask{
			fd:      nfd,
			addr:    addr,
			reactor: l.reactor,
			state:   echoStateReading,
		})
	}
}

const (
	echoStateReading = iota
	echoStateWriting
)

type EchoTask struct {
	fd      int
	addr    string
	reactor *IoReactor
	state   int
	buf     [4096]byte
	readLen int
	written int
}

func (t *EchoTask) Poll(waker Waker) PollStatus {
	for {
		switch t.state {
		case echoStateReading:
			n, err := syscall.Read(t.fd, t.buf[:])
			if err != nil {
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if isWouldBlock(err) {
					if err := t.reactor.WaitReadable(t.fd, waker); err != nil {
						t.close()
						return Ready
					}
					return Pending
				}
				log.Printf("[fd=%d] Read error: %v", t.fd, err)
				t.close()
				return Ready
			}
			if n == 0 {
				// EOF
				t.close()
				return Ready
			}
			t.readLen = n
			t.written = 0
			t.state = echoStateWriting
		case echoStateWriting:
			n, err := syscall.Write(t.fd, t.buf[t.written:t.readLen])
			if err != nil {
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if isWouldBlock(err) {
					if err := t.reactor.WaitWritable(t.fd, waker); err != nil {
						t.close()
						return Ready
					}
					return Pending
				}
				log.Printf("[fd=%d] Write error: %v", t.fd, err)
				t.close()
				return Ready
			}
			if n == 0 {
				t.close()
				return Ready
			}
			t.written += n
			if t.written == t.readLen {
				t.state = echoStateReading
			}
		}
	}
}

func (t *EchoTask) close() {
	log.Printf("[fd=%d] connection closed: %s", t.fd, t.addr)
	t.reactor.Unregister(t.fd)
	_ = syscall.Close(t.fd)
}

func isWouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}

func formatAddr(sa syscall.Sockaddr) string {
	if inet4, ok := sa.(*syscall.SockaddrInet4); ok {
		return fmt.Sprintf("%d.%d.%d.%d:%d", inet4.Addr[0], inet4.Addr[1], inet4.Addr[2], inet4.Addr[3], inet4.Port)
	}
	return "unknown"
}

func createNonBlockListener(port int) (int, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}

	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: port}); err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}
	if err := syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}
