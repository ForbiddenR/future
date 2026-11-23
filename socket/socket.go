package socket

// import (
// 	"context"
// 	"fmt"
// 	"log"

// 	"golang.org/x/sys/unix"
// )

// const (
// 	socketFlags = unix.SOCK_NONBLOCK | unix.SOCK_CLOEXEC
// 	acceptBatch = 32
// )

// func watch(epfd, op, fd int, events uint32) error {
// 	event := unix.EpollEvent{Events: events, Fd: int32(fd)}
// 	return unix.EpollCtl(epfd, op, fd, &event)
// }

// func acceptReady(epfd, listener int, clients map[int][]byte) error {
// 	for range acceptBatch {
// 		fd, _, err := unix.Accept4(listener, socketFlags)
// 		switch err {
// 		case unix.EAGAIN:
// 			return nil
// 		case unix.EINTR, unix.ECONNABORTED,
// 			unix.ENETDOWN, unix.EPROTO, unix.ENOPROTOOPT,
// 			unix.EHOSTDOWN, unix.ENONET, unix.EHOSTUNREACH,
// 			unix.EOPNOTSUPP, unix.ENETUNREACH:
// 			continue
// 		case nil:
// 		default:
// 			return fmt.Errorf("accept4: %w", err)
// 		}

// 		if err := watch(epfd, unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN); err != nil {
// 			_ = unix.Close(fd)
// 			return fmt.Errorf("epoll add client: %w", err)
// 		}
// 		clients[fd] = nil
// 	}

// 	return nil
// }

// func serve(ctx context.Context) error {
// 	listener, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|socketFlags, 0)
// 	if err != nil {
// 		return fmt.Errorf("socket: %w", err)
// 	}
// 	defer unix.Close(listener)

// 	if err := unix.SetsockoptInt(listener, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
// 		return fmt.Errorf("setsocketopt: %w", err)
// 	}
// 	addr := &unix.SockaddrInet4{Port: 9000, Addr: [4]byte{127, 0, 0, 1}}
// 	if err := unix.Bind(listener, addr); err != nil {
// 		return fmt.Errorf("bind: %w", err)
// 	}
// 	if err := unix.Listen(listener, unix.SOMAXCONN); err != nil {
// 		return fmt.Errorf("listen: %w", err)
// 	}

// 	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
// 	if err != nil {
// 		return fmt.Errorf("epoll_create1: %w", err)
// 	}
// 	defer unix.Close(epfd)

// 	if err := watch(epfd, unix.EPOLL_CTL_ADD, listener, unix.EPOLLIN); err != nil {
// 		return fmt.Errorf("epoll add listener: %w", err)
// 	}

// 	clients := make(map[int][]byte)
// 	closeClient := func(fd int) {
// 		delete(clients, fd)
// 		_ = unix.EpollCtl(epfd, unix.EPOLL_CTL_DEL, fd, nil)
// 		_ = unix.Close(fd)
// 	}
// 	defer func() {
// 		for fd := range clients {
// 			closeClient(fd)
// 		}
// 	}()

// 	log.Print("listening on 127.0.0.1:9000")
// 	events := make([]unix.EpollEvent, 128)
// 	buf := make([]byte, 4096)

// 	for ctx.Err() == nil {
// 		n, err := unix.EpollWait(epfd, events, 500)
// 		if err == unix.EINTR {
// 			continue
// 		}
// 		if err != nil {
// 			return fmt.Errorf("epoll_wait: %w", err)
// 		}

// 		for _, event := range events[:n] {
// 			fd := int(event.Fd)
// 			if fd == listener {
// 				if event.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
// 					return fmt.Errorf("listener event: %#x", event.Events)
// 				}

// 			}
// 		}
// 	}

// }
