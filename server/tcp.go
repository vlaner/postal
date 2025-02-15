package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type TCPServer struct {
	Addr string
	ln   net.Listener

	connWg sync.WaitGroup
	quit   chan struct{}
}

func NewServer(addr string) (*TCPServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("server: listen on %s: %w", addr, err)
	}

	srv := &TCPServer{
		Addr:   addr,
		ln:     ln,
		connWg: sync.WaitGroup{},
		quit:   make(chan struct{}, 1),
	}

	return srv, nil
}

func (s *TCPServer) Start() {
	go s.acceptLoop()
}

func (s *TCPServer) Stop(ctx context.Context) error {
	listenErr := s.ln.Close()

	s.quit <- struct{}{}

	waitCh := make(chan struct{})
	go func() {
		// TODO: close all existing connections, otherwise this blocks
		s.connWg.Wait()
		waitCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("server: stop server: %w", errors.Join(listenErr, ctx.Err()))
	case <-waitCh:
		if listenErr != nil {
			return fmt.Errorf("server: stop listener: %w", listenErr)
		}

		return nil
	}
}

func (s *TCPServer) acceptLoop() {
	for {
		select {
		case <-s.quit:
			return
		default:
			conn, err := s.ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
					return
				}

				log.Printf("server: accept connection: %v", err)
				continue
			}

			tcpConn := conn.(*net.TCPConn)
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Minute)

			s.connWg.Add(1)
			go s.handleConn(tcpConn)
		}
	}
}

func (s *TCPServer) handleConn(c net.Conn) {
	defer c.Close()
	defer s.connWg.Done()

	r := NewProtoReader(c)
	w := NewProtoWriter(c)

	for {
		select {
		case <-s.quit:
			return
		default:
			proto, err := r.Parse()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				log.Println("server: from read connection:", err)
				continue
			}

			log.Printf("server: received message %+v\n", proto)
			err = w.Write(Proto{Command: string(MESSAGE), Topic: "test", PayloadLen: 4, Data: []byte("TEST")})
			if err != nil {
				log.Printf("server: write to client %v\n", proto)
			}
		}
	}
}
