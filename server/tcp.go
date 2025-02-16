package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/vlaner/postal/broker"
)

type Client struct {
	conn  net.Conn
	msgCh chan broker.Message
}

type Broker interface {
	Publish(msg broker.Message)
	Register(req broker.SubscribeRequest)
	Remove(ch chan broker.Message)
	Ack(msgID string)
}

type TCPServer struct {
	Addr string
	ln   net.Listener

	broker  Broker
	clients sync.Map
	connWg  sync.WaitGroup
	quit    chan struct{}
}

func NewServer(addr string, broker Broker) (*TCPServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("server: listen on %s: %w", addr, err)
	}

	srv := &TCPServer{
		Addr:    addr,
		ln:      ln,
		broker:  broker,
		clients: sync.Map{},
		connWg:  sync.WaitGroup{},
		quit:    make(chan struct{}, 1),
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
			client := Client{msgCh: make(chan broker.Message, 1), conn: tcpConn}
			s.clients.Store(tcpConn, client)
			go s.handleClient(client)
		}
	}
}

func (s *TCPServer) handleClient(client Client) {
	defer func() {
		client.conn.Close()
		s.connWg.Done()
		s.clients.Delete(client.conn)
		s.broker.Remove(client.msgCh)
		close(client.msgCh)
	}()

	r := NewProtoReader(client.conn)
	w := NewProtoWriter(client.conn)

	// TODO: refactor
	go func() {
		for {
			select {
			case <-s.quit:
				return
			case msg := <-client.msgCh:
				err := w.Write(Proto{MessageID: msg.ID, Command: string(MESSAGE), Topic: msg.Topic, PayloadLen: len(msg.Payload), Data: msg.Payload})
				if err != nil {
					var opErr *net.OpError
					if errors.As(err, &opErr) && !opErr.Temporary() {
						return
					}
					log.Printf("server: write proto to client %v\n", err)
				}
			}
		}
	}()

	err := w.Write(Proto{Command: string(MESSAGE), Topic: "$WELCOME", PayloadLen: 0, Data: []byte("")})
	if err != nil {
		log.Printf("server: write welcome to client %v\n", err)
	}

	for {
		select {
		case <-s.quit:
			return
		default:
			proto, err := r.Parse()
			if err != nil {
				var opErr *net.OpError
				if errors.As(err, &opErr) && !opErr.Temporary() {
					return
				}
				if errors.Is(err, io.EOF) {
					return
				}

				log.Println("server: from read connection:", err)
				continue
			}

			log.Printf("server: received message %+v\n", proto)

			switch proto.Command {
			case string(SUBSCRIBE):
				s.broker.Register(broker.SubscribeRequest{Topic: proto.Topic, ConsumeCh: client.msgCh})
			case string(PUBLISH):
				msgID, err := generateMessageID()
				if err != nil {
					log.Printf("server: generate message ID %v\n", err)
					continue
				}

				s.broker.Publish(broker.NewMessage(msgID, proto.Topic, proto.Data))
			case string(UNSUBSCRIBE):
				s.broker.Remove(client.msgCh)
			case string(ACK):
				s.broker.Ack(proto.MessageID)
			}
		}
	}
}

func generateMessageID() (string, error) {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}
