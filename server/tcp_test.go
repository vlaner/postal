package server

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestSimpleServer(t *testing.T) {
	port := ":9090"
	s, err := NewServer(port)
	if err != nil {
		t.Errorf("unexpected new server error: %v", err)
	}

	s.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := net.Dial("tcp", "127.0.0.1"+port)
	if err != nil {
		t.Errorf("unexpected dial error: %v", err)
	}

	_, err = clientConn.Write([]byte("SUB TEST\r\n"))
	if err != nil {
		t.Errorf("unexpected write to server error: %v", err)
	}
	buf := make([]byte, 128)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Errorf("unexpected read from server error: %v", err)
	}
	t.Logf("GOT FROM SERVER %s", buf[:n])
	clientConn.Close()

	err = s.Stop(ctx)
	if err != nil {
		t.Errorf("unexpected stop server error: %v", err)
	}
}
