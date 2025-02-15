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

	_, err = clientConn.Write([]byte("SUB TEST"))
	if err != nil {
		t.Errorf("unexpected write to server error: %v", err)
	}
	clientConn.Close()

	err = s.Stop(ctx)
	if err != nil {
		t.Errorf("unexpected stop server error: %v", err)
	}
}
