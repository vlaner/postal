package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vlaner/postal/broker"
	"github.com/vlaner/postal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	broker := broker.NewBroker()
	go broker.Run()

	srv, err := server.NewServer(":8080", broker)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	srv.Start()

	log.Println("server started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("received signal %v: shutting down", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return srv.Stop(ctx)
}
