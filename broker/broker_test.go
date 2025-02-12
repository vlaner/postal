package broker_test

import (
	"testing"

	"github.com/vlaner/postal/broker"
)

func TestSimple(t *testing.T) {
	b := broker.NewBroker()
	go b.Run()

	topic := "test"
	subCh := make(chan broker.Message, 1)
	cons := broker.SubscribeRequest{
		Topic:     topic,
		ConsumeCh: subCh,
	}

	b.Register(cons)

	payload := "testpayload"
	msg := broker.Message{
		ID:      "test",
		Topic:   topic,
		Payload: payload,
	}

	sendWait := make(chan struct{})
	go func() {
		<-sendWait
		t.Log("about to publish")
		b.Publish(msg)
		t.Log("published")
	}()

	sendWait <- struct{}{}
	got := <-cons.ConsumeCh
	t.Logf("GOT %+v", got)
	if got.Payload != payload {
		t.Errorf("got wrong payload: expected %+v got %+v", payload, got.Payload)
	}

	b.Stop()
}
