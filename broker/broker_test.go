package broker_test

import (
	"runtime"
	"testing"

	"github.com/vlaner/postal/broker"
)

type testPubSub struct {
	t  *testing.T
	ch chan broker.Message
	b  *broker.Broker
}

func newTestPubSub(t *testing.T, b *broker.Broker) *testPubSub {
	return &testPubSub{
		t:  t,
		ch: make(chan broker.Message, 1),
		b:  b,
	}
}

func (ps *testPubSub) subscribe(topic string) {
	ps.b.Register(broker.SubscribeRequest{
		Topic:     topic,
		ConsumeCh: ps.ch,
	})
}

func (ps *testPubSub) publish(msg broker.Message) {
	ps.b.Publish(msg)
}

func (ps *testPubSub) readMessage() broker.Message {
	return <-ps.ch
}

func TestSimple(t *testing.T) {
	b := broker.NewBroker()
	go b.Run()

	topic := "test"

	payload := "testpayload"
	msg := broker.Message{
		ID:      "test",
		Topic:   topic,
		Payload: payload,
	}

	tc := newTestPubSub(t, b)
	tc.subscribe(topic)
	tc.publish(msg)

	got := tc.readMessage()
	t.Logf("GOT %+v", got)
	if got.Payload != payload {
		t.Errorf("got wrong payload: expected %+v got %+v", payload, got.Payload)
	}

	b.Stop()
}

func TestMessageAck(t *testing.T) {
	b := broker.NewBroker()
	go b.Run()

	topic := "test"

	payload := "testpayload"
	msg := broker.Message{
		ID:      "test",
		Topic:   topic,
		Payload: payload,
	}

	tc := newTestPubSub(t, b)
	tc.subscribe(topic)
	tc.publish(msg)
	got := tc.readMessage()

	t.Logf("GOT %+v", got)
	if got.Payload != payload {
		t.Errorf("got wrong payload: expected %+v got %+v", payload, got.Payload)
	}

	unacked := b.Unacked()
	t.Log("unacked1", unacked)
	if len(unacked) == 0 {
		t.Error("unexpected unacked len of 0")
	}

	unackedMsg := unacked[0]
	if unackedMsg.ID != msg.ID {
		t.Errorf("unexpected unacked message ID %s but wanted ID %s", unackedMsg.ID, msg.ID)
	}

	b.Ack(msg.ID)
	// TODO: better way to sync
	runtime.Gosched()
	unacked = b.Unacked()
	t.Log("unacked2", unacked)
	if len(unacked) != 0 {
		t.Errorf("unexpected unacked len of %d", len(unacked))
	}

	b.Stop()
}

func TestMessageNack(t *testing.T) {
	b := broker.NewBroker()
	go b.Run()

	topic := "test"
	payload := "testpayload"
	msg := broker.Message{
		ID:      "test",
		Topic:   topic,
		Payload: payload,
	}

	tc := newTestPubSub(t, b)
	tc.subscribe(topic)
	tc.publish(msg)
	got := tc.readMessage()

	t.Logf("GOT %+v", got)
	if got.Payload != payload {
		t.Errorf("got wrong payload: expected %+v got %+v", payload, got.Payload)
	}

	unacked := b.Unacked()
	t.Log("unacked1", unacked)
	if len(unacked) == 0 {
		t.Error("unexpected unacked len of 0")
	}

	unackedMsg := unacked[0]
	if unackedMsg.ID != msg.ID {
		t.Errorf("unexpected unacked message ID %s but wanted ID %s", unackedMsg.ID, msg.ID)
	}

	b.Nack(msg.ID)
	t.Log("nacked", msg.ID)

	// TODO: better way to sync
	runtime.Gosched()

	unacked = b.Unacked()
	if len(unacked) == 0 {
		t.Error("unexpected unacked len of 0")
	}

	gotAfterNack := tc.readMessage()
	t.Logf("GOT AFTER NACK %+v", got)
	if got.Payload != payload {
		t.Errorf("got wrong payload after nack: expected %+v got %+v", payload, gotAfterNack.Payload)
	}

	b.Stop()
}
