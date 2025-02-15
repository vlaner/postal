package broker

import (
	"sync"
)

type Message struct {
	ID      string
	Topic   string
	Payload []byte
}

type Topic struct {
	name      string
	queue     *Queue
	consumers []chan Message
}

type SubscribeRequest struct {
	Topic     string
	ConsumeCh chan Message
}

type Broker struct {
	topics map[string]*Topic

	register chan SubscribeRequest

	msgsCh    chan Message
	msgAckCh  chan string
	msgNackCh chan string
	unackedMu sync.RWMutex
	unacked   map[string]Message
	deliverCh chan struct{}

	quitCh chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		topics:    make(map[string]*Topic),
		msgsCh:    make(chan Message),
		register:  make(chan SubscribeRequest),
		msgAckCh:  make(chan string),
		msgNackCh: make(chan string),
		unacked:   make(map[string]Message),

		// deliver channel size of 1 because we have single goroutine to handle channel
		deliverCh: make(chan struct{}, 1),
		quitCh:    make(chan struct{}),
	}

	return b
}

func (b *Broker) Run() {
	for {
		select {
		case sub := <-b.register:
			topic := b.getOrCreateTopic(sub.Topic)
			topic.consumers = append(topic.consumers, sub.ConsumeCh)
			b.topics[sub.Topic] = topic

			b.deliverSignal()

		case msg := <-b.msgsCh:
			topic := b.getOrCreateTopic(msg.Topic)
			topic.queue.Enqueue(msg)

			b.unacked[msg.ID] = msg

			b.deliverSignal()

		case <-b.deliverCh:
			b.deliverMessages()

		case msgID := <-b.msgAckCh:
			b.ack(msgID)

		case msgID := <-b.msgNackCh:
			b.nack(msgID)

		case <-b.quitCh:
			return
		}
	}
}

func (b *Broker) Register(req SubscribeRequest) {
	b.register <- req
}

func (b *Broker) Publish(msg Message) {
	b.msgsCh <- msg
}

func (b *Broker) Ack(msgID string) {
	b.msgAckCh <- msgID
}

func (b *Broker) Nack(msgID string) {
	b.msgNackCh <- msgID
}

func (b *Broker) Unacked() []Message {
	b.unackedMu.Lock()
	defer b.unackedMu.Unlock()

	msgs := make([]Message, len(b.unacked))

	i := 0
	for _, msg := range b.unacked {
		msgs[i] = msg
		i++
	}

	return msgs
}

func (b *Broker) ack(msgID string) {
	b.unackedMu.Lock()
	delete(b.unacked, msgID)
	b.unackedMu.Unlock()
}

func (b *Broker) nack(msgID string) {
	b.unackedMu.Lock()
	defer b.unackedMu.Unlock()

	msg, ok := b.unacked[msgID]
	if !ok {
		return
	}

	delete(b.unacked, msgID)

	msgTopic := b.getOrCreateTopic(msg.Topic)
	msgTopic.queue.Enqueue(msg)

	b.deliverSignal()
}

func (b *Broker) deliverSignal() {
	select {
	case b.deliverCh <- struct{}{}:
	default:
	}
}

func (b *Broker) deliverMessages() {
	for _, t := range b.topics {
		if len(t.consumers) == 0 {
			return
		}

		for !t.queue.Empty() {
			msg, hasMessage := t.queue.Dequeue()
			if !hasMessage {
				break
			}

			message := msg.(Message)
			for _, c := range t.consumers {
				select {
				case c <- message:
				default:
					// TODO: requeue for many consumers
				}

				// TODO: ack for many consumers
				b.unackedMu.Lock()
				b.unacked[message.ID] = message
				b.unackedMu.Unlock()
			}
		}
	}
}

func (b *Broker) getOrCreateTopic(name string) *Topic {
	topic, exists := b.topics[name]
	if !exists {
		topic = &Topic{
			name:      name,
			queue:     NewQueue(),
			consumers: make([]chan Message, 0),
		}
	}

	return topic
}

func (b *Broker) Stop() {
	b.quitCh <- struct{}{}
}
