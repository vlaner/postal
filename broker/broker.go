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
	Consumers []chan Message
}

type SubscribeRequest struct {
	Topic     string
	ConsumeCh chan Message
}

type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex

	register  chan SubscribeRequest
	remove    chan chan Message
	msgsCh    chan Message
	msgAckCh  chan string
	msgNackCh chan string
	unacked   map[string]Message
	deliverCh chan struct{}

	quitCh chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		topics:    make(map[string]*Topic),
		msgsCh:    make(chan Message),
		register:  make(chan SubscribeRequest),
		remove:    make(chan chan Message),
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
			b.newRegister(sub)

		case subCh := <-b.remove:
			b.mu.Lock()
			for _, topic := range b.topics {
				for i, consumerCh := range topic.Consumers {
					if consumerCh == subCh {
						topic.Consumers = append(topic.Consumers[:i], topic.Consumers[i+1:]...)
						break
					}
				}
			}
			b.mu.Unlock()

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

func (b *Broker) Remove(subCh chan Message) {
	b.remove <- subCh
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	msgs := make([]Message, len(b.unacked))

	i := 0
	for _, msg := range b.unacked {
		msgs[i] = msg
		i++
	}

	return msgs
}

func (b *Broker) Topics() []Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]Topic, len(b.topics))

	i := 0
	for _, topic := range b.topics {
		topics[i] = *topic
		i++
	}

	return topics
}

func (b *Broker) ack(msgID string) {
	b.mu.Lock()
	delete(b.unacked, msgID)
	b.mu.Unlock()
}

func (b *Broker) nack(msgID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg, ok := b.unacked[msgID]
	if !ok {
		return
	}

	delete(b.unacked, msgID)

	msgTopic := b.getOrCreateTopic(msg.Topic)
	msgTopic.queue.Enqueue(msg)

	b.deliverSignal()
}

func (b *Broker) newRegister(sub SubscribeRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.getOrCreateTopic(sub.Topic)
	topic.Consumers = append(topic.Consumers, sub.ConsumeCh)
	b.topics[sub.Topic] = topic

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
		if len(t.Consumers) == 0 {
			return
		}

		for !t.queue.Empty() {
			msg, hasMessage := t.queue.Dequeue()
			if !hasMessage {
				break
			}

			message := msg.(Message)
			for _, c := range t.Consumers {
				select {
				case c <- message:
				default:
					// TODO: requeue for many consumers
				}

				// TODO: ack for many consumers
				b.mu.Lock()
				b.unacked[message.ID] = message
				b.mu.Unlock()
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
			Consumers: make([]chan Message, 0),
		}
	}

	return topic
}

func (b *Broker) Stop() {
	b.quitCh <- struct{}{}
}
