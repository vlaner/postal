package broker

import "fmt"

type Message struct {
	ID      string
	Topic   string
	Payload any
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
	unacked   map[string]Message
	deliverCh chan struct{}

	quitCh chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		topics:   make(map[string]*Topic),
		msgsCh:   make(chan Message),
		register: make(chan SubscribeRequest),
		msgAckCh: make(chan string),
		unacked:  make(map[string]Message),

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
			fmt.Println("registered", sub)

			topic := b.getOrCreateTopic(sub.Topic)
			topic.consumers = append(topic.consumers, sub.ConsumeCh)
			b.topics[sub.Topic] = topic

			b.deliverSignal()

		case msg := <-b.msgsCh:
			fmt.Println("GOT MESSAGE", msg)

			topic := b.getOrCreateTopic(msg.Topic)
			topic.queue.Enqueue(msg)

			b.deliverSignal()

		case <-b.deliverCh:
			fmt.Println("delivering")
			b.deliverMessages()

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

func (b *Broker) deliverSignal() {
	select {
	case b.deliverCh <- struct{}{}:
	default:
		fmt.Println("deliver is full")
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
				b.unacked[message.ID] = message
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
