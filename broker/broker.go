package broker

import (
	"encoding/json"
	"log"
	"time"

	"github.com/vlaner/postal/schema"
)

type Message struct {
	ID          string
	Topic       string
	Payload     []byte
	SentAt      time.Time
	DeliveredAt time.Time
}

func NewMessage(id, topic string, payload []byte) Message {
	return Message{
		ID:      id,
		Topic:   topic,
		Payload: payload,
		SentAt:  time.Now(),
	}
}

type Topic struct {
	name      string
	queue     *Queue
	Consumers []chan Message
	schema    *schema.NodeSchema
}

type SubscribeRequest struct {
	Topic     string
	ConsumeCh chan Message
}

type Broker struct {
	topics *SyncMap[*Topic]

	register  chan SubscribeRequest
	remove    chan chan Message
	msgsCh    chan Message
	msgAckCh  chan string
	msgNackCh chan string
	unacked   *SyncMap[*Message]
	deliverCh chan struct{}

	quitCh chan struct{}

	unackedTickerDuration time.Duration
	unackedTimeout        time.Duration
}

func NewBroker() *Broker {
	b := &Broker{
		topics:    NewSyncMap[*Topic](),
		msgsCh:    make(chan Message),
		register:  make(chan SubscribeRequest),
		remove:    make(chan chan Message),
		msgAckCh:  make(chan string),
		msgNackCh: make(chan string),
		unacked:   NewSyncMap[*Message](),
		// deliver channel size of 1 because we have single goroutine to handle channel
		deliverCh:             make(chan struct{}, 1),
		quitCh:                make(chan struct{}),
		unackedTickerDuration: 3 * time.Second,
		unackedTimeout:        5 * time.Second,
	}

	return b
}

func (b *Broker) Run() {
	unackedTicker := time.NewTicker(b.unackedTickerDuration)
	defer unackedTicker.Stop()

	for {
		select {
		case sub := <-b.register:
			b.newRegister(sub)

		case subCh := <-b.remove:
			b.topics.mu.Lock()
			for _, topic := range b.topics.m {
				for i, consumerCh := range topic.Consumers {
					if consumerCh == subCh {
						topic.Consumers = append(topic.Consumers[:i], topic.Consumers[i+1:]...)
						break
					}
				}
			}
			b.topics.mu.Unlock()

		case msg := <-b.msgsCh:
			b.queueMessage(msg)

		case <-b.deliverCh:
			b.deliverMessages()

		case msgID := <-b.msgAckCh:
			b.ack(msgID)

		case msgID := <-b.msgNackCh:
			b.nack(msgID)

		case <-unackedTicker.C:
			b.checkUnacked()

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
	b.unacked.mu.RLock()
	defer b.unacked.mu.RUnlock()

	msgs := make([]Message, len(b.unacked.m))

	i := 0
	for _, msg := range b.unacked.m {
		msgs[i] = *msg
		i++
	}

	return msgs
}

func (b *Broker) Topics() []Topic {
	b.topics.mu.RLock()
	defer b.topics.mu.RUnlock()

	topics := make([]Topic, len(b.topics.m))
	i := 0
	for _, topic := range b.topics.m {
		topics[i] = *topic
		i++
	}

	return topics
}

func (b *Broker) SetSchema(topicName string, schema schema.NodeSchema) {
	if topic, exists := b.topics.Get(topicName); exists {
		topic.schema = &schema
	}
}

func (b *Broker) checkUnacked() {
	now := time.Now()
	for _, unackedMsg := range b.unacked.m {
		if unackedMsg.DeliveredAt.Add(b.unackedTimeout).After(now) {
			b.unacked.Delete(unackedMsg.ID)
			b.queueMessage(*unackedMsg)
		}
	}
}

func (b *Broker) queueMessage(msg Message) {
	topic := b.getOrCreateTopic(msg.Topic)
	topic.queue.Enqueue(msg)
	if topic.schema != nil {
		log.Println("PAYLOAD IS ", string(msg.Payload))

		var data map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			log.Println("UNMARSHAL SCHEMA PAYLOAD", err)
			return
		}

		log.Println("DATA IS ", data)

		err := schema.ValidateMap(*topic.schema, data)
		if err != nil {
			log.Println("ERROR VALIDATE BYTES", err)
			return
		}
	}

	b.unacked.Set(msg.ID, &msg)

	b.deliverSignal()
}

func (b *Broker) ack(msgID string) {
	b.unacked.Delete(msgID)
}

func (b *Broker) nack(msgID string) {
	msg, ok := b.unacked.Get(msgID)
	if !ok {
		return
	}
	b.unacked.Delete(msgID)

	msgTopic := b.getOrCreateTopic(msg.Topic)
	msgTopic.queue.Enqueue(*msg)

	b.deliverSignal()
}

func (b *Broker) newRegister(sub SubscribeRequest) {
	topic := b.getOrCreateTopic(sub.Topic)
	topic.Consumers = append(topic.Consumers, sub.ConsumeCh)
	b.topics.Set(sub.Topic, topic)

	b.deliverSignal()
}

func (b *Broker) deliverSignal() {
	select {
	case b.deliverCh <- struct{}{}:
	default:
	}
}

func (b *Broker) deliverMessages() {
	for _, t := range b.topics.m {
		if len(t.Consumers) == 0 {
			return
		}

		for !t.queue.Empty() {
			msg, hasMessage := t.queue.Dequeue()
			if !hasMessage {
				break
			}

			message := msg.(Message)
			message.DeliveredAt = time.Now()
			for _, c := range t.Consumers {
				select {
				case c <- message:
					// TODO: ack for many consumers
					b.unacked.Set(message.ID, &message)
				default:
					// TODO: requeue for many consumers
				}

			}
		}
	}
}

func (b *Broker) getOrCreateTopic(name string) *Topic {
	topic, exists := b.topics.Get(name)
	if !exists {
		topic = &Topic{
			name:      name,
			queue:     NewQueue(),
			Consumers: make([]chan Message, 0),
			schema:    nil,
		}
	}

	return topic
}

func (b *Broker) Stop() {
	b.quitCh <- struct{}{}
}
