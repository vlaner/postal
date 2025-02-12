package broker

import (
	"container/list"
)

type Queue struct {
	list *list.List
}

func NewQueue() *Queue {
	return &Queue{list: list.New()}
}

func (q Queue) Enqueue(data any) {
	q.list.PushBack(data)
}

func (q Queue) Dequeue() (any, bool) {
	if q.list.Len() == 0 {
		return nil, false
	}

	val := q.list.Front()
	q.list.Remove(val)

	return val.Value, true
}

func (q Queue) Len() int {
	return q.list.Len()
}

func (q Queue) Empty() bool {
	return q.Len() == 0
}
