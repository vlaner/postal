package broker

import (
	"sync"
)

type SyncMap[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

func NewSyncMap[T any]() *SyncMap[T] {
	return &SyncMap[T]{
		m: make(map[string]T),
	}
}

func (m *SyncMap[T]) Set(key string, val T) {
	m.mu.Lock()
	m.m[key] = val
	m.mu.Unlock()
}

func (m *SyncMap[T]) Get(key string) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.m[key]

	return val, ok
}

func (m *SyncMap[T]) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)
}
