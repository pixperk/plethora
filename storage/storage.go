package storage

import (
	"sync"

	"github.com/pixperk/plethora/types"
)

type Store interface {
	Get(key types.Key) ([]types.Value, bool)
	Put(key types.Key, val types.Value)
}

type Storage struct {
	lock sync.RWMutex
	data map[types.Key][]types.Value
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[types.Key][]types.Value),
	}
}

func (s *Storage) Get(key types.Key) ([]types.Value, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	values, exists := s.data[key]
	valuesCopy := make([]types.Value, len(values))
	copy(valuesCopy, values)
	// return a copy to prevent caller from mutating internal state
	return valuesCopy, exists
}

func (s *Storage) Put(key types.Key, val types.Value) {
	s.lock.Lock()
	defer s.lock.Unlock()

	existing, exists := s.data[key]
	if !exists {
		s.data[key] = []types.Value{val}
		return
	}

	// walk existing values, compare clocks
	var kept []types.Value
	for _, ev := range existing {
		// if an existing value's clock descends from (or equals) the new one,
		// the new write is stale, ignore it entirely
		if ev.Clock.Descends(val.Clock) {
			return
		}
		// if the new value's clock descends from an existing one,
		// the existing one is an ancestor, drop it
		if val.Clock.Descends(ev.Clock) {
			continue
		}
		// otherwise they conflict, keep the existing sibling
		kept = append(kept, ev)
	}

	// add the new value alongside any surviving siblings
	s.data[key] = append(kept, val)
}

// returns a list of all keys in the store.
func (s *Storage) Keys() []types.Key {
	s.lock.RLock()
	defer s.lock.RUnlock()
	keys := make([]types.Key, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
