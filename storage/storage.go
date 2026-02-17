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
	data map[types.Key][]types.Value //an array of values for each key to support multiple versions, i.e vector clocks within the context
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
	return values, exists
}

func (s *Storage) Put(key types.Key, val types.Value) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//check if the key already exists
	existingValues, exists := s.data[key]

	//if it doesn't exist, create a new entry
	if !exists {
		s.data[key] = []types.Value{val}
		return
	}

	//if it exists, check if the node ID matches and update the version
	for i, existingValue := range existingValues {
		if existingValue.Context.NodeID == val.Context.NodeID {
			val.Context.Version = existingValue.Context.Version + 1
			s.data[key][i] = val
			return
		}
	}

	//if the node ID doesn't match, append the new value to the existing values
	s.data[key] = append(existingValues, val)
}
