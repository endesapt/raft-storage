package main

import (
	"encoding/json"
	"log"
	"sync"
)

type KVStore struct {
	store    map[string]string
	mutex    *sync.Mutex
	proposeC chan<- string
}
type kv struct {
	Key   string
	Value string
}

func NewKVStore(proposeC chan<- string, commitC <-chan string) *KVStore {
	s := &KVStore{
		store:    make(map[string]string),
		mutex:    &sync.Mutex{},
		proposeC: proposeC,
	}
	go s.readCommits(commitC)
	return s
}
func (s *KVStore) Get(key string) (string, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	value, exists := s.store[key]
	return value, exists
}
func (s *KVStore) Propose(key, value string) {
	encoded, err := json.Marshal(kv{Key: key, Value: value})
	if err != nil {
		log.Fatal(err)
	}
	s.proposeC <- string(encoded)
}
func (s *KVStore) readCommits(commitC <-chan string) {
	for c := range commitC {
		var k kv
		if err := json.Unmarshal([]byte(c), &k); err != nil {
			log.Fatal(err)
		}
		s.mutex.Lock()
		s.store[k.Key] = k.Value
		s.mutex.Unlock()
	}
}
