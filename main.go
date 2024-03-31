package main

import (
	"crypto/sha1"
	"fmt"
	"sync"
)

type Shard struct {
	sync.RWMutex
	m map[string]any
}

type ShardedMap []*Shard

func NewShardedMap(nshards int) ShardedMap {
	shards := make([]*Shard, nshards)
	for i := 0; i < nshards; i++ {
		shard := make(map[string]interface{})
		shards[i] = &Shard{m: shard}
	}
	return shards
}

func (m ShardedMap) getShardIndex(key string) int {
	checksum := sha1.Sum([]byte(key))
	hash := int(checksum[17])
	return hash % len(m)
}

func (m ShardedMap) getShard(key string) *Shard {
	index := m.getShardIndex(key)
	return m[index]
}

func (m ShardedMap) Get(key string) any {
	shard := m.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.m[key]
}

func (m ShardedMap) Set(key string, value interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.m[key] = value
}

func (m ShardedMap) Keys() []string {
	keys := make([]string, 0)
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(m))
	for _, shard := range m {
		go func(s *Shard) {
			s.RLock()
			for key := range s.m {
				mutex.Lock()
				keys = append(keys, key)
				mutex.Unlock()
			}
			s.RUnlock()
			wg.Done()
		}(shard)
	}
	wg.Wait()
	return keys
}

func main() {
	shardedMap := NewShardedMap(10)
	shardedMap.Set("first_name", "Augustine")
	shardedMap.Set("last_name", "Smith")
	fmt.Println(shardedMap.Get("first_name"))
}
