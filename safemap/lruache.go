package safemap

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	list     *list.List
	mutex    sync.Mutex
}

type entry struct {
	key   string
	value interface{}
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (lru *LRUCache) Get(key string) (interface{}, bool) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	if elem, ok := lru.cache[key]; ok {
		lru.list.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}

	return nil, false
}

func (lru *LRUCache) Put(key string, value interface{}) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	if elem, ok := lru.cache[key]; ok {
		lru.list.MoveToFront(elem)
		elem.Value.(*entry).value = value
		return
	}

	if len(lru.cache) >= lru.capacity {
		oldest := lru.list.Back()
		if oldest != nil {
			delete(lru.cache, oldest.Value.(*entry).key)
			lru.list.Remove(oldest)
		}
	}

	elem := lru.list.PushFront(&entry{key, value})
	lru.cache[key] = elem
}

func (lru *LRUCache) Delete(key string) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	if elem, ok := lru.cache[key]; ok {
		delete(lru.cache, key)
		lru.list.Remove(elem)
	}
}

func (lru *LRUCache) Traverse() {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	for elem := lru.list.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*entry)
		println(entry.key, entry.value)
	}
}
