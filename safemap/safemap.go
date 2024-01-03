package safemap

import (
	"sync"
)

const (
	copyThreshold = 1000 //删的不到1000开始拷贝
	maxDeletion   = 10000
)

//如果删除的元素是值类型，如int，float，bool，string以及数组和struct，map的内存不会自动释放， struct改成*struct指针会回收
//如果删除的元素是引用类型，如指针，slice，map，chan等，map的内存会自动释放，但释放的内存是子元素应用类型的内存占用
//将map设置为nil后，内存被回收

// SafeMap provides a map alternative to avoid memory leak.
// This implementation is not needed until issue below fixed.
// https://github.com/golang/go/issues/20135
type SafeMap[K comparable, V any] struct {
	lock        sync.RWMutex
	deletionOld int
	deletionNew int
	dirtyOld    map[K]V
	dirtyNew    map[K]V
}

// NewSafeMap returns a SafeMap.
func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		dirtyOld: make(map[K]V),
		dirtyNew: make(map[K]V),
	}
}

// Del deletes the value with the given key from m.
func (m *SafeMap[K, V]) Del(key K) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.dirtyOld[key]; ok {
		delete(m.dirtyOld, key)
		m.deletionOld++
	} else if _, ok := m.dirtyNew[key]; ok {
		delete(m.dirtyNew, key)
		m.deletionNew++
	}
	if m.deletionOld >= maxDeletion && len(m.dirtyOld) < copyThreshold {
		for k, v := range m.dirtyOld {
			m.dirtyNew[k] = v
		}
		m.dirtyOld = m.dirtyNew
		m.deletionOld = m.deletionNew
		m.dirtyNew = make(map[K]V)
		m.deletionNew = 0
	}
	if m.deletionNew >= maxDeletion && len(m.dirtyNew) < copyThreshold {
		for k, v := range m.dirtyNew {
			m.dirtyOld[k] = v
		}
		m.dirtyNew = make(map[K]V)
		m.deletionNew = 0
	}
}

// Get gets the value with the given key from m.
func (m *SafeMap[K, V]) Get(key K) (V, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if val, ok := m.dirtyOld[key]; ok {
		return val, true
	}

	val, ok := m.dirtyNew[key]
	return val, ok
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (m *SafeMap[K, V]) Range(f func(key K, val V) bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for k, v := range m.dirtyOld {
		if !f(k, v) {
			return
		}
	}
	for k, v := range m.dirtyNew {
		if !f(k, v) {
			return
		}
	}
}

// Set sets the value into m with the given key.
func (m *SafeMap[K, V]) Set(key K, value V) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.deletionOld <= maxDeletion {
		if _, ok := m.dirtyNew[key]; ok {
			delete(m.dirtyNew, key)
			m.deletionNew++
		}
		m.dirtyOld[key] = value
	} else {
		if _, ok := m.dirtyOld[key]; ok {
			delete(m.dirtyOld, key)
			m.deletionOld++
		}
		m.dirtyNew[key] = value
	}
}

// Size returns the size of m.
func (m *SafeMap[K, V]) Size() int {
	m.lock.RLock()
	size := len(m.dirtyOld) + len(m.dirtyNew)
	m.lock.RUnlock()
	return size
}

func (m *SafeMap[K, V]) Keys() []K {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var keys []K
	for k := range m.dirtyOld {
		keys = append(keys, k)
	}
	for k := range m.dirtyNew {
		keys = append(keys, k)
	}
	return keys
}
