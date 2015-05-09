package hbase

import (
	"sync"
)

type atomicCounter struct {
	n    int
	lock *sync.RWMutex
}

func newAtomicCounter() *atomicCounter {
	return &atomicCounter{
		n:    0,
		lock: &sync.RWMutex{},
	}
}

func (a *atomicCounter) Get() int {
	a.lock.RLock()
	v := a.n
	a.lock.RUnlock()
	return v
}

func (a *atomicCounter) IncrAndGet() int {
	a.lock.Lock()
	a.n++
	v := a.n
	a.lock.Unlock()
	return v
}
