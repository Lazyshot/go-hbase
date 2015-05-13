package hbase

import (
	pb "github.com/golang/protobuf/proto"

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

func incrementByteString(d []byte, i int) []byte {
	r := make([]byte, len(d))
	copy(r, d)
	if i <= 0 {
		return append(make([]byte, 1), r...)
	}
	r[i]++
	return r
}

func merge(cs ...chan pb.Message) chan pb.Message {
	var wg sync.WaitGroup
	out := make(chan pb.Message)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan pb.Message) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
