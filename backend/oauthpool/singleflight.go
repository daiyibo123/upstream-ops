package oauthpool

import "sync"

// flightGroup is the small subset of singleflight needed by the two pool
// snapshots. Keeping it local avoids adding a module dependency to the parent
// project for two low-cardinality keys.
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	done  chan struct{}
	value any
	err   error
}

func (g *flightGroup) Do(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*flightCall)
	}
	if current := g.calls[key]; current != nil {
		g.mu.Unlock()
		<-current.done
		return current.value, current.err, true
	}
	current := &flightCall{done: make(chan struct{})}
	g.calls[key] = current
	g.mu.Unlock()

	current.value, current.err = fn()
	close(current.done)
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return current.value, current.err, false
}
