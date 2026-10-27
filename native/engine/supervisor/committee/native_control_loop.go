package committee

import (
	"sync"
	"sync/atomic"
	"time"
)

// Supervisor owns this loop and must join it before closing its resources.
// Ticks consume external evidence at the last observed epoch, not epoch+1.
func (r *RelayCommitteeModule) StartNativeControl() func() {
	stop, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if r.groupControl != nil {
					if err := r.groupControl.poll(); err != nil {
						panic(err)
					}
				}
				if r.groupControl != nil {
					if err := r.pollGroupCompletions(); err != nil {
						panic(err)
					}
				}
				r.admission.pollNativeControlAtObservedEpoch(int(atomic.LoadInt64(&r.observedEpoch)))
			}
		}
	}()
	return func() { once.Do(func() { close(stop) }); <-done }
}
