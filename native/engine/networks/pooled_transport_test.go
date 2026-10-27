package networks

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// These tests deliberately do not run in parallel: upload tokens are shared.
func pooledTestSetup(t *testing.T) {
	t.Helper()
	old := rateLimiterUpload
	CloseAllConnInPool()
	rateLimiterUpload = rate.NewLimiter(rate.Inf, 1<<20)
	t.Cleanup(func() {
		CloseAllConnInPool()
		rateLimiterUpload = old
	})
}

// The pinned x/time v0.6.0 stores its zero-rate one-shot allowance in Burst,
// not Tokens. This fixture checks accounting without modeling a refill rate.
func pooledTestBudget(burst int) *rate.Limiter {
	return rate.NewLimiter(0, burst)
}

func pooledTestPut(addr string, conn net.Conn) {
	connMaplock.Lock()
	connectionPool[addr] = conn
	connMaplock.Unlock()
}

func pooledTestGet(addr string) net.Conn {
	connMaplock.Lock()
	defer connMaplock.Unlock()
	return connectionPool[addr]
}

func pooledTestNoDial(_, _ string) (net.Conn, error) {
	return nil, errors.New("unexpected dial for pooled connection")
}

func pooledTestSend(payload []byte, addr string, observe func(string, time.Time, error), dial func(string, string) (net.Conn, error)) <-chan error {
	done := make(chan error, 1)
	go func() { done <- sendPooledFrame(payload, addr, observe, dial) }()
	return done
}

func pooledTestWait(t *testing.T, ready <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for", what)
	}
}

func pooledTestResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("transport did not return")
		return nil
	}
}

func pooledTestRelease(t *testing.T, gate chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release
}

type pooledTestConn struct {
	net.Conn
	mu          sync.Mutex
	writes      [][]byte
	closes      int
	write       func([]byte) (int, error)
	deadlines   []time.Time
	deadlineErr error
}

func (c *pooledTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.mu.Unlock()
	if c.write != nil {
		return c.write(p)
	}
	return len(p), nil
}

func (c *pooledTestConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

func (c *pooledTestConn) SetWriteDeadline(at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, at)
	return c.deadlineErr
}

func (c *pooledTestConn) snapshot() ([][]byte, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.writes...), c.closes
}

type pooledTestStartedConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *pooledTestStartedConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(p)
}

func TestPooledTransportSlowPeerDoesNotBlockAnotherPeer(t *testing.T) {
	pooledTestSetup(t)
	slow, unread := net.Pipe()
	t.Cleanup(func() { _ = unread.Close() })
	started := make(chan struct{})
	slowAddr, fastAddr := t.Name()+"/slow", t.Name()+"/fast"
	pooledTestPut(slowAddr, &pooledTestStartedConn{Conn: slow, started: started})
	fast := &pooledTestConn{}
	pooledTestPut(fastAddr, fast)
	slowDone := pooledTestSend([]byte("slow\n"), slowAddr, nil, pooledTestNoDial)
	pooledTestWait(t, started, "blocked pipe write")
	if err := pooledTestResult(t, pooledTestSend([]byte("fast\n"), fastAddr, nil, pooledTestNoDial)); err != nil {
		t.Fatal("another peer could not progress:", err)
	}
	if writes, _ := fast.snapshot(); len(writes) != 1 || string(writes[0]) != "fast\n" {
		t.Fatal("fast frame was not preserved", writes)
	}
	_ = unread.Close()
	if err := pooledTestResult(t, slowDone); err == nil {
		t.Fatal("closed pipe write unexpectedly succeeded")
	}
}

func TestPooledTransportBlockedObserverDoesNotBlockAnotherPeer(t *testing.T) {
	pooledTestSetup(t)
	addr, other := t.Name()+"/observed", t.Name()+"/other"
	pooledTestPut(addr, &pooledTestConn{})
	pooledTestPut(other, &pooledTestConn{})
	entered, gate := make(chan struct{}), make(chan struct{})
	release := pooledTestRelease(t, gate)
	observe := func(event string, _ time.Time, _ error) {
		if event == "first_socket_write" {
			close(entered)
			<-gate
		}
	}
	done := pooledTestSend([]byte("observed\n"), addr, observe, pooledTestNoDial)
	pooledTestWait(t, entered, "blocked observer")
	if err := pooledTestResult(t, pooledTestSend([]byte("other\n"), other, nil, pooledTestNoDial)); err != nil {
		t.Fatal("another peer waited for observer:", err)
	}
	release()
	if err := pooledTestResult(t, done); err != nil {
		t.Fatal(err)
	}
}

// Split each underlying frame into several socket writes so an unprotected
// whole-frame send can interleave. This does not assert goroutine call FIFO.
type pooledTestChunkConn struct{ net.Conn }

func (c *pooledTestChunkConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > 2 {
			n = 2
		}
		written, err := c.Conn.Write(p[:n])
		total += written
		if err != nil {
			return total, err
		}
		if written != n {
			return total, io.ErrShortWrite
		}
		p = p[n:]
	}
	return total, nil
}

func TestPooledTransportWholeFramesAndCallbackOrderPerPeer(t *testing.T) {
	pooledTestSetup(t)
	addr := t.Name()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	_ = b.SetReadDeadline(time.Now().Add(5 * time.Second))
	pooledTestPut(addr, &pooledTestChunkConn{Conn: a})
	const count = 12
	start := make(chan struct{})
	done := make(chan error, count)
	var mu sync.Mutex
	var callbacks []string
	for i := 0; i < count; i++ {
		frame := fmt.Sprintf("frame-%02d", i)
		go func(frame string) {
			<-start
			done <- sendPooledFrame([]byte(frame+"\n"), addr, func(event string, _ time.Time, err error) {
				mu.Lock()
				defer mu.Unlock()
				callbacks = append(callbacks, frame+":"+event)
				if err != nil {
					t.Error("unexpected observer error:", err)
				}
			}, pooledTestNoDial)
		}(frame)
	}
	close(start)
	reader := bufio.NewScanner(b)
	var received []string
	seen := map[string]bool{}
	for i := 0; i < count; i++ {
		if !reader.Scan() {
			t.Fatal("missing frame:", reader.Err())
		}
		frame := reader.Text()
		if len(frame) != len("frame-00") || seen[frame] {
			t.Fatal("interleaved or duplicate frame:", frame)
		}
		seen[frame] = true
		received = append(received, frame)
	}
	for i := 0; i < count; i++ {
		if err := pooledTestResult(t, done); err != nil {
			t.Fatal(err)
		}
		if !seen[fmt.Sprintf("frame-%02d", i)] {
			t.Fatal("missing frame identity", i)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(callbacks) != count {
		t.Fatal("callback count", callbacks)
	}
	for i, frame := range received {
		if callbacks[i] != frame+":first_socket_write" {
			t.Fatal("callback order differs from frame order", received, callbacks)
		}
	}
}

func TestPooledTransportPartialAndShortWritesAreNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "partial-error", err: errors.New("partial socket failure")},
		{name: "short-nil-error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pooledTestSetup(t)
			rateLimiterUpload = pooledTestBudget(32)
			addr, payload := t.Name(), []byte("failed-frame\n")
			bad := &pooledTestConn{write: func([]byte) (int, error) { return 3, tc.err }}
			dials := 0
			var events []string
			var eventErrors []error
			observe := func(event string, _ time.Time, err error) {
				events = append(events, event)
				eventErrors = append(eventErrors, err)
			}
			err := pooledTestResult(t, pooledTestSend(payload, addr, observe, func(_, _ string) (net.Conn, error) {
				dials++
				return bad, nil
			}))
			want := tc.err
			if want == nil {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) || dials != 1 {
				t.Fatal("error changed or automatic redial occurred", err, dials)
			}
			if len(events) != 2 || events[0] != "first_socket_write" || events[1] != "write_error" || eventErrors[0] != nil || !errors.Is(eventErrors[1], want) {
				t.Fatal("first/error callbacks must occur once in order", events, eventErrors)
			}
			writes, closes := bad.snapshot()
			if len(writes) != 1 || !bytes.Equal(writes[0], payload) || closes != 1 || pooledTestGet(addr) != nil {
				t.Fatal("failed connection replayed, retained or not closed", writes, closes)
			}
			if got := rateLimiterUpload.Burst(); got != 32-len(payload) {
				t.Fatal("partial write did not charge the whole frame", got)
			}
			fresh := &pooledTestConn{}
			if err := pooledTestResult(t, pooledTestSend([]byte("next\n"), addr, nil, func(_, _ string) (net.Conn, error) {
				dials++
				return fresh, nil
			})); err != nil {
				t.Fatal("distinct later frame could not reconnect:", err)
			}
			if writes, _ := fresh.snapshot(); dials != 2 || len(writes) != 1 || string(writes[0]) != "next\n" {
				t.Fatal("failed frame replayed on fresh connection", dials, writes)
			}
		})
	}
	t.Run("deadline-reset-error", func(t *testing.T) {
		pooledTestSetup(t)
		addr := t.Name()
		want := errors.New("deadline reset failed")
		conn := &pooledTestConn{deadlineErr: want}
		pooledTestPut(addr, conn)
		var events []string
		var observed error
		err := pooledTestResult(t, pooledTestSend([]byte("never-written\n"), addr, func(event string, _ time.Time, err error) {
			events = append(events, event)
			observed = err
		}, pooledTestNoDial))
		if !errors.Is(err, want) || len(events) != 1 || events[0] != "write_error" || !errors.Is(observed, want) {
			t.Fatal("deadline failure was not reported once", err, events, observed)
		}
		if writes, closes := conn.snapshot(); len(writes) != 0 || closes != 1 || pooledTestGet(addr) != nil {
			t.Fatal("deadline failure wrote or retained connection", writes, closes)
		}
		if len(conn.deadlines) != 1 || !conn.deadlines[0].IsZero() {
			t.Fatal("ordinary transport did not clear prior deadline", conn.deadlines)
		}
	})
}

func TestPooledTransportCloseDuringDialRejectsOldGeneration(t *testing.T) {
	pooledTestSetup(t)
	addr := t.Name()
	lane := peerSendLane(addr)
	entered, gate := make(chan struct{}), make(chan struct{})
	release := pooledTestRelease(t, gate)
	conn := &pooledTestConn{}
	var events []string
	var observed error
	done := pooledTestSend([]byte("stale\n"), addr, func(event string, _ time.Time, err error) {
		events = append(events, event)
		observed = err
	}, func(_, _ string) (net.Conn, error) {
		close(entered)
		<-gate
		return conn, nil
	})
	pooledTestWait(t, entered, "dial boundary")
	closed := make(chan struct{})
	go func() { CloseAllConnInPool(); close(closed) }()
	pooledTestWait(t, closed, "pool close during dial")
	if peerSendLane(addr) != lane {
		t.Fatal("pool close replaced the live peer lane")
	}
	release()
	if err := pooledTestResult(t, done); !errors.Is(err, net.ErrClosed) {
		t.Fatal("old dial generation was accepted", err)
	}
	if len(events) != 1 || events[0] != "connect_error" || !errors.Is(observed, net.ErrClosed) {
		t.Fatal("stale dial observer result", events, observed)
	}
	if writes, closes := conn.snapshot(); len(writes) != 0 || closes != 1 || pooledTestGet(addr) != nil {
		t.Fatal("stale dial was written, retained or not closed", writes, closes)
	}
	if peerSendLane(addr) != lane {
		t.Fatal("peer lane changed after stale dial returned")
	}
}

func TestPooledTransportCloseUnblocksWriteAndWaitingLane(t *testing.T) {
	pooledTestSetup(t)
	addr := t.Name()
	lane := peerSendLane(addr)
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	entered := make(chan struct{})
	pooledTestPut(addr, &pooledTestStartedConn{Conn: a, started: entered})
	first := pooledTestSend([]byte("blocked\n"), addr, nil, pooledTestNoDial)
	pooledTestWait(t, entered, "blocked first write")
	started := make(chan struct{})
	second := make(chan error, 1)
	fresh := &pooledTestConn{}
	go func() {
		close(started)
		second <- sendPooledFrame([]byte("waiting\n"), addr, nil, func(_, _ string) (net.Conn, error) { return fresh, nil })
	}()
	pooledTestWait(t, started, "second send")
	closed := make(chan struct{})
	go func() { CloseAllConnInPool(); close(closed) }()
	pooledTestWait(t, closed, "close while a writer owns the lane")
	if err := pooledTestResult(t, first); err == nil {
		t.Fatal("closed blocked write succeeded")
	}
	if err := pooledTestResult(t, second); err != nil {
		t.Fatal("waiting sender did not progress after close:", err)
	}
	if peerSendLane(addr) != lane {
		t.Fatal("close rebuilt the peer lane")
	}
	if writes, _ := fresh.snapshot(); len(writes) != 1 || string(writes[0]) != "waiting\n" {
		t.Fatal("waiting frame was not preserved", writes)
	}
}

func TestPooledTransportRetainsSharedWholeFrameUploadTokens(t *testing.T) {
	pooledTestSetup(t)
	limiter := pooledTestBudget(12)
	rateLimiterUpload = limiter
	for i, payload := range [][]byte{[]byte("one\n"), []byte("two\n")} {
		addr := fmt.Sprintf("%s/%d", t.Name(), i)
		pooledTestPut(addr, &pooledTestConn{})
		if err := pooledTestResult(t, pooledTestSend(payload, addr, nil, pooledTestNoDial)); err != nil {
			t.Fatal(err)
		}
	}
	if rateLimiterUpload != limiter || limiter.Burst() != 4 {
		t.Fatal("different peers did not consume the same upload budget", limiter.Burst())
	}
}
