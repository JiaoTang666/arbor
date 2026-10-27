package networks

import (
	"io"
	"net"
	"time"
)

// Lanes outlive pool resets: replacing a lane could allow two writers to the
// same peer. Only lookup is global; dial, rate limiting, Write and audit are not.
var peerSendLanes = make(map[string]chan struct{}) // guarded by connMaplock

func peerSendLane(addr string) chan struct{} {
	connMaplock.Lock()
	defer connMaplock.Unlock()
	lane := peerSendLanes[addr]
	if lane == nil {
		lane = make(chan struct{}, 1)
		peerSendLanes[addr] = lane
	}
	return lane
}

// The dial argument makes the close-during-dial boundary deterministic in
// regression tests. Production passes net.Dial; no retries or new deadlines.
func sendPooledFrame(payload []byte, addr string, observe func(string, time.Time, error), dial func(string, string) (net.Conn, error)) (err error) {
	lane := peerSendLane(addr)
	lane <- struct{}{}
	defer func() { <-lane }()
	notify := func(event string, err error) {
		if observe != nil {
			observe(event, time.Now(), err)
		}
	}
	connMaplock.Lock()
	conn, generation := connectionPool[addr], connectionPoolGeneration
	connMaplock.Unlock()
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetKeepAlive(true); err != nil {
			_ = conn.Close()
			connMaplock.Lock()
			if connectionPool[addr] == conn {
				delete(connectionPool, addr)
			}
			connMaplock.Unlock()
			conn = nil
		}
	}
	if conn == nil {
		conn, err = dial("tcp", addr)
		if err != nil {
			notify("connect_error", err)
			return err
		}
		connMaplock.Lock()
		stale := generation != connectionPoolGeneration
		if !stale {
			connectionPool[addr] = conn
		}
		connMaplock.Unlock()
		if stale {
			_ = conn.Close()
			notify("connect_error", net.ErrClosed)
			return net.ErrClosed
		}
	}
	defer func() {
		if err != nil {
			connMaplock.Lock()
			if connectionPool[addr] == conn {
				delete(connectionPool, addr)
			}
			connMaplock.Unlock()
			_ = conn.Close()
		}
	}()
	// An epoch sender may have used this same pool entry with a deadline.
	// Ordinary transport retains its original unbounded socket-write behavior.
	if err = conn.SetWriteDeadline(time.Time{}); err != nil {
		notify("write_error", err)
		return err
	}
	writer := io.Writer(conn)
	var observed *observedWriteConn
	if observe != nil {
		observed = &observedWriteConn{Conn: conn, observe: observe}
		writer = observed
	}
	// Preserve the original process-wide, whole-frame token accounting.
	limited := &rateLimitedWriter{writer: writer, limiter: rateLimiterUpload}
	n, err := limited.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil && observed != nil && !observed.failed {
		// WaitN can fail before the socket wrapper observes a Write. Preserve
		// that error as well, without duplicating a socket write_error event.
		notify("write_error", err)
	}
	return err
}
