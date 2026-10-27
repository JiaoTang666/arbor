package networks

import (
	"blockEmulator/params"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"
)

// Only the map lookup is global. A lane serializes whole frames to one peer,
// while other peers may progress. The upload limiter remains process-wide.
func sendEpochPooled(payload []byte, addr string, observe func(string, time.Time, error), budget time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	lane := peerSendLane(addr)
	select {
	case lane <- struct{}{}:
		defer func() { <-lane }()
	case <-ctx.Done():
		return ctx.Err()
	}
	delay := params.Delay
	if params.JitterRange > 0 {
		delay += rand.Intn(params.JitterRange) - params.JitterRange/2
	}
	if delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	connMaplock.Lock()
	conn, generation := connectionPool[addr], connectionPoolGeneration
	connMaplock.Unlock()
	if conn == nil {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
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
			return net.ErrClosed
		}
	}
	// Never replay a failed/partially written frame. A later distinct send may
	// establish a fresh connection, but this send's error is preserved.
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
	deadline, _ := ctx.Deadline()
	if err = conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	first := true
	for len(payload) > 0 {
		n := len(payload)
		if rateLimiterUpload != nil {
			burst := rateLimiterUpload.Burst()
			if burst <= 0 {
				return fmt.Errorf("epoch upload burst must be positive")
			}
			if n > burst {
				n = burst
			}
			if err = rateLimiterUpload.WaitN(ctx, n); err != nil {
				return err
			}
		}
		written, writeErr := conn.Write(payload[:n])
		if written > 0 && first {
			first = false
			if observe != nil {
				// A positive write has returned; retain any socket blocking time.
				// This is neither full payload completion nor a remote receipt.
				observe("first_socket_write", time.Now(), nil)
			}
		}
		if writeErr != nil {
			return writeErr
		}
		if written != n {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}
