package networks

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type reviewSignaledConn struct {
	net.Conn
	started chan struct{}
}

func (c *reviewSignaledConn) Write(p []byte) (int, error) {
	select {
	case c.started <- struct{}{}:
	default:
	}
	return c.Conn.Write(p)
}

func TestEpochTransportSlowPeerIsolationAndEviction(t *testing.T) {
	old := rateLimiterUpload
	rateLimiterUpload = nil
	t.Cleanup(func() { CloseAllConnInPool(); rateLimiterUpload = old })
	slow, unread := net.Pipe()
	defer unread.Close()
	fast, reader := net.Pipe()
	defer reader.Close()
	started := make(chan struct{}, 1)
	connMaplock.Lock()
	connectionPool["review-slow"] = &reviewSignaledConn{slow, started}
	connectionPool["review-fast"] = fast
	connMaplock.Unlock()
	done := make(chan error, 1)
	go func() { done <- sendEpochPooled([]byte("slow\n"), "review-slow", nil, 300*time.Millisecond) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow write never started")
	}
	got := make(chan string, 1)
	go func() {
		b := make([]byte, 5)
		_, e := io.ReadFull(reader, b)
		if e != nil {
			got <- e.Error()
		} else {
			got <- string(b)
		}
	}()
	if err := sendEpochPooled([]byte("fast\n"), "review-fast", nil, 150*time.Millisecond); err != nil {
		t.Fatal("unrelated peer blocked", err)
	}
	if s := <-got; s != "fast\n" {
		t.Fatal(s)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("write was not bounded")
	}
	connMaplock.Lock()
	_, retained := connectionPool["review-slow"]
	connMaplock.Unlock()
	if retained {
		t.Fatal("failed pooled connection retained")
	}
}

func TestEpochTransportFramesRemainWhole(t *testing.T) {
	old := rateLimiterUpload
	rateLimiterUpload = rate.NewLimiter(rate.Inf, 2) // Force multiple writes per frame.
	t.Cleanup(func() { CloseAllConnInPool(); rateLimiterUpload = old })
	a, b := net.Pipe()
	defer b.Close()
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	connMaplock.Lock()
	connectionPool["review-frames"] = a
	connMaplock.Unlock()
	results := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			results <- sendEpochPooled([]byte(fmt.Sprintf("frame-%02d\n", i)), "review-frames", nil, 2*time.Second)
		}(i)
	}
	scanner := bufio.NewScanner(b)
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		if !scanner.Scan() {
			t.Fatal("missing frame", scanner.Err())
		}
		s := scanner.Text()
		if len(s) != 8 || seen[s] {
			t.Fatal("interleaved or duplicate frame", s)
		}
		seen[s] = true
	}
	for i := 0; i < 20; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if !seen[fmt.Sprintf("frame-%02d", i)] {
			t.Fatal("missing identity", i)
		}
	}
}
