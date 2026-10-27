package networks

import (
	"io"
	"net"
	"testing"
	"time"
)

type returnTimedConn struct {
	net.Conn
	returnedAt time.Time
}

func (c *returnTimedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.returnedAt = time.Now()
	return n, err
}

func TestObservedWritePreservesBytesAndFirstWrite(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	done := make(chan string, 1)
	go func() {
		defer b.Close()
		buf := make([]byte, 4)
		_, e := io.ReadFull(b, buf)
		if e != nil {
			done <- "error"
		} else {
			done <- string(buf)
		}
	}()
	count := 0
	inner := &returnTimedConn{Conn: a}
	c := &observedWriteConn{Conn: inner, observe: func(event string, at time.Time, e error) {
		if event == "first_socket_write" {
			if at.Before(inner.returnedAt) {
				t.Fatal("first write timestamp excluded time blocked inside Write")
			}
			count++
		}
	}}
	if _, e := c.Write([]byte("ab")); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Write([]byte("cd")); e != nil {
		t.Fatal(e)
	}
	if got := <-done; got != "abcd" || count != 1 {
		t.Fatal(got, count)
	}
}

func TestObservedWriteFailureIsNotDispatch(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	b.Close()
	first, failed := 0, 0
	c := &observedWriteConn{Conn: a, observe: func(event string, at time.Time, e error) {
		if event == "first_socket_write" {
			first++
		}
		if event == "write_error" {
			failed++
		}
	}}
	if _, e := c.Write([]byte("x")); e == nil {
		t.Fatal("closed peer accepted write")
	}
	if first != 0 || failed != 1 {
		t.Fatal(first, failed)
	}
}
