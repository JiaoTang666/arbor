package networks

import (
	"blockEmulator/params"
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

func TestHistoryTransportDeliversAndCloses(t *testing.T) {
	oldDelay, oldJitter := params.Delay, params.JitterRange
	params.Delay = 0
	params.JitterRange = 0
	defer func() { params.Delay = oldDelay; params.JitterRange = oldJitter }()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	result := make(chan error, 1)
	go func() {
		c, e := listener.Accept()
		if e != nil {
			result <- e
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(time.Second))
		reader := bufio.NewReader(c)
		line, e := reader.ReadString('\n')
		if e == nil && line != "history\n" {
			e = net.ErrClosed
		}
		if e == nil {
			_, e = reader.ReadByte()
			if e == io.EOF {
				e = nil
			} else {
				e = net.ErrClosed
			}
		}
		result <- e
	}()
	if err = SendHistoryBounded([]byte("history"), listener.Addr().String(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
}
func TestHistoryTransportBudgetIncludesDelay(t *testing.T) {
	oldDelay, oldJitter := params.Delay, params.JitterRange
	params.Delay = 1000
	params.JitterRange = 0
	defer func() { params.Delay = oldDelay; params.JitterRange = oldJitter }()
	start := time.Now()
	err := SendHistoryBounded([]byte("history"), "127.0.0.1:1", 20*time.Millisecond)
	if err == nil || time.Since(start) > time.Second {
		t.Fatal("transport did not respect budget")
	}
}
