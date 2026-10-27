package networks

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestTcpDialWaitDrainsWholeFrame(t *testing.T) {
	for _, epoch := range []string{"", "1"} {
		t.Run("epoch="+epoch, func(t *testing.T) {
			pooledTestSetup(t)
			t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", epoch)
			writer, reader := net.Pipe()
			defer reader.Close()
			started := make(chan struct{}, 1)
			pooledTestPut("shutdown-peer", &reviewSignaledConn{Conn: writer, started: started})
			done := make(chan error, 1)
			go func() { done <- TcpDialWait([]byte("stop"), "shutdown-peer") }()
			pooledTestWait(t, started, "stop write")
			select {
			case err := <-done:
				t.Fatalf("returned before the blocked write completed: %v", err)
			default:
			}
			line, err := bufio.NewReader(reader).ReadString('\n')
			if err != nil || line != "stop\n" {
				t.Fatalf("frame=%q error=%v", line, err)
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("completed write did not return")
			}
		})
	}
}

func TestTcpDialWaitReturnsWriteFailure(t *testing.T) {
	pooledTestSetup(t)
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "")
	writer, reader := net.Pipe()
	reader.Close()
	pooledTestPut("closed-shutdown-peer", writer)
	if err := TcpDialWait([]byte("stop"), "closed-shutdown-peer"); err == nil {
		t.Fatal("write failure was lost")
	}
}
