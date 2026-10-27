package networks

import (
	"blockEmulator/params"
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

var connMaplock sync.Mutex
var connectionPool = make(map[string]net.Conn, 0)
var connectionPoolGeneration uint64 // guarded by connMaplock

// network params.
var randomDelayGenerator *rand.Rand
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

// Define the latency, jitter and bandwidth here.
// Init tools.
func InitNetworkTools() {
	// avoid wrong params.
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	// generate the random seed.
	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	// Limit the download rate
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	// Limit the upload rate
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

func TcpDial(context []byte, addr string) {
	TcpDialObserved(context, addr, nil)
}

// Observer is optional. Whole frames and callbacks are serialized per peer,
// never under the global connection-map lock. It must not reenter networking.
func TcpDialObserved(context []byte, addr string, observe func(string, time.Time, error)) {
	payload := append(append([]byte(nil), context...), '\n')
	go func() {
		if err := tcpSendFrame(payload, addr, observe); err != nil {
			log.Printf("transport to %s failed (no automatic retry): %v", addr, err)
		}
	}()
}

// TcpDialWait waits for the entire frame's socket write. Shutdown uses it before
// closing the connection pool; a fixed sleep cannot drain asynchronous sends.
func TcpDialWait(context []byte, addr string) error {
	return tcpSendFrame(append(append([]byte(nil), context...), '\n'), addr, nil)
}

func tcpSendFrame(payload []byte, addr string, observe func(string, time.Time, error)) error {
	if os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_V1") == "1" {
		err := sendEpochPooled(payload, addr, observe, 120*time.Second)
		if err != nil && observe != nil {
			observe("transport_error", time.Now(), err)
		}
		return err
	}
	thisDelay := params.Delay
	if params.JitterRange != 0 {
		thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
	}
	time.Sleep(time.Millisecond * time.Duration(thisDelay))
	return sendPooledFrame(payload, addr, observe, net.Dial)
}

type observedWriteConn struct {
	net.Conn
	observe func(string, time.Time, error)
	started bool
	failed  bool
}

func (c *observedWriteConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 && !c.started {
		c.started = true
		// Include time blocked inside Write. This is the first positive write's
		// return boundary, not whole-message delivery or a remote receipt.
		c.observe("first_socket_write", time.Now(), nil)
	}
	if err != nil {
		c.failed = true
		c.observe("write_error", time.Now(), err)
	}
	return n, err
}

// Broadcast sends a message to multiple receivers, excluding the sender.
func Broadcast(sender string, receivers []string, msg []byte) {
	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		go TcpDial(msg, ip)
	}
}

// CloseAllConnInPool closes all connections in the connection pool.
func CloseAllConnInPool() {
	connMaplock.Lock()
	old := connectionPool
	connectionPool = make(map[string]net.Conn) // Reset the pool
	connectionPoolGeneration++                 // fence already-started dials from the old pool
	connMaplock.Unlock()
	for _, conn := range old {
		_ = conn.Close()
	}
}

// ReadFromConn reads data from a connection.
func ReadFromConn(addr string) {
	connMaplock.Lock()
	conn := connectionPool[addr]
	connMaplock.Unlock()
	if conn == nil {
		return
	}

	// new a conn reader
	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		// add message to buffer
		messageBuffer.Write(buffer[:n])

		// handle the full message
		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				// continue to load if buffer is short
				break
			} else if err == nil {
				// log the full message
				log.Println("Received from", addr, ":", message)
			} else {
				// handle other errs
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
