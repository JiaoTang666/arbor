package chain

// Opt-in forensic instrumentation. It changes no chain state, authorization,
// routing or transaction selection. Runs using it are diagnostic, not an arm
// for claiming a performance improvement.
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var executionWaitCaptures atomic.Uint32

func beginExecutionWaitTrace(shard, height uint64) func() {
	dir := os.Getenv("COOLCHAIN_EXECUTION_WAIT_TRACE_DIR")
	if dir == "" || executionWaitCaptures.Load() >= 2 {
		return nil
	}
	started := time.Now()
	done := make(chan struct{})
	var once sync.Once
	go func() {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
		}
		select {
		case <-done:
			return
		default:
		}
		capture := executionWaitCaptures.Add(1)
		if capture > 2 {
			return
		}
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		stackAt := time.Now()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		finished := false
		select {
		case <-done:
			finished = true
		default:
		}
		metadata := struct {
			Schema                                         string `json:"schema"`
			PID                                            int    `json:"pid"`
			Shard, Height                                  uint64
			Capture                                        uint32
			Started, StackCaptured                         time.Time
			ElapsedSeconds                                 float64
			ExecutionFinishedDuringCapture, StackTruncated bool
			HeapAlloc, HeapInuse, HeapSys, PauseTotalNs    uint64
			NumGC                                          uint32
		}{"coolchain/execution-wait-trace-v1", os.Getpid(), shard, height,
			capture, started, stackAt, stackAt.Sub(started).Seconds(), finished,
			n == len(stack), mem.HeapAlloc, mem.HeapInuse, mem.HeapSys, mem.PauseTotalNs, mem.NumGC}
		if err := os.MkdirAll(dir, 0700); err != nil {
			fmt.Fprintf(os.Stderr, "EXECUTION_WAIT_TRACE error=%q\n", err)
			return
		}
		path := filepath.Join(dir, fmt.Sprintf("s%d-p%d-h%d-c%d.txt", shard, os.Getpid(), height, capture))
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			err = json.NewEncoder(f).Encode(metadata)
			if err == nil {
				_, err = f.Write(stack[:n])
			}
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "EXECUTION_WAIT_TRACE path=%q error=%q\n", path, err)
			return
		}
		fmt.Fprintf(os.Stderr, "EXECUTION_WAIT_TRACE path=%q elapsed_seconds=%.6f finished=%t truncated=%t\n", path, stackAt.Sub(started).Seconds(), finished, n == len(stack))
	}()
	return func() { once.Do(func() { close(done) }) }
}
