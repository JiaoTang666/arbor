package ownership

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

const runtimeStatusBufferBytes = 32 << 10

// This bounded buffer contains diagnostics, never certificates or authority
// state. The signed append journal retains its existing durability barriers.
// refreshMu serializes access. Flush before appending a complete line so direct
// stdout users cannot interleave inside one JSON record at a buffer boundary.
type runtimeStatusWriter struct {
	buffer *bufio.Writer
	err    error
	closed bool
}

func newRuntimeStatusWriter(output io.Writer) *runtimeStatusWriter {
	return &runtimeStatusWriter{buffer: bufio.NewWriterSize(output, runtimeStatusBufferBytes)}
}

func (w *runtimeStatusWriter) append(line []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("runtime ownership diagnostics already closed")
	}
	if len(line) == 0 || len(line) > runtimeStatusBufferBytes || line[len(line)-1] != '\n' {
		w.err = fmt.Errorf("runtime ownership diagnostic must be a bounded complete line")
		return w.err
	}
	if w.buffer.Available() < len(line) {
		if w.err = w.buffer.Flush(); w.err != nil {
			return w.err
		}
	}
	_, w.err = w.buffer.Write(line)
	return w.err
}

func (w *runtimeStatusWriter) close() error {
	if !w.closed {
		w.closed = true
		if w.err == nil {
			w.err = w.buffer.Flush()
		}
	}
	return w.err
}

func (view *View) runtimeStatusLocked() *runtimeStatusWriter {
	if view.runtimeStatus == nil {
		view.runtimeStatus = newRuntimeStatusWriter(os.Stdout)
	}
	return view.runtimeStatus
}

// CloseRuntimeDiagnostics preserves every completed diagnostic at clean exit,
// surfaces sticky write errors and never closes the process's shared stdout.
// Crash/SIGKILL may lose this bounded diagnostic tail, not authorization WAL.
func (view *View) CloseRuntimeDiagnostics() error {
	if view == nil || view.journalPath == "" {
		return nil
	}
	view.refreshMu.Lock()
	defer view.refreshMu.Unlock()
	return view.runtimeStatusLocked().close()
}
