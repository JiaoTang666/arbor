package ownership

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type runtimeStatusTestWriter struct {
	writes     [][]byte
	closeCalls int
	failure    error
	shortWrite bool
}

func (w *runtimeStatusTestWriter) Write(raw []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), raw...))
	if w.failure != nil {
		return 0, w.failure
	}
	if w.shortWrite {
		return len(raw) - 1, nil
	}
	return len(raw), nil
}

func (w *runtimeStatusTestWriter) Close() error {
	w.closeCalls++
	return nil
}

func (w *runtimeStatusTestWriter) bytes() []byte { return bytes.Join(w.writes, nil) }

func TestRuntimeStatusBuffersShortLinesUntilClose(t *testing.T) {
	sink := &runtimeStatusTestWriter{}
	w := newRuntimeStatusWriter(sink)
	if runtimeStatusBufferBytes != 32<<10 || w.buffer.Size() != runtimeStatusBufferBytes {
		t.Fatal("runtime status buffer is not bounded to 32 KiB")
	}
	want := []byte("first record\nsecond record\n")
	for _, line := range [][]byte{[]byte("first record\n"), []byte("second record\n")} {
		if err := w.append(line); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.writes) != 0 {
		t.Fatal("short records reached the underlying writer before close")
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sink.bytes(), want) || len(sink.writes) != 1 || !w.closed {
		t.Fatalf("close did not flush the complete exact tail: writes=%q closed=%v", sink.writes, w.closed)
	}
	if err := w.close(); err != nil || len(sink.writes) != 1 {
		t.Fatalf("close is not idempotent: err=%v writes=%d", err, len(sink.writes))
	}
	if err := w.append([]byte("after close\n")); err == nil {
		t.Fatal("closed status writer accepted a record")
	}
	if !bytes.Equal(sink.bytes(), want) || sink.closeCalls != 0 {
		t.Fatal("close/post-close append changed output or closed the shared underlying writer")
	}
}

func TestRuntimeStatusFlushPreservesWholeLines(t *testing.T) {
	sink := &runtimeStatusTestWriter{}
	w := newRuntimeStatusWriter(sink)
	first := append(bytes.Repeat([]byte("a"), runtimeStatusBufferBytes-9), '\n')
	second, third := []byte("bbbbbbb\n"), []byte("tail\n")
	for _, line := range [][]byte{first, second} {
		if err := w.append(line); err != nil {
			t.Fatal(err)
		}
	}
	if w.buffer.Buffered() != runtimeStatusBufferBytes || len(sink.writes) != 0 {
		t.Fatal("exactly full buffer was not retained until the next append")
	}
	if err := w.append(third); err != nil {
		t.Fatal(err)
	}
	full := append(append([]byte(nil), first...), second...)
	if len(sink.writes) != 1 || !bytes.Equal(sink.writes[0], full) {
		t.Fatalf("append split a complete line across a flush boundary: lengths=%v", runtimeStatusWriteLengths(sink.writes))
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 2 || !bytes.Equal(sink.writes[1], third) || !bytes.Equal(sink.bytes(), append(full, third...)) {
		t.Fatal("close lost or duplicated records after the full-buffer flush")
	}
	for _, raw := range sink.writes {
		if len(raw) > runtimeStatusBufferBytes || len(raw) == 0 || raw[len(raw)-1] != '\n' {
			t.Fatal("underlying write was oversized or ended inside a record")
		}
	}
}

func runtimeStatusWriteLengths(writes [][]byte) []int {
	lengths := make([]int, len(writes))
	for i, raw := range writes {
		lengths[i] = len(raw)
	}
	return lengths
}

func TestRuntimeStatusWriteFailuresAreSticky(t *testing.T) {
	writeFailure := errors.New("runtime status test write failure")
	for _, mode := range []string{"write-error", "short-write"} {
		for _, boundary := range []string{"append-flush", "close-flush"} {
			t.Run(mode+"/"+boundary, func(t *testing.T) {
				sink := &runtimeStatusTestWriter{failure: writeFailure}
				wantErr := writeFailure
				if mode == "short-write" {
					sink.failure, sink.shortWrite, wantErr = nil, true, io.ErrShortWrite
				}
				w := newRuntimeStatusWriter(sink)
				fullLine := append(bytes.Repeat([]byte("x"), runtimeStatusBufferBytes-1), '\n')
				if err := w.append(fullLine); err != nil || len(sink.writes) != 0 {
					t.Fatalf("capacity-sized complete record was not buffered: %v", err)
				}
				var err error
				if boundary == "append-flush" {
					err = w.append([]byte("next\n"))
				} else {
					err = w.close()
				}
				if !errors.Is(err, wantErr) || !errors.Is(w.err, wantErr) {
					t.Fatalf("underlying failure was not retained: err=%v sticky=%v", err, w.err)
				}
				if err := w.append([]byte("retry\n")); !errors.Is(err, wantErr) {
					t.Fatalf("append did not retain the original failure: %v", err)
				}
				for i := 0; i < 2; i++ {
					if err := w.close(); !errors.Is(err, wantErr) {
						t.Fatalf("close did not retain the original failure: %v", err)
					}
				}
				if len(sink.writes) != 1 || sink.closeCalls != 0 || !w.closed {
					t.Fatal("sticky failure retried I/O, closed the underlying writer, or failed to close")
				}
			})
		}
	}
}

func TestRuntimeStatusRejectsOversizeWithoutWrites(t *testing.T) {
	oversize := append(bytes.Repeat([]byte("x"), runtimeStatusBufferBytes), '\n')
	for _, tc := range []struct {
		name string
		line []byte
	}{{"oversize", oversize}, {"empty", nil}, {"incomplete", []byte("incomplete")}} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &runtimeStatusTestWriter{}
			w := newRuntimeStatusWriter(sink)
			before := []byte("before\n")
			if err := w.append(before); err != nil {
				t.Fatal(err)
			}
			invalidErr := w.append(tc.line)
			if invalidErr == nil || !errors.Is(w.err, invalidErr) {
				t.Fatal("invalid diagnostic line was accepted or its error was not sticky")
			}
			if w.closed || w.buffer.Buffered() != len(before) || len(sink.writes) != 0 {
				t.Fatal("invalid line rejection changed buffered data or performed underlying I/O")
			}
			if err := w.append([]byte("after\n")); !errors.Is(err, invalidErr) || w.buffer.Buffered() != len(before) {
				t.Fatal("later append did not preserve the original rejection and buffered data")
			}
			for i := 0; i < 2; i++ {
				if err := w.close(); !errors.Is(err, invalidErr) {
					t.Fatalf("close masked the diagnostic rejection: %v", err)
				}
			}
			if len(sink.writes) != 0 || sink.closeCalls != 0 {
				t.Fatal("rejected diagnostic flushed or closed the underlying writer")
			}
		})
	}
}

func runtimeStatusExpectedLine(t *testing.T, version, certificates uint64, offset int64, owners int) []byte {
	t.Helper()
	// The legacy map encoder fixes both the six fields and their JSON ordering.
	raw, err := json.Marshal(map[string]interface{}{
		"pid": os.Getpid(), "registry_version": version,
		"verified_new_certificates": certificates, "journal_offset": offset,
		"certified_owners": owners, "mode": "verified_runtime_append_journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte("RUNTIME_OWNERSHIP_AUTHORIZATION "), raw...), '\n')
}

func runtimeStatusInject(view *View, output io.Writer) {
	view.refreshMu.Lock()
	defer view.refreshMu.Unlock()
	view.runtimeStatus = newRuntimeStatusWriter(output)
}

func TestRuntimeStatusRefreshValidatesAndKeepsLegacyFields(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	sink := &runtimeStatusTestWriter{}
	runtimeStatusInject(view, sink)
	a, b, rejected := testAccount(1), testAccount(2), testAccount(3)
	first := runtimeCertificate(t, f, 0, 0, 2, a)
	firstRaw := appendRuntime(t, journal, first, true)
	metrics, err := view.RefreshMeasured()
	if err != nil || metrics.Certificates != 1 || view.Version() != 1 {
		t.Fatalf("real quorum certificate did not refresh: err=%v metrics=%+v", err, metrics)
	}
	want := runtimeStatusExpectedLine(t, 1, 1, int64(len(firstRaw)), 1)
	replayRaw := appendRuntime(t, journal, first, true)
	metrics, err = view.RefreshMeasured()
	if err != nil || metrics.Certificates != 1 || view.Version() != 1 {
		t.Fatalf("idempotent replay changed authorization: err=%v metrics=%+v", err, metrics)
	}
	secondRaw := appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, b), true)
	metrics, err = view.RefreshMeasured()
	offset := int64(len(firstRaw) + len(replayRaw) + len(secondRaw))
	if err != nil || metrics.Certificates != 1 || view.Version() != 2 || view.journalOffset != offset {
		t.Fatalf("second certificate did not preserve the verified prefix: err=%v metrics=%+v", err, metrics)
	}
	want = append(want, runtimeStatusExpectedLine(t, 2, 1, offset, 2)...)
	bad := runtimeCertificate(t, f, 2, 4, 6, rejected)
	bad.Signatures[0].Value[0] ^= 1
	appendRuntime(t, journal, bad, true)
	if err := view.Refresh(); err == nil || view.Version() != 2 || view.journalOffset != offset {
		t.Fatal("buffered diagnostics bypassed signature rejection or changed the verified prefix")
	}
	if owner, ok := view.Owner(a); !ok || owner != 1 {
		t.Fatal("first real certificate did not authorize its binding")
	}
	if owner, ok := view.Owner(b); !ok || owner != 1 {
		t.Fatal("second real certificate did not authorize its binding")
	}
	if _, ok := view.Owner(rejected); ok {
		t.Fatal("bad signature authorized an owner")
	}
	if len(sink.writes) != 0 {
		t.Fatal("small runtime status records were written before close")
	}
	if err := view.CloseRuntimeDiagnostics(); err != nil || !bytes.Equal(sink.bytes(), want) {
		t.Fatalf("status changed its legacy six fields, framing, replay policy, or values: err=%v got=%q want=%q", err, sink.bytes(), want)
	}
	if err := view.CloseRuntimeDiagnostics(); err != nil || len(sink.writes) != 1 || sink.closeCalls != 0 {
		t.Fatal("view close was not idempotent or closed the shared output")
	}
	if err := view.Refresh(); err == nil || view.Version() != 2 || view.journalOffset != offset {
		t.Fatal("refresh after diagnostic close was not fenced")
	}
}

func TestRuntimeStatusViewWriteFailureFencesLaterRefresh(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	writeFailure := errors.New("runtime status view write failure")
	sink := &runtimeStatusTestWriter{failure: writeFailure}
	runtimeStatusInject(view, sink)
	view.refreshMu.Lock()
	err := view.runtimeStatus.append(append(bytes.Repeat([]byte("p"), runtimeStatusBufferBytes-1), '\n'))
	view.refreshMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	a, b := testAccount(1), testAccount(2)
	firstRaw := appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, a), true)
	metrics, err := view.RefreshMeasured()
	if !errors.Is(err, writeFailure) || metrics.Certificates != 1 || view.Version() != 1 || view.journalOffset != int64(len(firstRaw)) {
		t.Fatalf("diagnostic flush failure was masked or rolled back the already verified head: err=%v metrics=%+v", err, metrics)
	}
	if owner, ok := view.Owner(a); !ok || owner != 1 {
		t.Fatal("diagnostic failure removed an already verified owner")
	}
	appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, b), true)
	metrics, err = view.RefreshMeasured()
	if !errors.Is(err, writeFailure) || metrics.Certificates != 0 || view.Version() != 1 || view.journalOffset != int64(len(firstRaw)) {
		t.Fatalf("sticky diagnostic failure permitted later journal consumption: err=%v metrics=%+v", err, metrics)
	}
	if _, ok := view.Owner(b); ok {
		t.Fatal("refresh after sticky output failure authorized the next certificate")
	}
	for i := 0; i < 2; i++ {
		if err := view.CloseRuntimeDiagnostics(); !errors.Is(err, writeFailure) {
			t.Fatalf("view close masked the original output failure: %v", err)
		}
	}
	if len(sink.writes) != 1 || sink.closeCalls != 0 {
		t.Fatal("view retried failed diagnostic I/O or closed the shared underlying output")
	}
}

type runtimeStatusBlockingWriter struct {
	runtimeStatusTestWriter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *runtimeStatusBlockingWriter) Write(raw []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	return w.runtimeStatusTestWriter.Write(raw)
}

func runtimeStatusAwait(t *testing.T, done <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func TestRuntimeStatusConcurrentRefreshAndClose(t *testing.T) {
	for _, firstOperation := range []string{"refresh", "close"} {
		t.Run(firstOperation+"-holds-refresh-lock", func(t *testing.T) {
			f, view, journal := runtimeFixture(t)
			sink := &runtimeStatusBlockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(sink.release) }) }
			t.Cleanup(unblock)
			runtimeStatusInject(view, sink)
			raw := appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, testAccount(1)), true)
			want := runtimeStatusExpectedLine(t, 1, 1, int64(len(raw)), 1)
			var refreshErr, closeErr error
			refreshDone, closeDone := make(chan struct{}), make(chan struct{})
			refresh := func() { refreshErr = view.Refresh(); close(refreshDone) }
			closeDiagnostics := func() { closeErr = view.CloseRuntimeDiagnostics(); close(closeDone) }
			if firstOperation == "refresh" {
				// Force this real certificate's status append to flush while it
				// still owns refreshMu; close must wait behind that refresh.
				prefix := append(bytes.Repeat([]byte("p"), runtimeStatusBufferBytes-1), '\n')
				view.refreshMu.Lock()
				err := view.runtimeStatus.append(prefix)
				view.refreshMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				want = append(prefix, want...)
				go refresh()
				runtimeStatusAwait(t, sink.entered, "active refresh's flush")
				go closeDiagnostics()
				select {
				case <-closeDone:
					t.Fatal("close returned while the active refresh still held refreshMu")
				default:
				}
			} else {
				if err := view.Refresh(); err != nil {
					t.Fatal(err)
				}
				go closeDiagnostics()
				runtimeStatusAwait(t, sink.entered, "close's flush")
				go refresh()
				select {
				case <-refreshDone:
					t.Fatal("refresh returned while close still held refreshMu")
				default:
				}
			}
			unblock()
			runtimeStatusAwait(t, refreshDone, "concurrent refresh")
			runtimeStatusAwait(t, closeDone, "concurrent close")
			if closeErr != nil || (firstOperation == "refresh" && refreshErr != nil) || (firstOperation == "close" && refreshErr == nil) {
				t.Fatalf("refresh/close serialization mismatch: refresh=%v close=%v", refreshErr, closeErr)
			}
			if view.Version() != 1 || view.journalOffset != int64(len(raw)) || !bytes.Equal(sink.bytes(), want) {
				t.Fatal("concurrent close lost or duplicated a successful refresh's record or authority state")
			}
			if err := view.CloseRuntimeDiagnostics(); err != nil || sink.closeCalls != 0 {
				t.Fatal("concurrent close was not idempotent or closed shared output")
			}
			if err := view.Refresh(); err == nil {
				t.Fatal("refresh succeeded after concurrent close")
			}
		})
	}
}

func TestRuntimeStatusCloseBeforeRefreshFencesJournal(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	if view.runtimeStatus != nil {
		t.Fatal("empty initial refresh eagerly allocated diagnostics")
	}
	sink := &runtimeStatusTestWriter{}
	runtimeStatusInject(view, sink)
	if err := view.CloseRuntimeDiagnostics(); err != nil {
		t.Fatal(err)
	}
	appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, testAccount(1)), true)
	metrics, err := view.RefreshMeasured()
	if err == nil || metrics.Certificates != 0 || view.Version() != 0 || view.journalOffset != 0 {
		t.Fatalf("closed diagnostics allowed new journal consumption: err=%v metrics=%+v", err, metrics)
	}
	if _, ok := view.Owner(testAccount(1)); ok {
		t.Fatal("refresh after close authorized an owner")
	}
	if len(sink.writes) != 0 || sink.closeCalls != 0 {
		t.Fatal("empty close wrote output or closed the underlying writer")
	}
}
