package committee

import (
	"blockEmulator/core"
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// A small replayable view of the same trusted supervisor events. It contains
// no transaction payload and does not replace the full lifecycle audit. One
// route row joins acceptance to its immutable route; completion stays ordered.
// Both streams share batchAudit's lock and periodic flush, never per-row fsync.
type observerProjection struct {
	path, input string
	f           *os.File
	w           *bufio.Writer
	failed      bool
}

func newObserverProjection(lifecycle, input string) *observerProjection {
	p := &observerProjection{path: lifecycle + ".observer.v1.jsonl", input: input}
	raw, err := hex.DecodeString(input)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != input {
		p.fail(fmt.Errorf("observer projection input SHA256 is not canonical"))
		return p
	}
	p.f, err = os.OpenFile(p.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		p.fail(err)
		return p
	}
	p.w = bufio.NewWriterSize(p.f, 1024*1024)
	header := map[string]interface{}{"schema": "occurrence_observer_v1", "input_sha256": input, "lifecycle_file": filepath.Base(lifecycle)}
	if err = json.NewEncoder(p.w).Encode(header); err != nil {
		p.fail(err)
	}
	return p
}

func (p *observerProjection) fail(err error) {
	if p.failed {
		return
	}
	p.failed = true
	// A projection failure invalidates controller evidence, not ordinary native
	// execution. The controller and final run check consume this explicit marker.
	raw, _ := json.Marshal(map[string]interface{}{"schema": "occurrence_observer_failure_v1", "error": err.Error()})
	f, e := os.OpenFile(p.path+".failed.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e == nil {
		_, e = f.Write(append(raw, '\n'))
		if ce := f.Close(); e == nil {
			e = ce
		}
	}
	if e != nil {
		panic(fmt.Errorf("observer projection and failure reporting failed: %v; %w", err, e))
	}
}

func (p *observerProjection) flush() {
	if !p.failed {
		if e := p.w.Flush(); e != nil {
			p.fail(e)
		}
	}
}

func (a *batchAudit) writeObserver(tx *core.Transaction, event string, accepted time.Time) {
	if a.async != nil {
		if a.observer == nil {
			return
		}
		if len(tx.TxHash) != 32 || (tx.AdmissionID != "" && tx.AdmissionID != "native-input/"+a.observer.input+"/"+strconv.FormatUint(tx.IngressSequence, 10)) {
			a.enqueueAudit(auditRecord{projectionError: fmt.Errorf("invalid compact audit identity")})
			return
		}
		var row []interface{}
		if event == "route_bound" {
			if accepted.IsZero() {
				panic("route lacks acceptance")
			}
			row = []interface{}{"r", tx.IngressSequence, hex.EncodeToString(tx.TxHash), accepted.UTC(), tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.MappingVersion, tx.RoutingEpoch}
		} else {
			row = []interface{}{"c", tx.IngressSequence, hex.EncodeToString(tx.TxHash), tx.MappingVersion}
		}
		raw, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		a.enqueueAudit(auditRecord{data: append(raw, '\n'), projection: true})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.observer
	if p == nil || p.failed {
		return
	}
	wantID := "native-input/" + p.input + "/" + strconv.FormatUint(tx.IngressSequence, 10)
	// Effect indexing is optional on the ordinary Relay path. The input header,
	// ingress sequence and payload hash bind an occurrence independently of it.
	if (tx.AdmissionID != "" && tx.AdmissionID != wantID) || len(tx.TxHash) != 32 {
		p.fail(fmt.Errorf("observer projection occurrence identity mismatch"))
		return
	}
	var row []interface{}
	if event == "route_bound" {
		if accepted.IsZero() {
			p.fail(fmt.Errorf("observer route lacks its original acceptance"))
			return
		}
		row = []interface{}{"r", tx.IngressSequence, hex.EncodeToString(tx.TxHash), accepted.UTC(), tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.MappingVersion, tx.RoutingEpoch}
	} else {
		row = []interface{}{"c", tx.IngressSequence, hex.EncodeToString(tx.TxHash), tx.MappingVersion}
	}
	raw, e := json.Marshal(row)
	if e != nil {
		p.fail(e)
		return
	}
	// Prevent the compact buffer's implicit flush from overtaking the full log.
	if p.w.Available() < len(raw)+1 {
		if e = a.w.Flush(); e != nil {
			panic(e)
		}
		p.flush()
		if p.failed {
			return
		}
	}
	if _, e = p.w.Write(append(raw, '\n')); e != nil {
		p.fail(e)
	}
}
