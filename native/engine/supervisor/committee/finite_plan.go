package committee

// Test-only finite source lifecycle. No receipt, route policy, or fence state
// transitions are replaced. A begun operation may finish during closeout.
import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type finitePlanConfig struct {
	Mode             string `json:"mode"`
	InputSHA256      string `json:"input_sha256"`
	Count            int    `json:"count"`
	Rate             int    `json:"rate"`
	AfterLastSeconds int    `json:"after_last_seconds"`
	StopSeconds      int    `json:"stop_seconds"`
	MaxRSSGiB        int    `json:"max_rss_gib"`
	MaxDatabaseGiB   int    `json:"max_database_gib"`
	MaxTextGiB       int    `json:"max_text_gib"`
	MaxProcesses     int    `json:"max_processes"`
	SourcePlanSHA256 string `json:"source_plan_sha256"`
	SourcePlanPath   string `json:"source_plan_path"`
}

type finitePlan struct {
	count          int
	afterLast      time.Duration
	mu             sync.Mutex
	origin, cutoff time.Time
	now            func() time.Time
	path           string
	exceeded       bool
	stopped        bool
	maxDebt        int64
	lastSecond     int64
}

func (m *RelayCommitteeModule) configureFinitePlan() error {
	p := os.Getenv("SAA_FINITE_PLAN_CONFIG")
	if p == "" {
		return nil
	}
	raw, e := os.ReadFile(p)
	if e != nil {
		return e
	}
	var c finitePlanConfig
	if e = json.Unmarshal(raw, &c); e != nil {
		return e
	}
	if c.Mode != "finite_plan_bounded_catchup" || c.Count < 1 || c.Count > 1000000 || c.Rate != 1000 || c.AfterLastSeconds < 300 || c.AfterLastSeconds > 172800 || c.StopSeconds != 60 || c.MaxRSSGiB != 240 || c.MaxDatabaseGiB < 32 || c.MaxDatabaseGiB > 96 || c.MaxTextGiB < 1 || c.MaxTextGiB > 32 || c.MaxProcesses != 65 {
		return fmt.Errorf("finite_plan_invalid_bounds")
	}
	if m.dataTotalNum != c.Count || m.batchDataNum != 1000 || m.offeredLoadTPS != 1000 || !m.streamFirstPlacement || m.firstAdmission == nil || m.offeredAuditPath == "" || os.Getenv("SAA_FIRST_PLACEMENT_AUDIT") == "" || m.epochAwareDispatchEnabled {
		return fmt.Errorf("finite_plan_requires_audited_fixed_stream_without_epoch_wait")
	}
	if m.csvPath == c.SourcePlanPath {
		return fmt.Errorf("finite_identity_paths_must_differ")
	}
	for path, want := range map[string]string{m.csvPath: c.InputSHA256, c.SourcePlanPath: c.SourcePlanSHA256} {
		if len(want) != 64 || path == "" {
			return fmt.Errorf("finite_plan_missing_identity")
		}
		st, e := os.Stat(path)
		if e != nil {
			return e
		}
		if st.Size() > 2*1024*1024*1024 {
			return fmt.Errorf("finite_identity_file_too_large")
		}
		input, e := os.Open(path)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, input)
		input.Close()
		if e != nil {
			return e
		}
		if hex.EncodeToString(h.Sum(nil)) != want {
			return fmt.Errorf("finite_plan_identity_mismatch")
		}
		if path == m.csvPath {
			input, e := os.Open(path)
			if e != nil {
				return e
			}
			reader := csv.NewReader(input)
			i := 0
			for {
				row, e := reader.Read()
				if e == io.EOF {
					break
				}
				if e != nil {
					input.Close()
					return e
				}
				if _, ok := data2tx(row, uint64(i)); !ok {
					input.Close()
					return fmt.Errorf("finite_input_rejected_row_%d", i)
				}
				i++
			}
			input.Close()
			if i != c.Count {
				return fmt.Errorf("finite_input_count_invalid")
			}
		} else {
			input, e := os.Open(path)
			if e != nil {
				return e
			}
			scanner := bufio.NewScanner(input)
			i := 0
			for scanner.Scan() {
				line := scanner.Bytes()
				var r struct {
					ID       int   `json:"id"`
					Relative int64 `json:"relative_plan_ns"`
				}
				if json.Unmarshal(line, &r) != nil || r.ID != i || r.Relative != int64(i)*1000000 {
					input.Close()
					return fmt.Errorf("finite_plan_sequence_invalid")
				}
				i++
			}
			e = scanner.Err()
			input.Close()
			if e != nil {
				return e
			}
			if i != c.Count {
				return fmt.Errorf("finite_plan_count_invalid")
			}
		}
	}
	out := m.offeredAuditPath + ".finite.v2.jsonl"
	f, e := os.OpenFile(out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	m.finitePlan = &finitePlan{count: c.Count, now: time.Now, path: out, lastSecond: -1, afterLast: time.Duration(c.AfterLastSeconds) * time.Second}
	return nil
}

func (p *finitePlan) recordLocked(kind string, fields map[string]interface{}) {
	if p.path == "" {
		return
	}
	fields["event"] = kind
	at, ok := fields["at"].(time.Time)
	if !ok {
		at = p.now()
	}
	fields["at"] = at.UTC()
	if !p.origin.IsZero() {
		fields["relative_ns"] = at.Sub(p.origin).Nanoseconds()
	}
	f, e := os.OpenFile(p.path, os.O_APPEND|os.O_WRONLY, 0644)
	if e != nil {
		panic(e)
	}
	e = json.NewEncoder(f).Encode(fields)
	ce := f.Close()
	if e != nil {
		panic(e)
	}
	if ce != nil {
		panic(ce)
	}
}
func (p *finitePlan) start(t time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.origin.IsZero() {
		return
	}
	p.origin = t
	after := p.afterLast
	if after == 0 {
		after = 300 * time.Second
	}
	count := p.totalCount()
	last := time.Duration(count-1) * time.Millisecond
	duration := last + after
	p.cutoff = t.Add(duration)
	p.recordLocked("finite_plan_started", map[string]interface{}{"count": count, "last_plan_ns": int64(last), "cutoff_ns": int64(duration), "clock": "Go monotonic; fixed at first source attempt"})
}

func (p *finitePlan) totalCount() int {
	if p.count == 0 {
		return 12000
	}
	return p.count
}
func (p *finitePlan) stopLocked() bool {
	if p.origin.IsZero() {
		return false
	}
	if !p.now().Before(p.cutoff) {
		if !p.stopped {
			p.stopped = true
			p.recordLocked("finite_cutoff", map[string]interface{}{"max_legacy_debt": p.maxDebt, "boundary": "no new operation starts; previously started writes may finish"})
		}
	}
	return p.stopped
}
func (p *finitePlan) closed() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopLocked()
}

// Each operation's acceptance point is this check, not later I/O completion.
func (p *finitePlan) begin(kind string, id uint64) bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.origin.IsZero() {
		return false
	}
	at := p.now()
	if p.stopped || !at.Before(p.cutoff) {
		p.stopLocked()
		return false
	}
	p.recordLocked("finite_operation_begin", map[string]interface{}{"operation": kind, "ingress_sequence": id, "at": at})
	return true
}
func (p *finitePlan) context() (context.Context, context.CancelFunc) {
	if p == nil {
		return context.WithTimeout(context.Background(), 120*time.Second)
	}
	p.mu.Lock()
	cut := p.cutoff
	p.mu.Unlock()
	if cut.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithDeadline(context.Background(), cut)
}
func (p *finitePlan) observe(due, ordinal int64) {
	if p == nil {
		if due-ordinal > 10000 {
			panic("planned arrival debt exceeded 10000 source events; preserve artifacts")
		}
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	debt := due - ordinal
	if debt > p.maxDebt {
		p.maxDebt = debt
	}
	if debt > 10000 && !p.exceeded {
		p.exceeded = true
		p.recordLocked("finite_debt_first_exceeded", map[string]interface{}{"legacy_debt": debt, "due": due, "ordinal": ordinal, "threshold": 10000})
	}
	second := int64(p.now().Sub(p.origin) / time.Second)
	if second != p.lastSecond {
		p.lastSecond = second
		p.recordLocked("finite_progress", map[string]interface{}{"due": due, "legacy_debt": debt, "max_legacy_debt": p.maxDebt, "attempt_ordinal": ordinal, "phase": map[bool]string{true: "bounded_catchup", false: "planned_arrivals"}[second*1000 >= int64(p.totalCount())]})
	}
}
