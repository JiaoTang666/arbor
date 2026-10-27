package committee

import (
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/utils"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"time"
)

const forestEpochWorkerSchema = "forest_epoch_worker_v1"
const forestEpochMaxMessage = 64 << 20
const forestEpochObserveGroupBatches = 8
const forestEpochObserveGroupBytes = 2 << 20

// SourceTime is the original CSV clock, never the scheduled offered timestamp
// stored in Transaction.Time. Sequence is the zero-based accepted occurrence.
type forestEpochObservation struct {
	Sequence   uint64  `json:"seq"`
	SourceTime float64 `json:"source_time"`
	Sender     string  `json:"sender"`
	Recipient  string  `json:"recipient"`
}

type forestEpochRequest struct {
	Schema               string                     `json:"schema"`
	ID                   uint64                     `json:"id"`
	Op                   string                     `json:"op"`
	Rows                 []forestEpochObservation   `json:"rows"`
	Batches              [][]forestEpochObservation `json:"batches,omitempty"`
	Epoch                uint64                     `json:"epoch,omitempty"`
	ObservedPrefix       uint64                     `json:"observed_prefix"`
	CurrentMapping       map[string]uint64          `json:"current_mapping"`
	CurrentMappingSHA256 string                     `json:"current_mapping_sha256,omitempty"`
	Eligible             []string                   `json:"eligible"`
	InitialLedgerSHA256  string                     `json:"initial_ledger_sha256"`
	InputSHA256          string                     `json:"input_sha256,omitempty"`
	Shards               uint64                     `json:"shards,omitempty"`
}

type forestEpochReply struct {
	Running              bool              `json:"running"`
	Schema               string            `json:"schema"`
	ID                   uint64            `json:"id"`
	Status               string            `json:"status"`
	Error                string            `json:"error"`
	RequestSHA256        string            `json:"request_sha256"`
	Epoch                uint64            `json:"epoch"`
	ObservedPrefix       uint64            `json:"observed_prefix"`
	CurrentMappingSHA256 string            `json:"current_mapping_sha256"`
	InitialLedgerSHA256  string            `json:"initial_ledger_sha256"`
	InputSHA256          string            `json:"input_sha256"`
	Shards               uint64            `json:"shards"`
	PrefixSHA256         string            `json:"prefix_sha256"`
	Delta                map[string]uint64 `json:"delta"`
	Report               json.RawMessage   `json:"report"`
	Evidence             json.RawMessage   `json:"evidence"`
	RequestBytes         int               `json:"-"`
	ReplyBytes           int               `json:"-"`
}

type forestWorkerReply struct {
	raw []byte
	err error
}

// There is one outstanding request. A timeout or protocol failure permanently
// poisons the worker and kills it; no CLPA fallback or replay is attempted.
type forestEpochWorker struct {
	mu        sync.Mutex
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    *os.File
	replies   chan forestWorkerReply
	exited    chan error
	abort     chan struct{}
	abortOnce sync.Once
	sequence  uint64
	timeout   time.Duration
	failure   error
	closed    bool
}

func newForestEpochWorker(python, script, config, stderrPath string, timeout time.Duration) (*forestEpochWorker, error) {
	if python == "" || script == "" || config == "" || stderrPath == "" || timeout <= 0 {
		return nil, fmt.Errorf("forest worker executable/script/config/stderr/timeout required")
	}
	for _, path := range []string{script, config} {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("forest worker input is not a file: %s", path)
		}
	}
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	w := &forestEpochWorker{cmd: exec.Command(python, "-u", script, "--config", config), stderr: stderr, replies: make(chan forestWorkerReply, 1), exited: make(chan error, 1), abort: make(chan struct{}), timeout: timeout}
	w.cmd.Stderr = stderr
	w.stdin, err = w.cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		return nil, err
	}
	w.stdout, err = w.cmd.StdoutPipe()
	if err != nil {
		w.stdin.Close()
		stderr.Close()
		return nil, err
	}
	if err = w.cmd.Start(); err != nil {
		w.stdin.Close()
		w.stdout.Close()
		stderr.Close()
		return nil, err
	}
	go w.readReplies()
	return w, nil
}

func (w *forestEpochWorker) readReplies() {
	// Wait only after stdout has been drained/closed, so a quick worker exit
	// cannot close StdoutPipe before the final Close acknowledgement is read.
	defer func() {
		err := w.cmd.Wait()
		closeErr := w.stderr.Close()
		if err == nil {
			err = closeErr
		}
		w.exited <- err
	}()
	scanner := bufio.NewScanner(w.stdout)
	scanner.Buffer(make([]byte, 4096), forestEpochMaxMessage)
	for scanner.Scan() {
		row := forestWorkerReply{raw: append([]byte(nil), scanner.Bytes()...)}
		select {
		case w.replies <- row:
		case <-w.abort:
			return
		}
	}
	err := scanner.Err()
	if err == nil {
		err = io.EOF
	}
	select {
	case w.replies <- forestWorkerReply{err: err}:
	case <-w.abort:
	}
}

func (w *forestEpochWorker) terminate() {
	w.abortOnce.Do(func() {
		close(w.abort)
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
		if w.stdin != nil {
			_ = w.stdin.Close()
		}
		if w.stdout != nil {
			_ = w.stdout.Close()
		}
	})
}

func validateForestEpochReply(raw []byte, request forestEpochRequest, requestRaw []byte) (forestEpochReply, error) {
	var reply forestEpochReply
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&reply); err != nil {
		return reply, fmt.Errorf("forest worker response JSON: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return reply, fmt.Errorf("forest worker response contains trailing JSON")
	}
	digest := sha256.Sum256(requestRaw)
	if reply.Schema != forestEpochWorkerSchema || reply.ID != request.ID || reply.RequestSHA256 != hex.EncodeToString(digest[:]) {
		return reply, fmt.Errorf("forest worker response identity/binding mismatch")
	}
	if reply.Status != "OK" {
		return reply, fmt.Errorf("forest worker %s failed: %s", request.Op, reply.Error)
	}
	reply.RequestBytes = len(requestRaw)
	reply.ReplyBytes = len(raw)
	return reply, nil
}

func (w *forestEpochWorker) request(request forestEpochRequest) (reply forestEpochReply, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return reply, fmt.Errorf("forest worker already closed")
	}
	if w.failure != nil {
		return reply, w.failure
	}
	w.sequence++
	request.ID = w.sequence
	request.Schema = forestEpochWorkerSchema
	raw, err := json.Marshal(request)
	if err != nil {
		return reply, err
	}
	if len(raw) > forestEpochMaxMessage {
		return reply, fmt.Errorf("forest request exceeds bounded message size")
	}
	defer func() {
		if err != nil {
			w.failure = err
			w.terminate()
		}
	}()
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()
	written := make(chan error, 1)
	go func() {
		data := append(append([]byte(nil), raw...), '\n')
		n, e := w.stdin.Write(data)
		if e == nil && n != len(data) {
			e = io.ErrShortWrite
		}
		written <- e
	}()
	select {
	case err = <-written:
		if err != nil {
			return reply, fmt.Errorf("forest worker write: %w", err)
		}
	case <-timer.C:
		return reply, fmt.Errorf("forest worker %s write timeout", request.Op)
	}
	select {
	case result := <-w.replies:
		if result.err != nil {
			return reply, fmt.Errorf("forest worker read: %w", result.err)
		}
		return validateForestEpochReply(result.raw, request, raw)
	case <-timer.C:
		return reply, fmt.Errorf("forest worker %s response timeout", request.Op)
	}
}

func (w *forestEpochWorker) Close() error {
	w.mu.Lock()
	if w.closed {
		err := w.failure
		w.mu.Unlock()
		return err
	}
	failed := w.failure
	w.mu.Unlock()
	if failed == nil {
		_, failed = w.request(forestEpochRequest{Op: "close"})
	}
	w.mu.Lock()
	w.closed = true
	if failed != nil && w.failure == nil {
		w.failure = failed
	}
	w.mu.Unlock()
	if w.stdin != nil {
		_ = w.stdin.Close()
	}
	if failed != nil {
		w.terminate()
	}
	select {
	case err := <-w.exited:
		if failed != nil {
			return failed
		}
		return err
	case <-time.After(w.timeout):
		w.terminate()
		select {
		case <-w.exited:
		case <-time.After(time.Second):
		}
		return fmt.Errorf("forest worker exit timeout")
	}
}

type forestEpochProvider struct {
	worker *forestEpochWorker
	// call/close are replaceable only by focused protocol tests.
	call              func(forestEpochRequest) (forestEpochReply, error)
	close             func() error
	observed          uint64
	lastEpoch         uint64
	prefixHash        string
	inputHash         string
	initialRoutes     map[string]uint64 // Immutable base; never serialized into IPC.
	initialLedgerHash string
	eligible          []string
	report            map[string]interface{}
	closed            bool
}

func newForestEpochProvider(initialRoutes map[string]uint64, initialLedgerHash string) (*forestEpochProvider, error) {
	timeout := time.Duration(epochPositiveEnv("BLOCK_EMULATOR_FOREST_TIMEOUT_SECONDS", 60)) * time.Second
	w, err := newForestEpochWorker(os.Getenv("BLOCK_EMULATOR_FOREST_PYTHON"), os.Getenv("BLOCK_EMULATOR_FOREST_WORKER"), os.Getenv("BLOCK_EMULATOR_FOREST_CONFIG"), os.Getenv("BLOCK_EMULATOR_FOREST_STDERR_PATH"), timeout)
	if err != nil {
		return nil, err
	}
	p := &forestEpochProvider{worker: w, call: w.request, close: w.Close, inputHash: os.Getenv("BLOCK_EMULATOR_CLPA_INPUT_SHA256"), initialRoutes: initialRoutes, initialLedgerHash: initialLedgerHash}
	hello, err := p.call(forestEpochRequest{Op: "hello", InputSHA256: p.inputHash, InitialLedgerSHA256: initialLedgerHash, Shards: uint64(params.ShardNum)})
	if err == nil && (hello.InputSHA256 != p.inputHash || hello.InitialLedgerSHA256 != initialLedgerHash || hello.Shards != uint64(params.ShardNum)) {
		err = fmt.Errorf("forest worker configuration differs from native input/ledger/topology")
	}
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (*forestEpochProvider) Name() string { return "forest" }
func (p *forestEpochProvider) SetEligible(accounts []string) {
	p.eligible = append([]string(nil), accounts...)
	sort.Strings(p.eligible)
}
func (p *forestEpochProvider) Report() map[string]interface{} { return p.report }

func forestCanonicalAccount(account string) bool {
	raw, err := hex.DecodeString(account)
	return err == nil && len(raw) == 20 && hex.EncodeToString(raw) == account
}

func forestHashValid(raw string) bool {
	decoded, err := hex.DecodeString(raw)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == raw
}

func (p *forestEpochProvider) baseOwner(account string) uint64 {
	if owner, ok := p.initialRoutes[account]; ok {
		return owner
	}
	return uint64(utils.Addr2Shard(account))
}

// Each request carries the COMPLETE sparse override set relative to the
// immutable initial ledger, not a patch to the worker's previous proposal.
// Returning to an initial owner removes the wire override on the next plan.
func (p *forestEpochProvider) currentOverrides(current map[string]uint64) (map[string]uint64, error) {
	overrides := map[string]uint64{}
	for account, owner := range current {
		if !forestCanonicalAccount(account) || owner >= uint64(params.ShardNum) {
			return nil, fmt.Errorf("invalid current forest mapping")
		}
		if owner != p.baseOwner(account) {
			overrides[account] = owner
		}
	}
	return overrides, nil
}

func (p *forestEpochProvider) Observe(rows []forestEpochObservation) error {
	if p.closed {
		return fmt.Errorf("forest provider is closed")
	}
	for i, row := range rows {
		if row.Sequence != p.observed+uint64(i) || !forestCanonicalAccount(row.Sender) || !forestCanonicalAccount(row.Recipient) || math.IsNaN(row.SourceTime) || math.IsInf(row.SourceTime, 0) || math.Trunc(row.SourceTime) != row.SourceTime {
			return fmt.Errorf("invalid forest observation identity/time")
		}
	}
	reply, err := p.call(forestEpochRequest{Op: "observe", Rows: append([]forestEpochObservation{}, rows...), ObservedPrefix: p.observed})
	if err != nil {
		return err
	}
	expected := p.observed + uint64(len(rows))
	if reply.ObservedPrefix != expected || !forestHashValid(reply.PrefixSHA256) {
		return fmt.Errorf("forest observation acknowledgement mismatch")
	}
	if len(rows) == 0 && p.prefixHash != "" && reply.PrefixSHA256 != p.prefixHash {
		return fmt.Errorf("forest flush changed observation prefix")
	}
	p.observed = expected
	p.prefixHash = reply.PrefixSHA256
	return nil
}

// ObserveGroup preserves the original journal's sub-batch boundaries. The
// worker commits all their SQL writes once before acknowledging this prefix;
// no intermediate provider watermark or plan snapshot is published.
func (p *forestEpochProvider) ObserveGroup(batches [][]forestEpochObservation) error {
	if p.closed {
		return fmt.Errorf("forest provider is closed")
	}
	if len(batches) == 0 || len(batches) > forestEpochObserveGroupBatches {
		return fmt.Errorf("forest observation group must contain 1 to 8 batches")
	}
	expected := p.observed
	for _, rows := range batches {
		if len(rows) == 0 {
			return fmt.Errorf("empty forest observation sub-batch")
		}
		for _, row := range rows {
			if row.Sequence != expected || !forestCanonicalAccount(row.Sender) || !forestCanonicalAccount(row.Recipient) || math.IsNaN(row.SourceTime) || math.IsInf(row.SourceTime, 0) || math.Trunc(row.SourceTime) != row.SourceTime || expected == ^uint64(0) {
				return fmt.Errorf("invalid forest observation identity/time")
			}
			expected++
		}
	}
	reply, err := p.call(forestEpochRequest{Op: "observe_group", Batches: batches, ObservedPrefix: p.observed})
	if err != nil {
		return err
	}
	if reply.ObservedPrefix != expected || !forestHashValid(reply.PrefixSHA256) {
		return fmt.Errorf("forest observation group acknowledgement mismatch")
	}
	p.observed = expected
	p.prefixHash = reply.PrefixSHA256
	return nil
}

func (p *forestEpochProvider) Plan(epoch uint64, _ *partition.CLPAState, current map[string]uint64) (map[string]uint64, error) {
	// Compatibility adapter only. The native Forest controller calls PlanDelta
	// with committed sparse overrides, never this full-map adapter.
	overrides, err := p.currentOverrides(current)
	if err != nil {
		return nil, err
	}
	delta, err := p.PlanDelta(epoch, overrides)
	if err != nil {
		return nil, err
	}
	result := copyEpochMapping(current)
	for account, target := range delta {
		result[account] = target
	}
	return result, nil
}

// PlanDelta reads a complete, committed override snapshot relative to the
// immutable initial ledger. It neither changes that snapshot nor publishes
// proposed owners: only the controller's all-replica Applied barrier does so.
func (p *forestEpochProvider) PlanDelta(epoch uint64, overrides map[string]uint64) (map[string]uint64, error) {
	if p.closed || epoch != p.lastEpoch+1 {
		return nil, fmt.Errorf("forest plan epoch is not contiguous")
	}
	for account, owner := range overrides {
		if !forestCanonicalAccount(account) || owner >= uint64(params.ShardNum) || owner == p.baseOwner(account) {
			return nil, fmt.Errorf("invalid committed forest override")
		}
	}
	eligible := map[string]bool{}
	for _, account := range p.eligible {
		_, initial := p.initialRoutes[account]
		if !forestCanonicalAccount(account) || eligible[account] || initial {
			return nil, fmt.Errorf("invalid noninitial eligible forest account")
		}
		eligible[account] = true
	}
	hash := clpaEpochMappingDigest(overrides)
	reply, err := p.call(forestEpochRequest{Op: "plan", Epoch: epoch, ObservedPrefix: p.observed, CurrentMapping: overrides, CurrentMappingSHA256: hash, InitialLedgerSHA256: p.initialLedgerHash, Eligible: append([]string{}, p.eligible...)})
	if err != nil {
		return nil, err
	}
	if reply.Epoch != epoch || reply.ObservedPrefix != p.observed || reply.CurrentMappingSHA256 != hash || reply.InitialLedgerSHA256 != p.initialLedgerHash || reply.PrefixSHA256 != p.prefixHash {
		return nil, fmt.Errorf("forest plan is not bound to current epoch/prefix/mapping/ledger")
	}
	if reply.Delta == nil {
		return nil, fmt.Errorf("forest plan omitted changed mapping")
	}
	for account, target := range reply.Delta {
		_, initial := p.initialRoutes[account]
		if !forestCanonicalAccount(account) || target >= uint64(params.ShardNum) || (!initial && !eligible[account]) {
			return nil, fmt.Errorf("forest proposed invalid or uncreated account %s", account)
		}
		owner, ok := overrides[account]
		if !ok {
			owner = p.baseOwner(account)
		}
		if target == owner {
			return nil, fmt.Errorf("forest delta contains unchanged owner %s", account)
		}
	}
	var explanation, evidence map[string]interface{}
	if json.Unmarshal(reply.Report, &explanation) != nil || explanation == nil || json.Unmarshal(reply.Evidence, &evidence) != nil || evidence == nil {
		return nil, fmt.Errorf("forest plan omitted explanatory report/evidence")
	}
	p.report = map[string]interface{}{"provider": "forest", "epoch": epoch, "observed_count": p.observed, "input_sha256": p.inputHash, "initial_ledger_sha256": p.initialLedgerHash, "current_mapping_sha256": hash, "current_mapping_semantics": "complete sparse overrides relative to immutable initial ledger; noninitial fallback is native hash", "eligible_semantics": "initial ledger accounts plus supplied noninitial terminal-created accounts", "override_count": len(overrides), "additional_eligible_count": len(p.eligible), "request_sha256": reply.RequestSHA256, "prefix_sha256": p.prefixHash, "plan_request_json_bytes": reply.RequestBytes, "plan_reply_json_bytes": reply.ReplyBytes, "report": explanation, "evidence": evidence}
	p.lastEpoch = epoch
	return reply.Delta, nil
}

func (p *forestEpochProvider) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return p.close()
}

func epochPlannerReport(provider CLPAEpochPlanProvider) map[string]interface{} {
	if reporter, ok := provider.(interface{ Report() map[string]interface{} }); ok {
		return reporter.Report()
	}
	return nil
}

// The run goroutine calls this directly before dispatch. No planning call can
// interleave between observation acknowledgement and that batch's dispatch.
// It deliberately performs pipe I/O without holding the controller mutex.
func (e *clpaEpochController) observeForestRows(rows []forestEpochObservation) error {
	p, ok := e.provider.(*forestEpochProvider)
	if !ok {
		return nil
	}
	if uint64(e.dispatched) != p.observed {
		return fmt.Errorf("forest observation prefix differs from dispatched prefix")
	}
	return p.Observe(rows)
}

func forestObservationFromCSV(row []string, seq uint64, sender, recipient string) (forestEpochObservation, error) {
	if len(row) < 2 {
		return forestEpochObservation{}, fmt.Errorf("forest input lacks source timestamp")
	}
	source, err := strconv.ParseFloat(row[1], 64)
	if err != nil || math.IsNaN(source) || math.IsInf(source, 0) || math.Trunc(source) != source {
		return forestEpochObservation{}, fmt.Errorf("forest input source timestamp must be finite integer seconds")
	}
	return forestEpochObservation{Sequence: seq, SourceTime: source, Sender: sender, Recipient: recipient}, nil
}
