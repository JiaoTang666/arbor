package ownership

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// RefreshMetrics measures one serialized journal refresh. Certificates counts
// consumed complete records, including idempotent replays, not version changes.
// PublishSeconds includes state-lock waiting and atomic apply/offset updates.
type RefreshMetrics struct {
	TotalSeconds        float64 `json:"total_seconds"`
	RefreshWaitSeconds  float64 `json:"refresh_wait_seconds"`
	ReadDecodeSeconds   float64 `json:"read_decode_seconds"`
	VerifySeconds       float64 `json:"verify_seconds"`
	PublishSeconds      float64 `json:"publish_seconds"`
	StatusEncodeSeconds float64 `json:"status_encode_seconds"`
	StatusWriteSeconds  float64 `json:"status_write_seconds"`
	FileCloseSeconds    float64 `json:"file_close_seconds"`
	LockReleaseSeconds  float64 `json:"lock_release_seconds"`
	Certificates        uint64  `json:"certificates"`
}

// LoadRuntimeView pins public identities and the initial state, not future
// owners. The local append transport is trusted OS IPC; each record still needs
// its own quorum signature. Restart replays the authenticated prefix.
func LoadRuntimeView(configPath, configHash, journalPath string, genesis map[string]uint64, genesisHash string, shards uint64) (*View, error) {
	base, err := NewGenesis(genesis, shards)
	if err != nil {
		return nil, err
	}
	return loadRuntimeViewWithGenesis(configPath, configHash, journalPath, base, genesisHash, shards)
}

func loadRuntimeViewWithGenesis(configPath, configHash, journalPath string, genesis *Genesis, genesisHash string, shards uint64) (*View, error) {
	var config Config
	if err := ReadPinnedJSON(configPath, configHash, &config, 1<<20); err != nil {
		return nil, err
	}
	v, err := NewVerifier(config)
	if err != nil {
		return nil, err
	}
	if v.Context().GenesisDigest != genesisHash || v.Context().Shards != shards {
		return nil, fmt.Errorf("runtime ownership genesis/topology mismatch")
	}
	view, err := newViewWithGenesis(v, genesis)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(journalPath)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("runtime ownership journal is not a regular file")
	}
	view.configHash, view.journalPath, view.journalIdentity = configHash, journalPath, info
	if err := view.Refresh(); err != nil {
		return nil, err
	}
	return view, nil
}

// Refresh consumes complete bounded records once. An incomplete last record is
// never adopted; a bad signature, reordered version, truncate or replacement
// fails closed without deleting or skipping the offending record.
func (view *View) Refresh() error {
	_, err := view.RefreshMeasured()
	return err
}

// RefreshMeasured keeps journal consumption serialized without holding the
// owner-table lock during file reads, JSON decoding or public verification.
func (view *View) RefreshMeasured() (metrics RefreshMetrics, err error) {
	totalStarted := time.Now()
	// Registered first: include file close and lock release on every return.
	defer func() { metrics.TotalSeconds = time.Since(totalStarted).Seconds() }()
	if view == nil || view.journalPath == "" {
		return metrics, nil
	}
	waitStarted := time.Now()
	view.refreshMu.Lock()
	metrics.RefreshWaitSeconds = time.Since(waitStarted).Seconds()
	defer func() {
		started := time.Now()
		view.refreshMu.Unlock()
		metrics.LockReleaseSeconds = time.Since(started).Seconds()
	}()
	if status := view.runtimeStatus; status != nil {
		if status.err != nil {
			return metrics, status.err
		}
		if status.closed {
			return metrics, fmt.Errorf("runtime ownership diagnostics already closed")
		}
	}

	publishStarted := time.Now()
	view.mu.RLock()
	offset, initialVersion := view.journalOffset, view.version
	version, certifiedOwners := view.version, len(view.owners)
	view.mu.RUnlock()
	metrics.PublishSeconds += time.Since(publishStarted).Seconds()

	readStarted := time.Now()
	f, err := os.Open(view.journalPath)
	if err != nil {
		metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
		return metrics, err
	}
	defer func() {
		started := time.Now()
		closeErr := f.Close()
		metrics.FileCloseSeconds = time.Since(started).Seconds()
		if err == nil {
			err = closeErr
		}
	}()
	info, err := f.Stat()
	if err != nil || !os.SameFile(info, view.journalIdentity) || info.Size() < offset {
		metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
		return metrics, fmt.Errorf("runtime ownership journal replaced or truncated")
	}
	if info.Size() == offset {
		metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
		return metrics, nil
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
		return metrics, err
	}
	remaining := info.Size() - offset
	bufferSize := int64(1 << 20)
	if remaining < bufferSize {
		// Leave one byte of slack so a partial last record reaches EOF,
		// rather than looking like an oversized record at a full buffer.
		bufferSize = remaining + 1
	}
	// A typical certificate is tens of KiB. Preserve the same 1 MiB record
	// limit without allocating a full MiB for every node's small new suffix.
	reader := bufio.NewReaderSize(io.LimitReader(f, remaining), int(bufferSize))
	metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
	for {
		readStarted = time.Now()
		raw, err := reader.ReadSlice('\n')
		if err == io.EOF {
			metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
			if version != initialVersion {
				started := time.Now()
				// Keep the old six fields and alphabetical JSON order, without
				// allocating a map of interfaces for each refresh.
				status, encodeErr := json.Marshal(struct {
					CertifiedOwners         int    `json:"certified_owners"`
					JournalOffset           int64  `json:"journal_offset"`
					Mode                    string `json:"mode"`
					PID                     int    `json:"pid"`
					RegistryVersion         uint64 `json:"registry_version"`
					VerifiedNewCertificates uint64 `json:"verified_new_certificates"`
				}{certifiedOwners, offset, "verified_runtime_append_journal", os.Getpid(), version, version - initialVersion})
				line := append(append([]byte("RUNTIME_OWNERSHIP_AUTHORIZATION "), status...), '\n')
				metrics.StatusEncodeSeconds = time.Since(started).Seconds()
				if encodeErr != nil {
					return metrics, encodeErr
				}
				started = time.Now()
				writeErr := view.runtimeStatusLocked().append(line)
				metrics.StatusWriteSeconds = time.Since(started).Seconds()
				if writeErr != nil {
					return metrics, writeErr
				}
			}
			return metrics, nil
		}
		if err != nil {
			metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
			return metrics, fmt.Errorf("runtime certificate record exceeds bound: %w", err)
		}
		var cert Certificate
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cert); err != nil {
			metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
			return metrics, err
		}
		if decoder.Decode(new(interface{})) != io.EOF {
			metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
			return metrics, fmt.Errorf("trailing runtime certificate data")
		}
		metrics.ReadDecodeSeconds += time.Since(readStarted).Seconds()
		verifyStarted := time.Now()
		checked, digest, err := view.verifier.Verify(cert)
		metrics.VerifySeconds += time.Since(verifyStarted).Seconds()
		if err != nil {
			return metrics, err
		}
		if checked.Batch.EvidenceVersion == 0 {
			return metrics, fmt.Errorf("runtime certificate lacks signed causal evidence")
		}
		publishStarted = time.Now()
		view.mu.Lock()
		if view.journalOffset != offset {
			view.mu.Unlock()
			metrics.PublishSeconds += time.Since(publishStarted).Seconds()
			return metrics, fmt.Errorf("runtime ownership journal offset changed during refresh")
		}
		_, err = view.apply(checked, digest)
		if err == nil {
			offset += int64(len(raw))
			view.journalOffset = offset
			version, certifiedOwners = view.version, len(view.owners)
		}
		view.mu.Unlock()
		metrics.PublishSeconds += time.Since(publishStarted).Seconds()
		if err != nil {
			return metrics, err
		}
		metrics.Certificates++
	}
}

// CheckPlanDependency requires the first-owner certificate used in this batch
// to bind the same causal prior as the placement result. Previously bound
// accounts retain their owner and do not need reauthorization every epoch.
func (view *View) CheckPlanDependency(account string, sequence, version uint64, digest string, cutoff uint64) error {
	view.mu.RLock()
	defer view.mu.RUnlock()
	if _, ok := view.ownerLocked(account); !ok {
		return fmt.Errorf("plan account is not authorized")
	}
	if dependency, ok := view.evidence[account]; ok && dependency.AdmissionStart <= sequence && sequence < dependency.AdmissionEnd {
		if dependency.EvidenceVersion != version || dependency.EvidenceDigest != digest || dependency.EvidenceCutoff != cutoff {
			return fmt.Errorf("plan and first-owner certificate use different evidence")
		}
	}
	if dependency, ok := view.evidence[account]; ok && sequence < dependency.AdmissionStart {
		return fmt.Errorf("future first-owner certificate used before its input batch")
	}
	return nil
}

func (view *View) CheckAdmission(account string, sequence uint64) error {
	view.mu.RLock()
	defer view.mu.RUnlock()
	if dependency, ok := view.evidence[account]; ok && sequence < dependency.AdmissionStart {
		return fmt.Errorf("future first-owner certificate used before its input batch")
	}
	return nil
}
