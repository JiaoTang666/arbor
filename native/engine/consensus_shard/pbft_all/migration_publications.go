package pbft_all

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Existing immutable publications need no per-item rewrite. If a commit
// survived but publication did not, the durable single-record path repairs it.
func (n *PbftConsensusNode) ensureInboundPublication(dir string, existing core.MigrationArtifacts, h *core.MigrationHandoff) error {
	if err := core.ValidateMigrationHandoff(h); err != nil {
		return err
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if old, ok := existing[h.ID]; ok {
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("inbound outbox conflict %s", h.ID)
		}
		return nil
	}
	if err = n.publishSourceHandoff(dir, h); err != nil {
		return err
	}
	existing[h.ID] = raw
	return nil
}

// Local to one commit callback, not shared mutable node state or an async job.
// A failed flush never installs seen markers. A retry reconstructs the same
// artifacts from committed execution records without applying balances again.
type migrationPublications struct {
	groups      map[string]core.MigrationArtifacts
	directories map[string]string
	kinds       map[string]string
	seen        []string
}

func (p *migrationPublications) add(path string, raw []byte) error {
	name := filepath.Base(path)
	kind := ""
	for _, k := range []string{".relay-ack.json", ".ack.json", ".relay.json", ".handoff.json"} {
		if strings.HasSuffix(name, k) {
			kind = k
			break
		}
	}
	if kind == "" {
		return fmt.Errorf("unsupported batch publication")
	}
	if p.groups == nil {
		p.groups = map[string]core.MigrationArtifacts{}
		p.directories = map[string]string{}
		p.kinds = map[string]string{}
	}
	dir := filepath.Dir(path)
	key := dir + "|" + kind
	if p.groups[key] == nil {
		p.groups[key] = core.MigrationArtifacts{}
		p.directories[key] = dir
		p.kinds[key] = kind
	}
	id := strings.TrimSuffix(name, kind)
	if old, ok := p.groups[key][id]; ok && string(old) != string(raw) {
		return fmt.Errorf("conflicting buffered publication")
	}
	p.groups[key][id] = append(json.RawMessage(nil), raw...)
	return nil
}
func (p *migrationPublications) mark(key string) { p.seen = append(p.seen, key) }
func (p *migrationPublications) flush(n *PbftConsensusNode) error {
	start := time.Now()
	keys := make([]string, 0, len(p.groups))
	count := 0
	for k, v := range p.groups {
		keys = append(keys, k)
		count += len(v)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := core.PublishMigrationArtifacts(p.directories[key], p.kinds[key], p.groups[key]); err != nil {
			return err
		}
	}
	for _, key := range p.seen {
		n.sourcePrepareSeen.Store(key, true)
	}
	if n.pl != nil && count > 0 {
		n.pl.Plog.Printf("migration publication batch records=%d files=%d elapsedMs=%d", count, len(keys), time.Since(start).Milliseconds())
	}
	return nil
}
