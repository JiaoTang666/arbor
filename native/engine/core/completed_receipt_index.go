package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const completedReceiptIndexLimit = 64 << 20

// journalGate owns this disposable index. It caches only validated receipt
// digests, never queue payloads or an alternative durable completion fact.
// Metadata remains checked on every call, under the same trusted immutable-
// artifact assumption as MigrationArtifactIndex (not hostile inode/mtime reuse).
type completedReceiptIndex struct {
	outbox, journal string
	limit, used     int64
	fullReload      bool
	roots           map[string]os.FileInfo
	files           map[string]os.FileInfo
	receipts        map[string][sha256.Size]byte
	stats           completedReceiptIndexStats
}

type completedReceiptIndexStats struct {
	Files, Reads, Validated int
	BytesRead               int64
	FullReload              bool
}

func (p *TxPool) checkedCompletedReceiptStateLocked(directory string, items []CompletedHandoffConfirmation) (map[string]completedBatchReceiptItem, error) {
	if p.completedReceipts == nil {
		p.completedReceipts = &completedReceiptIndex{limit: completedReceiptIndexLimit}
	}
	return p.completedReceipts.check(directory, p.migrationJournal.directory, items)
}

func (x *completedReceiptIndex) check(directory, journal string, items []CompletedHandoffConfirmation) (map[string]completedBatchReceiptItem, error) {
	outbox, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	journal, err = filepath.Abs(journal)
	if err != nil {
		return nil, err
	}
	if x.files == nil {
		x.outbox, x.journal = outbox, journal
		x.files, x.roots = map[string]os.FileInfo{}, map[string]os.FileInfo{}
		x.receipts = map[string][sha256.Size]byte{}
	} else if x.outbox != outbox || x.journal != journal {
		return nil, fmt.Errorf("completed receipt index scope changed")
	}
	if err := x.refresh(); err != nil {
		return nil, err
	}
	selected := make(map[string]completedBatchReceiptItem, len(items))
	if x.fullReload {
		all, err := loadCompletedBatchReceipts(outbox, journal)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if prior, ok := all[item.H.ID]; ok {
				selected[item.H.ID] = prior
			}
		}
	} else {
		for _, item := range items {
			prior, ok := x.receipts[item.H.ID]
			if !ok {
				continue
			}
			current := completedBatchReceiptItem{ID: item.H.ID, Ack: item.Ack, Relay: item.Relay}
			digest, err := completedReceiptDigest(current)
			if err != nil || digest != prior {
				return nil, fmt.Errorf("conflicting completed batch receipt for %s", item.H.ID)
			}
			selected[item.H.ID] = current
		}
	}
	// Per-item legacy files are still checked afresh for this batch only.
	return checkSelectedCompletedReceipts(directory, items, selected)
}

func completedReceiptDigest(item completedBatchReceiptItem) ([sha256.Size]byte, error) {
	raw, err := json.Marshal(item)
	return sha256.Sum256(raw), err
}

func (x *completedReceiptIndex) refresh() error {
	x.stats = completedReceiptIndexStats{}
	newFiles, roots := map[string]os.FileInfo{}, map[string]os.FileInfo{}
	delta := map[string][sha256.Size]byte{}
	visited := map[string]bool{}
	cost, overflow := int64(0), x.fullReload
	directories := []string{x.outbox}
	if x.journal != x.outbox {
		directories = append(directories, x.journal)
	}
	for _, directory := range directories {
		root, err := os.Stat(directory)
		if err != nil {
			return err
		}
		if !root.IsDir() {
			return fmt.Errorf("completed receipt scope is not a directory")
		}
		if old, ok := x.roots[directory]; ok && !os.SameFile(old, root) {
			return fmt.Errorf("completed receipt directory replaced: %s", directory)
		}
		roots[directory] = root
		entries, err := os.ReadDir(directory)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			batch := directory == x.outbox && strings.HasPrefix(name, completedBatchReceiptPrefix) && strings.HasSuffix(name, ".json")
			queue := directory == x.journal && strings.HasSuffix(name, ".queue.json")
			if !batch && !queue {
				continue
			}
			path := filepath.Join(directory, name)
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("nonregular completed receipt artifact %s", path)
			}
			x.stats.Files++
			visited[path] = true
			if old, ok := x.files[path]; ok {
				if !sameArtifactFile(old, info) {
					return fmt.Errorf("immutable completed receipt artifact changed: %s", path)
				}
				continue
			}
			fileCost := int64(len(path) + 256)
			if overflow || fileCost > x.limit-x.used-cost {
				overflow = true
				continue // The original stateless loader validates the whole set.
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			x.stats.Reads++
			x.stats.BytesRead += int64(len(raw))
			after, err := os.Lstat(path)
			if err != nil || !sameArtifactFile(info, after) {
				return fmt.Errorf("completed receipt artifact changed during read: %s", path)
			}
			var receipts []completedBatchReceiptItem
			if batch {
				receipts, err = decodeCompletedBatchReceipt(name, raw)
			} else if completed, e := classifyConfirmGeneration(path, raw); e != nil {
				err = e
			} else if completed {
				var s migrationQueueSnapshot
				s, err = decodeConfirmGeneration(raw)
				for _, c := range s.Confirmations {
					receipts = append(receipts, c.Receipt)
				}
			}
			if err != nil {
				return err
			}
			cost += fileCost
			newFiles[path] = after
			x.stats.Validated += len(receipts)
			for _, receipt := range receipts {
				digest, err := completedReceiptDigest(receipt)
				if err != nil {
					return err
				}
				prior, known := x.receipts[receipt.ID]
				if pending, ok := delta[receipt.ID]; ok {
					prior, known = pending, true
				}
				if known && prior != digest {
					return fmt.Errorf("conflicting completed confirmations for %s", receipt.ID)
				}
				if !known {
					cost += int64(len(receipt.ID) + sha256.Size + 128)
					delta[receipt.ID] = digest
				}
			}
			if cost > x.limit-x.used {
				overflow = true
			}
		}
		after, err := os.Stat(directory)
		if err != nil || !os.SameFile(root, after) {
			return fmt.Errorf("completed receipt directory changed during read")
		}
	}
	for path := range x.files {
		if !visited[path] {
			return fmt.Errorf("immutable completed receipt artifact removed: %s", path)
		}
	}
	// No partial scan may contribute a remembered confirmation. The bounded
	// fallback retains old metadata checks but does not use stale receipt data.
	x.roots = roots
	x.fullReload, x.stats.FullReload = overflow, overflow
	if !overflow {
		for path, info := range newFiles {
			x.files[path] = info
		}
		for id, digest := range delta {
			x.receipts[id] = digest
		}
		x.used += cost
	}
	return nil
}
