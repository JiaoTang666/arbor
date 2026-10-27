package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

var groupHistoryKey = []byte("__sef_group_history_v1__")

func groupRecordKey(base []byte, id ...string) []byte {
	if len(id) == 0 {
		return base
	}
	hash := sha256.Sum256([]byte(id[0]))
	return []byte(string(base) + hex.EncodeToString(hash[:]))
}

func (bc *BlockChain) GroupSourcePreparations() []*core.GroupPreparation {
	bc.sourcePreparationMu.RLock()
	defer bc.sourcePreparationMu.RUnlock()
	var result []*core.GroupPreparation
	for _, p := range bc.groupSourceHistory {
		result = append(result, cloneGroup(p))
	}
	if len(result) == 0 && bc.groupSourcePreparation != nil {
		result = append(result, cloneGroup(bc.groupSourcePreparation))
	}
	return result
}

func (bc *BlockChain) validateNextSourceGroup(c core.GroupPrepareCommand) error {
	history := bc.GroupSourcePreparations()
	generation, err := bc.SourceMigrationGeneration()
	if err != nil {
		return err
	}
	if generation.ActiveProposal != "" {
		return fmt.Errorf("previous source generation not retired")
	}
	for _, old := range history {
		r, err := bc.GroupSourceRetirement(old.Command.Members[0].ProposalID)
		if err != nil {
			return err
		}
		if r == nil {
			return fmt.Errorf("previous source group not retired")
		}
		for _, m := range old.Members {
			for _, next := range c.Members {
				if next.Account == m.Command.Account {
					return fmt.Errorf("account remigration not supported")
				}
			}
		}
	}
	return nil
}

func writeGroupHistory(st *trie.Trie, history []*core.GroupPreparation) error {
	var ids []string
	for _, p := range history {
		id := p.Command.Members[0].ProposalID
		ids = append(ids, id)
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		// Historical snapshots are immutable. Only append the new record; do not
		// rewrite all retired state bytes at every generation.
		old, readErr := st.Get(groupRecordKey(groupPreparationKey, id))
		if readErr != nil {
			return readErr
		}
		if len(old) != 0 {
			if !bytes.Equal(old, raw) {
				return fmt.Errorf("historical preparation changed")
			}
			continue
		}
		if err = st.Update(groupRecordKey(groupPreparationKey, id), raw); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return st.Update(groupHistoryKey, raw)
}

func readGroupHistory(st *trie.Trie, latest *core.GroupPreparation) ([]*core.GroupPreparation, error) {
	raw, err := st.Get(groupHistoryKey)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return []*core.GroupPreparation{cloneGroup(latest)}, nil
	} // legacy one-group state
	var ids []string
	if err = json.Unmarshal(raw, &ids); err != nil {
		return nil, err
	}
	if len(ids) < 1 {
		return nil, fmt.Errorf("invalid group history length")
	}
	var history []*core.GroupPreparation
	seen := map[string]bool{}
	accounts := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("invalid group history identity")
		}
		seen[id] = true
		raw, err = st.Get(groupRecordKey(groupPreparationKey, id))
		if err != nil {
			return nil, err
		}
		var p core.GroupPreparation
		if err = json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		if err = validGroupIdentity(p.Command); err != nil {
			return nil, err
		}
		if p.Command.Members[0].ProposalID != id {
			return nil, fmt.Errorf("group history key mismatch")
		}
		for _, m := range p.Command.Members {
			if accounts[m.Account] {
				return nil, fmt.Errorf("overlapping source histories")
			}
			accounts[m.Account] = true
		}
		history = append(history, &p)
	}
	a, _ := json.Marshal(latest)
	b, _ := json.Marshal(history[len(history)-1])
	if !bytes.Equal(a, b) {
		return nil, fmt.Errorf("latest group differs from history")
	}
	return history, nil
}
