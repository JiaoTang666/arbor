package pbft_all

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (n *PbftConsensusNode) sourceQueueDirectory() string {
	root := os.Getenv("SAA_NATIVE_SOURCE_QUEUE_ROOT")
	if root == "" {
		return ""
	}
	return filepath.Join(root, fmt.Sprintf("s%d-n%d", n.ShardID, n.NodeID))
}

// Called in the commit hook before the state transition, never PrePrepare.
// Local pending transactions are not part of the replicated account state.
func (n *PbftConsensusNode) prepareSourceQueue(c core.SourcePrepareCommand) error {
	dir := n.sourceQueueDirectory()
	if dir == "" {
		return nil
	}
	if n.sourceQueueReady {
		return nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err = core.PublishSourcePrepareFile(filepath.Join(dir, "binding.json"), raw); err != nil {
		return err
	}
	if err = n.CurChain.Txpool.EnableMigrationQueueJournal(dir, c.Account); err != nil {
		return err
	}
	n.sourceQueueReady = true
	return nil
}

// Run before networking. A precommit journal without committed preparation is
// quarantined rather than silently discarded or treated as an activated route.
func (n *PbftConsensusNode) restoreSourceQueue() error {
	if err := n.restoreSourceQueueUnchecked(); err != nil {
		return err
	}
	return n.CurChain.Txpool.ValidateIndexedMigrationAdmission()
}

func (n *PbftConsensusNode) restoreSourceQueueUnchecked() error {
	if core.SerialGroupEnabled() {
		return n.restoreSerialSourceQueue()
	}
	dir := n.sourceQueueDirectory()
	if dir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "binding.json"))
	groupRaw, groupErr := os.ReadFile(filepath.Join(dir, "group-binding.json"))
	group := n.CurChain.GroupSourcePreparation()
	if group != nil || groupErr == nil {
		if groupErr != nil {
			return groupErr
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("group and legacy queue bindings coexist")
		}
		var c core.GroupPrepareCommand
		if e := json.Unmarshal(groupRaw, &c); e != nil {
			return e
		}
		if group == nil {
			return fmt.Errorf("group queue exists without committed preparation; recovery required")
		}
		if len(c.Members) != len(group.Command.Members) {
			return fmt.Errorf("group queue binding mismatch")
		}
		for i, m := range c.Members {
			if m != group.Command.Members[i] {
				return fmt.Errorf("group queue member mismatch")
			}
		}
		if _, e := os.Stat(filepath.Join(dir, "00000000000000000001.queue.json")); e != nil {
			return e
		}
		accounts := []string{}
		for _, m := range c.Members {
			accounts = append(accounts, m.Account)
		}
		if e := n.CurChain.Txpool.EnableGroupMigrationQueueJournal(dir, accounts); e != nil {
			return e
		}
		n.sourceQueueReady = true
		return nil
	}
	if !os.IsNotExist(groupErr) {
		return groupErr
	}
	p := n.CurChain.SourcePreparation()
	if os.IsNotExist(err) && p == nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("source queue binding unavailable: %w", err)
	}
	var c core.SourcePrepareCommand
	if err = json.Unmarshal(raw, &c); err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("source queue exists without committed preparation; recovery required")
	}
	if c != p.Command {
		return fmt.Errorf("source queue binding conflicts with committed preparation")
	}
	if _, err = os.Stat(filepath.Join(dir, "00000000000000000001.queue.json")); err != nil {
		return fmt.Errorf("prepared source lacks initial queue snapshot: %w", err)
	}
	if err = n.CurChain.Txpool.EnableMigrationQueueJournal(dir, c.Account); err != nil {
		return err
	}
	n.sourceQueueReady = true
	return nil
}
