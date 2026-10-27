package pbft_all

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

func (n *PbftConsensusNode) restoreSerialSourceQueue() error {
	dir := n.sourceQueueDirectory()
	if dir == "" {
		return nil
	}
	groups := n.CurChain.GroupSourcePreparations()
	bindings := core.GroupTransportPaths(filepath.Join(dir, "group-binding.json"))
	if len(bindings) != len(groups) {
		return fmt.Errorf("serial queue bindings differ from committed groups; recovery required")
	}
	for _, name := range []string{"binding.json", "group-binding.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return fmt.Errorf("legacy and serial queue formats coexist")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if len(groups) == 0 {
		files, err := filepath.Glob(filepath.Join(dir, "*.queue.json"))
		if err != nil {
			return err
		}
		if len(files) > 0 {
			return fmt.Errorf("unbound queue generations; recovery required")
		}
		return nil
	}
	var union []string
	for _, p := range groups {
		path := core.GroupTransportPath(filepath.Join(dir, "group-binding.json"), p.Command.Members[0].ProposalID)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var c core.GroupPrepareCommand
		if err = json.Unmarshal(raw, &c); err != nil {
			return err
		}
		if !reflect.DeepEqual(c, p.Command) {
			return fmt.Errorf("serial journal binding mismatch")
		}
		for _, m := range c.Members {
			union = append(union, m.Account)
		}
	}
	sort.Strings(union)
	if err := n.CurChain.Txpool.RestoreGroupMigrationQueueJournal(dir, union); err != nil {
		return err
	}
	n.sourceQueueReady = true
	return nil
}
