package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// Find the original block that introduced the matching activation marker.
// A local database witness, not an independently verified quorum certificate.
func (bc *BlockChain) TargetActivationCommit(c TargetActivation) (*core.Block, error) {
	expected, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	current := bc.CurrentBlock
	var installed *core.Block
	for {
		st, err := trie.New(trie.TrieID(common.BytesToHash(current.Header.StateRoot)), bc.triedb)
		if err != nil {
			return nil, err
		}
		raw, err := st.Get(targetActivationKey(c.Account))
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			break
		}
		if !bytes.Equal(raw, expected) {
			return nil, fmt.Errorf("activation evidence mismatch")
		}
		installed = current
		if current.Header.Number == 0 {
			break
		}
		current, err = bc.Storage.GetBlock(current.Header.ParentBlockHash)
		if err != nil {
			return nil, err
		}
	}
	if installed == nil {
		return nil, fmt.Errorf("activation not committed")
	}
	return installed, nil
}
