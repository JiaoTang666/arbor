package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	"os"
	"strings"
)

// Called from serialized RawRelay commit handling. Read exactly the committed
// trie root; missing accounts are not synthesized. Payload size excludes
// storage/code bodies, queued transactions, proof and transport overhead.
func (bc *BlockChain) ForecastBlockAccountStateSizes(block *core.Block) (map[string]uint64, string, string) {
	if strings.TrimSpace(os.Getenv("SAA_INTEGRATED_CONTROLLER")) != "forecast" {
		return nil, "", "disabled"
	}
	if block == nil || bc.triedb == nil {
		return nil, "", "state_trie_missing"
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), bc.triedb)
	if err != nil {
		return nil, "", "state_trie_unavailable"
	}
	result := map[string]uint64{}
	seen := map[string]bool{}
	for _, tx := range block.Body {
		if tx == nil {
			continue
		}
		for _, account := range []string{tx.Sender, tx.Recipient} {
			if seen[account] {
				continue
			}
			seen[account] = true
			encoded, err := st.Get([]byte(account))
			if err != nil {
				return nil, "", "state_read_failed"
			}
			if encoded != nil {
				result[account] = uint64(len(encoded))
			}
		}
	}
	return result, hex.EncodeToString(block.Header.StateRoot), "committed_touched_account_trie_payload_only"
}
