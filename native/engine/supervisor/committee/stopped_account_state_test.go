package committee

import (
	"blockEmulator/core"
	"fmt"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

// readSAAAccount reads a stopped node's committed account state for test fixtures.
func readSAAAccount(root string, shard uint64, account string) (*core.Block, *core.AccountState, []byte, error) {
	db, err := bolt.Open(filepath.Join(root, fmt.Sprintf("exp/database/chainDB/S%d_N0", shard)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return nil, nil, nil, err
	}
	var head *core.Block
	err = db.View(func(tx *bolt.Tx) error {
		heads, blocks := tx.Bucket([]byte("newestBlockHash")), tx.Bucket([]byte("block"))
		if heads == nil || blocks == nil {
			return fmt.Errorf("chain absent")
		}
		data := blocks.Get(heads.Get([]byte("OnlyNewestBlock")))
		if len(data) == 0 {
			return fmt.Errorf("head absent")
		}
		head = core.DecodeB(data)
		return nil
	})
	db.Close()
	if err != nil {
		return nil, nil, nil, err
	}
	state, err := rawdb.NewLevelDBDatabase(filepath.Join(root, fmt.Sprintf("exp/database/mptDB/ldb/s%d/n0", shard)), 0, 1, "saa-readonly", true)
	if err != nil {
		return nil, nil, nil, err
	}
	defer state.Close()
	st, err := trie.New(trie.TrieID(common.BytesToHash(head.Header.StateRoot)), trie.NewDatabase(state))
	if err != nil {
		return nil, nil, nil, err
	}
	raw, err := st.Get([]byte(account))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(raw) == 0 {
		return nil, nil, nil, fmt.Errorf("account absent")
	}
	return head, core.DecodeAS(raw), raw, nil
}
