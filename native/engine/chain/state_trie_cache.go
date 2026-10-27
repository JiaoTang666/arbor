package chain

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

const stateTrieCacheEnv = "BLOCK_EMULATOR_STATE_TRIE_CACHE_MB"

// This is geth's bounded, content-addressed clean-node cache, not an account
// cache or an authorization cache. Geth fills it after successful persistence
// or a disk read. Roots, dirty writes, commit errors and security checks retain
// their existing semantics. An unset value preserves the old zero-cache path.
func newStateTrieDatabase(db ethdb.Database) (*trie.Database, error) {
	mb := 0
	if raw := os.Getenv(stateTrieCacheEnv); raw != "" {
		var err error
		mb, err = strconv.Atoi(raw)
		if err != nil || mb < 0 || mb > 1024 {
			return nil, fmt.Errorf("invalid %s=%q: require 0..1024 MiB", stateTrieCacheEnv, raw)
		}
	}
	if mb > 0 {
		fmt.Printf("state_trie_cache cleanMB=%d persistence=unchanged authorization_cache=false\n", mb)
	}
	return trie.NewDatabaseWithConfig(db, &trie.Config{Cache: mb, Preimages: true}), nil
}
