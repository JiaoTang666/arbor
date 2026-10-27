package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"fmt"
	"math/big"
)

// Read-only prevalidation using the same lazy-initialization rule as ordinary
// Relay. Migrated endpoints keep their explicit must-exist contract. Trie I/O
// errors are never converted into absence; creation occurs only at commit.
func (bc *BlockChain) handoffAccountState(address string, mustExist bool) (*core.AccountState, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get([]byte(address))
	if err != nil {
		return nil, err
	}
	if raw != nil {
		return core.DecodeAS(raw), nil
	}
	if mustExist {
		return nil, fmt.Errorf("required handoff account %q is not present", address)
	}
	return &core.AccountState{Nonce: 0, Balance: new(big.Int).Set(params.Init_Balance)}, nil
}
