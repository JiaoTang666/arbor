package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"fmt"
	"sort"
)

func clpaCanonicalAccount(address string) bool {
	b, err := hex.DecodeString(address)
	return err == nil && len(b) == 20 && hex.EncodeToString(b) == address
}

func clpaEOA(state *core.AccountState) bool {
	return state != nil && state.Balance != nil && state.Balance.Sign() >= 0 &&
		state.AcAddress == "" && len(state.StorageRoot) == 0 && len(state.CodeHash) == 0
}

// ExportCLPAEpochAccounts reads one closed, committed trie. Unlike FetchAccounts,
// an absent account is an error, not an account with fabricated initial balance.
// Caller holds the native consensus/cut boundary; this is not a concurrent read.
func (bc *BlockChain) ExportCLPAEpochAccounts(addrs []string) ([]*core.AccountState, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(addrs))
	states := make([]*core.AccountState, 0, len(addrs))
	for _, address := range addrs {
		if !clpaCanonicalAccount(address) || seen[address] || bc.Get_PartitionMap(address) != bc.ChainConfig.ShardID {
			return nil, fmt.Errorf("invalid CLPA export owner/member %s", address)
		}
		seen[address] = true
		raw, err := st.Get([]byte(address))
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("CLPA export account is absent: %s", address)
		}
		state := core.DecodeAS(raw)
		if !clpaEOA(state) {
			return nil, fmt.Errorf("CLPA v1 supports EOA state only: %s", address)
		}
		states = append(states, state)
	}
	return states, nil
}

// InstallCLPAEpochAccounts is invoked only for a PBFT-committed batch. It validates
// the complete incoming set before writing, persists one state-root block, then
// publishes the route map while ordinary proposals remain behind the epoch fence.
// Old physical copies remain unreachable by ownership, as in upstream CLPA.
// This helper is NOT a distributed atomic commit or a restart recovery protocol.
func (bc *BlockChain) InstallCLPAEpochAccounts(mapping map[string]uint64, addrs []string, states []*core.AccountState, miner int32) error {
	if len(addrs) != len(states) {
		return fmt.Errorf("CLPA account/state length mismatch")
	}
	expected := make(map[string]bool)
	for address, target := range mapping {
		if !clpaCanonicalAccount(address) || target >= bc.ChainConfig.ShardNums {
			return fmt.Errorf("invalid CLPA mapping")
		}
		if err := bc.rejectPreparedWrite(address); err != nil {
			return err
		}
		if target == bc.ChainConfig.ShardID && bc.Get_PartitionMap(address) != target {
			expected[address] = true
		}
	}
	if len(expected) != len(addrs) {
		return fmt.Errorf("CLPA incoming set is incomplete or excessive")
	}
	byAddress := make(map[string]*core.AccountState, len(addrs))
	for i, address := range addrs {
		if !expected[address] || byAddress[address] != nil || !clpaEOA(states[i]) {
			return fmt.Errorf("invalid CLPA incoming account %s", address)
		}
		byAddress[address] = states[i]
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	ordered := append([]string(nil), addrs...)
	sort.Strings(ordered)
	for _, address := range ordered {
		if err := st.Update([]byte(address), byAddress[address].Encode()); err != nil {
			return err
		}
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return err
	}
	bc.pmlock.Lock()
	for address, target := range mapping {
		bc.PartitionMap[address] = target
	}
	bc.pmlock.Unlock()
	return nil
}
