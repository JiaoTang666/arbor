// Package initialledger defines an explicit synthetic initial-state manifest.
// Loading it never infers state from future transactions or a routing fallback.
package initialledger

import (
	"blockEmulator/core"
	"blockEmulator/ownership"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strings"
)

const Marker = "coolchain/initial-ledger-v1"

type Account struct {
	Address     string `json:"account"`
	Balance     string `json:"balance"`
	Nonce       uint64 `json:"nonce"`
	StorageRoot string `json:"storage_root"`
	CodeHash    string `json:"code_hash"`
	Shard       uint64 `json:"shard"`
}
type Manifest struct {
	Schema   string    `json:"schema"`
	Shards   uint64    `json:"shards"`
	Accounts []Account `json:"accounts"`
}
type Ledger struct {
	Manifest
	FileHash     string
	LogicalHash  string
	Routes       map[string]uint64
	sealedRoutes *ownership.Genesis
}

// SealRoutes is a bootstrap-only ownership transfer into a private index.
// Legacy callers retain the mutable Routes API until explicitly sealed. The
// old map is discarded only after validation; any retained caller alias cannot
// mutate the sealed index. Repeated calls reuse the same immutable object.
func (l *Ledger) SealRoutes() (*ownership.Genesis, error) {
	if l.sealedRoutes == nil {
		base, err := ownership.NewGenesis(l.Routes, l.Shards)
		if err != nil {
			return nil, err
		}
		l.sealedRoutes, l.Routes = base, nil
	}
	return l.sealedRoutes, nil
}

func (l *Ledger) HistoricalOwner(account string) (uint64, bool) {
	if l.sealedRoutes != nil {
		return l.sealedRoutes.Owner(account)
	}
	owner, exists := l.Routes[account]
	return owner, exists
}

func (a Account) State() *core.AccountState {
	b, _ := new(big.Int).SetString(a.Balance, 10)
	s, _ := hex.DecodeString(a.StorageRoot)
	c, _ := hex.DecodeString(a.CodeHash)
	return &core.AccountState{Balance: b, Nonce: a.Nonce, StorageRoot: s, CodeHash: c}
}
func Load(path, expected string, shards uint64) (*Ledger, error) {
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	hash := sha256.Sum256(raw)
	if len(expected) != 64 || hex.EncodeToString(hash[:]) != expected {
		return nil, fmt.Errorf("initial ledger file hash mismatch")
	}
	l := &Ledger{FileHash: expected, Routes: map[string]uint64{}}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&l.Manifest); e != nil {
		return nil, e
	}
	var tail interface{}
	if e = dec.Decode(&tail); e != io.EOF {
		return nil, fmt.Errorf("trailing manifest data")
	}
	if l.Schema != "synthetic_initial_ledger_v1" || l.Shards != shards || shards == 0 {
		return nil, fmt.Errorf("initial ledger schema/topology mismatch")
	}
	sort.Slice(l.Accounts, func(i, j int) bool { return l.Accounts[i].Address < l.Accounts[j].Address })
	digest := sha256.New()
	for _, a := range l.Accounts {
		address, e := hex.DecodeString(a.Address)
		b, ok := new(big.Int).SetString(a.Balance, 10)
		if e != nil || len(address) != 20 || strings.ToLower(a.Address) != a.Address || !ok || b.Sign() < 0 || b.String() != a.Balance || a.Shard >= shards {
			return nil, fmt.Errorf("invalid initial account %s", a.Address)
		}
		if _, ok = l.Routes[a.Address]; ok {
			return nil, fmt.Errorf("duplicate initial account")
		}
		for _, h := range []string{a.StorageRoot, a.CodeHash} {
			v, e := hex.DecodeString(h)
			if e != nil || (len(v) != 0 && len(v) != 32) || strings.ToLower(h) != h {
				return nil, fmt.Errorf("invalid state digest")
			}
		}
		l.Routes[a.Address] = a.Shard
		// Stable canonical logical digest deliberately excludes physical placement.
		logical := a
		logical.Shard = 0
		if e = json.NewEncoder(digest).Encode(logical); e != nil {
			return nil, e
		}
	}
	l.LogicalHash = hex.EncodeToString(digest.Sum(nil))
	return l, nil
}
func FromEnv(shards uint64) (*Ledger, error) {
	path := os.Getenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH")
	if path == "" {
		if os.Getenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256") != "" {
			return nil, fmt.Errorf("ledger hash without path")
		}
		return nil, nil
	}
	for _, key := range []string{"BLOCK_EMULATOR_ACCOUNT_CLUSTER_MAP_PATH", "BLOCK_EMULATOR_CLUSTER_SHARD_MAP_PATH"} {
		if os.Getenv(key) != "" {
			return nil, fmt.Errorf("initial ledger conflicts with %s", key)
		}
	}
	// Opt-in only: genesis supplies initial state, while SEF may submit later
	// consensus-ordered migrations. Existing DB/restart rejection is unchanged.
	live := os.Getenv("SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1") == "true"
	if live {
		if os.Getenv("SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT") != "true" {
			return nil, fmt.Errorf("ledger migration requires committed receipt gate")
		}
		for _, key := range []string{"SAA_NATIVE_SOURCE_QUEUE_ROOT", "SAA_NATIVE_SOURCE_PREPARE_PATH", "SAA_NATIVE_SOURCE_PREPARED_PATH", "SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_INSTALLATION_RECEIPT_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_TARGET_ACTIVATED_PATH", "SAA_NATIVE_SOURCE_RETIRED_PATH", "SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
			if os.Getenv(key) == "" {
				return nil, fmt.Errorf("ledger migration missing %s", key)
			}
		}
	}
	for _, key := range []string{"SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_SOURCE_PREPARE_PATH"} {
		if path := os.Getenv(key); path != "" {
			if !live {
				return nil, fmt.Errorf("initial ledger conflicts with %s", key)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				return nil, fmt.Errorf("genesis bootstrap requires an absent migration command: %s", key)
			}
		}
	}
	return Load(path, os.Getenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256"), shards)
}
