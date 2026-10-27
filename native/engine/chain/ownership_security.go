package chain

import (
	"blockEmulator/core"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"fmt"
	"os"
	"strings"
)

// The v1 view is pinned before genesis and contains public verification keys
// only. It authorizes placement, not payments, remote debit proofs or PBFT votes.
func (bc *BlockChain) configureOwnershipSecurity() error {
	var loadGenesis func() (*ownership.Genesis, error)
	var digest string
	if bc.initialLedger != nil {
		loadGenesis, digest = bc.initialLedger.SealRoutes, bc.initialLedger.FileHash
	}
	view, err := ownership.FromEnvWithGenesis(loadGenesis, digest, bc.ChainConfig.ShardNums)
	if err != nil || view == nil {
		return err
	}
	if params.ConsensusMethod != 3 {
		return fmt.Errorf("ownership security v1 supports RawRelay only")
	}
	for _, key := range []string{"BLOCK_EMULATOR_CLPA_EPOCH_V1", "SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1", "SAA_INTEGRATED_CONTROLLER", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_DYNAMIC_ENABLED", "SAA_FIRST_PLACEMENT_FENCE"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" && value != "false" && value != "0" && value != "off" {
			return fmt.Errorf("ownership security conflicts with %s", key)
		}
	}
	for _, key := range []string{externalMappingUpdatePathEnv, runtimeUnknownMappingUpdatePathEnv} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("ownership security conflicts with %s", key)
		}
	}
	// Migration transports are deliberately excluded instead of pretending
	// that an account-creation certificate also authorizes state replacement.
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if value != "" && (strings.HasPrefix(key, "SAA_GROUP_") ||
			(strings.HasPrefix(key, "SAA_NATIVE_") && (strings.HasSuffix(key, "_PATH") || strings.HasSuffix(key, "_DIR") || strings.HasSuffix(key, "_ROOT")))) {
			return fmt.Errorf("ownership security excludes migration transport %s", key)
		}
	}
	if mode := os.Getenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER"); mode != "" && mode != "off" && mode != "fifo" {
		return fmt.Errorf("ownership security v1 requires unchanged FIFO selection")
	}
	bc.ownership = view
	return bc.Txpool.ConfigureTransactionValidation(bc.CheckOwnershipTransaction)
}

// CheckOwnershipTransaction is side-effect free and shared by admission,
// proposal validation, replay and direct indexed execution. The registry
// version is NOT a routing version: this no-migration path retains version 0.
func (bc *BlockChain) CheckOwnershipTransaction(tx *core.Transaction) error {
	if bc.ownership == nil {
		return nil
	}
	if tx == nil || !tx.RoutingFrozen || tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" || len(tx.RawTxHash) != 0 || tx.FirstPlacement != nil || tx.MappingVersion != 0 {
		return fmt.Errorf("ownership security requires frozen, unmigrated RawRelay")
	}
	if tx.Value == nil || tx.Value.Sign() < 0 {
		return fmt.Errorf("invalid ownership execution value")
	}
	if len(tx.TxHash) != 32 || len(tx.RawTxHash) != 0 {
		return fmt.Errorf("invalid RawRelay transaction identity")
	}
	bindings := [...]ownership.Binding{{Account: tx.Sender, Shard: tx.RoutedSenderShard},
		{Account: tx.Recipient, Shard: tx.RoutedRecipientShard}}
	for _, binding := range bindings {
		canonical, err := ownership.NormalizeAccount(binding.Account)
		// RawRelay's state trie uses the original address bytes. Accepting an
		// alias here would authorize multiple state keys for one account.
		if err != nil || canonical != binding.Account {
			return fmt.Errorf("noncanonical ownership execution account")
		}
	}
	if err := bc.ownership.CheckAuthorizedBindings(bindings[:], tx.IngressSequence); err != nil {
		return err
	}
	owner := tx.RoutedSenderShard
	if tx.Relayed {
		if tx.RoutedSenderShard == tx.RoutedRecipientShard {
			return fmt.Errorf("local transfer cannot be a Relay second leg")
		}
		owner = tx.RoutedRecipientShard
	}
	if owner != bc.ChainConfig.ShardID {
		return fmt.Errorf("authorized execution stage belongs to another shard")
	}
	return nil
}

func (bc *BlockChain) checkOwnershipTransactions(txs []*core.Transaction) error {
	if bc.ownership != nil {
		if err := bc.RefreshOwnershipAuthorization(); err != nil {
			return err
		}
		for _, tx := range txs {
			if err := bc.CheckOwnershipTransaction(tx); err != nil {
				return err
			}
		}
	}
	return nil
}

// RefreshOwnershipAuthorization is the native batch boundary for journal I/O
// and quorum verification. Transaction and queue checks only read the verified
// append-only table; a missing owner never triggers I/O under the pool lock.
func (bc *BlockChain) RefreshOwnershipAuthorization() error {
	if bc.ownership == nil {
		return nil
	}
	return bc.ownership.Refresh()
}

func (bc *BlockChain) OwnershipSecurityEnabled() bool { return bc.ownership != nil }

// Called before the node's main goroutine returns, not only from asynchronous
// database shutdown: the final verified-head diagnostics are audit evidence.
func (bc *BlockChain) CloseOwnershipDiagnostics() error {
	return bc.ownership.CloseRuntimeDiagnostics()
}

func (bc *BlockChain) OwnershipSecurityStatus() ownership.BootstrapStatus {
	return bc.ownership.Status()
}

// Network callers reject invalid authorization without terminating the node.
// This is not a new consensus path: valid work enters the original FIFO pool.
func (bc *BlockChain) TrySendAuthorizedTxs(txs []*core.Transaction) error {
	if bc.ownership == nil {
		return fmt.Errorf("ownership security not enabled")
	}
	if err := bc.RefreshOwnershipAuthorization(); err != nil {
		return err
	}
	return bc.Txpool.AddAuthorizedTxs(txs)
}
