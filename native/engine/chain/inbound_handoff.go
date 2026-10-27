package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

func (bc *BlockChain) transactionWritesHeldAccount(tx *core.Transaction, account string) bool {
	if tx == nil {
		return false
	}
	if tx.HasBroker {
		return core.TransactionTouchesAccount(tx, account)
	}
	local := bc.ChainConfig.ShardID
	account = normalizePartitionMapKey(account)
	return (!tx.Relayed && normalizePartitionMapKey(tx.Sender) == account && bc.ResolveTxSenderShard(tx) == local) || (normalizePartitionMapKey(tx.Recipient) == account && bc.ResolveTxRecipientShard(tx) == local)
}

type InboundDebit struct {
	Original []byte
	Credit   core.MigrationHandoff
	Block    uint64
}

// Execute only the source-owned debit, with a durable matching credit handoff.
// Requires consensus serialization; the migration account itself stays frozen.
func (bc *BlockChain) CommitInboundDebit(tx *core.Transaction, version uint64) (*InboundDebit, error) {
	return bc.applyInboundDebit(tx, version, true)
}
func (bc *BlockChain) ValidateInboundDebit(tx *core.Transaction, version uint64) error {
	_, err := bc.applyInboundDebit(tx, version, false)
	return err
}
func (bc *BlockChain) applyInboundDebit(tx *core.Transaction, version uint64, commit bool) (*InboundDebit, error) {
	p := bc.inboundPreparation(tx)
	if p == nil || tx == nil || tx.Relayed || tx.HasBroker || !tx.RoutingFrozen || tx.Value == nil || tx.Value.Sign() < 0 || len(tx.TxHash) != 32 || version <= p.Command.MappingVersion || normalizePartitionMapKey(tx.Recipient) != p.Command.Account || normalizePartitionMapKey(tx.Sender) == p.Command.Account || tx.RoutedSenderShard != p.Command.SourceShard || tx.RoutedRecipientShard != p.Command.SourceShard {
		return nil, fmt.Errorf("invalid source inbound debit")
	}
	original := tx.Encode()
	hash := sha256.Sum256(append([]byte(fmt.Sprintf("%s|%d|", p.Command.ProposalID, version)), original...))
	key := []byte("__sef_inbound_debit_v1__" + hex.EncodeToString(hash[:]))
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get(key)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		var old InboundDebit
		err = json.Unmarshal(raw, &old)
		return &old, err
	}
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if indexed {
		credit, lookupErr := LookupTransactionEffect(st, tx, core.TransactionCredit)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if credit == EffectSeen {
			return nil, fmt.Errorf("inbound credit already included in frozen source state")
		}
		if credit != EffectUnseen {
			return nil, fmt.Errorf("inbound source credit coverage unknown")
		}
	}
	if !indexed {
		sender, err := bc.handoffAccountState(tx.Sender, tx.SenderRequiresExistingState)
		if err != nil {
			return nil, err
		}
		if sender.Balance.Cmp(tx.Value) < 0 {
			return nil, fmt.Errorf("insufficient source debit balance")
		}
	}
	debit := core.DecodeTx(original)
	debit.RoutedRecipientShard = p.Command.TargetShard
	// Preserve the original endpoint contract; an ordinary new sender may be
	// lazily created, but a required-existing sender must never be recreated.
	if err = bc.CheckPreparedTransactions([]*core.Transaction{debit}); err != nil {
		return nil, err
	}
	relay := core.DecodeTx(original)
	relay.Relayed = true
	credit, err := core.BuildMigrationHandoff(relay, p.Command.ProposalID, p.Command.Account, p.Command.SourceShard, p.Command.TargetShard, version)
	if group := bc.GroupSourcePreparation(p.Command.ProposalID); group != nil {
		if version != p.Command.MappingVersion+1 {
			return nil, fmt.Errorf("group inbound mapping generation mismatch")
		}
		accounts := []string{}
		for _, m := range group.Members {
			accounts = append(accounts, m.Command.Account)
		}
		credit, err = core.BuildGroupMigrationHandoff(relay, p.Command.ProposalID, accounts, p.Command.SourceShard, p.Command.TargetShard, version)
	}
	if err != nil {
		return nil, err
	}
	if indexed {
		if _, err = bc.applyIndexedRawRelay(st, debit, true); err != nil {
			return nil, err
		}
	}
	if !commit {
		return &InboundDebit{Original: original, Credit: *credit, Block: bc.CurrentBlock.Header.Number + 1}, nil
	}
	next := st
	if !indexed {
		root := bc.GetUpdateStatusTrie([]*core.Transaction{debit})
		next, err = trie.New(trie.TrieID(root), bc.triedb)
		if err != nil {
			return nil, err
		}
	}
	frozen, err := next.Get([]byte(p.Command.Account))
	if err != nil || !bytes.Equal(frozen, p.State) {
		return nil, fmt.Errorf("inbound debit altered frozen account")
	}
	for _, group := range bc.GroupSourcePreparations() {
		for _, m := range group.Members {
			frozen, err := next.Get([]byte(m.Command.Account))
			if err != nil || !bytes.Equal(frozen, m.State) {
				return nil, fmt.Errorf("inbound debit altered frozen group member")
			}
		}
	}
	record := &InboundDebit{Original: original, Credit: *credit, Block: bc.CurrentBlock.Header.Number + 1}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = next.Update(key, encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(next); err != nil {
		return nil, err
	}
	return record, nil
}
