package chain

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func (bc *BlockChain) FindInboundDebit(tx *core.Transaction, version uint64) (*InboundDebit, error) {
	p := bc.inboundPreparation(tx)
	if p == nil || tx == nil {
		return nil, nil
	}
	original := tx.Encode()
	hash := sha256.Sum256(append([]byte(fmt.Sprintf("%s|%d|", p.Command.ProposalID, version)), original...))
	key := []byte("__sef_inbound_debit_v1__" + hex.EncodeToString(hash[:]))
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get(key)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var record InboundDebit
	err = json.Unmarshal(raw, &record)
	return &record, err
}
