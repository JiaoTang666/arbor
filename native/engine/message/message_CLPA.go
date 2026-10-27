package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
)

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64
}

type AccountTransferMsg struct {
	ModifiedMap        map[string]uint64
	Addrs              []string
	AccountState       []*core.AccountState
	ATid               uint64
	MigrationEnvelopes []StateTransferEnvelope `json:"migration_envelopes,omitempty"`
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs              []string
	AccountState       []*core.AccountState
	Txs                []*core.Transaction
	FromShard          uint64
	MigrationEnvelopes []StateTransferEnvelope `json:"migration_envelopes,omitempty"`
}

type StateTransferEnvelope struct {
	ProposalID      string `json:"proposal_id"`
	Account         string `json:"account"`
	SourceShard     uint64 `json:"source_shard"`
	TargetShard     uint64 `json:"target_shard"`
	MappingVersion  uint64 `json:"mapping_version"`
	AnchorVersion   uint64 `json:"anchor_version"`
	SourceStateRoot string `json:"source_state_root"`
	StateHash       string `json:"state_hash"`
}
type StateTransferReceipt struct {
	ProposalID      string `json:"proposal_id"`
	Account         string `json:"account"`
	SourceShard     uint64 `json:"source_shard"`
	TargetShard     uint64 `json:"target_shard"`
	MappingVersion  uint64 `json:"mapping_version"`
	AnchorVersion   uint64 `json:"anchor_version"`
	StateHash       string `json:"state_hash"`
	SourceStateRoot string `json:"source_state_root,omitempty"`
	TargetStateRoot string `json:"target_state_root,omitempty"`
	InstallBlock    uint64 `json:"install_block,omitempty"`
	InstallRound    uint64 `json:"install_round,omitempty"`
	Installed       bool   `json:"installed"`
	Committed       bool   `json:"committed"`
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}
