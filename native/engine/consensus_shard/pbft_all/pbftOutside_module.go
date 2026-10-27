package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"bytes"
)

// This module used in the blockChain using transaction relaying mechanism.
// "Raw" means that the pbft only make block consensus.
type RawRelayOutsideModule struct {
	pbftNode *PbftConsensusNode
}

// msgType canbe defined in message
func (rrom *RawRelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		rrom.handleRelay(content)
	case message.CRelayWithProof:
		rrom.handleRelayWithProof(content)
	case message.CInject:
		rrom.handleInjectTx(content)
	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (rrom *RawRelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	if !rrom.decodeOutside(content, relay) || !rrom.checkOwnershipTransport(relay.Txs, true, relay.SenderShardID) {
		return
	}
	var err error
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	relay.Txs, err = rrom.pbftNode.filterCommittedRelayCredits(relay.Txs)
	if err != nil {
		rrom.pbftNode.pl.Plog.Printf("rejected relay credit admission: %v", err)
		return // Do not mark admission occurrences or advance the relay boundary.
	}
	relay.Txs = rrom.pbftNode.filterAdmissionOccurrences(relay.Txs, true)
	if !rrom.enqueueOutside(relay.Txs, "relay") {
		return
	}
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

func (rrom *RawRelayOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	if !rrom.decodeOutside(content, rwp) || !rrom.checkOwnershipTransport(rwp.Txs, true, rwp.SenderShardID) {
		return
	}
	var err error
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	if len(rwp.TxProofs) != len(rwp.Txs) {
		rrom.pbftNode.pl.Plog.Println("rejected relay proof/transaction count mismatch")
		return
	}
	// validate the proofs of txs
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		proof := &rwp.TxProofs[i]
		if tx == nil || !bytes.Equal(proof.TxHash, tx.TxHash) || len(proof.KeyList) != len(proof.ValueList) {
			isAllCorrect = false
			break
		}
		if ok, _ := chain.TxProofVerify(tx.TxHash, proof); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		rwp.Txs, err = rrom.pbftNode.filterCommittedRelayCredits(rwp.Txs)
		if err != nil {
			rrom.pbftNode.pl.Plog.Printf("rejected proven relay credit admission: %v", err)
			return
		}
		rwp.Txs = rrom.pbftNode.filterAdmissionOccurrences(rwp.Txs, true)
		rrom.pbftNode.pl.Plog.Println("All proofs are passed.")
		if !rrom.enqueueOutside(rwp.Txs, "relay_with_proof") {
			return
		}
	} else {
		rrom.pbftNode.pl.Plog.Println("Err: wrong proof!")
		return // An invalid proof cannot certify receipt of the relay boundary.
	}

	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

func (rrom *RawRelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	if !rrom.decodeOutside(content, it) || !rrom.checkOwnershipTransport(it.Txs, false, it.ToShardID) {
		return
	}
	it.Txs = rrom.pbftNode.filterAdmissionOccurrences(it.Txs, false)
	if !rrom.enqueueOutside(it.Txs, "inject") {
		return
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, len(it.Txs))
}
